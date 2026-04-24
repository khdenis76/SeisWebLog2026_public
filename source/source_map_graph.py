from __future__ import annotations

import re
import threading
import traceback
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from bokeh.core.property.vectorization import value
from bokeh.embed import json_item
from bokeh.io import show
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, HoverTool, Button, CustomJS, Div, Slider, FactorRange, LegendItem, Legend
from bokeh.palettes import Category10, Category20, Turbo256
from bokeh.plotting import figure
from bokeh.models import CheckboxGroup, TextInput, CheckboxGroup
import sqlite3
import geopandas as gpd
from bokeh.models.tiles import WMTSTileSource
from bokeh.transform import factor_cmap
from pyproj import Transformer
import plotly.express as px
import plotly.graph_objects as go



@dataclass
class SourceMapGraphics:
    """
    Source progress map:
      - SLPreplot segments: planned geometry
      - SLSolution segments: actual/processed geometry (with vessel/sailline/seq for hover)

    Assumptions (adjust SQL if your column names differ):
      SLPreplot:  Line, StartX, StartY, EndX, EndY
      SLSolution: Line, SailLine, Seq, VesselName, StartX, StartY, EndX, EndY, ProdShots
    """

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)

    def _connect(self):

        print("\n" + "=" * 80)
        print("[DB OPEN]")
        print("DB:", self.db_path)
        print("THREAD:", threading.get_ident())
        traceback.print_stack(limit=12)

        conn = sqlite3.connect(
            str(self.db_path),
            timeout=120,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA busy_timeout = 120000;")
        return conn

    @contextmanager
    def get_conn(self):
        conn = self._connect()
        try:
            yield conn
        finally:
            print("[DB CLOSE]", self.db_path)
            conn.close()

    def add_project_shapes_layers(
            self,
            p,
            shapes_table: str = "project_shapes",
            default_src_epsg: int | None = None,  # used only if shapefile has no CRS
            fill_alpha: float = 0.25,
            line_alpha: float = 0.95,
            point_size: int = 6,
    ):
        """
        Read all shapefiles from `project_shapes` and plot them on existing Bokeh figure `p`.

        Table schema expected:
          FullName (path), FileName, isFilled, FillColor, LineColor, LineWidth, LineStyle
        """

        def _bokeh_dash(style: str) -> str:
            s = (style or "").strip().lower()
            # Bokeh supports: 'solid', 'dashed', 'dotted', 'dotdash', 'dashdot'
            mapping = {
                "": "solid",
                "solid": "solid",
                "-": "solid",
                "dashed": "dashed",
                "--": "dashed",
                "dash": "dashed",
                "dotted": "dotted",
                ":": "dotted",
                "dotdash": "dotdash",
                "-.": "dashdot",
                "dashdot": "dashdot",
            }
            return mapping.get(s, "solid")

        # 1) Read styles from DB
        with self.get_conn() as con:
            rows = con.execute(f"""
                SELECT
                    FullName,
                    FileName,
                    COALESCE(isFilled, 0) AS isFilled,
                    COALESCE(FillColor, '#000000') AS FillColor,
                    COALESCE(LineColor, '#000000') AS LineColor,
                    COALESCE(LineWidth, 1) AS LineWidth,
                    COALESCE(LineStyle, '') AS LineStyle,
                    COALESCE(HatchPattern, '') AS HatchPattern
                FROM {shapes_table}
                ORDER BY FileName, FullName
            """).fetchall()

        if not rows:
            return p  # nothing to add

        # 2) Plot each shapefile
        for r in rows:
            shp_path = r["FullName"]
            layer_name = (r["FileName"] or Path(shp_path).stem) if shp_path else "shape"

            is_filled = int(r["isFilled"] or 0) == 1
            fill_color = r["FillColor"] or "#000000"
            line_color = r["LineColor"] or "#000000"
            line_width = int(r["LineWidth"] or 1)
            line_dash = _bokeh_dash(r["LineStyle"])
            hatch_pattern = r["HatchPattern"]

            if not shp_path or not Path(shp_path).exists():
                # skip missing files
                continue

            # Load shapefile
            gdf = gpd.read_file(shp_path)

            # Ensure CRS
            if gdf.crs is None:
                if default_src_epsg is None:
                    raise ValueError(f"SHP has no CRS: {shp_path}. Provide default_src_epsg (e.g. 4326 or 32634).")
                gdf = gdf.set_crs(epsg=default_src_epsg)

            # Reproject to WebMercator for tiled maps
            gdf = gdf.to_crs(epsg=3857)

            if gdf.empty:
                continue

            # explode multiparts into single features (important)
            gdf = gdf.explode(index_parts=False)

            gtypes = set(gdf.geometry.geom_type.unique().tolist())

            # ---- Points / MultiPoints ----
            if "Point" in gtypes or "MultiPoint" in gtypes:
                pts = gdf[gdf.geometry.geom_type.isin(["Point", "MultiPoint"])].copy()
                pts = pts.explode(index_parts=False)
                pts["x"] = pts.geometry.x
                pts["y"] = pts.geometry.y
                src = ColumnDataSource(pts.drop(columns=["geometry"], errors="ignore"))
                p.scatter(
                    x="x", y="y",
                    source=src,
                    size=point_size,
                    fill_color=line_color,
                    line_color=line_color,
                    alpha=line_alpha,
                    legend_label=layer_name,
                    level="glyph",
                )

            # ---- Lines / MultiLines ----
            if "LineString" in gtypes or "MultiLineString" in gtypes:
                lines = gdf[gdf.geometry.geom_type.isin(["LineString", "MultiLineString"])].copy()
                lines = lines.explode(index_parts=False)

                xs, ys = [], []
                for geom in lines.geometry:
                    if geom is None:
                        continue
                    x, y = geom.xy
                    xs.append(list(x))
                    ys.append(list(y))

                if xs:
                    src = ColumnDataSource({"xs": xs, "ys": ys})
                    p.multi_line(
                        xs="xs", ys="ys",
                        source=src,
                        line_color=line_color,
                        line_width=line_width,
                        line_dash=line_dash,
                        line_alpha=line_alpha,
                        legend_label=layer_name,
                        level="glyph",
                    )

            # ---- Polygons / MultiPolygons (exterior only) ----
            if "Polygon" in gtypes or "MultiPolygon" in gtypes:
                polys = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
                polys = polys.explode(index_parts=False)

                xs, ys = [], []
                for geom in polys.geometry:
                    if geom is None:
                        continue
                    # exterior ring only (holes ignored)
                    x, y = geom.exterior.xy
                    xs.append(list(x))
                    ys.append(list(y))

                if xs:
                    src = ColumnDataSource({"xs": xs, "ys": ys})
                    hatch = None if hatch_pattern == "" else hatch_pattern
                    p.patches(
                        xs="xs", ys="ys",
                        source=src,
                        fill_color=(fill_color if is_filled else None),
                        fill_alpha=(fill_alpha if is_filled else 0.0),
                        hatch_pattern=hatch,
                        hatch_color=line_color,
                        line_color=line_color,
                        line_width=line_width,
                        line_dash=line_dash,
                        line_alpha=line_alpha,
                        legend_label=layer_name,
                        level="glyph",
                    )

        # click legend to hide/show layers
        if p.legend:
            p.legend.click_policy = "hide"

        return p

    def add_csv_layers_to_map(
            self,
            p,  # bokeh figure
            csv_epsg: int | None = None,
            show_tiles: bool = True,
            max_labels: int = 2000,  # safety: labels can be heavy
    ):
        """
        Add CSVLayers/CSVpoints on top of existing figure `p`.

        Legend label: CSVLayers.Name
        Marker style/size/color: CSVLayers.PointStyle/PointSize/PointColor
        Point text label: CSVpoints.Point (LabelSet)
        """

        def _bokeh_marker(marker: str | None) -> str:
            m = (marker or "").strip().lower()
            allowed = {
                "circle", "square", "triangle", "diamond",
                "inverted_triangle", "asterisk",
                "cross", "x", "star", "hex",
            }
            return m if m in allowed else "circle"

        # ---- load layers + points ----
        with self.get_conn() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            cur.execute("""
                SELECT
                    ID, Name, PointStyle, PointColor, PointSize
                FROM CSVLayers
                ORDER BY ID DESC
            """)
            layers = [dict(r) for r in cur.fetchall()]

            if not layers:
                return p

            # Prepare transformer if needed (to WebMercator for tiles)
            transformer = None
            if show_tiles and csv_epsg:
                transformer = Transformer.from_crs(f"EPSG:{csv_epsg}", "EPSG:3857", always_xy=True)



            for layer in layers:
                layer_id = layer["ID"]
                layer_name = layer.get("Name") or f"Layer {layer_id}"
                marker = _bokeh_marker(layer.get("PointStyle"))
                color = layer.get("PointColor") or "#000000"
                size = int(layer.get("PointSize") or 4)

                cur.execute("""
                    SELECT Point, X, Y, Z, Attr1, Attr2, Attr3
                    FROM CSVpoints
                    WHERE Layer_FK = ?
                """, (layer_id,))
                pts = [dict(r) for r in cur.fetchall()]
                if not pts:
                    continue

                # Build columns
                xs = [row.get("X") for row in pts]
                ys = [row.get("Y") for row in pts]
                names = [str(row.get("Point") or "") for row in pts]

                # Convert CRS if needed
                if transformer:
                    xs, ys = transformer.transform(xs, ys)

                src = ColumnDataSource(data=dict(
                    x=xs,
                    y=ys,
                    Point=names,
                    Z=[row.get("Z") for row in pts],
                    Attr1=[row.get("Attr1") for row in pts],
                    Attr2=[row.get("Attr2") for row in pts],
                    Attr3=[row.get("Attr3") for row in pts],
                ))

                # Draw points (scatter)
                r = p.scatter(
                    "x", "y",
                    source=src,
                    marker=marker,
                    size=size,
                    fill_color=value(color),  # constant color
                    line_color=None,
                    fill_alpha=0.9,
                    legend_label=layer_name,
                )

                # Hover for this layer
                p.add_tools(HoverTool(
                    renderers=[r],
                    tooltips=[
                        ("Layer", layer_name),
                        ("Point", "@Point"),
                        ("X", "@x{0,0.00}"),
                        ("Y", "@y{0,0.00}"),
                        ("Z", "@Z"),
                        ("Attr1", "@Attr1"),
                        ("Attr2", "@Attr2"),
                        ("Attr3", "@Attr3"),
                    ]
                ))

                # Text labels near symbols (LabelSet)
                # NOTE: labels can be heavy; limit for performance
                if max_labels and len(xs) > max_labels:
                    # label only first max_labels points
                    label_src = ColumnDataSource(data=dict(
                        x=xs[:max_labels],
                        y=ys[:max_labels],
                        Point=names[:max_labels],
                    ))
                else:
                    label_src = src

                r_text = p.text(
                    x="x",
                    y="y",
                    text="Point",
                    source=label_src,
                    x_offset=6,
                    y_offset=6,
                    text_font_size="9pt",
                    text_alpha=0.9,
                    legend_label=layer_name,  # ✅ same legend label
                )

        return p
    def _error_layout(
            self,
            title: str,
            message: str,
            *,
            details: str = "",
            level: str = "error",  # "error" | "warning" | "info"
            is_show: bool = False,
            json_return: bool = False,
            retry_js: str = "window.location.reload();",
    ):
        # Timestamp (no imports)
        ts = str(pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S"))

        # Simple HTML escaping (avoid breaking layout if message has < > &)
        def _esc(s):
            if s is None:
                return ""
            s = str(s)
            return (
                s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )

        icon_map = {
            "error": "❌",
            "warning": "⚠️",
            "info": "ℹ️",
        }
        border_map = {
            "error": "#ef4444",
            "warning": "#f59e0b",
            "info": "#3b82f6",
        }
        bg_map = {
            "error": "#fff5f5",
            "warning": "#fffbeb",
            "info": "#eff6ff",
        }

        icon = icon_map.get(level, "❌")
        border = border_map.get(level, "#ef4444")
        bg = bg_map.get(level, "#fff5f5")

        title_html = _esc(title)
        msg_html = _esc(message)
        details_html = _esc(details).replace("\n", "<br>")

        panel = Div(
            text=f"""
            <div style="
                border:1px solid {border};
                border-left:6px solid {border};
                background:{bg};
                padding:12px 14px;
                border-radius:10px;
                font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
            ">
              <div style="display:flex; gap:10px; align-items:flex-start;">
                <div style="font-size:20px; line-height:1;">{icon}</div>
                <div style="flex:1;">
                  <div style="font-weight:700; font-size:14px; margin-bottom:2px;">
                    {title_html}
                  </div>
                  <div style="font-size:13px; margin-bottom:6px;">
                    {msg_html}
                  </div>

                  <div style="display:flex; gap:12px; flex-wrap:wrap; align-items:center;">
                    <div style="font-size:12px; color:#6b7280;">
                      <b>Time:</b> {ts}
                    </div>
                    <div style="font-size:12px; color:#6b7280;">
                      <b>Level:</b> {_esc(level)}
                    </div>
                  </div>

                  {"<div style='margin-top:8px; font-size:12px; color:#374151;'><b>Details:</b><div style='margin-top:4px;'>" + details_html + "</div></div>" if details_html else ""}
                </div>
              </div>
            </div>
            """,
            sizing_mode="stretch_width",
        )

        retry_btn = Button(label="Retry", button_type="primary", width=90)
        retry_btn.js_on_click(CustomJS(code=retry_js))

        # Empty plot placeholder (keeps plot area consistent)
        p = figure(
            height=220,
            toolbar_location=None,
            x_axis_type="datetime",
            title="",
            width_policy="max",
        )
        p.xaxis.visible = False
        p.yaxis.visible = False
        p.xgrid.visible = False
        p.ygrid.visible = False
        p.outline_line_alpha = 0.25

        layout = column(
            panel,
            row(retry_btn, sizing_mode="stretch_width"),
            p,
            sizing_mode="stretch_both",
        )

        if is_show:
            show(layout)
            return None

        if json_return:
            return json_item(layout)

        return layout



    def _plotly_error_html(
            self,
            title="Plot Error",
            message="Something went wrong.",
            details=None,
            level="error",  # "error" | "warning" | "info"
            retry_js=None,  # optional JS function name to call (no parentheses)
            is_show=False,
            json_return=False,
    ):
        """
        Plotly-friendly error output.

        - Default: returns HTML string for {{ plotly_plot|safe }}
        - json_return=True: returns dict suitable for JsonResponse
        - is_show=True: prints the HTML to console (useful in tests) and returns None
        """

        icon_map = {"error": "❌", "warning": "⚠", "info": "ℹ"}
        color_map = {"error": "#f8d7da", "warning": "#fff3cd", "info": "#e7f1ff"}
        border_map = {"error": "#dc3545", "warning": "#ffc107", "info": "#0d6efd"}

        lvl = str(level or "error").strip().lower()
        icon = icon_map.get(lvl, "❌")
        bg = color_map.get(lvl, "#f8d7da")
        border = border_map.get(lvl, "#dc3545")

        # Robust timestamp (works even if datetime wasn't imported elsewhere)
        try:
            ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            ts = ""

        # Avoid None and keep HTML safe-ish
        title_txt = "" if title is None else str(title)
        msg_txt = "" if message is None else str(message)

        retry_button = ""
        if retry_js:
            fn = str(retry_js).strip()
            # allow passing "reloadChart" or "reloadChart()"
            onclick = fn if fn.endswith(")") else f"{fn}()"
            retry_button = f"""
            <button class="btn btn-sm btn-outline-dark mt-3" onclick="{onclick}">
                Retry
            </button>
            """

        details_block = ""
        if details:
            details_block = f"""
            <div class="mt-2 small text-muted" style="white-space:pre-wrap;">
                <b>Details:</b><br>{details}
            </div>
            """

        html = f"""
        <div style="
            border: 1px solid {border};
            background: {bg};
            padding: 20px;
            border-radius: 10px;
            width: 100%;
        ">
            <div style="font-size:18px; font-weight:600;">
                {icon} {title_txt}
            </div>

            <div class="mt-2">
                {msg_txt}
            </div>

            {details_block}

            <div class="mt-3 small text-muted">
                Generated: {ts}
            </div>

            {retry_button}
        </div>
        """

        if is_show:
            # Plotly errors are HTML; showing in console is the safest "show"
            print(html)
            return None

        if json_return:
            # Good for Django JsonResponse({"ok": False, **result})
            return {
                "ok": False,
                "level": lvl,
                "title": title_txt,
                "message": msg_txt,
                "details": details,
                "timestamp": ts,
                "html": html,
            }

        return html

    def build_source_progress_map(
            self,
            production_only: bool = True,
            use_tiles: bool = False,
            is_show: bool = False,
            json_return: bool = False,
            show_shapes: bool = True,
            show_layers: bool = True,
            default_epsg: int = 0,
            max_csv_labels: int = 5,
    ):
        try:
            # ----------------------------
            # Transformer (only needed if tiles are used)
            # ----------------------------
            transformer = None
            if use_tiles and default_epsg:
                transformer = Transformer.from_crs(
                    f"EPSG:{default_epsg}",
                    "EPSG:3857",
                    always_xy=True,
                )

            # ----------------------------
            # SQL
            # ----------------------------
            sql_preplot = """
                SELECT
                    COALESCE(Line, 0)      AS Line,
                    COALESCE(StartX, 0.0)  AS x0,
                    COALESCE(StartY, 0.0)  AS y0,
                    COALESCE(EndX, 0.0)    AS x1,
                    COALESCE(EndY, 0.0)    AS y1
                FROM SLPreplot
                WHERE COALESCE(StartX,0) != 0 AND COALESCE(StartY,0) != 0
                  AND COALESCE(EndX,0)   != 0 AND COALESCE(EndY,0)   != 0
            """

            sql_solution = """
                SELECT
                    COALESCE(sl.Line, 0)                      AS Line,
                    COALESCE(sl.SailLine, '')                 AS SailLine,
                    COALESCE(sl.Seq, 0)                       AS Seq,
                    COALESCE(TRIM(pf.vessel_name), 'Unknown') AS VesselName,
                    COALESCE(sl.StartX, 0.0)                  AS x0,
                    COALESCE(sl.StartY, 0.0)                  AS y0,
                    COALESCE(sl.EndX, 0.0)                    AS x1,
                    COALESCE(sl.EndY, 0.0)                    AS y1,
                    COALESCE(sl.ProductionCount, 0)           AS ProdShots
                FROM SLSolution sl
                LEFT JOIN project_fleet pf
                       ON pf.id = sl.Vessel_FK
                WHERE COALESCE(sl.StartX,0) != 0 AND COALESCE(sl.StartY,0) != 0
                  AND COALESCE(sl.EndX,0)   != 0 AND COALESCE(sl.EndY,0)   != 0
            """
            if production_only:
                sql_solution += " AND COALESCE(sl.ProductionCount,0) > 0 "

            # DSR: Deployment (ROV not empty)
            sql_dsr_deploy = """
                SELECT
                    COALESCE(PrimaryEasting, 0.0)  AS x,
                    COALESCE(PrimaryNorthing, 0.0) AS y,
                    COALESCE(TRIM(ROV), '')        AS ROV
                FROM DSR
                WHERE COALESCE(PrimaryEasting,0) != 0
                  AND COALESCE(PrimaryNorthing,0) != 0
                  AND TRIM(COALESCE(ROV,'')) <> ''
            """

            # DSR: Recovery (ROV1 not empty)
            sql_dsr_recover = """
                SELECT
                    COALESCE(PrimaryEasting, 0.0)  AS x,
                    COALESCE(PrimaryNorthing, 0.0) AS y,
                    COALESCE(TRIM(ROV1), '')       AS ROV1
                FROM DSR
                WHERE COALESCE(PrimaryEasting,0) != 0
                  AND COALESCE(PrimaryNorthing,0) != 0
                  AND TRIM(COALESCE(ROV1,'')) <> ''
            """

            with self.get_conn() as conn:
                preplot_rows = conn.execute(sql_preplot).fetchall()
                sol_rows = conn.execute(sql_solution).fetchall()
                dsr_dep_rows = conn.execute(sql_dsr_deploy).fetchall()
                dsr_rec_rows = conn.execute(sql_dsr_recover).fetchall()

            # Optional: friendly “empty data” panel (instead of blank plot / errors)
            if not preplot_rows and not sol_rows and not dsr_dep_rows and not dsr_rec_rows:
                return self._error_layout(
                    "No data to plot",
                    "SLPreplot, SLSolution and DSR queries returned 0 rows.",
                    details="Check database content and filters (production_only, coordinates != 0).",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            # ----------------------------
            # Helpers
            # ----------------------------
            def _cds_from_rows(rows, keys, transformer=None, xy_keys=("x0", "y0", "x1", "y1")):
                """
                Convert sqlite rows to ColumnDataSource.
                If transformer provided, vectorized transform for xy_keys.
                xy_keys can be ("x0","y0","x1","y1") for segments OR ("x","y") for scatters.
                """
                import numpy as np
                cols = {k: [] for k in keys}
                for r in rows:
                    for i, k in enumerate(keys):
                        cols[k].append(r[i])

                if transformer:
                    try:
                        if len(xy_keys) == 4:
                            ix0 = keys.index(xy_keys[0]);
                            iy0 = keys.index(xy_keys[1])
                            ix1 = keys.index(xy_keys[2]);
                            iy1 = keys.index(xy_keys[3])

                            x0 = np.asarray(cols[keys[ix0]], dtype="float64")
                            y0 = np.asarray(cols[keys[iy0]], dtype="float64")
                            x1 = np.asarray(cols[keys[ix1]], dtype="float64")
                            y1 = np.asarray(cols[keys[iy1]], dtype="float64")

                            x0t, y0t = transformer.transform(x0, y0)
                            x1t, y1t = transformer.transform(x1, y1)

                            cols[keys[ix0]] = x0t.tolist()
                            cols[keys[iy0]] = y0t.tolist()
                            cols[keys[ix1]] = x1t.tolist()
                            cols[keys[iy1]] = y1t.tolist()

                        elif len(xy_keys) == 2:
                            ix = keys.index(xy_keys[0]);
                            iy = keys.index(xy_keys[1])

                            x = np.asarray(cols[keys[ix]], dtype="float64")
                            y = np.asarray(cols[keys[iy]], dtype="float64")

                            xt, yt = transformer.transform(x, y)
                            cols[keys[ix]] = xt.tolist()
                            cols[keys[iy]] = yt.tolist()

                    except ValueError:
                        pass

                return ColumnDataSource(cols)

            def _palette_for(n: int):
                if n <= 10:
                    return Category10[10][:n]
                if n <= 20:
                    return Category20[20][:n]
                if n >= 256:
                    return Turbo256
                step = max(1, 256 // n)
                return Turbo256[::step][:n]

            # ----------------------------
            # Build sources
            # ----------------------------
            preplot_keys = ["Line", "x0", "y0", "x1", "y1"]
            sol_keys = ["Line", "SailLine", "Seq", "VesselName", "x0", "y0", "x1", "y1", "ProdShots"]

            src_preplot = _cds_from_rows(
                preplot_rows,
                preplot_keys,
                transformer=transformer,
                xy_keys=("x0", "y0", "x1", "y1"),
            )

            # group SLSolution by vessel
            by_vessel: dict[str, list[tuple]] = {}
            for r in sol_rows:
                v = r[3]
                v = (str(v).strip() if v is not None else "Unknown")
                if not v:
                    v = "Unknown"
                by_vessel.setdefault(v, []).append(r)

            vessels = sorted(by_vessel.keys())
            pal = _palette_for(max(1, len(vessels)))

            # DSR sources (scatter)
            dsr_dep_keys = ["x", "y", "ROV"]
            dsr_rec_keys = ["x", "y", "ROV1"]
            src_dsr_dep = _cds_from_rows(dsr_dep_rows, dsr_dep_keys, transformer=transformer, xy_keys=("x", "y"))
            src_dsr_rec = _cds_from_rows(dsr_rec_rows, dsr_rec_keys, transformer=transformer, xy_keys=("x", "y"))

            # ----------------------------
            # Plot
            # ----------------------------
            p = figure(
                x_axis_type="mercator" if use_tiles else "linear",
                y_axis_type="mercator" if use_tiles else "linear",
                title="Source Progress Map (SLPreplot vs SLSolution)",
                x_axis_label="Easting",
                y_axis_label="Northing",
                tools="pan,wheel_zoom,reset,save",
                active_scroll="wheel_zoom",
                sizing_mode="stretch_both",
                match_aspect=True,
            )

            if use_tiles:
                p.add_tile(WMTSTileSource(url="https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"))

            if show_shapes:
                self.add_project_shapes_layers(p, default_src_epsg=default_epsg)

            if show_layers:
                self.add_csv_layers_to_map(
                    p,
                    csv_epsg=default_epsg,
                    show_tiles=use_tiles,
                    max_labels=max_csv_labels,
                )

            # Planned (SLPreplot)
            p.segment(
                x0="x0", y0="y0", x1="x1", y1="y1",
                source=src_preplot,
                line_width=2,
                line_alpha=0.30,
                line_dash="dashed",
                legend_label="SLPreplot (planned)",
            )

            # DSR scatters (Deployment + Recovery)
            dsr_default_size = 6
            r_dep = p.scatter(
                x="x", y="y",
                source=src_dsr_dep,
                marker="circle",
                size=dsr_default_size,
                alpha=0.65, color="green", fill_color="green",
                legend_label="Deployment",
            )
            r_rec = p.scatter(
                x="x", y="y",
                source=src_dsr_rec,
                marker="circle",
                size=dsr_default_size,
                alpha=0.65, color="red", fill_color="red",
                legend_label="Recovery",
            )

            # Actual per vessel (SLSolution)
            sol_renderers = []
            for i, vessel in enumerate(vessels):
                src_v = _cds_from_rows(
                    by_vessel[vessel],
                    sol_keys,
                    transformer=transformer,
                    xy_keys=("x0", "y0", "x1", "y1"),
                )
                r = p.segment(
                    x0="x0", y0="y0", x1="x1", y1="y1",
                    source=src_v,
                    line_width=3,
                    line_alpha=0.95,
                    line_color=pal[i % len(pal)],
                    legend_label=vessel,
                )
                sol_renderers.append(r)

            # Hovers
            p.add_tools(HoverTool(
                renderers=sol_renderers,
                tooltips=[
                    ("Vessel", "@VesselName"),
                    ("SailLine", "@SailLine"),
                    ("Seq", "@Seq"),
                    ("Line", "@Line"),
                    ("ProdShots", "@ProdShots"),
                ],
            ))
            p.add_tools(HoverTool(
                renderers=[r_dep],
                tooltips=[("Deployment ROV", "@ROV"), ("X", "@x"), ("Y", "@y")],
            ))
            p.add_tools(HoverTool(
                renderers=[r_rec],
                tooltips=[("Recovery ROV1", "@ROV1"), ("X", "@x"), ("Y", "@y")],
            ))

            # Legend
            p.legend.title = "Layers"
            p.legend.click_policy = "hide"

            # ----------------------------
            # JS Controls: legend hide + legend corners + DSR size
            # ----------------------------
            # --- Legend buttons (use exactly your working sample pattern)
            controls_items = []

            if p.legend and len(p.legend) > 0:
                toggle_legend_btn = Button(label="Hide legend", button_type="primary", width=120)
                toggle_legend_btn.js_on_click(CustomJS(
                    args=dict(legend=p.legend[0], btn=toggle_legend_btn),
                    code="""
                               legend.visible = !legend.visible;
                               btn.label = legend.visible ? "Hide legend" : "Show legend";
                           """
                ))

                cycle_legend_pos_btn = Button(label="Legend position", button_type="default", width=150)
                cycle_legend_pos_btn.js_on_click(CustomJS(
                    args=dict(legend=p.legend[0]),
                    code="""
                               const positions = ["top_left", "top_right", "bottom_right", "bottom_left"];
                               const current = legend.location;
                               const idx = positions.indexOf(current);
                               legend.location = positions[(idx + 1) % positions.length];
                           """
                ))

                controls_items.extend([toggle_legend_btn, cycle_legend_pos_btn])

            dsr_size = Slider(title="DSR point size", start=2, end=25, step=1, value=dsr_default_size, width=260)
            dsr_size.js_on_change("value", CustomJS(args=dict(r_dep=r_dep, r_rec=r_rec), code="""
                r_dep.glyph.size = cb_obj.value;
                r_rec.glyph.size = cb_obj.value;
            """))
            controls_items.append(dsr_size)
            controls = row(*controls_items, sizing_mode="stretch_width")
            layout = column(controls, p, sizing_mode="stretch_both")

            if is_show:
                show(layout)
                return None

            if json_return:
                return json_item(layout, target="source-progress-map")

            return layout

        except Exception as e:
            return self._error_layout(
                "Source Progress Map failed",
                "An error occurred while generating the Source QC map.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
                retry_js="window.location.reload();",
            )

    def build_source_sunburst(
            self,
            is_show: bool = False,
            json_return: bool = False,
            title: str = "Source — Vessel → Purpose → Shot totals",
            drop_zeros: bool = True,
            theme: str = "light",  # "light" or "dark"
            legend_bottom: bool = True,
    ):
        if theme == "dark":
            bg_color = "#1e1e1e"
            font_color = "#e0e0e0"
            line_color = "#2a2a2a"

            vessel_palette = [
                "#4dabf7", "#ff922b", "#51cf66", "#ff6b6b",
                "#b197fc", "#ffa94d", "#63e6be", "#ced4da"
            ]

            purpose_palette = [
                "#20c997", "#fcc419", "#339af0",
                "#da77f2", "#adb5bd", "#ff8787"
            ]

            metric_colors = {
                "Production": "#69db7c",
                "Non-Production": "#ffa94d",
                "Kill": "#ff6b6b",
            }

        else:  # LIGHT
            bg_color = "#ffffff"
            font_color = "#212529"
            line_color = "#ffffff"

            vessel_palette = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"
            ]

            purpose_palette = [
                "#1abc9c", "#f39c12", "#3498db",
                "#9b59b6", "#95a5a6", "#e74c3c"
            ]

            metric_colors = {
                "Production": "#2ecc71",
                "Non-Production": "#f39c12",
                "Kill": "#e74c3c",
            }
        try:
            sql = """
            SELECT vessel_name, purpose, 'ProductionTotal' AS metric, COALESCE(ProductionTotal,0) AS value
            FROM V_SLSolution_VesselPurposeSummary
            UNION ALL
            SELECT vessel_name, purpose, 'NonProductionTotal', COALESCE(NonProductionTotal,0)
            FROM V_SLSolution_VesselPurposeSummary
            UNION ALL
            SELECT vessel_name, purpose, 'KillTotal', COALESCE(KillTotal,0)
            FROM V_SLSolution_VesselPurposeSummary
            """

            with self.get_conn() as conn:
                df = pd.read_sql(sql, conn)
            # -----------------------------
            # Clean NULL values (IMPORTANT)
            # -----------------------------
            df["vessel_name"] = df["vessel_name"].fillna("Unknown Vessel").astype(str).str.strip()
            df["purpose"] = df["purpose"].fillna("Other").astype(str).str.strip()
            df["metric"] = df["metric"].fillna("Other").astype(str).str.strip()

            # also clean empty strings
            df.loc[df["vessel_name"] == "", "vessel_name"] = "Unknown Vessel"
            df.loc[df["purpose"] == "", "purpose"] = "Other"
            df.loc[df["metric"] == "", "metric"] = "Other"
            if df is None or df.empty:
                return self._plotly_error_html(
                    title="No data",
                    message="V_SLSolution_VesselPurposeSummary returned no rows.",
                    details="Sunburst requires at least one row.",
                    level="info",
                    is_show=is_show,
                    json_return=json_return,
                )

            df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)

            if drop_zeros:
                df = df[df["value"] > 0].copy()

            if df.empty:
                return self._plotly_error_html(
                    title="No non-zero totals",
                    message="All totals are zero.",
                    details="Nothing to draw after filtering zeros.",
                    level="info",
                    is_show=is_show,
                    json_return=json_return,
                )

            # Outer ring labels
            df["metric_label"] = df["metric"].map({
                "ProductionTotal": "Production",
                "NonProductionTotal": "Non-Production",
                "KillTotal": "Kill",
            }).fillna(df["metric"])

            # -----------------------------
            # Deterministic colors (NO hardcoded vessel names)
            # -----------------------------
            vessel_palette = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
                "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
                "#2E86C1", "#28B463", "#AF7AC5", "#F5B041", "#EC7063",
                "#45B39D", "#5D6D7E", "#DC7633", "#922B21", "#1ABC9C",
            ]
            purpose_palette = [
                "#16a085", "#27ae60", "#2980b9", "#8e44ad", "#2c3e50",
                "#f39c12", "#d35400", "#c0392b", "#7f8c8d", "#34495e",
            ]
            metric_colors = {
                "Production": "#2ecc71",
                "Non-Production": "#f39c12",
                "Kill": "#e74c3c",
            }

            def _pick_from_palette(key, palette):
                s = "" if key is None else str(key)
                h = 0
                for ch in s:
                    h = (h * 31 + ord(ch)) & 0xFFFFFFFF
                return palette[h % len(palette)]

            # Purpose colors: stable + fallback
            purpose_colors = {
                "Production": "#1abc9c",
                "Non-Production": "#f39c12",
                "Non-Production-Infill": "#f1c40f",
                "Production-Infill": "#3498db",
                "Test": "#9b59b6",
                "Other": "#95a5a6",
            }

            # -----------------------------
            # Build explicit node arrays (so each ring has its own color set)
            # -----------------------------
            grp_purpose = df.groupby(["vessel_name", "purpose"], as_index=False, dropna=False)["value"].sum()
            grp_vessel = df.groupby(["vessel_name"], as_index=False, dropna=False)["value"].sum()

            labels, parents, values, ids, colors = [], [], [], [], []

            ROOT_ID = "root"
            labels.append("All Vessels")
            parents.append("")
            values.append(float(grp_vessel["value"].sum()))
            ids.append(ROOT_ID)
            colors.append("#ffffff")

            # Ring 1: vessels (each vessel different)
            vessel_color_map = {}
            for _, r in grp_vessel.iterrows():
                v = r["vessel_name"]
                vessel_color_map[v] = _pick_from_palette(v, vessel_palette)

                v_id = f"v|{v}"
                labels.append(str(v))
                parents.append(ROOT_ID)
                values.append(float(r["value"]))
                ids.append(v_id)
                colors.append(vessel_color_map[v])

            # Ring 2: purposes (each purpose different)
            purpose_color_map = {}
            for _, r in grp_purpose.iterrows():
                v = r["vessel_name"]
                p = r["purpose"]

                # fixed map if known, otherwise deterministic fallback (still from DB value)
                purpose_color_map[p] = purpose_colors.get(p, _pick_from_palette(p, purpose_palette))

                v_id = f"v|{v}"
                p_id = f"p|{v}|{p}"
                labels.append(str(p))
                parents.append(v_id)
                values.append(float(r["value"]))
                ids.append(p_id)
                colors.append(purpose_color_map[p])

            # Ring 3: metric categories (each category different)
            for _, r in df.iterrows():
                v = r["vessel_name"]
                p = r["purpose"]
                m = r["metric_label"]
                val = float(r["value"])

                p_id = f"p|{v}|{p}"
                m_id = f"m|{v}|{p}|{m}"

                labels.append(str(m))
                parents.append(p_id)
                values.append(val)
                ids.append(m_id)
                colors.append(metric_colors.get(m, "#bdc3c7"))

            fig = go.Figure()

            fig.add_trace(
                go.Sunburst(
                    ids=ids,
                    labels=labels,
                    parents=parents,
                    values=values,
                    branchvalues="total",
                    marker=dict(colors=colors, line=dict(color=line_color, width=1)),
                    hovertemplate="<b>%{label}</b><br>Value: %{value}<br>%{percentParent} of parent<extra></extra>",
                    # ✅ NO showlegend here (Sunburst doesn't support it)
                )
            )
            fig.update_layout(
                title=title,
                margin=dict(t=55, l=10, r=10, b=10),
                showlegend=False,
                paper_bgcolor=bg_color,
                plot_bgcolor=bg_color,
                font=dict(color=font_color),

                xaxis=dict(visible=False),
                yaxis=dict(visible=False),
            )

            # -----------------------------
            # Legend at bottom (dummy traces)
            # -----------------------------
            if legend_bottom:
                # Vessel legend
                for v, c in sorted(vessel_color_map.items(), key=lambda x: str(x[0])):
                    fig.add_trace(
                        go.Scatter(
                            x=[None], y=[None],
                            mode="markers",
                            marker=dict(size=10, color=c),
                            name=f"Vessel: {v}",
                            legendgroup=f"vessel|{v}",
                            showlegend=True,
                            hoverinfo="skip",
                        )
                    )

                # Purpose legend
                for p, c in sorted(purpose_color_map.items(), key=lambda x: str(x[0])):
                    fig.add_trace(
                        go.Scatter(
                            x=[None], y=[None],
                            mode="markers",
                            marker=dict(size=10, color=c),
                            name=f"Purpose: {p}",
                            legendgroup=f"purpose|{p}",
                            showlegend=True,
                            hoverinfo="skip",
                        )
                    )

                # Metric legend
                for m, c in metric_colors.items():
                    fig.add_trace(
                        go.Scatter(
                            x=[None], y=[None],
                            mode="markers",
                            marker=dict(size=10, color=c),
                            name=f"Type: {m}",
                            legendgroup=f"metric|{m}",
                            showlegend=True,
                            hoverinfo="skip",
                        )
                    )

            fig.update_layout(
                title=title,
                margin=dict(t=55, l=10, r=10, b=10),
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=-0.10,
                    xanchor="left",
                    x=0.0,
                    title_text="Legend",
                    itemsizing="constant",
                ) if legend_bottom else dict(),
            )
            fig.update_layout(
                title=title,
                margin=dict(t=55, l=10, r=10, b=10),
                showlegend=False,

                xaxis=dict(
                    visible=False,
                    showgrid=False,
                    zeroline=False
                ),
                yaxis=dict(
                    visible=False,
                    showgrid=False,
                    zeroline=False
                )
            )

            if is_show:
                fig.show()
                return None

            if json_return:
                return {"ok": True, "title": title, "figure": fig.to_dict()}

            return fig.to_html(full_html=False, include_plotlyjs=False)

        except Exception as e:
            return self._plotly_error_html(
                title="Sunburst build failed",
                message="Could not generate Plotly Sunburst chart.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

    def build_daybyday_source_production(
            self,
            production_code: str,
            non_production_code: str,
            kill_code: str,
            is_show: bool = False,
            json_return: bool = False,
            title: str = "Day-by-Day Source Production",
            max_days: int | None = None,
            include_other: bool = False,
    ):
        try:
            def _hex_to_rgb(h):
                h = h.lstrip("#")
                return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)

            def _rgb_to_hex(r, g, b):
                r = max(0, min(255, int(r)))
                g = max(0, min(255, int(g)))
                b = max(0, min(255, int(b)))
                return f"#{r:02x}{g:02x}{b:02x}"

            def _shade(hex_color, factor):
                r, g, b = _hex_to_rgb(hex_color)
                return _rgb_to_hex(r * factor, g * factor, b * factor)

            def _safe_name(s):
                s = str(s or "").strip()
                return s if s else "Unknown"

            def _safe_field_name(s):
                s = str(s or "").strip()
                s = s.replace(".", "_")
                s = s.replace(" ", "_")
                s = s.replace("-", "_")
                s = s.replace("/", "_")
                s = s.replace("\\", "_")
                s = s.replace("(", "_")
                s = s.replace(")", "_")
                s = s.replace("[", "_")
                s = s.replace("]", "_")
                s = s.replace("{", "_")
                s = s.replace("}", "_")
                s = s.replace(":", "_")
                s = s.replace(";", "_")
                s = s.replace(",", "_")
                while "__" in s:
                    s = s.replace("__", "_")
                return s.strip("_")

            prod_set = production_code
            nonprod_set = non_production_code
            kill_set = kill_code

            sql = """
                SELECT
                    COALESCE(s.Year, 0) AS Year,
                    COALESCE(s.JDay, 0) AS JDay,
                    COALESCE(s.Vessel_FK, 0) AS Vessel_FK,
                    COALESCE(pf.vessel_name, '') AS vessel_name,
                    COALESCE(s.FireCode, '') AS FireCode,
                    COUNT(*) AS Cnt
                FROM SPSolution s
                LEFT JOIN project_fleet pf
                       ON pf.id = s.Vessel_FK
                GROUP BY
                    COALESCE(s.Year, 0),
                    COALESCE(s.JDay, 0),
                    COALESCE(s.Vessel_FK, 0),
                    COALESCE(pf.vessel_name, ''),
                    COALESCE(s.FireCode, '')
                ORDER BY
                    Year ASC,
                    JDay ASC,
                    Vessel_FK ASC
            """

            conn = self._connect()
            try:
                cur = conn.cursor()
                rows = cur.execute(sql).fetchall()
            finally:
                print("[DB CLOSE]", self.db_path)
                conn.close()

            keys = ["prod", "nonprod", "kill", "other", "total"]
            data = defaultdict(lambda: {k: 0 for k in keys})

            days_set = set()
            vessels_set = set()

            for year, jday, vessel_fk, vessel_name, firecode, cnt in rows:
                year = int(year or 0)
                jday = int(jday or 0)
                firecode = str(firecode or "")
                cnt = int(cnt or 0)

                if year <= 0 or jday <= 0:
                    continue

                dt = datetime(year, 1, 1) + timedelta(days=jday - 1)
                vessel_name = _safe_name(vessel_name) or f"Vessel {int(vessel_fk or 0)}"

                key = (dt, vessel_name)

                days_set.add(dt)
                vessels_set.add(vessel_name)

                if firecode in prod_set:
                    data[key]["prod"] += cnt
                elif firecode in nonprod_set:
                    data[key]["nonprod"] += cnt
                elif firecode in kill_set:
                    data[key]["kill"] += cnt
                else:
                    data[key]["other"] += cnt

                data[key]["total"] += cnt

            days = sorted(days_set)
            if max_days and max_days > 0:
                days = days[-max_days:]

            vessels = sorted(vessels_set)

            if not days or not vessels:
                p = figure(
                    height=420,
                    width=1200,
                    x_axis_type="datetime",
                    title=title,
                    toolbar_location="above",
                )
                p.text(
                    x=[datetime.now()],
                    y=[0],
                    text=["No data"],
                    text_font_size="14px",
                )
                if is_show:
                    show(p)
                return json_item(p) if json_return else p

            palette = Category20[20] if len(vessels) > 10 else Category10[10]
            vessel_color = {v: palette[i % len(palette)] for i, v in enumerate(vessels)}

            cat_factor = {
                "Production": 1.00,
                "NonProduction": 0.78,
                "Kill": 0.58,
                "Other": 0.38,
            }

            stackers = []
            stacker_labels = []
            stacker_colors = []

            for v in vessels:
                base = vessel_color[v]

                prod_field = _safe_field_name(f"{v}.Production")
                nonprod_field = _safe_field_name(f"{v}.Non-Production")
                kill_field = _safe_field_name(f"{v}.Kill")

                stackers.append(prod_field)
                stacker_labels.append(f"{v}.Production")
                stacker_colors.append(_shade(base, cat_factor["Production"]))

                stackers.append(nonprod_field)
                stacker_labels.append(f"{v}.Non-Production")
                stacker_colors.append(_shade(base, cat_factor["NonProduction"]))

                stackers.append(kill_field)
                stacker_labels.append(f"{v}.Kill")
                stacker_colors.append(_shade(base, cat_factor["Kill"]))

                if include_other:
                    other_field = _safe_field_name(f"{v}.Other")
                    stackers.append(other_field)
                    stacker_labels.append(f"{v}.Other")
                    stacker_colors.append(_shade(base, cat_factor["Other"]))

            src_dict = {
                "x": days,
                "DateStr": [d.strftime("%Y-%m-%d") for d in days],
                "Total": [],
            }

            for field in stackers:
                src_dict[field] = []

            for d in days:
                day_total = 0

                for v in vessels:
                    prod_val = data[(d, v)]["prod"]
                    nonprod_val = data[(d, v)]["nonprod"]
                    kill_val = data[(d, v)]["kill"]
                    other_val = data[(d, v)]["other"]

                    src_dict[_safe_field_name(f"{v}.Production")].append(prod_val)
                    src_dict[_safe_field_name(f"{v}.Non-Production")].append(nonprod_val)
                    src_dict[_safe_field_name(f"{v}.Kill")].append(kill_val)

                    if include_other:
                        src_dict[_safe_field_name(f"{v}.Other")].append(other_val)

                    day_total += prod_val + nonprod_val + kill_val
                    if include_other:
                        day_total += other_val

                src_dict["Total"].append(day_total)

            for field, label in zip(stackers, stacker_labels):
                src_dict[f"lbl__{field}"] = [label] * len(days)

            source = ColumnDataSource(src_dict)

            p = figure(
                sizing_mode='stretch_both',
                x_axis_type="datetime",
                title=title,
                toolbar_location="above",
                tools="pan,wheel_zoom,box_zoom,reset,save",
            )

            rs = p.vbar_stack(
                stackers,
                x="x",
                width=24 * 60 * 60 * 1000 * 0.8,
                source=source,
                color=stacker_colors,
                legend_label=stacker_labels,
            )

            tooltips = [
                ("Date", "@DateStr"),
                ("Total", "@Total"),
            ]

            for field, label in zip(stackers, stacker_labels):
                tooltips.append((label, f"@{field}"))

            p.add_tools(HoverTool(tooltips=tooltips, renderers=rs))

            p.xaxis.axis_label = "Date"
            p.yaxis.axis_label = "Points count"
            p.grid.grid_line_alpha = 0.15

            p.legend.location = "top_left"
            p.legend.orientation="horizontal"
            p.legend.click_policy = "hide"
            p.legend.label_text_font_size = "10px"
            controls_items=[]
            if p.legend and len(p.legend) > 0:
                toggle_legend_btn = Button(label="Hide legend", button_type="primary", width=120)
                toggle_legend_btn.js_on_click(CustomJS(
                    args=dict(legend=p.legend[0], btn=toggle_legend_btn),
                    code="""
                               legend.visible = !legend.visible;
                               btn.label = legend.visible ? "Hide legend" : "Show legend";
                           """
                ))

                cycle_legend_pos_btn = Button(label="Legend position", button_type="default", width=150)
                cycle_legend_pos_btn.js_on_click(CustomJS(
                    args=dict(legend=p.legend[0]),
                    code="""
                               const positions = ["top_left", "top_right", "bottom_right", "bottom_left"];
                               const current = legend.location;
                               const idx = positions.indexOf(current);
                               legend.location = positions[(idx + 1) % positions.length];
                           """
                ))

                controls_items.extend([toggle_legend_btn, cycle_legend_pos_btn])

            controls = row(*controls_items, sizing_mode="stretch_width")
            layout = column(controls, p, sizing_mode="stretch_both")

            if is_show:
               show(layout)
               return None
            if json_return:
               return json_item(layout) if json_return else p
            else:
                return layout

        except Exception as e:
            p = self._error_layout("build_daybyday_source_production", e)
            if is_show:
                show(p)
                return None
            elif json_return:
                return json_item(p) if json_return else p
            else:
                return p

    def build_sp_solution_vs_preplot(
            self,
            line: int,
            is_show: bool = False,
            json_return: bool = False,
            label_every: int = 10,
            preplot_dash: str = "dashed",
            point_size: int = 7,
    ):
        line = int(line)
        label_every = max(1, min(20, int(label_every)))
        point_size = max(2, int(point_size))

        sql_preplot = """
            SELECT
                Line,
                Point,
                X AS x,
                Y AS y,
                Z AS z,
                PointCode
            FROM SPPreplot
            WHERE Line = ?
              AND COALESCE(X, 0) != 0
              AND COALESCE(Y, 0) != 0
            ORDER BY Point
        """

        sql_solution = """
            SELECT
                s.Line,
                s.Seq,
                s.Point,
                s.Easting AS x,
                s.Northing AS y,
                s.Elevation AS z,
                s.FireCode,
                s.PointCode,
                s.ArrayCode,
                COALESCE(sva.purpose, 'Unknown') AS SeqPurpose
            FROM SPSolution s
            LEFT JOIN sequence_vessel_assignment sva
                ON s.Seq BETWEEN sva.seq_first AND sva.seq_last
               AND COALESCE(sva.is_active, 1) = 1
            WHERE s.Line = ?
              AND COALESCE(s.Easting, 0) != 0
              AND COALESCE(s.Northing, 0) != 0
            ORDER BY s.Seq, s.Point
        """

        try:
            with self.get_conn() as conn:
                preplot_df = pd.read_sql_query(sql_preplot, conn, params=(line,))
                solution_df = pd.read_sql_query(sql_solution, conn, params=(line,))

            if preplot_df.empty and solution_df.empty:
                raise ValueError(f"No SPPreplot / SPSolution data found for Line {line}")

            if not preplot_df.empty:
                preplot_df = preplot_df.copy()
                preplot_df["PointCode"] = preplot_df["PointCode"].fillna("").astype(str)
                preplot_df["label"] = preplot_df["Point"].astype(str)
                preplot_df["size"] = point_size

            if not solution_df.empty:
                solution_df = solution_df.copy()
                solution_df["Seq"] = pd.to_numeric(solution_df["Seq"], errors="coerce").fillna(0).astype(int)
                solution_df["PointCode"] = solution_df["PointCode"].fillna("").astype(str)
                solution_df["FireCode"] = solution_df["FireCode"].fillna("").astype(str)
                solution_df["ArrayCode"] = pd.to_numeric(solution_df["ArrayCode"], errors="coerce")
                solution_df["SeqPurpose"] = solution_df["SeqPurpose"].fillna("Unknown").astype(str)
                solution_df["label"] = solution_df["Point"].astype(str)
                solution_df["size"] = point_size

            xs, ys = [], []

            if not preplot_df.empty:
                xs.extend(preplot_df["x"].tolist())
                ys.extend(preplot_df["y"].tolist())

            if not solution_df.empty:
                xs.extend(solution_df["x"].tolist())
                ys.extend(solution_df["y"].tolist())

            if not xs or not ys:
                raise ValueError(f"No valid coordinates found for Line {line}")

            min_x, max_x = min(xs), max(xs)
            min_y, max_y = min(ys), max(ys)
            dx = max(max_x - min_x, 1.0)
            dy = max(max_y - min_y, 1.0)
            padx = dx * 0.05
            pady = dy * 0.05

            p = figure(
                title=f"SPSolution vs SPPreplot - Line {line}",
                x_range=(min_x - padx, max_x + padx),
                y_range=(min_y - pady, max_y + pady),
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
                toolbar_location="above",
                sizing_mode="stretch_both",
                match_aspect=True,
            )

            p.xaxis.axis_label = "X / Easting"
            p.yaxis.axis_label = "Y / Northing"

            all_sources = []
            all_legends = []
            seq_renderers = {}
            seq_legends = {}
            seq_labels = []
            label_sources = []

            def _red_gradient(v, vmin_, vmax_):
                if pd.isna(v):
                    return "#ff8080"

                if vmax_ <= vmin_:
                    t = 0.5
                else:
                    t = (float(v) - vmin_) / (vmax_ - vmin_)

                r = 255
                g = int(210 - 140 * t)
                b = int(210 - 140 * t)

                g = max(0, min(255, g))
                b = max(0, min(255, b))

                return f"#{r:02x}{g:02x}{b:02x}"

            # -------------------------------------------------
            # PREPLOT
            # -------------------------------------------------
            if not preplot_df.empty:
                src_pre = ColumnDataSource(preplot_df)
                all_sources.append(src_pre)

                r_pre_line = p.line(
                    x="x",
                    y="y",
                    source=src_pre,
                    line_width=2,
                    line_dash=preplot_dash,
                    line_alpha=0.95,
                    color="gray",
                )

                r_pre_pts = p.scatter(
                    x="x",
                    y="y",
                    source=src_pre,
                    size="size",
                    marker="circle",
                    alpha=0.75,
                    color="gray",
                )

                pre_lbl_df = preplot_df.copy()
                pre_lbl_df["idx"] = list(range(len(pre_lbl_df)))
                pre_lbl_df["text"] = [
                    lbl if (i % label_every == 0) else ""
                    for i, lbl in enumerate(pre_lbl_df["label"].tolist())
                ]
                src_pre_lbl = ColumnDataSource(pre_lbl_df)
                label_sources.append(src_pre_lbl)

                r_pre_txt = p.text(
                    x="x",
                    y="y",
                    text="text",
                    source=src_pre_lbl,
                    text_font_size="8pt",
                    text_alpha=0.75,
                    text_color="gray",
                    x_offset=5,
                    y_offset=5,
                )

                p.add_tools(HoverTool(
                    renderers=[r_pre_pts],
                    tooltips=[
                        ("Type", "SPPreplot"),
                        ("Line", "@Line"),
                        ("Point", "@Point"),
                        ("PointCode", "@PointCode"),
                        ("X", "@x{0.00}"),
                        ("Y", "@y{0.00}"),
                        ("Z", "@z{0.00}"),
                    ]
                ))

                lg_pre = Legend(
                    title="Preplot",
                    items=[
                        LegendItem(label="SPPreplot", renderers=[r_pre_line, r_pre_pts]),
                        LegendItem(label="Labels", renderers=[r_pre_txt]),
                    ],
                    click_policy="hide",
                    label_text_font_size="9pt",
                    title_text_font_size="10pt",
                    spacing=4,
                    glyph_height=16,
                    glyph_width=22,
                )
                p.add_layout(lg_pre, "right")
                all_legends.append(lg_pre)

            # -------------------------------------------------
            # SPSOLUTION - one legend per Seq
            # -------------------------------------------------
            seq_color_pool = [
                "#1f77b4", "#d62728", "#2ca02c", "#ff7f0e",
                "#9467bd", "#8c564b", "#e377c2", "#17becf",
                "#bcbd22", "#7f7f7f", "#393b79", "#637939",
                "#8c6d31", "#843c39", "#7b4173",
            ]

            pointcode_color_pool = [
                "#1f77b4", "#2ca02c", "#ff7f0e", "#9467bd",
                "#8c564b", "#e377c2", "#17becf", "#bcbd22",
                "#7f7f7f", "#393b79", "#637939", "#8c6d31",
                "#843c39", "#7b4173",
            ]

            if not solution_df.empty:
                seq_values = sorted(solution_df["Seq"].dropna().unique().tolist())

                for seq_index, seq in enumerate(seq_values):
                    seq_df = solution_df[solution_df["Seq"] == seq].copy()
                    if seq_df.empty:
                        continue

                    purpose = seq_df["SeqPurpose"].iloc[0] if "SeqPurpose" in seq_df.columns else "Unknown"
                    seq_label = f"Seq {seq} ({purpose})"

                    seq_labels.append(seq_label)
                    seq_renderers[str(seq)] = []

                    track_color = seq_color_pool[seq_index % len(seq_color_pool)]

                    src_seq_line = ColumnDataSource(seq_df)
                    all_sources.append(src_seq_line)

                    r_seq_line = p.line(
                        x="x",
                        y="y",
                        source=src_seq_line,
                        line_width=2,
                        line_alpha=0.9,
                        color=track_color,
                    )
                    seq_renderers[str(seq)].append(r_seq_line)

                    seq_legend_items = [
                        LegendItem(label="Track", renderers=[r_seq_line])
                    ]

                    pointcodes = sorted(seq_df["PointCode"].astype(str).unique().tolist())
                    if not pointcodes:
                        pointcodes = [""]

                    for pc_index, pc in enumerate(pointcodes):
                        part = seq_df[seq_df["PointCode"].astype(str) == str(pc)].copy()
                        if part.empty:
                            continue

                        base_color = pointcode_color_pool[pc_index % len(pointcode_color_pool)]
                        part["color"] = base_color

                        kill_mask = part["FireCode"].astype(str).str.upper() == "K"
                        if kill_mask.any():
                            kill_vals = pd.to_numeric(part.loc[kill_mask, "ArrayCode"], errors="coerce")

                            if kill_vals.notna().any():
                                vmin = float(kill_vals.min())
                                vmax = float(kill_vals.max())
                                part.loc[kill_mask, "color"] = [
                                    _red_gradient(v, vmin, vmax) for v in kill_vals
                                ]
                            else:
                                part.loc[kill_mask, "color"] = "#cc0000"

                        src_pc = ColumnDataSource(part)
                        all_sources.append(src_pc)

                        r_pc = p.scatter(
                            x="x",
                            y="y",
                            source=src_pc,
                            size="size",
                            marker="circle",
                            alpha=0.9,
                            color="color",
                        )
                        seq_renderers[str(seq)].append(r_pc)

                        p.add_tools(HoverTool(
                            renderers=[r_pc],
                            tooltips=[
                                ("Type", "SPSolution"),
                                ("Line", "@Line"),
                                ("Seq", "@Seq"),
                                ("Purpose", "@SeqPurpose"),
                                ("Point", "@Point"),
                                ("FireCode", "@FireCode"),
                                ("PointCode", "@PointCode"),
                                ("ArrayCode", "@ArrayCode"),
                                ("X", "@x{0.00}"),
                                ("Y", "@y{0.00}"),
                                ("Z", "@z{0.00}"),
                            ]
                        ))

                        pc_label = pc if str(pc).strip() else "(blank)"
                        seq_legend_items.append(
                            LegendItem(label=pc_label, renderers=[r_pc])
                        )

                    seq_lbl_df = seq_df.copy()
                    seq_lbl_df["idx"] = list(range(len(seq_lbl_df)))
                    seq_lbl_df["text"] = [
                        lbl if (i % label_every == 0) else ""
                        for i, lbl in enumerate(seq_lbl_df["label"].tolist())
                    ]
                    src_seq_lbl = ColumnDataSource(seq_lbl_df)
                    label_sources.append(src_seq_lbl)

                    r_seq_lbl = p.text(
                        x="x",
                        y="y",
                        text="text",
                        source=src_seq_lbl,
                        text_font_size="8pt",
                        text_alpha=0.75,
                        text_color="black",
                        x_offset=5,
                        y_offset=5,
                    )
                    seq_renderers[str(seq)].append(r_seq_lbl)

                    seq_legend_items.append(
                        LegendItem(label="Labels", renderers=[r_seq_lbl])
                    )

                    lg_seq = Legend(
                        title=f"Seq {seq}",
                        items=seq_legend_items,
                        click_policy="hide",
                        label_text_font_size="9pt",
                        title_text_font_size="10pt",
                        spacing=4,
                        glyph_height=16,
                        glyph_width=22,
                    )
                    p.add_layout(lg_seq, "right")
                    all_legends.append(lg_seq)
                    seq_legends[str(seq)] = lg_seq

            # -------------------------------------------------
            # Controls
            # -------------------------------------------------
            btn_legend = Button(label="Hide / Show Legends", width=170)
            btn_legend.js_on_click(CustomJS(args=dict(legs=all_legends), code="""
                if (!legs || legs.length === 0) return;
                const make_visible = !legs[0].visible;
                for (let i = 0; i < legs.length; i++) {
                    legs[i].visible = make_visible;
                }
            """))

            size_slider = Slider(
                start=2,
                end=20,
                value=point_size,
                step=1,
                title="Point Size",
                width=220,
            )

            size_slider.js_on_change("value", CustomJS(args=dict(srcs=all_sources), code="""
                const v = cb_obj.value;
                for (let i = 0; i < srcs.length; i++) {
                    const s = srcs[i];
                    if (!("size" in s.data)) continue;
                    const n = s.data["size"].length;
                    s.data["size"] = Array(n).fill(v);
                    s.change.emit();
                }
            """))

            label_input = TextInput(
                title="Label every (1-20)",
                value=str(label_every),
                width=130,
            )

            label_input.js_on_change("value", CustomJS(args=dict(srcs=label_sources), code="""
                let step = parseInt(cb_obj.value);

                if (isNaN(step) || step < 1) step = 1;
                if (step > 20) step = 20;

                cb_obj.value = String(step);

                for (let k = 0; k < srcs.length; k++) {
                    const src = srcs[k];
                    const data = src.data;
                    const idx = data["idx"];
                    const labels = data["label"];
                    const out = [];

                    for (let i = 0; i < idx.length; i++) {
                        out.push((i % step === 0) ? labels[i] : "");
                    }

                    data["text"] = out;
                    src.change.emit();
                }
            """))

            controls = [btn_legend, size_slider, label_input]

            if seq_labels:
                seq_checkbox = CheckboxGroup(
                    labels=seq_labels,
                    active=list(range(len(seq_labels))),
                    inline=True,
                )

                seq_checkbox.js_on_change("active", CustomJS(
                    args=dict(
                        seq_labels=seq_labels,
                        seq_renderers=seq_renderers,
                        seq_legends=seq_legends,
                    ),
                    code="""
                        const activeSet = new Set(cb_obj.active);

                        for (let i = 0; i < seq_labels.length; i++) {
                            const label = seq_labels[i];
                            const parts = label.split(" ");
                            const seq = parts.length > 1 ? parts[1] : "";
                            const visible = activeSet.has(i);

                            const rs = seq_renderers[seq] || [];
                            for (let j = 0; j < rs.length; j++) {
                                rs[j].visible = visible;
                            }

                            if (seq_legends[seq]) {
                                seq_legends[seq].visible = visible;
                            }
                        }
                    """
                ))

                controls.append(Div(text="<b>Show / hide Seq:</b>"))
                controls.append(seq_checkbox)

            top_controls = row(*controls, sizing_mode="stretch_width")
            layout = column(top_controls, p, sizing_mode="stretch_both")

            if json_return:
                return json_item(layout, f"sp-solution-vs-preplot-{line}")

            if is_show:
                show(layout)

            return layout

        except Exception as e:
            raise RuntimeError(f"Failed to build SPSolution vs SPPreplot plot for Line {line}: {e}") from e
