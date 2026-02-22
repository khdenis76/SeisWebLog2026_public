from __future__ import annotations

from datetime import datetime
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple, Union

import sqlite3
import pandas as pd
import numpy as np
from bokeh.core.property.vectorization import value
from bokeh.embed import json_item
from bokeh.io import show
from bokeh.layouts import row, column, gridplot
from bokeh.models import Span,Range1d
from bokeh.palettes import Category10, Category20

from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, Span, Range1d,Label, HoverTool, Button, Spinner, CustomJS, LabelSet, DatetimeTickFormatter, Div, \
    DatetimeTicker
from bokeh.models import WMTSTileSource
import geopandas as gpd
from bokeh.transform import factor_cmap, cumsum
import plotly.graph_objects as go
from pyproj import Transformer
import xyzservices.providers as xyz

PathLike = Union[str, Path]


@dataclass
class DSRMapConfig:
    # If your coordinates are already WebMercator (EPSG:3857), keep True and add tiles.
    # If you store UTM Easting/Northing and just want a plain map without tiles, set False.
    use_tiles: bool = False
    use_shapes: bool = False # Add shapes to map or not
    use_csv: bool = False # SHow CV layer on the map
    tile_vendor: str = "CARTODBPOSITRON"  # one of Vendors.*
    width: int = 1100
    height: int = 700
    match_aspect: bool = True
    default_epsg: Optional[int] = 4326 #WGS84

class DSRMapPlots:
    """
    Read RPPreplot + DSR from SQLite and plot them in Bokeh on a single map.

    Expected columns (adjust SQL if yours differ):
      RPPreplot: Line, Station (or LinePoint), Node, PreplotEasting, PreplotNorthing
      DSR: Line, Station, Node, PrimaryEasting, PrimaryNorthing, SecondaryEasting, SecondaryNorthing, Status, ROV, TimeStamp
    """

    def __init__(
            self,
            db_path: PathLike,
            config: Optional[DSRMapConfig] = None,
            **config_overrides
    ):
        self.db_path = str(db_path)

        # Create default config if not provided
        self.cfg = config or DSRMapConfig()

        # Apply overrides like default_epsg=32615
        for key, value in config_overrides.items():
            if hasattr(self.cfg, key):
                setattr(self.cfg, key, value)
            else:
                raise ValueError(f"Invalid config parameter: {key}")

    # -------------------------
    # DB helpers
    # -------------------------
    @staticmethod
    def add_inline_xline_offsets(
            dsr_df: pd.DataFrame,
            rp_preplot_df: pd.DataFrame,
            *,
            from_xy=("PreplotEasting", "PreplotNorthing"),
            to_xy=("PrimaryEasting", "PrimaryNorthing"),
            bearing_col="LineBearing",
            out_prefix="Pri",
    ) -> pd.DataFrame:
        """
        Add Inline/Xline offset columns into dsr_df using a common line bearing
        taken from first row of rp_preplot_df[bearing_col].

        Offsets are computed as:
          dx = to_x - from_x
          dy = to_y - from_y

        Bearing is assumed AZIMUTH clockwise from North:
          0 = North, 90 = East

        Inline axis points along the bearing direction.
        Xline axis is +90° to the right of inline.

        Output columns:
          {out_prefix}OffE, {out_prefix}OffN, {out_prefix}OffInline, {out_prefix}OffXline
        """

        if dsr_df is None or dsr_df.empty:
            return dsr_df

        if rp_preplot_df is None or rp_preplot_df.empty:
            raise ValueError("rp_preplot_df is empty; cannot read LineBearing.")

        if bearing_col not in rp_preplot_df.columns:
            raise ValueError(f"'{bearing_col}' not found in rp_preplot_df columns.")

        # Take common bearing from first non-null value (safer than iloc[0])
        bearing_series = pd.to_numeric(rp_preplot_df[bearing_col], errors="coerce").dropna()
        if bearing_series.empty:
            raise ValueError(f"'{bearing_col}' has no numeric values in rp_preplot_df.")
        bearing_deg = float(bearing_series.iloc[0])

        # Ensure required columns exist in dsr_df
        fx, fy = from_xy
        tx, ty = to_xy
        for c in (fx, fy, tx, ty):
            if c not in dsr_df.columns:
                raise ValueError(f"'{c}' missing in dsr_df; cannot compute offsets.")

        # numeric arrays (NaN-safe)
        from_x = pd.to_numeric(dsr_df[fx], errors="coerce").to_numpy(dtype="float64")
        from_y = pd.to_numeric(dsr_df[fy], errors="coerce").to_numpy(dtype="float64")
        to_x = pd.to_numeric(dsr_df[tx], errors="coerce").to_numpy(dtype="float64")
        to_y = pd.to_numeric(dsr_df[ty], errors="coerce").to_numpy(dtype="float64")

        dx = to_x - from_x
        dy = to_y - from_y

        # Convert bearing (azimuth from North) to unit vectors
        # inline unit vector (east, north) = (sinθ, cosθ)
        th = np.deg2rad(bearing_deg)
        uix, uiy = np.sin(th), np.cos(th)

        # xline unit vector = rotate inline +90° (to the right)
        # (east, north) = (cosθ, -sinθ)
        ux, uy = np.cos(th), -np.sin(th)

        inline_off = dx * uix + dy * uiy
        xline_off = dx * ux + dy * uy

        # Write outputs
        dsr_df[f"{out_prefix}OffE"] = dx
        dsr_df[f"{out_prefix}OffN"] = dy
        dsr_df[f"{out_prefix}OffInline"] = inline_off
        dsr_df[f"{out_prefix}OffXline"] = xline_off

        # Optional: also total offset distance
        dsr_df[f"{out_prefix}OffDist"] = np.sqrt(dx * dx + dy * dy)

        return dsr_df
    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _ensure_list(values: Optional[Iterable]) -> Optional[list]:
        if values is None:
            return None
        vals = list(values)
        return vals if vals else None

    @staticmethod
    def _sql_in_clause(values: Sequence, param_prefix: str = "v") -> Tuple[str, dict]:
        """
        Returns: ("(:v0,:v1,...)", {"v0":..., "v1":...})
        """
        params = {f"{param_prefix}{i}": v for i, v in enumerate(values)}
        placeholders = ",".join([f":{k}" for k in params.keys()])
        return f"({placeholders})", params
    #---------------------------------------------
    #In case of any error  blank plot will be generated
    #-------------------------------------------------------
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
    # -------------------------
    # Readers
    # -------------------------
    def read_rp_preplot(
        self,
        lines: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Read RPPreplot into a DataFrame.
        Adjust table/column names here if needed.
        """
        lines_list = self._ensure_list(lines)

        sql = """
                  SELECT
                      Line,
                      -- choose ONE naming convention below (keep both if you want)
                      Point,
                      LinePoint,
                      File_FK,
                      X,Y,LineBearing  
                  FROM RPPreplot
                  WHERE 1=1
        """
        params: dict = {}

        if lines_list is not None:
            in_clause, p = self._sql_in_clause(lines_list, "ln")
            sql += f" AND Line IN {in_clause}"
            params.update(p)

        if limit is not None:
            sql += " LIMIT :lim"
            params["lim"] = int(limit)

        with self._connect() as con:
            df = pd.read_sql_query(sql, con, params=params)

        # Normalize types a bit
        for c in ("Line", "Point"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        return df
    def read_recdb(
        self,
        lines: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Read REC_DB into a DataFrame.
        Adjust table/column names here if needed.
        """
        lines_list = self._ensure_list(lines)

        sql = """SELECT * FROM REC_DB WHERE 1=1"""
        params: dict = {}

        if lines_list is not None:
            in_clause, p = self._sql_in_clause(lines_list, "ln")
            sql += f" AND Line IN {in_clause}"
            params.update(p)

        if limit is not None:
            sql += " LIMIT :lim"
            params["lim"] = int(limit)

        with self._connect() as con:
            df = pd.read_sql_query(sql, con, params=params)

        # Normalize types a bit
        for c in ("Line", "Point"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        return df
    def select_all_except(
            self,
            table_name: str,
            exclude: list[str] | None = None,
            where: str | None = None,
            params: dict | None = None,
            order_by: str | None = None,
            limit: int | None = None,
    ):
        """
        Select all columns from table except those listed in `exclude`.
        Example:
            df = self.select_all_except(
                "DSR",
                exclude=["PrimaryEasting", "PrimaryNorthing"],
                where="Line = :line",
                params={"line": 101},
                order_by="Station",
                limit=1000
            )
        """
        exclude = exclude or []
        params = params or {}

        with self._connect() as con:
            cur = con.execute(f"PRAGMA table_info({table_name})")
            cols = [row[1] for row in cur.fetchall()]

        # Remove excluded columns
        selected_cols = [c for c in cols if c not in exclude]

        if not selected_cols:
            raise ValueError("No columns left after exclusion.")

        col_string = ", ".join(selected_cols)

        sql = f"SELECT {col_string} FROM {table_name}"

        if where:
            sql += f" WHERE {where}"

        if order_by:
            sql += f" ORDER BY {order_by}"

        if limit:
            sql += f" LIMIT {int(limit)}"

        with self._connect() as con:
            df = pd.read_sql_query(sql, con, params=params)

        return df
    def read_dsr(
        self,
        lines: Optional[Iterable[int]] = None,
        solution_fk: Optional[int] = 1,
        only_processed: bool = False,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Read DSR into a DataFrame.
        Adjust table/column names here if needed.
        """
        lines_list = self._ensure_list(lines)

        sql = """SELECT * FROM DSR WHERE 1=1"""
        params: dict = {}

        if solution_fk is not None:
            sql += " AND Solution_FK = :solution_fk"
            params["solution_fk"] = int(solution_fk)

        if lines_list is not None:
            in_clause, p = self._sql_in_clause(lines_list, "ln")
            sql += f" AND Line IN {in_clause}"
            params.update(p)

        if only_processed:
            # Your “processed” logic often means REC_ID not empty.
            # If you prefer that, uncomment and adjust column name:
            # sql += " AND REC_ID IS NOT NULL AND TRIM(REC_ID) <> ''"
            pass

        if limit is not None:
            sql += " LIMIT :lim"
            params["lim"] = int(limit)

        with self._connect() as con:
            df = pd.read_sql_query(sql, con, params=params)

        for c in ("Line", "Station"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

        return df

    # -------------------------
    # Plotting
    # -------------------------
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
        with self._connect() as con:
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
        with self._connect() as conn:
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

                labels = LabelSet(
                    x="x", y="y", text="Point",
                    source=label_src,
                    x_offset=6, y_offset=6,
                    text_font_size="9pt",
                    text_alpha=0.9,
                )
                p.add_layout(labels)

        return p
    def make_map(
            self,
            rp_df: Optional[pd.DataFrame] = None,
            dsr_df: Optional[pd.DataFrame] = None,
            title: str = "RPPreplot + DSR Map",
            show_secondary: bool = False,
            show_primary: bool = True,
            show_preplot: bool = True,
            show_shapes:bool =True,
            show_layers=True,
            is_show: bool = False,
            jason_item: bool = False,

    ):
        """
        Returns a Bokeh layout (controls + plot) with:
          - Legend toggle button (if legend exists)
          - Legend corner cycle button (if legend exists)
          - RP size spinner (1..100)
          - DSR size spinner (1..100) affects primary + secondary (if present)
        """
        if self.cfg.default_epsg:
            transformer = Transformer.from_crs(
                f"EPSG:{self.cfg.default_epsg}", "EPSG:3857", always_xy=True
            )
            rp_df["x0"], rp_df["y0"] = transformer.transform(rp_df["X"].values, rp_df["Y"].values)
            dsr_df["x0"], dsr_df["y0"] = transformer.transform(dsr_df["PrimaryEasting"].values, dsr_df["PrimaryNorthing"].values)
        p = figure(
            title=title,
            sizing_mode="stretch_both",
            x_axis_type="mercator" if self.cfg.use_tiles else "linear",
            y_axis_type="mercator" if self.cfg.use_tiles else "linear",
            match_aspect=self.cfg.match_aspect,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
        )

        # Tiles (Bokeh 3.x)
        if self.cfg.use_tiles:
            p.add_tile(xyz.CartoDB.Positron)
        # Keep handles to renderers so JS can change glyph sizes
        r_rp = None
        r_d1 = None
        r_d2 = None
        if show_shapes:
            self.add_project_shapes_layers(p, default_src_epsg=self.cfg.default_epsg)
        if show_layers:
            self.add_csv_layers_to_map(
                p,
                csv_epsg=default_epsg,
                show_tiles=show_tiles,
                max_labels=max_csv_labels,
            )
        # --- RPPreplot layer
        if show_preplot and rp_df is not None and len(rp_df) > 0:
            rp = rp_df.copy()
            rp = rp.dropna(subset=["x0", "y0"])
            src_rp = ColumnDataSource(rp)

            r_rp = p.circle(
                x="x0",
                y="y0",
                size=5,
                alpha=0.8,
                legend_label=f"Receiver Preplot. {len(rp)} sta.",
                source=src_rp,
                color='grey', fill_color='grey'
            )
            p.add_tools(
                HoverTool(
                    renderers=[r_rp],
                    tooltips=[
                        ("Layer", "Preplot"),
                        ("Line", "@Line"),
                        ("Station", "@Point"),
                        ("E", "@PreplotEasting{0,0.00}"),
                        ("N", "@PreplotNorthing{0,0.00}"),
                    ],
                )
            )
        # ---Plot project shapes

        # --- DSR Primary layer
        if show_primary and dsr_df is not None and len(dsr_df) > 0:
            d1 = dsr_df.copy()
            d1 = d1.dropna(subset=["x0", "y0"])
            src_d1 = ColumnDataSource(d1)

            r_d1 = p.circle(
                x="x0",
                y="y0",
                size=6,
                alpha=0.9,
                legend_label="DSR Primary",
                source=src_d1,
            )
            p.add_tools(
                HoverTool(
                    renderers=[r_d1],
                    tooltips=[
                        ("Layer", "DSR Primary"),
                        ("Line", "@Line"),
                        ("Station", "@Station"),
                        ("Node", "@Node"),
                        ("Status", "@Status"),
                        ("ROV", "@ROV"),
                        ("TS", "@TimeStamp"),
                        ("E", "@PrimaryEasting{0,0.00}"),
                        ("N", "@PrimaryNorthing{0,0.00}"),
                    ],
                )
            )

        # --- DSR Secondary layer
        if show_secondary and dsr_df is not None and len(dsr_df) > 0:
            d2 = dsr_df.copy()
            d2 = d2.dropna(subset=["SecondaryEasting", "SecondaryNorthing"])
            src_d2 = ColumnDataSource(d2)

            r_d2 = p.triangle(
                x="SecondaryEasting",
                y="SecondaryNorthing",
                size=7,
                alpha=0.85,
                legend_label="DSR Secondary",
                source=src_d2,
            )
            p.add_tools(
                HoverTool(
                    renderers=[r_d2],
                    tooltips=[
                        ("Layer", "DSR Secondary"),
                        ("Line", "@Line"),
                        ("Station", "@Station"),
                        ("Node", "@Node"),
                        ("Status", "@Status"),
                        ("ROV", "@ROV"),
                        ("TS", "@TimeStamp"),
                        ("E", "@SecondaryEasting{0,0.00}"),
                        ("N", "@SecondaryNorthing{0,0.00}"),
                    ],
                )
            )

        # Legend defaults (legend exists only if at least one glyph used legend_label)
        if p.legend and len(p.legend) > 0:
            p.legend.click_policy = "hide"
            p.legend.location = "top_left"
            p.legend.visible = True

        # -------------------------
        # Controls
        # -------------------------
        sp_rp = Spinner(title="RP size", low=1, high=100, step=1, value=5, width=130)
        sp_dsr = Spinner(title="DSR size", low=1, high=100, step=1, value=6, width=130)

        # RP size spinner
        if r_rp is not None:
            sp_rp.js_on_change(
                "value",
                CustomJS(
                    args=dict(r=r_rp),
                    code="r.glyph.size = cb_obj.value;",
                ),
            )
        else:
            sp_rp.disabled = True

        # DSR size spinner (primary + secondary)
        dsr_renderers = [r for r in (r_d1, r_d2) if r is not None]
        if dsr_renderers:
            sp_dsr.js_on_change(
                "value",
                CustomJS(
                    args=dict(renderers=dsr_renderers),
                    code="""
                        for (const r of renderers) {
                            r.glyph.size = cb_obj.value;
                        }
                    """,
                ),
            )
        else:
            sp_dsr.disabled = True

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

        # Always include spinners
        controls_items.extend([sp_rp, sp_dsr])

        controls = row(*controls_items, sizing_mode="stretch_width")
        layout = column(controls, p, sizing_mode="stretch_both")

        if is_show:
            show(layout)
            return None

        if jason_item:
            return None

        return layout

    def make_map_multi_layers(
            self,
            rp_df: Optional[pd.DataFrame] = None,
            dsr_df: Optional[pd.DataFrame] = None,
            rec_db_df: Optional[pd.DataFrame] = None,
            title: str = "Map",
            layers: Optional[list[dict]] = None,
            show_preplot: bool = True,
            show_shapes: bool = True,
            show_layers: bool = True,
            show_tiles: Optional[bool] = None,
            is_show: bool = False,
            jason_item: bool = False,  # kept your flag name
    ):
        """
        Multi-layer map (dsr / rec_db) with:
          - per-layer pandas query filter: where="ROV.isna() or ROV == ''" (<> supported)
          - per-layer coloring:
              * categorical: color_col="ROV"
              * numeric grouped: color_col="TodayDaysInWater" + bins=8 (or intervals=[[0,10],...])
          - integer interval labels when bins are used (0–10, 11–20, ...)

        Layer dict keys (important ones):
          {
            "df": "dsr" | "rec" | "rec_db",
            "name": "...",
            "x_col": "...",
            "y_col": "...",
            "marker": "circle",
            "size": 6,
            "alpha": 0.9,

            "where": "ROV.isna() or ROV == ''",

            "color_col": "TodayDaysInWater",    # categorical or numeric
            "color": "orange",                  # fixed color if no color_col

            # numeric grouping (choose one mode):
            "bins": 8,                          # >=2 => auto bins
            "bin_method": "equal"|"quantile",   # default "equal"
            "include_lowest": True,

            # OR explicit intervals (overrides bins):
            "intervals": [[0,10],[11,20],[21,30],[31,60],[61,9999]],
            "interval_labels": ["0–10","11–20","21–30","31–60","61+"],  # optional

            # palette:
            "palette": "Turbo256"|"Category10"|"Category20"|<list-of-colors>,
            "palette_colors": [ ... ],          # explicit list overrides palette
          }
        """

        import numpy as np  # needed for integer bin labels

        # ---- defaults
        if layers is None:
            layers = [
                dict(
                    df="dsr",
                    name="DSR Primary",
                    x_col="PrimaryEasting",
                    y_col="PrimaryNorthing",
                    marker="circle",
                    size=6,
                    alpha=0.9,
                    color="orange",
                )
            ]

        # ---- transformer
        transformer = None
        if getattr(self.cfg, "default_epsg", None):
            transformer = Transformer.from_crs(
                f"EPSG:{self.cfg.default_epsg}", "EPSG:3857", always_xy=True
            )

        # ---- show tiles
        if show_tiles is None:
            show_tiles = bool(getattr(self.cfg, "use_tiles", False))

        # ---- figure
        p = figure(
            title=title,
            sizing_mode="stretch_both",
            x_axis_type="mercator" if show_tiles else "linear",
            y_axis_type="mercator" if show_tiles else "linear",
            match_aspect=self.cfg.match_aspect,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
        )

        # ---- tiles (xyzservices)
        if show_tiles:
            vendor = getattr(self.cfg, "tile_vendor", "CARTODB_POSITRON")
            provider = {
                "CARTODB_POSITRON": xyz.CartoDB.Positron,
                "CARTODB_DARK": xyz.CartoDB.DarkMatter,
                "OSM": xyz.OpenStreetMap.Mapnik,
                "ESRI_IMAGERY": xyz.Esri.WorldImagery,
            }.get(vendor, xyz.CartoDB.Positron)
            p.add_tile(provider)

        # ---- shapes overlay
        if show_shapes:
            self.add_project_shapes_layers(
                p, default_src_epsg=getattr(self.cfg, "default_epsg", None)
            )
        if show_layers:
            self.add_csv_layers_to_map(
                p,
                csv_epsg=self.cfg.default_epsg,
                show_tiles=show_tiles,
            )

        # ---- RPPreplot layer (scatter)
        r_rp = None
        if show_preplot and rp_df is not None and len(rp_df) > 0:
            rp = rp_df.copy().dropna(subset=["X", "Y"])

            if transformer is not None:
                mx, my = transformer.transform(rp["X"].values, rp["Y"].values)
                rp["__mx"] = mx
                rp["__my"] = my
            else:
                rp["__mx"] = rp["X"]
                rp["__my"] = rp["Y"]

            src_rp = ColumnDataSource(rp)

            r_rp = p.scatter(
                x="__mx",
                y="__my",
                marker="circle",
                size=5,
                alpha=0.8,
                legend_label=f"Receiver Preplot. {len(rp)} sta.",
                source=src_rp,
                line_color="grey",
                fill_color="grey",
            )

            p.add_tools(
                HoverTool(
                    renderers=[r_rp],
                    tooltips=[
                        ("Layer", "Preplot"),
                        ("Line", "@Line"),
                        ("Station", "@Point"),
                        ("E", "@PreplotEasting{0,0.00}"),
                        ("N", "@PreplotNorthing{0,0.00}"),
                    ],
                )
            )

        # ---- DataFrame selector
        df_map = {
            "dsr": dsr_df,
            "rec": rec_db_df,
            "rec_db": rec_db_df,
            "recdb": rec_db_df,
        }

        layer_spinners = []
        used_legend_titles = []

        # ---- Helper: palette selection per layer (supports "Turbo256", list, Category10/20)
        def _pick_palette(n: int, layer: dict):
            from bokeh.palettes import Category10, Category20, Turbo256
            import numpy as np

            palette_colors = layer.get("palette_colors", None)
            palette_raw = layer.get("palette", None)

            # 1) explicit list overrides
            if isinstance(palette_colors, (list, tuple)) and len(palette_colors) > 0:
                pal = list(palette_colors)
                return (pal * ((n // len(pal)) + 1))[:n]

            # 2) palette passed as list
            if isinstance(palette_raw, (list, tuple)) and len(palette_raw) > 0:
                pal = list(palette_raw)
                return (pal * ((n // len(pal)) + 1))[:n]

            # 3) palette passed as string
            if isinstance(palette_raw, str):
                name = palette_raw.strip().upper()

                if name == "TURBO256":
                    if n <= 1:
                        return [Turbo256[0]]
                    # evenly spaced indices across 0..255
                    idx = np.linspace(0, 255, n).round().astype(int)
                    return [Turbo256[i] for i in idx]

                if name == "CATEGORY10":
                    return Category10[10][:min(n, 10)]
                if name == "CATEGORY20":
                    return Category20[20][:min(n, 20)]

            # fallback
            if n <= 10:
                return Category10[10][:n]
            if n <= 20:
                return Category20[20][:n]
            return (Category20[20] * ((n // 20) + 1))[:n]

        # ---- Helper: grouped labels for numeric column (bins -> integer labels, non-overlapping)
        def _build_grouped_numeric_column(df: pd.DataFrame, col: str, layer: dict, suffix: str) -> str:
            """
            Returns column name to use for categorical coloring/legend.
            Creates df[newcol] with labels if numeric grouping enabled (bins/intervals).
            If not grouping, converts df[col] to str and returns col.
            """
            s = df[col]
            s_num = pd.to_numeric(s, errors="coerce")
            is_numeric = s_num.notna().any()

            intervals = layer.get("intervals", None)
            interval_labels = layer.get("interval_labels", None)

            bins_n = int(layer.get("bins", 0) or 0)
            bin_method = (layer.get("bin_method") or "equal").lower()
            include_lowest = bool(layer.get("include_lowest", True))

            wants_intervals = isinstance(intervals, (list, tuple)) and len(intervals) >= 1
            wants_bins = bins_n >= 2

            if not is_numeric or not (wants_intervals or wants_bins):
                df[col] = df[col].astype(str)
                return col

            newcol = f"{col}__grp_{suffix}"
            df[newcol] = "Unknown"

            # ---- Mode B: explicit intervals overrides bins
            if wants_intervals:
                labs = interval_labels
                if not (isinstance(labs, (list, tuple)) and len(labs) == len(intervals)):
                    labs = []
                    for a, b in intervals:
                        if b is None:
                            b = float("inf")
                        if b == float("inf"):
                            labs.append(f"{int(a)}+")
                        else:
                            labs.append(f"{int(a)}–{int(b)}")

                for (a, b), lab in zip(intervals, labs):
                    if b is None:
                        b = float("inf")
                    mask = s_num.ge(a) & s_num.le(b)
                    df.loc[mask, newcol] = str(lab)

                df.loc[s_num.isna(), newcol] = "Unknown"
                return newcol

            # ---- Mode A: auto bins (equal / quantile)
            if bin_method in ("quantile", "q", "Q", "qcut"):
                try:
                    cats = pd.qcut(s_num, q=bins_n, duplicates="drop")
                except ValueError:
                    cats = pd.cut(s_num, bins=bins_n, include_lowest=include_lowest)
            else:
                cats = pd.cut(s_num, bins=bins_n, include_lowest=include_lowest)

            # ---- Integer, non-overlapping labels:
            # Example: 0–10, 11–20, 21–30 ...
            labels = []
            prev_right = None
            for idx, interval in enumerate(cats.cat.categories):
                il = int(np.floor(interval.left))
                ir = int(np.ceil(interval.right))

                if idx == 0 and include_lowest:
                    # keep il as is
                    pass
                elif prev_right is not None:
                    il = int(prev_right) + 1

                if ir < il:
                    ir = il

                labels.append(f"{il}–{ir}")
                prev_right = ir

            df[newcol] = cats.cat.rename_categories(labels).astype(str)
            df.loc[s_num.isna(), newcol] = "Unknown"
            return newcol

        # ---- Build each layer (scatter everywhere)
        for i, layer in enumerate(layers, start=1):
            layer_name = layer.get("name", f"Layer {i}")
            df_key = (layer.get("df") or "dsr").lower()

            base_df = df_map.get(df_key)
            if base_df is None or len(base_df) == 0:
                continue

            x_col = layer.get("x_col")
            y_col = layer.get("y_col")
            if not x_col or not y_col:
                raise ValueError(f"Layer '{layer_name}' must define x_col and y_col")

            marker = (layer.get("marker") or "circle").lower()
            size0 = int(layer.get("size", 6))
            alpha = float(layer.get("alpha", 0.9))

            fixed_color = layer.get("color", None)
            color_col = layer.get("color_col", None)
            where = layer.get("where", None)

            df = base_df.copy()

            # Filter (pandas query) with "<>" support
            if where:
                where_clean = where.replace("<>", "!=")
                try:
                    df = df.query(where_clean, engine="python")
                except Exception as e:
                    raise ValueError(
                        f"Invalid where filter in layer '{layer_name}': {where}\n{e}"
                    )

            # Drop missing coords
            df = df.dropna(subset=[x_col, y_col]).copy()
            if len(df) == 0:
                continue

            # Transform coords to plot columns
            mx_col = f"__mx_{i}"
            my_col = f"__my_{i}"

            if transformer is not None:
                mx, my = transformer.transform(df[x_col].values, df[y_col].values)
                df[mx_col] = mx
                df[my_col] = my
            else:
                df[mx_col] = df[x_col]
                df[my_col] = df[y_col]

            # ---- Determine color field (may become grouped for numeric)
            color_field = None
            if color_col and color_col in df.columns:
                color_field = _build_grouped_numeric_column(df, color_col, layer, suffix=str(i))

            src = ColumnDataSource(df)

            glyph_kwargs = dict(
                x=mx_col,
                y=my_col,
                marker=marker,
                size=size0,
                alpha=alpha,
                source=src,
                legend_label=layer_name,  # replaced by legend_field when used
            )

            # ---- Color logic (categorical legend)
            if color_field and (color_field in df.columns):
                factors = sorted(df[color_field].dropna().unique().tolist())
                n = len(factors)

                if n == 0:
                    if fixed_color is None:
                        fixed_color = "black"
                    glyph_kwargs["line_color"] = fixed_color
                    glyph_kwargs["fill_color"] = fixed_color
                else:
                    palette = _pick_palette(n, layer)
                    mapper = factor_cmap(
                        field_name=color_field,
                        palette=palette,
                        factors=factors,
                    )

                    glyph_kwargs["line_color"] = mapper
                    glyph_kwargs["fill_color"] = mapper

                    glyph_kwargs.pop("legend_label", None)
                    glyph_kwargs["legend_field"] = color_field

                    used_legend_titles.append(
                        f"{color_col} (grouped)" if color_col and color_field != color_col else (color_col or "")
                    )
            else:
                if fixed_color is None:
                    fixed_color = "black"
                glyph_kwargs["line_color"] = fixed_color
                glyph_kwargs["fill_color"] = fixed_color

            r = p.scatter(**glyph_kwargs)

            # Hover tool
            hover = layer.get("hover", None)
            if hover is None:
                hover = [("Layer", layer_name), ("DF", df_key)]

                if color_col and color_field:
                    if color_field != color_col:
                        hover.append((f"{color_col} group", f"@{color_field}"))
                        hover.append((f"{color_col}", f"@{color_col}"))
                    else:
                        hover.append((f"{color_col}", f"@{color_field}"))

                hover.extend(
                    [
                        ("Line", "@Line"),
                        ("Station", "@Station"),
                        ("Node", "@Node"),
                        ("ROV", "@ROV"),
                        ("Status", "@Status"),
                        (x_col, f"@{x_col}{{0,0.00}}"),
                        (y_col, f"@{y_col}{{0,0.00}}"),
                    ]
                )

            p.add_tools(HoverTool(renderers=[r], tooltips=hover))

            # Spinner for layer size
            sp = Spinner(
                title=f"{layer_name} size",
                low=1,
                high=100,
                step=1,
                value=size0,
                width=170,
            )
            sp.js_on_change(
                "value", CustomJS(args=dict(r=r), code="r.glyph.size = cb_obj.value;")
            )
            layer_spinners.append(sp)

        # ---- legend setup
        if p.legend and len(p.legend) > 0:
            p.legend.click_policy = "hide"
            p.legend.location = "top_left"
            p.legend.visible = True

            uniq_titles = sorted(set([t for t in used_legend_titles if t]))
            if len(uniq_titles) == 1:
                p.legend.title = uniq_titles[0]

        # ---- controls
        controls_items = []

        if p.legend and len(p.legend) > 0:
            toggle_legend_btn = Button(label="Hide legend", button_type="primary", width=120)
            toggle_legend_btn.js_on_click(
                CustomJS(
                    args=dict(legend=p.legend[0], btn=toggle_legend_btn),
                    code="""
                        legend.visible = !legend.visible;
                        btn.label = legend.visible ? "Hide legend" : "Show legend";
                    """,
                )
            )

            cycle_legend_pos_btn = Button(label="Legend position", button_type="default", width=150)
            cycle_legend_pos_btn.js_on_click(
                CustomJS(
                    args=dict(legend=p.legend[0]),
                    code="""
                        const positions = ["top_left", "top_right", "bottom_right", "bottom_left"];
                        const current = legend.location;
                        const idx = positions.indexOf(current);
                        legend.location = positions[(idx + 1) % positions.length];
                    """,
                )
            )
            controls_items.extend([toggle_legend_btn, cycle_legend_pos_btn])

        # RP size spinner
        sp_rp = Spinner(title="RP size", low=1, high=100, step=1, value=5, width=130)
        if r_rp is not None:
            sp_rp.js_on_change("value", CustomJS(args=dict(r=r_rp), code="r.glyph.size = cb_obj.value;"))
        else:
            sp_rp.disabled = True
        controls_items.append(sp_rp)

        controls_items.extend(layer_spinners)

        controls = row(*controls_items, sizing_mode="stretch_width")
        layout = column(controls, p, sizing_mode="stretch_both")

        if is_show:
            show(layout)
            return None

        if jason_item:
            return None  # you likely return json_item(layout/p) in your Django view

        return layout
    # -------------------------
    # Convenience: read + plot
    # -------------------------
    def build_map_for_lines(
        self,
        lines: Optional[Iterable[int]] = None,
        solution_fk: int = 1,
        title: Optional[str] = None,
        dsr_limit: Optional[int] = None,
        rp_limit: Optional[int] = None,
    ):
        rp_df = self.read_rp_preplot(lines=lines, solution_fk=solution_fk, limit=rp_limit)
        dsr_df = self.read_dsr(lines=lines, solution_fk=solution_fk, limit=dsr_limit)

        ttl = title or (
            f"RPPreplot + DSR Map (Solution {solution_fk})"
            + (f" Lines: {min(lines)}–{max(lines)}" if lines else "")
        )
        return self.make_map(rp_df=rp_df, dsr_df=dsr_df, title=ttl)

    def day_by_day_deployment(self, is_show=False, json_return=False):

        sql = """
        SELECT
            ProdDate,
            ROV,
            SUM(TotalNodes) AS CNT
        FROM Daily_Deployment
        GROUP BY ProdDate, ROV
        ORDER BY ProdDate
        """

        # --------- DB read ----------
        try:
            with self._connect() as conn:
                data = pd.read_sql(sql, conn)
        except Exception as e:
            return self._error_layout(
                title="Deployment plot failed",
                message="Database query error while reading Daily_Deployment view.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

        if data is None or len(data) == 0:
            return self._error_layout(
                title="No deployment data",
                message="Daily_Deployment view returned no rows.",
                details="Check: DSR has TimeStamp and ROV filled; view Daily_Deployment exists and is populated.",
                level="warning",
                is_show=is_show,
                json_return=json_return,
            )

        # --------- Normalize ----------
        data["ProdDate"] = pd.to_datetime(data["ProdDate"], errors="coerce").dt.floor("D")
        data["ROV"] = data["ROV"].astype(str).str.strip()
        data["CNT"] = pd.to_numeric(data["CNT"], errors="coerce").fillna(0)

        data = data[(data["ROV"] != "") & data["ProdDate"].notna()]
        if len(data) == 0:
            return self._error_layout(
                title="No valid deployment rows",
                message="All rows were filtered out after cleaning (missing ProdDate or empty ROV).",
                details="Check Daily_Deployment.ProdDate and Daily_Deployment.ROV values.",
                level="warning",
                is_show=is_show,
                json_return=json_return,
            )

        rovs = sorted(data["ROV"].unique().tolist())
        if len(rovs) == 0:
            return self._error_layout(
                title="No ROVs found",
                message="Daily_Deployment contains no valid ROV values after trimming.",
                level="warning",
                is_show=is_show,
                json_return=json_return,
            )

        # --------- Prepare pivot ----------
        try:
            day_index = pd.date_range(data["ProdDate"].min(), data["ProdDate"].max(), freq="D")

            pivot = (
                data.pivot_table(index="ProdDate", columns="ROV", values="CNT", aggfunc="sum")
                .reindex(day_index)
                .fillna(0)
            )

            df = pd.DataFrame({"ProdDate": day_index})
            for r in rovs:
                df[r] = pd.to_numeric(pivot[r], errors="coerce").fillna(0).values if r in pivot.columns else 0

            df["Total"] = df[rovs].sum(axis=1)
            max_total = float(df["Total"].max()) if len(df) else 0.0
        except Exception as e:
            return self._error_layout(
                title="Deployment plot failed",
                message="Data preparation error while building pivot/day index.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

        # --------- Plot ----------
        try:
            day_ms = 86_400_000
            bar_w = day_ms * 0.9

            palette = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
                "#bcbd22", "#17becf"
            ]
            colors = [palette[i % len(palette)] for i in range(len(rovs))]

            p = figure(
                title="Deployment Day by Day",
                toolbar_location="left",
                x_axis_type="datetime",
                x_axis_label="Days",
                y_axis_label="Total Nodes",
                width_policy="max",
                y_range=(0, max_total * 1.25 if max_total > 0 else 1),
            )

            num_days = int((df["ProdDate"].max() - df["ProdDate"].min()).days) + 1
            p.xaxis[0].ticker.desired_num_ticks = max(2, num_days)

            # Legend totals (fast)
            totals = data.groupby("ROV")["CNT"].sum().to_dict()

            bars = p.vbar_stack(
                stackers=rovs,
                x="ProdDate",
                width=bar_w,
                color=colors,
                line_color="black",
                source=df,
                legend_label=[f"{r} {int(totals.get(r, 0))} nodes" for r in rovs],
            )

            # One HoverTool per stack (color-matched)
            for renderer, rov, col in zip(bars, rovs, colors):
                field = str(rov)
                field_expr = f"@{{{field}}}{{0,0}}"

                hover = HoverTool(
                    renderers=[renderer],
                    tooltips=f"""
                    <div style="font-size:12px;">
                        <div><b>Date:</b> @ProdDate{{%d/%m/%Y}}</div>
                        <div>
                            <span style="color:{col}; font-weight:bold;">{field}</span>
                            : {field_expr}
                        </div>
                        <div><b>Total:</b> @Total{{0,0}}</div>
                    </div>
                    """,
                    formatters={"@ProdDate": "datetime"},
                    mode="mouse",
                )
                p.add_tools(hover)

            p.legend.orientation = "horizontal"
            p.legend.click_policy = "hide"

            p.xaxis.formatter = DatetimeTickFormatter(
                days="%d/%m/%Y",
                months="%d/%m/%Y",
                years="%d/%m/%Y",
            )
            p.xaxis.major_label_orientation = 1.5708
            p.xaxis.ticker = DatetimeTicker(desired_num_ticks=15)
            layout = column([p], sizing_mode="stretch_both")

        except Exception as e:
            return self._error_layout(
                title="Deployment plot failed",
                message="Bokeh rendering error while building stacked bars/hover.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

        # --------- Output ----------
        if is_show:
            show(layout)
            return None

        if json_return:
            return json_item(layout)

        return layout

    def day_by_day_recovery(self, is_show=False, json_return=False):

        sql = """
        SELECT
            ProdDate,
            ROV,
            SUM(TotalNodes) AS CNT
        FROM Daily_Recovery 
        GROUP BY ProdDate, ROV
        ORDER BY ProdDate
        """

        # --------- DB read ----------
        try:
            with self._connect() as conn:
                data = pd.read_sql(sql, conn)
        except Exception as e:
            return self._error_layout(
                title="Recovery plot failed",
                message="Database query error while reading Daily_Recovery view.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

        if data is None or len(data) == 0:
            return self._error_layout(
                title="No recovery data",
                message="Daily_Recovery view returned no rows.",
                details="Check: DSR has TimeStamp and ROV filled; view Daily_Recovery exists and is populated.",
                level="warning",
                is_show=is_show,
                json_return=json_return,
            )

        # --------- Normalize ----------
        data["ProdDate"] = pd.to_datetime(data["ProdDate"], errors="coerce").dt.floor("D")
        data["ROV"] = data["ROV"].astype(str).str.strip()
        data["CNT"] = pd.to_numeric(data["CNT"], errors="coerce").fillna(0)

        data = data[(data["ROV"] != "") & data["ProdDate"].notna()]
        if len(data) == 0:
            return self._error_layout(
                title="No valid recovery rows",
                message="All rows were filtered out after cleaning (missing ProdDate or empty ROV).",
                details="Check Daily_Recovery.ProdDate and Daily_Recovery.ROV values.",
                level="warning",
                is_show=is_show,
                json_return=json_return,
            )

        rovs = sorted(data["ROV"].unique().tolist())
        if len(rovs) == 0:
            return self._error_layout(
                title="No ROVs found",
                message="Daily_Recovery contains no valid ROV values after trimming.",
                level="warning",
                is_show=is_show,
                json_return=json_return,
            )

        # --------- Prepare pivot ----------
        try:
            day_index = pd.date_range(data["ProdDate"].min(), data["ProdDate"].max(), freq="D")

            pivot = (
                data.pivot_table(index="ProdDate", columns="ROV", values="CNT", aggfunc="sum")
                .reindex(day_index)
                .fillna(0)
            )

            df = pd.DataFrame({"ProdDate": day_index})
            for r in rovs:
                df[r] = pd.to_numeric(pivot[r], errors="coerce").fillna(0).values if r in pivot.columns else 0

            df["Total"] = df[rovs].sum(axis=1)
            max_total = float(df["Total"].max()) if len(df) else 0.0
        except Exception as e:
            return self._error_layout(
                title="Recovery plot failed",
                message="Data preparation error while building pivot/day index.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

        # --------- Plot ----------
        try:
            day_ms = 86_400_000
            bar_w = day_ms * 0.9

            palette = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
                "#bcbd22", "#17becf"
            ]
            colors = [palette[i % len(palette)] for i in range(len(rovs))]

            p = figure(
                title="Recovery Day by Day",
                toolbar_location="left",
                x_axis_type="datetime",
                x_axis_label="Days",
                y_axis_label="Total Nodes",
                width_policy="max",
                y_range=(0, max_total * 1.25 if max_total > 0 else 1),
            )

            num_days = int((df["ProdDate"].max() - df["ProdDate"].min()).days) + 1
            p.xaxis[0].ticker.desired_num_ticks = max(2, num_days)

            # Legend totals (fast)
            totals = data.groupby("ROV")["CNT"].sum().to_dict()

            bars = p.vbar_stack(
                stackers=rovs,
                x="ProdDate",
                width=bar_w,
                color=colors,
                line_color="black",
                source=df,
                legend_label=[f"{r} {int(totals.get(r, 0))} nodes" for r in rovs],
            )

            # One HoverTool per stack (color-matched)
            for renderer, rov, col in zip(bars, rovs, colors):
                field = str(rov)
                field_expr = f"@{{{field}}}{{0,0}}"

                hover = HoverTool(
                    renderers=[renderer],
                    tooltips=f"""
                    <div style="font-size:12px;">
                        <div><b>Date:</b> @ProdDate{{%d/%m/%Y}}</div>
                        <div>
                            <span style="color:{col}; font-weight:bold;">{field}</span>
                            : {field_expr}
                        </div>
                        <div><b>Total:</b> @Total{{0,0}}</div>
                    </div>
                    """,
                    formatters={"@ProdDate": "datetime"},
                    mode="mouse",
                )
                p.add_tools(hover)

            p.legend.orientation = "horizontal"
            p.legend.click_policy = "hide"

            p.xaxis.formatter = DatetimeTickFormatter(
                days="%d/%m/%Y",
                months="%d/%m/%Y",
                years="%d/%m/%Y",
            )
            p.xaxis.major_label_orientation = 1.5708
            p.xaxis.ticker = DatetimeTicker(desired_num_ticks=15)

            layout = column([p], sizing_mode="stretch_both")

        except Exception as e:
            return self._error_layout(
                title="Recovery plot failed",
                message="Bokeh rendering error while building stacked bars/hover.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

        # --------- Output ----------
        if is_show:
            show(layout)
            return None

        if json_return:
            return json_item(layout)

        return layout

    def donut_rov_summary(self, metric="Stations", is_show=False, json_return=False):
        """
        One donut chart:
          - each ROV is a sector
          - value = DEPLOY_ROV_Summary.<metric> per ROV
          - baseline (100%) = RPPreplot COUNT(*)
          - remainder = baseline - SUM(ROV sectors) (clamped to >= 0)
          - style: percent labels on wedges + 1 exploded (largest ROV) slice (like sample image)
        """

        allowed_metrics = {
            "Lines", "Stations", "Nodes", "Days",
            "RECLines", "RECStations", "RECNodes", "RECDays",
            "ProcLines", "ProcStations", "ProcNodes", "ProcDays",
            "SMDepLines", "SMDepStations", "SMDepNodes",
            "SMColLine", "SMColStations", "SMColNodes",
            "SMPULines", "SMPUStations", "SMPUNodes",
        }
        if metric not in allowed_metrics:
            return self._error_layout(
                title="Donut chart failed",
                message=f"Unsupported metric: {metric}",
                details=f"Allowed: {', '.join(sorted(allowed_metrics))}",
                level="warning",
                is_show=is_show,
                json_return=json_return,
            )

        sql_rov = f"""
        SELECT
            TRIM(Rov) AS Rov,
            COALESCE({metric}, 0) AS Val
        FROM DEPLOY_ROV_Summary
        WHERE Rov IS NOT NULL
          AND TRIM(Rov) <> ''
          AND TRIM(Rov) <> 'Total'
        ORDER BY Rov
        """

        sql_base = "SELECT COUNT(*) AS Total FROM RPPreplot"

        try:
            if hasattr(self, "_connect") and callable(getattr(self, "_connect")):
                with self._connect() as conn:
                    df = pd.read_sql(sql_rov, conn)
                    base_df = pd.read_sql(sql_base, conn)
            else:
                df = pd.read_sql(sql_rov, self.db)
                base_df = pd.read_sql(sql_base, self.db)
        except Exception as e:
            return self._error_layout(
                title="Donut chart failed",
                message="Database query error while reading DEPLOY_ROV_Summary / RPPreplot.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

        if df is None or len(df) == 0:
            return self._error_layout(
                title="No donut data",
                message="DEPLOY_ROV_Summary returned no ROV rows.",
                details="Check DEPLOY_ROV_Summary view and ensure Rov rows exist (not only Total).",
                level="warning",
                is_show=is_show,
                json_return=json_return,
            )

        try:
            baseline = int(base_df.iloc[0]["Total"]) if (base_df is not None and len(base_df) > 0) else 0
        except Exception:
            baseline = 0
        baseline_disp = format(int(baseline), ",")  # safe even if baseline is numpy/int-like

        if baseline <= 0:
            return self._error_layout(
                title="Donut chart failed",
                message="Baseline is zero (RPPreplot COUNT(*) = 0).",
                details="Load RPPreplot first to define 100% total nodes.",
                level="warning",
                is_show=is_show,
                json_return=json_return,
            )

        try:
            df["Rov"] = df["Rov"].astype(str).str.strip()
            df["Val"] = pd.to_numeric(df["Val"], errors="coerce").fillna(0).astype("float64")
            df = df[df["Rov"] != ""]
            if len(df) == 0:
                return self._error_layout(
                    title="No donut data",
                    message="ROV rows became empty after cleaning.",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            df["Val"] = df["Val"].clip(lower=0)

            rov_sum = float(df["Val"].sum())
            remainder = float(max(0.0, baseline - rov_sum))

            # Build final lists (ROVs + Remaining)
            labels = df["Rov"].tolist()
            values = df["Val"].tolist()

            labels.append("Remaining")
            values.append(remainder)

            total_value = float(sum(values))
            if total_value <= 0:
                return self._error_layout(
                    title="Donut chart failed",
                    message="All donut values are zero.",
                    details=f"Metric={metric}, baseline={baseline}",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

        except Exception as e:
            return self._error_layout(
                title="Donut chart failed",
                message="Data preparation error.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

        try:
            # Colors (ROVs distinct, Remaining gray)
            palette = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
                "#bcbd22", "#17becf"
            ]
            colors = [palette[i % len(palette)] for i in range(len(labels) - 1)] + ["#e5e7eb"]

            # Explode largest ROV slice (ignore Remaining)
            explode_idx = 0
            max_val = -1
            for i in range(len(values) - 1):
                if values[i] > max_val:
                    max_val = values[i]
                    explode_idx = i

            inner_r = 0.55
            outer_r = 1.00
            label_r = (inner_r + outer_r) / 2.0
            explode_r = 0.12

            start_angles = []
            end_angles = []
            xs = []
            ys = []
            lx = []
            ly = []
            pct_text = []
            pct_num = []

            angle = 0.0
            for i, v in enumerate(values):
                frac = float(v) / total_value
                da = frac * (2.0 * math.pi)

                start = angle
                end = angle + da
                mid = (start + end) / 2.0

                off = explode_r if i == explode_idx else 0.0
                x0 = off * math.cos(mid)
                y0 = off * math.sin(mid)

                tx = x0 + label_r * math.cos(mid)
                ty = y0 + label_r * math.sin(mid)

                start_angles.append(start)
                end_angles.append(end)
                xs.append(x0)
                ys.append(y0)
                lx.append(tx)
                ly.append(ty)

                pct = frac * 100.0
                pct_num.append(pct)
                pct_text.append(f"{pct:.1f}%" if pct >= 1.0 else f"{pct:.2f}%")

                angle = end

            src = ColumnDataSource(data=dict(
                label=labels,
                value=values,
                color=colors,
                start=start_angles,
                end=end_angles,
                x=xs,
                y=ys,
                lx=lx,
                ly=ly,
                pct_txt=pct_text,
                pct=pct_num,
                baseline=[baseline] * len(labels),
            ))

            p = figure(
                height=360,
                title="Deployment",
                toolbar_location=None,
                x_range=(-1.4, 1.4),
                y_range=(-1.2, 1.2),
                width_policy="max",
            )

            p.annular_wedge(
                x="x", y="y",
                inner_radius=inner_r,
                outer_radius=outer_r,
                start_angle="start",
                end_angle="end",
                line_color="white",
                line_width=1,
                fill_color="color",
                source=src,
            )

            # Percent labels on wedges (like sample)
            p.text(
                x="lx", y="ly",
                text="pct_txt",
                text_align="center",
                text_baseline="middle",
                text_color="white",
                text_font_size="10pt",
                source=src,
            )

            p.add_tools(HoverTool(
                tooltips=[
                    ("Slice", "@label"),
                    (metric, "@value{0,0}"),
                    ("Percent", "@pct{0.0}%"),
                    ("Baseline", "@baseline{0,0}"),
                ]
            ))

            p.axis.visible = False
            p.grid.visible = False
            p.outline_line_color = None

            layout = column([p], sizing_mode="stretch_both")

        except Exception as e:
            return self._error_layout(
                title="Donut chart failed",
                message="Bokeh rendering error while building donut chart.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

        if is_show:
            show(layout)
            return None

        if json_return:
            return json_item(layout)

        return layout

    def donut_rov_summary_plotly(self, metric="Stations", is_show=False, json_return=False):
        """
        Plotly donut:
          - sectors: each ROV + "Remaining"
          - values: DEPLOY_ROV_Summary.<metric>
          - baseline: RPPreplot COUNT(*)
          - remainder: baseline - SUM(ROV)
          - exploded: largest ROV slice

        Returns:
          - if is_show: shows figure and returns None
          - if json_return: returns fig.to_json()
          - else: returns plotly Figure
        """

        allowed_metrics = {
            "Lines", "Stations", "Nodes", "Days",
            "RECLines", "RECStations", "RECNodes", "RECDays",
            "ProcLines", "ProcStations", "ProcNodes", "ProcDays",
            "SMDepLines", "SMDepStations", "SMDepNodes",
            "SMColLine", "SMColStations", "SMColNodes",
            "SMPULines", "SMPUStations", "SMPUNodes",
        }
        if metric not in allowed_metrics:
            # Plotly cannot use your _error_layout visually; return it as fallback if you're embedding Bokeh panels.
            return self._error_layout(
                title="Donut chart failed",
                message=f"Unsupported metric: {metric}",
                details=f"Allowed: {', '.join(sorted(allowed_metrics))}",
                level="warning",
                is_show=is_show,
                json_return=False,  # Bokeh only
            )

        sql_rov = f"""
        SELECT
            TRIM(Rov) AS Rov,
            COALESCE({metric}, 0) AS Val
        FROM DEPLOY_ROV_Summary
        WHERE Rov IS NOT NULL
          AND TRIM(Rov) <> ''
          AND TRIM(Rov) <> 'Total'
        ORDER BY Rov
        """

        sql_base = "SELECT COUNT(*) AS Total FROM RPPreplot"

        try:
            if hasattr(self, "_connect") and callable(getattr(self, "_connect")):
                with self._connect() as conn:
                    df = pd.read_sql(sql_rov, conn)
                    base_df = pd.read_sql(sql_base, conn)
            else:
                df = pd.read_sql(sql_rov, self.db)
                base_df = pd.read_sql(sql_base, self.db)
        except Exception as e:
            return self._error_layout(
                title="Donut chart failed",
                message="Database query error while reading DEPLOY_ROV_Summary / RPPreplot.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        if df is None or len(df) == 0:
            return self._error_layout(
                title="No donut data",
                message="DEPLOY_ROV_Summary returned no ROV rows.",
                details="Check DEPLOY_ROV_Summary view and ensure Rov rows exist (not only Total).",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        try:
            baseline = int(base_df.iloc[0]["Total"]) if (base_df is not None and len(base_df) > 0) else 0
        except Exception:
            baseline = 0
        baseline = int(baseline) if baseline else 0
        baseline_disp = format(baseline, ",")
        if baseline <= 0:
            return self._error_layout(
                title="Donut chart failed",
                message="Baseline is zero (RPPreplot COUNT(*) = 0).",
                details="Load RPPreplot first to define 100% total nodes.",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        try:
            df["Rov"] = df["Rov"].astype(str).str.strip()
            df["Val"] = pd.to_numeric(df["Val"], errors="coerce").fillna(0).astype("float64")
            df = df[df["Rov"] != ""]
            if len(df) == 0:
                return self._error_layout(
                    title="No donut data",
                    message="ROV rows became empty after cleaning.",
                    level="warning",
                    is_show=is_show,
                    json_return=False,
                )

            df["Val"] = df["Val"].clip(lower=0)

            labels = df["Rov"].tolist()
            values = df["Val"].tolist()

            rov_sum = float(sum(values))
            remainder = float(max(0.0, baseline - rov_sum))

            labels.append("Remaining")
            values.append(remainder)

            total_value = float(sum(values))
            if total_value <= 0:
                return self._error_layout(
                    title="Donut chart failed",
                    message="All donut values are zero.",
                    details=f"Metric={metric}, baseline={baseline}",
                    level="warning",
                    is_show=is_show,
                    json_return=False,
                )

            # percent for hover
            perc = [(v / baseline * 100.0) if baseline else 0.0 for v in values]

            # Pull out the biggest ROV slice (ignore Remaining)
            explode = [0.0] * len(labels)
            if len(values) > 1:
                i_max = 0
                max_v = -1
                for i in range(len(values) - 1):
                    if values[i] > max_v:
                        max_v = values[i]
                        i_max = i
                explode[i_max] = 0.10  # 0..1

            # Colors
            palette = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
                "#bcbd22", "#17becf"
            ]
            colors = [palette[i % len(palette)] for i in range(len(labels) - 1)] + ["#e5e7eb"]

        except Exception as e:
            return self._error_layout(
                title="Donut chart failed",
                message="Data preparation error.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        try:
            # Plotly (assumes plotly.graph_objects as go is imported at module level)
            fig = go.Figure(
                data=[
                    go.Pie(
                        labels=labels,
                        values=values,
                        hole=0.55,
                        pull=explode,
                        sort=False,
                        marker=dict(colors=colors, line=dict(color="white", width=1)),
                        textinfo="percent",
                        textposition="inside",
                        hovertemplate=(
                                "<b>%{label}</b><br>"
                                + f"{metric}: %{{value:,.0f}}<br>"
                                + "Percent of baseline: %{customdata:.1f}%<br>"
                                + f"Baseline: {baseline_disp}<extra></extra>"
                        ),
                        customdata=perc,
                    )
                ]
            )

            fig.update_layout(
                title=dict(text="Deployment", x=0.02, xanchor="left"),
                margin=dict(l=10, r=10, t=40, b=10),
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.05, xanchor="left", x=0),
            )

        except Exception as e:
            return self._error_layout(
                title="Donut chart failed",
                message="Plotly rendering error while building donut chart.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        if is_show:
            fig.show()
            return None

        if json_return:
            return fig.to_json()

        return fig

    def layer_donut_deployment_plotly(self, metric="Stations", is_show=False, json_return=False):
        """
        2-layer donut (Sunburst):
          - inner ring: Deployed vs Remaining (baseline - deployed)
          - outer ring: Deployed by ROV (children of Deployed)
        Baseline = RPPreplot COUNT(*)
        Deployed = SUM(DEPLOY_ROV_Summary.<metric> per ROV)
        """

        # protect against SQL injection + wrong field names
        allowed_metrics = {
            "Lines", "Stations", "Nodes", "Days",
            "RECLines", "RECStations", "RECNodes", "RECDays",
            "ProcLines", "ProcStations", "ProcNodes", "ProcDays",
            "SMDepLines", "SMDepStations", "SMDepNodes",
            "SMColLine", "SMColStations", "SMColNodes",
            "SMPULines", "SMPUStations", "SMPUNodes",
        }
        if metric not in allowed_metrics:
            return self._error_layout(
                title="Layer donut failed",
                message=f"Unsupported metric: {metric}",
                details=f"Allowed: {', '.join(sorted(allowed_metrics))}",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        sql_rov = f"""
        SELECT
            TRIM(Rov) AS Rov,
            COALESCE({metric}, 0) AS Val
        FROM DEPLOY_ROV_Summary
        WHERE Rov IS NOT NULL
          AND TRIM(Rov) <> ''
          AND TRIM(Rov) <> 'Total'
        ORDER BY Rov
        """

        sql_base = "SELECT COUNT(*) AS Total FROM RPPreplot"

        try:
            if hasattr(self, "_connect") and callable(getattr(self, "_connect")):
                with self._connect() as conn:
                    df = pd.read_sql(sql_rov, conn)
                    base_df = pd.read_sql(sql_base, conn)
            else:
                df = pd.read_sql(sql_rov, self.db)
                base_df = pd.read_sql(sql_base, self.db)
        except Exception as e:
            return self._error_layout(
                title="Layer donut failed",
                message="Database query error while reading DEPLOY_ROV_Summary / RPPreplot.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        if df is None or len(df) == 0:
            return self._error_layout(
                title="Layer donut failed",
                message="DEPLOY_ROV_Summary returned no ROV rows.",
                details="Check that DEPLOY_ROV_Summary exists and has Rov rows (not only Total).",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        try:
            baseline = int(base_df.iloc[0]["Total"]) if (base_df is not None and len(base_df) > 0) else 0
            baseline = int(baseline) if baseline else 0
        except Exception:
            baseline = 0

        if baseline <= 0:
            return self._error_layout(
                title="Layer donut failed",
                message="Baseline is zero (RPPreplot COUNT(*) = 0).",
                details="Load RPPreplot to define 100% total nodes.",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        try:
            df["Rov"] = df["Rov"].astype(str).str.strip()
            df["Val"] = pd.to_numeric(df["Val"], errors="coerce").fillna(0).astype("float64")
            df = df[df["Rov"] != ""]
            df["Val"] = df["Val"].clip(lower=0)

            rovs = df["Rov"].tolist()
            rov_vals = df["Val"].tolist()

            deployed = float(sum(rov_vals))
            remaining = float(max(0.0, baseline - deployed))

            # If deployed > baseline, we clamp remaining to 0 but still display (warn in title)
            over = deployed > baseline

            baseline_disp = format(int(baseline), ",")
            deployed_disp = format(int(deployed), ",")
            remaining_disp = format(int(remaining), ",")

        except Exception as e:
            return self._error_layout(
                title="Layer donut failed",
                message="Data preparation error.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        try:
            # Sunburst hierarchy:
            #   Baseline (root)
            #     Deployed
            #        ROV_A
            #        ROV_B
            #        ...
            #     Remaining
            labels = ["Baseline", "Deployed", "Remaining"] + rovs
            parents = ["", "Baseline", "Baseline"] + (["Deployed"] * len(rovs))
            values = [baseline, deployed, remaining] + rov_vals

            # Colors (Remaining gray, Deployed darker, ROVs palette)
            palette = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
                "#bcbd22", "#17becf"
            ]
            rov_colors = [palette[i % len(palette)] for i in range(len(rovs))]
            colors = ["#0b1220", "#2563eb", "#e5e7eb"] + rov_colors  # baseline, deployed, remaining, rovs...

            title = "Deployment"
            if over:
                title = "Deployment (Deployed > Baseline)"

            fig = go.Figure(
                go.Sunburst(
                    labels=labels,
                    parents=parents,
                    values=values,
                    branchvalues="total",
                    marker=dict(colors=colors, line=dict(color="white", width=1)),
                    maxdepth=2,
                    insidetextorientation="radial",
                    hovertemplate=(
                            "<b>%{label}</b><br>"
                            + f"{metric}: %{{value:,.0f}}<br>"
                            + "Share of baseline: %{percentRoot:.1%}<extra></extra>"
                    ),
                )
            )

            fig.update_layout(
                title=dict(text=title, x=0.02, xanchor="left"),
                margin=dict(l=10, r=10, t=40, b=10),
                uniformtext=dict(minsize=10, mode="hide"),
                annotations=[
                    dict(
                        text=(
                            f"<b>{metric}</b><br>"
                            f"{deployed_disp} / {baseline_disp}<br>"
                            f"Remaining: {remaining_disp}"
                        ),
                        x=0.5, y=0.5, showarrow=False,
                        font=dict(size=12, color="#111827"),
                        align="center",
                    )
                ],
            )

        except Exception as e:
            return self._error_layout(
                title="Layer donut failed",
                message="Plotly rendering error (Sunburst).",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        if is_show:
            fig.show()
            return None

        if json_return:
            return fig.to_json()

        return fig

    def layer_donut_deploy_recovery_plotly(self, is_show=False, json_return=False):
        """
        4-ring Sunburst:
          Ring 1: Baseline (RPPreplot COUNT(*))
          Ring 2: Deployed (SUM Stations) vs Remaining baseline
          Ring 3: Recovered (SUM RECStations) vs Still Deployed
          Ring 4: Recovered by ROV (RECStations per Rov)

        NOTE: This assumes Stations / RECStations are meaningful against RPPreplot COUNT(*).
              If Stations != nodes, switch to Nodes/RECNodes (recommended).
        """

        sql = """
        SELECT
            TRIM(Rov) AS Rov,
            COALESCE(Stations, 0)    AS Stations,
            COALESCE(RECStations, 0) AS RECStations
        FROM DEPLOY_ROV_Summary
        WHERE Rov IS NOT NULL
          AND TRIM(Rov) <> ''
          AND TRIM(Rov) <> 'Total'
        ORDER BY Rov
        """

        sql_base = "SELECT COUNT(*) AS Total FROM RPPreplot"

        try:
            if hasattr(self, "_connect") and callable(getattr(self, "_connect")):
                with self._connect() as conn:
                    df = pd.read_sql(sql, conn)
                    base_df = pd.read_sql(sql_base, conn)
            else:
                df = pd.read_sql(sql, self.db)
                base_df = pd.read_sql(sql_base, self.db)
        except Exception as e:
            return self._error_layout(
                title="Layer donut failed",
                message="Database query error while reading DEPLOY_ROV_Summary / RPPreplot.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        if df is None or len(df) == 0:
            return self._error_layout(
                title="Layer donut failed",
                message="DEPLOY_ROV_Summary returned no ROV rows.",
                details="Check DEPLOY_ROV_Summary view and ensure Rov rows exist (not only Total).",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        try:
            baseline = int(base_df.iloc[0]["Total"]) if (base_df is not None and len(base_df) > 0) else 0
            baseline = int(baseline) if baseline else 0
        except Exception:
            baseline = 0

        if baseline <= 0:
            return self._error_layout(
                title="Layer donut failed",
                message="Baseline is zero (RPPreplot COUNT(*) = 0).",
                details="Load RPPreplot to define 100% baseline.",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        try:
            df["Rov"] = df["Rov"].astype(str).str.strip()
            df["Stations"] = pd.to_numeric(df["Stations"], errors="coerce").fillna(0).astype("float64").clip(lower=0)
            df["RECStations"] = pd.to_numeric(df["RECStations"], errors="coerce").fillna(0).astype("float64").clip(
                lower=0)

            df = df[df["Rov"] != ""]
            if len(df) == 0:
                return self._error_layout(
                    title="Layer donut failed",
                    message="ROV rows became empty after cleaning.",
                    level="warning",
                    is_show=is_show,
                    json_return=False,
                )

            deployed_total = float(df["Stations"].sum())
            recovered_total = float(df["RECStations"].sum())

            # Clamp recovered so it can’t exceed deployed in the chart
            recovered_total = min(recovered_total, deployed_total)

            still_deployed = max(0.0, deployed_total - recovered_total)
            remaining_baseline = max(0.0, float(baseline) - deployed_total)

            # Per-ROV recovered values (also clamp each to its deployed value)
            rovs = df["Rov"].tolist()
            rec_by_rov = []
            for _, r in df.iterrows():
                rec_by_rov.append(float(min(r["RECStations"], r["Stations"])))

            over_baseline = deployed_total > baseline

            baseline_disp = format(int(baseline), ",")
            dep_disp = format(int(deployed_total), ",")
            rec_disp = format(int(recovered_total), ",")
            rem_disp = format(int(remaining_baseline), ",")

        except Exception as e:
            return self._error_layout(
                title="Layer donut failed",
                message="Data preparation error.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        try:
            # Hierarchy:
            # Baseline
            #   Deployed
            #     Recovered
            #       ROV_A
            #       ROV_B ...
            #     Still Deployed
            #   Remaining
            #
            # This gives 4 rings:
            # 1 Baseline, 2 Deployed/Remaining, 3 Recovered/StillDeployed, 4 ROVs under Recovered.

            labels = (
                    ["Baseline", "Deployed", "Remaining", "Recovered", "Still Deployed"]
                    + rovs
            )

            parents = (
                    ["", "Baseline", "Baseline", "Deployed", "Deployed"]
                    + (["Recovered"] * len(rovs))
            )

            values = (
                    [baseline, deployed_total, remaining_baseline, recovered_total, still_deployed]
                    + rec_by_rov
            )

            palette = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
                "#bcbd22", "#17becf"
            ]
            rov_colors = [palette[i % len(palette)] for i in range(len(rovs))]

            colors = (
                    ["#0b1220", "#2563eb", "#e5e7eb", "#22c55e",
                     "#f59e0b"]  # baseline, deployed, remaining, recovered, still
                    + rov_colors
            )

            title = "Deployment / Recovery"
            if over_baseline:
                title = "Deployment / Recovery (Deployed > Baseline)"

            fig = go.Figure(
                go.Sunburst(
                    labels=labels,
                    parents=parents,
                    values=values,
                    branchvalues="total",
                    maxdepth=4,
                    marker=dict(colors=colors, line=dict(color="white", width=1)),
                    insidetextorientation="radial",
                    hovertemplate=(
                            "<b>%{label}</b><br>"
                            + "Value: %{value:,.0f}<br>"
                            + "Share of baseline: %{percentRoot:.1%}<extra></extra>"
                    ),
                )
            )

            fig.update_layout(
                title=dict(text=title, x=0.02, xanchor="left"),
                margin=dict(l=10, r=10, t=45, b=10),
                uniformtext=dict(minsize=10, mode="hide"),
                annotations=[
                    dict(
                        text=(
                            f"<b>Baseline</b><br>{baseline_disp}<br>"
                            f"<b>Deployed</b><br>{dep_disp}<br>"
                            f"<b>Recovered</b><br>{rec_disp}<br>"
                            f"<b>Remaining</b><br>{rem_disp}"
                        ),
                        x=0.5, y=0.5, showarrow=False,
                        align="center",
                        font=dict(size=12, color="#111827"),
                    )
                ],
            )

        except Exception as e:
            return self._error_layout(
                title="Layer donut failed",
                message="Plotly rendering error (Sunburst).",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        if is_show:
            fig.show()
            return None

        if json_return:
            return fig.to_json()

        return fig

    def layer_donut_deploy_recovery_by_rov_plotly(self, is_show=False, json_return=False):
        """
        5-ring Sunburst + legend:
          Ring 1: Baseline (RPPreplot COUNT(*))
          Ring 2: Deployed vs Remaining baseline
          Ring 3: Deployed by ROV (Stations per ROV)
          Ring 4: For each ROV -> Recovered vs Still Deployed
          Ring 5: (implicit) already per ROV (Recovered/Still are children under each ROV)

        Shows:
          - labels + % of baseline in sectors
          - hover: value + % baseline + % parent
          - legend: ROV colors + recovered/still colors

        NOTE: If baseline is nodes, better use Nodes/RECNodes instead of Stations/RECStations.
        Requires module-level: import plotly.graph_objects as go
        """

        sql = """
        SELECT
            TRIM(Rov) AS Rov,
            COALESCE(Stations, 0)    AS Stations,
            COALESCE(RECStations, 0) AS RECStations
        FROM DEPLOY_ROV_Summary
        WHERE Rov IS NOT NULL
          AND TRIM(Rov) <> ''
          AND TRIM(Rov) <> 'Total'
        ORDER BY Rov
        """

        sql_base = "SELECT COUNT(*) AS Total FROM RPPreplot"

        try:
            if hasattr(self, "_connect") and callable(getattr(self, "_connect")):
                with self._connect() as conn:
                    df = pd.read_sql(sql, conn)
                    base_df = pd.read_sql(sql_base, conn)
            else:
                df = pd.read_sql(sql, self.db)
                base_df = pd.read_sql(sql_base, self.db)
        except Exception as e:
            return self._error_layout(
                title="Layer donut failed",
                message="Database query error while reading DEPLOY_ROV_Summary / RPPreplot.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        if df is None or len(df) == 0:
            return self._error_layout(
                title="Layer donut failed",
                message="DEPLOY_ROV_Summary returned no ROV rows.",
                details="Check DEPLOY_ROV_Summary view and ensure Rov rows exist (not only Total).",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        try:
            baseline = int(base_df.iloc[0]["Total"]) if (base_df is not None and len(base_df) > 0) else 0
            baseline = int(baseline) if baseline else 0
        except Exception:
            baseline = 0

        if baseline <= 0:
            return self._error_layout(
                title="Layer donut failed",
                message="Baseline is zero (RPPreplot COUNT(*) = 0).",
                details="Load RPPreplot to define 100% baseline.",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        try:
            df["Rov"] = df["Rov"].astype(str).str.strip()
            df["Stations"] = pd.to_numeric(df["Stations"], errors="coerce").fillna(0).astype("float64").clip(lower=0)
            df["RECStations"] = pd.to_numeric(df["RECStations"], errors="coerce").fillna(0).astype("float64").clip(
                lower=0)

            df = df[df["Rov"] != ""]
            if len(df) == 0:
                return self._error_layout(
                    title="Layer donut failed",
                    message="ROV rows became empty after cleaning.",
                    level="warning",
                    is_show=is_show,
                    json_return=False,
                )

            # Clamp REC per ROV to not exceed deployed per ROV
            df["RECStations"] = df[["RECStations", "Stations"]].min(axis=1)

            deployed_total = float(df["Stations"].sum())
            remaining_baseline = float(max(0.0, baseline - deployed_total))
            over_baseline = deployed_total > baseline

            rovs = df["Rov"].tolist()
            dep_by_rov = [float(x) for x in df["Stations"].tolist()]
            rec_by_rov = [float(x) for x in df["RECStations"].tolist()]
            still_by_rov = [float(x) for x in (df["Stations"] - df["RECStations"]).tolist()]

            baseline_disp = format(int(baseline), ",")
            dep_disp = format(int(deployed_total), ",")
            rem_disp = format(int(remaining_baseline), ",")

        except Exception as e:
            return self._error_layout(
                title="Layer donut failed",
                message="Data preparation error.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        try:
            labels = []
            parents = []
            values = []
            colors = []

            palette = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
                "#bcbd22", "#17becf"
            ]
            rov_colors = [palette[i % len(palette)] for i in range(len(rovs))]

            # Ring 1
            labels.append("Baseline")
            parents.append("")
            values.append(float(baseline))
            colors.append("#0b1220")

            # Ring 2
            labels += ["Deployed", "Remaining"]
            parents += ["Baseline", "Baseline"]
            values += [float(deployed_total), float(remaining_baseline)]
            colors += ["#2563eb", "#e5e7eb"]

            # Ring 3 + 4 per ROV
            for rov, dep, rec, still, c in zip(rovs, dep_by_rov, rec_by_rov, still_by_rov, rov_colors):
                rov_node = f"{rov}"
                labels.append(rov_node)
                parents.append("Deployed")
                values.append(float(dep))
                colors.append(c)

                labels.append(f"{rov}<br> • Rec.")
                parents.append(rov_node)
                values.append(float(rec))
                colors.append("#22c55e")

                labels.append(f"{rov}<br> • Dep.")
                parents.append(rov_node)
                values.append(float(still))
                colors.append("#f59e0b")

            title = "Deployment / Recovery by ROV"
            if over_baseline:
                title = "Deployment / Recovery by ROV (Deployed > Baseline)"

            fig = go.Figure(
                go.Sunburst(
                    labels=labels,
                    parents=parents,
                    values=values,
                    branchvalues="total",
                    maxdepth=5,
                    marker=dict(colors=colors, line=dict(color="white", width=1)),

                    # Show label + % of baseline in sectors
                    textinfo="label+percent root",
                    insidetextorientation="radial",

                    hovertemplate=(
                            "<b>%{label}</b><br>"
                            + "Value: %{value:,.0f}<br>"
                            + "Percent of baseline: %{percentRoot:.1%}<br>"
                            + "Percent of parent: %{percentParent:.1%}"
                            + "<extra></extra>"
                    ),
                )
            )

            # Add a legend using invisible scatter traces
            for rov, c in zip(rovs, rov_colors):
                fig.add_trace(
                    go.Scatter(
                        x=[None],
                        y=[None],
                        mode="markers",
                        marker=dict(size=10, color=c),
                        name=rov,
                        showlegend=True,
                    )
                )

            fig.add_trace(go.Scatter(
                x=[None], y=[None],
                mode="markers",
                marker=dict(size=10, color="#22c55e"),
                name="Recovered",
                showlegend=True
            ))

            fig.add_trace(go.Scatter(
                x=[None], y=[None],
                mode="markers",
                marker=dict(size=10, color="#f59e0b"),
                name="Deployed",
                showlegend=True
            ))

            fig.update_layout(
                title=dict(text=title, x=0.02, xanchor="left"),
                margin=dict(l=10, r=10, t=45, b=40),
                uniformtext=dict(minsize=10, mode="show"),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.12,
                    xanchor="left",
                    x=0,
                ),
                annotations=[
                    dict(
                        text=(
                            f"<b>Baseline</b><br>{baseline_disp}<br>"
                            f"<b>Deployed</b><br>{dep_disp}<br>"
                            f"<b>Remaining</b><br>{rem_disp}"
                        ),
                        x=0.5, y=0.5, showarrow=False,
                        align="center",
                        font=dict(size=12, color="#111827"),
                    )
                ],
            )

        except Exception as e:
            return self._error_layout(
                title="Layer donut failed",
                message="Plotly rendering error (Sunburst).",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        if is_show:
            fig.show()
            return None

        if json_return:
            return fig.to_json()

        return fig

    def layer_donut_deploy_and_recovery_plotly(self, is_show=False, json_return=False):
        """
        5-ring Sunburst (as requested):

          Ring 1: Baseline (RPPreplot COUNT(*))

          Ring 2: Deployed vs RemainingBaseline
                  Deployed = SUM(Stations)
                  RemainingBaseline = baseline - SUM(Stations)

          Ring 3: Deployed by ROV (Stations per Rov)

          Ring 4: Under each ROV: Recovered vs Still (Recovered uses RECStations)
                  RecoveredROV = RECStations (clamped <= Stations)
                  StillROV = Stations - RECStations

          Ring 5: Recovered by ROV (RECStations per Rov)
                  (This is the "Recovered" child under each ROV; it is already per ROV)

        NOTE: If your baseline is nodes, strongly consider switching Stations/RECStations to Nodes/RECNodes.
        Requires module-level: import plotly.graph_objects as go
        """

        sql = """
        SELECT
            TRIM(Rov) AS Rov,
            COALESCE(Stations, 0)    AS Stations,
            COALESCE(RECStations, 0) AS RECStations
        FROM DEPLOY_ROV_Summary
        WHERE Rov IS NOT NULL
          AND TRIM(Rov) <> ''
          AND TRIM(Rov) <> 'Total'
        ORDER BY Rov
        """

        sql_base = "SELECT COUNT(*) AS Total FROM RPPreplot"

        # ---- read
        try:
            if hasattr(self, "_connect") and callable(getattr(self, "_connect")):
                with self._connect() as conn:
                    df = pd.read_sql(sql, conn)
                    base_df = pd.read_sql(sql_base, conn)
            else:
                df = pd.read_sql(sql, self.db)
                base_df = pd.read_sql(sql_base, self.db)
        except Exception as e:
            return self._error_layout(
                title="Layer donut failed",
                message="Database query error while reading DEPLOY_ROV_Summary / RPPreplot.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        if df is None or len(df) == 0:
            return self._error_layout(
                title="Layer donut failed",
                message="DEPLOY_ROV_Summary returned no ROV rows.",
                details="Check DEPLOY_ROV_Summary view and ensure Rov rows exist (not only Total).",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        # ---- baseline
        try:
            baseline = int(base_df.iloc[0]["Total"]) if (base_df is not None and len(base_df) > 0) else 0
            baseline = int(baseline) if baseline else 0
        except Exception:
            baseline = 0

        if baseline <= 0:
            return self._error_layout(
                title="Layer donut failed",
                message="Baseline is zero (RPPreplot COUNT(*) = 0).",
                details="Load RPPreplot to define 100% baseline.",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        # ---- normalize + compute
        try:
            df["Rov"] = df["Rov"].astype(str).str.strip()
            df["Stations"] = pd.to_numeric(df["Stations"], errors="coerce").fillna(0).astype("float64").clip(lower=0)
            df["RECStations"] = pd.to_numeric(df["RECStations"], errors="coerce").fillna(0).astype("float64").clip(
                lower=0)

            df = df[df["Rov"] != ""]
            if len(df) == 0:
                return self._error_layout(
                    title="Layer donut failed",
                    message="ROV rows became empty after cleaning.",
                    level="warning",
                    is_show=is_show,
                    json_return=False,
                )

            # Clamp recovered per ROV so it cannot exceed deployed per ROV
            df["RECStations"] = df[["RECStations", "Stations"]].min(axis=1)
            df["StillStations"] = (df["Stations"] - df["RECStations"]).clip(lower=0)

            deployed_total = float(df["Stations"].sum())
            remaining_baseline = float(max(0.0, baseline - deployed_total))
            over_baseline = deployed_total > baseline

            rovs = df["Rov"].tolist()
            dep_by_rov = [float(x) for x in df["Stations"].tolist()]
            rec_by_rov = [float(x) for x in df["RECStations"].tolist()]
            still_by_rov = [float(x) for x in df["StillStations"].tolist()]

            # (Optional) totals for center annotation
            recovered_total = float(sum(rec_by_rov))
            still_total = float(sum(still_by_rov))

            baseline_disp = format(int(baseline), ",")
            dep_disp = format(int(deployed_total), ",")
            rem_disp = format(int(remaining_baseline), ",")
            rec_disp = format(int(recovered_total), ",")
            still_disp = format(int(still_total), ",")

        except Exception as e:
            return self._error_layout(
                title="Layer donut failed",
                message="Data preparation error.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        # ---- build sunburst
        try:
            labels = []
            parents = []
            values = []
            colors = []

            palette = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
                "#bcbd22", "#17becf"
            ]
            rov_colors = [palette[i % len(palette)] for i in range(len(rovs))]

            # Ring 1
            labels.append("Baseline")
            parents.append("")
            values.append(float(baseline))
            colors.append("#0b1220")

            # Ring 2
            labels += ["Deployed", "Remaining"]
            parents += ["Baseline", "Baseline"]
            values += [float(deployed_total), float(remaining_baseline)]
            colors += ["#2563eb", "#e5e7eb"]

            # Ring 3 + Ring 4/5 per ROV
            for rov, dep, rec, still, c in zip(rovs, dep_by_rov, rec_by_rov, still_by_rov, rov_colors):
                rov_node = f"{rov}"
                labels.append(rov_node)
                parents.append("Deployed")
                values.append(float(dep))
                colors.append(c)

                # Ring 4 under each ROV
                rec_node = f"{rov} • Recovered"
                still_node = f"{rov} • Still"

                labels.append(rec_node)
                parents.append(rov_node)
                values.append(float(rec))
                colors.append("#22c55e")  # recovered green

                labels.append(still_node)
                parents.append(rov_node)
                values.append(float(still))
                colors.append("#f59e0b")  # still amber

                # Ring 5 "Recovered by ROV" is already represented by rec_node (it is per ROV).
                # If you want an extra 5th ring OUTSIDE recovered (needs another dimension),
                # tell me what you want to split recovery by (day/line/vessel/etc.).

            title = "Deployed / Recovered (by ROV)"
            if over_baseline:
                title = "Deployed / Recovered (by ROV) (Deployed > Baseline)"

            fig = go.Figure(
                go.Sunburst(
                    labels=labels,
                    parents=parents,
                    values=values,
                    branchvalues="total",
                    maxdepth=5,
                    marker=dict(colors=colors, line=dict(color="white", width=1)),
                    insidetextorientation="radial",
                    textinfo="label+percent root",
                    hovertemplate=(
                            "<b>%{label}</b><br>"
                            + "Value: %{value:,.0f}<br>"
                            + "Percent of baseline: %{percentRoot:.1%}<br>"
                            + "Percent of parent: %{percentParent:.1%}"
                            + "<extra></extra>"
                    ),
                )
            )

            # Legend workaround (Sunburst has no native legend)
            for rov, c in zip(rovs, rov_colors):
                fig.add_trace(
                    go.Scatter(
                        x=[None], y=[None],
                        mode="markers",
                        marker=dict(size=10, color=c),
                        name=rov,
                        showlegend=True,
                    )
                )
            fig.add_trace(go.Scatter(
                x=[None], y=[None],
                mode="markers",
                marker=dict(size=10, color="#22c55e"),
                name="Recovered",
                showlegend=True
            ))
            fig.add_trace(go.Scatter(
                x=[None], y=[None],
                mode="markers",
                marker=dict(size=10, color="#f59e0b"),
                name="Still Deployed",
                showlegend=True
            ))
            fig.add_trace(go.Scatter(
                x=[None], y=[None],
                mode="markers",
                marker=dict(size=10, color="#e5e7eb"),
                name="Remaining baseline",
                showlegend=True
            ))

            fig.update_layout(
                title=dict(text=title, x=0.02, xanchor="left"),
                margin=dict(l=10, r=10, t=45, b=50),
                uniformtext=dict(minsize=10, mode="show"),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.18,
                    xanchor="left",
                    x=0,
                ),
                annotations=[
                    dict(
                        text=(
                            f"<b>Baseline</b><br>{baseline_disp}<br>"
                            f"<b>Deployed</b><br>{dep_disp}<br>"
                            f"<b>Recovered</b><br>{rec_disp}<br>"
                            f"<b>Still</b><br>{still_disp}<br>"
                            f"<b>Remaining</b><br>{rem_disp}"
                        ),
                        x=0.5, y=0.5, showarrow=False,
                        align="center",
                        font=dict(size=12, color="#111827"),
                    )
                ],
            )

        except Exception as e:
            return self._error_layout(
                title="Layer donut failed",
                message="Plotly rendering error (Sunburst).",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        if is_show:
            fig.show()
            return None

        if json_return:
            return fig.to_json()

        return fig

    def sunburst_prod_3layers_plotly(
            self,
            metric="Stations",
            title=None,
            labels=None,
            is_show=False,
            json_return=False,
    ):
        """
        Universal Sunburst (3 layers):
          Ring 1: Baseline = RPPreplot COUNT(*)
          Ring 2: Total(metric) vs Remaining = baseline - SUM(metric)
          Ring 3: metric by ROV

        Parameters
        ----------
        metric : str
            Column name from DEPLOY_ROV_Summary (e.g. "Stations", "RECStations", "Nodes", "RECNodes", ...)
        title : str | None
            Optional plot title override. If None -> auto based on metric.
        labels : dict | None
            Optional label overrides:
              {
                "baseline": "Baseline",
                "total": "Deployment",      # the ring-2 total label
                "remaining": "Remaining",
                "unit": "stations",         # used in hover (optional)
              }
            If None -> auto based on metric.
        is_show : bool
            If True -> fig.show() and returns None
        json_return : bool
            If True -> returns fig.to_json() (Plotly JSON)

        Requires module-level: import plotly.graph_objects as go
        """

        allowed_metrics = {
            "Lines", "Stations", "Nodes", "Days",
            "RECLines", "RECStations", "RECNodes", "RECDays",
            "ProcLines", "ProcStations", "ProcNodes", "ProcDays",
            "SMDepLines", "SMDepStations", "SMDepNodes",
            "SMColLine", "SMColStations", "SMColNodes",
            "SMPULines", "SMPUStations", "SMPUNodes",
        }
        if metric not in allowed_metrics:
            return self._plotly_error_html(
                title="Sunburst failed",
                message=f"Unsupported metric: {metric}",
                details=f"Allowed: {', '.join(sorted(allowed_metrics))}",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        # ---- auto naming based on metric
        m = str(metric).strip()
        m_upper = m.upper()

        # heuristics
        is_recovery = m_upper.startswith("REC") or m_upper.endswith("REC") or "REC" in m_upper
        is_processed = m_upper.startswith("PROC") or "PROC" in m_upper
        is_sm = m_upper.startswith("SM")

        if is_recovery:
            default_total = "Recovery"
        elif is_processed:
            default_total = "Processed"
        elif is_sm:
            default_total = "SM"
        else:
            default_total = "Deployment"

        default_title = f"{default_total} — {m}"

        # defaults for labels
        lbl = {
            "baseline": "Baseline",
            "total": default_total,
            "remaining": "Remaining",
            "unit": m,  # shown in hover
        }
        if isinstance(labels, dict):
            lbl.update({k: v for k, v in labels.items() if v is not None})

        if title is None:
            title = default_title

        sql_rov = f"""
        SELECT
            TRIM(Rov) AS Rov,
            COALESCE({m}, 0) AS Val
        FROM DEPLOY_ROV_Summary
        WHERE Rov IS NOT NULL
          AND TRIM(Rov) <> ''
          AND TRIM(Rov) <> 'Total'
        ORDER BY Rov
        """

        sql_base = "SELECT COUNT(*) AS Total FROM RPPreplot"

        # ---- read
        try:
            if hasattr(self, "_connect") and callable(getattr(self, "_connect")):
                with self._connect() as conn:
                    df = pd.read_sql(sql_rov, conn)
                    base_df = pd.read_sql(sql_base, conn)
            else:
                df = pd.read_sql(sql_rov, self.db)
                base_df = pd.read_sql(sql_base, self.db)
        except Exception as e:
            return self._plotly_error_html(
                title="Sunburst failed",
                message="Database query error while reading DEPLOY_ROV_Summary / RPPreplot.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        if df is None or len(df) == 0:
            return self._plotly_error_html(
                title="Sunburst failed",
                message="DEPLOY_ROV_Summary returned no ROV rows.",
                details="Check DEPLOY_ROV_Summary view and ensure Rov rows exist (not only Total).",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        # ---- baseline
        try:
            baseline = int(base_df.iloc[0]["Total"]) if (base_df is not None and len(base_df) > 0) else 0
            baseline = int(baseline) if baseline else 0
        except Exception:
            baseline = 0

        if baseline <= 0:
            return self._plotly_error_html(
                title="Sunburst failed",
                message="Baseline is zero (RPPreplot COUNT(*) = 0).",
                details="Load RPPreplot to define 100% baseline.",
                level="warning",
                is_show=is_show,
                json_return=False,
            )

        # ---- normalize + compute
        try:
            df["Rov"] = df["Rov"].astype(str).str.strip()
            df["Val"] = pd.to_numeric(df["Val"], errors="coerce").fillna(0).astype("float64").clip(lower=0)
            df = df[df["Rov"] != ""]
            if len(df) == 0:
                return self._plotly_error_html(
                    title="Sunburst failed",
                    message="ROV rows became empty after cleaning.",
                    level="warning",
                    is_show=is_show,
                    json_return=False,
                )

            rovs = df["Rov"].tolist()
            rov_vals = [float(v) for v in df["Val"].tolist()]

            total_val = float(sum(rov_vals))
            remaining_val = float(max(0.0, baseline - total_val))

            over = total_val > baseline
            if over:
                remaining_val = 0.0

            baseline_disp = format(int(baseline), ",")
            total_disp = format(int(total_val), ",")
            remaining_disp = format(int(remaining_val), ",")

        except Exception as e:
            return self._plotly_error_html(
                title="Sunburst failed",
                message="Data preparation error.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        # ---- build sunburst (3 layers)
        try:
            # Hierarchy:
            # baseline
            #   total
            #     rovs...
            #   remaining
            labels_sb = [lbl["baseline"], lbl["total"], lbl["remaining"]] + rovs
            parents_sb = ["", lbl["baseline"], lbl["baseline"]] + ([lbl["total"]] * len(rovs))
            values_sb = [float(baseline), float(total_val), float(remaining_val)] + rov_vals

            palette = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
                "#bcbd22", "#17becf"
            ]
            rov_colors = [palette[i % len(palette)] for i in range(len(rovs))]

            # color for total depends on meaning (deployment/recovery/processed/etc.)
            total_color_map = {
                "Deployment": "#2563eb",
                "Recovery": "#22c55e",
                "Processed": "#a855f7",
                "SM": "#f97316",
            }
            total_color = total_color_map.get(lbl["total"], "#2563eb")

            colors_sb = [
                            "#0b1220",  # baseline
                            total_color,  # total
                            "#e5e7eb",  # remaining
                        ] + rov_colors

            final_title = title
            if over:
                final_title = f"{title} (Total > Baseline)"

            fig = go.Figure(
                go.Sunburst(
                    labels=labels_sb,
                    parents=parents_sb,
                    values=values_sb,
                    branchvalues="total",
                    maxdepth=3,
                    marker=dict(colors=colors_sb, line=dict(color="white", width=1)),
                    insidetextorientation="radial",
                    textinfo="label+percent root",
                    hovertemplate=(
                            "<b>%{label}</b><br>"
                            + f"{lbl['unit']}: %{{value:,.0f}}<br>"
                            + "Percent of baseline: %{percentRoot:.1%}<br>"
                            + "Percent of parent: %{percentParent:.1%}"
                            + "<extra></extra>"
                    ),
                )
            )

            # Legend workaround (Sunburst has no native legend)
            for rov, c in zip(rovs, rov_colors):
                fig.add_trace(go.Scatter(
                    x=[None], y=[None],
                    mode="markers",
                    marker=dict(size=10, color=c),
                    name=rov,
                    showlegend=True
                ))
            fig.add_trace(go.Scatter(
                x=[None], y=[None],
                mode="markers",
                marker=dict(size=10, color=total_color),
                name=lbl["total"],
                showlegend=True
            ))
            fig.add_trace(go.Scatter(
                x=[None], y=[None],
                mode="markers",
                marker=dict(size=10, color="#e5e7eb"),
                name=lbl["remaining"],
                showlegend=True
            ))

            fig.update_layout(
                title=dict(text=final_title, x=0.02, xanchor="left"),
                margin=dict(l=10, r=10, t=45, b=55),
                uniformtext=dict(minsize=10, mode="show"),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.18,
                    xanchor="left",
                    x=0,
                ),
                annotations=[
                    dict(
                        text=(
                            f"<b>{lbl['baseline']}</b><br>{baseline_disp}<br>"
                            f"<b>{lbl['total']}</b><br>{total_disp}<br>"
                            f"<b>{lbl['remaining']}</b><br>{remaining_disp}"
                        ),
                        x=0.5, y=0.5, showarrow=False,
                        align="center",
                        font=dict(size=12, color="#111827"),
                    )
                ],
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                template="plotly_dark",
                paper_bgcolor="#111827",  # outer background
                plot_bgcolor="#111827",  # inner background
                font=dict(color="white"),
            )

        except Exception as e:
            return self._plotly_error_html(
                title="Sunburst failed",
                message="Plotly rendering error (Sunburst).",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        if is_show:
            fig.show()
            return None
        elif json_return:
            return fig.to_json()
        else:
            plot_html = fig.to_html(
            full_html=False,
            include_plotlyjs="cdn",
            config={"responsive": True})
            return plot_html

    def build_offsets_histograms_by_rov(
            self,
            dsr_df,
            *,
            rov_col="ROV",
            inline_col="PriOffInline",
            xline_col="PriOffXline",
            radial_col="PriOffDist",
            bins=60,
            title_prefix="Offsets",
            show_mean_line=True,
            show_std_lines=True,
            std_k=1.0,
            show_kde=True,
            kde_points=300,
            kde_bw="scott",
            max_offset=None,  # <--- NEW (float). If set:
            #      inline/xline: [-max_offset, +max_offset]
            #      radial:       [0, max_offset]
            is_show=False,
            json_import=False,
            target_id="dsr_offsets_hist",
    ):
        def _error_layout(msg, exc=None):
            txt = f"<b>Offsets histograms error</b><br>{msg}"
            if exc is not None:
                txt += f"<br><pre>{str(exc)}</pre>"
            return Div(text=txt, sizing_mode="stretch_both")

        try:
            if dsr_df is None or len(dsr_df) == 0:
                layout = _error_layout("Empty dataframe.")
                if json_import:
                    return json_item(layout, target_id)
                if is_show:
                    show(layout)
                return layout

            for c in (rov_col, inline_col, xline_col, radial_col):
                if c not in dsr_df.columns:
                    raise ValueError(f"Missing column '{c}' in dsr_df")

            df = dsr_df.copy()
            df[inline_col] = pd.to_numeric(df[inline_col], errors="coerce")
            df[xline_col] = pd.to_numeric(df[xline_col], errors="coerce")
            df[radial_col] = pd.to_numeric(df[radial_col], errors="coerce")

            df[rov_col] = df[rov_col].astype(str).fillna("")
            df = df[df[rov_col].str.strip() != ""]
            if len(df) == 0:
                layout = _error_layout(f"Column '{rov_col}' is empty after filtering.")
                if json_import:
                    return json_item(layout, target_id)
                if is_show:
                    show(layout)
                return layout

            def _finite(v):
                v = np.asarray(v, dtype="float64")
                return v[np.isfinite(v)]

            def _nonzero_bins_range(values, *, nbins, fallback_pad=1.0):
                v = _finite(values)
                if len(v) == 0:
                    return (-fallback_pad, fallback_pad), None, None

                lo = float(np.min(v))
                hi = float(np.max(v))
                if lo == hi:
                    pad = fallback_pad if lo == 0 else abs(lo) * 0.05
                    lo, hi = lo - pad, hi + pad

                counts, edges = np.histogram(v, bins=nbins, range=(lo, hi))
                nz = np.where(counts > 0)[0]
                if nz.size == 0:
                    return (lo, hi), edges, counts

                i0 = int(nz[0])
                i1 = int(nz[-1])
                x_min = float(edges[i0])
                x_max = float(edges[i1 + 1])

                if x_min == x_max:
                    pad = fallback_pad if x_min == 0 else abs(x_min) * 0.05
                    x_min, x_max = x_min - pad, x_max + pad

                return (x_min, x_max), edges, counts

            # ---- shared X ranges per column
            if max_offset is not None:
                try:
                    mo = float(max_offset)
                except Exception:
                    raise ValueError("max_offset must be a number or None")
                if not np.isfinite(mo) or mo <= 0:
                    raise ValueError("max_offset must be > 0")

                inline_range = Range1d(-mo, +mo)
                xline_range = Range1d(-mo, +mo)
                radial_range = Range1d(0.0, mo)
            else:
                inline_x, _, _ = _nonzero_bins_range(df[inline_col].to_numpy(dtype="float64"), nbins=bins)
                xline_x, _, _ = _nonzero_bins_range(df[xline_col].to_numpy(dtype="float64"), nbins=bins)
                radial_x, _, _ = _nonzero_bins_range(df[radial_col].to_numpy(dtype="float64"), nbins=bins)

                inline_range = Range1d(inline_x[0], inline_x[1])
                xline_range = Range1d(xline_x[0], xline_x[1])
                radial_range = Range1d(radial_x[0], radial_x[1])

            # -------- KDE helper (no scipy)
            def _kde_xy(values, x_min, x_max, n_points=300, bw="scott"):
                v = _finite(values)
                n = len(v)
                if n < 2:
                    return None

                std = float(np.std(v, ddof=1))
                if std <= 0:
                    return None

                if isinstance(bw, (int, float)) and bw > 0:
                    h = float(bw)
                else:
                    h = std * (n ** (-0.2))  # Scott

                if not np.isfinite(h) or h <= 0:
                    return None

                x = np.linspace(float(x_min), float(x_max), int(n_points))
                z = (x[:, None] - v[None, :]) / h
                density = np.mean(np.exp(-0.5 * z * z), axis=1) / (h * np.sqrt(2.0 * np.pi))
                return x, density

            def _hist_fig(values, title, shared_range):
                values = _finite(values)

                # clip to x-range so histogram/KDE reflect chosen max_offset window
                x0 = float(shared_range.start)
                x1 = float(shared_range.end)
                if len(values) > 0:
                    values = values[(values >= x0) & (values <= x1)]

                p = figure(
                    title=title,
                    sizing_mode="stretch_both",
                    x_range=shared_range,
                    tools="pan,wheel_zoom,box_zoom,reset,save",
                )
                p.xaxis.axis_label = "Offset"
                p.yaxis.axis_label = "Number of Nodes"

                if len(values) == 0:
                    return p

                counts, edges = np.histogram(values, bins=bins, range=(x0, x1))

                # remove zero bins (do not draw)
                mask = counts > 0
                if np.any(mask):
                    left = edges[:-1][mask]
                    right = edges[1:][mask]
                    top = counts[mask]
                    src = ColumnDataSource(dict(left=left, right=right, top=top))
                    p.quad(left="left", right="right", bottom=0, top="top", source=src)

                bin_w = float(edges[1] - edges[0]) if len(edges) > 1 else 1.0

                m = float(np.mean(values))
                s = float(np.std(values, ddof=1)) if len(values) > 1 else 0.0

                if show_mean_line and np.isfinite(m):
                    p.add_layout(
                        Span(
                            location=m,
                            dimension="height",
                            line_color="red",
                            line_width=2,
                            line_dash="dashed",
                        )
                    )

                if show_std_lines and np.isfinite(m) and np.isfinite(s) and s > 0:
                    for loc in (m - std_k * s, m + std_k * s):
                        p.add_layout(
                            Span(
                                location=float(loc),
                                dimension="height",
                                line_color="black",
                                line_width=1,
                                line_dash="dotdash",
                            )
                        )
                    p.add_layout(
                        Label(
                            x=5,
                            y=5,
                            x_units="screen",
                            y_units="screen",
                            text=f"μ={m:.2f}  σ={s:.2f}",
                            text_font_size="9pt",
                        )
                    )

                if show_kde and len(values) >= 5:
                    kde = _kde_xy(values, x0, x1, n_points=kde_points, bw=kde_bw)
                    if kde is not None:
                        xk, dk = kde
                        yk = dk * float(len(values)) * bin_w
                        p.line(xk, yk, line_width=2)

                return p

            rows = []
            for rov, g in df.groupby(rov_col, sort=True):
                p1 = _hist_fig(g[inline_col].to_numpy(dtype="float64"), f"{title_prefix} | {rov} | Inline",
                               inline_range)
                p2 = _hist_fig(g[xline_col].to_numpy(dtype="float64"), f"{title_prefix} | {rov} | Xline", xline_range)
                p3 = _hist_fig(g[radial_col].to_numpy(dtype="float64"), f"{title_prefix} | {rov} | RadialOffset",
                               radial_range)
                rows.append([p1, p2, p3])

            layout = gridplot(rows, sizing_mode="stretch_both", merge_tools=True) if rows else _error_layout(
                "No groups found.")
            if is_show:
                show(layout)
            if json_import:
                return json_item(layout, target_id)
            return layout

        except Exception as e:
            layout = _error_layout("Unhandled exception while building plots.", e)
            if json_import:
                return json_item(layout, target_id)
            if is_show:
                show(layout)
            return layout
