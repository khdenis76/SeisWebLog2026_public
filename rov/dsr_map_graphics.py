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
from bokeh.models import Span, Range1d, FactorRange, Legend, LegendItem, LinearColorMapper, BasicTicker, \
    NumeralTickFormatter, ColorBar, Select, CheckboxGroup
from bokeh.palettes import Category10, Category20, Turbo256

from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, Span, Range1d,Label, HoverTool, Button, Spinner, CustomJS, LabelSet, DatetimeTickFormatter, Div, \
    DatetimeTicker, LinearAxis
from bokeh.models import WMTSTileSource
import geopandas as gpd
from bokeh.transform import factor_cmap, cumsum
import plotly.graph_objects as go
from pyproj import Transformer
from bokeh.io import output_file, save, show
from bokeh.layouts import column, row
from bokeh.models import DateRangeSlider, CustomJSFilter, CDSView
from bokeh.models import (
    ColumnDataSource, HoverTool, CustomJS, LinearColorMapper, ColorBar,
    BasicTicker, NumeralTickFormatter, Spinner, Legend, LegendItem,
    DateRangeSlider, Slider, Button, MultiChoice
)
import xyzservices.providers as xyz
from bokeh.layouts import column, row, gridplot, Spacer
from bokeh.models import Legend, LegendItem, DataTable, TableColumn, NumberFormatter

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

    def read_recdb_with_dsr(
        self,
        lines: Optional[Iterable[int]] = None,
    ) -> pd.DataFrame:
        """Read REC_DB rows together with matched DSR recovery coordinates and ROV."""
        lines_list = self._ensure_list(lines)
        sql = """
            SELECT
                r.*,
                d.PrimaryEasting1 AS DSR_PrimaryEasting1,
                d.PrimaryNorthing1 AS DSR_PrimaryNorthing1,
                d.ROV AS DSR_ROV
            FROM REC_DB r
            LEFT JOIN DSR d
              ON CAST(d.Line AS INTEGER) = CAST(r.Line AS INTEGER)
             AND CAST(d.Station AS INTEGER) = CAST(r.Point AS INTEGER)
             AND COALESCE(d.Solution_FK, 1) = 1
            WHERE 1=1
        """
        params: dict = {}
        if lines_list is not None:
            in_clause, p = self._sql_in_clause(lines_list, "ln")
            sql += f" AND CAST(r.Line AS INTEGER) IN {in_clause}"
            params.update(p)

        with self._connect() as con:
            df = pd.read_sql_query(sql, con, params=params)

        for c in ("Line", "Point"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        return df

    def read_line_summary(self, parse_dates: bool = True):
        """
        Read all rows from V_DSR_LineSummary view
        and return as pandas DataFrame.
        """

        with self._connect() as conn:
            df = pd.read_sql_query(
                "SELECT * FROM V_DSR_LineSummary",
                conn
            )

        if df.empty:
            return df

        if parse_dates:
            for col in df.columns:
                if "Time" in col or "Date" in col:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

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

    def make_map_multi_layers_datashader(
            self,
            *,
            rp_df=None,
            dsr_df=None,
            sm_df=None,
            rec_db_df=None,
            title="PROJECT PROGRESS MAP",
            layers=None,
            show_preplot=True,
            show_shapes=True,
            show_sm=True,
            show_tiles=False,
            plot_width=1300,
            plot_height=800,
            canvas_width=1600,
            canvas_height=1000,
            is_show=False,
            save_html_path=None,
    ):


        if layers is None:
            layers = []

        df_map = {
            "rp": rp_df,
            "preplot": rp_df,
            "dsr": dsr_df,
            "sm": sm_df,
            "rec": rec_db_df,
            "rec_db": rec_db_df,
        }

        def _clean_df(df, x_col, y_col, where=None):
            if df is None or df.empty:
                return pd.DataFrame()

            if x_col not in df.columns or y_col not in df.columns:
                return pd.DataFrame()

            out = df.copy()

            if where:
                try:
                    out = out.query(where, engine="python")
                except Exception as e:
                    print(f"[Datashader map] Layer filter failed: {where} -> {e}")

            out[x_col] = pd.to_numeric(out[x_col], errors="coerce")
            out[y_col] = pd.to_numeric(out[y_col], errors="coerce")
            out = out.dropna(subset=[x_col, y_col])

            return out

        # ---------------------------------------------------------
        # Collect global bounds from all visible layers
        # ---------------------------------------------------------
        bounds_x = []
        bounds_y = []

        prepared_layers = []

        for layer in layers:
            df_key = layer.get("df")
            df = df_map.get(df_key)

            if df is None or df.empty:
                continue

            x_col = layer.get("x_col")
            y_col = layer.get("y_col")
            where = layer.get("where")

            ldf = _clean_df(df, x_col, y_col, where)

            if ldf.empty:
                continue

            bounds_x.extend([ldf[x_col].min(), ldf[x_col].max()])
            bounds_y.extend([ldf[y_col].min(), ldf[y_col].max()])

            prepared_layers.append((layer, ldf))

        if show_preplot and rp_df is not None and not rp_df.empty:
            x_col = "Easting" if "Easting" in rp_df.columns else None
            y_col = "Northing" if "Northing" in rp_df.columns else None

            if x_col and y_col:
                tmp = _clean_df(rp_df, x_col, y_col)
                if not tmp.empty:
                    bounds_x.extend([tmp[x_col].min(), tmp[x_col].max()])
                    bounds_y.extend([tmp[y_col].min(), tmp[y_col].max()])

        if not bounds_x or not bounds_y:
            p = figure(
                title=title,
                width=plot_width,
                height=plot_height,
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )
            p.text(x=[0], y=[0], text=["No data for Datashader map"])
            return p if is_show else json_item(p)

        x_min, x_max = float(min(bounds_x)), float(max(bounds_x))
        y_min, y_max = float(min(bounds_y)), float(max(bounds_y))

        pad_x = max((x_max - x_min) * 0.03, 10.0)
        pad_y = max((y_max - y_min) * 0.03, 10.0)

        x_range = (x_min - pad_x, x_max + pad_x)
        y_range = (y_min - pad_y, y_max + pad_y)

        p = figure(
            title=title,
            width=plot_width,
            height=plot_height,
            x_range=Range1d(*x_range),
            y_range=Range1d(*y_range),
            match_aspect=True,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
        )

        p.xaxis.axis_label = "Easting"
        p.yaxis.axis_label = "Northing"

        if show_tiles:
            # Only use this if your coordinates are already Web Mercator.
            p.add_tile("CartoDB Positron")

        cvs = ds.Canvas(
            plot_width=canvas_width,
            plot_height=canvas_height,
            x_range=x_range,
            y_range=y_range,
        )

        legend_items = []

        # ---------------------------------------------------------
        # Optional preplot as normal Bokeh dots
        # ---------------------------------------------------------
        if show_preplot and rp_df is not None and not rp_df.empty:
            if "Easting" in rp_df.columns and "Northing" in rp_df.columns:
                pre = _clean_df(rp_df, "Easting", "Northing")
                if not pre.empty:
                    r = p.scatter(
                        x=pre["Easting"],
                        y=pre["Northing"],
                        size=2,
                        color="gray",
                        alpha=0.35,
                        marker="circle",
                        name="Preplot",
                    )
                    legend_items.append(LegendItem(label="Preplot", renderers=[r]))

        # ---------------------------------------------------------
        # Datashader raster layers
        # ---------------------------------------------------------
        for layer, ldf in prepared_layers:
            name = layer.get("name", "Layer")
            x_col = layer.get("x_col")
            y_col = layer.get("y_col")
            color = layer.get("color", "blue")
            alpha = float(layer.get("alpha", 0.9))
            color_col = layer.get("color_col")

            if color_col and color_col in ldf.columns:
                ldf[color_col] = ldf[color_col].astype(str).fillna("Unknown").astype("category")
                agg = cvs.points(ldf, x_col, y_col, agg=ds.count_cat(color_col))

                cats = list(ldf[color_col].cat.categories)
                palette = list(cc.glasbey_dark)
                color_key = {
                    cat: palette[i % len(palette)]
                    for i, cat in enumerate(cats)
                }

                img = tf.shade(
                    agg,
                    color_key=color_key,
                    how="eq_hist",
                )
            else:
                agg = cvs.points(ldf, x_col, y_col, agg=ds.count())
                img = tf.shade(
                    agg,
                    cmap=[color],
                    how="eq_hist",
                )

            img = tf.dynspread(img, threshold=0.5, max_px=2)

            rgba = np.asarray(img.data)

            renderer = p.image_rgba(
                image=[rgba],
                x=x_range[0],
                y=y_range[0],
                dw=x_range[1] - x_range[0],
                dh=y_range[1] - y_range[0],
                alpha=alpha,
                name=name,
            )

            legend_items.append(LegendItem(label=name, renderers=[renderer]))

        if legend_items:
            legend = Legend(items=legend_items, location="top_left", click_policy="hide")
            p.add_layout(legend, "right")

        if save_html_path:
            output_file(str(save_html_path), title=title)
            save(p)

        return p if is_show else json_item(p)
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
            show_sm: bool = False,  # NEW
            is_show: bool = False,
            jason_item: bool = False,  # kept your flag name
    ):
        """
        Multi-layer map (dsr / sm / rec_db) with:
          - per-layer pandas query filter: where="ROV.isna() or ROV == ''" (<> supported)
          - per-layer coloring:
              * categorical: color_col="ROV"
              * numeric grouped: color_col="TodayDaysInWater" + bins=8 (or intervals=[[0,10],...])
          - integer interval labels when bins are used (0–10, 11–20, ...)

        Layer dict keys (important ones):
          {
            "df": "dsr" | "sm" | "rec" | "rec_db",
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

        SM layer:
          - df="sm"
          - source is dsr_df filtered by Status == "Deployed"
          - appears wherever you place it in layers order
        """
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

            if show_sm:
                """
                layers.append(
                    dict(
                        df="sm",
                        name="SM Deployed",
                        x_col="PrimaryEasting",
                        y_col="PrimaryNorthing",
                        marker="circle",
                        size=6,
                        alpha=0.9,
                        color="deepskyblue",
                    )
                )
               """
                pass
            layers.append(
                dict(
                    df="rec",
                    name="REC_DB",
                    x_col="Easting",
                    y_col="Northing",
                    marker="circle",
                    size=6,
                    alpha=0.9,
                    color="green",
                )
            )

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

        # ---- tiles
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

        # ---- RPPreplot layer
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

        # ---- NEW: build SM dataframe from DSR where Status == "Deployed"
        sm_df = None
        if show_sm and dsr_df is not None and len(dsr_df) > 0:
            sm_df = dsr_df.copy()
            if "Status" in sm_df.columns:
                sm_df = sm_df[sm_df["Status"] != None].copy()
            else:
                sm_df = sm_df.iloc[0:0].copy()

        # ---- DataFrame selector
        df_map = {
            "dsr": dsr_df,
            "sm": sm_df,  # NEW
            "survey_manager": sm_df,
            "rec": rec_db_df,
            "rec_db": rec_db_df,
            "recdb": rec_db_df,
        }

        layer_spinners = []
        used_legend_titles = []

        def _pick_palette(n: int, layer: dict):
            from bokeh.palettes import Category10, Category20, Turbo256
            import numpy as np

            palette_colors = layer.get("palette_colors", None)
            palette_raw = layer.get("palette", None)

            if isinstance(palette_colors, (list, tuple)) and len(palette_colors) > 0:
                pal = list(palette_colors)
                return (pal * ((n // len(pal)) + 1))[:n]

            if isinstance(palette_raw, (list, tuple)) and len(palette_raw) > 0:
                pal = list(palette_raw)
                return (pal * ((n // len(pal)) + 1))[:n]

            if isinstance(palette_raw, str):
                name = palette_raw.strip().upper()

                if name == "TURBO256":
                    if n <= 1:
                        return [Turbo256[0]]
                    idx = np.linspace(0, 255, n).round().astype(int)
                    return [Turbo256[i] for i in idx]

                if name == "CATEGORY10":
                    if n <= 10:
                        return Category10[10][:n]
                    return (Category10[10] * ((n // 10) + 1))[:n]

                if name == "CATEGORY20":
                    if n <= 20:
                        return Category20[20][:n]
                    return (Category20[20] * ((n // 20) + 1))[:n]

            if n <= 10:
                return Category10[10][:n]
            if n <= 20:
                return Category20[20][:n]
            return (Category20[20] * ((n // 20) + 1))[:n]

        def _build_grouped_numeric_column(df: pd.DataFrame, col: str, layer: dict, suffix: str) -> str:
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

            if bin_method in ("quantile", "q", "Q", "qcut"):
                try:
                    cats = pd.qcut(s_num, q=bins_n, duplicates="drop")
                except ValueError:
                    cats = pd.cut(s_num, bins=bins_n, include_lowest=include_lowest)
            else:
                cats = pd.cut(s_num, bins=bins_n, include_lowest=include_lowest)

            labels = []
            prev_right = None
            for idx, interval in enumerate(cats.cat.categories):
                il = int(np.floor(interval.left))
                ir = int(np.ceil(interval.right))

                if idx == 0 and include_lowest:
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

        # ---- Build each layer
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

            if where:
                where_clean = where.replace("<>", "!=")
                try:
                    df = df.query(where_clean, engine="python")
                except Exception as e:
                    raise ValueError(
                        f"Invalid where filter in layer '{layer_name}': {where}\n{e}"
                    )

            df = df.dropna(subset=[x_col, y_col]).copy()
            if len(df) == 0:
                continue

            mx_col = f"__mx_{i}"
            my_col = f"__my_{i}"

            if transformer is not None:
                mx, my = transformer.transform(df[x_col].values, df[y_col].values)
                df[mx_col] = mx
                df[my_col] = my
            else:
                df[mx_col] = df[x_col]
                df[my_col] = df[y_col]

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
                legend_label=layer_name,
            )

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
            return None

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
                planned_nodes = int(conn.execute(
                    "SELECT COALESCE(SUM(Points), 0) FROM RLPreplot"
                ).fetchone()[0] or 0)
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
            df["Cumulative"] = df["Total"].cumsum()
            df["ProgressPct"] = (
                df["Cumulative"] * 100.0 / planned_nodes
                if planned_nodes > 0 else 0.0
            )
            max_total = float(df["Total"].max()) if len(df) else 0.0

            completed_nodes = int(df["Cumulative"].iloc[-1])
            elapsed_days = max(1, len(df))
            average_speed = completed_nodes / elapsed_days
            remaining_nodes = max(0, planned_nodes - completed_nodes)
            if planned_nodes <= 0:
                eoj_text = "Preplot total unavailable"
            elif remaining_nodes == 0:
                eoj_text = df["ProdDate"].iloc[-1].strftime("%d/%m/%Y")
            elif average_speed > 0:
                days_left = int(math.ceil(remaining_nodes / average_speed))
                eoj_text = (df["ProdDate"].iloc[-1] + pd.Timedelta(days=days_left)).strftime("%d/%m/%Y")
            else:
                eoj_text = "Not available"
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
                title="Deployment Day by Day — Progress v2",
                toolbar_location="left",
                x_axis_type="datetime",
                x_axis_label="Days",
                y_axis_label="Total Nodes",
                width_policy="max",
                height=360,
                y_range=(0, max_total * 1.25 if max_total > 0 else 1),
            )
            p.min_border_right = 70

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

            # Cumulative completion against the complete receiver preplot.
            p.extra_y_ranges = {"progress": Range1d(start=0, end=100)}
            p.add_layout(LinearAxis(
                y_range_name="progress",
                axis_label="Progress (% of Preplot)",
                formatter=NumeralTickFormatter(format="0.0"),
            ), "right")
            p.line(
                x="ProdDate", y="ProgressPct", source=df,
                y_range_name="progress", line_color="#111111", line_width=2,
            )
            progress_points = p.scatter(
                x="ProdDate", y="ProgressPct", source=df,
                y_range_name="progress", marker="circle", size=7,
                color="#111111", line_color="white", line_width=1,
                legend_label="Deployment progress",
            )
            p.add_tools(HoverTool(
                renderers=[progress_points],
                tooltips=[
                    ("Date", "@ProdDate{%d/%m/%Y}"),
                    ("Completed", "@Cumulative{0,0} nodes"),
                    ("Progress", "@ProgressPct{0.0}%"),
                ],
                formatters={"@ProdDate": "datetime"},
                mode="mouse",
            ))

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
            summary = Div(height=32, text=(
                f"<b>Deployment:</b> {completed_nodes:,} / {planned_nodes:,} nodes "
                f"({(completed_nodes * 100 / planned_nodes if planned_nodes else 0):.1f}%)"
                f"&nbsp;&nbsp;|&nbsp;&nbsp;<b>Average speed:</b> {average_speed:,.1f} nodes/calendar day"
                f"&nbsp;&nbsp;|&nbsp;&nbsp;<b>Predicted EOJ:</b> {eoj_text}"
            ))
            layout = column(summary, p, sizing_mode="stretch_both")

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
                planned_nodes = int(conn.execute(
                    "SELECT COALESCE(SUM(Points), 0) FROM RLPreplot"
                ).fetchone()[0] or 0)
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
            df["Cumulative"] = df["Total"].cumsum()
            df["ProgressPct"] = (
                df["Cumulative"] * 100.0 / planned_nodes
                if planned_nodes > 0 else 0.0
            )
            max_total = float(df["Total"].max()) if len(df) else 0.0

            completed_nodes = int(df["Cumulative"].iloc[-1])
            elapsed_days = max(1, len(df))
            average_speed = completed_nodes / elapsed_days
            remaining_nodes = max(0, planned_nodes - completed_nodes)
            if planned_nodes <= 0:
                eoj_text = "Preplot total unavailable"
            elif remaining_nodes == 0:
                eoj_text = df["ProdDate"].iloc[-1].strftime("%d/%m/%Y")
            elif average_speed > 0:
                days_left = int(math.ceil(remaining_nodes / average_speed))
                eoj_text = (df["ProdDate"].iloc[-1] + pd.Timedelta(days=days_left)).strftime("%d/%m/%Y")
            else:
                eoj_text = "Not available"
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
                title="Recovery Day by Day — Progress v2",
                toolbar_location="left",
                x_axis_type="datetime",
                x_axis_label="Days",
                y_axis_label="Total Nodes",
                width_policy="max",
                height=360,
                y_range=(0, max_total * 1.25 if max_total > 0 else 1),
            )
            p.min_border_right = 70

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

            # Cumulative completion against the complete receiver preplot.
            p.extra_y_ranges = {"progress": Range1d(start=0, end=100)}
            p.add_layout(LinearAxis(
                y_range_name="progress",
                axis_label="Progress (% of Preplot)",
                formatter=NumeralTickFormatter(format="0.0"),
            ), "right")
            p.line(
                x="ProdDate", y="ProgressPct", source=df,
                y_range_name="progress", line_color="#111111", line_width=2,
            )
            progress_points = p.scatter(
                x="ProdDate", y="ProgressPct", source=df,
                y_range_name="progress", marker="circle", size=7,
                color="#111111", line_color="white", line_width=1,
                legend_label="Recovery progress",
            )
            p.add_tools(HoverTool(
                renderers=[progress_points],
                tooltips=[
                    ("Date", "@ProdDate{%d/%m/%Y}"),
                    ("Completed", "@Cumulative{0,0} nodes"),
                    ("Progress", "@ProgressPct{0.0}%"),
                ],
                formatters={"@ProdDate": "datetime"},
                mode="mouse",
            ))

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

            summary = Div(height=32, text=(
                f"<b>Recovery:</b> {completed_nodes:,} / {planned_nodes:,} nodes "
                f"({(completed_nodes * 100 / planned_nodes if planned_nodes else 0):.1f}%)"
                f"&nbsp;&nbsp;|&nbsp;&nbsp;<b>Average speed:</b> {average_speed:,.1f} nodes/calendar day"
                f"&nbsp;&nbsp;|&nbsp;&nbsp;<b>Predicted EOJ:</b> {eoj_text}"
            ))
            layout = column(summary, p, sizing_mode="stretch_both")

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
                message="Database query error while reading DSR / DEPLOY_ROV_Summary / RPPreplot.",
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
            template="plotly_dark",
    ):
        """
        Universal Sunburst (3 layers):
          Ring 1: Baseline = RPPreplot COUNT(*)
          Ring 2: Total(metric) vs Remaining = baseline - SUM(metric)
          Ring 3: metric by ROV

        Notes
        -----
        Plotly Sunburst with branchvalues="total" requires:
            parent >= sum(children)

        So when Total > Baseline, this function automatically expands the root
        displayed value to max(Baseline, Total) so the chart still renders.
        The real baseline is still shown in the center annotation.

        Parameters
        ----------
        metric : str
            Column name from DEPLOY_ROV_Summary
            (e.g. "Stations", "RECStations", "Nodes", "RECNodes", ...)
        title : str | None
            Optional plot title override. If None -> auto based on metric.
        labels : dict | None
            Optional label overrides:
              {
                "baseline": "Baseline",
                "total": "Deployment",
                "remaining": "Remaining",
                "unit": "stations",
              }
        is_show : bool
            If True -> fig.show() and returns None
        json_return : bool
            If True -> returns fig.to_json()
        template : str
            Plotly template, e.g. "plotly_dark" or "plotly_white"

        Requires module-level:
            import pandas as pd
            import plotly.graph_objects as go
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

        m = str(metric).strip()
        m_upper = m.upper()

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

        lbl = {
            "baseline": "Baseline",
            "total": default_total,
            "remaining": "Remaining",
            "unit": m,
        }
        if isinstance(labels, dict):
            lbl.update({k: v for k, v in labels.items() if v is not None})

        if title is None:
            title = default_title

        # Recovery must be grouped by DSR.ROV1, not by the deployment ROV.
        # DEPLOY_ROV_Summary historically used COALESCE(ROV, ROV1) as its key,
        # which attributes recovered stations to ROV whenever both columns are
        # populated.  It also required SQLite to parse TimeStamp1 before a row
        # was counted, so legacy MM/DD/YYYY timestamps produced zero recovery.
        #
        # Read recovery metrics directly from DSR.  A non-empty TimeStamp1 is
        # sufficient to identify a recovered row; only RECDays needs a parsed
        # date.  This keeps the recovery donut correct for both existing and
        # newly imported project databases.
        recovery_metric_exprs = {
            "RECLines": "COUNT(DISTINCT NULLIF(TRIM(CAST(Line AS TEXT)), ''))",
            "RECStations": """
                COUNT(DISTINCT COALESCE(
                    NULLIF(TRIM(CAST(LinePoint AS TEXT)), ''),
                    TRIM(CAST(Line AS TEXT)) || ':' || TRIM(CAST(Station AS TEXT))
                ))
            """,
            "RECNodes": "COUNT(*)",
            "RECDays": """
                COUNT(DISTINCT COALESCE(
                    DATE(NULLIF(TRIM(Day1), '')),
                    DATE(TimeStamp1),
                    CASE
                        WHEN TRIM(TimeStamp1) GLOB
                             '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]*'
                        THEN DATE(
                            SUBSTR(TRIM(TimeStamp1), 7, 4) || '-' ||
                            SUBSTR(TRIM(TimeStamp1), 1, 2) || '-' ||
                            SUBSTR(TRIM(TimeStamp1), 4, 2)
                        )
                    END
                ))
            """,
        }

        if m in recovery_metric_exprs:
            sql_rov = f"""
            SELECT
                TRIM(ROV1) AS Rov,
                {recovery_metric_exprs[m]} AS Val
            FROM DSR
            WHERE ROV1 IS NOT NULL
              AND TRIM(ROV1) <> ''
              AND TimeStamp1 IS NOT NULL
              AND TRIM(TimeStamp1) <> ''
            GROUP BY TRIM(ROV1)
            ORDER BY TRIM(ROV1)
            """
        else:
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

            baseline_disp = format(int(round(baseline)), ",")
            total_disp = format(int(round(total_val)), ",")
            remaining_disp = format(int(round(remaining_val)), ",")

        except Exception as e:
            return self._plotly_error_html(
                title="Sunburst failed",
                message="Data preparation error.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=False,
            )

        try:
            root_label = lbl["baseline"]
            root_value = float(max(baseline, total_val))

            labels_sb = [root_label, lbl["total"]]
            parents_sb = ["", root_label]
            values_sb = [root_value, float(total_val)]

            if remaining_val > 0:
                labels_sb.append(lbl["remaining"])
                parents_sb.append(root_label)
                values_sb.append(float(remaining_val))

            labels_sb += rovs
            parents_sb += [lbl["total"]] * len(rovs)
            values_sb += rov_vals

            palette = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
                "#bcbd22", "#17becf"
            ]
            rov_colors = [palette[i % len(palette)] for i in range(len(rovs))]

            total_color_map = {
                "Deployment": "#2563eb",
                "Recovery": "#22c55e",
                "Processed": "#a855f7",
                "SM": "#f97316",
            }
            total_color = total_color_map.get(lbl["total"], "#2563eb")

            colors_sb = ["#0b1220", total_color]
            if remaining_val > 0:
                colors_sb.append("#e5e7eb")
            colors_sb += rov_colors

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
                    marker=dict(
                        colors=colors_sb,
                        line=dict(color="white", width=1)
                    ),
                    insidetextorientation="radial",
                    # The root occupies the centre of the Sunburst.  Its native
                    # label would overlap the custom centre summary below, so
                    # suppress only the root text and keep labels on all rings.
                    texttemplate=(
                        [""]
                        + ["%{label}<br>%{percentRoot:.0%}"]
                        * (len(labels_sb) - 1)
                    ),
                    # Reserve a real band below the chart for the horizontal
                    # legend.  Without an explicit domain Plotly enlarges the
                    # Sunburst into that band and clips the lower outer ring.
                    domain=dict(x=[0.03, 0.97], y=[0.18, 0.98]),
                    hovertemplate=(
                            "<b>%{label}</b><br>"
                            + f"{lbl['unit']}: %{{value:,.0f}}<br>"
                            + "Percent of root: %{percentRoot:.1%}<br>"
                            + "Percent of parent: %{percentParent:.1%}"
                            + "<extra></extra>"
                    ),
                )
            )

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

            if remaining_val > 0:
                fig.add_trace(go.Scatter(
                    x=[None], y=[None],
                    mode="markers",
                    marker=dict(size=10, color="#e5e7eb"),
                    name=lbl["remaining"],
                    showlegend=True
                ))

            if template == "plotly_dark":
                paper_bgcolor = "#111827"
                plot_bgcolor = "#111827"
                text_color = "white"
                center_text_color = "white"
            else:
                paper_bgcolor = "white"
                plot_bgcolor = "white"
                text_color = "black"
                center_text_color = "black"

            annotation_lines = [
                f"<b>{lbl['baseline']}</b><br>{baseline_disp}",
                f"<b>{lbl['total']}</b><br>{total_disp}",
            ]
            if remaining_val > 0:
                annotation_lines.append(f"<b>{lbl['remaining']}</b><br>{remaining_disp}")
            if over:
                annotation_lines.append(f"<b>Over baseline</b><br>{format(int(round(total_val - baseline)), ',')}")

            fig.update_layout(
                title=dict(
                    text=final_title,
                    x=0.02,
                    xanchor="left"
                ),

                autosize=True,

                margin=dict(
                    l=5,
                    r=5,
                    t=35,
                    b=5
                ),

                uniformtext=dict(
                    minsize=10,
                    mode="show"
                ),

                legend=dict(
                    orientation="h",
                    yanchor="middle",
                    y=0.075,
                    xanchor="center",
                    x=0.5,
                    font=dict(size=10),
                    itemsizing="constant"
                ),

                annotations=[
                    dict(
                        text="<br>".join(annotation_lines),
                        x=0.5,
                        # Centre of the explicit Sunburst y-domain above.
                        y=0.58,
                        showarrow=False,
                        align="center",
                        font=dict(
                            size=12,
                            color=center_text_color
                        ),
                    )
                ],

                # Hide fake scatter axes used for legends
                xaxis=dict(
                    showgrid=False,
                    zeroline=False,
                    showticklabels=False,
                    showline=False,
                    ticks="",
                    fixedrange=True
                ),

                yaxis=dict(
                    showgrid=False,
                    zeroline=False,
                    showticklabels=False,
                    showline=False,
                    ticks="",
                    fixedrange=True
                ),

                # Let Plotly resize dynamically
                height=None,
                width=None,

                template=template,

                paper_bgcolor=paper_bgcolor,
                plot_bgcolor=plot_bgcolor,

                font=dict(
                    color=text_color
                ),
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
                config={
                    "responsive": True
                },
                default_height="100%",
                default_width="100%"
            )

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

    def build_line_summary_qc_grid(self, df, json_export: bool = True, is_show: bool = True, ncols: int = 2):
        """
        Build Bokeh QC grid from V_DSR_LineSummary dataframe.

        Requirements:
          - df must contain column: Line
          - metric columns should exist for each group (Avg*, Min*, Max*)

        Plots:
          - One plot per metric group
          - Max = red vbar (back)
          - Avg = green line + green points (both toggle together)
          - Min = blue vbar (front)
          - One GLOBAL legend at top (Max/Avg/Min). Clicking hides/shows across ALL plots.
          - Shared X axis (Line) for all plots
          - sizing_mode="stretch_both"
          - Y axis label everywhere: "Meters"

        Returns:
          - json_item(layout, "dsr_line_summary_qc") if json_export else layout
        """

        if df is None or getattr(df, "empty", True):
            layout = column(sizing_mode="stretch_both")
            if is_show:
                show(layout)
            return json_item(layout, "dsr_line_summary_qc") if json_export else layout

        if "Line" not in df.columns:
            raise ValueError("DataFrame must contain 'Line' column.")

        d = df.copy()

        # X factors
        d["Line"] = d["Line"].astype(str)

        # Sort by numeric Line if possible
        try:
            d["_LineNum"] = d["Line"].astype(float)
            d = d.sort_values("_LineNum")
        except Exception:
            d = d.sort_values("Line")

        x_factors = d["Line"].tolist()
        x_range = FactorRange(*x_factors)
        source = ColumnDataSource(d)

        # Each group (title, avg, min, max)
        groups = [
            ("DeltaE", "AvgDeltaE", "MinDeltaE", "MaxDeltaE"),
            ("DeltaN", "AvgDeltaN", "MinDeltaN", "MaxDeltaN"),
            ("DeltaE1", "AvgDeltaE1", "MinDeltaE1", "MaxDeltaE1"),
            ("DeltaN1", "AvgDeltaN1", "MinDeltaN1", "MaxDeltaN1"),
            ("Sigma", "AvgSigma", "MinSigma", "MaxSigma"),
            ("Sigma1", "AvgSigma1", "MinSigma1", "MaxSigma1"),
            ("Sigma2", "AvgSigma2", "MinSigma2", "MaxSigma2"),
            ("Sigma3", "AvgSigma3", "MinSigma3", "MaxSigma3"),
            ("Radial Offset", "AvgRadOffset", "MinRadOffset", "MaxRadOffset"),
            ("Range Prim→Sec", "AvgRangePrimToSec", "MinRangePrimToSec", "MaxRangePrimToSec"),
        ]

        # --- Collect renderers across all plots so global legend can toggle them ---
        max_renderers = []
        min_renderers = []
        avg_line_renderers = []
        avg_point_renderers = []

        def _make_triplet_plot(title, avg_col, min_col, max_col):
            # Skip if none of the columns exist
            if avg_col not in d.columns and min_col not in d.columns and max_col not in d.columns:
                return None

            p = figure(
                title=title,
                x_range=x_range,
                sizing_mode="stretch_both",
                toolbar_location="above",
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )

            p.xaxis.axis_label = "Line"
            p.yaxis.axis_label = "Meters"
            p.xaxis.major_label_orientation = 1.0
            p.xgrid.grid_line_alpha = 0.15
            p.ygrid.grid_line_alpha = 0.15

            # Hover shows all three values
            p.add_tools(
                HoverTool(
                    tooltips=[
                        ("Line", "@Line"),
                        ("Min", f"@{{{min_col}}}{{0.00}}" if min_col in d.columns else ""),
                        ("Avg", f"@{{{avg_col}}}{{0.00}}" if avg_col in d.columns else ""),
                        ("Max", f"@{{{max_col}}}{{0.00}}" if max_col in d.columns else ""),
                    ]
                )
            )

            # Max (red) behind
            if max_col in d.columns:
                r_max = p.vbar(
                    x="Line",
                    top=max_col,
                    source=source,
                    width=0.82,
                    alpha=0.25,
                    line_alpha=0.0,
                    color="red",
                )
                max_renderers.append(r_max)

            # Min (blue) in front
            if min_col in d.columns:
                r_min = p.vbar(
                    x="Line",
                    top=min_col,
                    source=source,
                    width=0.55,
                    alpha=0.65,
                    line_alpha=0.0,
                    color="blue",
                )
                min_renderers.append(r_min)

            # Avg (green) line + points
            if avg_col in d.columns:
                r_avg_line = p.line(
                    x="Line",
                    y=avg_col,
                    source=source,
                    line_width=2,
                    color="green",
                )
                r_avg_pts = p.circle(
                    x="Line",
                    y=avg_col,
                    source=source,
                    size=6,
                    color="green",
                )
                avg_line_renderers.append(r_avg_line)
                avg_point_renderers.append(r_avg_pts)

            # No per-plot legend (we use a global legend at the top)
            p.legend.visible = False

            return p

        plots = []
        for title, avg_c, min_c, max_c in groups:
            p = _make_triplet_plot(title, avg_c, min_c, max_c)
            if p is not None:
                plots.append(p)

        if not plots:
            layout = column(sizing_mode="stretch_both")
            if is_show:
                show(layout)
            return json_item(layout, "dsr_line_summary_qc") if json_export else layout

        # -----------------------------
        # Global legend (top)
        # -----------------------------
        # We create a tiny "legend-only" figure with dummy renderers.
        # Clicking legend hides those dummy renderers, and CustomJS propagates
        # the visibility to ALL actual renderers in all plots.
        legend_fig = figure(
            height=60,
            sizing_mode="stretch_width",
            toolbar_location=None,
            x_range=(0, 1),
            y_range=(0, 1),
        )
        legend_fig.outline_line_alpha = 0
        legend_fig.grid.visible = False
        legend_fig.axis.visible = False

        # Dummy renderers (invisible by being off-canvas-ish + minimal)
        dummy_max = legend_fig.line([0, 0.001], [0, 0], line_width=6, color="red")
        dummy_avg_line = legend_fig.line([0, 0.001], [0.2, 0.2], line_width=3, color="green")
        dummy_avg_pts = legend_fig.circle([0], [0.2], size=8, color="green")
        dummy_min = legend_fig.line([0, 0.001], [0.4, 0.4], line_width=6, color="blue")

        # Legend items: Avg should toggle BOTH line+points together => use both dummy renderers
        legend = Legend(
            items=[
                LegendItem(label="Max", renderers=[dummy_max]),
                LegendItem(label="Avg", renderers=[dummy_avg_line, dummy_avg_pts]),
                LegendItem(label="Min", renderers=[dummy_min]),
            ],
            orientation="horizontal",
            location="center",
            click_policy="hide",
        )
        legend_fig.add_layout(legend, "center")

        # Propagate toggles to all plots when dummy renderer visibility changes
        # Max
        dummy_max.js_on_change(
            "visible",
            CustomJS(
                args=dict(renderers=max_renderers, dummy=dummy_max),
                code="for (const r of renderers) { r.visible = dummy.visible; }",
            ),
        )
        # Min
        dummy_min.js_on_change(
            "visible",
            CustomJS(
                args=dict(renderers=min_renderers, dummy=dummy_min),
                code="for (const r of renderers) { r.visible = dummy.visible; }",
            ),
        )
        # Avg (line + points)
        dummy_avg_line.js_on_change(
            "visible",
            CustomJS(
                args=dict(lines=avg_line_renderers, pts=avg_point_renderers, dummy=dummy_avg_line),
                code="""
                    for (const r of lines) { r.visible = dummy.visible; }
                    for (const r of pts)   { r.visible = dummy.visible; }
                """,
            ),
        )
        dummy_avg_pts.js_on_change(
            "visible",
            CustomJS(
                args=dict(lines=avg_line_renderers, pts=avg_point_renderers, dummy=dummy_avg_pts),
                code="""
                    for (const r of lines) { r.visible = dummy.visible; }
                    for (const r of pts)   { r.visible = dummy.visible; }
                """,
            ),
        )

        # -----------------------------
        # Grid layout + global legend on top
        # -----------------------------
        grid = gridplot(plots, ncols=ncols, sizing_mode="stretch_both", toolbar_location="above", merge_tools=True)
        layout = column(legend_fig, grid, sizing_mode="stretch_both")

        if is_show:
            show(layout)

        return json_item(layout, "dsr_line_summary_qc") if json_export else layout

    def make_dsr_rov_status_map(
            self,
            lines=None,
            solution_fk=1,
            mode="deployment",  # "deployment" or "recovery"
            show_preplot=True,
            show_shapes=True,
            show_layers=True,
            show_tiles=None,
            is_show=False,
            jason_item=False,
    ):
        """
        Dynamic Bokeh ROV status map.

        Deployment:
          - color by DSR.ROV
          - coordinate: PrimaryEasting / PrimaryNorthing
          - From/To day dropdowns use TimeStamp

        Recovery:
          - color by DSR.ROV1
          - coordinate: PrimaryEasting1 / PrimaryNorthing1
          - From/To day dropdowns use TimeStamp1
          - only rows where ROV1 is not empty
        """

        dsr_df = self.read_dsr(lines=lines, solution_fk=solution_fk)
        rp_df = self.read_rp_preplot(lines=lines) if show_preplot else None

        if dsr_df is None or dsr_df.empty:
            return self._error_layout(
                title="No DSR data",
                message="DSR table returned no rows for selected line/filter.",
                level="warning",
                is_show=is_show,
            )

        mode = str(mode or "deployment").lower().strip()

        if mode in ("deployment", "deploy", "dep"):
            rov_col = "ROV"
            time_col = "TimeStamp"
            x_col = "PrimaryEasting"
            y_col = "PrimaryNorthing"
            title = "Deployment by ROV"
            layer_name = "Deployment"
            marker = "circle"

            if rov_col not in dsr_df.columns:
                raise ValueError(f"Column '{rov_col}' not found in DSR dataframe.")

            dsr_df[rov_col] = (
                dsr_df[rov_col]
                .fillna("")
                .astype(str)
                .str.strip()
                .replace("", "Unknown")
            )

        elif mode in ("recovery", "recover", "rec"):
            rov_col = "ROV1"
            time_col = "TimeStamp1"
            x_col = "PrimaryEasting1"
            y_col = "PrimaryNorthing1"
            title = "Recovery by ROV"
            layer_name = "Recovery"
            marker = "triangle"

            if rov_col not in dsr_df.columns:
                raise ValueError(f"Column '{rov_col}' not found in DSR dataframe.")

            dsr_df[rov_col] = dsr_df[rov_col].fillna("").astype(str).str.strip()
            dsr_df = dsr_df[dsr_df[rov_col] != ""].copy()

            if dsr_df.empty:
                return self._error_layout(
                    title="No Recovery Data",
                    message="No rows with ROV1 found in DSR table.",
                    level="warning",
                    is_show=is_show,
                )
        else:
            raise ValueError("mode must be 'deployment' or 'recovery'")

        required_cols = [x_col, y_col, time_col, rov_col]
        for c in required_cols:
            if c not in dsr_df.columns:
                raise ValueError(f"Column '{c}' not found in DSR dataframe.")

        dsr_df[x_col] = pd.to_numeric(dsr_df[x_col], errors="coerce")
        dsr_df[y_col] = pd.to_numeric(dsr_df[y_col], errors="coerce")
        dsr_df[time_col] = pd.to_datetime(dsr_df[time_col], errors="coerce")

        dsr_df = dsr_df.dropna(subset=[x_col, y_col, time_col]).copy()

        if dsr_df.empty:
            return self._error_layout(
                title="No valid DSR data",
                message=f"No valid rows after checking {x_col}, {y_col}, and {time_col}.",
                level="warning",
                is_show=is_show,
            )

        dsr_df["_MapTime"] = dsr_df[time_col].dt.strftime("%Y-%m-%d %H:%M:%S")
        dsr_df["_day"] = dsr_df[time_col].dt.strftime("%Y-%m-%d")

        if "Line" not in dsr_df.columns:
            dsr_df["Line"] = ""
        if "Station" not in dsr_df.columns:
            dsr_df["Station"] = ""
        if "Node" not in dsr_df.columns:
            dsr_df["Node"] = ""
        if "ROV" not in dsr_df.columns:
            dsr_df["ROV"] = ""
        if "ROV1" not in dsr_df.columns:
            dsr_df["ROV1"] = ""
        if "Status" not in dsr_df.columns:
            dsr_df["Status"] = ""

        show_tiles = bool(getattr(self.cfg, "use_tiles", False)) if show_tiles is None else bool(show_tiles)

        transformer = None
        if show_tiles and getattr(self.cfg, "default_epsg", None):
            transformer = Transformer.from_crs(
                f"EPSG:{self.cfg.default_epsg}",
                "EPSG:3857",
                always_xy=True,
            )

        if transformer is not None:
            mx, my = transformer.transform(dsr_df[x_col].values, dsr_df[y_col].values)
            dsr_df["__mx"] = mx
            dsr_df["__my"] = my
        else:
            dsr_df["__mx"] = dsr_df[x_col]
            dsr_df["__my"] = dsr_df[y_col]

        p = figure(
            title=title,
            sizing_mode="stretch_both",
            x_axis_type="mercator" if show_tiles else "linear",
            y_axis_type="mercator" if show_tiles else "linear",
            match_aspect=getattr(self.cfg, "match_aspect", True),
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
        )

        if show_tiles:
            vendor = getattr(self.cfg, "tile_vendor", "CARTODB_POSITRON")
            provider = {
                "CARTODB_POSITRON": xyz.CartoDB.Positron,
                "CARTODB_DARK": xyz.CartoDB.DarkMatter,
                "OSM": xyz.OpenStreetMap.Mapnik,
                "ESRI_IMAGERY": xyz.Esri.WorldImagery,
            }.get(vendor, xyz.CartoDB.Positron)
            p.add_tile(provider)

        if show_shapes:
            self.add_project_shapes_layers(
                p,
                default_src_epsg=getattr(self.cfg, "default_epsg", None),
            )

        if show_layers:
            self.add_csv_layers_to_map(
                p,
                csv_epsg=getattr(self.cfg, "default_epsg", None),
                show_tiles=show_tiles,
            )

        r_rp = None

        if show_preplot and rp_df is not None and not rp_df.empty:
            rp = rp_df.copy()

            if "X" in rp.columns and "Y" in rp.columns:
                rp["X"] = pd.to_numeric(rp["X"], errors="coerce")
                rp["Y"] = pd.to_numeric(rp["Y"], errors="coerce")
                rp = rp.dropna(subset=["X", "Y"]).copy()

                if not rp.empty:
                    if transformer is not None:
                        mx, my = transformer.transform(rp["X"].values, rp["Y"].values)
                        rp["__mx"] = mx
                        rp["__my"] = my
                    else:
                        rp["__mx"] = rp["X"]
                        rp["__my"] = rp["Y"]

                    if "Line" not in rp.columns:
                        rp["Line"] = ""
                    if "Point" not in rp.columns:
                        rp["Point"] = ""

                    src_rp = ColumnDataSource(rp)

                    r_rp = p.scatter(
                        x="__mx",
                        y="__my",
                        marker="circle",
                        size=5,
                        alpha=0.75,
                        source=src_rp,
                        line_color="grey",
                        fill_color="grey",
                        legend_label=f"Receiver Preplot ({len(rp)})",
                    )

                    p.add_tools(
                        HoverTool(
                            renderers=[r_rp],
                            tooltips=[
                                ("Layer", "Preplot"),
                                ("Line", "@Line"),
                                ("Station", "@Point"),
                                ("E", "@X{0,0.00}"),
                                ("N", "@Y{0,0.00}"),
                            ],
                        )
                    )

        rovs = sorted(dsr_df[rov_col].dropna().astype(str).unique().tolist())

        if len(rovs) <= 10:
            colors = Category10[10][:len(rovs)]
        elif len(rovs) <= 20:
            colors = Category20[20][:len(rovs)]
        else:
            colors = (Category20[20] * ((len(rovs) // 20) + 1))[:len(rovs)]

        full_sources = []
        plot_sources = []
        dsr_renderers = []

        for rov_name, color in zip(rovs, colors):
            d = dsr_df[dsr_df[rov_col].astype(str) == str(rov_name)].copy()

            full_src = ColumnDataSource(d)
            plot_src = ColumnDataSource(d)

            r = p.scatter(
                x="__mx",
                y="__my",
                marker=marker,
                size=7,
                alpha=0.90,
                source=plot_src,
                line_color=color,
                fill_color=color,
                legend_label=f"{rov_name} ({len(d)})",
            )

            p.add_tools(
                HoverTool(
                    renderers=[r],
                    tooltips=[
                        ("Layer", layer_name),
                        ("Line", "@Line"),
                        ("Station", "@Station"),
                        ("Node", "@Node"),
                        ("Deploy ROV", "@ROV"),
                        ("Recovery ROV", "@ROV1"),
                        ("Status", "@Status"),
                        ("Time", "@_MapTime"),
                        (x_col, f"@{x_col}{{0,0.00}}"),
                        (y_col, f"@{y_col}{{0,0.00}}"),
                    ],
                )
            )

            full_sources.append(full_src)
            plot_sources.append(plot_src)
            dsr_renderers.append(r)

        days = sorted(dsr_df["_day"].dropna().unique().tolist())

        from_day_select = Select(
            title="From day",
            value=days[0],
            options=days,
            width=130,
        )

        to_day_select = Select(
            title="To day",
            value=days[-1],
            options=days,
            width=130,
        )

        rov_select = MultiChoice(
            title="Visible ROVs",
            value=rovs,
            options=rovs,
            width=250,
        )

        callback_code = """
            let fromDay = from_day_select.value;
            let toDay = to_day_select.value;

            if (fromDay > toDay) {
                const tmp = fromDay;
                fromDay = toDay;
                toDay = tmp;
            }

            const selected = new Set(rov_select.value);

            for (let s = 0; s < full_sources.length; s++) {
                const full = full_sources[s];
                const out = plot_sources[s];
                const rovName = rov_names[s];

                const keys = Object.keys(full.data);
                const newData = {};

                for (const k of keys) {
                    newData[k] = [];
                }

                if (selected.has(rovName)) {
                    const days = full.data["_day"];

                    for (let i = 0; i < days.length; i++) {
                        if (days[i] >= fromDay && days[i] <= toDay) {
                            for (const k of keys) {
                                newData[k].push(full.data[k][i]);
                            }
                        }
                    }
                }

                out.data = newData;
                out.change.emit();
            }
        """

        filter_cb = CustomJS(
            args=dict(
                from_day_select=from_day_select,
                to_day_select=to_day_select,
                rov_select=rov_select,
                full_sources=full_sources,
                plot_sources=plot_sources,
                rov_names=rovs,
            ),
            code=callback_code,
        )

        from_day_select.js_on_change("value", filter_cb)
        to_day_select.js_on_change("value", filter_cb)
        rov_select.js_on_change("value", filter_cb)

        reset_btn = Button(label="Reset", button_type="default", width=70)
        reset_btn.js_on_click(
            CustomJS(
                args=dict(
                    from_day_select=from_day_select,
                    to_day_select=to_day_select,
                    first_day=days[0],
                    last_day=days[-1],
                ),
                code="""
                    from_day_select.value = first_day;
                    to_day_select.value = last_day;
                """,
            )
        )

        select_all_btn = Button(label="All ROVs", button_type="primary", width=80)
        select_all_btn.js_on_click(
            CustomJS(
                args=dict(rov_select=rov_select, rovs=rovs),
                code="rov_select.value = rovs;",
            )
        )

        clear_rovs_btn = Button(label="No ROVs", button_type="default", width=85)
        clear_rovs_btn.js_on_click(
            CustomJS(
                args=dict(rov_select=rov_select),
                code="rov_select.value = [];",
            )
        )

        if p.legend and len(p.legend) > 0:
            p.legend.click_policy = "hide"
            p.legend.location = "top_left"
            p.legend.visible = True
            p.legend.title = rov_col

        toggle_legend_btn = Button(label="Hide legend", button_type="primary", width=105)

        if p.legend and len(p.legend) > 0:
            toggle_legend_btn.js_on_click(
                CustomJS(
                    args=dict(legend=p.legend[0], btn=toggle_legend_btn),
                    code="""
                        legend.visible = !legend.visible;
                        btn.label = legend.visible ? "Hide legend" : "Show legend";
                    """,
                )
            )
        else:
            toggle_legend_btn.disabled = True

        cycle_legend_pos_btn = Button(label="Legend pos", button_type="default", width=105)

        if p.legend and len(p.legend) > 0:
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
        else:
            cycle_legend_pos_btn.disabled = True

        sp_rp = Spinner(title="RP size", low=1, high=100, step=1, value=5, width=90)

        if r_rp is not None:
            sp_rp.js_on_change(
                "value",
                CustomJS(args=dict(r=r_rp), code="r.glyph.size = cb_obj.value;"),
            )
        else:
            sp_rp.disabled = True

        sp_dsr = Spinner(title="DSR size", low=1, high=100, step=1, value=7, width=95)
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

        info = Div(
            text=f"""
            <div style="font-size:12px; color:#666; white-space:nowrap;">
                <b>{layer_name}</b> by <b>{rov_col}</b>,
                date: <b>{time_col}</b>,
                rows: <b>{len(dsr_df)}</b>
            </div>
            """,
            width=260,
        )

        controls = row(
            info,
            from_day_select,
            to_day_select,
            rov_select,
            reset_btn,
            select_all_btn,
            clear_rovs_btn,
            toggle_legend_btn,
            cycle_legend_pos_btn,
            sp_rp,
            sp_dsr,
            sizing_mode="stretch_width",
        )

        layout = column(
            controls,
            p,
            sizing_mode="stretch_both",
        )

        if is_show:
            show(layout)
            return None

        if jason_item:
            return json_item(layout)

        return layout

    def make_dsr_deploy_speed_heading_map(
            self,
            line=None,
            solution_fk=1,
            rov=None,
            title=None,
            phase="deployment",
            show_preplot=True,
            show_shapes=True,
            show_layers=True,
            show_tiles=None,
            min_dt_sec=1,
            max_dt_min=120,
            rolling_window=3,
            arrow_size=14,
            segment_width=3,
            save_path=None,
            is_show=False,
            jason_item=False,
    ):
        """
        Interactive DSR deployment or recovery map.

        Features:
          - whole database if line=None
          - consecutive node speed/heading by Line + ROV
          - speed in knots
          - arrow marker shows heading
          - separate ROV legend series, hide/show by legend
          - date range filter
          - line filter
          - time playback slider
          - export to standalone HTML using save_path
        """

        lines = [line] if line is not None else None
        dsr = self.read_dsr(lines=lines, solution_fk=solution_fk)

        phase = str(phase or "deployment").strip().lower()
        if phase not in ("deployment", "recovery"):
            raise ValueError("phase must be 'deployment' or 'recovery'")
        if title is None:
            title = "DSR Recovery Speed / Heading Map" if phase == "recovery" else "DSR Deployment Speed / Heading Map"

        if dsr is None or dsr.empty:
            return self._error_layout(
                title="No DSR data",
                message="DSR table is empty.",
                level="warning",
                is_show=is_show,
                json_return=jason_item,
            )

        if phase == "recovery":
            source_columns = {
                "ROV1": "ROV",
                "TimeStamp1": "TimeStamp",
                "PrimaryEasting1": "PrimaryEasting",
                "PrimaryNorthing1": "PrimaryNorthing",
            }
        else:
            source_columns = {
                "ROV": "ROV",
                "TimeStamp": "TimeStamp",
                "PrimaryEasting": "PrimaryEasting",
                "PrimaryNorthing": "PrimaryNorthing",
            }

        required = ["Line", "Station", *source_columns.keys()]
        missing = [c for c in required if c not in dsr.columns]
        if missing:
            return self._error_layout(
                title="Missing DSR columns",
                message=f"Missing columns: {missing}",
                level="error",
                is_show=is_show,
                json_return=jason_item,
            )

        df = dsr.copy()
        if phase == "recovery":
            for source_col, canonical_col in source_columns.items():
                df[canonical_col] = df[source_col]

        if rov:
            df = df[df["ROV"].astype(str).str.strip().eq(str(rov).strip())]

        df["Line"] = pd.to_numeric(df["Line"], errors="coerce")
        df["Station"] = pd.to_numeric(df["Station"], errors="coerce")
        df["ROV"] = df["ROV"].astype(str).str.strip()
        df["TimeStamp"] = pd.to_datetime(df["TimeStamp"], errors="coerce")
        df["PrimaryEasting"] = pd.to_numeric(df["PrimaryEasting"], errors="coerce")
        df["PrimaryNorthing"] = pd.to_numeric(df["PrimaryNorthing"], errors="coerce")

        df = df.dropna(
            subset=["Line", "Station", "ROV", "TimeStamp", "PrimaryEasting", "PrimaryNorthing"]
        )

        if df.empty:
            return self._error_layout(
                title=f"No valid DSR {phase} data",
                message="No valid rows after cleaning.",
                level="warning",
                is_show=is_show,
                json_return=jason_item,
            )

        df["Line"] = df["Line"].astype(int)
        df["Station"] = df["Station"].astype(int)
        df["LineStr"] = df["Line"].astype(str)

        # Consecutive nodes per Line + ROV
        df = df.sort_values(["Line", "ROV", "TimeStamp", "Station"]).reset_index(drop=True)
        g = df.groupby(["Line", "ROV"], dropna=False)

        df["PrevEasting"] = g["PrimaryEasting"].shift(1)
        df["PrevNorthing"] = g["PrimaryNorthing"].shift(1)
        df["PrevTimeStamp"] = g["TimeStamp"].shift(1)
        df["PrevStation"] = g["Station"].shift(1)
        df["PrevNode"] = g["Node"].shift(1) if "Node" in df.columns else ""

        df["dE"] = df["PrimaryEasting"] - df["PrevEasting"]
        df["dN"] = df["PrimaryNorthing"] - df["PrevNorthing"]
        df["SegmentDistance_m"] = np.sqrt(df["dE"] ** 2 + df["dN"] ** 2)

        df["dt_sec"] = (df["TimeStamp"] - df["PrevTimeStamp"]).dt.total_seconds()
        df.loc[df["dt_sec"] < min_dt_sec, "dt_sec"] = np.nan
        df.loc[df["dt_sec"] > max_dt_min * 60, "dt_sec"] = np.nan

        # knots = m/s * 1.94384
        df["Speed_knots"] = (df["SegmentDistance_m"] / df["dt_sec"]) * 1.94384

        # Heading: 0=N, 90=E
        df["Heading_deg"] = (
                                    np.degrees(np.arctan2(df["dE"], df["dN"])) + 360.0
                            ) % 360.0

        df["AvgSpeed_knots"] = (
            df.groupby(["Line", "ROV"])["Speed_knots"]
            .transform(lambda s: s.rolling(rolling_window, min_periods=1).mean())
        )

        df["ArrowAngle"] = -np.deg2rad(df["Heading_deg"].fillna(0))
        df["TimeMS"] = (df["TimeStamp"].astype("int64") // 10 ** 6).astype("int64")

        show_tiles = bool(self.cfg.use_tiles) if show_tiles is None else bool(show_tiles)

        transformer = None
        if getattr(self.cfg, "default_epsg", None):
            transformer = Transformer.from_crs(
                f"EPSG:{self.cfg.default_epsg}",
                "EPSG:3857",
                always_xy=True,
            )

        if transformer is not None and show_tiles:
            df["__mx"], df["__my"] = transformer.transform(
                df["PrimaryEasting"].values,
                df["PrimaryNorthing"].values,
            )
            df["__prev_mx"], df["__prev_my"] = transformer.transform(
                df["PrevEasting"].fillna(df["PrimaryEasting"]).values,
                df["PrevNorthing"].fillna(df["PrimaryNorthing"]).values,
            )
        else:
            df["__mx"] = df["PrimaryEasting"]
            df["__my"] = df["PrimaryNorthing"]
            df["__prev_mx"] = df["PrevEasting"]
            df["__prev_my"] = df["PrevNorthing"]

        valid_speed = df["AvgSpeed_knots"].dropna()
        low = float(valid_speed.quantile(0.02)) if not valid_speed.empty else 0.0
        high = float(valid_speed.quantile(0.98)) if not valid_speed.empty else 1.0
        if high <= low:
            high = low + 0.1

        mapper = LinearColorMapper(
            palette=Turbo256,
            low=low,
            high=high,
            nan_color="#cccccc",
        )

        p = figure(
            title=title,
            sizing_mode="stretch_both",
            x_axis_type="mercator" if show_tiles else "linear",
            y_axis_type="mercator" if show_tiles else "linear",
            match_aspect=self.cfg.match_aspect,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
        )

        if show_tiles:
            vendor = getattr(self.cfg, "tile_vendor", "CARTODB_POSITRON")
            provider = {
                "CARTODB_POSITRON": xyz.CartoDB.Positron,
                "CARTODB_DARK": xyz.CartoDB.DarkMatter,
                "OSM": xyz.OpenStreetMap.Mapnik,
                "ESRI_IMAGERY": xyz.Esri.WorldImagery,
            }.get(vendor, xyz.CartoDB.Positron)
            p.add_tile(provider)

        if show_shapes:
            self.add_project_shapes_layers(
                p,
                default_src_epsg=getattr(self.cfg, "default_epsg", None),
            )

        if show_layers:
            self.add_csv_layers_to_map(
                p,
                csv_epsg=self.cfg.default_epsg,
                show_tiles=show_tiles,
            )

        legend_items = []
        all_arrow_renderers = []
        all_segment_renderers = []
        filter_targets = []

        for rov_name in sorted(df["ROV"].dropna().unique()):
            rov_df = df[df["ROV"] == rov_name].copy()

            full_pts = ColumnDataSource(rov_df)
            filt_pts = ColumnDataSource(rov_df)

            seg_df = rov_df.dropna(
                subset=["__prev_mx", "__prev_my", "__mx", "__my", "AvgSpeed_knots"]
            ).copy()

            full_seg = ColumnDataSource(seg_df)
            filt_seg = ColumnDataSource(seg_df)

            r_seg = p.segment(
                x0="__prev_mx",
                y0="__prev_my",
                x1="__mx",
                y1="__my",
                source=filt_seg,
                line_color={"field": "AvgSpeed_knots", "transform": mapper},
                line_width=segment_width,
                line_alpha=0.75,
            )

            r_pts = p.scatter(
                x="__mx",
                y="__my",
                source=filt_pts,
                marker="triangle",
                angle="ArrowAngle",
                size=arrow_size,
                fill_color={"field": "AvgSpeed_knots", "transform": mapper},
                line_color="black",
                line_width=0.5,
                fill_alpha=0.95,
            )

            all_arrow_renderers.append(r_pts)
            all_segment_renderers.append(r_seg)
            filter_targets.append([full_pts, filt_pts])
            filter_targets.append([full_seg, filt_seg])

            legend_items.append(
                LegendItem(label=str(rov_name), renderers=[r_seg, r_pts])
            )

            p.add_tools(
                HoverTool(
                    renderers=[r_pts],
                    tooltips=[
                        ("ROV", "@ROV"),
                        ("Line", "@Line"),
                        ("Station", "@Station"),
                        ("Prev Station", "@PrevStation"),
                        ("Node", "@Node"),
                        ("Prev Node", "@PrevNode"),
                        ("Time", "@TimeStamp{%F %T}"),
                        ("Speed", "@Speed_knots{0.00} kn"),
                        ("Avg speed", "@AvgSpeed_knots{0.00} kn"),
                        ("Heading", "@Heading_deg{0.0}°"),
                        ("Distance", "@SegmentDistance_m{0.00} m"),
                        ("dt", "@dt_sec{0.0} sec"),
                    ],
                    formatters={
                        "@TimeStamp": "datetime",
                        "@PrevTimeStamp": "datetime",
                    },
                )
            )

        legend = Legend(
            items=legend_items,
            location="top_left",
            click_policy="hide",
        )
        p.add_layout(legend, "right")

        color_bar = ColorBar(
            color_mapper=mapper,
            ticker=BasicTicker(),
            formatter=NumeralTickFormatter(format="0.00"),
            title="Avg speed, knots",
            location=(0, 0),
        )
        p.add_layout(color_bar, "right")

        min_time = int(df["TimeMS"].min())
        max_time = int(df["TimeMS"].max())

        line_options = sorted(df["LineStr"].unique().tolist(), key=lambda x: int(x))

        line_filter = MultiChoice(
            title="Line filter",
            value=[],
            options=line_options,
            width=360,
        )

        date_filter = DateRangeSlider(
            title="Date range",
            start=min_time,
            end=max_time,
            value=(min_time, max_time),
            step=60 * 1000,
            width=460,
        )

        time_slider = Slider(
            title="Playback time",
            start=min_time,
            end=max_time,
            value=max_time,
            step=60 * 1000,
            width=460,
        )

        play_btn = Button(label="▶ Play", width=100, button_type="success")

        sp_arrow = Spinner(
            title="Arrow size",
            low=4,
            high=40,
            step=1,
            value=arrow_size,
            width=130,
        )

        sp_line = Spinner(
            title="Segment width",
            low=1,
            high=15,
            step=1,
            value=segment_width,
            width=140,
        )

        filter_code = """
            const selectedLines = new Set(line_filter.value);
            const dateStart = date_filter.value[0];
            const dateEnd = date_filter.value[1];
            const playTime = time_slider.value;

            for (const pair of targets) {
                const full = pair[0].data;
                const filtSource = pair[1];

                const out = {};
                for (const k in full) {
                    out[k] = [];
                }

                const n = full["TimeMS"].length;

                for (let i = 0; i < n; i++) {
                    const lineOk = selectedLines.size === 0 || selectedLines.has(String(full["LineStr"][i]));
                    const t = full["TimeMS"][i];
                    const timeOk = t >= dateStart && t <= dateEnd && t <= playTime;

                    if (lineOk && timeOk) {
                        for (const k in full) {
                            out[k].push(full[k][i]);
                        }
                    }
                }

                filtSource.data = out;
                filtSource.change.emit();
            }
        """

        filter_cb = CustomJS(
            args=dict(
                targets=filter_targets,
                line_filter=line_filter,
                date_filter=date_filter,
                time_slider=time_slider,
            ),
            code=filter_code,
        )

        line_filter.js_on_change("value", filter_cb)
        date_filter.js_on_change("value", filter_cb)
        time_slider.js_on_change("value", filter_cb)

        play_btn.js_on_click(
            CustomJS(
                args=dict(
                    btn=play_btn,
                    time_slider=time_slider,
                    date_filter=date_filter,
                    timer_key=f"_dsr_{phase}_play_timer",
                ),
                code="""
                    if (window[timer_key]) {
                        clearInterval(window[timer_key]);
                        window[timer_key] = null;
                        btn.label = "▶ Play";
                        btn.button_type = "success";
                        return;
                    }

                    btn.label = "⏸ Pause";
                    btn.button_type = "warning";

                    const step = time_slider.step || 60000;

                    window[timer_key] = setInterval(function() {
                        let v = time_slider.value + step * 10;

                        const start = date_filter.value[0];
                        const end = date_filter.value[1];

                        if (v > end) {
                            v = start;
                        }

                        time_slider.value = v;
                        time_slider.change.emit();
                    }, 250);
                """,
            )
        )

        sp_arrow.js_on_change(
            "value",
            CustomJS(
                args=dict(renderers=all_arrow_renderers),
                code="""
                    for (const r of renderers) {
                        r.glyph.size = cb_obj.value;
                    }
                """,
            ),
        )

        sp_line.js_on_change(
            "value",
            CustomJS(
                args=dict(renderers=all_segment_renderers),
                code="""
                    for (const r of renderers) {
                        r.glyph.line_width = cb_obj.value;
                    }
                """,
            ),
        )

        controls = column(
            row(line_filter, date_filter, sizing_mode="stretch_width"),
            row(play_btn, time_slider, sp_arrow, sp_line, sizing_mode="stretch_width"),
            sizing_mode="stretch_width",
        )

        layout = column(controls, p, sizing_mode="stretch_both")

        if save_path:
            output_file(save_path, title=title)
            save(layout)

        if is_show:
            show(layout)
            return None

        if jason_item:
            return json_item(layout)

        return layout
    def make_dsr_primary_preplot_bullseye(
            self,
            dsr_df: Optional[pd.DataFrame] = None,
            *,
            lines: Optional[Iterable[int]] = None,
            solution_fk: Optional[int] = 1,
            title: str = "DSR Primary vs Preplot Bullseye",
            rov_col: str = "ROV",
            x_col: str = "PrimaryEasting",
            y_col: str = "PrimaryNorthing",
            pp_x_col: str = "PreplotEasting",
            pp_y_col: str = "PreplotNorthing",
            point_size: int = 7,
            circle_step: float = 2.0,
            max_radius: Optional[float] = None,
            show_p50_p95: bool = True,
            is_show: bool = False,
            json_return: bool = False,
            target_id: str = "dsr-primary-preplot-bullseye",
    ):
        """
        Bullseye QC plot for DSR deployment position vs preplot.

        Calculates:
            dE = PrimaryEasting  - PreplotEasting
            dN = PrimaryNorthing - PreplotNorthing
            RadialOffset = sqrt(dE² + dN²)

        Each point is colored by deployment ROV.
        """

        try:
            if dsr_df is None:
                dsr_df = self.read_dsr(
                    lines=lines,
                    solution_fk=solution_fk,
                )

            if dsr_df is None or dsr_df.empty:
                return self._error_layout(
                    title="Bullseye plot failed",
                    message="No DSR data found.",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            required_cols = [
                x_col, y_col, pp_x_col, pp_y_col,
                rov_col, "Line", "Station", "Node",
            ]

            missing = [c for c in required_cols if c not in dsr_df.columns]
            if missing:
                return self._error_layout(
                    title="Bullseye plot failed",
                    message="Missing required DSR columns.",
                    details=", ".join(missing),
                    level="error",
                    is_show=is_show,
                    json_return=json_return,
                )

            df = dsr_df.copy()

            for c in [x_col, y_col, pp_x_col, pp_y_col]:
                df[c] = pd.to_numeric(df[c], errors="coerce")

            df["dE"] = df[x_col] - df[pp_x_col]
            df["dN"] = df[y_col] - df[pp_y_col]
            df["RadialOffset"] = np.sqrt(df["dE"] ** 2 + df["dN"] ** 2)

            df[rov_col] = df[rov_col].fillna("Unknown").astype(str).str.strip()
            df.loc[df[rov_col] == "", rov_col] = "Unknown"

            df = df.dropna(subset=["dE", "dN", "RadialOffset"])

            if df.empty:
                return self._error_layout(
                    title="Bullseye plot failed",
                    message="No valid Primary/Preplot coordinates after cleaning.",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            # ---------------------------------------------------------
            # Plot limit
            # ---------------------------------------------------------
            auto_radius = float(np.nanmax(np.abs(df[["dE", "dN"]].to_numpy())))
            auto_radius = max(auto_radius, float(df["RadialOffset"].max()), 1.0)

            if max_radius is None:
                max_radius = math.ceil(auto_radius * 1.15)

            max_radius = float(max_radius)

            # ---------------------------------------------------------
            # Figure
            # ---------------------------------------------------------
            p = figure(
                title=title,
                sizing_mode="stretch_both",
                height=700,
                match_aspect=True,
                aspect_scale=1,
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
                x_range=Range1d(-max_radius, max_radius),
                y_range=Range1d(-max_radius, max_radius),
            )

            p.xaxis.axis_label = "dE = PrimaryEasting - PreplotEasting (m)"
            p.yaxis.axis_label = "dN = PrimaryNorthing - PreplotNorthing (m)"

            # ---------------------------------------------------------
            # Bullseye rings
            # ---------------------------------------------------------
            def _circle_xy(radius, n=240):
                ang = np.linspace(0, 2 * np.pi, n)
                return radius * np.cos(ang), radius * np.sin(ang)

            r = circle_step
            while r <= max_radius:
                xs, ys = _circle_xy(r)
                p.line(
                    xs,
                    ys,
                    line_color="gray",
                    line_alpha=0.35,
                    line_dash="dashed",
                    line_width=1,
                )

                p.text(
                    x=[r],
                    y=[0],
                    text=[f"{r:g} m"],
                    text_font_size="8pt",
                    text_color="gray",
                    text_alpha=0.8,
                )

                r += circle_step

            # Center cross
            p.line([-max_radius, max_radius], [0, 0], line_color="black", line_alpha=0.5)
            p.line([0, 0], [-max_radius, max_radius], line_color="black", line_alpha=0.5)

            # ---------------------------------------------------------
            # Optional percentile circles
            # ---------------------------------------------------------
            if show_p50_p95:
                for label, radius, color in [
                    ("P50", float(df["RadialOffset"].quantile(0.50)), "blue"),
                    ("P95", float(df["RadialOffset"].quantile(0.95)), "red"),
                ]:
                    if np.isfinite(radius) and radius > 0:
                        xs, ys = _circle_xy(radius)
                        p.line(
                            xs,
                            ys,
                            line_color=color,
                            line_alpha=0.75,
                            line_width=2,
                            legend_label=f"{label}: {radius:.2f} m",
                        )

            # ---------------------------------------------------------
            # Points by ROV
            # ---------------------------------------------------------
            rovs = sorted(df[rov_col].dropna().unique().tolist())

            if len(rovs) <= 10:
                palette = Category10[10]
            else:
                palette = Category20[20]

            for i, rov in enumerate(rovs):
                rdf = df[df[rov_col] == rov].copy()
                src = ColumnDataSource(rdf)

                renderer = p.scatter(
                    x="dE",
                    y="dN",
                    source=src,
                    size=point_size,
                    marker="circle",
                    fill_alpha=0.75,
                    line_alpha=0.9,
                    fill_color=palette[i % len(palette)],
                    line_color=palette[i % len(palette)],
                    legend_label=f"{rov} ({len(rdf)})",
                )

                p.add_tools(HoverTool(
                    renderers=[renderer],
                    tooltips=[
                        ("ROV", f"@{rov_col}"),
                        ("Line", "@Line"),
                        ("Station", "@Station"),
                        ("Node", "@Node"),
                        ("dE", "@dE{0.000} m"),
                        ("dN", "@dN{0.000} m"),
                        ("Radial", "@RadialOffset{0.000} m"),
                        ("Primary E", f"@{x_col}{{0,0.000}}"),
                        ("Primary N", f"@{y_col}{{0,0.000}}"),
                        ("Preplot E", f"@{pp_x_col}{{0,0.000}}"),
                        ("Preplot N", f"@{pp_y_col}{{0,0.000}}"),
                    ],
                ))

            p.legend.click_policy = "hide"
            p.legend.location = "top_left"

            p.grid.grid_line_alpha = 0.25
            p.outline_line_alpha = 0.4

            layout = column(p, sizing_mode="stretch_both")

            if is_show:
                show(layout)
                return None

            if json_return:
                return json_item(layout, target=target_id)

            return layout

        except Exception as e:
            return self._error_layout(
                title="Bullseye plot failed",
                message="Error while building DSR Primary vs Preplot bullseye plot.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

    def bullseye_dsr_vs_recdb(
            self,
            lines=None,
            solution_fk=1,
            compare_mode="deployment",
            color_by="ROV",
            max_offset=50,
            use_inline_xline=False,
            bins=50,
            point_size=7,
            title=None,
            plot_height=720,
            hist_ratio=0.16,
            left_hist_width=220,
            right_panel_width=260,
            is_show=False,
            json_return=False,
            save_html=None,
    ):
        """
        Bullseye QC dashboard comparing DSR position to REC_DB position.

        Layout:
            - Top histogram: dX / Xline
            - Left histogram: dY / Inline
            - Center: large bullseye plot
            - Right panel: ROV toggle + summary
            - Bottom: statistics table by ROV
        """

        compare_mode = (compare_mode or "deployment").lower().strip()
        lines_list = self._ensure_list(lines)

        sql = """
        SELECT
            d.Line,
            d.Station,
            d.Node,
            d.ROV,
            d.ROV1,
            d.PrimaryEasting,
            d.PrimaryNorthing,
            d.PrimaryElevation,
            d.PrimaryEasting1,
            d.PrimaryNorthing1,
            d.PrimaryElevation1,
            d.TimeStamp,
            d.TimeStamp1,
            r.REC_X,
            r.REC_Y,
            r.REC_Z,
            rp.LineBearing
        FROM DSR d
        LEFT JOIN REC_DB r
            ON d.Line = r.Line
           AND d.Station = r.Point
        LEFT JOIN RLPreplot rp
            ON d.Line = rp.Line
        WHERE 1=1
        """

        params = {}

        if solution_fk is not None:
            sql += " AND d.Solution_FK = :solution_fk"
            params["solution_fk"] = int(solution_fk)

        if lines_list:
            in_clause, p = self._sql_in_clause(lines_list, "ln")
            sql += f" AND d.Line IN {in_clause}"
            params.update(p)

        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=params)

        if df.empty:
            return self._error_layout(
                title="Bullseye failed",
                message="No matching DSR / REC_DB rows found.",
                level="warning",
                is_show=is_show,
                json_return=json_return,
            )

        if compare_mode == "recovery":
            df["X1"] = pd.to_numeric(df["PrimaryEasting1"], errors="coerce")
            df["Y1"] = pd.to_numeric(df["PrimaryNorthing1"], errors="coerce")
            df["Z1"] = pd.to_numeric(df["PrimaryElevation1"], errors="coerce")
            df["ROV_USED"] = df["ROV1"].fillna("Unknown").astype(str)
        else:
            df["X1"] = pd.to_numeric(df["PrimaryEasting"], errors="coerce")
            df["Y1"] = pd.to_numeric(df["PrimaryNorthing"], errors="coerce")
            df["Z1"] = pd.to_numeric(df["PrimaryElevation"], errors="coerce")
            df["ROV_USED"] = df["ROV"].fillna("Unknown").astype(str)

        df["X2"] = pd.to_numeric(df["REC_X"], errors="coerce")
        df["Y2"] = pd.to_numeric(df["REC_Y"], errors="coerce")
        df["Z2"] = pd.to_numeric(df["REC_Z"], errors="coerce")

        df = df.dropna(subset=["X1", "Y1", "X2", "Y2"]).copy()

        if df.empty:
            return self._error_layout(
                title="Bullseye failed",
                message="No valid coordinate pairs found.",
                level="warning",
                is_show=is_show,
                json_return=json_return,
            )

        df["dX"] = df["X2"] - df["X1"]
        df["dY"] = df["Y2"] - df["Y1"]
        df["dZ"] = df["Z2"] - df["Z1"]
        df["Radial"] = np.sqrt(df["dX"] ** 2 + df["dY"] ** 2)

        if use_inline_xline:
            bearings = pd.to_numeric(df["LineBearing"], errors="coerce").fillna(0)
            th = np.deg2rad(bearings)

            df["Inline"] = df["dX"] * np.sin(th) + df["dY"] * np.cos(th)
            df["Xline"] = df["dX"] * np.cos(th) + df["dY"] * (-np.sin(th))

            x_field = "Xline"
            y_field = "Inline"
            x_label = "Xline Offset (m)"
            y_label = "Inline Offset (m)"
        else:
            x_field = "dX"
            y_field = "dY"
            x_label = "dX / Delta Easting (m)"
            y_label = "dY / Delta Northing (m)"

        color_field = color_by if color_by in df.columns else "ROV_USED"
        df[color_field] = df[color_field].fillna("Unknown").astype(str)

        rovs = sorted(df[color_field].unique().tolist())
        palette = Category10[10] if len(rovs) <= 10 else Category20[20]
        color_map = {rov: palette[i % len(palette)] for i, rov in enumerate(rovs)}

        hist_height = max(95, int(plot_height * hist_ratio))

        # ---------------------------------------------------------
        # Main bullseye
        # ---------------------------------------------------------
        p = figure(
            title=title or f"Bullseye DSR vs REC_DB ({compare_mode})",
            height=plot_height,
            sizing_mode="stretch_both",
            match_aspect=True,
            aspect_scale=1,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
            x_range=(-max_offset, max_offset),
            y_range=(-max_offset, max_offset),
        )

        p.xaxis.axis_label = x_label
        p.yaxis.axis_label = y_label
        p.title.text_font_size = "14pt"
        p.axis.axis_label_text_font_size = "11pt"

        for radius in [1, 2, 5, 10, 20, 30, 40, 50]:
            if radius <= max_offset:
                p.circle(
                    x=0,
                    y=0,
                    radius=radius,
                    fill_alpha=0,
                    line_alpha=0.25,
                    line_dash="dashed",
                    line_color="gray",
                )
                p.text(
                    x=[1.0],
                    y=[radius],
                    text=[f"{radius} m"],
                    text_font_size="8pt",
                    text_color="#555555",
                )

        p.add_layout(Span(location=0, dimension="width", line_dash="dashed", line_color="black", line_alpha=0.65))
        p.add_layout(Span(location=0, dimension="height", line_dash="dashed", line_color="black", line_alpha=0.65))

        # ---------------------------------------------------------
        # Histograms
        # ---------------------------------------------------------
        p_top = figure(
            title=f"{x_label} Histogram",
            height=hist_height,
            sizing_mode="stretch_width",
            x_range=p.x_range,
            toolbar_location=None,
        )
        p_top.yaxis.axis_label = "Count"
        p_top.xaxis.visible = False
        p_top.title.text_font_size = "12pt"

        p_left = figure(
            title=f"{y_label} Histogram",
            width=left_hist_width,
            height=plot_height,
            y_range=p.y_range,
            toolbar_location=None,
        )
        p_left.xaxis.axis_label = "Count"
        p_left.yaxis.visible = False
        p_left.title.text_font_size = "11pt"

        scatter_renderers = []
        toggle_renderers = []
        legend_items = []
        checkbox_labels = []

        # stacked histogram accumulators
        x_edges = np.linspace(-max_offset, max_offset, bins + 1)
        y_edges = np.linspace(-max_offset, max_offset, bins + 1)
        x_stack = np.zeros(bins)
        y_stack = np.zeros(bins)

        for rov in rovs:
            sub = df[df[color_field] == rov].copy()
            src = ColumnDataSource(sub)
            color = color_map[rov]

            scatter_r = p.scatter(
                x=x_field,
                y=y_field,
                source=src,
                size=point_size,
                alpha=0.85,
                fill_color=color,
                line_color=color,
                legend_label=str(rov),
            )

            scatter_renderers.append(scatter_r)

            # top stacked histogram
            hx, _ = np.histogram(sub[x_field].dropna(), bins=x_edges)
            hx_bottom = x_stack.copy()
            hx_top = x_stack + hx
            x_stack = hx_top.copy()

            hx_src = ColumnDataSource(dict(
                left=x_edges[:-1],
                right=x_edges[1:],
                bottom=hx_bottom,
                top=hx_top,
            ))

            hx_r = p_top.quad(
                left="left",
                right="right",
                bottom="bottom",
                top="top",
                source=hx_src,
                fill_color=color,
                line_color=color,
                alpha=0.75,
                legend_label=str(rov),
            )

            # left stacked histogram
            hy, _ = np.histogram(sub[y_field].dropna(), bins=y_edges)
            hy_left = y_stack.copy()
            hy_right = y_stack + hy
            y_stack = hy_right.copy()

            hy_src = ColumnDataSource(dict(
                left=hy_left,
                right=hy_right,
                bottom=y_edges[:-1],
                top=y_edges[1:],
            ))

            hy_r = p_left.quad(
                left="left",
                right="right",
                bottom="bottom",
                top="top",
                source=hy_src,
                fill_color=color,
                line_color=color,
                alpha=0.75,
            )

            toggle_renderers.append([scatter_r, hx_r, hy_r])
            legend_items.append(LegendItem(label=str(rov), renderers=[scatter_r, hx_r, hy_r]))
            checkbox_labels.append(f"{rov} ({len(sub):,})")

        p.add_tools(HoverTool(
            tooltips=[
                ("Line", "@Line"),
                ("Station", "@Station"),
                ("Node", "@Node"),
                ("ROV", "@ROV_USED"),
                ("dX", "@dX{0.00}"),
                ("dY", "@dY{0.00}"),
                ("dZ", "@dZ{0.00}"),
                ("Radial", "@Radial{0.00}"),
                ("DSR X", "@X1{0.00}"),
                ("DSR Y", "@Y1{0.00}"),
                ("REC X", "@X2{0.00}"),
                ("REC Y", "@Y2{0.00}"),
                ("REC Z", "@Z2{0.00}"),
            ]
        ))

        p.legend.click_policy = "hide"
        p.legend.location = "top_right"
        p_top.legend.click_policy = "hide"
        p_top.legend.location = "top_right"

        # ---------------------------------------------------------
        # Summary values
        # ---------------------------------------------------------
        total_points = int(len(df))
        e95 = float(np.percentile(df["Radial"].dropna(), 95))
        mean_radial = float(df["Radial"].mean())
        max_radial = float(df["Radial"].max())
        std_radial = float(df["Radial"].std())

        # E95 ellipse/circle
        p.circle(
            x=0,
            y=0,
            radius=e95,
            fill_alpha=0,
            line_width=3,
            line_color="red",
            legend_label=f"E95 = {e95:.2f} m",
        )

        # ---------------------------------------------------------
        # Right panel with checkbox toggle
        # ---------------------------------------------------------
        checkbox = CheckboxGroup(
            labels=checkbox_labels,
            active=list(range(len(checkbox_labels))),
            width=right_panel_width - 25,
        )

        checkbox.js_on_change(
            "active",
            CustomJS(
                args=dict(renderers=toggle_renderers),
                code="""
                const active = new Set(cb_obj.active);
                for (let i = 0; i < renderers.length; i++) {
                    const visible = active.has(i);
                    for (const r of renderers[i]) {
                        r.visible = visible;
                    }
                }
                """
            )
        )

        summary_div = Div(
            width=right_panel_width,
            text=f"""
            <div style="
                border:1px solid #d7e0ee;
                border-radius:10px;
                padding:12px;
                font-family:system-ui,Segoe UI,Arial;
                background:#ffffff;
                margin-top:10px;
            ">
              <div style="font-size:14px; font-weight:700; margin-bottom:8px;">Summary</div>
              <table style="width:100%; font-size:13px;">
                <tr><td>Total Points:</td><td style="text-align:right; font-weight:700; color:#0d47a1;">{total_points:,}</td></tr>
                <tr><td>E95 Radial:</td><td style="text-align:right; font-weight:700; color:red;">{e95:.2f} m</td></tr>
                <tr><td>Mean Radial:</td><td style="text-align:right;">{mean_radial:.2f} m</td></tr>
                <tr><td>Max Radial:</td><td style="text-align:right;">{max_radial:.2f} m</td></tr>
                <tr><td>Std Radial:</td><td style="text-align:right;">{std_radial:.2f} m</td></tr>
              </table>
            </div>
            """
        )

        rov_panel = Div(
            width=right_panel_width,
            text="""
            <div style="
                border:1px solid #d7e0ee;
                border-radius:10px 10px 0 0;
                padding:10px 12px 2px 12px;
                font-family:system-ui,Segoe UI,Arial;
                background:#ffffff;
                font-size:16px;
                font-weight:700;
                color:#0b1f55;
            ">
                ROV <span style="font-size:12px; font-weight:400;">(click to toggle)</span>
            </div>
            """
        )

        right_panel = column(
            rov_panel,
            checkbox,
            summary_div,
            width=right_panel_width,
            sizing_mode="fixed",
        )

        # ---------------------------------------------------------
        # Statistics table
        # ---------------------------------------------------------
        stats = (
            df.groupby(color_field)
            .agg(
                Count=("Radial", "count"),
                dX_Min=("dX", "min"),
                dX_Max=("dX", "max"),
                dX_Avg=("dX", "mean"),
                dX_Std=("dX", "std"),
                dY_Min=("dY", "min"),
                dY_Max=("dY", "max"),
                dY_Avg=("dY", "mean"),
                dY_Std=("dY", "std"),
                Radial_Avg=("Radial", "mean"),
                Radial_Max=("Radial", "max"),
            )
            .reset_index()
            .rename(columns={color_field: "ROV"})
        )

        total_row = {
            "ROV": "TOTAL",
            "Count": int(stats["Count"].sum()),
            "dX_Min": df["dX"].min(),
            "dX_Max": df["dX"].max(),
            "dX_Avg": df["dX"].mean(),
            "dX_Std": df["dX"].std(),
            "dY_Min": df["dY"].min(),
            "dY_Max": df["dY"].max(),
            "dY_Avg": df["dY"].mean(),
            "dY_Std": df["dY"].std(),
            "Radial_Avg": df["Radial"].mean(),
            "Radial_Max": df["Radial"].max(),
        }

        stats = pd.concat([stats, pd.DataFrame([total_row])], ignore_index=True)

        stats_src = ColumnDataSource(stats)
        num_fmt = NumberFormatter(format="0.00")

        table = DataTable(
            source=stats_src,
            columns=[
                TableColumn(field="ROV", title="ROV"),
                TableColumn(field="Count", title="Count"),
                TableColumn(field="dX_Min", title="dX Min", formatter=num_fmt),
                TableColumn(field="dX_Max", title="dX Max", formatter=num_fmt),
                TableColumn(field="dX_Avg", title="dX Avg", formatter=num_fmt),
                TableColumn(field="dX_Std", title="dX Std", formatter=num_fmt),
                TableColumn(field="dY_Min", title="dY Min", formatter=num_fmt),
                TableColumn(field="dY_Max", title="dY Max", formatter=num_fmt),
                TableColumn(field="dY_Avg", title="dY Avg", formatter=num_fmt),
                TableColumn(field="dY_Std", title="dY Std", formatter=num_fmt),
                TableColumn(field="Radial_Avg", title="Radial Avg", formatter=num_fmt),
                TableColumn(field="Radial_Max", title="Radial Max", formatter=num_fmt),
            ],
            height=190,
            sizing_mode="stretch_width",
            index_position=None,
        )

        # ---------------------------------------------------------
        # Controls
        # ---------------------------------------------------------
        sp = Spinner(
            title="Point Size",
            low=1,
            high=50,
            step=1,
            value=point_size,
            width=150,
        )

        sp.js_on_change(
            "value",
            CustomJS(
                args=dict(renderers=scatter_renderers),
                code="""
                for (const r of renderers) {
                    r.glyph.size = cb_obj.value;
                }
                """
            )
        )

        controls = column(
            sp,
            width=left_hist_width,
            height=hist_height,
            sizing_mode="fixed",
        )

        top_row = row(
            controls,
            p_top,
            sizing_mode="stretch_width",
        )

        main_row = row(
            p_left,
            p,
            right_panel,
            sizing_mode="stretch_both",
        )

        table_title = Div(
            text="""
            <div style="
                font-family:system-ui,Segoe UI,Arial;
                font-size:16px;
                font-weight:700;
                padding:6px 4px;
                color:#0b1f55;
            ">
                📊 Statistics by ROV
            </div>
            """,
            sizing_mode="stretch_width",
        )

        layout = column(
            top_row,
            main_row,
            table_title,
            table,
            sizing_mode="stretch_both",
        )

        if save_html:
            output_file(str(save_html))
            save(layout)

        if is_show:
            show(layout)
            return None

        if json_return:
            return json_item(layout)

        return layout

    def polar_histogram_plotly(
            self,
            df=None,
            *,
            table="DSR",  # "DSR" or "REC_DB"
            lines=None,  # None = whole DB, int = one line, list/tuple/set = many lines
            solution_fk=1,  # used only for DSR
            e_col=None,
            n_col=None,
            preplot_e_col=None,
            preplot_n_col=None,
            group_col=None,
            title=None,
            angle_bins=36,
            max_radius=None,
            template="plotly_dark",
            is_show=False,
            json_return=False,
            percent=False,
            show_dominant_sector=False,
    ):
        """
        Plotly polar histogram / rose plot.

        Works for:
          - whole database: lines=None
          - single line: lines=13801
          - multiple lines: lines=[13801, 13873, 13945]

        0° = North, 90° = East.
        """

        try:
            table = str(table or "DSR").upper().strip()

            # ---------------------------------------------------------
            # Auto-read dataframe if df was not supplied
            # ---------------------------------------------------------
            if df is None:
                if isinstance(lines, int):
                    lines = [lines]
                elif lines is not None:
                    lines = list(lines)

                if table == "DSR":
                    df = self.read_dsr(
                        lines=lines,
                        solution_fk=solution_fk,
                    )

                    e_col = e_col or "PrimaryEasting"
                    n_col = n_col or "PrimaryNorthing"
                    preplot_e_col = preplot_e_col or "PreplotEasting"
                    preplot_n_col = preplot_n_col or "PreplotNorthing"
                    group_col = group_col or "ROV"
                    title = title or "DSR Primary Offset Direction"

                elif table in ("REC_DB", "RECDB", "REC"):
                    df = self.read_recdb(lines=lines)

                    e_col = e_col or "REC_X"
                    n_col = n_col or "REC_Y"
                    preplot_e_col = preplot_e_col or "RPRE_X"
                    preplot_n_col = preplot_n_col or "RPRE_Y"
                    group_col = group_col or None
                    title = title or "REC_DB Offset Direction"

                else:
                    raise ValueError("table must be 'DSR' or 'REC_DB'")

            title = title or "Polar Histogram"

            if df is None or df.empty:
                return self._plotly_error_html(
                    title=title,
                    message="No data for polar histogram.",
                    level="warning",
                    json_return=json_return,
                )

            # ---------------------------------------------------------
            # Optional dataframe-level line filtering
            # Useful when df was passed manually
            # ---------------------------------------------------------
            data = df.copy()

            if lines is not None and "Line" in data.columns:
                if isinstance(lines, int):
                    lines_filter = [lines]
                else:
                    lines_filter = list(lines)

                data["Line"] = pd.to_numeric(data["Line"], errors="coerce")
                data = data[data["Line"].isin(lines_filter)].copy()

            if data.empty:
                return self._plotly_error_html(
                    title=title,
                    message="No rows found for selected line filter.",
                    details=f"lines={lines}",
                    level="warning",
                    json_return=json_return,
                )

            # ---------------------------------------------------------
            # Validate columns
            # ---------------------------------------------------------
            required = [e_col, n_col, preplot_e_col, preplot_n_col]

            for c in required:
                if not c or c not in data.columns:
                    raise ValueError(f"Missing column: {c}")
                data[c] = pd.to_numeric(data[c], errors="coerce")

            data = data.dropna(subset=required)

            if data.empty:
                return self._plotly_error_html(
                    title=title,
                    message="No valid coordinate rows after cleaning.",
                    level="warning",
                    json_return=json_return,
                )

            # ---------------------------------------------------------
            # Calculate offsets and bearing
            # ---------------------------------------------------------
            data["dE"] = data[e_col] - data[preplot_e_col]
            data["dN"] = data[n_col] - data[preplot_n_col]
            data["RadialOffset"] = np.sqrt(data["dE"] ** 2 + data["dN"] ** 2)

            if max_radius is not None:
                data = data[data["RadialOffset"] <= float(max_radius)].copy()

            if data.empty:
                return self._plotly_error_html(
                    title=title,
                    message="No rows left after max_radius filter.",
                    details=f"max_radius={max_radius}",
                    level="warning",
                    json_return=json_return,
                )

            data["Bearing"] = (
                                      np.degrees(np.arctan2(data["dE"], data["dN"])) + 360.0
                              ) % 360.0

            bin_width = 360.0 / int(angle_bins)
            theta_bins = np.arange(bin_width / 2.0, 360.0, bin_width)

            data["BearingBin"] = (
                    np.floor(data["Bearing"] / bin_width) * bin_width + bin_width / 2.0
            )

            # ---------------------------------------------------------
            # Grouping
            # ---------------------------------------------------------
            if group_col and group_col in data.columns:
                data[group_col] = data[group_col].fillna("Unknown").astype(str).str.strip()
                data.loc[data[group_col] == "", group_col] = "Unknown"
                groups = sorted(data[group_col].unique().tolist())
            else:
                group_col = "__Group"
                data[group_col] = "All"
                groups = ["All"]

            fig = go.Figure()

            for group in groups:
                gdf = data[data[group_col] == group]

                hist = (
                    gdf.groupby("BearingBin")
                    .size()
                    .reindex(theta_bins, fill_value=0)
                    .reset_index(name="Count")
                )

                value_col = "Count"
                if percent:
                    total = float(hist["Count"].sum())
                    hist["Percent"] = (hist["Count"] / total * 100.0) if total else 0.0
                    value_col = "Percent"

                fig.add_trace(go.Barpolar(
                    r=hist[value_col],
                    theta=hist["BearingBin"],
                    width=[bin_width] * len(hist),
                    name=str(group),
                    opacity=0.75,
                    hovertemplate=(
                        "<b>%{fullData.name}</b><br>"
                        "Bearing: %{theta:.1f}°<br>"
                        + ("Percent: %{r:.2f}%<extra></extra>" if percent else "Count: %{r}<extra></extra>")
                    ),
                ))

            dominant_text = None
            if show_dominant_sector:
                all_hist = data.groupby("BearingBin").size().reindex(theta_bins, fill_value=0)
                if int(all_hist.sum()) > 0:
                    dominant_bin = float(all_hist.idxmax())
                    compass = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
                    sector = compass[int(((dominant_bin + 22.5) % 360) // 45)]
                    share = float(all_hist.max()) / float(all_hist.sum()) * 100.0
                    dominant_text = f"Dominant offset sector: {sector} ({dominant_bin:.1f}°), {share:.1f}%"

            # ---------------------------------------------------------
            # Better dynamic title
            # ---------------------------------------------------------
            if lines is None:
                line_label = "All lines"
            elif isinstance(lines, int):
                line_label = f"Line {lines}"
            else:
                line_label = f"{len(list(lines))} lines"

            fig.update_layout(
                title=dict(
                    text=f"{title} — {line_label}",
                    x=0.02,
                    xanchor="left",
                ),
                template=template,
                autosize=True,
                margin=dict(l=20, r=20, t=55, b=25),
                polar=dict(
                    angularaxis=dict(
                        direction="clockwise",
                        rotation=90,
                        tickmode="array",
                        tickvals=[0, 45, 90, 135, 180, 225, 270, 315],
                        ticktext=["N", "NE", "E", "SE", "S", "SW", "W", "NW"],
                    ),
                    radialaxis=dict(
                        title="Offset Count (%)" if percent else "Node Count",
                        ticks="",
                        ticksuffix="%" if percent else "",
                    ),
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.18,
                    xanchor="center",
                    x=0.5,
                ),
                annotations=([dict(
                    text=dominant_text,
                    x=0.5, y=1.07, xref="paper", yref="paper",
                    showarrow=False, font=dict(size=12),
                )] if dominant_text else []),
            )

            if is_show:
                fig.show()
                return None

            if json_return:
                return fig.to_json()

            return fig.to_html(
                full_html=False,
                include_plotlyjs="cdn",
                config={"responsive": True},
                default_height="100%",
                default_width="100%",
            )

        except Exception as e:
            return self._plotly_error_html(
                title="Polar histogram failed",
                message="Error while building Plotly polar histogram.",
                details=str(e),
                level="error",
                json_return=json_return,
            )
