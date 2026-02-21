from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple, Union

import sqlite3
import pandas as pd
from bokeh.core.property.vectorization import value
from bokeh.io import show
from bokeh.layouts import row, column
from bokeh.palettes import Category10, Category20

from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, HoverTool, Button, Spinner, CustomJS, LabelSet
from bokeh.models import WMTSTileSource
import geopandas as gpd
from bokeh.transform import factor_cmap
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
                      X,Y 
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