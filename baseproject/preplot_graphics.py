from pathlib import Path
import sqlite3
import pandas as pd
import geopandas as gpd
from pyproj import Transformer

from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, HoverTool
import xyzservices.providers as xyz


class PreplotGraphics:
    def __init__(self, db_path):
        self.db_path = Path(db_path)
    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def preplot_map(
            self,
            rl_table: str = "RLPreplot",
            sl_table: str = "SLPreplot",
            src_epsg: int | None = None,
            height: int = 650,
            rl_line_width: int = 2,
            sl_line_width: int = 2,
            show_tiles: bool = True,
    ):
        def _load_segments(table_name: str):
            with self._connect() as con:
                df = pd.read_sql(f"""
                    SELECT
                        Line,
                        TierLine,
                        RealStartX,
                        RealStartY,
                        RealEndX,
                        RealEndY,
                        Points,
                        RealLineLength
                    FROM {table_name}
                    WHERE RealStartX IS NOT NULL
                      AND RealStartY IS NOT NULL
                      AND RealEndX   IS NOT NULL
                      AND RealEndY   IS NOT NULL
                """, con)

            if df.empty:
                return df

            return df.rename(columns={
                "RealStartX": "x0",
                "RealStartY": "y0",
                "RealEndX": "x1",
                "RealEndY": "y1",
            })

        rl_df = _load_segments(rl_table)
        sl_df = _load_segments(sl_table)

        if rl_df.empty and sl_df.empty:
            raise ValueError("No RLPreplot or SLPreplot data to plot")

        # CRS conversion (UTM → WebMercator)
        if src_epsg:
            transformer = Transformer.from_crs(
                f"EPSG:{src_epsg}", "EPSG:3857", always_xy=True
            )

            for df in (rl_df, sl_df):
                if df.empty:
                    continue
                df["x0"], df["y0"] = transformer.transform(df["x0"].values, df["y0"].values)
                df["x1"], df["y1"] = transformer.transform(df["x1"].values, df["y1"].values)

        p = figure(
            x_axis_type="mercator" if show_tiles else "linear",
            y_axis_type="mercator" if show_tiles else "linear",
            sizing_mode="stretch_both",
            height=height,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
            title="Preplot Map – RL + SL",
        )

        if show_tiles:
            p.add_tile(xyz.CartoDB.Positron)

        # RL layer
        if not rl_df.empty:
            rl_src = ColumnDataSource(rl_df)
            rl_r = p.segment(
                x0="x0", y0="y0", x1="x1", y1="y1",
                source=rl_src,
                line_width=rl_line_width,
                alpha=0.9,
                legend_label="Receiver Lines",
            )
            p.add_tools(HoverTool(
                renderers=[rl_r],
                tooltips=[
                    ("Layer", "RL"),
                    ("Line", "@Line"),
                    ("TierLine", "@TierLine"),
                ]
            ))

        # SL layer
        if not sl_df.empty:
            sl_src = ColumnDataSource(sl_df)
            sl_r = p.segment(
                x0="x0", y0="y0", x1="x1", y1="y1",
                source=sl_src,
                line_width=sl_line_width,

                alpha=0.9,
                color='red',
                legend_label="Source Lines",
            )
            p.add_tools(HoverTool(
                renderers=[sl_r],
                tooltips=[
                    ("Layer", "SL"),
                    ("Line", "@Line"),
                    ("TierLine", "@TierLine"),
                    ('Total Nodes','@Points'),
                    ('Length','@RealLineLength')
                ]
            ))

        # Auto zoom
        dfs = [df for df in (rl_df, sl_df) if not df.empty]
        xmin = min(df[["x0", "x1"]].min().min() for df in dfs)
        xmax = max(df[["x0", "x1"]].max().max() for df in dfs)
        ymin = min(df[["y0", "y1"]].min().min() for df in dfs)
        ymax = max(df[["y0", "y1"]].max().max() for df in dfs)

        pad = 2000 if show_tiles else 0
        p.x_range.start, p.x_range.end = xmin - pad, xmax + pad
        p.y_range.start, p.y_range.end = ymin - pad, ymax + pad

        p.legend.location = "top_left"
        p.legend.click_policy = "hide"

        return p

    def add_scale_bar(self,
            p,
            length_m=1000,  # scale length in meters
            location="bottom_left",
            height_px=6,
    ):
        """
        Add a simple scale bar to a WebMercator Bokeh map.
        Assumes x/y are EPSG:3857 (meters).
        """

        # Estimate position from current ranges
        x0 = p.x_range.start
        x1 = p.x_range.end
        y0 = p.y_range.start
        y1 = p.y_range.end

        pad_x = (x1 - x0) * 0.05
        pad_y = (y1 - y0) * 0.05

        if location == "bottom_left":
            x = x0 + pad_x
            y = y0 + pad_y
        elif location == "bottom_right":
            x = x1 - pad_x - length_m
            y = y0 + pad_y
        else:
            raise ValueError("location must be 'bottom_left' or 'bottom_right'")

        # Scale bar (rectangle)
        p.rect(
            x=x + length_m / 2,
            y=y,
            width=length_m,
            height=(y1 - y0) * 0.002,
            fill_color="black",
            line_color="black",
        )

        # Label
        label = f"{int(length_m / 1000)} km" if length_m >= 1000 else f"{length_m} m"

        p.text(
            x=[x + length_m / 2],
            y=[y + (y1 - y0) * 0.01],
            text=[label],
            text_align="center",
            text_baseline="bottom",
            text_font_size="10pt",
        )

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
                    COALESCE(LineStyle, '') AS LineStyle
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
                    p.patches(
                        xs="xs", ys="ys",
                        source=src,
                        fill_color=(fill_color if is_filled else None),
                        fill_alpha=(fill_alpha if is_filled else 0.0),
                        line_color=line_color,
                        line_width=line_width,
                        line_dash=line_dash,
                        line_alpha=line_alpha,
                        legend_label=layer_name,
                    )

        # click legend to hide/show layers
        if p.legend:
            p.legend.click_policy = "hide"

        return p

