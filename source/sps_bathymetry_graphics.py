from pathlib import Path
import math
import sqlite3

import numpy as np
import pandas as pd

from scipy.interpolate import griddata
from scipy.ndimage import gaussian_filter

import matplotlib.pyplot as plt
from matplotlib.colors import LightSource

import plotly.graph_objects as go
import plotly.io as pio

import geopandas as gpd
from shapely.geometry import mapping
from shapely.ops import unary_union
from rasterio.features import geometry_mask
from rasterio.transform import from_bounds


class SPSBathymetryGraphics:
    """
    SPSolution bathymetry graphics for very large datasets.

    Main workflow:
        1. build_bathy_grid_cache()
        2. make_hillshade_from_cache()
        3. make_3d_surface_from_cache()

    Uses:
        SPSolution.Easting
        SPSolution.Northing
        SPSolution.WaterDepth
        SPSolution.FireCode

    Optional polygon clipping:
        Reads shapefile path from project_shapes table by FileName.
    """

    def __init__(self, db_path: str):
        self.db_path = str(db_path)

    # ------------------------------------------------------------------
    # DB
    # ------------------------------------------------------------------
    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=120)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 120000;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        return conn

    def ensure_bathy_grid_table(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS SPS_BathyGrid (
                    cell_size REAL NOT NULL,
                    fire_code TEXT NOT NULL DEFAULT '',
                    gx REAL NOT NULL,
                    gy REAL NOT NULL,
                    depth_avg REAL,
                    depth_min REAL,
                    depth_max REAL,
                    point_count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (cell_size, fire_code, gx, gy)
                );
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_sps_bathygrid_lookup
                ON SPS_BathyGrid (cell_size, fire_code, gx, gy);
            """)

            conn.commit()

    def ensure_sps_indexes(self):
        """
        Run once. Helpful for big SPSolution tables.
        """
        with self._connect() as conn:
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_spsolution_bathy_basic
                ON SPSolution (FireCode, Easting, Northing, WaterDepth);
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_spsolution_bathy_line_seq
                ON SPSolution (Line, Seq, FireCode);
            """)

            conn.commit()

    # ------------------------------------------------------------------
    # Shape helpers
    # ------------------------------------------------------------------
    def get_shape_path_by_filename(self, shape_filename: str) -> str:
        if not shape_filename:
            raise ValueError("shape_filename is required.")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT FullName
                FROM project_shapes
                WHERE FileName = ?
                  AND FileCheck = 1
                LIMIT 1;
                """,
                (shape_filename,),
            ).fetchone()

        if not row:
            raise ValueError(
                f"Shape file not found in project_shapes table: {shape_filename}"
            )

        shp_path = row["FullName"]

        if not Path(shp_path).exists():
            raise FileNotFoundError(f"Shape file path does not exist: {shp_path}")

        return shp_path

    def _load_shape_polygon(
        self,
        *,
        shape_filename: str | None = None,
        shape_path: str | None = None,
    ):
        if shape_filename:
            shape_path = self.get_shape_path_by_filename(shape_filename)

        if not shape_path:
            raise ValueError("shape_filename or shape_path is required.")

        if not Path(shape_path).exists():
            raise FileNotFoundError(f"Shape file path does not exist: {shape_path}")

        gdf = gpd.read_file(shape_path)

        if gdf.empty:
            raise ValueError(f"Shape file is empty: {shape_path}")

        gdf = gdf[gdf.geometry.notnull()].copy()

        if gdf.empty:
            raise ValueError(f"No valid geometry found in shape file: {shape_path}")

        polygon = unary_union(gdf.geometry)

        if polygon.is_empty:
            raise ValueError(f"Polygon geometry is empty: {shape_path}")

        return polygon

    def _clip_grid_by_shape(
        self,
        grid_x,
        grid_y,
        grid_z,
        *,
        shape_filename: str | None = None,
        shape_path: str | None = None,
    ):
        polygon = self._load_shape_polygon(
            shape_filename=shape_filename,
            shape_path=shape_path,
        )

        xmin = float(np.nanmin(grid_x))
        xmax = float(np.nanmax(grid_x))
        ymin = float(np.nanmin(grid_y))
        ymax = float(np.nanmax(grid_y))

        height, width = grid_z.shape

        transform = from_bounds(
            xmin,
            ymin,
            xmax,
            ymax,
            width,
            height,
        )

        mask = geometry_mask(
            [mapping(polygon)],
            out_shape=grid_z.shape,
            transform=transform,
            invert=True,
        )

        return np.where(mask, grid_z, np.nan)

    # ------------------------------------------------------------------
    # Cache builder
    # ------------------------------------------------------------------
    def clear_bathy_grid_cache(
        self,
        *,
        cell_size: float,
        fire_code: str = "T",
    ):
        self.ensure_bathy_grid_table()

        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM SPS_BathyGrid
                WHERE cell_size = ?
                  AND fire_code = ?;
                """,
                (float(cell_size), fire_code or ""),
            )
            conn.commit()

    def build_bathy_grid_cache(
        self,
        *,
        cell_size: float = 25.0,
        fire_code: str = "T",
        depth_field: str = "WaterDepth",
        line: int | None = None,
        seq: int | None = None,
        min_depth: float | None = None,
        max_depth: float | None = None,
        clear_existing: bool = True,
    ) -> dict:
        allowed_depth_fields = {"WaterDepth", "PointDepth"}
        if depth_field not in allowed_depth_fields:
            raise ValueError(f"depth_field must be one of {allowed_depth_fields}")

        self.ensure_bathy_grid_table()

        if clear_existing:
            self.clear_bathy_grid_cache(
                cell_size=cell_size,
                fire_code=fire_code,
            )

        where = [
            "Easting IS NOT NULL",
            "Northing IS NOT NULL",
            f"{depth_field} IS NOT NULL",
            f"{depth_field} > 0",
        ]
        params = []

        if fire_code is not None and fire_code != "":
            where.append("FireCode = ?")
            params.append(fire_code)

        if line is not None:
            where.append("Line = ?")
            params.append(line)

        if seq is not None:
            where.append("Seq = ?")
            params.append(seq)

        if min_depth is not None:
            where.append(f"{depth_field} >= ?")
            params.append(min_depth)

        if max_depth is not None:
            where.append(f"{depth_field} <= ?")
            params.append(max_depth)

        where_sql = " AND ".join(where)

        sql = f"""
            INSERT OR REPLACE INTO SPS_BathyGrid (
                cell_size,
                fire_code,
                gx,
                gy,
                depth_avg,
                depth_min,
                depth_max,
                point_count
            )
            SELECT
                ? AS cell_size,
                ? AS fire_code,
                CAST(Easting / ? AS INTEGER) * ? AS gx,
                CAST(Northing / ? AS INTEGER) * ? AS gy,
                AVG({depth_field}) AS depth_avg,
                MIN({depth_field}) AS depth_min,
                MAX({depth_field}) AS depth_max,
                COUNT(*) AS point_count
            FROM SPSolution
            WHERE {where_sql}
            GROUP BY
                CAST(Easting / ? AS INTEGER),
                CAST(Northing / ? AS INTEGER);
        """

        final_params = [
            float(cell_size),
            fire_code or "",
            float(cell_size),
            float(cell_size),
            float(cell_size),
            float(cell_size),
            *params,
            float(cell_size),
            float(cell_size),
        ]

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            conn.execute(sql, final_params)
            conn.commit()

            stats = conn.execute(
                """
                SELECT
                    COUNT(*) AS grid_cells,
                    SUM(point_count) AS source_points,
                    MIN(depth_avg) AS min_depth,
                    MAX(depth_avg) AS max_depth,
                    MIN(gx) AS min_x,
                    MAX(gx) AS max_x,
                    MIN(gy) AS min_y,
                    MAX(gy) AS max_y
                FROM SPS_BathyGrid
                WHERE cell_size = ?
                  AND fire_code = ?;
                """,
                (float(cell_size), fire_code or ""),
            ).fetchone()

        return dict(stats)

    # ------------------------------------------------------------------
    # Load cache
    # ------------------------------------------------------------------
    def load_bathy_grid_cache(
        self,
        *,
        cell_size: float = 25.0,
        fire_code: str = "T",
        min_points_per_cell: int = 1,
        depth_column: str = "depth_avg",
    ) -> pd.DataFrame:
        allowed_depth_columns = {"depth_avg", "depth_min", "depth_max"}
        if depth_column not in allowed_depth_columns:
            raise ValueError(f"depth_column must be one of {allowed_depth_columns}")

        sql = f"""
            SELECT
                gx,
                gy,
                {depth_column} AS depth,
                point_count
            FROM SPS_BathyGrid
            WHERE cell_size = ?
              AND fire_code = ?
              AND point_count >= ?
              AND {depth_column} IS NOT NULL
            ORDER BY gy, gx;
        """

        with self._connect() as conn:
            df = pd.read_sql_query(
                sql,
                conn,
                params=(
                    float(cell_size),
                    fire_code or "",
                    int(min_points_per_cell),
                ),
            )

        if df.empty:
            raise ValueError(
                "No SPS_BathyGrid data found. Run build_bathy_grid_cache() first."
            )

        return df

    # ------------------------------------------------------------------
    # Convert cache to grid
    # ------------------------------------------------------------------
    def _cache_to_regular_grid(
        self,
        df: pd.DataFrame,
        *,
        max_grid_size: int = 1500,
        fill_missing: bool = True,
        interpolate_missing: bool = True,
        smooth_sigma: float = 0.8,
    ):
        x_unique = np.sort(df["gx"].unique())
        y_unique = np.sort(df["gy"].unique())

        nx = len(x_unique)
        ny = len(y_unique)

        if nx > max_grid_size or ny > max_grid_size:
            step = int(math.ceil(max(nx, ny) / max_grid_size))
            df = df.iloc[::step].copy()

            x_unique = np.sort(df["gx"].unique())
            y_unique = np.sort(df["gy"].unique())

            nx = len(x_unique)
            ny = len(y_unique)

        x_index = {x: i for i, x in enumerate(x_unique)}
        y_index = {y: i for i, y in enumerate(y_unique)}

        grid_z = np.full((ny, nx), np.nan, dtype=float)

        for row in df.itertuples(index=False):
            ix = x_index[row.gx]
            iy = y_index[row.gy]
            grid_z[iy, ix] = row.depth

        grid_x, grid_y = np.meshgrid(x_unique, y_unique)

        if fill_missing and np.isnan(grid_z).any():
            mask = ~np.isnan(grid_z)

            if mask.sum() < 3:
                raise ValueError("Not enough valid bathymetry grid cells.")

            valid_x = grid_x[mask]
            valid_y = grid_y[mask]
            valid_z = grid_z[mask]

            nearest_z = griddata(
                points=(valid_x, valid_y),
                values=valid_z,
                xi=(grid_x, grid_y),
                method="nearest",
            )

            if interpolate_missing:
                linear_z = griddata(
                    points=(valid_x, valid_y),
                    values=valid_z,
                    xi=(grid_x, grid_y),
                    method="linear",
                )
                grid_z = np.where(np.isnan(linear_z), nearest_z, linear_z)
            else:
                grid_z = np.where(np.isnan(grid_z), nearest_z, grid_z)

        if smooth_sigma and smooth_sigma > 0:
            # Keep NaN mask safe during smoothing
            nan_mask = np.isnan(grid_z)
            if nan_mask.any():
                filled = np.where(nan_mask, np.nanmean(grid_z), grid_z)
                smoothed = gaussian_filter(filled, sigma=float(smooth_sigma))
                grid_z = np.where(nan_mask, np.nan, smoothed)
            else:
                grid_z = gaussian_filter(grid_z, sigma=float(smooth_sigma))

        return grid_x, grid_y, grid_z

    # ------------------------------------------------------------------
    # Hillshade
    # ------------------------------------------------------------------
    def make_hillshade_from_cache(
            self,
            *,
            cell_size: float = 25.0,
            fire_code: str = "T",
            depth_column: str = "depth_avg",
            min_points_per_cell: int = 1,
            max_grid_size: int = 1800,
            smooth_sigma: float = 0.8,
            shape_filename: str | None = None,
            shape_path: str | None = None,
            cmap: str = "terrain",
            azdeg: float = 315,
            altdeg: float = 45,
            contour_interval: float = 10.0,
            output_png: str | None = None,
            is_show: bool = False,
    ):
        df = self.load_bathy_grid_cache(
            cell_size=cell_size,
            fire_code=fire_code,
            min_points_per_cell=min_points_per_cell,
            depth_column=depth_column,
        )

        # No smoothing here. Smooth after polygon mask only.
        grid_x, grid_y, grid_depth = self._cache_to_regular_grid(
            df,
            max_grid_size=max_grid_size,
            fill_missing=True,
            interpolate_missing=True,
            smooth_sigma=0,
        )

        if shape_filename or shape_path:
            grid_depth = self._clip_grid_by_shape(
                grid_x,
                grid_y,
                grid_depth,
                shape_filename=shape_filename,
                shape_path=shape_path,
            )

        if np.all(np.isnan(grid_depth)):
            raise ValueError("All bathymetry grid cells are outside shape polygon.")

        grid_depth = self._smooth_inside_mask_only(
            grid_depth,
            smooth_sigma=smooth_sigma,
        )

        valid_mask = ~np.isnan(grid_depth)
        safe_depth = np.where(valid_mask, grid_depth, np.nanmean(grid_depth))
        elevation = -safe_depth

        fig, ax = plt.subplots(figsize=(14, 11), dpi=160)

        light = LightSource(azdeg=azdeg, altdeg=altdeg)
        rgb = light.shade(
            elevation,
            cmap=plt.get_cmap(cmap),
            vert_exag=1.8,
            blend_mode="soft",
        )

        alpha = np.where(valid_mask, 1.0, 0.0)

        ax.imshow(
            rgb,
            extent=[grid_x.min(), grid_x.max(), grid_y.min(), grid_y.max()],
            origin="lower",
            aspect="equal",
            alpha=alpha,
        )

        if contour_interval and contour_interval > 0:
            d_min = np.nanmin(grid_depth)
            d_max = np.nanmax(grid_depth)

            levels = np.arange(
                np.floor(d_min / contour_interval) * contour_interval,
                np.ceil(d_max / contour_interval) * contour_interval + contour_interval,
                contour_interval,
            )

            cs = ax.contour(
                grid_x,
                grid_y,
                grid_depth,
                levels=levels,
                linewidths=0.45,
                colors="black",
                alpha=0.55,
            )
            ax.clabel(cs, inline=True, fontsize=6, fmt="%.0f")

        title = f"SPS Bathymetry Hillshade | cell={cell_size:g} | FireCode={fire_code}"
        if shape_filename:
            title += f" | clipped: {shape_filename}"

        ax.set_title(title)
        ax.set_xlabel("Easting")
        ax.set_ylabel("Northing")
        ax.grid(True, linewidth=0.25, alpha=0.35)

        plt.tight_layout()

        if output_png:
            output_path = Path(output_png)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=220, bbox_inches="tight")

        if is_show:
            plt.show()
        else:
            plt.close(fig)

        return fig

    # ------------------------------------------------------------------
    # 3D Plotly
    # ------------------------------------------------------------------
    def make_3d_surface_from_cache(
            self,
            *,
            cell_size: float = 25.0,
            fire_code: str = "T",
            depth_column: str = "depth_avg",
            min_points_per_cell: int = 1,
            max_grid_size: int = 450,
            smooth_sigma: float = 0.8,
            shape_filename: str | None = None,
            shape_path: str | None = None,
            colorscale: str = "Earth",
            vertical_exaggeration: float = 3.0,
            save_html: str | None = None,
            is_show: bool = False,
    ):
        df = self.load_bathy_grid_cache(
            cell_size=cell_size,
            fire_code=fire_code,
            min_points_per_cell=min_points_per_cell,
            depth_column=depth_column,
        )

        # No smoothing here. Smooth after polygon mask only.
        grid_x, grid_y, grid_depth = self._cache_to_regular_grid(
            df,
            max_grid_size=max_grid_size,
            fill_missing=True,
            interpolate_missing=True,
            smooth_sigma=0,
        )

        if shape_filename or shape_path:
            grid_depth = self._clip_grid_by_shape(
                grid_x,
                grid_y,
                grid_depth,
                shape_filename=shape_filename,
                shape_path=shape_path,
            )

        if np.all(np.isnan(grid_depth)):
            raise ValueError("All bathymetry grid cells are outside shape polygon.")

        grid_depth = self._smooth_inside_mask_only(
            grid_depth,
            smooth_sigma=smooth_sigma,
        )

        z_surface = -grid_depth * vertical_exaggeration

        fig = go.Figure()

        fig.add_trace(
            go.Surface(
                x=grid_x,
                y=grid_y,
                z=z_surface,
                surfacecolor=grid_depth,
                colorscale=colorscale,
                colorbar=dict(title="WaterDepth"),
                opacity=0.96,
                connectgaps=False,
                contours={
                    "z": {
                        "show": True,
                        "usecolormap": True,
                        "highlightcolor": "white",
                        "project_z": True,
                    }
                },
            )
        )

        title = (
            f"SPS Bathymetry 3D Surface | "
            f"cell={cell_size:g} | FireCode={fire_code} | "
            f"VE x{vertical_exaggeration:g}"
        )

        if shape_filename:
            title += f" | clipped: {shape_filename}"

        fig.update_layout(
            title=title,
            scene=dict(
                xaxis_title="Easting",
                yaxis_title="Northing",
                zaxis_title="Depth",
                aspectmode="data",
            ),
            margin=dict(l=0, r=0, t=45, b=0),
        )

        if save_html:
            save_path = Path(save_html)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            pio.write_html(fig, file=str(save_path), auto_open=False)

        if is_show:
            fig.show()

        return fig

    # ------------------------------------------------------------------
    # Full workflow
    # ------------------------------------------------------------------
    def build_and_render(
        self,
        *,
        cell_size: float = 25.0,
        fire_code: str = "T",
        shape_filename: str | None = None,
        shape_path: str | None = None,
        cmap: str = "terrain",
        colorscale: str = "Earth",
        output_png: str | None = None,
        save_html: str | None = None,
        is_show: bool = False,
    ) -> dict:
        stats = self.build_bathy_grid_cache(
            cell_size=cell_size,
            fire_code=fire_code,
            depth_field="WaterDepth",
            clear_existing=True,
        )

        self.make_hillshade_from_cache(
            cell_size=cell_size,
            fire_code=fire_code,
            shape_filename=shape_filename,
            shape_path=shape_path,
            cmap=cmap,
            output_png=output_png,
            is_show=is_show,
        )

        self.make_3d_surface_from_cache(
            cell_size=cell_size,
            fire_code=fire_code,
            shape_filename=shape_filename,
            shape_path=shape_path,
            colorscale=colorscale,
            save_html=save_html,
            is_show=is_show,
        )

        return stats

    def _smooth_inside_mask_only(self, grid_depth, *, smooth_sigma: float):
        """
        Smooth only valid cells. NaN area stays muted/transparent.
        This prevents smoothing from bleeding outside polygon.
        """
        if not smooth_sigma or smooth_sigma <= 0:
            return grid_depth

        valid_mask = ~np.isnan(grid_depth)

        if not valid_mask.any():
            return grid_depth

        filled = np.where(valid_mask, grid_depth, 0.0)
        weights = valid_mask.astype(float)

        smooth_data = gaussian_filter(filled, sigma=float(smooth_sigma))
        smooth_weights = gaussian_filter(weights, sigma=float(smooth_sigma))

        with np.errstate(invalid="ignore", divide="ignore"):
            smoothed = smooth_data / smooth_weights

        smoothed[~valid_mask] = np.nan
        return smoothed