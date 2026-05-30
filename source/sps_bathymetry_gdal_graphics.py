from pathlib import Path
import csv
import sqlite3
import tempfile

import numpy as np
import pandas as pd
from osgeo import gdal, osr

from source.gis_env import configure_qgis_env

configure_qgis_env(verbose=False)

from osgeo import gdal
from osgeo import ogr

gdal.UseExceptions()

import rasterio
import rasterio.mask

import plotly.graph_objects as go
import plotly.io as pio
import plotly.io as pio
from pathlib import Path

import numpy as np
from osgeo import gdal, osr


class SPSBathymetryGDALGraphics:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=120)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 120000;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        return conn

    def ensure_sps_indexes(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_spsolution_bathy_gdal
                ON SPSolution (FireCode, Easting, Northing, WaterDepth);
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_spsolution_bathy_line_seq_gdal
                ON SPSolution (Line, Seq, FireCode);
            """)
            conn.commit()

    def get_shape_path_by_filename(self, shape_filename: str) -> str:
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
            raise ValueError(f"Shape file not found in project_shapes: {shape_filename}")

        shp_path = row["FullName"]

        if not Path(shp_path).exists():
            raise FileNotFoundError(f"Shape file path does not exist: {shp_path}")

        return shp_path

    def export_xyz_from_sps(
        self,
        *,
        output_xyz: str,
        fire_code: str = "A",
        depth_field: str = "WaterDepth",
        line: int | None = None,
        seq: int | None = None,
        min_depth: float | None = None,
        max_depth: float | None = None,
        chunk_size: int = 200000,
    ) -> dict:
        allowed_depth_fields = {"WaterDepth", "PointDepth"}
        if depth_field not in allowed_depth_fields:
            raise ValueError(f"depth_field must be one of {allowed_depth_fields}")

        output_path = Path(output_xyz)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        where = [
            "Easting IS NOT NULL",
            "Northing IS NOT NULL",
            f"{depth_field} IS NOT NULL",
            f"{depth_field} > 0",
        ]
        params = []

        if fire_code:
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

        sql = f"""
            SELECT
                Easting AS x,
                Northing AS y,
                {depth_field} AS z
            FROM SPSolution
            WHERE {" AND ".join(where)}
        """

        total = 0
        min_x = max_x = min_y = max_y = min_z = max_z = None

        with self._connect() as conn:
            with output_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["x", "y", "z"])

                for chunk in pd.read_sql_query(sql, conn, params=params, chunksize=chunk_size):
                    chunk = chunk.dropna(subset=["x", "y", "z"])
                    chunk["x"] = pd.to_numeric(chunk["x"], errors="coerce")
                    chunk["y"] = pd.to_numeric(chunk["y"], errors="coerce")
                    chunk["z"] = pd.to_numeric(chunk["z"], errors="coerce")
                    chunk = chunk.dropna(subset=["x", "y", "z"])

                    if chunk.empty:
                        continue

                    for row in chunk.itertuples(index=False):
                        writer.writerow([float(row.x), float(row.y), float(row.z)])

                    total += len(chunk)

                    cx_min, cx_max = chunk["x"].min(), chunk["x"].max()
                    cy_min, cy_max = chunk["y"].min(), chunk["y"].max()
                    cz_min, cz_max = chunk["z"].min(), chunk["z"].max()

                    min_x = cx_min if min_x is None else min(min_x, cx_min)
                    max_x = cx_max if max_x is None else max(max_x, cx_max)
                    min_y = cy_min if min_y is None else min(min_y, cy_min)
                    max_y = cy_max if max_y is None else max(max_y, cy_max)
                    min_z = cz_min if min_z is None else min(min_z, cz_min)
                    max_z = cz_max if max_z is None else max(max_z, cz_max)

        if total == 0:
            raise ValueError("No SPSolution XYZ data exported.")

        return {
            "xyz": str(output_path),
            "rows": total,
            "min_x": float(min_x),
            "max_x": float(max_x),
            "min_y": float(min_y),
            "max_y": float(max_y),
            "min_depth": float(min_z),
            "max_depth": float(max_z),
        }

    def create_vrt_from_xyz(
            self,
            *,
            xyz_path: str,
            vrt_path: str,
            layer_name: str = "sps_bathy_xyz",
            epsg: int | None = None,
    ) -> str:
        xyz_path = Path(xyz_path)
        vrt_path = Path(vrt_path)

        vrt_path.parent.mkdir(parents=True, exist_ok=True)

        if epsg:
            srs_text = f"EPSG:{epsg}"
        else:
            srs_text = 'LOCAL_CS["Undefined"]'

        vrt_text = f"""<OGRVRTDataSource>
        <OGRVRTLayer name="{layer_name}">
            <SrcDataSource>{xyz_path.as_posix()}</SrcDataSource>
            <GeometryType>wkbPoint</GeometryType>
            <LayerSRS>{srs_text}</LayerSRS>
            <GeometryField encoding="PointFromColumns" x="x" y="y" z="z"/>
        </OGRVRTLayer>
    </OGRVRTDataSource>
    """

        vrt_path.write_text(vrt_text, encoding="utf-8")

        return str(vrt_path)

    def make_dem_geotiff_from_sps(
            self,
            *,
            output_tif: str,
            fire_code: str = "A",
            depth_field: str = "WaterDepth",
            cell_size: float = 100.0,
            line: int | None = None,
            seq: int | None = None,
            epsg: int | None = None,
            min_depth: float | None = None,
            max_depth: float | None = None,
            algorithm: str = "average",
            nodata: float = -9999.0,
            temp_dir: str | None = None,
            keep_xyz: bool = False,
    ) -> dict:
        allowed_depth_fields = {"WaterDepth", "PointDepth"}
        if depth_field not in allowed_depth_fields:
            raise ValueError(f"depth_field must be one of {allowed_depth_fields}")

        output_tif_path = Path(output_tif)
        output_tif_path.parent.mkdir(parents=True, exist_ok=True)

        work_dir_obj = None
        if temp_dir:
            work_dir = Path(temp_dir)
            work_dir.mkdir(parents=True, exist_ok=True)
        else:
            work_dir_obj = tempfile.TemporaryDirectory()
            work_dir = Path(work_dir_obj.name)

        xyz_path = work_dir / "sps_bathy_xyz.csv"
        vrt_path = work_dir / "sps_bathy_xyz.vrt"

        stats = self.export_xyz_from_sps(
            output_xyz=str(xyz_path),
            fire_code=fire_code,
            depth_field=depth_field,
            line=line,
            seq=seq,
            min_depth=min_depth,
            max_depth=max_depth,
        )
        if epsg is None:
            epsg = self.get_epsg_from_shape(
                shape_filename=None,
                shape_path=None,
            )
        self.create_vrt_from_xyz(
            xyz_path=str(xyz_path),
            vrt_path=str(vrt_path),
            epsg=epsg,
        )

        min_x = stats["min_x"]
        max_x = stats["max_x"]
        min_y = stats["min_y"]
        max_y = stats["max_y"]

        width = max(1, int((max_x - min_x) / cell_size))
        height = max(1, int((max_y - min_y) / cell_size))

        search_radius = float(cell_size) * 3.0

        if algorithm == "invdistnn":
            algorithm_string = (
                f"invdistnn:"
                f"power=2.0:"
                f"smoothing=0.0:"
                f"radius={search_radius}:"
                f"max_points=12:"
                f"min_points=1:"
                f"nodata={nodata}"
            )
        elif algorithm == "invdist":
            algorithm_string = (
                f"invdist:"
                f"power=2.0:"
                f"smoothing=0.0:"
                f"radius1={search_radius}:"
                f"radius2={search_radius}:"
                f"angle=0.0:"
                f"nodata={nodata}"
            )
        elif algorithm == "nearest":
            algorithm_string = (
                f"nearest:"
                f"radius1={search_radius}:"
                f"radius2={search_radius}:"
                f"angle=0.0:"
                f"nodata={nodata}"
            )
        elif algorithm == "average":
            algorithm_string = (
                f"average:"
                f"radius1={search_radius}:"
                f"radius2={search_radius}:"
                f"angle=0.0:"
                f"min_points=1:"
                f"nodata={nodata}"
            )
        else:
            algorithm_string = algorithm

        options = gdal.GridOptions(
            format="GTiff",
            outputType=gdal.GDT_Float32,
            width=width,
            height=height,
            outputBounds=[min_x, min_y, max_x, max_y],
            algorithm=algorithm_string,
            noData=nodata,
            outputSRS=f"EPSG:{epsg}" if epsg else None,
            creationOptions=[
                "TILED=YES",
                "COMPRESS=LZW",
                "BIGTIFF=YES",
            ],
        )

        ds = gdal.Grid(
            destName=str(output_tif_path),
            srcDS=str(vrt_path),
            options=options,
        )

        if ds is None:
            raise RuntimeError(
                f"GDAL Grid failed to create DEM GeoTIFF. Algorithm: {algorithm_string}"
            )

        ds.FlushCache()
        ds = None

        self.build_overviews(str(output_tif_path))

        stats["dem_tif"] = str(output_tif_path)
        stats["cell_size"] = cell_size
        stats["width"] = width
        stats["height"] = height
        stats["algorithm"] = algorithm_string

        if keep_xyz:
            stats["xyz"] = str(xyz_path)
            stats["vrt"] = str(vrt_path)

        if work_dir_obj is not None and not keep_xyz:
            work_dir_obj.cleanup()

        return stats

    def clip_raster_by_shape(
        self,
        *,
        input_tif: str,
        output_tif: str,
        shape_filename: str | None = None,
        shape_path: str | None = None,
        nodata: float = -9999.0,
    ) -> str:
        if shape_filename:
            shape_path = self.get_shape_path_by_filename(shape_filename)

        if not shape_path:
            raise ValueError("shape_filename or shape_path is required.")

        output_path = Path(output_tif)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        shp_ds = ogr.Open(shape_path)
        if shp_ds is None:
            raise RuntimeError(f"Could not open shapefile: {shape_path}")

        layer = shp_ds.GetLayer()

        options = gdal.WarpOptions(
            format="GTiff",
            cutlineDSName=shape_path,
            cropToCutline=True,
            dstNodata=nodata,
            creationOptions=[
                "TILED=YES",
                "COMPRESS=LZW",
                "BIGTIFF=YES",
            ],
        )

        ds = gdal.Warp(
            destNameOrDestDS=str(output_path),
            srcDSOrSrcDSTab=str(input_tif),
            options=options,
        )

        if ds is None:
            raise RuntimeError("GDAL Warp clipping failed.")

        ds.FlushCache()
        ds = None
        shp_ds = None

        self.build_overviews(str(output_path))
        return str(output_path)

    def make_hillshade_from_dem(
        self,
        *,
        dem_tif: str,
        output_tif: str,
        output_png: str | None = None,
        azimuth: float = 315,
        altitude: float = 45,
        z_factor: float = 1.0,
        nodata: float = -9999.0,
    ) -> str:
        output_tif_path = Path(output_tif)
        output_tif_path.parent.mkdir(parents=True, exist_ok=True)

        options = gdal.DEMProcessingOptions(
            format="GTiff",
            azimuth=azimuth,
            altitude=altitude,
            zFactor=z_factor,
            computeEdges=True,
            creationOptions=[
                "TILED=YES",
                "COMPRESS=LZW",
                "BIGTIFF=YES",
            ],
        )

        ds = gdal.DEMProcessing(
            destName=str(output_tif_path),
            srcDS=dem_tif,
            processing="hillshade",
            options=options,
        )

        if ds is None:
            raise RuntimeError("GDAL DEMProcessing hillshade failed.")

        ds.FlushCache()
        ds = None

        self.build_overviews(str(output_tif_path))

        if output_png:
            self.translate_raster_to_png(
                input_tif=str(output_tif_path),
                output_png=output_png,
            )

        return str(output_tif_path)

    def translate_raster_to_png(
        self,
        *,
        input_tif: str,
        output_png: str,
    ) -> str:
        output_path = Path(output_png)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        options = gdal.TranslateOptions(
            format="PNG",
            creationOptions=["WORLDFILE=YES"],
        )

        ds = gdal.Translate(
            destName=str(output_path),
            srcDS=input_tif,
            options=options,
        )

        if ds is None:
            raise RuntimeError("GDAL Translate to PNG failed.")

        ds.FlushCache()
        ds = None

        return str(output_path)

    def build_overviews(
        self,
        raster_path: str,
        *,
        levels: list[int] | None = None,
    ):
        if levels is None:
            levels = [2, 4, 8, 16, 32]

        ds = gdal.Open(raster_path, gdal.GA_Update)
        if ds is None:
            return

        ds.BuildOverviews("AVERAGE", levels)
        ds.FlushCache()
        ds = None

    def read_dem_for_plotly(
        self,
        *,
        dem_tif: str,
        max_grid_size: int = 450,
        nodata: float = -9999.0,
    ):
        with rasterio.open(dem_tif) as src:
            arr = src.read(1).astype(float)
            transform = src.transform

            arr[arr == nodata] = np.nan
            if src.nodata is not None:
                arr[arr == src.nodata] = np.nan

            height, width = arr.shape

            step = max(1, int(np.ceil(max(height, width) / max_grid_size)))
            arr = arr[::step, ::step]

            rows, cols = arr.shape

            x_coords = np.array([
                transform.c + (c * step + 0.5) * transform.a
                for c in range(cols)
            ])

            y_coords = np.array([
                transform.f + (r * step + 0.5) * transform.e
                for r in range(rows)
            ])

            grid_x, grid_y = np.meshgrid(x_coords, y_coords)

        return grid_x, grid_y, arr

    def make_3d_surface_from_dem(
            self,
            *,
            dem_tif: str,
            save_html: str | None = None,
            colorscale: str = "Earth",
            vertical_exaggeration: float = 3.0,
            max_grid_size: int = 450,
            show_source_lines: bool = True,
            source_lines_z_offset: float = 50.0,
            source_lines_production_only: bool = True,
            is_show: bool = False,
    ):
        grid_x, grid_y, grid_depth = self.read_dem_for_plotly(
            dem_tif=dem_tif,
            max_grid_size=max_grid_size,
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
                connectgaps=False,
                opacity=0.96,
                name="Bathymetry",
            )
        )

        if show_source_lines:
            valid_z = z_surface[~np.isnan(z_surface)]

            if valid_z.size:
                z_level = float(np.nanmax(valid_z)) + float(source_lines_z_offset)
            else:
                z_level = 0.0

            self._add_source_lines_to_plotly_fig(
                fig,
                z_level=z_level,
                production_only=source_lines_production_only,
                show_preplot=True,
                show_solution=True,
            )

        fig.update_layout(
            title=f"SPS Bathymetry GDAL 3D Surface | VE x{vertical_exaggeration:g}",
            scene=dict(
                xaxis_title="Easting",
                yaxis_title="Northing",
                zaxis_title="Depth",
                aspectmode="data",
                camera=dict(
                    eye=dict(x=1.6, y=1.6, z=0.8),
                ),
            ),
            margin=dict(l=0, r=0, t=45, b=0),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=0.01,
                xanchor="left",
                x=0.01,
            ),
        )

        if save_html:
            save_path = Path(save_html)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            pio.write_html(fig, file=str(save_path), auto_open=False)

        if is_show:
            fig.show()

        return fig

    def build_dem_hillshade_3d(
        self,
        *,
        output_folder: str,
        fire_code: str = "A",
        depth_field: str = "WaterDepth",
        cell_size: float = 100.0,
        epsg: int | None = None,
        shape_filename: str | None = None,
        shape_path: str | None = None,
        algorithm: str = "invdistnn",
        colorscale: str = "Earth",
        vertical_exaggeration: float = 3.0,
        is_show: bool = False,
    ) -> dict:
        output_dir = Path(output_folder)
        output_dir.mkdir(parents=True, exist_ok=True)

        raw_dem = output_dir / f"sps_bathy_dem_{cell_size:g}m.tif"
        clipped_dem = output_dir / f"sps_bathy_dem_{cell_size:g}m_clipped.tif"
        hillshade_tif = output_dir / f"sps_bathy_hillshade_{cell_size:g}m.tif"
        hillshade_png = output_dir / f"sps_bathy_hillshade_{cell_size:g}m.png"
        surface_html = output_dir / f"sps_bathy_3d_{cell_size:g}m.html"
        if epsg is None and (shape_filename or shape_path):
            epsg = self.get_epsg_from_shape(
                shape_filename=shape_filename,
                shape_path=shape_path,
            )

        print("Detected EPSG:", epsg)
        stats = self.make_dem_geotiff_from_sps(
            output_tif=str(raw_dem),
            fire_code=fire_code,
            depth_field=depth_field,
            cell_size=cell_size,
            algorithm=algorithm,
            epsg=epsg,
        )

        dem_for_products = str(raw_dem)

        if shape_filename or shape_path:
            dem_for_products = self.clip_raster_by_shape(
                input_tif=str(raw_dem),
                output_tif=str(clipped_dem),
                shape_filename=shape_filename,
                shape_path=shape_path,
            )

        self.make_hillshade_from_dem(
            dem_tif=dem_for_products,
            output_tif=str(hillshade_tif),
            output_png=str(hillshade_png),
        )

        self.make_3d_surface_from_dem(
            dem_tif=dem_for_products,
            save_html=str(surface_html),
            colorscale=colorscale,
            vertical_exaggeration=vertical_exaggeration,
            is_show=is_show,
        )

        stats.update({
            "raw_dem": str(raw_dem),
            "dem_for_products": dem_for_products,
            "hillshade_tif": str(hillshade_tif),
            "hillshade_png": str(hillshade_png),
            "surface_html": str(surface_html),
        })

        return stats

    def get_epsg_from_shape(
            self,
            *,
            shape_filename: str | None = None,
            shape_path: str | None = None,
    ) -> int | None:
        """
        Detect EPSG from shapefile CRS.
        """
        if shape_filename:
            shape_path = self.get_shape_path_by_filename(shape_filename)

        if not shape_path:
            return None

        ds = ogr.Open(shape_path)

        if ds is None:
            return None

        layer = ds.GetLayer()

        if layer is None:
            return None

        srs = layer.GetSpatialRef()

        if srs is None:
            return None

        srs.AutoIdentifyEPSG()

        code = srs.GetAuthorityCode(None)

        if code is None:
            return None

        try:
            return int(code)
        except Exception:
            return None

    def make_sp_density_grid(
            self,
            *,
            output_tif: str,
            fire_code: str = "A",
            cell_size: float = 100.0,
            shape_filename: str | None = None,
            shape_path: str | None = None,
            epsg: int | None = None,
            nodata: float = 0.0,
            density_type: str = "count",
            search_radius: float | None = None,
            cells_per_radius: int = 1,
            area_units: str = "cell",
    ) -> dict:
        """
        Create SP density raster from SPSolution.

        density_type:
            count    = raw point count per raster cell
            gaussian = OpenCV Gaussian-smoothed density grid

        search_radius:
            Radius in map units/meters. If supplied, cell_size becomes:
            search_radius / cells_per_radius.

        area_units:
            cell  = SP count per cell
            sq_km = SP count per square kilometer
            sq_m  = SP count per square meter
        """
        output_path = Path(output_tif)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        density_type = (density_type or "count").lower().strip()
        area_units = (area_units or "cell").lower().strip()

        if density_type not in {"count", "gaussian"}:
            raise ValueError("density_type must be 'count' or 'gaussian'.")

        if area_units not in {"cell", "sq_km", "sq_m"}:
            raise ValueError("area_units must be 'cell', 'sq_km', or 'sq_m'.")

        cells_per_radius = int(cells_per_radius or 1)
        if cells_per_radius < 1:
            cells_per_radius = 1

        if search_radius is not None:
            search_radius = float(search_radius)
            if search_radius <= 0:
                search_radius = None

        if search_radius:
            cell_size = float(search_radius) / float(cells_per_radius)

        cell_size = float(cell_size)
        if cell_size <= 0:
            raise ValueError("cell_size must be greater than zero.")

        if epsg is None and (shape_filename or shape_path):
            epsg = self.get_epsg_from_shape(
                shape_filename=shape_filename,
                shape_path=shape_path,
            )

        where = [
            "Easting IS NOT NULL",
            "Northing IS NOT NULL",
            "FireCode = ?",
        ]
        params = [fire_code]

        sql_bounds = f"""
            SELECT
                MIN(Easting) AS min_x,
                MAX(Easting) AS max_x,
                MIN(Northing) AS min_y,
                MAX(Northing) AS max_y,
                COUNT(*) AS total_points
            FROM SPSolution
            WHERE {" AND ".join(where)}
        """

        with self._connect() as conn:
            bounds = conn.execute(sql_bounds, params).fetchone()

        if not bounds or not bounds["total_points"]:
            raise ValueError(f"No SPSolution points found for FireCode={fire_code}")

        min_x = float(bounds["min_x"])
        max_x = float(bounds["max_x"])
        min_y = float(bounds["min_y"])
        max_y = float(bounds["max_y"])

        width = max(1, int((max_x - min_x) / cell_size) + 1)
        height = max(1, int((max_y - min_y) / cell_size) + 1)

        density = np.zeros((height, width), dtype=np.float32)

        sql = f"""
            SELECT
                CAST((Easting - ?) / ? AS INTEGER) AS ix,
                CAST((Northing - ?) / ? AS INTEGER) AS iy,
                COUNT(*) AS n
            FROM SPSolution
            WHERE {" AND ".join(where)}
            GROUP BY ix, iy
        """

        with self._connect() as conn:
            cur = conn.execute(
                sql,
                [min_x, cell_size, min_y, cell_size, *params],
            )

            for row in cur:
                ix = int(row["ix"])
                iy = int(row["iy"])

                if 0 <= ix < width and 0 <= iy < height:
                    density[height - 1 - iy, ix] = float(row["n"])

        raw_max_density = float(np.nanmax(density)) if density.size else 0.0

        sigma_cells = 0.0
        kernel_size = 0

        if density_type == "gaussian":
            import cv2

            if search_radius:
                sigma_cells = max(0.1, float(search_radius) / float(cell_size))
            else:
                sigma_cells = max(0.1, float(cells_per_radius))

            kernel_size = int(max(3, round(sigma_cells * 6.0)))
            if kernel_size % 2 == 0:
                kernel_size += 1

            density = cv2.GaussianBlur(
                density,
                ksize=(kernel_size, kernel_size),
                sigmaX=sigma_cells,
                sigmaY=sigma_cells,
                borderType=cv2.BORDER_CONSTANT,
            ).astype(np.float32)

        cell_area_m2 = cell_size * cell_size

        if area_units == "sq_km":
            density = density * (1_000_000.0 / cell_area_m2)
            density_units = "SP count / km²"
        elif area_units == "sq_m":
            density = density / cell_area_m2
            density_units = "SP count / m²"
        else:
            density_units = "SP count / cell"

        driver = gdal.GetDriverByName("GTiff")
        ds = driver.Create(
            str(output_path),
            width,
            height,
            1,
            gdal.GDT_Float32,
            options=[
                "TILED=YES",
                "COMPRESS=LZW",
                "BIGTIFF=YES",
            ],
        )

        geotransform = (
            min_x,
            cell_size,
            0,
            max_y,
            0,
            -cell_size,
        )

        ds.SetGeoTransform(geotransform)

        if epsg:
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(int(epsg))
            ds.SetProjection(srs.ExportToWkt())

        band = ds.GetRasterBand(1)
        band.SetNoDataValue(nodata)
        band.WriteArray(density)
        band.SetDescription(
            f"SP density FireCode={fire_code}; "
            f"type={density_type}; "
            f"units={density_units}; "
            f"cell_size={cell_size:g}"
        )
        band.FlushCache()

        ds.FlushCache()
        ds = None

        self.build_overviews(str(output_path))

        clipped_path = None

        if shape_filename or shape_path:
            clipped_path = str(output_path).replace(".tif", "_clipped.tif")
            clipped_path = self.clip_raster_by_shape(
                input_tif=str(output_path),
                output_tif=clipped_path,
                shape_filename=shape_filename,
                shape_path=shape_path,
                nodata=nodata,
            )

        return {
            "density_tif": str(output_path),
            "clipped_density_tif": clipped_path,
            "fire_code": fire_code,
            "cell_size": cell_size,
            "search_radius": search_radius,
            "cells_per_radius": cells_per_radius,
            "density_type": density_type,
            "area_units": area_units,
            "density_units": density_units,
            "epsg": epsg,
            "width": width,
            "height": height,
            "total_points": int(bounds["total_points"]),
            "raw_max_density": raw_max_density,
            "max_density": float(np.nanmax(density)) if density.size else 0.0,
            "sigma_cells": sigma_cells,
            "kernel_size": kernel_size,
        }

    def make_3d_density_surface_from_raster(
            self,
            *,
            density_tif: str,
            save_html: str | None = None,
            colorscale: str = "Turbo",
            vertical_exaggeration: float = 1.0,
            max_grid_size: int = 500,
            density_units: str = "SP count / cell",
            is_show: bool = False,
    ):
        grid_x, grid_y, density = self.read_dem_for_plotly(
            dem_tif=density_tif,
            max_grid_size=max_grid_size,
            nodata=0.0,
        )

        z_surface = density * vertical_exaggeration

        fig = go.Figure()

        fig.add_trace(
            go.Surface(
                x=grid_x,
                y=grid_y,
                z=z_surface,
                surfacecolor=density,
                colorscale=colorscale,
                colorbar=dict(title=density_units),
                connectgaps=False,
                opacity=0.96,
            )
        )

        fig.update_layout(
            title=f"SP Density 3D Surface | {density_units} | VE x{vertical_exaggeration:g}",
            scene=dict(
                xaxis_title="Easting",
                yaxis_title="Northing",
                zaxis_title=density_units,
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

    def _add_source_lines_to_plotly_fig(
            self,
            fig,
            *,
            z_level: float,
            production_only: bool = True,
            show_preplot: bool = True,
            show_solution: bool = True,
    ):
        vessel_colors = [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
            "#9467bd", "#8c564b", "#e377c2", "#17becf",
            "#bcbd22", "#7f7f7f",
        ]

        sql_preplot = """
            SELECT
                COALESCE(Line, 0) AS Line,
                COALESCE(StartX, 0.0) AS x0,
                COALESCE(StartY, 0.0) AS y0,
                COALESCE(EndX, 0.0) AS x1,
                COALESCE(EndY, 0.0) AS y1
            FROM SLPreplot
            WHERE COALESCE(StartX,0) != 0
              AND COALESCE(StartY,0) != 0
              AND COALESCE(EndX,0) != 0
              AND COALESCE(EndY,0) != 0
        """

        sql_solution = """
            SELECT
                COALESCE(sl.Line, 0) AS Line,
                COALESCE(sl.SailLine, '') AS SailLine,
                COALESCE(sl.Seq, 0) AS Seq,
                COALESCE(TRIM(pf.vessel_name), 'Unknown') AS VesselName,
                COALESCE(sl.StartX, 0.0) AS x0,
                COALESCE(sl.StartY, 0.0) AS y0,
                COALESCE(sl.EndX, 0.0) AS x1,
                COALESCE(sl.EndY, 0.0) AS y1,
                COALESCE(sl.ProductionCount, 0) AS ProdShots
            FROM SLSolution sl
            LEFT JOIN project_fleet pf
                   ON pf.id = sl.Vessel_FK
            WHERE COALESCE(sl.StartX,0) != 0
              AND COALESCE(sl.StartY,0) != 0
              AND COALESCE(sl.EndX,0) != 0
              AND COALESCE(sl.EndY,0) != 0
        """

        if production_only:
            sql_solution += " AND COALESCE(sl.ProductionCount,0) > 0 "

        with self._connect() as conn:
            preplot_rows = conn.execute(sql_preplot).fetchall()
            solution_rows = conn.execute(sql_solution).fetchall()

        if show_preplot and preplot_rows:
            x_vals = []
            y_vals = []
            z_vals = []
            hover_vals = []

            for r in preplot_rows:
                x_vals.extend([r["x0"], r["x1"], None])
                y_vals.extend([r["y0"], r["y1"], None])
                z_vals.extend([z_level, z_level, None])
                hover_vals.extend([
                    f"SLPreplot<br>Line: {r['Line']}",
                    f"SLPreplot<br>Line: {r['Line']}",
                    None,
                ])

            fig.add_trace(
                go.Scatter3d(
                    x=x_vals,
                    y=y_vals,
                    z=z_vals,
                    mode="lines",
                    name="SLPreplot planned",
                    line=dict(color="black", width=2, dash="dash"),
                    text=hover_vals,
                    hovertemplate="%{text}<extra></extra>",
                )
            )

        if show_solution and solution_rows:
            by_vessel = {}

            for r in solution_rows:
                vessel = str(r["VesselName"] or "Unknown").strip() or "Unknown"
                by_vessel.setdefault(vessel, []).append(r)

            for i, (vessel, rows) in enumerate(sorted(by_vessel.items())):
                color = vessel_colors[i % len(vessel_colors)]

                x_vals = []
                y_vals = []
                z_vals = []
                hover_vals = []

                for r in rows:
                    hover = (
                        f"SLSolution<br>"
                        f"Vessel: {vessel}<br>"
                        f"SailLine: {r['SailLine']}<br>"
                        f"Seq: {r['Seq']}<br>"
                        f"Line: {r['Line']}<br>"
                        f"ProdShots: {r['ProdShots']}"
                    )

                    x_vals.extend([r["x0"], r["x1"], None])
                    y_vals.extend([r["y0"], r["y1"], None])
                    z_vals.extend([z_level, z_level, None])
                    hover_vals.extend([hover, hover, None])

                fig.add_trace(
                    go.Scatter3d(
                        x=x_vals,
                        y=y_vals,
                        z=z_vals,
                        mode="lines",
                        name=f"SLSolution {vessel}",
                        line=dict(color=color, width=5),
                        text=hover_vals,
                        hovertemplate="%{text}<extra></extra>",
                    )
                )

        return fig