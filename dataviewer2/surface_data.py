from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Sequence

import numpy as np


class SurfaceDataError(RuntimeError):
    pass


@dataclass(slots=True)
class SurfacePoints:
    source: str
    x_field: str
    y_field: str
    z_field: str
    x: np.ndarray
    y: np.ndarray
    z: np.ndarray
    color_values: np.ndarray
    labels: np.ndarray
    metadata: dict[str, np.ndarray]


class SurfaceDataRepository:
    """Generic field reader shared by the 2D and 3D surface workbenches."""

    SOURCE_NAMES = ("SPSolution", "DSR", "REC_DB")

    def __init__(self, project_path: str | Path) -> None:
        path = Path(project_path).expanduser().resolve()
        if path.is_file():
            self.db_path = path
            self.project_path = (
                path.parent.parent if path.parent.name.lower() == "data" else path.parent
            )
        else:
            self.project_path = path
            candidates = (
                path / "data" / "project.sqlite3",
                path / "project.sqlite3",
                path / "db.sqlite3",
            )
            self.db_path = next((p for p in candidates if p.is_file()), candidates[0])
        if not self.db_path.is_file():
            raise SurfaceDataError(f"Project database not found: {self.db_path}")
        self._production_code: str | None = None
        self._production_code_loaded = False

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_path), timeout=30)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _tables(con: sqlite3.Connection) -> dict[str, str]:
        return {
            str(row[0]).lower(): str(row[0])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
            )
        }

    @staticmethod
    def _columns(con: sqlite3.Connection, table: str) -> list[str]:
        return [str(row[1]) for row in con.execute(f'PRAGMA table_info("{table}")')]

    def available_sources(self) -> list[str]:
        with self._connect() as con:
            tables = self._tables(con)
        return [name for name in self.SOURCE_NAMES if name.lower() in tables]

    def columns(self, source: str) -> list[str]:
        with self._connect() as con:
            table = self._tables(con).get(source.lower())
            return self._columns(con, table) if table else []

    def production_code(self) -> str | None:
        """Return project_geometry.production_code, when configured."""
        if self._production_code_loaded:
            return self._production_code
        self._production_code_loaded = True
        try:
            with self._connect() as con:
                table = self._tables(con).get("project_geometry")
                if not table:
                    return None
                columns = {c.lower(): c for c in self._columns(con, table)}
                field = columns.get("production_code")
                if not field:
                    return None
                row = con.execute(
                    f'SELECT "{field}" FROM "{table}" '
                    f'WHERE "{field}" IS NOT NULL LIMIT 1'
                ).fetchone()
                if row and row[0] is not None:
                    value = str(row[0]).strip()
                    self._production_code = value or None
        except sqlite3.Error:
            self._production_code = None
        return self._production_code


    def production_fire_codes(self) -> list[str]:
        """Return individual production FireCode characters.

        Example: project_geometry.production_code='AP' means both A and P
        are production points. Separators and whitespace are ignored.
        """
        value = self.production_code() or ""
        return list(dict.fromkeys(ch.upper() for ch in value if ch.isalnum()))

    def _production_where(self, columns: list[str]) -> tuple[str, list[object]]:
        if "FireCode" not in columns:
            return "", []
        codes = self.production_fire_codes()
        if not codes:
            return "", []
        placeholders = ", ".join("?" for _ in codes)
        return (
            f'UPPER(TRIM("FireCode")) IN ({placeholders})',
            list(codes),
        )

    def source_note(self, source: str) -> str:
        if source == "SPSolution":
            code = self.production_code()
            if code and "FireCode" in self.columns(source):
                return f"Production shots only: FireCode in {', '.join(self.production_fire_codes())}"
        return ""

    def numeric_columns(self, source: str, sample_size: int = 200) -> list[str]:
        cols = self.columns(source)
        if not cols:
            return []
        with self._connect() as con:
            table = self._tables(con).get(source.lower())
            where = ""
            params: list[object] = []
            if source == "SPSolution":
                clause, prod_params = self._production_where(cols)
                if clause:
                    where = " WHERE " + clause
                    params.extend(prod_params)
            rows = con.execute(
                f'SELECT * FROM "{table}"{where} LIMIT {int(sample_size)}', params
            ).fetchall()
        result: list[str] = []
        for col in cols:
            good = 0
            seen = 0
            for row in rows:
                value = row[col]
                if value is None or value == "":
                    continue
                seen += 1
                try:
                    number = float(value)
                    if np.isfinite(number) and abs(number) < 1e12:
                        good += 1
                except (TypeError, ValueError):
                    pass
            if seen == 0 or good / max(seen, 1) >= 0.6:
                result.append(col)
        return result

    def default_fields(self, source: str) -> tuple[str, str, str]:
        cols = self.columns(source)
        lookup = {c.lower(): c for c in cols}

        def pick(*names: str) -> str:
            return next(
                (lookup[name.lower()] for name in names if name.lower() in lookup), ""
            )

        if source == "SPSolution":
            return (
                pick("Easting", "X", "SourceEasting"),
                pick("Northing", "Y", "SourceNorthing"),
                pick("WaterDepth", "PointDepth", "Depth", "Elevation", "Z"),
            )
        if source == "REC_DB":
            return (
                pick("REC_X", "Easting", "X"),
                pick("REC_Y", "Northing", "Y"),
                pick("REC_Z", "WaterDepth", "Elevation", "Z"),
            )
        return (
            pick("PreplotEasting", "PrimaryEasting", "ActualX", "Easting"),
            pick("PreplotNorthing", "PrimaryNorthing", "ActualY", "Northing"),
            pick(
                "PrimaryElevation",
                "PrimaryElevation1",
                "SecondaryElevation",
                "SecondaryElevation1",
                "WaterDepth",
                "ActualZ",
                "Elevation",
                "Z",
            ),
        )

    def coordinate_pairs(self, source: str) -> list[tuple[str, str, str]]:
        """Return valid named X/Y pairs for surface construction."""
        columns = set(self.columns(source))
        candidates: list[tuple[str, str, str]]
        if source == "SPSolution":
            candidates = [
                ("SPS Easting / Northing", "Easting", "Northing"),
                ("Source Easting / Northing", "SourceEasting", "SourceNorthing"),
            ]
        elif source == "DSR":
            candidates = [
                ("Receiver preplot", "PreplotEasting", "PreplotNorthing"),
                ("Primary deployment", "PrimaryEasting", "PrimaryNorthing"),
                ("Secondary deployment", "SecondaryEasting", "SecondaryNorthing"),
                ("Primary recovery", "PrimaryEasting1", "PrimaryNorthing1"),
                ("Secondary recovery", "SecondaryEasting1", "SecondaryNorthing1"),
                ("Actual coordinates", "ActualX", "ActualY"),
            ]
        else:
            candidates = [
                ("Recovery database", "REC_X", "REC_Y"),
                ("Easting / Northing", "Easting", "Northing"),
            ]
        return [item for item in candidates if item[1] in columns and item[2] in columns]

    def label_candidates(self, source: str) -> list[str]:
        """Return all fields, with operational identifiers first."""
        cols = self.columns(source)
        preferred = [
            "Line",
            "Station",
            "Point",
            "Node",
            "ROV",
            "ROV1",
            "REC_ID",
            "FireCode",
            "TimeStamp",
            "TimeStamp1",
            "WaterDepth",
            "PrimaryRadial",
        ]
        ordered = [c for c in preferred if c in cols]
        ordered.extend(c for c in cols if c not in ordered)
        return ordered

    @staticmethod
    def _compose_label(row: sqlite3.Row, fields: Sequence[str], separator: str) -> str:
        values: list[str] = []
        for field in fields:
            if not field or field not in row.keys():
                continue
            value = row[field]
            if value is None or str(value).strip() == "":
                continue
            values.append(str(value))
        return separator.join(values)


    def load_sps_density_grid(
        self,
        *,
        cell_size: float = 100.0,
        density_type: str = "count",
        area_units: str = "cell",
        search_radius: float | None = None,
        cells_per_radius: int = 1,
        max_cells: int = 2_000_000,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict]:
        """Build an SPS production-shot density grid in memory.

        ``project_geometry.production_code`` is treated as a set of FireCode
        characters, so ``AP`` includes both A and P rows.
        """
        if "SPSolution" not in self.available_sources():
            raise SurfaceDataError("SPSolution table is not available.")
        cell_size = float(cell_size)
        if search_radius is not None and float(search_radius) > 0:
            cell_size = float(search_radius) / max(1, int(cells_per_radius))
        if cell_size <= 0:
            raise SurfaceDataError("Cell size must be greater than zero.")
        density_type = (density_type or "count").strip().lower()
        area_units = (area_units or "cell").strip().lower()
        if density_type not in {"count", "gaussian"}:
            raise SurfaceDataError("Density type must be Count or Gaussian.")
        if area_units not in {"cell", "sq_km", "sq_m"}:
            raise SurfaceDataError("Density units must be cell, sq_km, or sq_m.")
        cols = self.columns("SPSolution")
        clause, params = self._production_where(cols)
        where = [
            '"Easting" IS NOT NULL', '"Northing" IS NOT NULL',
            clause or "1=1",
        ]
        with self._connect() as con:
            rows = con.execute(
                'SELECT "Easting", "Northing" FROM "SPSolution" WHERE ' + " AND ".join(where),
                params,
            ).fetchall()
        if not rows:
            raise SurfaceDataError("No production SPSolution points were found.")
        xy = np.asarray([(float(r[0]), float(r[1])) for r in rows], dtype=float)
        finite = np.isfinite(xy).all(axis=1)
        xy = xy[finite]
        if xy.size == 0:
            raise SurfaceDataError("No valid production SPS coordinates were found.")
        xmin, ymin = np.nanmin(xy, axis=0)
        xmax, ymax = np.nanmax(xy, axis=0)
        nx = max(1, int(np.ceil((xmax-xmin)/cell_size)) + 1)
        ny = max(1, int(np.ceil((ymax-ymin)/cell_size)) + 1)
        if nx * ny > int(max_cells):
            scale = (nx * ny / float(max_cells)) ** 0.5
            cell_size *= scale
            nx = max(1, int(np.ceil((xmax-xmin)/cell_size)) + 1)
            ny = max(1, int(np.ceil((ymax-ymin)/cell_size)) + 1)
        counts, xedges, yedges = np.histogram2d(
            xy[:, 0], xy[:, 1], bins=(nx, ny),
            range=((xmin, xmin + nx*cell_size), (ymin, ymin + ny*cell_size)),
        )
        grid = counts.T.astype(float)
        sigma_cells = 0.0
        if density_type == "gaussian":
            if search_radius is not None and float(search_radius) > 0:
                sigma_cells = max(0.1, float(search_radius) / cell_size)
            else:
                sigma_cells = max(0.8, float(cells_per_radius))
            try:
                from scipy.ndimage import gaussian_filter
                grid = gaussian_filter(grid, sigma=sigma_cells, mode="constant")
            except Exception:
                # Small NumPy separable Gaussian fallback.
                radius = max(1, int(round(sigma_cells * 3)))
                x = np.arange(-radius, radius + 1, dtype=float)
                kernel = np.exp(-(x*x)/(2*sigma_cells*sigma_cells))
                kernel /= kernel.sum()
                grid = np.apply_along_axis(lambda a: np.convolve(a, kernel, mode="same"), 0, grid)
                grid = np.apply_along_axis(lambda a: np.convolve(a, kernel, mode="same"), 1, grid)
        cell_area = cell_size * cell_size
        if area_units == "sq_km":
            grid *= 1_000_000.0 / cell_area
            units = "SP count / km²"
        elif area_units == "sq_m":
            grid /= cell_area
            units = "SP count / m²"
        else:
            units = "SP count / cell"
        gx = (xedges[:-1] + xedges[1:]) * 0.5
        gy = (yedges[:-1] + yedges[1:]) * 0.5
        info = {
            "cell_size": float(cell_size), "density_type": density_type,
            "units": units, "total_points": int(xy.shape[0]),
            "production_codes": self.production_fire_codes(),
            "sigma_cells": float(sigma_cells),
        }
        return gx, gy, grid, info

    def load_sps_production_overlay(self, max_points: int = 300000) -> SurfacePoints:
        x, y, z = self.default_fields("SPSolution")
        z = "PointDepth" if "PointDepth" in self.columns("SPSolution") else z
        return self.load_points(
            "SPSolution", x, y, z,
            color_field="FireCode" if "FireCode" in self.columns("SPSolution") else z,
            label_fields=[f for f in ("Line", "Point", "FireCode") if f in self.columns("SPSolution")],
            metadata_fields=self.label_candidates("SPSolution"),
            max_points=max_points,
        )

    def load_slsolution_segments(self) -> list[dict]:
        """Return valid SLSolution start/end segments with vessel metadata."""
        with self._connect() as con:
            tables = self._tables(con)
            table = tables.get("slsolution")
            if not table:
                raise SurfaceDataError("SLSolution table is not available.")
            cols = self._columns(con, table)
            required = {"StartX", "StartY", "EndX", "EndY"}
            if not required.issubset(cols):
                raise SurfaceDataError("SLSolution does not contain StartX/StartY/EndX/EndY.")
            has_fleet = "project_fleet" in tables and "Vessel_FK" in cols
            select = [
                'sl."StartX"', 'sl."StartY"', 'sl."EndX"', 'sl."EndY"',
                'sl."Line"' if "Line" in cols else 'NULL AS "Line"',
                'sl."SailLine"' if "SailLine" in cols else 'NULL AS "SailLine"',
                'sl."Seq"' if "Seq" in cols else 'NULL AS "Seq"',
                'sl."ProductionCount"' if "ProductionCount" in cols else 'NULL AS "ProductionCount"',
            ]
            join = ""
            if has_fleet:
                fleet_table = tables["project_fleet"]
                fleet_cols = self._columns(con, fleet_table)
                vessel_name_col = next((c for c in ("vessel_name", "name", "VesselName") if c in fleet_cols), None)
                if vessel_name_col:
                    select.append(f'COALESCE(pf."{vessel_name_col}", "Unknown") AS "Vessel"')
                else:
                    select.append('CAST(sl."Vessel_FK" AS TEXT) AS "Vessel"')
                join = f' LEFT JOIN "{fleet_table}" pf ON pf."id" = sl."Vessel_FK" '
            else:
                select.append('"Unknown" AS "Vessel"')
            sql = (
                'SELECT ' + ', '.join(select) + f' FROM "{table}" sl ' + join +
                ' WHERE sl."StartX" IS NOT NULL AND sl."StartY" IS NOT NULL '
                'AND sl."EndX" IS NOT NULL AND sl."EndY" IS NOT NULL'
            )
            rows = con.execute(sql).fetchall()
        result=[]
        for r in rows:
            vals=[r["StartX"],r["StartY"],r["EndX"],r["EndY"]]
            try: vals=[float(v) for v in vals]
            except Exception: continue
            if not np.isfinite(vals).all(): continue
            result.append({
                "x0":vals[0],"y0":vals[1],"x1":vals[2],"y1":vals[3],
                "line":r["Line"],"sailline":r["SailLine"],"seq":r["Seq"],
                "production_count":r["ProductionCount"],"vessel":r["Vessel"] or "Unknown",
            })
        return result

    def load_points(
        self,
        source: str,
        x_field: str,
        y_field: str,
        z_field: str,
        *,
        color_field: str | None = None,
        label_field: str | None = None,
        label_fields: Sequence[str] | None = None,
        label_separator: str = " / ",
        metadata_fields: Sequence[str] | None = None,
        line_filter: int | None = None,
        max_points: int = 200000,
    ) -> SurfacePoints:
        columns = self.columns(source)
        if not all(field in columns for field in (x_field, y_field, z_field)):
            raise SurfaceDataError(f"Selected fields are not available in {source}.")

        color_field = color_field if color_field in columns else z_field
        line_col = next(
            (
                c
                for c in ("Line", "RLine", "ReceiverLine", "SailLine")
                if c in columns
            ),
            None,
        )
        requested_labels = list(label_fields or ())
        if label_field:
            requested_labels.insert(0, label_field)
        requested_labels = [
            field for field in dict.fromkeys(requested_labels) if field in columns
        ]

        metadata_preferred = (
            "Line",
            "Station",
            "Point",
            "Node",
            "ROV",
            "ROV1",
            "FireCode",
            "TimeStamp",
            "TimeStamp1",
            "WaterDepth",
            "ActualZ",
            "PrimaryRadial",
        )
        metadata_cols = [c for c in metadata_preferred if c in columns]
        for field in metadata_fields or ():
            if field in columns and field not in metadata_cols:
                metadata_cols.append(field)
        select_cols: list[str] = []
        for column in (
            x_field,
            y_field,
            z_field,
            color_field,
            *requested_labels,
            *metadata_cols,
        ):
            if column and column not in select_cols:
                select_cols.append(column)

        with self._connect() as con:
            table = self._tables(con).get(source.lower())
            if not table:
                raise SurfaceDataError(f"Source table is not available: {source}")
            where = [
                f'"{x_field}" IS NOT NULL',
                f'"{y_field}" IS NOT NULL',
                f'"{z_field}" IS NOT NULL',
            ]
            params: list[object] = []
            if line_filter is not None and line_col:
                where.append(f'CAST("{line_col}" AS INTEGER)=?')
                params.append(int(line_filter))
            if source == "SPSolution":
                clause, prod_params = self._production_where(columns)
                if clause:
                    where.append(clause)
                    params.extend(prod_params)
            quoted_columns = ", ".join(f'"{column}"' for column in select_cols)
            sql = (
                f'SELECT {quoted_columns} FROM "{table}" WHERE '
                + " AND ".join(where)
            )
            rows = con.execute(sql, params).fetchall()

        if not rows:
            suffix = ""
            if source == "SPSolution" and self.production_code():
                suffix = f" for production code {self.production_code()}"
            raise SurfaceDataError(f"No valid records found in {source}{suffix}.")

        def num(value: object) -> float:
            try:
                number = float(value)
                return number if np.isfinite(number) and abs(number) < 1e12 else np.nan
            except (TypeError, ValueError):
                return np.nan

        x = np.asarray([num(row[x_field]) for row in rows], dtype=float)
        y = np.asarray([num(row[y_field]) for row in rows], dtype=float)
        z = np.asarray([num(row[z_field]) for row in rows], dtype=float)
        color_values = np.asarray(
            [num(row[color_field]) for row in rows], dtype=float
        )
        labels = np.asarray(
            [
                self._compose_label(row, requested_labels, label_separator)
                for row in rows
            ],
            dtype=object,
        )
        metadata = {
            column: np.asarray([row[column] for row in rows], dtype=object)
            for column in metadata_cols
        }
        valid = np.isfinite(x) & np.isfinite(y) & np.isfinite(z)
        x, y, z = x[valid], y[valid], z[valid]
        color_values, labels = color_values[valid], labels[valid]
        metadata = {key: values[valid] for key, values in metadata.items()}
        if x.size < 1:
            raise SurfaceDataError(f"No finite points found in {source}.")
        if max_points > 0 and x.size > max_points:
            indices = np.linspace(0, x.size - 1, max_points, dtype=np.int64)
            x, y, z = x[indices], y[indices], z[indices]
            color_values, labels = color_values[indices], labels[indices]
            metadata = {key: values[indices] for key, values in metadata.items()}
        return SurfacePoints(
            source,
            x_field,
            y_field,
            z_field,
            x,
            y,
            z,
            color_values,
            labels,
            metadata,
        )

    def line_values(self, source: str) -> list[int]:
        columns = self.columns(source)
        line = next(
            (
                c
                for c in ("Line", "RLine", "ReceiverLine", "SailLine")
                if c in columns
            ),
            None,
        )
        if not line:
            return []
        with self._connect() as con:
            table = self._tables(con).get(source.lower())
            where = [f'"{line}" IS NOT NULL']
            params: list[object] = []
            if source == "SPSolution":
                clause, prod_params = self._production_where(columns)
                if clause:
                    where.append(clause)
                    params.extend(prod_params)
            rows = con.execute(
                f'SELECT DISTINCT CAST("{line}" AS INTEGER) FROM "{table}" '
                f'WHERE {" AND ".join(where)} ORDER BY 1',
                params,
            ).fetchall()
        return [int(row[0]) for row in rows if row[0] is not None]
