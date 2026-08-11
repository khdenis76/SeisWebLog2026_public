from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3

import numpy as np


class Bathymetry3DDataError(RuntimeError):
    pass


@dataclass(slots=True)
class BathymetryPoints:
    source: str
    x: np.ndarray
    y: np.ndarray
    z: np.ndarray
    labels: np.ndarray
    z_field: str


@dataclass(slots=True)
class ReceiverPoints3D:
    x: np.ndarray
    y: np.ndarray
    z: np.ndarray
    line: np.ndarray
    station: np.ndarray
    node: np.ndarray
    rov: np.ndarray
    position_name: str


class Bathymetry3DRepository:
    def __init__(self, project_path: str | Path) -> None:
        path = Path(project_path).expanduser().resolve()
        self.db_path = self._resolve_db_path(path)

    @staticmethod
    def _resolve_db_path(path: Path) -> Path:
        if path.is_file() and path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
            return path
        candidates = (
            path / "data" / "project.sqlite3",
            path / "project.sqlite3",
            path / "db.sqlite3",
            path.parent / "data" / "project.sqlite3",
        )
        for candidate in candidates:
            if candidate.is_file():
                return candidate.resolve()
        raise Bathymetry3DDataError(
            "Project database was not found.\nChecked:\n" + "\n".join(str(c) for c in candidates)
        )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path), timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _tables(connection: sqlite3.Connection) -> dict[str, str]:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
        ).fetchall()
        return {str(row[0]).lower(): str(row[0]) for row in rows}

    @staticmethod
    def _columns(connection: sqlite3.Connection, table: str) -> list[str]:
        return [str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')]

    @staticmethod
    def _pick(columns: list[str], aliases: tuple[str, ...]) -> str | None:
        lookup = {column.lower(): column for column in columns}
        return next((lookup[name.lower()] for name in aliases if name.lower() in lookup), None)

    @staticmethod
    def _to_float(value: object) -> float:
        try:
            result = float(value)
        except (TypeError, ValueError):
            return np.nan
        return result if np.isfinite(result) and abs(result) < 1e12 else np.nan

    def available_sources(self) -> list[str]:
        result: list[str] = []
        with self._connect() as connection:
            tables = self._tables(connection)
            if "spsolution" in tables:
                result.append("SPSolution")
            if "dsr" in tables:
                result.append("DSR")
        return result

    def dsr_lines(self) -> list[int]:
        with self._connect() as connection:
            table = self._tables(connection).get("dsr")
            if not table:
                return []
            columns = self._columns(connection, table)
            line_col = self._pick(columns, ("Line", "RLine", "ReceiverLine"))
            if not line_col:
                return []
            rows = connection.execute(
                f'SELECT DISTINCT CAST("{line_col}" AS INTEGER) '
                f'FROM "{table}" WHERE "{line_col}" IS NOT NULL ORDER BY 1'
            ).fetchall()
        return [int(row[0]) for row in rows if row[0] is not None]

    @staticmethod
    def dsr_position_options() -> list[tuple[str, tuple[str, ...], tuple[str, ...], tuple[str, ...]]]:
        return [
            (
                "Primary deployment",
                ("PrimaryEasting",),
                ("PrimaryNorthing",),
                ("PrimaryElevation", "PrimaryDepth", "WaterDepth", "Depth"),
            ),
            (
                "Secondary deployment",
                ("SecondaryEasting",),
                ("SecondaryNorthing",),
                ("SecondaryElevation", "SecondaryDepth", "WaterDepth", "Depth"),
            ),
            (
                "Primary recovery",
                ("PrimaryEasting1",),
                ("PrimaryNorthing1",),
                ("PrimaryElevation1", "PrimaryDepth1", "WaterDepth", "Depth"),
            ),
            (
                "Secondary recovery",
                ("SecondaryEasting1",),
                ("SecondaryNorthing1",),
                ("SecondaryElevation1", "SecondaryDepth1", "WaterDepth", "Depth"),
            ),
            (
                "Preplot",
                ("PreplotEasting",),
                ("PreplotNorthing",),
                ("PreplotElevation", "WaterDepth", "Depth", "Elevation"),
            ),
            (
                "Survey Manager",
                ("ActualX",),
                ("ActualY",),
                ("ActualZ", "WaterDepth", "Depth"),
            ),
        ]

    def load_sps_bathymetry(self, max_points: int = 150000) -> BathymetryPoints:
        with self._connect() as connection:
            table = self._tables(connection).get("spsolution")
            if not table:
                raise Bathymetry3DDataError("SPSolution table was not found.")
            columns = self._columns(connection, table)
            x_col = self._pick(columns, ("Easting", "X", "SPSX", "SourceEasting"))
            y_col = self._pick(columns, ("Northing", "Y", "SPSY", "SourceNorthing"))
            z_col = self._pick(
                columns,
                ("WaterDepth", "PointDepth", "Depth", "Elevation", "Z"),
            )
            line_col = self._pick(columns, ("Line", "SailLine", "LineName"))
            point_col = self._pick(columns, ("Point", "Station", "PointIdx"))
            if not x_col or not y_col or not z_col:
                raise Bathymetry3DDataError(
                    "SPSolution requires X/Y/depth fields.\nAvailable columns: "
                    + ", ".join(columns)
                )
            label_expr = "''"
            if line_col and point_col:
                label_expr = f'CAST("{line_col}" AS TEXT) || "/" || CAST("{point_col}" AS TEXT)'
            elif point_col:
                label_expr = f'CAST("{point_col}" AS TEXT)'
            rows = connection.execute(
                f'SELECT "{x_col}" AS x, "{y_col}" AS y, "{z_col}" AS z, '
                f'{label_expr} AS label FROM "{table}" '
                f'WHERE "{x_col}" IS NOT NULL AND "{y_col}" IS NOT NULL AND "{z_col}" IS NOT NULL'
            ).fetchall()
        return self._rows_to_bathymetry(rows, "SPSolution", z_col, max_points)

    def load_dsr_bathymetry(
        self,
        position_name: str,
        line: int | None = None,
        max_points: int = 150000,
    ) -> BathymetryPoints:
        with self._connect() as connection:
            table = self._tables(connection).get("dsr")
            if not table:
                raise Bathymetry3DDataError("DSR table was not found.")
            columns = self._columns(connection, table)
            option = next(
                (item for item in self.dsr_position_options() if item[0] == position_name),
                self.dsr_position_options()[0],
            )
            x_col = self._pick(columns, option[1])
            y_col = self._pick(columns, option[2])
            z_col = self._pick(columns, option[3])
            line_col = self._pick(columns, ("Line", "RLine", "ReceiverLine"))
            station_col = self._pick(columns, ("Station", "LinePoint", "Point"))
            if not x_col or not y_col or not z_col:
                raise Bathymetry3DDataError(
                    f"{position_name} does not have X/Y/Z fields in DSR.\nAvailable columns: "
                    + ", ".join(columns)
                )
            label_expr = "''"
            if line_col and station_col:
                label_expr = f'CAST("{line_col}" AS TEXT) || "/" || CAST("{station_col}" AS TEXT)'
            where = [f'"{x_col}" IS NOT NULL', f'"{y_col}" IS NOT NULL', f'"{z_col}" IS NOT NULL']
            params: list[object] = []
            if line is not None and line_col:
                where.append(f'CAST("{line_col}" AS INTEGER)=?')
                params.append(int(line))
            rows = connection.execute(
                f'SELECT "{x_col}" AS x, "{y_col}" AS y, "{z_col}" AS z, '
                f'{label_expr} AS label FROM "{table}" WHERE ' + " AND ".join(where),
                params,
            ).fetchall()
        return self._rows_to_bathymetry(rows, f"DSR — {position_name}", z_col, max_points)

    def _rows_to_bathymetry(
        self,
        rows: list[sqlite3.Row],
        source: str,
        z_field: str,
        max_points: int,
    ) -> BathymetryPoints:
        if not rows:
            raise Bathymetry3DDataError(f"No valid bathymetry records were found in {source}.")
        x = np.asarray([self._to_float(row["x"]) for row in rows], dtype=float)
        y = np.asarray([self._to_float(row["y"]) for row in rows], dtype=float)
        z = np.asarray([self._to_float(row["z"]) for row in rows], dtype=float)
        labels = np.asarray([str(row["label"] or "") for row in rows], dtype=object)
        valid = np.isfinite(x) & np.isfinite(y) & np.isfinite(z)
        x, y, z, labels = x[valid], y[valid], z[valid], labels[valid]
        if x.size < 3:
            raise Bathymetry3DDataError(f"{source} has fewer than three valid bathymetry points.")
        if max_points > 0 and x.size > max_points:
            indices = np.linspace(0, x.size - 1, max_points, dtype=np.int64)
            x, y, z, labels = x[indices], y[indices], z[indices], labels[indices]
        return BathymetryPoints(source, x, y, z, labels, z_field)

    def load_receivers(
        self,
        position_name: str,
        line: int | None = None,
        max_points: int = 200000,
    ) -> ReceiverPoints3D:
        with self._connect() as connection:
            table = self._tables(connection).get("dsr")
            if not table:
                raise Bathymetry3DDataError("DSR table was not found.")
            columns = self._columns(connection, table)
            option = next(
                (item for item in self.dsr_position_options() if item[0] == position_name),
                self.dsr_position_options()[0],
            )
            x_col = self._pick(columns, option[1])
            y_col = self._pick(columns, option[2])
            z_col = self._pick(columns, option[3])
            line_col = self._pick(columns, ("Line", "RLine", "ReceiverLine"))
            station_col = self._pick(columns, ("Station", "LinePoint", "Point"))
            node_col = self._pick(columns, ("Node", "NODE_HEX_ID", "RemoteUnit"))
            rov_col = self._pick(columns, ("ROV", "ROV1"))
            if not x_col or not y_col:
                raise Bathymetry3DDataError(f"{position_name} X/Y fields were not found in DSR.")
            selections = [
                f'"{x_col}" AS x',
                f'"{y_col}" AS y',
                f'"{z_col}" AS z' if z_col else "NULL AS z",
                f'"{line_col}" AS line' if line_col else "NULL AS line",
                f'"{station_col}" AS station' if station_col else "NULL AS station",
                f'"{node_col}" AS node' if node_col else "'' AS node",
                f'"{rov_col}" AS rov' if rov_col else "'' AS rov",
            ]
            where = [f'"{x_col}" IS NOT NULL', f'"{y_col}" IS NOT NULL']
            params: list[object] = []
            if line is not None and line_col:
                where.append(f'CAST("{line_col}" AS INTEGER)=?')
                params.append(int(line))
            order = f' ORDER BY CAST("{line_col}" AS REAL), CAST("{station_col}" AS REAL)' if line_col and station_col else ""
            rows = connection.execute(
                f'SELECT {", ".join(selections)} FROM "{table}" WHERE ' + " AND ".join(where) + order,
                params,
            ).fetchall()
        if not rows:
            raise Bathymetry3DDataError("No DSR receiver positions were found.")
        x = np.asarray([self._to_float(row["x"]) for row in rows], float)
        y = np.asarray([self._to_float(row["y"]) for row in rows], float)
        z = np.asarray([self._to_float(row["z"]) for row in rows], float)
        lines = np.asarray([self._to_float(row["line"]) for row in rows], float)
        stations = np.asarray([self._to_float(row["station"]) for row in rows], float)
        nodes = np.asarray([str(row["node"] or "") for row in rows], object)
        rovs = np.asarray([str(row["rov"] or "") for row in rows], object)
        valid = np.isfinite(x) & np.isfinite(y)
        x, y, z, lines, stations, nodes, rovs = (
            x[valid], y[valid], z[valid], lines[valid], stations[valid], nodes[valid], rovs[valid]
        )
        if max_points > 0 and x.size > max_points:
            indices = np.linspace(0, x.size - 1, max_points, dtype=np.int64)
            x, y, z, lines, stations, nodes, rovs = (
                x[indices], y[indices], z[indices], lines[indices], stations[indices], nodes[indices], rovs[indices]
            )
        return ReceiverPoints3D(x, y, z, lines, stations, nodes, rovs, position_name)
