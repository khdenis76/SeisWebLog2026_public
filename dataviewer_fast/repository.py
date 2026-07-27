from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

import numpy as np

from .models import PointLayerData, ProjectShapeDefinition


class ProjectRepositoryError(RuntimeError):
    pass


class ProjectRepository:
    """Read only the columns required by the map and return NumPy arrays."""

    def __init__(self, project_path: str | Path) -> None:
        project_path = Path(project_path).expanduser().resolve()
        self.db_path = project_path if project_path.suffix.lower() in {".sqlite", ".sqlite3", ".db"} else project_path / "data" / "project.sqlite3"
        if not self.db_path.exists():
            raise ProjectRepositoryError(f"Project database not found: {self.db_path}")

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path), timeout=15.0, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA temp_store = MEMORY")
        return connection

    @staticmethod
    def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
        return {str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')}

    @staticmethod
    def _first(columns: set[str], candidates: Iterable[str]) -> str | None:
        return next((name for name in candidates if name in columns), None)

    def project_epsg(self) -> str:
        with self._connect() as connection:
            try:
                row = connection.execute("SELECT epsg FROM project_main LIMIT 1").fetchone()
            except sqlite3.Error:
                return ""
        return "" if not row or row[0] is None else str(row[0])

    def load_rp_preplot(self) -> PointLayerData:
        with self._connect() as connection:
            columns = self._table_columns(connection, "RPPreplot")
            x_col = self._first(columns, ("X", "Easting", "PreplotEasting"))
            y_col = self._first(columns, ("Y", "Northing", "PreplotNorthing"))
            if not x_col or not y_col:
                raise ProjectRepositoryError("RPPreplot coordinate columns were not found.")
            line_col = self._first(columns, ("Line", "RLine", "ReceiverLine"))
            point_col = self._first(columns, ("Point", "Station", "LinePoint"))
            select = ["rowid AS source_index", f'CAST("{x_col}" AS REAL) AS x', f'CAST("{y_col}" AS REAL) AS y']
            if line_col:
                select.append(f'"{line_col}" AS line')
            if point_col:
                select.append(f'"{point_col}" AS point')
            order = ", ".join(filter(None, [f'"{line_col}"' if line_col else None, f'"{point_col}"' if point_col else None]))
            sql = f'SELECT {", ".join(select)} FROM RPPreplot WHERE "{x_col}" IS NOT NULL AND "{y_col}" IS NOT NULL'
            if order:
                sql += f" ORDER BY {order}"
            rows = connection.execute(sql).fetchall()
        return self._rows_to_layer("RPPreplot", rows)

    def load_dsr_layer(self, mode: str = "primary", line: int | None = None) -> PointLayerData:
        modes = {
            "preplot": ("PreplotEasting", "PreplotNorthing"),
            "primary": ("PrimaryEasting", "PrimaryNorthing"),
            "secondary": ("SecondaryEasting", "SecondaryNorthing"),
            "recovery_primary": ("PrimaryEasting1", "PrimaryNorthing1"),
            "recovery_secondary": ("SecondaryEasting1", "SecondaryNorthing1"),
        }
        if mode not in modes:
            raise ValueError(f"Unsupported DSR mode: {mode}")
        x_col, y_col = modes[mode]
        with self._connect() as connection:
            columns = self._table_columns(connection, "DSR")
            if x_col not in columns or y_col not in columns:
                return PointLayerData(mode, np.array([], float), np.array([], float), np.array([], np.int64))
            optional = [name for name in ("ID", "Line", "Station", "LinePoint", "Node", "ROV", "ROV1", "TimeStamp", "TimeStamp1") if name in columns]
            select = ["rowid AS source_index", f'CAST("{x_col}" AS REAL) AS x', f'CAST("{y_col}" AS REAL) AS y'] + [f'"{name}" AS "{name.lower()}"' for name in optional]
            where = [f'"{x_col}" IS NOT NULL', f'"{y_col}" IS NOT NULL']
            params: list[object] = []
            if line is not None and "Line" in columns:
                where.append('"Line" = ?')
                params.append(int(line))
            order_parts = [name for name in ("Line", "LinePoint", "Station") if name in columns]
            sql = f'SELECT {", ".join(select)} FROM DSR WHERE {" AND ".join(where)}'
            if order_parts:
                sql += " ORDER BY " + ", ".join(f'"{name}"' for name in order_parts)
            rows = connection.execute(sql, params).fetchall()
        return self._rows_to_layer(f"DSR {mode.replace('_', ' ').title()}", rows)

    def load_rec_db(self, line: int | None = None) -> PointLayerData:
        with self._connect() as connection:
            columns = self._table_columns(connection, "REC_DB")
            x_col = self._first(columns, ("REC_X", "X", "Easting"))
            y_col = self._first(columns, ("REC_Y", "Y", "Northing"))
            if not x_col or not y_col:
                return PointLayerData("REC_DB", np.array([], float), np.array([], float), np.array([], np.int64))
            optional = [name for name in ("ID", "REC_ID", "Line", "Station", "DEPLOY", "RPI", "REC_Z") if name in columns]
            select = ["rowid AS source_index", f'CAST("{x_col}" AS REAL) AS x', f'CAST("{y_col}" AS REAL) AS y'] + [f'"{name}" AS "{name.lower()}"' for name in optional]
            where = [f'"{x_col}" IS NOT NULL', f'"{y_col}" IS NOT NULL']
            params: list[object] = []
            if line is not None and "Line" in columns:
                where.append('"Line" = ?')
                params.append(int(line))
            rows = connection.execute(f'SELECT {", ".join(select)} FROM REC_DB WHERE {" AND ".join(where)}', params).fetchall()
        return self._rows_to_layer("REC_DB", rows)


    def load_shape_definitions(self) -> list[ProjectShapeDefinition]:
        """Read database-registered shapefiles and their display styles.

        The project schema has changed names over time, so this method finds
        the shape table by its columns instead of hard-coding one table name.
        """
        with self._connect() as connection:
            tables = [str(row[0]) for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )]
            shape_table: str | None = None
            shape_columns: set[str] = set()
            full_name_column: str | None = None

            for table in tables:
                columns = self._table_columns(connection, table)
                by_lower = {column.lower(): column for column in columns}
                candidate = next((by_lower[key] for key in ("full_name", "fullname", "file_name", "filename", "path") if key in by_lower), None)
                style_hits = sum(key in by_lower for key in ("line_color", "fill_color", "line_width", "is_filled", "line_style"))
                table_hint = "shape" in table.lower()
                if candidate and (style_hits >= 2 or table_hint):
                    shape_table = table
                    shape_columns = columns
                    full_name_column = candidate
                    break

            if not shape_table or not full_name_column:
                return []

            by_lower = {column.lower(): column for column in shape_columns}
            def col(*names: str) -> str | None:
                return next((by_lower[name.lower()] for name in names if name.lower() in by_lower), None)

            selected = [f'"{full_name_column}" AS full_name']
            aliases = {
                "is_filled": col("is_filled", "filled"),
                "fill_color": col("fill_color", "fillcolour"),
                "line_color": col("line_color", "linecolour", "color", "colour"),
                "line_width": col("line_width", "linewidth", "width"),
                "line_style": col("line_style", "line_dashed", "linestyle"),
                "hatch_pattern": col("hatch_pattern", "hatch"),
                "display_name": col("name", "shape_name", "display_name", "title"),
            }
            for alias, column in aliases.items():
                if column:
                    selected.append(f'"{column}" AS "{alias}"')

            rows = connection.execute(
                f'SELECT {", ".join(selected)} FROM "{shape_table}" '
                f'WHERE "{full_name_column}" IS NOT NULL AND TRIM("{full_name_column}") <> "" '
                f'ORDER BY "{full_name_column}"'
            ).fetchall()

        definitions: list[ProjectShapeDefinition] = []
        for row in rows:
            values = dict(row)
            raw_path = Path(str(values["full_name"]).strip()).expanduser()
            if not raw_path.is_absolute():
                raw_path = (self.db_path.parent.parent / raw_path).resolve()
            display_name = str(values.get("display_name") or raw_path.stem)
            try:
                width = float(values.get("line_width") or 1.0)
            except (TypeError, ValueError):
                width = 1.0
            definitions.append(ProjectShapeDefinition(
                name=display_name,
                full_name=raw_path,
                is_filled=bool(values.get("is_filled") or False),
                fill_color=str(values.get("fill_color") or "#000000"),
                line_color=str(values.get("line_color") or "#000000"),
                line_width=max(0.5, width),
                line_style=str(values.get("line_style") or "solid"),
                hatch_pattern=str(values.get("hatch_pattern") or ""),
            ))
        return definitions

    @staticmethod
    def _rows_to_layer(name: str, rows: list[sqlite3.Row]) -> PointLayerData:
        count = len(rows)
        if count == 0:
            return PointLayerData(name, np.array([], float), np.array([], float), np.array([], np.int64))
        keys = rows[0].keys()
        x = np.fromiter((row["x"] for row in rows), dtype=np.float64, count=count)
        y = np.fromiter((row["y"] for row in rows), dtype=np.float64, count=count)
        source = np.fromiter((row["source_index"] for row in rows), dtype=np.int64, count=count)
        metadata: dict[str, np.ndarray] = {}
        for key in keys:
            if key not in {"x", "y", "source_index"}:
                metadata[key] = np.asarray([row[key] for row in rows], dtype=object)
        return PointLayerData(name, x, y, source, metadata)
