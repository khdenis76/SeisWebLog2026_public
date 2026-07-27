from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

import numpy as np

from .models import PointLayerData, ProjectShapeDefinition, BlackBoxData, BlackBoxFileInfo


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
                "source_epsg": col("epsg", "source_epsg", "shape_epsg", "crs", "srid"),
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
                source_epsg=str(values.get("source_epsg") or ""),
            ))
        return definitions


    def _find_blackbox_tables(self, connection: sqlite3.Connection) -> tuple[str | None, str | None]:
        tables = [str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        lower = {name.lower(): name for name in tables}
        files = lower.get("blackbox_files")
        data = lower.get("blackbox")
        if data is None:
            data = next((name for name in tables if "blackbox" in name.lower() and "file" not in name.lower()), None)
        if files is None:
            files = next((name for name in tables if "blackbox" in name.lower() and "file" in name.lower()), None)
        return files, data

    def list_blackbox_files(self) -> list[BlackBoxFileInfo]:
        with self._connect() as connection:
            files_table, data_table = self._find_blackbox_tables(connection)
            if data_table is None:
                return []
            data_columns = self._table_columns(connection, data_table)
            fk = self._first(data_columns, ("File_FK", "FileID", "FileId", "file_id", "BlackBoxFile_FK"))
            time_col = self._first(data_columns, ("TimeStamp", "Timestamp", "DateTime", "datetime", "Time", "time"))
            if files_table:
                file_columns = self._table_columns(connection, files_table)
                id_col = self._first(file_columns, ("ID", "Id", "id", "FileID", "file_id"))
                name_col = self._first(file_columns, ("FileName", "Filename", "Name", "name", "FullName", "Path"))
                if id_col:
                    select_name = f'f."{name_col}"' if name_col else f'CAST(f."{id_col}" AS TEXT)'
                    joins = ""
                    count_expr = "0"
                    start_expr = "''"
                    end_expr = "''"
                    if fk:
                        joins = f' LEFT JOIN "{data_table}" d ON d."{fk}" = f."{id_col}"'
                        count_expr = "COUNT(d.rowid)"
                        if time_col:
                            start_expr = f'COALESCE(MIN(d."{time_col}"), \'\')'
                            end_expr = f'COALESCE(MAX(d."{time_col}"), \'\')'
                    rows = connection.execute(
                        f'SELECT f."{id_col}" AS file_id, {select_name} AS name, {start_expr} AS start_time, '
                        f'{end_expr} AS end_time, {count_expr} AS row_count FROM "{files_table}" f{joins} '
                        f'GROUP BY f."{id_col}", {select_name} ORDER BY f."{id_col}" DESC'
                    ).fetchall()
                    return [BlackBoxFileInfo(int(row["file_id"]), str(row["name"] or row["file_id"]),
                                             str(row["start_time"] or ""), str(row["end_time"] or ""), int(row["row_count"] or 0))
                            for row in rows]
            if fk:
                name_expr = f'CAST("{fk}" AS TEXT)'
                start_expr = f'COALESCE(MIN("{time_col}"), \'\')' if time_col else "''"
                end_expr = f'COALESCE(MAX("{time_col}"), \'\')' if time_col else "''"
                rows = connection.execute(
                    f'SELECT "{fk}" AS file_id, {name_expr} AS name, {start_expr} AS start_time, {end_expr} AS end_time, '
                    f'COUNT(*) AS row_count FROM "{data_table}" GROUP BY "{fk}" ORDER BY "{fk}" DESC'
                ).fetchall()
                return [BlackBoxFileInfo(int(row["file_id"]), str(row["name"]), str(row["start_time"]),
                                         str(row["end_time"]), int(row["row_count"])) for row in rows]
            count = connection.execute(f'SELECT COUNT(*) FROM "{data_table}"').fetchone()[0]
            return [BlackBoxFileInfo(0, data_table, row_count=int(count or 0))]

    @staticmethod
    def _numeric_array(rows: list[sqlite3.Row], column: str) -> np.ndarray:
        values = np.empty(len(rows), dtype=np.float64)
        values.fill(np.nan)
        for index, row in enumerate(rows):
            try:
                value = row[column]
                if value is not None and value != "":
                    values[index] = float(value)
            except (TypeError, ValueError, KeyError, IndexError):
                pass
        return values

    def load_blackbox_file(self, file_id: int) -> BlackBoxData:
        with self._connect() as connection:
            files_table, data_table = self._find_blackbox_tables(connection)
            if data_table is None:
                raise ProjectRepositoryError("BlackBox data table was not found.")
            columns = self._table_columns(connection, data_table)
            fk = self._first(columns, ("File_FK", "FileID", "FileId", "file_id", "BlackBoxFile_FK"))
            time_col = self._first(columns, ("TimeStamp", "Timestamp", "DateTime", "datetime", "Time", "time"))
            sql = f'SELECT rowid AS _source_index, * FROM "{data_table}"'
            params: list[object] = []
            if fk and file_id:
                sql += f' WHERE "{fk}" = ?'
                params.append(file_id)
            if time_col:
                sql += f' ORDER BY "{time_col}"'
            rows = connection.execute(sql, params).fetchall()

        info = next((item for item in self.list_blackbox_files() if item.file_id == file_id), None)
        if info is None:
            info = BlackBoxFileInfo(file_id, f"BlackBox {file_id}", row_count=len(rows))
        count = len(rows)
        if not rows:
            return BlackBoxData(info, np.array([], float), np.array([], object))

        labels = np.asarray([str(row[time_col]) if time_col and row[time_col] is not None else str(i) for i, row in enumerate(rows)], dtype=object)
        seconds = np.arange(count, dtype=np.float64)
        if time_col:
            import datetime as _dt
            parsed: list[float] = []
            first = None
            for label in labels:
                value = None
                text = str(label).strip().replace("Z", "+00:00")
                for parser in (_dt.datetime.fromisoformat,):
                    try:
                        value = parser(text).timestamp(); break
                    except Exception:
                        pass
                if value is None:
                    try:
                        parts = text.split(":")
                        value = float(parts[-1]) + 60 * float(parts[-2]) + (3600 * float(parts[-3]) if len(parts) >= 3 else 0)
                    except Exception:
                        value = float(len(parsed))
                if first is None:
                    first = value
                parsed.append(value - first)
            seconds = np.asarray(parsed, dtype=np.float64)

        by_lower = {name.lower(): name for name in columns}
        tracks: dict[str, tuple[np.ndarray, np.ndarray]] = {}
        used: set[str] = set()

        # Discover every coordinate pair from the real BlackBox schema.
        # Supported examples: GNSS1Easting/GNSS1Northing, GNSS1_X/GNSS1_Y,
        # ROV1X/ROV1Y, Vessel_E/Vessel_N, USBL Easting/USBL Northing.
        import re

        def normalized_prefix(column: str, suffix_pattern: str) -> str | None:
            match = re.match(rf"^(.*?)(?:[ _-]*)({suffix_pattern})$", column, flags=re.IGNORECASE)
            if not match:
                return None
            prefix = re.sub(r"[ _-]+$", "", match.group(1)).strip()
            return prefix or None

        x_suffixes = r"easting|east|x|e"
        y_suffixes = r"northing|north|y|n"
        x_by_prefix: dict[str, str] = {}
        y_by_prefix: dict[str, str] = {}

        ignored_prefixes = {"", "preplot", "primary", "secondary"}
        for actual in columns:
            prefix = normalized_prefix(actual, x_suffixes)
            if prefix and prefix.lower() not in ignored_prefixes:
                x_by_prefix.setdefault(prefix.lower(), actual)
            prefix = normalized_prefix(actual, y_suffixes)
            if prefix and prefix.lower() not in ignored_prefixes:
                y_by_prefix.setdefault(prefix.lower(), actual)

        # Prefer established names/order, then append all other discovered pairs.
        preferred = ["gnss1", "gnss2", "vessel", "ins", "usbl", "rov1", "rov2"]
        prefixes = [p for p in preferred if p in x_by_prefix and p in y_by_prefix]
        prefixes.extend(sorted(p for p in (x_by_prefix.keys() & y_by_prefix.keys()) if p not in prefixes))

        for prefix in prefixes:
            xcol = x_by_prefix[prefix]
            ycol = y_by_prefix[prefix]
            x = self._numeric_array(rows, xcol)
            y = self._numeric_array(rows, ycol)
            finite = np.isfinite(x) & np.isfinite(y)
            if not finite.any():
                continue
            label = re.sub(r"[_-]+", " ", re.sub(r"(?<=[a-z])(?=[A-Z])", " ", xcol))
            label = re.sub(r"(?i)[ _-]*(easting|east|x|e)$", "", label).strip()
            label = label or prefix.upper()
            # Keep familiar labels compact.
            canonical = {"gnss1": "GNSS1", "gnss2": "GNSS2", "vessel": "Vessel",
                         "ins": "INS", "usbl": "USBL", "rov1": "ROV1", "rov2": "ROV2"}
            label = canonical.get(prefix, label)
            unique_label = label
            suffix = 2
            while unique_label in tracks:
                unique_label = f"{label} {suffix}"
                suffix += 1
            tracks[unique_label] = (x[finite], y[finite])
            used.update((xcol, ycol))

        qc_names = [
            "HDOP", "PDOP", "VDOP", "NOS", "DiffAge", "FixQuality", "SOG", "Speed", "HDG", "Heading", "COG",
            "Pitch", "Roll", "Heave", "Depth", "Depth1", "Depth2", "Altitude", "WaterDepth"
        ]
        qc_columns: dict[str, np.ndarray] = {}
        for candidate in qc_names:
            actual = by_lower.get(candidate.lower())
            if actual and actual not in used:
                values = self._numeric_array(rows, actual)
                if np.isfinite(values).any():
                    qc_columns[actual] = values
        # Include additional numeric QC columns without loading coordinates twice.
        for actual in sorted(columns):
            if actual in used or actual in qc_columns or actual in {fk, time_col, "ID", "id"}:
                continue
            values = self._numeric_array(rows, actual)
            finite_count = int(np.isfinite(values).sum())
            if finite_count >= max(3, count // 20):
                qc_columns[actual] = values

        return BlackBoxData(info, seconds, labels, qc_columns, tracks)

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
