from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

import numpy as np

from .models import PointLayerData, ProjectShapeDefinition, BlackBoxData, BlackBoxFileInfo, DsrQcData


class ProjectRepositoryError(RuntimeError):
    pass


class ProjectRepository:
    """Read only the columns required by the map and return NumPy arrays."""

    def __init__(self, project_path: str | Path) -> None:
        project_path = Path(project_path).expanduser().resolve()
        self.db_path = project_path if project_path.suffix.lower() in {".sqlite", ".sqlite3", ".db"} else project_path / "data" / "project.sqlite3"
        if not self.db_path.exists():
            raise ProjectRepositoryError(f"Project database not found: {self.db_path}")

    def _write_connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path), timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def ensure_geopackage_tables(self) -> None:
        with self._write_connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS project_geopackages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    full_name TEXT NOT NULL UNIQUE,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS project_geopackage_layers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    geopackage_id INTEGER NOT NULL,
                    layer_name TEXT NOT NULL,
                    display_name TEXT,
                    geometry_type TEXT,
                    source_crs TEXT,
                    is_visible INTEGER NOT NULL DEFAULT 1,
                    line_color TEXT DEFAULT '#000000',
                    line_width REAL DEFAULT 1.0,
                    line_style TEXT DEFAULT 'solid',
                    is_filled INTEGER NOT NULL DEFAULT 0,
                    fill_color TEXT DEFAULT '#808080',
                    fill_opacity INTEGER DEFAULT 100,
                    hatch_pattern TEXT DEFAULT '',
                    point_size REAL DEFAULT 6.0,
                    layer_order INTEGER DEFAULT 0,
                    FOREIGN KEY (geopackage_id) REFERENCES project_geopackages(id) ON DELETE CASCADE,
                    UNIQUE (geopackage_id, layer_name)
                );
            """)

    def attach_geopackage(self, path: str | Path, layers: list[dict]) -> None:
        self.ensure_geopackage_tables()
        source = Path(path).expanduser().resolve()
        with self._write_connect() as connection:
            connection.execute(
                "INSERT INTO project_geopackages(name, full_name, is_active) VALUES(?,?,1) "
                "ON CONFLICT(full_name) DO UPDATE SET name=excluded.name, is_active=1",
                (source.stem, str(source)),
            )
            gpkg_id = int(connection.execute("SELECT id FROM project_geopackages WHERE full_name=?", (str(source),)).fetchone()[0])
            for order, layer in enumerate(layers):
                geom = str(layer.get("geometry_type") or "")
                filled = 1 if "polygon" in geom.lower() else 0
                connection.execute("""
                    INSERT INTO project_geopackage_layers(
                        geopackage_id, layer_name, display_name, geometry_type, source_crs,
                        is_visible, is_filled, layer_order
                    ) VALUES(?,?,?,?,?,1,?,?)
                    ON CONFLICT(geopackage_id, layer_name) DO UPDATE SET
                        display_name=excluded.display_name, geometry_type=excluded.geometry_type,
                        source_crs=excluded.source_crs, is_visible=1, layer_order=excluded.layer_order
                """, (gpkg_id, layer["name"], layer.get("display_name") or layer["name"], geom,
                      str(layer.get("crs") or ""), filled, order))

    def load_geopackage_definitions(self) -> list[ProjectShapeDefinition]:
        # Startup must remain read-only. The tables are created only when the
        # user explicitly attaches a GeoPackage. This is important for projects
        # stored on network drives or opened with restricted permissions.
        with self._connect() as connection:
            existing = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name IN ('project_geopackages','project_geopackage_layers')"
                ).fetchall()
            }
            if existing != {'project_geopackages', 'project_geopackage_layers'}:
                return []
            rows = connection.execute("""
                SELECT l.*, g.name AS container_name, g.full_name
                FROM project_geopackage_layers l
                JOIN project_geopackages g ON g.id=l.geopackage_id
                WHERE g.is_active=1
                ORDER BY g.name, l.layer_order, l.id
            """).fetchall()
        result = []
        for row in rows:
            result.append(ProjectShapeDefinition(
                name=str(row["display_name"] or row["layer_name"]), full_name=Path(str(row["full_name"])),
                source_type="gpkg", source_layer=str(row["layer_name"]), database_id=int(row["id"]),
                container_name=str(row["container_name"]), is_visible=self._database_bool(row["is_visible"]),
                is_filled=self._database_bool(row["is_filled"]), fill_color=str(row["fill_color"] or "#808080"),
                line_color=str(row["line_color"] or "#000000"), line_width=float(row["line_width"] or 1.0),
                line_style=str(row["line_style"] or "solid"), hatch_pattern=str(row["hatch_pattern"] or ""),
                source_epsg=str(row["source_crs"] or ""),
            ))
        return result

    def update_geopackage_layer_style(self, definition: ProjectShapeDefinition, style: dict) -> None:
        if definition.database_id is None: return
        with self._write_connect() as connection:
            connection.execute("""UPDATE project_geopackage_layers SET
                line_color=?, line_width=?, line_style=?, is_filled=?, fill_color=?,
                fill_opacity=?, point_size=?, is_visible=? WHERE id=?""",
                (style.get("outline_color", "#000000"), float(style.get("outline_width", 1.0)),
                 style.get("outline_style", "solid"), int(bool(style.get("fill_enabled", False))),
                 style.get("fill_color", "#808080"), int(style.get("fill_opacity", 100)),
                 float(style.get("point_size", 6.0)), int(definition.is_visible), definition.database_id))

    def set_geopackage_layer_visible(self, definition: ProjectShapeDefinition, visible: bool) -> None:
        if definition.database_id is None: return
        with self._write_connect() as connection:
            connection.execute("UPDATE project_geopackage_layers SET is_visible=? WHERE id=?", (int(visible), definition.database_id))

    def detach_geopackage_layer(self, definition: ProjectShapeDefinition) -> None:
        if definition.database_id is None: return
        with self._write_connect() as connection:
            connection.execute("DELETE FROM project_geopackage_layers WHERE id=?", (definition.database_id,))

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

    def max_radial_offset(self, default: float = 5.0) -> float:
        """Return project_node_qc.max_radial_offset with a safe fallback."""
        with self._connect() as connection:
            try:
                columns = self._table_columns(connection, "project_node_qc")
                column = self._first(columns, ("max_radial_offset", "MaxRadialOffset", "max_radial"))
                if not column:
                    return float(default)
                row = connection.execute(
                    f'SELECT "{column}" FROM project_node_qc '
                    f'WHERE "{column}" IS NOT NULL LIMIT 1'
                ).fetchone()
            except sqlite3.Error:
                return float(default)
        if not row or row[0] is None:
            return float(default)
        try:
            value = float(row[0])
        except (TypeError, ValueError):
            return float(default)
        return value if np.isfinite(value) and value > 0 else float(default)

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

    def load_ocr_image_counts(self) -> PointLayerData:
        """Return one label anchor per RPPreplot position having OCR images."""
        preplot = self.load_rp_preplot()
        image_counts: dict[tuple[int, int], int] = {}
        with self._connect() as connection:
            ocr_columns = self._table_columns(connection, "ocr_results")
            if "line" in ocr_columns and "station" in ocr_columns:
                count_rows = connection.execute(
                    'SELECT CAST("line" AS INTEGER) AS line_value, '
                    'CAST("station" AS INTEGER) AS point_value, COUNT(*) AS image_count '
                    'FROM "ocr_results" '
                    'WHERE "line" IS NOT NULL AND "station" IS NOT NULL '
                    'GROUP BY CAST("line" AS INTEGER), CAST("station" AS INTEGER)'
                ).fetchall()
                image_counts = {
                    (int(row["line_value"]), int(row["point_value"])): int(row["image_count"])
                    for row in count_rows
                }

        line_values = preplot.metadata.get("line")
        point_values = preplot.metadata.get("point")
        counts = np.zeros(preplot.count, dtype=np.int64)
        if line_values is not None and point_values is not None:
            for index, (line, point) in enumerate(zip(line_values, point_values)):
                try:
                    key = (int(float(str(line).strip())), int(float(str(point).strip())))
                except (TypeError, ValueError):
                    continue
                counts[index] = image_counts.get(key, 0)
        indices = np.flatnonzero(counts > 0)
        metadata: dict[str, np.ndarray] = {"Images": counts[indices]}
        if line_values is not None:
            metadata["line"] = line_values[indices]
        if point_values is not None:
            metadata["point"] = point_values[indices]
        return PointLayerData(
            "OCR Image Counts",
            preplot.x[indices],
            preplot.y[indices],
            preplot.source_index[indices],
            metadata,
        )

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
            requested = (
                "ID", "Line", "Station", "LinePoint", "Node",
                "PrimaryEasting", "PrimaryNorthing", "PrimaryElevation",
                "SecondaryEasting", "SecondaryNorthing", "SecondaryElevation",
                "PrimaryEasting1", "PrimaryNorthing1", "PrimaryElevation1",
                "SecondaryEasting1", "SecondaryNorthing1", "SecondaryElevation1",
                "ROV", "ROV1", "TimeStamp", "TimeStamp1", "Comments"
            )
            optional = [name for name in requested if name in columns]
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
        """Load first-break positions with Line and Point metadata when available."""
        with self._connect() as connection:
            columns = self._table_columns(connection, "REC_DB")
            x_col = self._first(columns, ("REC_X", "X", "Easting"))
            y_col = self._first(columns, ("REC_Y", "Y", "Northing"))
            if not x_col or not y_col:
                return PointLayerData("REC_DB", np.array([], float), np.array([], float), np.array([], np.int64))

            direct_line = self._first(columns, ("Line", "RLine", "ReceiverLine"))
            direct_point = self._first(columns, ("Point", "Station", "LinePoint"))
            preplot_fk = self._first(columns, ("Preplot_FK", "RLPreplot_FK", "RPPreplot_FK"))

            join_sql = ""
            joined_line = None
            joined_point = None
            if (not direct_line or not direct_point) and preplot_fk:
                tables = {str(row[0]) for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )}
                for table in ("RPPreplot", "RLPreplot"):
                    if table not in tables:
                        continue
                    pcols = self._table_columns(connection, table)
                    id_col = self._first(pcols, ("ID", "Id", "id"))
                    line_col = self._first(pcols, ("Line", "RLine", "ReceiverLine"))
                    point_col = self._first(pcols, ("Point", "Station", "LinePoint"))
                    if id_col and (line_col or point_col):
                        join_sql = f' LEFT JOIN "{table}" p ON r."{preplot_fk}" = p."{id_col}"'
                        joined_line = line_col
                        joined_point = point_col
                        break

            select = [
                "r.rowid AS source_index",
                f'CAST(r."{x_col}" AS REAL) AS x',
                f'CAST(r."{y_col}" AS REAL) AS y',
            ]
            line_expr = f'r."{direct_line}"' if direct_line else (f'p."{joined_line}"' if joined_line else None)
            point_expr = f'r."{direct_point}"' if direct_point else (f'p."{joined_point}"' if joined_point else None)
            if line_expr:
                select.append(f'{line_expr} AS line')
            if point_expr:
                select.append(f'{point_expr} AS point')

            for name in ("ID", "REC_ID", "DEPLOY", "RPI", "REC_Z"):
                if name in columns:
                    select.append(f'r."{name}" AS "{name.lower()}"')

            where = [f'r."{x_col}" IS NOT NULL', f'r."{y_col}" IS NOT NULL']
            params: list[object] = []
            if line is not None and line_expr:
                where.append(f'{line_expr} = ?')
                params.append(int(line))
            order = []
            if line_expr:
                order.append(line_expr)
            if point_expr:
                order.append(point_expr)
            sql = f'SELECT {", ".join(select)} FROM REC_DB r{join_sql} WHERE {" AND ".join(where)}'
            if order:
                sql += " ORDER BY " + ", ".join(order)
            rows = connection.execute(sql, params).fetchall()
        return self._rows_to_layer("REC_DB", rows)


    def list_dsr_lines(self) -> list[int]:
        """Return receiver lines available in DSR, sorted numerically."""
        with self._connect() as connection:
            columns = self._table_columns(connection, "DSR")
            line_col = self._first(columns, ("Line", "RLine", "ReceiverLine"))
            if not line_col:
                return []
            rows = connection.execute(
                f'SELECT DISTINCT "{line_col}" FROM DSR '
                f'WHERE "{line_col}" IS NOT NULL ORDER BY CAST("{line_col}" AS INTEGER)'
            ).fetchall()
        result: list[int] = []
        for row in rows:
            try:
                result.append(int(row[0]))
            except (TypeError, ValueError):
                continue
        return result

    def load_dsr_qc(self, line: int) -> DsrQcData:
        """Load all usable numeric DSR parameters for one receiver line.

        Column discovery is dynamic because DSR QC field names differ between
        projects. Station is used as the common x-axis. Coordinate, offset,
        sigma, depth and other numeric fields are returned and grouped by the
        QC window.
        """
        with self._connect() as connection:
            columns = self._table_columns(connection, "DSR")
            line_col = self._first(columns, ("Line", "RLine", "ReceiverLine"))
            station_col = self._first(columns, ("Station", "LinePoint", "Point"))
            if not line_col or not station_col:
                raise ProjectRepositoryError("DSR Line/Station columns were not found.")

            # Keep all DSR columns so the QC viewer can expose project-specific
            # metrics. Conversion below discards text/categorical fields.
            ordered_columns = sorted(columns)
            select = ", ".join(f'"{name}"' for name in ordered_columns)
            rows = connection.execute(
                f'SELECT {select} FROM DSR WHERE "{line_col}" = ? '
                f'ORDER BY CAST("{station_col}" AS REAL)',
                (int(line),),
            ).fetchall()

        count = len(rows)
        if count == 0:
            return DsrQcData(int(line), np.array([], dtype=np.float64), {})

        station = self._numeric_array(rows, station_col)
        exclude = {
            line_col.lower(), station_col.lower(), "id", "file_fk", "solution_fk",
            "rlpreplot_fk", "preplot_fk", "node", "node_hex_id", "auqrcode",
            "remoteunit", "rov", "rov1", "status", "comments",
        }
        numeric: dict[str, np.ndarray] = {}
        minimum_finite = max(3, count // 20)
        for name in ordered_columns:
            if name.lower() in exclude:
                continue
            values = self._numeric_array(rows, name)
            if int(np.isfinite(values).sum()) < minimum_finite:
                continue
            # Do not add a duplicate of the station axis.
            if name == station_col:
                continue
            numeric[name] = values

        return DsrQcData(int(line), station, numeric)


    @staticmethod
    def _database_bool(value) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}

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
                is_filled=self._database_bool(values.get("is_filled")),
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

    @staticmethod
    def _table_names(connection: sqlite3.Connection) -> list[str]:
        return [str(row[0]) for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )]

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
                    # Resolve operational names from BlackBox_Files.Config_FK -> BBox_Configs_List.ID.
                    config_table = next((name for name in self._table_names(connection)
                                         if name.lower() == "bbox_configs_list"), None)
                    config_fk = self._first(file_columns, ("Config_FK", "ConfigFK", "Config_ID", "ConfigID"))
                    config_join = ""
                    config_select = ", NULL AS config_id, 'Vessel' AS vessel_name, 'ROV 1' AS rov1_name, 'ROV 2' AS rov2_name, 'GNSS1' AS gnss1_name, 'GNSS2' AS gnss2_name, 'MRU 1' AS mru1_name, 'MRU 2' AS mru2_name, 'MRU 3' AS mru3_name"
                    group_extra = ""
                    if config_table and config_fk:
                        config_columns = self._table_columns(connection, config_table)
                        config_id_col = self._first(config_columns, ("ID", "Id", "id"))
                        vessel_col = self._first(config_columns, ("Vessel_name", "VesselName", "vessel_name"))
                        rov1_col = self._first(config_columns, ("rov1_name", "ROV1_name", "Rov1Name"))
                        rov2_col = self._first(config_columns, ("rov2_name", "ROV2_name", "Rov2Name"))
                        gnss1_col = self._first(config_columns, ("gnss1_name", "GNSS1_name", "Gnss1Name"))
                        gnss2_col = self._first(config_columns, ("gnss2_name", "GNSS2_name", "Gnss2Name"))
                        mru1_col = self._first(config_columns, ("mru1_name", "MRU1_name", "Mru1Name"))
                        mru2_col = self._first(config_columns, ("mru2_name", "MRU2_name", "Mru2Name"))
                        mru3_col = self._first(config_columns, ("mru3_name", "MRU3_name", "Mru3Name"))
                        if config_id_col:
                            config_join = f' LEFT JOIN "{config_table}" c ON c."{config_id_col}" = f."{config_fk}"'
                            config_select = f', f."{config_fk}" AS config_id'
                            if vessel_col:
                                config_select += f", COALESCE(c.\"{vessel_col}\", 'Vessel') AS vessel_name"
                            else:
                                config_select += ", 'Vessel' AS vessel_name"
                            if rov1_col:
                                config_select += f", COALESCE(c.\"{rov1_col}\", 'ROV 1') AS rov1_name"
                            else:
                                config_select += ", 'ROV 1' AS rov1_name"
                            if rov2_col:
                                config_select += f", COALESCE(c.\"{rov2_col}\", 'ROV 2') AS rov2_name"
                            else:
                                config_select += ", 'ROV 2' AS rov2_name"
                            for alias, column, fallback in (("gnss1_name", gnss1_col, "GNSS1"), ("gnss2_name", gnss2_col, "GNSS2"), ("mru1_name", mru1_col, "MRU 1"), ("mru2_name", mru2_col, "MRU 2"), ("mru3_name", mru3_col, "MRU 3")):
                                if column:
                                    config_select += f", COALESCE(c.\"{column}\", '{fallback}') AS {alias}"
                                else:
                                    config_select += f", '{fallback}' AS {alias}"
                            group_extra = f', f."{config_fk}"'
                            for column in (vessel_col, rov1_col, rov2_col, gnss1_col, gnss2_col, mru1_col, mru2_col, mru3_col):
                                if column:
                                    group_extra += f', c."{column}"'
                    rows = connection.execute(
                        f'SELECT f."{id_col}" AS file_id, {select_name} AS name, {start_expr} AS start_time, '
                        f'{end_expr} AS end_time, {count_expr} AS row_count{config_select} '
                        f'FROM "{files_table}" f{joins}{config_join} '
                        f'GROUP BY f."{id_col}", {select_name}{group_extra} ORDER BY f."{id_col}" DESC'
                    ).fetchall()
                    return [BlackBoxFileInfo(
                                int(row["file_id"]), str(row["name"] or row["file_id"]),
                                str(row["start_time"] or ""), str(row["end_time"] or ""), int(row["row_count"] or 0),
                                int(row["config_id"]) if row["config_id"] is not None else None,
                                str(row["vessel_name"] or "Vessel"),
                                str(row["rov1_name"] or "ROV 1"),
                                str(row["rov2_name"] or "ROV 2"),
                                str(row["gnss1_name"] or "GNSS1"),
                                str(row["gnss2_name"] or "GNSS2"),
                                str(row["mru1_name"] or "MRU 1"),
                                str(row["mru2_name"] or "MRU 2"),
                                str(row["mru3_name"] or "MRU 3"),
                            ) for row in rows]
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
        track_indices: dict[str, np.ndarray] = {}
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
            track_indices[unique_label] = np.flatnonzero(finite).astype(np.int64, copy=False)
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
        # Keep GNSS coordinates available for the dedicated GNSS comparison dashboard.
        for candidate in ("GNSS1_Easting", "GNSS1_Northing", "GNSS2_Easting", "GNSS2_Northing"):
            actual = by_lower.get(candidate.lower())
            if actual:
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

        return BlackBoxData(info, seconds, labels, qc_columns, tracks, track_indices)

    def load_survey_manager_layer(self, mode: str) -> PointLayerData:
        """Load Survey Manager deployment or recovery positions from DSR.

        Horizontal map coordinates are always read from ActualX and ActualY.
        ActualZ is retained as water-depth metadata and is never used as the
        map Y coordinate. Deployment records require Deployed = Yes and
        recovery records require PickedUp = Yes.
        """
        mode = str(mode).lower().strip()
        if mode not in {"deployment", "recovery"}:
            raise ValueError(f"Unsupported Survey Manager mode: {mode}")

        with self._connect() as connection:
            columns = self._table_columns(connection, "DSR")
            by_lower = {name.lower(): name for name in columns}

            def col(*names: str) -> str | None:
                return next(
                    (by_lower[name.lower()] for name in names if name.lower() in by_lower),
                    None,
                )

            x_col = col("ActualX", "SMX", "ActualEasting")
            y_col = col("ActualY", "SMY", "ActualNorthing")
            z_col = col("ActualZ", "SMZ", "WaterDepth")

            if not x_col or not y_col:
                return PointLayerData(
                    f"SM {mode.title()}",
                    np.array([], dtype=np.float64),
                    np.array([], dtype=np.float64),
                    np.array([], dtype=np.int64),
                )

            line_col = col("Line", "RLine", "ReceiverLine")
            station_col = col("Station", "LinePoint", "Point")
            node_col = col("Node", "DSRNode", "SMNode", "RemoteUnit", "AUQRCode")
            deployed_col = col("Deployed")
            picked_up_col = col("PickedUp", "Picked_Up", "Picked Up")
            timestamp_col = col("TimeStamp", "Timestamp", "DateTime")
            timestamp1_col = col("TimeStamp1", "Timestamp1", "RecoveryTimeStamp")

            selected = [
                "rowid AS source_index",
                f'CAST("{x_col}" AS REAL) AS x',
                f'CAST("{y_col}" AS REAL) AS y',
            ]
            aliases = {
                "line": line_col,
                "station": station_col,
                "node": node_col,
                "water_depth": z_col,
                "deployed": deployed_col,
                "picked_up": picked_up_col,
                "timestamp": timestamp_col,
                "timestamp1": timestamp1_col,
            }
            for alias, actual in aliases.items():
                if actual:
                    selected.append(f'"{actual}" AS "{alias}"')

            where = [f'"{x_col}" IS NOT NULL', f'"{y_col}" IS NOT NULL']

            if mode == "deployment":
                if not deployed_col:
                    return PointLayerData(
                        "SM Deployment",
                        np.array([], dtype=np.float64),
                        np.array([], dtype=np.float64),
                        np.array([], dtype=np.int64),
                    )
                where.append(
                    f"LOWER(TRIM(CAST(\"{deployed_col}\" AS TEXT))) = 'yes'"
                )
            else:
                if not picked_up_col:
                    return PointLayerData(
                        "SM Recovery",
                        np.array([], dtype=np.float64),
                        np.array([], dtype=np.float64),
                        np.array([], dtype=np.int64),
                    )
                where.append(
                    f"LOWER(TRIM(CAST(\"{picked_up_col}\" AS TEXT))) = 'yes'"
                )

            order_parts = [value for value in (line_col, station_col) if value]
            sql = f'SELECT {", ".join(selected)} FROM DSR WHERE {" AND ".join(where)}'
            if order_parts:
                sql += " ORDER BY " + ", ".join(f'"{name}"' for name in order_parts)
            rows = connection.execute(sql).fetchall()

        return self._rows_to_layer(f"SM {mode.title()}", rows)

    def dsr_line_time_range(
        self,
        line: int,
        phase: str = "deployment",
    ) -> tuple[str, str] | None:
        """Return the chronological DSR time window for one receiver line.

        ``deployment`` uses DSR.TimeStamp and ``recovery`` uses
        DSR.TimeStamp1.  The earliest and latest non-empty timestamps are
        used, which also works when recovery proceeds in the opposite station
        direction from deployment.
        """
        phase = str(phase or "deployment").strip().lower()
        if phase not in {"deployment", "recovery"}:
            raise ProjectRepositoryError(f"Unsupported BlackBox phase: {phase}")

        with self._connect() as connection:
            columns = self._table_columns(connection, "DSR")
            line_col = self._first(columns, ("Line", "RLine", "ReceiverLine"))
            if phase == "deployment":
                time_col = self._first(columns, ("TimeStamp", "Timestamp", "DateTime"))
            else:
                time_col = self._first(
                    columns,
                    ("TimeStamp1", "Timestamp1", "RecoveryTimeStamp", "RecoveryTimestamp"),
                )
            if not line_col or not time_col:
                return None

            where = (
                f'"{line_col}" = ? AND "{time_col}" IS NOT NULL '
                f'AND TRIM(CAST("{time_col}" AS TEXT)) <> ""'
            )
            first = connection.execute(
                f'SELECT "{time_col}" FROM DSR WHERE {where} '
                f'ORDER BY "{time_col}" ASC LIMIT 1',
                (int(line),),
            ).fetchone()
            last = connection.execute(
                f'SELECT "{time_col}" FROM DSR WHERE {where} '
                f'ORDER BY "{time_col}" DESC LIMIT 1',
                (int(line),),
            ).fetchone()

        if not first or not last:
            return None
        return str(first[0]), str(last[0])

    def load_blackbox_tracks_for_dsr_line(
        self,
        line: int,
        phase: str = "deployment",
    ) -> list[PointLayerData]:
        """Create BlackBox XY layers for a DSR deployment/recovery window."""
        phase = str(phase or "deployment").strip().lower()
        if phase not in {"deployment", "recovery"}:
            raise ProjectRepositoryError(f"Unsupported BlackBox phase: {phase}")

        time_range = self.dsr_line_time_range(int(line), phase=phase)
        if time_range is None:
            timestamp_name = "TimeStamp" if phase == "deployment" else "TimeStamp1"
            raise ProjectRepositoryError(
                f"No {phase} timestamps ({timestamp_name}) were found for DSR line {line}."
            )
        start_time, end_time = time_range

        with self._connect() as connection:
            files_table, data_table = self._find_blackbox_tables(connection)
            if data_table is None:
                return []
            columns = self._table_columns(connection, data_table)
            fk = self._first(columns, ("File_FK", "FileID", "FileId", "file_id", "BlackBoxFile_FK"))
            time_col = self._first(columns, ("TimeStamp", "Timestamp", "DateTime", "datetime", "Time", "time"))
            if not time_col:
                raise ProjectRepositoryError("BlackBox timestamp column was not found.")

            import re

            def prefix_for(column: str, suffix_pattern: str) -> str | None:
                match = re.match(rf"^(.*?)(?:[ _-]*)({suffix_pattern})$", column, flags=re.IGNORECASE)
                if not match:
                    return None
                prefix = re.sub(r"[ _-]+$", "", match.group(1)).strip()
                return prefix or None

            x_by_prefix: dict[str, str] = {}
            y_by_prefix: dict[str, str] = {}
            ignored = {"", "preplot", "primary", "secondary", "actual"}
            for actual in columns:
                prefix = prefix_for(actual, r"easting|east|x|e")
                if prefix and prefix.lower() not in ignored:
                    x_by_prefix.setdefault(prefix.lower(), actual)
                prefix = prefix_for(actual, r"northing|north|y|n")
                if prefix and prefix.lower() not in ignored:
                    y_by_prefix.setdefault(prefix.lower(), actual)
            preferred = ["gnss1", "gnss2", "vessel", "ins", "usbl", "rov1", "rov2"]
            prefixes = [value for value in preferred if value in x_by_prefix and value in y_by_prefix]
            prefixes.extend(sorted(value for value in (x_by_prefix.keys() & y_by_prefix.keys()) if value not in prefixes))
            if not prefixes:
                return []

            select_columns = ["rowid AS source_index", f'"{time_col}" AS timestamp']
            if fk:
                select_columns.append(f'"{fk}" AS file_id')
            for prefix in prefixes:
                select_columns.append(f'CAST("{x_by_prefix[prefix]}" AS REAL) AS "{prefix}_x"')
                select_columns.append(f'CAST("{y_by_prefix[prefix]}" AS REAL) AS "{prefix}_y"')
            rows = connection.execute(
                f'SELECT {", ".join(select_columns)} FROM "{data_table}" '
                f'WHERE "{time_col}" >= ? AND "{time_col}" <= ? ORDER BY "{time_col}"',
                (start_time, end_time),
            ).fetchall()

        if not rows:
            return []
        file_names = {item.file_id: item.name for item in self.list_blackbox_files()}
        canonical = {
            "gnss1": "GNSS1", "gnss2": "GNSS2", "vessel": "Vessel",
            "ins": "INS", "usbl": "USBL", "rov1": "ROV1", "rov2": "ROV2",
        }
        phase_title = "Deployment" if phase == "deployment" else "Recovery"
        results: list[PointLayerData] = []
        file_ids = sorted({int(row["file_id"] or 0) if fk else 0 for row in rows})
        for file_id in file_ids:
            file_rows = [row for row in rows if (int(row["file_id"] or 0) if fk else 0) == file_id]
            for prefix in prefixes:
                x = self._numeric_array(file_rows, f"{prefix}_x")
                y = self._numeric_array(file_rows, f"{prefix}_y")
                finite = np.isfinite(x) & np.isfinite(y)
                if not finite.any():
                    continue
                x = x[finite]
                y = y[finite]
                timestamps = np.asarray(
                    [str(file_rows[index]["timestamp"]) for index in np.flatnonzero(finite)],
                    dtype=object,
                )
                source = canonical.get(prefix, prefix.upper())
                filename = file_names.get(file_id, f"File {file_id}")
                metadata = {
                    "line": np.full(x.size, int(line), dtype=np.int64),
                    "phase": np.asarray([phase_title] * x.size, dtype=object),
                    "source": np.asarray([source] * x.size, dtype=object),
                    "file": np.asarray([filename] * x.size, dtype=object),
                    "file_id": np.full(x.size, file_id, dtype=np.int64),
                    "timestamp": timestamps,
                    "window_start": np.asarray([start_time] * x.size, dtype=object),
                    "window_end": np.asarray([end_time] * x.size, dtype=object),
                    "track_group": np.zeros(x.size, dtype=np.int8),
                }
                results.append(PointLayerData(
                    f"BBox {phase_title} Line {line} — {filename} — {source}",
                    x,
                    y,
                    np.arange(x.size, dtype=np.int64),
                    metadata,
                ))
        return results

    def dsr_columns(self) -> list[str]:
        with self._connect() as connection:
            return sorted(self._table_columns(connection, "DSR"), key=str.lower)

    def load_custom_dsr_layer(self, definition) -> PointLayerData:
        """Load a configured DSR point layer using validated column names."""
        with self._connect() as connection:
            columns = self._table_columns(connection, "DSR")
            required = {definition.x_field, definition.y_field}
            missing = required - columns
            if missing:
                raise ProjectRepositoryError(
                    "Custom DSR layer is missing column(s): " + ", ".join(sorted(missing))
                )
            optional = [name for name in ("ID", "Line", "Station", "LinePoint", "Node", "ROV", "ROV1", "Status", definition.category_field) if name and name in columns]
            # Preserve order while removing duplicates.
            optional = list(dict.fromkeys(optional))
            select = [
                "rowid AS source_index",
                f'CAST("{definition.x_field}" AS REAL) AS x',
                f'CAST("{definition.y_field}" AS REAL) AS y',
            ] + [f'"{name}" AS "{name.lower()}"' for name in optional]
            where = [f'"{definition.x_field}" IS NOT NULL', f'"{definition.y_field}" IS NOT NULL']
            params: list[object] = []
            field = str(definition.filter_field or "")
            operator = str(definition.filter_operator or "").upper()
            if field and field in columns and operator:
                if operator in {"IS NULL", "IS NOT NULL"}:
                    where.append(f'"{field}" {operator}')
                elif operator in {"=", "!=", ">", ">=", "<", "<="}:
                    where.append(f'"{field}" {operator} ?')
                    params.append(definition.filter_value)
            order_parts = [name for name in ("Line", "LinePoint", "Station") if name in columns]
            sql = f'SELECT {", ".join(select)} FROM DSR WHERE {" AND ".join(where)}'
            if order_parts:
                sql += " ORDER BY " + ", ".join(f'"{name}"' for name in order_parts)
            rows = connection.execute(sql, params).fetchall()
        return self._rows_to_layer(definition.name, rows)

    def load_ocr_results(self, line: object, station: object) -> list[dict[str, object]]:
        """Return OCR records matching ``ocr_results.line`` and ``ocr_results.station``.

        The DSR point supplies the selected Line and Station values, but the lookup
        intentionally ignores ``dsr_line``, ``dsr_station``, ``file_line`` and
        ``file_station``. Numeric comparison is used because SQLite projects may
        store Line and Station as INTEGER, REAL, or TEXT.
        """
        if line is None or station is None:
            return []
        try:
            line_value = int(float(str(line).strip()))
            station_value = int(float(str(station).strip()))
        except (TypeError, ValueError):
            return []

        requested = (
            "id", "image_path", "image_name", "resolution", "config_used",
            "file_role", "file_line", "file_station", "file_index", "rov",
            "dive", "date", "time", "line", "station", "east", "north",
            "dsr_line", "dsr_station", "dsr_x", "dsr_y", "dsr_timestamp",
            "dsr_timestamp1", "dsr_rov", "dsr_rov1", "delta_m",
            "ocr_vs_file", "file_vs_dsr", "status", "station_image_count",
            "expected_images", "station_status", "message", "checked",
            "processed_at",
        )
        with self._connect() as connection:
            columns = self._table_columns(connection, "ocr_results")
            if "line" not in columns or "station" not in columns:
                return []

            selected = [name for name in requested if name in columns]
            if not selected:
                return []

            order = [
                name for name in ("date", "time", "file_role", "file_index", "id")
                if name in columns
            ]
            sql = (
                "SELECT " + ", ".join(f'"{name}"' for name in selected)
                + ' FROM "ocr_results" '
                + 'WHERE CAST("line" AS INTEGER) = ? '
                + 'AND CAST("station" AS INTEGER) = ?'
            )
            if order:
                sql += " ORDER BY " + ", ".join(f'"{name}"' for name in order)

            rows = connection.execute(sql, (line_value, station_value)).fetchall()

        return [{key: row[key] for key in row.keys()} for row in rows]

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
