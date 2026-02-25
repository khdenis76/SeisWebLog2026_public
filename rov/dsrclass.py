import csv
import hashlib
import io
import re
import sqlite3
from pathlib import Path
import datetime
import datetime as _dt
from typing import Optional, Tuple, Any
import traceback

import pandas as pd
from django.utils.html import escape
from django.template.loader import render_to_string

from core.projectdb import ProjectDB
class ProjectDbError(Exception):
    pass

class DSRDB:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.pdb=ProjectDB(self.db_path)

    # --------------------------------------------------
    # Connection
    # --------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn
    @property
    def linescaler(self)->int:
        mask = getattr(self.pdb.get_geometry(), "rl_mask", "")
        return 10 ** (mask.count("L")) if "L" in mask else 0

    @property
    def pointscaler(self)->int:
        mask = getattr(self.pdb.get_geometry(), "rl_mask", "")
        return 10 ** (mask.count("P")) if "P" in mask else 0

    @property
    def linepointscaler(self)->int:
        result = self.linescaler*self.pointscaler
        return result if result>0 else 0
    def linepointidxscaler(self)->int:
        result = self.linescaler*self.pointscaler*10
        return result if result>0 else 0


    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------
    @staticmethod
    def _to_int(s: str) -> Optional[int]:
        s = (s or "").strip()
        return int(float(s)) if s else None

    @staticmethod
    def _to_float(s: str) -> Optional[float]:
        s = (s or "").strip()
        return float(s) if s else None

    @staticmethod
    def _to_text(s: str) -> Optional[str]:
        s = (s or "").strip()
        return s if s else None

    @staticmethod
    def _to_node(s: str) -> str:
        s = (s or "").strip()
        return s if s else "NA"

    @staticmethod
    def _parse_ts(ts: str) -> Optional[datetime]:
        ts = (ts or "").strip()
        if not ts:
            return None
        try:
            return _dt.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                return _dt.datetime.fromisoformat(ts)
            except ValueError:
                return None

    @staticmethod
    def _dt_parts(dt: Optional[datetime]):
        if not dt:
            return (None, None, None, None, None)
        iso = dt.isocalendar()
        return (
            dt.year,
            dt.month,
            int(iso.week),
            dt.strftime("%a"),
            dt.timetuple().tm_yday,
        )
    @staticmethod
    def _node_to_int_12(s: str) -> int:
        """
        Stable deterministic NodeId from Node string.
        12 digits (0..999,999,999,999).
        """
        if s is None:
            return None
        ss = str(s).strip()
        if ss == "":
            return None
        return int(hashlib.md5(ss.encode("utf-8")).hexdigest(), 16) % (10 ** 12)

    @staticmethod
    def _dt_parts(dt):
        """
        Returns: (Year, Month, WeekOfYear, Day, JulianDay)
        Week is ISO week number (1..53).
        JDay is day-of-year (1..366).
        """
        if not dt:
            return None, None, None, None, 0

        iso = dt.isocalendar()  # (iso_year, iso_week, iso_weekday)
        year = dt.year
        month = dt.month
        week = int(iso.week)
        day = dt.strftime("%Y-%m-%d")  # keep TEXT like your schema (Day is TEXT)
        jday = int(dt.timetuple().tm_yday)
        return year, month, week, day, jday

    # --------------------------------------------------
    # Solution FK
    # --------------------------------------------------
    def get_or_create_solution(self, name: str) -> int:
        name = (name or "Normal").strip() or "Normal"
        with self._connect() as conn:
            row = conn.execute(
                "SELECT ID FROM DSRSolution WHERE NAME=?",
                (name,),
            ).fetchone()
            if row:
                return int(row["ID"])
            cur = conn.execute(
                "INSERT INTO DSRSolution(NAME) VALUES (?)",
                (name,),
            )
            return int(cur.lastrowid)

    # --------------------------------------------------
    # Main loader
    # --------------------------------------------------
    def upsert_ip_stream(
            self,
            file_obj,
            rec_idx: int = 1,
            tier: int = 1,
            chunk_size: int = 5000,
    ):
        file_cols = [
            "Line", "Station", "Node",
            "PreplotEasting", "PreplotNorthing",
            "ROV", "TimeStamp",
            "PrimaryEasting", "Sigma",
            "PrimaryNorthing", "Sigma1",
            "SecondaryEasting", "Sigma2",
            "SecondaryNorthing", "Sigma3",
            "DeltaEprimarytosecondary",
            "DeltaNprimarytosecondary",
            "Rangeprimarytosecondary",
            "RangetoPrePlot", "BrgtoPrePlot",
            "PrimaryElevation", "Sigma4",
            "SecondaryElevation", "Sigma5",
            "Quality",
            "ROV1", "TimeStamp1",
            "PrimaryEasting1", "Sigma6",
            "PrimaryNorthing1", "Sigma7",
            "SecondaryEasting1", "Sigma8",
            "SecondaryNorthing1", "Sigma9",
            "DeltaEprimarytosecondary1",
            "DeltaNprimarytosecondary1",
            "Rangeprimarytosecondary1",
            "RangetoPrePlot1", "BrgtoPrePlot1",
            "PrimaryElevation1", "Sigma10",
            "SecondaryElevation1", "Sigma11",
            "Quality1",
            "DeployedtoRetrievedEasting",
            "DeployedtoRetrievedNorthing",
            "DeployedtoRecoveredElevation",
            "DeployedtoRetrievedRange",
            "DeployedtoRetrievedBrg",
            "Comments",
        ]

        insert_cols = [
            "Solution_FK",
            "RLPreplot_FK",
            "LinePointIdx",
            "LinePoint",
            "RecIdx",
            "TIER",
            "NODE_HEX_ID",
            "Year", "Month", "Week", "Day", "JDay",
            "Year1", "Month1", "Week1", "Day1", "JDay1",
            *file_cols,
        ]

        placeholders = ",".join("?" * len(insert_cols))
        update_cols = [c for c in insert_cols if c not in ("Line", "Station", "NODE_HEX_ID")]
        update_sql = ", ".join(f'"{c}"=excluded."{c}"' for c in update_cols)

        sql_upsert = f"""
        INSERT INTO DSR ({",".join(insert_cols)})
        VALUES ({placeholders})
        ON CONFLICT(Line,Station,NODE_HEX_ID) DO UPDATE SET
        {update_sql};
        """

        processed = upserted = skipped = 0
        batch = []
        scaler = int(getattr(self, "pointscaler", 0) or 0)

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")

            sql_rl_fk = """
                SELECT ID
                FROM RLPreplot
                WHERE Line=? AND TIER=?
                LIMIT 1
            """

            text_stream = io.TextIOWrapper(file_obj, encoding="utf-8", errors="replace")
            reader = csv.reader(text_stream)
            next(reader, None)

            for row in reader:
                if not row:
                    continue

                line = self._to_int(row[0] if len(row) > 0 else "")
                station = self._to_int(row[1] if len(row) > 1 else "")
                if line is None or station is None:
                    skipped += 1
                    continue

                rov_raw = row[5] if len(row) > 5 else ""
                ts_raw = row[6] if len(row) > 6 else ""

                if not str(rov_raw).strip() or not str(ts_raw).strip():
                    skipped += 1
                    continue

                node = self._to_node(row[2] if len(row) > 2 else "")
                node_id = self._node_to_int_12(node)
                if not node_id:
                    skipped += 1
                    continue

                dt = self._parse_ts(ts_raw)
                dt1 = self._parse_ts(row[26] if len(row) > 26 else "")

                y, m, w, d, j = self._dt_parts(dt)
                y1, m1, w1, d1, j1 = self._dt_parts(dt1)

                lp = (line * scaler + station) if scaler > 0 else station
                lp_idx = (lp * 10 + rec_idx)

                rl_row = conn.execute(sql_rl_fk, (line, tier)).fetchone()
                rl_fk = int(rl_row[0]) if rl_row else None

                values = {
                    "Solution_FK": 1,
                    "RLPreplot_FK": rl_fk,
                    "LinePointIdx": lp_idx,
                    "LinePoint": lp,
                    "RecIdx": rec_idx,
                    "TIER": tier,
                    "NODE_HEX_ID": node_id,
                    "Year": y, "Month": m, "Week": w, "Day": d, "JDay": j,
                    "Year1": y1, "Month1": m1, "Week1": w1, "Day1": d1, "JDay1": j1,
                }

                for i, col in enumerate(file_cols):
                    raw = row[i] if i < len(row) else ""

                    if col == "Line":
                        values[col] = line
                    elif col == "Station":
                        values[col] = station
                    elif col == "Node":
                        values[col] = node
                    elif col == "ROV":
                        values[col] = self._to_text(rov_raw)
                    elif col == "TimeStamp":
                        values[col] = self._to_text(ts_raw)
                    elif col in {"Quality", "ROV1", "TimeStamp1", "Quality1", "Comments"}:
                        values[col] = self._to_text(raw)
                    else:
                        values[col] = self._to_float(raw)

                batch.append(tuple(values.get(c) for c in insert_cols))
                processed += 1

                if len(batch) >= chunk_size:
                    conn.executemany(sql_upsert, batch)
                    upserted += len(batch)
                    batch.clear()

            if batch:
                conn.executemany(sql_upsert, batch)
                upserted += len(batch)

            conn.commit()

        return processed, upserted, skipped

    def render_dsr_line_summary_body(self, request=None):
        """
        Returns rendered <tbody> HTML for DSR line summary table
        using dsr_line_body.html template.
        """

        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM V_DSR_LineSummary ORDER BY Line"
            ).fetchall()

        context = {
            "lines": rows
        }

        return render_to_string(
            "rov/partials/dsr_line_body.html",
            context=context,
            request=request,  # optional, but good for filters / csrf / i18n
        )
    def set_dsr_line_clicked(self,line):
        with self.pdb._connect() as conn:
            conn.execute(
                'UPDATE RLPreplot SET isLineClicked=1 WHERE Line=?',
                (line,)
            )
            conn.commit()

    def get_bbox_db_fieldnames(self):
        exclude = {"ID", "File_FK"}

        with self._connect() as conn:
            rows = conn.execute("PRAGMA table_info(BlackBox)").fetchall()
            cols = [row[1] for row in rows if row[1] not in exclude]

        return cols

    def get_config_selector_table(self):
        try:
            bb_field_list = self.get_bbox_db_fieldnames()

            if not bb_field_list:
                raise ValueError("No columns found in BlackBox table.")

            html = render_to_string(
                "rov/partials/bbox_config_selector.html",
                {"bb_field_list": bb_field_list},
            )
            return html

        except Exception as exc:
            # optional: log full traceback for debugging
            print("BBOX config selector error:")
            traceback.print_exc()

            # safe HTML message for UI
            error_msg = escape(str(exc))

            return f"""
            <div class="alert alert-danger m-2">
                <h6 class="mb-1">Configuration error</h6>
                <div>
                    Unable to build BBOX configuration selector.
                </div>
                <div class="mt-2 small text-muted">
                    {error_msg}
                </div>
            </div>
            """

    def save_bbox_config(
            self,
            *,
            name: str = 'NA',
            vessel_name:str,
            rov1_name: str = "",
            rov2_name: str = "",
            gnss1_name: str = "",
            gnss2_name: str = "",
            depth1_name: str = "",
            depth2_name: str = "",

            mapping: dict[str, str],
            is_default: bool = False,
    ) -> int:
        self.ensure_bbox_config_schema()

        norm_mapping: dict[str, str] = {
            str(k).strip(): ("" if v is None else str(v).strip())
            for k, v in (mapping or {}).items()
            if str(k).strip()
        }

        with self._connect() as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("BEGIN IMMEDIATE")

            # ✅ Force default if DB empty OR no default exists yet
            total = conn.execute("SELECT COUNT(*) FROM BBox_Configs_List").fetchone()[0]
            has_default = conn.execute(
                "SELECT 1 FROM BBox_Configs_List WHERE IsDefault = 1 LIMIT 1"
            ).fetchone() is not None

            if total == 0 or (not is_default and not has_default):
                is_default = True

            # 1) Upsert config header
            conn.execute(
                """
                INSERT INTO BBox_Configs_List (Name, Vessel_name,IsDefault, rov1_name, rov2_name, gnss1_name, gnss2_name,depth1_name, depth2_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(Name) DO UPDATE SET
                    IsDefault = excluded.IsDefault,
                    rov1_name = excluded.rov1_name,
                    rov2_name = excluded.rov2_name,
                    gnss1_name = excluded.gnss1_name,
                    gnss2_name = excluded.gnss2_name
                """,
                (
                    name,vessel_name,
                    1 if is_default else 0,
                    rov1_name,
                    rov2_name,
                    gnss1_name,
                    gnss2_name,
                    depth1_name,
                    depth2_name
                ),
            )

            cfg_id = int(conn.execute(
                "SELECT ID FROM BBox_Configs_List WHERE Name = ?",
                (name,),
            ).fetchone()[0])

            # 2) Mark old rows unused (optional but recommended)
            conn.execute(
                "UPDATE BBox_Config SET inUse = 0, FileColumn = '' WHERE CONFIG_FK = ?",
                (cfg_id,),
            )

            # 3) Upsert mapping rows (UNIQUE(CONFIG_FK, FieldName))
            conn.executemany(
                """
                INSERT INTO BBox_Config (FieldName, FileColumn, inUse, CONFIG_FK)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(CONFIG_FK, FieldName) DO UPDATE SET
                    FileColumn = excluded.FileColumn,
                    inUse = excluded.inUse
                """,
                [
                    (field, col, 1 if col else 0, cfg_id)
                    for field, col in norm_mapping.items()
                ],
            )

            conn.commit()
            return cfg_id

    def ensure_bbox_config_schema(self):
        """
        Ensure all BBox config tables, constraints, triggers exist.
        Safe to call multiple times.
        """
        ddl = """
        CREATE TABLE IF NOT EXISTS BBox_Configs_List (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Name TEXT NOT NULL,
            IsDefault INTEGER DEFAULT 0,
            rov1_name TEXT,
            rov2_name TEXT,
            gnss1_name TEXT,
            gnss2_name TEXT,
            CONSTRAINT ux_bbox_configs_name UNIQUE (Name)
        );

        CREATE TRIGGER IF NOT EXISTS trg_bbox_default_singleton
        AFTER UPDATE OF IsDefault ON BBox_Configs_List
        WHEN NEW.IsDefault = 1
        BEGIN
            UPDATE BBox_Configs_List
            SET IsDefault = 0
            WHERE ID != NEW.ID;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_bbox_default_singleton_ins
        AFTER INSERT ON BBox_Configs_List
        WHEN NEW.IsDefault = 1
        BEGIN
            UPDATE BBox_Configs_List
            SET IsDefault = 0
            WHERE ID != NEW.ID;
        END;

        CREATE TABLE IF NOT EXISTS BBox_Config (
            ID INTEGER PRIMARY KEY,
            FieldName TEXT NOT NULL,
            FileColumn TEXT,
            inUse INTEGER DEFAULT 0,
            CONFIG_FK INTEGER NOT NULL,
            FOREIGN KEY (CONFIG_FK) REFERENCES BBox_Configs_List(ID)
                ON UPDATE CASCADE
                ON DELETE RESTRICT
        );

        CREATE UNIQUE INDEX IF NOT EXISTS ux_bbox_config_cfg_field
        ON BBox_Config (CONFIG_FK, FieldName);
        """

        with self._connect() as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.executescript(ddl)

    def get_bbox_configs_list(self):
        """
        Returns list of BBox configs ordered with default first.
        """
        self.ensure_bbox_config_schema()

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    ID,
                    Name,
                    Vessel_name,
                    IsDefault,
                    rov1_name,
                    rov2_name,
                    gnss1_name,
                    gnss2_name,
                    Depth1_name,
                    Depth2_name 
                FROM BBox_Configs_List
                ORDER BY IsDefault DESC, Name COLLATE NOCASE
                """
            ).fetchall()

        return [
            {
                "id": r["ID"],
                "name": r["Name"],
                'vessel_name':r["Vessel_name"],
                "is_default": bool(r["IsDefault"]),
                "rov1_name": r["rov1_name"],
                "rov2_name": r["rov2_name"],
                "gnss1_name": r["gnss1_name"],
                "gnss2_name": r["gnss2_name"],
                "Depth1_name": r["Depth1_name"],
                "Depth2_name": r["Depth2_name"],
            }
            for r in rows
        ]

    def set_bbox_config_default(self, config_id: int) -> None:
        """
        Set given BBox config as default.
        Triggers will automatically reset others to IsDefault = 0.
        """
        self.ensure_bbox_config_schema()

        with self._connect() as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("BEGIN IMMEDIATE")

            # ensure config exists
            row = conn.execute(
                "SELECT ID FROM BBox_Configs_List WHERE ID = ?",
                (config_id,),
            ).fetchone()

            if not row:
                raise ValueError(f"BBox config with ID={config_id} does not exist")

            # set default (triggers handle the rest)
            conn.execute(
                "UPDATE BBox_Configs_List SET IsDefault = 1 WHERE ID = ?",
                (config_id,),
            )

            conn.commit()

    def ensure_blackbox_schema(self):
        """
        Creates BlackBox tables if missing.
        Includes BlackBox_Files for File_FK support.
        """
        ddl = """
        CREATE TABLE IF NOT EXISTS BlackBox_Files (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            FileName TEXT NOT NULL,
            Config_FK INTEGER,
            UploadedAt TEXT DEFAULT (datetime('now')),
            UNIQUE(FileName, Config_FK)
        );

        CREATE TABLE IF NOT EXISTS BlackBox (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,

            TimeStamp TEXT,

            VesselEasting REAL,
            VesselNorthing REAL,
            VesselElevation REAL,
            VesselHDG REAL,
            VesselSOG REAL,
            VesselCOG REAL,

            GNSS1_Easting REAL,
            GNSS1_Northing REAL,
            GNSS1_Elevation REAL,
            GNSS2_Easting REAL,
            GNSS2_Northing REAL,
            GNSS2_Elevation REAL,

            ROV1_INS_Easting REAL,
            ROV1_INS_Northing REAL,
            ROV1_USBL_Easting REAL,
            ROV1_USBL_Northing REAL,
            ROV1_Depth REAL,
            ROV1_HDG REAL,
            ROV1_SOG REAL,
            ROV1_COG REAL,
            ROV1_TMS_Easting REAL,
            ROV1_TMS_Northing REAL,
            ROV1_TMS_Depth REAL,

            ROV2_INS_Easting REAL,
            ROV2_INS_Northing REAL,
            ROV2_USBL_Easting REAL,
            ROV2_USBL_Northing REAL,
            ROV2_Depth REAL,
            ROV2_HDG REAL,
            ROV2_SOG REAL,
            ROV2_COG REAL,
            ROV2_TMS_Easting REAL,
            ROV2_TMS_Northing REAL,
            ROV2_TMS_Depth REAL,

            Crane_Easting REAL,
            Crane_Northing REAL,
            Crane_Depth REAL,

            GNSS1_RefStation TEXT,
            GNSS1_NOS INTEGER,
            GNSS1_DiffAge REAL,
            GNSS1_FixQuality INTEGER,
            GNSS1_HDOP REAL,
            GNSS1_PDOP REAL,
            GNSS1_VDOP REAL,

            GNSS2_RefStation TEXT,
            GNSS2_NOS INTEGER,
            GNSS2_DiffAge REAL,
            GNSS2_FixQuality INTEGER,
            GNSS2_HDOP REAL,
            GNSS2_PDOP REAL,
            GNSS2_VDOP REAL,

            ROV1_PITCH REAL,
            ROV1_ROLL REAL,
            ROV2_PITCH REAL,
            ROV2_ROLL REAL,

            ROV1_Depth1 REAL,
            ROV1_Depth2 REAL,
            ROV2_Depth1 REAL,
            ROV2_Depth2 REAL,

            Barometer REAL,

            File_FK INTEGER,
            FOREIGN KEY (File_FK) REFERENCES BlackBox_Files(ID) ON DELETE RESTRICT
        );

        CREATE INDEX IF NOT EXISTS idx_blackbox_ts ON BlackBox(TimeStamp);
        CREATE INDEX IF NOT EXISTS idx_blackbox_file ON BlackBox(File_FK);
        """
        with self._connect() as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.executescript(ddl)

    def get_bbox_config_mapping(self, config_id: int) -> dict[str, str]:
        """
        Returns mapping dict from BBox_Config for given config_id:
        { "VesselEasting": "IP E (Metre) (32615)", ... }
        Only rows with inUse=1 and non-empty FileColumn are returned.
        """
        self.ensure_bbox_config_schema()

        with self._connect() as conn:
            row = conn.execute(
                "SELECT ID FROM BBox_Configs_List WHERE ID=?",
                (config_id,),
            ).fetchone()
            if not row:
                raise ValueError(f"Config ID={config_id} not found")

            rows = conn.execute(
                """
                SELECT FieldName, FileColumn
                FROM BBox_Config
                WHERE CONFIG_FK = ?
                  AND inUse = 1
                  AND COALESCE(TRIM(FileColumn),'') <> ''
                """,
                (config_id,),
            ).fetchall()

        return {r["FieldName"]: r["FileColumn"] for r in rows}

    def upsert_blackbox_file(self, file_name: str, config_id: int) -> int:
        self.ensure_blackbox_schema()

        with self._connect() as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("BEGIN IMMEDIATE")

            conn.execute(
                """
                INSERT INTO BlackBox_Files (FileName, Config_FK)
                VALUES (?, ?)
                ON CONFLICT(FileName, Config_FK) DO NOTHING
                """,
                (file_name, config_id),
            )
            row = conn.execute(
                "SELECT ID FROM BlackBox_Files WHERE FileName=? AND Config_FK=?",
                (file_name, config_id),
            ).fetchone()
            if not row:
                raise RuntimeError("Failed to create/read BlackBox_Files row")

            conn.commit()
            return int(row["ID"])

    import pandas as pd

    def load_blackbox_csv(self, *, uploaded_file, mapping: dict[str, str], file_fk: int, chunk_rows: int = 5000) -> int:
        """
        Reads CSV and inserts into BlackBox.
        mapping: DB field -> CSV column name.
        """
        self.ensure_blackbox_schema()

        # get DB columns (exclude ID)
        with self._connect() as conn:
            schema = conn.execute("PRAGMA table_info(BlackBox)").fetchall()
        db_cols = [r[1] for r in schema if r[1] not in ("ID",)]  # keep File_FK

        # build numeric columns list (everything except TEXT-like)
        # Here we treat TimeStamp + RefStation as text; others numeric.
        text_cols = {"TimeStamp", "GNSS1_RefStation", "GNSS2_RefStation"}
        numeric_cols = [c for c in db_cols if c not in text_cols and c != "File_FK"]

        insert_cols = [c for c in db_cols if c != "ID"]  # includes File_FK
        placeholders = ",".join(["?"] * len(insert_cols))
        sql = f"INSERT INTO BlackBox ({','.join(insert_cols)}) VALUES ({placeholders})"

        total_inserted = 0

        # IMPORTANT: pandas can read Django UploadedFile directly
        for chunk in pd.read_csv(uploaded_file, chunksize=chunk_rows, low_memory=False):
            out = pd.DataFrame(index=chunk.index)

            # fill mapped columns
            for db_field in insert_cols:
                if db_field == "File_FK":
                    continue
                csv_col = mapping.get(db_field, "")
                if csv_col and csv_col in chunk.columns:
                    out[db_field] = chunk[csv_col]
                else:
                    out[db_field] = None

            # always set File_FK
            out["File_FK"] = int(file_fk)

            # coerce numeric
            for c in numeric_cols:
                if c in out.columns:
                    out[c] = pd.to_numeric(out[c], errors="coerce")

            # timestamp as string
            if "TimeStamp" in out.columns:
                out["TimeStamp"] = out["TimeStamp"].astype(str)

            rows = out[insert_cols].itertuples(index=False, name=None)

            with self._connect() as conn:
                conn.execute("PRAGMA foreign_keys = ON;")
                conn.execute("BEGIN")
                conn.executemany(sql, rows)
                conn.commit()

            total_inserted += len(out)

        return total_inserted

    def get_blackbox_files(self):
        """
        Return all records from BlackBox_Files table.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT *
                FROM BlackBox_Files
                ORDER BY FileName
                """
            )
            return [dict(r) for r in cur.fetchall()]
    def get_bbox_file_table(self):
        bbox_file_list = self.get_blackbox_files()
        html = render_to_string("rov/partials/bbox_list_body.html",{"bbox_file_list": bbox_file_list})
        return html
    def _detect_encoding(self, fname: str | Path) -> str:
        p = str(fname)
        if hasattr(self, "prj") and hasattr(self.prj, "detect_encoding"):
            try:
                enc = self.prj.detect_encoding(p)
                if enc:
                    return enc
            except Exception:
                pass
        return "utf-8-sig"

    def _get_separator_from_file(self, fname: str | Path, encoding: str) -> str:
        p = str(fname)
        if hasattr(self, "get_separator_from_file"):
            try:
                sep = self.get_separator_from_file(p, encoding=encoding)
                if sep:
                    return sep
            except Exception:
                pass

        with open(p, "r", encoding=encoding, errors="ignore") as f:
            head = f.read(4096)

        if head.count(",") > 0 and head.count(",") >= head.count("\t"):
            return ","
        if head.count("\t") > 0:
            return "\t"
        return r"\s+"

    @staticmethod
    def _guess_sep_from_text(sample: str) -> str:
        if sample.count(",") >= sample.count("\t") and sample.count(",") > 0:
            return ","
        if "\t" in sample:
            return "\t"
        return r"\s+"

    @staticmethod
    def _read_uploaded_as_text(file_obj) -> str:
        try:
            file_obj.seek(0)
        except Exception:
            pass

        raw = file_obj.read()
        if isinstance(raw, str):
            return raw
        if not raw:
            return ""
        try:
            return raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            return raw.decode("cp1252", errors="ignore")

    def load_sm_file_to_db(
            self,
            fname,  # Path/str OR UploadedFile OR file-like
            *,
            update_key: str = "unique",  # "unique" or "linepointidx"
    ) -> dict:
        """
        Import SM and UPDATE-ONLY DSR (no inserts).

        Key:
          - unique       -> (Line, Station, NODE_HEX_ID)  [NODE_HEX_ID is HASHED in DSR]
          - linepointidx -> (LinePointIdx)

        Node rule:
          - if AU QR Code has data -> NODE_HEX_ID = _node_to_int_12(AU QR Code)
          - else                   -> NODE_HEX_ID = _node_to_int_12(normalized RemoteUnit)

        IMPORTANT:
          RemoteUnit normalization matches your existing DSR hashing:
            "297080001/14987"  -> "14987 297080001"
        """
        import io
        import re
        import pandas as pd
        from pathlib import Path

        UPDATE_WHITELIST = [
            "Area",
            "RemoteUnit",
            "AUQRCode",
            "AURFID",
            "CUSerialNumber",
            "Status",
            "DeploymentType",
            "StartTimeEpoch",
            "StartTimeUTC",
            "DeployTimeEpoch",
            "DeployTimeUTC",
            "PickupTimeEpoch",
            "PickupTimeUTC",
            "StopTimeEpoch",
            "StopTimeUTC",
            "SPSX",
            "SPSY",
            "SPSZ",
            "ActualX",
            "ActualY",
            "ActualZ",
            "Deployed",
            "PickedUp",
            "Archived",
            "DeviceID",
            "BinID",
            "ExpectedTraces",
            "CollectedTraces",
            "DownloadedDatainMB",
            "ExpectedDatainMB",
            "DownloadError",
        ]

        def _clean_col(c: str) -> str:
            return re.sub(r"\W", "", str(c)).strip()

        def _find_col(df_cols, *aliases):
            cleaned = {re.sub(r"\W", "", str(c)).lower(): c for c in df_cols}
            for a in aliases:
                key = re.sub(r"\W", "", str(a)).lower()
                if key in cleaned:
                    return cleaned[key]
            return None

        def _norm_str(v):
            if v is None:
                return None
            s = str(v).strip()
            if s == "" or s.lower() in {"nan", "none", "null", "-1"}:
                return None
            return s

        def _normalize_remote_unit(v: str | None) -> str | None:
            """
            Normalize SM RemoteUnit so it matches the string used to hash NODE_HEX_ID in DSR.
            Your example:
              "297080001/14987" -> "14987 297080001"
            """
            s = _norm_str(v)
            if not s:
                return None

            # collapse whitespace
            s = re.sub(r"\s+", " ", s)

            # If it is "AAA/BBB" swap to "BBB AAA"
            if "/" in s:
                parts = [p.strip() for p in s.split("/") if p.strip()]
                if len(parts) == 2:
                    a, b = parts[0], parts[1]
                    return f"{b} {a}"

            return s

        # -------------------------
        # 1) Read into pandas
        # -------------------------
        if isinstance(fname, (str, Path)):
            p = Path(fname)
            if not p.exists():
                return {"error": f"File not found: {p}"}

            encoding = self._detect_encoding(p)
            sep = self._get_separator_from_file(p, encoding=encoding)

            try:
                engine = "python" if isinstance(sep, str) and (sep == r"\s+" or "\\" in sep) else "c"
                df = pd.read_csv(p, sep=sep, encoding=encoding, engine=engine)
            except Exception as e:
                return {"error": f"SM read_csv error: {e} ({p})"}
        else:
            text = self._read_uploaded_as_text(fname)
            if not text.strip():
                return {"error": "SM file is empty"}

            sep = self._guess_sep_from_text(text[:4096])
            try:
                buf = io.StringIO(text)
                engine = "python" if sep == r"\s+" else "c"
                df = pd.read_csv(buf, sep=sep, engine=engine)
            except Exception as e:
                return {"error": f"SM read_csv(upload) error: {e}"}

        if df.empty:
            return {"error": "SM file has no rows"}

        rows_in_file_raw = int(len(df))
        original_cols = list(df.columns)

        # -------------------------
        # 2) Normalize column names (remove spaces) + detect needed columns
        # -------------------------
        df.columns = [_clean_col(c) for c in df.columns]

        line_col = _find_col(df.columns, "Line", "ReceiverLine", "RecLine")
        station_col = _find_col(df.columns, "Station", "Point", "Stn", "Sta", "StationNo", "StationNumber")
        if not line_col or not station_col:
            return {"error": f"SM must contain Line and Station. Columns: {original_cols}"}

        au_col = _find_col(df.columns, "AUQRCode", "AUQRCodeID", "AUQR", "AUQRID", "AUQRcode")
        ru_col = _find_col(df.columns, "RemoteUnit", "RemoteUnitID", "RemoteUnitSN", "RemoteUnitSerial", "RU")
        if not au_col or not ru_col:
            return {"error": f"SM must contain both AU QR Code and Remote Unit. Columns: {original_cols}"}

        if line_col != "Line":
            df.rename(columns={line_col: "Line"}, inplace=True)
        if station_col != "Station":
            df.rename(columns={station_col: "Station"}, inplace=True)
        if au_col != "AUQRCode":
            df.rename(columns={au_col: "AUQRCode"}, inplace=True)
        if ru_col != "RemoteUnit":
            df.rename(columns={ru_col: "RemoteUnit"}, inplace=True)

        # Parse Line/Station
        df["Line"] = pd.to_numeric(df["Line"], errors="coerce")
        df["Station"] = pd.to_numeric(df["Station"], errors="coerce")
        df = df[df["Line"].notna() & df["Station"].notna()].copy()
        df["Line"] = df["Line"].astype("int64")
        df["Station"] = df["Station"].astype("int64")
        if df.empty:
            return {"error": "No valid Line/Station rows after parsing."}

        # -------------------------
        # 3) NODE_HEX_ID (HASHED) with RemoteUnit normalization
        # -------------------------
        au_clean = df["AUQRCode"].apply(_norm_str)
        ru_norm = df["RemoteUnit"].apply(_normalize_remote_unit)

        used_au = int(au_clean.notna().sum())
        used_ru = int((au_clean.isna() & ru_norm.notna()).sum())

        au_hash = au_clean.apply(self._node_to_int_12)
        ru_hash = ru_norm.apply(self._node_to_int_12)

        df["NODE_HEX_ID"] = au_hash.fillna(ru_hash)
        df = df[df["NODE_HEX_ID"].notna()].copy()
        if df.empty:
            return {"error": "All rows missing node values (AU QR Code and RemoteUnit both empty/unparseable)."}

        df["NODE_HEX_ID"] = df["NODE_HEX_ID"].astype("int64")

        # IMPORTANT: update RemoteUnit column in DSR with normalized value (optional but recommended)
        # so DSR text matches its hashing convention.
        df["RemoteUnit"] = ru_norm

        # -------------------------
        # 4) Compute LinePointIdx (kept)
        # -------------------------
        scaler = int(getattr(self, "pointscaler", 0) or 0)
        if scaler <= 0:
            scaler = 100000

        df["LinePoint"] = (df["Line"] * scaler + df["Station"]).astype("int64")
        df["RecIdx"] = df.groupby(["LinePoint", "NODE_HEX_ID"]).cumcount().add(1).astype("int64")
        df["LinePointIdx"] = (df["LinePoint"] * 10 + df["RecIdx"]).astype("int64")

        if "ROV" in df.columns:
            df.drop(columns=["ROV"], inplace=True)

        # -------------------------
        # 5) UPDATE-ONLY using TEMP TABLE + QC stats
        # -------------------------
        update_key = update_key.lower().strip()
        if update_key not in ("unique", "linepointidx"):
            return {"error": "update_key must be 'unique' or 'linepointidx'"}

        with self._connect() as conn:
            info = conn.execute("PRAGMA table_info(DSR)").fetchall()
            table_cols = [r["name"] for r in info]
            table_cols_lc = {c.lower(): c for c in table_cols}

            if update_key == "unique":
                for req in ("line", "station", "node_hex_id"):
                    if req not in table_cols_lc:
                        return {"error": f"DSR missing required column for unique key: {req.upper()}"}
                key_cols = ["Line", "Station", "NODE_HEX_ID"]
            else:
                if "linepointidx" not in table_cols_lc:
                    return {"error": "DSR missing required column for linepointidx key: LinePointIdx"}
                key_cols = ["LinePointIdx"]

            # Map DF column casing to DB column names
            df.rename(
                columns={c: table_cols_lc[c.lower()] for c in df.columns if c.lower() in table_cols_lc},
                inplace=True
            )

            # Whitelist SET columns
            set_cols = [c for c in UPDATE_WHITELIST if
                        (c in df.columns and c.lower() in table_cols_lc and c not in key_cols)]
            if not set_cols:
                present = [c for c in UPDATE_WHITELIST if c in df.columns]
                return {
                    "error": "None of the whitelisted columns are available to update in DSR.",
                    "whitelist_present_in_sm": present,
                    "dsr_cols": table_cols,
                }

            # De-dup by key
            df = df.drop_duplicates(subset=key_cols, keep="last").copy()
            unique_keys_in_file = int(len(df))

            temp_cols = key_cols + set_cols

            conn.execute("DROP TABLE IF EXISTS temp_sm_src;")
            col_defs = []
            for c in temp_cols:
                if c in ("Line", "Station", "LinePoint", "RecIdx", "LinePointIdx", "NODE_HEX_ID"):
                    col_defs.append(f"{c} INTEGER")
                else:
                    col_defs.append(f"{c} TEXT")
            conn.execute(f"CREATE TEMP TABLE temp_sm_src ({', '.join(col_defs)});")

            placeholders = ", ".join(["?"] * len(temp_cols))
            ins_sql = f"INSERT INTO temp_sm_src ({', '.join(temp_cols)}) VALUES ({placeholders});"

            rows = []
            for r in df.itertuples(index=False):
                d = r._asdict()
                rows.append(tuple(d.get(c) for c in temp_cols))

            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany(ins_sql, rows)

                # QC: existing keys
                if update_key == "unique":
                    exist_sql = """
                    SELECT COUNT(*)
                    FROM DSR
                    JOIN temp_sm_src s
                      ON DSR.Line = s.Line
                     AND DSR.Station = s.Station
                     AND DSR.NODE_HEX_ID = s.NODE_HEX_ID;
                    """
                else:
                    exist_sql = """
                    SELECT COUNT(*)
                    FROM DSR
                    JOIN temp_sm_src s
                      ON DSR.LinePointIdx = s.LinePointIdx;
                    """
                existing_in_dsr = int(conn.execute(exist_sql).fetchone()[0])

                # UPDATE ONLY
                where_match = " AND ".join([f"s.{k} = DSR.{k}" for k in key_cols])
                set_clause = ", ".join([
                    f"{c} = (SELECT s.{c} FROM temp_sm_src s WHERE {where_match})"
                    for c in set_cols
                ])

                upd_sql = f"""
                UPDATE DSR
                SET {set_clause}
                WHERE EXISTS (
                    SELECT 1 FROM temp_sm_src s
                    WHERE {where_match}
                );
                """
                conn.execute(upd_sql)
                updated_rows = int(conn.execute("SELECT changes();").fetchone()[0])

                conn.commit()

            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                return {"error": f"DSR update-only failed: {e}"}

            finally:
                try:
                    conn.execute("DROP TABLE IF EXISTS temp_sm_src;")
                except Exception:
                    pass

        skipped_missing = max(0, unique_keys_in_file - existing_in_dsr)

        return {
            "success": True,
            "update_key": update_key,
            "rows_in_file_raw": rows_in_file_raw,
            "unique_keys_in_file": unique_keys_in_file,
            "used_au_qr": used_au,
            "used_remote_unit": used_ru,
            "existing_in_dsr": existing_in_dsr,
            "skipped_missing_key": int(skipped_missing),
            "updated_rows": updated_rows,
            "key_cols": key_cols,
            "set_cols": set_cols,
            "remoteunit_normalization": 'If RemoteUnit contains "AAA/BBB" -> "BBB AAA"',
        }

    def load_fb_from_file(self, file_obj_or_path, *, chunk_rows: int = 50000, file_fk: int | None = None) -> dict:
        """
        FB/RECDB whitespace-delimited loader.
        UPSERT into REC_DB by UNIQUE(REC_ID, DEPLOY, RPI).

        Required columns in file:
          - REC_ID
          - RPI
          - DEPLOY

        Extra:
          - FINPITCH/FINROLL/FINYAW -> PITCHFIN/ROLLFIN/YAWFIN
          - Preplot_FK (FK -> RLPreplot.ID) is filled by mapping:
                REC_DB.Line = RLPreplot.Line
            (no MIN/MAX; we just take an ID returned by the query)
        """

        # -------------------------
        # helper: read upload as text
        # -------------------------
        def _read_uploaded_as_text(fobj) -> str:
            try:
                fobj.seek(0)
            except Exception:
                pass

            raw = fobj.read()
            if isinstance(raw, str):
                return raw
            if not raw:
                return ""
            try:
                return raw.decode("utf-8-sig")
            except UnicodeDecodeError:
                return raw.decode("cp1252", errors="ignore")

        # -------------------------
        # helper: fetch mapping Line -> RLPreplot.ID (no MIN/MAX)
        # If duplicates exist, we keep the first ID we see for that Line.
        # -------------------------
        def _fetch_preplot_id_by_line(conn, line_values) -> dict:
            BATCH = 900  # keep under SQLite variable limit
            out = {}
            if not line_values:
                return out

            lines = [l for l in set(line_values) if l is not None]
            if not lines:
                return out

            for i in range(0, len(lines), BATCH):
                batch = lines[i: i + BATCH]
                ph = ",".join(["?"] * len(batch))

                # Deterministic ordering, but no aggregation:
                sql = (
                    'SELECT "Line" AS line, "ID" AS id '
                    f'FROM RLPreplot WHERE "Line" IN ({ph}) '
                    'ORDER BY "Line", "ID"'
                )

                rows = conn.execute(sql, batch).fetchall()
                for r in rows:
                    ln = int(r["line"])
                    if ln not in out:
                        out[ln] = int(r["id"])
            return out

        # -------------------------
        # scalers from rl_mask (all return int)
        # -------------------------
        line_s = self.linescaler
        lp_s = self.linepointscaler
        lpi_s = self.linepointidxscaler()

        if not line_s or not lp_s or not lpi_s:
            return {"error": "Invalid rl_mask scalers"}

        # -------------------------
        # mask positions for parsing Line / Point from REC_ID
        # -------------------------
        geom = self.pdb.get_geometry()
        mask = getattr(geom, "rl_mask", "") or ""
        if not mask or "L" not in mask or "P" not in mask:
            return {"error": "rl_mask missing or invalid"}

        line_pos0 = mask.index("L")
        line_pos1 = mask.rfind("L")
        point_pos0 = mask.index("P")
        point_pos1 = mask.rfind("P")

        num_point_digits = point_pos1 - point_pos0 + 1
        scalar_point = int("1" + ("0" * num_point_digits))

        # -------------------------
        # FIN* rename map (file -> DB)
        # -------------------------
        fin_rename = {
            "FINPITCH": "PITCHFIN",
            "FINROLL": "ROLLFIN",
            "FINYAW": "YAWFIN",
        }

        # -------------------------
        # read REC_DB schema once
        # -------------------------
        with self._connect() as conn:
            rec_info = conn.execute("PRAGMA table_info(REC_DB)").fetchall()
            rec_cols = {r["name"].lower(): r["name"] for r in rec_info}

        conflict_cols = ["REC_ID", "DEPLOY", "RPI"]
        for req in ("rec_id", "deploy", "rpi"):
            if req not in rec_cols:
                return {"error": f'REC_DB table missing required column "{req.upper()}"'}
        db_conflict = [rec_cols[c.lower()] for c in conflict_cols]

        # -------------------------
        # build reader (whitespace)
        # -------------------------
        is_path = isinstance(file_obj_or_path, (str, Path))

        if is_path:
            p = Path(file_obj_or_path)
            if not p.exists():
                return {"error": f"File not found: {p}"}

            enc = "utf-8-sig"
            if hasattr(self, "prj") and hasattr(self.prj, "detect_encoding"):
                try:
                    enc = self.prj.detect_encoding(str(p)) or enc
                except Exception:
                    pass

            reader = pd.read_csv(
                p,
                sep=r"\s+",
                encoding=enc,
                chunksize=chunk_rows,
                low_memory=False,
                engine="python",
            )
            src_name = str(p)
        else:
            text = _read_uploaded_as_text(file_obj_or_path)
            if not text.strip():
                return {"error": "File empty or unreadable"}

            reader = pd.read_csv(
                io.StringIO(text),
                sep=r"\s+",
                chunksize=chunk_rows,
                low_memory=False,
                engine="c",
            )
            src_name = getattr(file_obj_or_path, "name", "uploaded_file")

        # -------------------------
        # process chunks
        # -------------------------
        total_rows = 0
        total_upserts = 0
        total_preplot_linked = 0

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for df in reader:
                    if df is None or df.empty:
                        continue

                    total_rows += len(df)

                    # normalize headers
                    df.columns = [re.sub(r"\W", "", str(c)).strip() for c in df.columns]

                    # apply FIN* renames
                    for old, new in fin_rename.items():
                        if old in df.columns:
                            df.rename(columns={old: new}, inplace=True)

                    # REQUIRED columns
                    if ("REC_ID" not in df.columns) or ("RPI" not in df.columns) or ("DEPLOY" not in df.columns):
                        raise ValueError("REC_DB file must contain REC_ID, RPI and DEPLOY")

                    # numeric safety
                    df["RPI"] = pd.to_numeric(df["RPI"], errors="coerce").fillna(0).astype("int64")
                    df["DEPLOY"] = pd.to_numeric(df["DEPLOY"], errors="coerce").fillna(0).astype("int64")

                    # parse Line/Point from REC_ID
                    rec_str = df["REC_ID"].astype(str)

                    line_str = rec_str.str.slice(line_pos0, line_pos1 + 1)
                    point_str = rec_str.str.slice(point_pos0, point_pos1 + 1)

                    line_val = pd.to_numeric(line_str, errors="coerce").fillna(0).astype("int64")
                    point_val = pd.to_numeric(point_str, errors="coerce").fillna(0).astype("int64")

                    df["Line"] = line_val
                    df["Point"] = point_val
                    df["LinePoint"] = (line_val * scalar_point + point_val).astype("int64")

                    # LinePointIdx = LinePoint * 10 + max(RPI, DEPLOY)   (you said both are <= 9)
                    suffix_val = df[["RPI", "DEPLOY"]].max(axis=1).astype("int64")
                    df["LinePointIdx"] = (df["LinePoint"] * 10 + suffix_val).astype("int64")

                    # Tier calculations (use rl_mask scalers, and add original values)
                    if "TIER" not in df.columns:
                        df["TIER"] = 1
                    tier_val = pd.to_numeric(df["TIER"], errors="coerce").fillna(1).astype("int64")

                    df["TierLine"] = (tier_val * line_s + df["Line"]).astype("int64")
                    df["TierLinePoint"] = (tier_val * lp_s + df["LinePoint"]).astype("int64")
                    df["TierLinePointIdx"] = (tier_val * lpi_s + df["LinePointIdx"]).astype("int64")

                    # inject File_FK if requested and column exists in REC_DB
                    if file_fk is not None and "file_fk" in rec_cols:
                        df["File_FK"] = int(file_fk)

                    # Preplot_FK lookup from RLPreplot by Line (no MIN/MAX)
                    if "preplot_fk" in rec_cols:
                        lines = df["Line"].dropna().astype("int64").unique().tolist()
                        preplot_map = _fetch_preplot_id_by_line(conn, lines)
                        df["Preplot_FK"] = df["Line"].map(preplot_map)
                        total_preplot_linked += int(pd.notnull(df["Preplot_FK"]).sum())

                    # keep only REC_DB columns (exclude ID)
                    keep_cols = [c for c in df.columns if c.lower() in rec_cols and c.lower() != "id"]

                    # ensure conflict cols are present
                    for cc in conflict_cols:
                        if cc not in keep_cols:
                            keep_cols.append(cc)

                    # rename to DB exact case
                    rename_to_db = {c: rec_cols[c.lower()] for c in keep_cols}
                    df.rename(columns=rename_to_db, inplace=True)

                    db_cols = [rename_to_db[c] for c in keep_cols]
                    update_cols = [c for c in db_cols if c not in db_conflict]

                    col_sql = ", ".join(f'"{c}"' for c in db_cols)
                    val_sql = ", ".join("?" for _ in db_cols)
                    conflict_sql = ", ".join(f'"{c}"' for c in db_conflict)

                    if update_cols:
                        update_sql = ", ".join(f'"{c}"=excluded."{c}"' for c in update_cols)
                        sql = (
                            f'INSERT INTO REC_DB ({col_sql}) VALUES ({val_sql}) '
                            f'ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_sql}'
                        )
                    else:
                        sql = (
                            f'INSERT INTO REC_DB ({col_sql}) VALUES ({val_sql}) '
                            f'ON CONFLICT ({conflict_sql}) DO NOTHING'
                        )

                    sub = df[db_cols].where(pd.notnull(df), None)
                    values = list(sub.itertuples(index=False, name=None))

                    if values:
                        conn.executemany(sql, values)
                        total_upserts += len(values)

                conn.commit()

            except Exception as e:
                conn.rollback()
                return {"error": f"load_fb_from_file error: {e}", "file": src_name}

        return {
            "success": f"File {src_name} processed",
            "rows_read": int(total_rows),
            "upserts_attempted": int(total_upserts),
            "preplot_fk_linked": int(total_preplot_linked),
            "preplot_fk_rule": "Preplot_FK = RLPreplot.ID where RLPreplot.Line = REC_DB.Line (first ID picked if duplicates)",
        }

    def export_dsr_to_csv(
            self,
            file_name: str = "",
            table_name: str = "DSR",
            sql: str = ""
    ) -> str:
        """
        Export SQLite table or custom SQL query to CSV file.
        Returns full path to created CSV.
        """

        # choose output file
        if file_name:
            out_path = Path(file_name)
        else:
            ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = Path(self.db_path).with_name(f"{table_name}_{ts}.csv")

        out_path.parent.mkdir(parents=True, exist_ok=True)

        with self._connect() as conn, out_path.open("w", newline="", encoding="utf-8") as f:
            cur = conn.cursor()

            if not sql:
                cur.execute(f'SELECT * FROM "{table_name}"')
            else:
                cur.execute(sql)

            rows = cur.fetchall()

            if not rows:
                print(f"Warning: table '{table_name}' is empty.")
                return ""

            cols = [d[0] for d in cur.description]

            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)

        return str(out_path)

    def build_dsr_export_sql(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(DSR)")
            cols = [row[1] for row in cur.fetchall()]

            cols.remove("StartTimeUTC")

            col_string = ", ".join(f'"{c}"' for c in cols)

            sql = f"""
            SELECT
    {col_string},

    -- normalized StartTimeUTC (your existing logic)
    COALESCE(NULLIF(StartTimeUTC, '-1'), DeployTimeUTC) AS StartTimeUTC,

    -- days since (StartTimeUTC or DeployTimeUTC) as INTEGER
    CAST(
        julianday('now') - julianday(
            CASE
                WHEN COALESCE(NULLIF(StartTimeUTC, '-1'), DeployTimeUTC) IS NULL
                     OR TRIM(COALESCE(NULLIF(StartTimeUTC, '-1'), DeployTimeUTC)) = ''
                THEN NULL
                ELSE
                    -- convert "MM/DD/YYYY ..." -> "YYYY-MM-DD"
                    substr(COALESCE(NULLIF(StartTimeUTC, '-1'), DeployTimeUTC), 7, 4) || '-' ||
                    substr(COALESCE(NULLIF(StartTimeUTC, '-1'), DeployTimeUTC), 1, 2) || '-' ||
                    substr(COALESCE(NULLIF(StartTimeUTC, '-1'), DeployTimeUTC), 4, 2)
            END
        )
    AS INTEGER) AS DaysSinceStart

FROM DSR
WHERE Area IS NOT NULL
  AND TRIM(Area) <> '';

            """

            return sql

    def get_dsr_statistics(self, view_name: str = "DEPLOY_ROV_Summary") -> list[dict]:
        """
        Read DSR statistics from a SQLite VIEW and return list of dicts.

        Expected view columns:
          Rov, Lines, Stations, Nodes, Days,
          SMLine, SMStations, SMNodes,
          SMRLine, SMRStations, SMRNodes,
          SMDDLine, SMDDStations, SMDDNodes,
          ProcLine, ProcStations, ProcNodes

        Returns:
          [
            { "Rov": "...", "Lines": 0, ... },
            ...
          ]
        """
        sql = f"""
            SELECT *
            FROM {view_name}
            ORDER BY Rov
        """
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql).fetchall()
            return [dict(r) for r in rows]

    def get_table_data(self, table_name: str):
        with self._connect() as conn:
            cur = conn.cursor()

            # check table exists


            # check if empty
            cur.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            if not cur.fetchone():
                return {"warning": f"Table '{table_name}' is empty.", "data": []}

            # otherwise fetch data
            cur.execute(f"SELECT * FROM {table_name}")
            rows = cur.fetchall()
            return {"data": rows}
    def get_dsr_html_stat(self)->Any:
        data = self.get_dsr_statistics("DEPLOY_ROV_Summary")
        html = render_to_string("rov/partials/dsr_statistic_table.html",{"data":data})
        return html

    def get_rovs_for_timeframe(
            self,
            mode: str = "day",
            day: str | None = None,
            dt_from: str | None = None,
            dt_to: str | None = None,
            table: str = "DSR",
            rov_deploy_col: str = "ROV",
            rov_recover_col: str = "ROV1",
            ts_deploy_col: str = "TimeStamp",
            ts_recover_col: str = "TimeStamp1",
    ) -> list[str]:
        """
        Return unique sorted list of ROV names for selected timeframe, combining:
          - deployment: DSR.ROV filtered by DSR.TimeStamp
          - recovery:   DSR.ROV1 filtered by DSR.TimeStamp1

        mode:
          - "day": day='YYYY-MM-DD'
          - "interval": dt_from/dt_to from datetime-local ('YYYY-MM-DDTHH:MM') or 'YYYY-MM-DD HH:MM[:SS]'
        """
        mode = (mode or "day").strip().lower()

        def _norm_dt(s: str) -> str:
            s = (s or "").strip()
            if not s:
                raise ValueError("Empty datetime string")
            if "T" in s:
                s = s.replace("T", " ")
            if len(s) == 16:  # 'YYYY-MM-DD HH:MM'
                s += ":00"
            return s

        if mode == "day":
            if not day:
                raise ValueError("day is required when mode='day'")
            ts_from = f"{day} 00:00:00"
            import datetime as _dt
            d0 = _dt.datetime.strptime(day, "%Y-%m-%d")
            d1 = d0 + _dt.timedelta(days=1)
            ts_to = d1.strftime("%Y-%m-%d 00:00:00")
        else:
            if not dt_from or not dt_to:
                raise ValueError("dt_from and dt_to are required when mode='interval'")
            ts_from, ts_to = _norm_dt(dt_from), _norm_dt(dt_to)

        sql = f"""
        WITH deploy AS (
            SELECT TRIM({rov_deploy_col}) AS rov
            FROM {table}
            WHERE {ts_deploy_col} IS NOT NULL
              AND TRIM({ts_deploy_col}) <> ''
              AND {ts_deploy_col} >= ?
              AND {ts_deploy_col} < ?
              AND {rov_deploy_col} IS NOT NULL
              AND TRIM({rov_deploy_col}) <> ''
        ),
        rec AS (
            SELECT TRIM({rov_recover_col}) AS rov
            FROM {table}
            WHERE {ts_recover_col} IS NOT NULL
              AND TRIM({ts_recover_col}) <> ''
              AND {ts_recover_col} >= ?
              AND {ts_recover_col} < ?
              AND {rov_recover_col} IS NOT NULL
              AND TRIM({rov_recover_col}) <> ''
        )
        SELECT rov FROM deploy
        UNION
        SELECT rov FROM rec
        ORDER BY rov
        """

        with self._connect() as conn:
            rows = conn.execute(sql, (ts_from, ts_to, ts_from, ts_to)).fetchall()

        return [r["rov"] for r in rows]

    def get_daily_recovery(
            self,
            date: str | None = None,
            line: str | None = None,
            rov: str | None = None,
            view_name: str = "Daily_Recovery",
            order_by: str = "ProdDate, Line, ROV",
    ):
        """
        Read data from SQLite view/table (default: Daily_Recovery).

        Parameters:
            date : 'YYYY-MM-DD'
            line : line name
            rov  : rov name
            view_name : SQLite view or table name
            order_by : custom ORDER BY clause

        Returns:
            list of dict rows
        """

        sql = f"""
            SELECT
                ProdDate,
                Line,
                ROV,
                FRP,
                LRP,
                TotalNodes
            FROM {view_name}
            WHERE 1=1
        """

        params = []

        if date:
            sql += " AND ProdDate = ?"
            params.append(date)

        if line:
            sql += " AND Line = ?"
            params.append(line)

        if rov:
            sql += f" AND ROV = ?"
            params.append(rov)

        sql += f" ORDER BY {order_by}"

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        return [dict(row) for row in rows]

    def export_dsr_to_sm(
            self,
            first_day: str,
            last_day: str | None = None,
            rovs: list[str] | None = None,
            export_type: int = 0,  # 0=DEPLOY, 1=RECOVERY
            export_abs: int = 0,  # 1 => abs(depth)
            zexp: int = 0,  # 1 => "SURVEY:1.4,..." lines
            output_dir: str | None = None,
            mark_exported: bool = True,
            table: str = "DSR",
            ts_from: str | None = None,  # "YYYY-MM-DD HH:MM:SS" (optional)
            ts_to: str | None = None,  # "YYYY-MM-DD HH:MM:SS" (optional)
    ):
        """
        Export from DSR table to SM format and SAVE to disk.

        If ts_from/ts_to are provided -> exports by exact timestamp interval:
            deploy: TimeStamp
            recovery: TimeStamp1
        Otherwise exports by Day/Day1 range.

        Returns:
          {"success": "<full_path>", "rows": N, "filename": "<name>"}
          or {"error": "<message>"}
        """

        # ---- choose columns by export_type ----
        def _safe_file_part(s: str) -> str:
            s = (s or "").strip()
            # replace Windows-illegal filename chars:  \ / : * ? " < > |
            for ch in ['\\', '/', ':', '*', '?', '"', '<', '>', '|']:
                s = s.replace(ch, "-")
            # also avoid trailing dots/spaces
            s = s.rstrip(" .")
            return s or "export"

        if int(export_type) == 0:
            FieldDate = "Day"
            FieldROV = "ROV"
            TimeStamp = "TimeStamp"
            PrimaryEasting = "PrimaryEasting"
            PrimaryNorthing = "PrimaryNorthing"
            PrimaryElevation = "PrimaryElevation"
            mode_txt = "DEPLOYED"
            op_tag = "deploy"
        else:
            FieldDate = "Day1"
            FieldROV = "ROV1"
            TimeStamp = "TimeStamp1"
            PrimaryEasting = "PrimaryEasting1"
            PrimaryNorthing = "PrimaryNorthing1"
            PrimaryElevation = "PrimaryElevation1"
            mode_txt = "RETRIEVED"
            op_tag = "recovery"

        # ---- output folder ----
        if output_dir is None:
            output_dir = (
                    getattr(getattr(self, "prj", None), "SM_Folder", None)
                    or getattr(getattr(self, "project", None), "SM_Folder", None)
                    or getattr(self, "sm_folder", None)
            )
        if not output_dir:
            return {"error": "SM output folder not set. Pass output_dir or set self.prj.SM_Folder / self.sm_folder."}

        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        # ---- build WHERE: timestamp interval OR day range ----
        day_where = ""
        day_params = []
        ts_where = ""
        ts_params = []
        label = ""

        if ts_from and ts_to:
            # exact timestamp interval (preferred when user selects time frame)
            ts_where = f" AND {TimeStamp} >= ? AND {TimeStamp} < ?"
            ts_params = [ts_from, ts_to]
            day_where = "1=1"
            label = f"{ts_from[:16].replace(' ', '_')}-{ts_to[:16].replace(' ', '_')}"
            label = _safe_file_part(label)

        else:
            # day/day-range
            def _parse_day(d: str) -> datetime.date:
                return _dt.datetime.strptime(d, "%Y-%m-%d").date()

            try:
                if last_day and str(last_day).strip() and str(last_day).strip() != "0":
                    d0 = _parse_day(first_day)
                    d1 = _parse_day(str(last_day).strip())
                    if d1 < d0:
                        d0, d1 = d1, d0
                    first_day_n = d0.strftime("%Y-%m-%d")
                    last_day_n = d1.strftime("%Y-%m-%d")
                    day_where = f"{FieldDate} BETWEEN ? AND ?"
                    day_params = [first_day_n, last_day_n]
                    label = f"{first_day_n}_{last_day_n}"
                else:
                    day_where = f"{FieldDate} = ?"
                    day_params = [first_day]
                    label = first_day
            except Exception as e:
                return {"error": f"Bad day format: {e}"}

        # ---- rovs filter ----
        rovs = rovs or []
        rovs = [r.strip() for r in rovs if r and r.strip()]
        rov_where = ""
        rov_params: list[str] = []
        rov_tag = "ALL"
        if rovs:
            placeholders = ",".join(["?"] * len(rovs))
            rov_where = f" AND TRIM({FieldROV}) IN ({placeholders})"
            rov_params = rovs
            rov_tag = "_".join([r.replace(" ", "_") for r in rovs])
            rov_tag = _safe_file_part(rov_tag)

        # ---- query ----
        sql = f"""
        SELECT
            ID,
            Node,
            TRIM(Line) AS Line,
            TRIM(Station) AS Station,
            CAST(NULLIF({PrimaryEasting}, '')  AS REAL) AS Easting,
            CAST(NULLIF({PrimaryNorthing}, '') AS REAL) AS Northing,
            CAST(NULLIF({PrimaryElevation}, '') AS REAL) AS Depth,
            {FieldDate} AS D,
            {TimeStamp} AS TS
        FROM {table}
        WHERE {day_where}
          AND {TimeStamp} IS NOT NULL AND TRIM({TimeStamp}) <> ''
          AND {FieldROV}  IS NOT NULL AND TRIM({FieldROV}) <> ''
          {ts_where}
          {rov_where}
        ORDER BY Line, Station, {TimeStamp}
        """

        params = [*day_params, *ts_params, *rov_params]

        try:
            with self._connect() as conn:
                rows = conn.execute(sql, params).fetchall()
                if not rows:
                    return {"error": "No data for selected filters (DSR query returned empty)."}

                if mark_exported:
                    conn.executemany(
                        f"UPDATE {table} SET isEXported = 1 WHERE ID = ?",
                        [(r["ID"],) for r in rows],
                    )
                    conn.commit()
        except Exception as e:
            return {"error": f"export_dsr_to_sm: sqlite error: {e}"}

        # ---- helpers ----
        def _mmddyyyy(day_value) -> str:
            s = ("" if day_value is None else str(day_value)).strip()
            if not s:
                return ""
            try:
                d = _dt.datetime.strptime(s[:10], "%Y-%m-%d").date()
                return d.strftime("%m%d%Y")
            except Exception:
                return s

        def _hhmmss(ts_value) -> str:
            s = ("" if ts_value is None else str(ts_value)).strip()
            if not s:
                return ""
            base = s.split(".")[0]
            try:
                dt0 = _dt.datetime.strptime(base, "%Y-%m-%d %H:%M:%S")
                return dt0.strftime("%H%M%S")
            except Exception:
                if " " in base:
                    t = base.split(" ", 1)[1]
                    return t.replace(":", "")[:6]
                return ""

        # ---- write file ----
        if int(zexp) == 1:
            filename = f"{label}_{rov_tag}_zexp_SM.csv"
            out_path = out_dir / filename

            with out_path.open("w", encoding="utf-8", newline="\n") as f:
                for r in rows:
                    node = (r["Node"] or "").strip()
                    node1 = node
                    serial = "290000001"
                    if node:
                        parts = node.split(" ")
                        if len(parts) >= 2:
                            node1 = parts[0]
                            serial = parts[1]

                    depth = float(r["Depth"] or 0.0)
                    if int(export_abs) == 1:
                        depth = abs(depth)

                    line = r["Line"] or ""
                    station = r["Station"] or ""
                    e = float(r["Easting"] or 0.0)
                    n = float(r["Northing"] or 0.0)
                    day_str = _mmddyyyy(r["D"])
                    hhmmss = _hhmmss(r["TS"])

                    f.write(
                        "SURVEY:1.4,HANDLED,"
                        f"PARTNO:{serial},"
                        f"SERIALNO:{node1},"
                        f"LINE:{line},"
                        f"STATION:{station},"
                        "CF:,"
                        f"MODE:{mode_txt},"
                        f"EASTING:{e:.1f},"
                        f"NORTHING:{n:.1f},"
                        f"DEPTH:{depth:.1f},"
                        f"DAY:{day_str},"
                        f"HHMMSS:{hhmmss},"
                        "survey\n"
                    )

            return {"success": str(out_path), "rows": len(rows), "filename": out_path.name}

        # normal CSV
        exp_name = ["QRCODE", "RFID", "LINE", "STATION", "CF", "MODE",
                    "EASTING", "NORTHING", "DEPTH", "DAY", "HHMMSS"]

        filename = f"{label}_{rov_tag}_{op_tag}_SM.csv"
        out_path = out_dir / filename

        with out_path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(exp_name)

            for r in rows:
                depth = float(r["Depth"] or 0.0)
                if int(export_abs) == 1:
                    depth = abs(depth)

                w.writerow([
                    (r["Node"] or ""),
                    "",
                    (r["Line"] or ""),
                    (r["Station"] or ""),
                    "",
                    mode_txt,
                    f"{float(r['Easting'] or 0.0):.1f}",
                    f"{float(r['Northing'] or 0.0):.1f}",
                    f"{depth:.1f}",
                    _mmddyyyy(r["D"]),
                    _hhmmss(r["TS"]),
                ])

        return {"success": str(out_path), "rows": len(rows), "filename": out_path.name}

    def _read_header_lines(self, header_file_path):
        if not header_file_path:
            return None

        header_file = Path(header_file_path)
        if not header_file.exists():
            return None

        try:
            with open(header_file, "rb") as fin:
                 buffer = fin.read(4096)
            enc = self.pdb._detect_text_encoding(buffer)
        except Exception as e:
            enc = "utf-8"

        try:
            with open(header_file, "r", encoding=enc) as fin:
                return fin.readlines()
        except Exception:
            return None

    def _fetch_dsr_for_lines(self, selected_lines):
        if not selected_lines:
            return pd.DataFrame()

        placeholders = ",".join(["?"] * len(selected_lines))

        sql = f"""
            SELECT
                TRIM(Line) AS Line,
                Station,
                TimeStamp,
                LinePoint,
                PrimaryEasting, PrimaryNorthing, PrimaryElevation,
                PrimaryEasting1, PrimaryNorthing1, PrimaryElevation1,
                ROV1
            FROM DSR
            WHERE TRIM(Line) IN ({placeholders})
        """

        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=tuple(selected_lines))

        return df

    from pathlib import Path
    import pandas as pd

    def export_dsr_lines_to_sps(
            self,
            export_dir,
            selected_lines,
            *,
            header_file_path=None,
            export_header=False,
            pcode="R1",
            sps_format=1,
            kind=0,
            use_seq=False,
            use_line_seq=False,
            seq=None,
            how_exp=2,
            line_code="",
            use_line_code=False,
    ):
        if not selected_lines:
            return {"ok": False, "message": "No lines selected.", "files": [], "errors": {}}

        export_dir = Path(export_dir)
        export_dir.mkdir(parents=True, exist_ok=True)

        pcode = (pcode or "R1").strip()[:4]
        seq = (seq or "").strip()
        if use_seq and not seq:
            seq = "01"

        header_lines = None
        if export_header:
            header_lines = self._read_header_lines(header_file_path)

        # ----------------------------
        # 1) Fetch DSR
        # ----------------------------
        df = self._fetch_dsr_for_lines(selected_lines)
        if df.empty:
            return {"ok": False, "message": "No DSR data found.", "files": [], "errors": {}}

        # Robust timestamp parsing
        df["TS"] = pd.to_datetime(df["TimeStamp"], errors="coerce")
        df = df.dropna(subset=["TS"])
        if df.empty:
            return {"ok": False, "message": "No valid timestamps.", "files": [], "errors": {}}

        # Normalize join keys in DSR (Line + LinePoint)
        df["Line"] = pd.to_numeric(df["Line"], errors="coerce")
        df["LinePoint"] = pd.to_numeric(df["LinePoint"], errors="coerce")
        df = df.dropna(subset=["Line", "LinePoint"])
        df["Line"] = df["Line"].astype("int64")
        df["LinePoint"] = df["LinePoint"].astype("int64")

        # Station is used in SPS output; keep numeric if present
        if "Station" in df.columns:
            df["Station"] = pd.to_numeric(df["Station"], errors="coerce")
            df = df.dropna(subset=["Station"])
            df["Station"] = df["Station"].astype("int64")
        else:
            # If Station isn't present, use LinePoint as Station
            df["Station"] = df["LinePoint"].astype("int64")

        # Sort base DSR
        df = df.sort_values(["Line", "Station", "TS"])

        # ----------------------------
        # 2) Load REC_DB and merge
        #    - Keep ALL REC_DB rows (even duplicates per Line+LinePoint)
        #    - Append DSR rows missing in REC_DB as KL
        #    - Also load Point from REC_DB
        # ----------------------------
        rec = self._fetch_rec_db_for_lines(selected_lines)  # must include Line, LinePoint, Point, REC_*, DEPLOY, RPI
        if rec is None:
            rec = pd.DataFrame()

        if not rec.empty:
            # Normalize join keys in REC_DB
            rec["Line"] = pd.to_numeric(rec["Line"], errors="coerce")
            rec["LinePoint"] = pd.to_numeric(rec["LinePoint"], errors="coerce")
            rec = rec.dropna(subset=["Line", "LinePoint"])
            rec["Line"] = rec["Line"].astype("int64")
            rec["LinePoint"] = rec["LinePoint"].astype("int64")

            # Keep needed columns (DO NOT drop duplicates!)
            keep_cols = [
                c for c in [
                    "Line",
                    "LinePoint",
                    "Point",
                    "REC_ID",
                    "REC_X",
                    "REC_Y",
                    "REC_Z",
                    "DEPLOY",
                    "RPI",
                    "PointIdx",
                ] if c in rec.columns
            ]
            rec = rec[keep_cols].copy()

            # 2.1) All REC_DB rows (many-to-one) enriched with DSR columns
            merged = rec.merge(
                df,
                how="left",
                on=["Line", "LinePoint"],
                suffixes=("", "_DSR"),
            )

            # 2.2) DSR rows with NO REC_DB match on (Line, LinePoint) -> append as KL
            rec_keys = rec[["Line", "LinePoint"]].drop_duplicates()

            dsr_only = df.merge(
                rec_keys,
                how="left",
                on=["Line", "LinePoint"],
                indicator=True,
            )
            dsr_only = dsr_only[dsr_only["_merge"] == "left_only"].drop(columns=["_merge"]).copy()

            # Fill missing REC_DB fields on dsr_only
            dsr_only["Point"] = pd.NA
            dsr_only["REC_ID"] = pd.NA
            dsr_only["REC_X"] = pd.NA
            dsr_only["REC_Y"] = pd.NA
            dsr_only["REC_Z"] = pd.NA
            dsr_only["RPI"] = 0

            # DEPLOY fallback for dsr_only (same logic as before)
            dsr_only["DEPLOY"] = 0

            # Combine: all REC_DB-based rows + DSR-only rows
            df = pd.concat([merged, dsr_only], ignore_index=True)

        else:
            # No REC_DB at all -> export everything as KL
            df["Point"] = pd.NA
            df["REC_ID"] = pd.NA
            df["REC_X"] = pd.NA
            df["REC_Y"] = pd.NA
            df["REC_Z"] = pd.NA
            df["RPI"] = 0
            df["DEPLOY"] = df.groupby(["Line", "Station"]).cumcount() + 1

        # If any rows came only from REC_DB with no DSR match, TS can be NaT -> drop (cannot build time fields)
        df["TS"] = pd.to_datetime(df.get("TS"), errors="coerce")
        df = df.dropna(subset=["TS"])
        if df.empty:
            return {"ok": False, "message": "After merge, no rows have valid timestamps (DSR match missing).",
                    "files": [], "errors": {}}

        # ----------------------------
        # 3) Build SPS fields
        # ----------------------------
        df["PointCode"] = pcode
        df[["Static", "Datum", "Elevation", "Uphole"]] = 0

        # DEPLOY: prefer REC_DB if present, fallback to cumcount
        df["DEPLOY"] = pd.to_numeric(df.get("DEPLOY"), errors="coerce")
        miss_dep = df["DEPLOY"].isna()
        if miss_dep.any():
            df.loc[miss_dep, "DEPLOY"] = df[miss_dep].groupby(["Line", "Station"]).cumcount() + 1
        df["DEPLOY"] = df["DEPLOY"].astype("int64")

        # RPI: prefer REC_DB, fallback 0
        df["RPI"] = pd.to_numeric(df.get("RPI"), errors="coerce").fillna(0).astype("int64")

        # Time fields
        df["JDay"] = df["TS"].dt.strftime("%j").astype(int)
        df["Hour"] = df["TS"].dt.hour.astype(int)
        df["Minute"] = df["TS"].dt.minute.astype(int)
        df["Second"] = df["TS"].dt.second.astype(int)

        # ----------------------------
        # 4) Choose coordinates (X,Y,Z)
        # ----------------------------
        if int(kind) == 0:
            df[["X", "Y", "Z"]] = df[["PrimaryEasting", "PrimaryNorthing", "PrimaryElevation"]]
            sub = "dep"

        elif int(kind) == 1:
            df[["X", "Y", "Z"]] = df[["PrimaryEasting1", "PrimaryNorthing1", "PrimaryElevation1"]]
            sub = "rec"
            if "ROV1" in df.columns:
                mask = df["ROV1"].isna()
                df.loc[mask, "PointCode"] = "KL"
                df.loc[mask, ["X", "Y", "Z"]] = df.loc[
                    mask, ["PrimaryEasting", "PrimaryNorthing", "PrimaryElevation"]
                ].values

        else:
            # FB: use REC_DB coords where available; fallback to primary coords
            df["X"] = pd.to_numeric(df.get("REC_X"), errors="coerce")
            df["Y"] = pd.to_numeric(df.get("REC_Y"), errors="coerce")
            df["Z"] = pd.to_numeric(df.get("REC_Z"), errors="coerce")
            df["DEPLOY"] = pd.to_numeric(df.get("PointIdx"), errors="coerce")
            sub = "fb"

            # Force KL when REC_ID missing OR coords missing; fallback to DSR primary coords
            no_rec = df.get("REC_ID").isna() if "REC_ID" in df.columns else df["X"].isna()
            df.loc[no_rec, "PointCode"] = "KL"

            fallback = df["X"].isna() | df["Y"].isna() | df["Z"].isna()
            df.loc[fallback, ["X", "Y", "Z"]] = df.loc[
                fallback, ["PrimaryEasting", "PrimaryNorthing", "PrimaryElevation"]
            ].values

        # Z formatting (same as your logic)
        df["Z"] = pd.to_numeric(df["Z"], errors="coerce").fillna(0).abs().round(1)

        def format_z(val):
            if val > 100:
                return str(int(val)).rjust(4)
            return f"{val:4.1f}"

        df["Zfmt"] = df["Z"].apply(format_z)

        def record_line_name(line):
            return f"{line}{seq}" if (use_line_seq and seq) else str(line)

        def file_line_name(line):
            return f"{line}{seq}" if (use_seq and seq) else str(line)

        def build_lines_v1(part, line_name_for_record):
            buf = []
            for r in part.sort_values(["Station", "DEPLOY"]).itertuples():
                buf.append(
                    "R{:<16}{:>8d}{:d}{:<1}{:>4d}{:>4}{:>4d}{:>2d}{:>4}{:>9.1f}{:>10.1f}{:>6.1f}{:03d}{:02d}{:02d}{:02d}\n".format(
                        str(line_name_for_record),
                        int(r.Station),
                        int(r.DEPLOY),
                        str(r.PointCode),
                        int(r.Static),
                        str(r.Zfmt),
                        int(r.Datum),
                        int(r.Uphole),
                        str(r.Zfmt),
                        float(r.X),
                        float(r.Y),
                        float(r.Elevation),
                        int(r.JDay),
                        int(r.Hour),
                        int(r.Minute),
                        int(r.Second),
                    )
                )
            return buf

        def write_file(path, blocks):
            with open(path, "w", encoding="utf-8") as out:
                if header_lines and export_header:
                    out.writelines(header_lines)
                for block in blocks:
                    out.writelines(block)

        out_dir = export_dir
        out_dir.mkdir(parents=True, exist_ok=True)

        created_files = []

        try:
            if int(how_exp) == 1:
                for line in selected_lines:
                    line_i = int(line)
                    part = df[df["Line"] == line_i].copy()
                    if part.empty:
                        continue

                    rec_name = record_line_name(line_i)
                    fname_line = file_line_name(line_i)
                    fname = f"{line_code}{fname_line}.R01" if use_line_code else f"{fname_line}.R01"
                    fpath = out_dir / fname

                    lines_block = build_lines_v1(part, rec_name)
                    write_file(fpath, [lines_block])
                    created_files.append(str(fpath))
            else:
                base = (
                    f"{selected_lines[0]}-{selected_lines[-1]}"if len(selected_lines) > 1else f"{selected_lines[0]}"
                )
                if use_seq and seq:
                    base = (
                        f"{selected_lines[0]}{seq}-{selected_lines[-1]}{seq}" if len(selected_lines) > 1 else f"{selected_lines[0]}{seq}")



                fname = f"{line_code}{base}.R01" if use_line_code else f"{base}.R01"
                fpath = out_dir / fname

                blocks = []
                for line in selected_lines:
                    line_i = int(line)
                    part = df[df["Line"] == line_i].copy()
                    if part.empty:
                        continue
                    rec_name = record_line_name(line_i)
                    blocks.append(build_lines_v1(part, rec_name))

                write_file(fpath, blocks)
                created_files.append(str(fpath))

        except Exception as e:
            return {"ok": False, "message": str(e), "files": [], "errors": {}}

        return {
            "ok": True,
            "message": f"Exported {len(created_files)} file(s).",
            "files": created_files,
            "errors": {},
        }

    def _fetch_rec_db_for_lines(self, selected_lines):
        """
        REC_DB must have (at least):
          Line, LinePoint, Point, REC_ID, REC_X, REC_Y, REC_Z, DEPLOY, RPI
        """
        if not selected_lines:
            return pd.DataFrame()

        placeholders = ",".join(["?"] * len(selected_lines))
        sql = f"""
            SELECT
                Line,
                LinePoint,
                Point,
                REC_ID,
                REC_X,
                REC_Y,
                REC_Z,
                DEPLOY,
                RPI,
                MAX(
                COALESCE(DEPLOY, 0),
                COALESCE(RPI, 0)
            ) AS PointIdx
            FROM REC_DB
            WHERE Line IN ({placeholders})
        """
        with self._connect() as conn:
            return pd.read_sql_query(sql, conn, params=tuple(selected_lines))


















