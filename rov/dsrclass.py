import csv
import hashlib
import io
import json
import os
import re
import math
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
        # DSR exports have used both ISO and day-first timestamps.  Normalize
        # all accepted forms before storing them so SQLite DATE()/julianday()
        # continue to work reliably.
        try:
            return _dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            pass

        for fmt in (
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%d-%m-%Y %H:%M:%S",
            "%d-%m-%Y %H:%M",
        ):
            try:
                return _dt.datetime.strptime(ts, fmt)
            except ValueError:
                continue
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

    @staticmethod
    def _coordinate_unit_factor(unit: str) -> float:
        """
        Convert from project base unit meters to selected export unit.
        """
        unit = (unit or "m").strip().lower()

        factors = {
            "m": 1.0,
            "meter": 1.0,
            "meters": 1.0,
            "ft": 3.280839895,
            "feet": 3.280839895,
            "usft": 3937.0 / 1200.0,
            "us_survey_ft": 3937.0 / 1200.0,
        }

        if unit not in factors:
            raise ValueError(f"Unsupported coordinate unit: {unit}")

        return factors[unit]
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
            "Line", "Station", "Node", "PreplotEasting", "PreplotNorthing", "ROV",
            "TimeStamp", "PrimaryEasting", "Sigma", "PrimaryNorthing", "Sigma1",
            "SecondaryEasting", "Sigma2", "SecondaryNorthing", "Sigma3",
            "DeltaEprimarytosecondary", "DeltaNprimarytosecondary", "Rangeprimarytosecondary",
            "RangetoPrePlot", "BrgtoPrePlot", "PrimaryElevation", "Sigma4",
            "SecondaryElevation", "Sigma5", "Quality", "ROV1", "TimeStamp1",
            "PrimaryEasting1", "Sigma6", "PrimaryNorthing1", "Sigma7",
            "SecondaryEasting1", "Sigma8", "SecondaryNorthing1", "Sigma9",
            "DeltaEprimarytosecondary1", "DeltaNprimarytosecondary1", "Rangeprimarytosecondary1",
            "RangetoPrePlot1", "BrgtoPrePlot1", "PrimaryElevation1", "Sigma10",
            "SecondaryElevation1", "Sigma11", "Quality1",
            "DeployedtoRetrievedEasting", "DeployedtoRetrievedNorthing",
            "DeployedtoRecoveredElevation", "DeployedtoRetrievedRange",
            "DeployedtoRetrievedBrg", "Comments",
        ]

        insert_cols = [
            "Solution_FK", "RLPreplot_FK", "LinePointIdx", "LinePoint", "RecIdx", "TIER",
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

        processed = 0
        upserted = 0
        skipped = 0
        changed_lines = set()
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
                    "Year": y,
                    "Month": m,
                    "Week": w,
                    "Day": d,
                    "JDay": j,
                    "Year1": y1,
                    "Month1": m1,
                    "Week1": w1,
                    "Day1": d1,
                    "JDay1": j1,
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
                        values[col] = dt.isoformat(sep=" ") if dt else self._to_text(ts_raw)
                    elif col == "TimeStamp1":
                        values[col] = dt1.isoformat(sep=" ") if dt1 else self._to_text(raw)
                    elif col in {"Quality", "ROV1", "Quality1", "Comments"}:
                        values[col] = self._to_text(raw)
                    else:
                        values[col] = self._to_float(raw)

                batch.append(tuple(values.get(c) for c in insert_cols))
                processed += 1
                changed_lines.add(line)

                if len(batch) >= chunk_size:
                    conn.executemany(sql_upsert, batch)
                    upserted += len(batch)
                    batch.clear()

            if batch:
                conn.executemany(sql_upsert, batch)
                upserted += len(batch)

            conn.commit()

        return processed, upserted, skipped, sorted(changed_lines)

    def render_dsr_line_summary_body(self, request=None):
        """
        Returns rendered HTML for DSR line summary table using dsr_line_body.html template.
        """
        with self._connect() as conn:
            self.ensure_dsr_line_summary_ready(conn=conn, rebuild_if_empty=True)

            rows = conn.execute(
                "SELECT * FROM DSR_LineSummary ORDER BY Line"
            ).fetchall()

        max_sma = self.pdb.get_node_qc().max_sma
        warning_sma = self.pdb.get_node_qc().warning_sma
        max_radial_offset = self.pdb.get_node_qc().max_radial_offset
        radial80 = max_radial_offset * 0.8

        context = {
            "lines": rows,
            "max_sma": max_sma,
            "warning_sma": warning_sma,
            "max_radial_offset": max_radial_offset,
            "radial80": radial80,
        }

        return render_to_string(
            "rov/partials/dsr_line_body.html",
            context=context,
            request=request,
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
    def ensure_recover_daily_view_schema(self):
        sql="""
            DROP VIEW IF EXISTS Daily_Deployment;
            CREATE VIEW Daily_Deployment AS
            SELECT
                COALESCE(
                    DATE(NULLIF(TRIM(Day), '')),
                    DATE(TimeStamp),
                    CASE
                        WHEN TRIM(TimeStamp) GLOB '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]*'
                        THEN DATE(SUBSTR(TRIM(TimeStamp), 7, 4) || '-' ||
                                  SUBSTR(TRIM(TimeStamp), 4, 2) || '-' ||
                                  SUBSTR(TRIM(TimeStamp), 1, 2))
                    END
                ) AS ProdDate,
                TRIM(Line) AS Line,
                TRIM(ROV) AS ROV,
                MIN(CAST(NULLIF(Station,'') AS REAL)) AS FRP,
                MAX(CAST(NULLIF(Station,'') AS REAL)) AS LRP,
                COUNT(*) AS TotalNodes
            FROM DSR
            WHERE TimeStamp IS NOT NULL AND TRIM(TimeStamp) <> ''
              AND ROV IS NOT NULL AND TRIM(ROV) <> ''
            GROUP BY ProdDate, TRIM(Line), TRIM(ROV);

            DROP VIEW IF EXISTS Daily_Recovery;
            CREATE VIEW Daily_Recovery AS
            SELECT
                COALESCE(
                    DATE(NULLIF(TRIM(Day1), '')),
                    DATE(TimeStamp1),
                    CASE
                        WHEN TRIM(TimeStamp1) GLOB '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]*'
                        THEN DATE(SUBSTR(TRIM(TimeStamp1), 7, 4) || '-' ||
                                  SUBSTR(TRIM(TimeStamp1), 4, 2) || '-' ||
                                  SUBSTR(TRIM(TimeStamp1), 1, 2))
                    END
                ) AS ProdDate,
                TRIM(Line) AS Line,
                TRIM(ROV1) AS ROV,
                MIN(CAST(NULLIF(Station,'') AS REAL)) AS FRP,
                MAX(CAST(NULLIF(Station,'') AS REAL)) AS LRP,
                COUNT(*) AS TotalNodes
            FROM DSR
            WHERE TimeStamp1 IS NOT NULL AND TRIM(TimeStamp1) <> ''
              AND ROV1 IS NOT NULL AND TRIM(ROV1) <> ''
            GROUP BY ProdDate, TRIM(Line), TRIM(ROV1);
        """
        with self._connect() as conn:
            conn.executescript(sql)
            conn.commit()

    def ensure_bbox_config_schema(self):
        """
        Ensure all BBox config tables, constraints, triggers and required columns exist.
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
            Vessel_name TEXT,
            Depth1_name TEXT,
            Depth2_name TEXT,
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

            # ---- add missing columns for old projects ----
            cols = {row["name"] for row in conn.execute("PRAGMA table_info(BBox_Configs_List)").fetchall()}

            if "Vessel_name" not in cols:
                conn.execute('ALTER TABLE BBox_Configs_List ADD COLUMN Vessel_name TEXT')
            if "Depth1_name" not in cols:
                conn.execute('ALTER TABLE BBox_Configs_List ADD COLUMN Depth1_name TEXT')
            if "Depth2_name" not in cols:
                conn.execute('ALTER TABLE BBox_Configs_List ADD COLUMN Depth2_name TEXT')

            conn.commit()

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

    def get_bbox_file_rows(self, vessel="", start_day="", end_day=""):
        conn = self._connect()
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            where = []
            params = []

            if vessel:
                where.append("COALESCE(cfg.Vessel_name, '') = ?")
                params.append(vessel)

            if start_day:
                where.append("date(COALESCE(fs.StartTime, bf.UploadedAt)) >= date(?)")
                params.append(start_day)

            if end_day:
                where.append("date(COALESCE(fs.EndTime, bf.UploadedAt)) <= date(?)")
                params.append(end_day)

            where_sql = f"WHERE {' AND '.join(where)}" if where else ""

            sql = f"""
            SELECT
                bf.ID,
                bf.FileName,
                bf.UploadedAt,
                bf.Config_FK,

                cfg.Name AS ConfigName,
                cfg.Vessel_name AS VesselName,
                cfg.rov1_name AS ROV1_Name,
                cfg.rov2_name AS ROV2_Name,
                cfg.gnss1_name AS GNSS1_Name,
                cfg.gnss2_name AS GNSS2_Name,

                fs.StartTime,
                fs.EndTime,
                fs.RowCount,
                fs.DurationSec,
                fs.MaxTimeGapSec,

                fs.ROV1_SOG_Min,
                fs.ROV1_SOG_Max,
                fs.ROV2_SOG_Min,
                fs.ROV2_SOG_Max,

                fs.ROV1_Depth_Min,
                fs.ROV1_Depth_Max,
                fs.ROV2_Depth_Min,
                fs.ROV2_Depth_Max,

                fs.ROV1_Depth1_Min,
                fs.ROV1_Depth1_Max,
                fs.ROV1_Depth2_Min,
                fs.ROV1_Depth2_Max,

                fs.ROV2_Depth1_Min,
                fs.ROV2_Depth1_Max,
                fs.ROV2_Depth2_Min,
                fs.ROV2_Depth2_Max,

                fs.GNSS1_HDOP_Min,
                fs.GNSS1_HDOP_Max,
                fs.GNSS2_HDOP_Min,
                fs.GNSS2_HDOP_Max,

                fs.GNSS1_PDOP_Min,
                fs.GNSS1_PDOP_Max,
                fs.GNSS2_PDOP_Min,
                fs.GNSS2_PDOP_Max,

                fs.GNSS1_VDOP_Min,
                fs.GNSS1_VDOP_Max,
                fs.GNSS2_VDOP_Min,
                fs.GNSS2_VDOP_Max,

                fs.GNSS1_NOS_Min,
                fs.GNSS1_NOS_Max,
                fs.GNSS1_DiffAge_Min,
                fs.GNSS1_DiffAge_Max,
                fs.GNSS1_FixQuality_Min,
                fs.GNSS1_FixQuality_Max,

                fs.GNSS2_NOS_Min,
                fs.GNSS2_NOS_Max,
                fs.GNSS2_DiffAge_Min,
                fs.GNSS2_DiffAge_Max,
                fs.GNSS2_FixQuality_Min,
                fs.GNSS2_FixQuality_Max,

                fs.Barometer_Min,
                fs.Barometer_Max,

                fs.ROV1_PosDiff_Min,
                fs.ROV1_PosDiff_Max,
                fs.ROV1_PosDiff_Avg,

                fs.ROV2_PosDiff_Min,
                fs.ROV2_PosDiff_Max,
                fs.ROV2_PosDiff_Avg

            FROM BlackBox_Files bf
            LEFT JOIN BlackBox_FileStats fs
                ON fs.File_FK = bf.ID
            LEFT JOIN BBox_Configs_List cfg
                ON cfg.ID = bf.Config_FK
            {where_sql}
            ORDER BY bf.ID DESC
            """

            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]

            for item in rows:
                duration = item.get("DurationSec")
                if duration is None:
                    item["DurationText"] = ""
                else:
                    try:
                        duration = int(duration)
                        hh = duration // 3600
                        mm = (duration % 3600) // 60
                        ss = duration % 60
                        item["DurationText"] = f"{hh:02d}:{mm:02d}:{ss:02d}"
                    except Exception:
                        item["DurationText"] = str(duration)

            return rows
        finally:
            conn.close()

    def get_bbox_file_table(self, vessel="", start_day="", end_day=""):
        rows = self.get_bbox_file_rows(
            vessel=vessel,
            start_day=start_day,
            end_day=end_day,
        )
        return render_to_string(
            "rov/partials/bbox_list_body.html",
            {"rows": rows}
        )


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

    def load_sm_file_to_db(self, fname) -> dict:
        """
        Import SM_Export.csv and UPDATE existing DSR rows only.

        Match rule:
            DSR.Line    = SM Line
            DSR.Station = SM Station
            DSR.Node    = normalized Remote Unit OR AU QR Code

        Remote Unit example:
            SM Remote Unit = "297080001/13471"
            DSR.Node       = "13471 297080001"
        """


        UPDATE_MAP = {
            "Area": "Area",
            "RemoteUnit": "RemoteUnit",
            "AUQRCode": "AUQRCode",
            "AURFID": "AURFID",
            "CUSerialNumber": "CUSerialNumber",
            "Status": "Status",
            "DeploymentType": "DeploymentType",
            "StartTimeEpoch": "StartTimeEpoch",
            "StartTimeUTC": "StartTimeUTC",
            "DeployTimeEpoch": "DeployTimeEpoch",
            "DeployTimeUTC": "DeployTimeUTC",
            "PickupTimeEpoch": "PickupTimeEpoch",
            "PickupTimeUTC": "PickupTimeUTC",
            "StopTimeEpoch": "StopTimeEpoch",
            "StopTimeUTC": "StopTimeUTC",
            "SPSX": "SPSX",
            "SPSY": "SPSY",
            "SPSZ": "SPSZ",
            "ActualX": "ActualX",
            "ActualY": "ActualY",
            "ActualZ": "ActualZ",
            "Deployed": "Deployed",
            "PickedUp": "PickedUp",
            "Archived": "Archived",
            "DeviceID": "DeviceID",
            "BinID": "BinID",
            "ExpectedTraces": "ExpectedTraces",
            "CollectedTraces": "CollectedTraces",
            "DownloadedDatainMB": "DownloadedDatainMB",
            "ExpectedDatainMB": "ExpectedDatainMB",
            "DownloadError": "DownloadError",
        }

        def clean_col(c):
            return re.sub(r"\W+", "", str(c)).strip()

        def norm_text(v):
            if v is None:
                return None
            s = str(v).strip()
            if s == "" or s.lower() in {"nan", "none", "null"}:
                return None
            return s

        def norm_remote_unit(v):
            s = norm_text(v)
            if not s or s == "-1":
                return None

            s = re.sub(r"\s+", " ", s)

            if "/" in s:
                parts = [p.strip() for p in s.split("/") if p.strip()]
                if len(parts) == 2:
                    return f"{parts[1]} {parts[0]}"

            return s

        def to_number_or_none(v):
            s = norm_text(v)
            if s is None or s == "-1":
                return None
            try:
                return float(s)
            except Exception:
                return None

        def to_int_or_none(v):
            n = to_number_or_none(v)
            if n is None:
                return None
            return int(n)

        # ------------------------------------------------------------------
        # 1. Read CSV
        # ------------------------------------------------------------------
        if isinstance(fname, (str, Path)):
            p = Path(fname)
            if not p.exists():
                return {"error": f"File not found: {p}"}

            encoding = self._detect_encoding(p)
            sep = self._get_separator_from_file(p, encoding=encoding)
            engine = "python" if sep == r"\s+" else "c"

            try:
                df = pd.read_csv(p, sep=sep, encoding=encoding, engine=engine)
            except Exception as e:
                return {"error": f"SM read_csv error: {e}"}
        else:
            text = self._read_uploaded_as_text(fname)
            if not text.strip():
                return {"error": "SM file is empty"}

            sep = self._guess_sep_from_text(text[:4096])
            engine = "python" if sep == r"\s+" else "c"

            try:
                df = pd.read_csv(io.StringIO(text), sep=sep, engine=engine)
            except Exception as e:
                return {"error": f"SM read_csv upload error: {e}"}

        if df.empty:
            return {"error": "SM file has no rows"}

        rows_in_file_raw = len(df)
        original_cols = list(df.columns)

        # ------------------------------------------------------------------
        # 2. Normalize columns
        # ------------------------------------------------------------------
        df.columns = [clean_col(c) for c in df.columns]

        required = ["Line", "Station", "RemoteUnit", "AUQRCode"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            return {
                "error": f"Missing required SM columns: {missing}",
                "columns": original_cols,
            }

        df["Line"] = pd.to_numeric(df["Line"], errors="coerce")
        df["Station"] = pd.to_numeric(df["Station"], errors="coerce")
        df = df[df["Line"].notna() & df["Station"].notna()].copy()

        if df.empty:
            return {"error": "No valid Line/Station rows after parsing"}

        df["Line"] = df["Line"].astype(int)
        df["Station"] = df["Station"].astype(int)

        df["RemoteUnitMatch"] = df["RemoteUnit"].apply(norm_remote_unit)
        df["AUQRCodeMatch"] = df["AUQRCode"].apply(norm_text)

        df = df[
            df["RemoteUnitMatch"].notna() | df["AUQRCodeMatch"].notna()
            ].copy()

        if df.empty:
            return {"error": "No valid Remote Unit or AU QR Code values"}

        # Store normalized RemoteUnit in DSR
        df["RemoteUnit"] = df["RemoteUnitMatch"]

        # ------------------------------------------------------------------
        # 3. Convert numeric fields
        # ------------------------------------------------------------------
        int_cols = [
            "StartTimeEpoch",
            "DeployTimeEpoch",
            "PickupTimeEpoch",
            "StopTimeEpoch",
            "DeviceID",
            "BinID",
            "ExpectedTraces",
            "CollectedTraces",
            "DownloadedDatainMB",
            "ExpectedDatainMB",
            "DownloadError",
        ]

        real_cols = [
            "SPSX",
            "SPSY",
            "SPSZ",
            "ActualX",
            "ActualY",
            "ActualZ",
        ]

        for c in int_cols:
            if c in df.columns:
                df[c] = df[c].apply(to_int_or_none)

        for c in real_cols:
            if c in df.columns:
                df[c] = df[c].apply(to_number_or_none)

        # Keep only latest duplicated key from SM
        df = df.drop_duplicates(
            subset=["Line", "Station", "RemoteUnitMatch", "AUQRCodeMatch"],
            keep="last",
        ).copy()

        # ------------------------------------------------------------------
        # 4. Update DSR only where matched
        # ------------------------------------------------------------------
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            dsr_cols = {
                r["name"].lower(): r["name"]
                for r in cur.execute("PRAGMA table_info(DSR)").fetchall()
            }

            set_cols = [
                db_col
                for sm_col, db_col in UPDATE_MAP.items()
                if sm_col in df.columns and db_col.lower() in dsr_cols
            ]

            if not set_cols:
                return {"error": "No SM columns available for DSR update"}

            temp_cols = [
                            "Line",
                            "Station",
                            "RemoteUnitMatch",
                            "AUQRCodeMatch",
                        ] + set_cols

            cur.execute("DROP TABLE IF EXISTS temp_sm_update")

            col_defs = [
                "Line INTEGER",
                "Station INTEGER",
                "RemoteUnitMatch TEXT",
                "AUQRCodeMatch TEXT",
            ]

            for c in set_cols:
                if c in int_cols:
                    col_defs.append(f"{c} INTEGER")
                elif c in real_cols:
                    col_defs.append(f"{c} REAL")
                else:
                    col_defs.append(f"{c} TEXT")

            cur.execute(f"CREATE TEMP TABLE temp_sm_update ({', '.join(col_defs)})")

            insert_sql = f"""
                INSERT INTO temp_sm_update ({', '.join(temp_cols)})
                VALUES ({', '.join(['?'] * len(temp_cols))})
            """

            rows = []
            for _, r in df.iterrows():
                rows.append(tuple(r.get(c) for c in temp_cols))

            try:
                cur.execute("BEGIN IMMEDIATE")
                cur.executemany(insert_sql, rows)

                match_where = """
                    DSR.Line = s.Line
                    AND DSR.Station = s.Station
                    AND (
                        TRIM(COALESCE(DSR.Node, '')) = TRIM(COALESCE(s.RemoteUnitMatch, ''))
                        OR TRIM(COALESCE(DSR.Node, '')) = TRIM(COALESCE(s.AUQRCodeMatch, ''))
                    )
                """

                existing_in_dsr = cur.execute(f"""
                    SELECT COUNT(*)
                    FROM DSR
                    WHERE EXISTS (
                        SELECT 1
                        FROM temp_sm_update s
                        WHERE {match_where}
                    )
                """).fetchone()[0]

                set_clause = ", ".join([
                    f"""{c} = (
                        SELECT s.{c}
                        FROM temp_sm_update s
                        WHERE {match_where}
                        LIMIT 1
                    )"""
                    for c in set_cols
                ])

                update_sql = f"""
                    UPDATE DSR
                    SET {set_clause}
                    WHERE EXISTS (
                        SELECT 1
                        FROM temp_sm_update s
                        WHERE {match_where}
                    )
                """

                cur.execute(update_sql)
                updated_rows = cur.execute("SELECT changes()").fetchone()[0]

                conn.commit()

            except Exception as e:
                conn.rollback()
                return {"error": f"DSR SM update failed: {e}"}

            finally:
                cur.execute("DROP TABLE IF EXISTS temp_sm_update")

        return {
            "success": True,
            "rows_in_file_raw": rows_in_file_raw,
            "unique_sm_rows": len(df),
            "existing_in_dsr": int(existing_in_dsr),
            "updated_rows": int(updated_rows),
            "skipped_missing": int(len(df) - existing_in_dsr),
            "match_rule": "Line + Station + DSR.Node equals normalized RemoteUnit or AUQRCode",
            "updated_columns": set_cols,
        }
    def load_sm_file_to_db2(
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
        FB / REC_DB whitespace-delimited loader.

        Fixes:
          - Correct Line / Point parsing from REC_ID using rl_mask counts.
            Example with rl_mask='LLLLLPPPP':
                REC_ID = 153351476
                Line   = 15335
                Point  = 1476
          - Returns changed_lines for refreshing DSR_LineSummary after upload.
          - UPSERT into REC_DB by UNIQUE(REC_ID, DEPLOY, RPI).
        """

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

        def _fetch_preplot_id_by_line(conn, line_values) -> dict:
            BATCH = 900
            out = {}

            lines = [int(l) for l in set(line_values) if l is not None]
            if not lines:
                return out

            for i in range(0, len(lines), BATCH):
                batch = lines[i:i + BATCH]
                ph = ",".join(["?"] * len(batch))

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

        # --------------------------------------------------
        # scalers / mask
        # --------------------------------------------------
        geom = self.pdb.get_geometry()
        mask = getattr(geom, "rl_mask", "") or ""

        if not mask or "L" not in mask or "P" not in mask:
            return {"error": "rl_mask missing or invalid"}

        num_line_digits = mask.count("L")
        num_point_digits = mask.count("P")
        expected_len = num_line_digits + num_point_digits

        line_s = self.linescaler
        lp_s = self.linepointscaler
        lpi_s = self.linepointidxscaler()

        if not line_s or not lp_s or not lpi_s:
            return {
                "error": f"Invalid rl_mask scalers. rl_mask={mask}, "
                         f"line_s={line_s}, lp_s={lp_s}, lpi_s={lpi_s}"
            }

        scalar_point = 10 ** num_point_digits

        fin_rename = {
            "FINPITCH": "PITCHFIN",
            "FINROLL": "ROLLFIN",
            "FINYAW": "YAWFIN",
        }

        # --------------------------------------------------
        # read REC_DB schema
        # --------------------------------------------------
        with self._connect() as conn:
            rec_info = conn.execute("PRAGMA table_info(REC_DB)").fetchall()
            rec_cols = {r["name"].lower(): r["name"] for r in rec_info}

        conflict_cols = ["REC_ID", "DEPLOY", "RPI"]

        for req in ("rec_id", "deploy", "rpi"):
            if req not in rec_cols:
                return {"error": f'REC_DB table missing required column "{req.upper()}"'}

        db_conflict = [rec_cols[c.lower()] for c in conflict_cols]

        # --------------------------------------------------
        # build pandas reader
        # --------------------------------------------------
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
                engine="python",
            )
            src_name = getattr(file_obj_or_path, "name", "uploaded_file")

        # --------------------------------------------------
        # process chunks
        # --------------------------------------------------
        total_rows = 0
        total_upserts = 0
        total_preplot_linked = 0
        changed_lines = set()

        with self._connect() as conn:
            conn.execute("PRAGMA busy_timeout = 120000")
            conn.execute("BEGIN IMMEDIATE")

            try:
                for df in reader:
                    if df is None or df.empty:
                        continue

                    total_rows += int(len(df))

                    # normalize headers
                    df.columns = [re.sub(r"\W", "", str(c)).strip() for c in df.columns]

                    # rename old FIN columns
                    for old, new in fin_rename.items():
                        if old in df.columns:
                            df.rename(columns={old: new}, inplace=True)

                    if "REC_ID" not in df.columns or "RPI" not in df.columns or "DEPLOY" not in df.columns:
                        raise ValueError(
                            f"REC_DB file must contain REC_ID, RPI and DEPLOY. "
                            f"Columns found: {list(df.columns)}"
                        )

                    # numeric safety
                    df["RPI"] = pd.to_numeric(df["RPI"], errors="coerce").fillna(0).astype("int64")
                    df["DEPLOY"] = pd.to_numeric(df["DEPLOY"], errors="coerce").fillna(0).astype("int64")

                    # --------------------------------------------------
                    # FIXED REC_ID parsing
                    # --------------------------------------------------
                    rec_str = (
                        df["REC_ID"]
                        .astype(str)
                        .str.replace(r"\.0$", "", regex=True)
                        .str.replace(r"\D", "", regex=True)
                        .str.strip()
                    )

                    rec_core = rec_str.str.slice(0, expected_len)

                    line_str = rec_core.str.slice(0, num_line_digits)
                    point_str = rec_core.str.slice(num_line_digits, expected_len)

                    line_val = pd.to_numeric(line_str, errors="coerce")
                    point_val = pd.to_numeric(point_str, errors="coerce")

                    valid_mask = line_val.notna() & point_val.notna()
                    df = df.loc[valid_mask].copy()

                    if df.empty:
                        continue

                    line_val = line_val.loc[valid_mask].astype("int64")
                    point_val = point_val.loc[valid_mask].astype("int64")

                    df["Line"] = line_val.values
                    df["Point"] = point_val.values

                    df["LinePoint"] = (
                            df["Line"].astype("int64") * scalar_point
                            + df["Point"].astype("int64")
                    ).astype("int64")

                    suffix_val = df[["RPI", "DEPLOY"]].max(axis=1).astype("int64")
                    df["LinePointIdx"] = (df["LinePoint"] * 10 + suffix_val).astype("int64")

                    changed_lines.update(
                        int(x) for x in df["Line"].dropna().unique().tolist()
                    )

                    # --------------------------------------------------
                    # Tier calculations
                    # --------------------------------------------------
                    if "TIER" not in df.columns:
                        df["TIER"] = 1

                    tier_val = pd.to_numeric(df["TIER"], errors="coerce").fillna(1).astype("int64")

                    df["TierLine"] = (tier_val * line_s + df["Line"]).astype("int64")
                    df["TierLinePoint"] = (tier_val * lp_s + df["LinePoint"]).astype("int64")
                    df["TierLinePointIdx"] = (tier_val * lpi_s + df["LinePointIdx"]).astype("int64")

                    # File_FK
                    if file_fk is not None and "file_fk" in rec_cols:
                        df["File_FK"] = int(file_fk)

                    # Preplot_FK
                    if "preplot_fk" in rec_cols:
                        lines = df["Line"].dropna().astype("int64").unique().tolist()
                        preplot_map = _fetch_preplot_id_by_line(conn, lines)
                        df["Preplot_FK"] = df["Line"].map(preplot_map)
                        total_preplot_linked += int(pd.notnull(df["Preplot_FK"]).sum())

                    # keep only REC_DB columns, exclude ID
                    keep_cols = [
                        c for c in df.columns
                        if c.lower() in rec_cols and c.lower() != "id"
                    ]

                    for cc in conflict_cols:
                        if cc in df.columns and cc not in keep_cols:
                            keep_cols.append(cc)

                    rename_to_db = {c: rec_cols[c.lower()] for c in keep_cols}
                    df.rename(columns=rename_to_db, inplace=True)

                    db_cols = [rename_to_db[c] for c in keep_cols]

                    update_cols = [c for c in db_cols if c not in db_conflict]

                    col_sql = ", ".join(f'"{c}"' for c in db_cols)
                    val_sql = ", ".join("?" for _ in db_cols)
                    conflict_sql = ", ".join(f'"{c}"' for c in db_conflict)

                    if update_cols:
                        update_sql = ", ".join(
                            f'"{c}"=excluded."{c}"'
                            for c in update_cols
                        )

                        sql = (
                            f'INSERT INTO REC_DB ({col_sql}) VALUES ({val_sql}) '
                            f'ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_sql}'
                        )
                    else:
                        sql = (
                            f'INSERT INTO REC_DB ({col_sql}) VALUES ({val_sql}) '
                            f'ON CONFLICT ({conflict_sql}) DO NOTHING'
                        )

                    sub = df[db_cols].where(pd.notnull(df[db_cols]), None)
                    values = list(sub.itertuples(index=False, name=None))

                    if values:
                        conn.executemany(sql, values)
                        total_upserts += len(values)

                conn.commit()

            except Exception as e:
                conn.rollback()
                return {
                    "error": f"load_fb_from_file error: {e}",
                    "file": src_name,
                    "rl_mask": mask,
                    "num_line_digits": num_line_digits,
                    "num_point_digits": num_point_digits,
                }

        return {
            "success": f"File {src_name} processed",
            "rows_read": int(total_rows),
            "upserts_attempted": int(total_upserts),
            "preplot_fk_linked": int(total_preplot_linked),
            "changed_lines": sorted(changed_lines),
            "rl_mask": mask,
            "num_line_digits": int(num_line_digits),
            "num_point_digits": int(num_point_digits),
            "preplot_fk_rule": (
                "Preplot_FK = RLPreplot.ID where RLPreplot.Line = REC_DB.Line "
                "(first ID picked if duplicates)"
            ),
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
            mode="day",
            status="deployed",
            day=None,
            dt_from=None,
            dt_to=None,
            line_from=None,
            line_to=None,
            station_from=None,
            station_to=None,
    ):
        import datetime as _dt
        import sqlite3

        status = (status or "deployed").strip().lower()

        if status == "recovered":
            rov_col = "ROV1"
            ts_col = "TimeStamp1"
        else:
            rov_col = "ROV"
            ts_col = "TimeStamp"

        ts_value = f"TRIM(COALESCE({ts_col}, ''))"

        # Positions used for M/D/YYYY and MM/DD/YYYY timestamps.
        first_slash = f"INSTR({ts_value}, '/')"
        after_first_slash = (
            f"SUBSTR({ts_value}, ({first_slash}) + 1)"
        )
        second_slash = (
            f"INSTR({after_first_slash}, '/')"
        )
        space_position = f"INSTR({ts_value}, ' ')"

        # Normalize stored timestamps into YYYY-MM-DD HH:MM:SS.
        timestamp_sql = f"""
            CASE
                WHEN {ts_value} = ''
                    THEN NULL

                -- ISO:
                -- YYYY-MM-DD HH:MM:SS
                -- YYYY-MM-DDTHH:MM:SS
                WHEN {ts_value} GLOB
                     '[0-9][0-9][0-9][0-9]-*'
                    THEN DATETIME(
                        REPLACE({ts_value}, 'T', ' ')
                    )

                -- US format with one- or two-digit month/day:
                -- M/D/YYYY
                -- MM/DD/YYYY
                -- M/D/YYYY HH:MM:SS.s
                WHEN ({first_slash}) > 0
                     AND ({second_slash}) > 0
                    THEN DATETIME(
                        PRINTF(
                            '%04d-%02d-%02d',
                            CAST(
                                SUBSTR(
                                    {after_first_slash},
                                    ({second_slash}) + 1,
                                    4
                                ) AS INTEGER
                            ),
                            CAST(
                                SUBSTR(
                                    {ts_value},
                                    1,
                                    ({first_slash}) - 1
                                ) AS INTEGER
                            ),
                            CAST(
                                SUBSTR(
                                    {after_first_slash},
                                    1,
                                    ({second_slash}) - 1
                                ) AS INTEGER
                            )
                        ) ||
                        CASE
                            WHEN ({space_position}) > 0
                                THEN ' ' || SUBSTR(
                                    {ts_value},
                                    ({space_position}) + 1
                                )
                            ELSE ' 00:00:00'
                        END
                    )

                ELSE NULL
            END
        """

        where = [
            f"TRIM(COALESCE({rov_col}, '')) <> ''",
            f"({timestamp_sql}) IS NOT NULL",
        ]
        params = []

        def _normalize_datetime(value, end_of_day=False):
            if value is None:
                return None

            # Also support Python date/datetime objects.
            if isinstance(value, _dt.datetime):
                parsed = value
                contains_time = True

            elif isinstance(value, _dt.date):
                parsed = _dt.datetime.combine(
                    value,
                    _dt.time.min,
                )
                contains_time = False

            else:
                value = str(value).strip()

                if not value:
                    return None

                value = value.replace("T", " ")
                contains_time = ":" in value

                formats = (
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M",
                    "%Y-%m-%d",
                    "%m/%d/%Y %H:%M:%S.%f",
                    "%m/%d/%Y %H:%M:%S",
                    "%m/%d/%Y %H:%M",
                    "%m/%d/%Y",
                )

                parsed = None

                for date_format in formats:
                    try:
                        parsed = _dt.datetime.strptime(
                            value,
                            date_format,
                        )
                        break
                    except ValueError:
                        continue

                if parsed is None:
                    raise ValueError(
                        f"Unsupported date/time format: {value!r}"
                    )

            if end_of_day and not contains_time:
                parsed = parsed.replace(
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=0,
                )

            return parsed.strftime("%Y-%m-%d %H:%M:%S")

        mode = (mode or "day").strip().lower()

        if mode == "day" and day:
            normalized_day = _normalize_datetime(day)

            where.append(
                f"DATE({timestamp_sql}) = DATE(?)"
            )
            params.append(normalized_day)

        elif mode == "interval":
            normalized_from = _normalize_datetime(dt_from)
            normalized_to = _normalize_datetime(
                dt_to,
                end_of_day=True,
            )

            if normalized_from:
                where.append(
                    f"{timestamp_sql} >= DATETIME(?)"
                )
                params.append(normalized_from)

            if normalized_to:
                where.append(
                    f"{timestamp_sql} <= DATETIME(?)"
                )
                params.append(normalized_to)

        def _clean_int(value):
            if value is None:
                return None

            value = str(value).strip()

            if not value:
                return None

            try:
                return int(float(value))
            except (TypeError, ValueError):
                return None

        lf = _clean_int(line_from)
        lt = _clean_int(line_to)
        sf = _clean_int(station_from)
        st = _clean_int(station_to)

        if lf is not None and lt is None:
            lt = lf
        elif lt is not None and lf is None:
            lf = lt

        if sf is not None and st is None:
            st = sf
        elif st is not None and sf is None:
            sf = st

        if lf is not None and lt is not None:
            if lf > lt:
                lf, lt = lt, lf

            where.append(
                "CAST(Line AS INTEGER) BETWEEN ? AND ?"
            )
            params.extend([lf, lt])

        if sf is not None and st is not None:
            if sf > st:
                sf, st = st, sf

            where.append(
                "CAST(Station AS INTEGER) BETWEEN ? AND ?"
            )
            params.extend([sf, st])

        sql = f"""
            SELECT DISTINCT
                TRIM({rov_col}) AS rov
            FROM DSR
            WHERE {" AND ".join(where)}
            ORDER BY rov COLLATE NOCASE
        """

        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()

        return [
            row["rov"]
            for row in rows
            if row["rov"]
        ]
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
    def get_daily_sm(
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
            first_day=None,
            last_day=None,
            rovs=None,
            export_type=0,
            export_abs=0,
            zexp=0,
            output_dir=None,
            mark_exported=False,
            ts_from=None,
            ts_to=None,
            always_primary_deployment=True,
            line_from=None,
            line_to=None,
            station_from=None,
            station_to=None,
            filename=None,
            table="DSR",
    ):
        import csv
        import re
        import sqlite3
        from pathlib import Path
        import datetime as _dt

        def _safe_file_part(s: str) -> str:
            s = (s or "").strip()
            for ch in ['\\', '/', ':', '*', '?', '"', '<', '>', '|']:
                s = s.replace(ch, "-")
            return s.rstrip(" .") or "export"

        def _clean_int(value):
            value = str(value or "").strip()
            if not value:
                return None
            if not value.isdigit():
                raise ValueError(f"Invalid numeric filter value: {value}")
            return int(value)

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

            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    dt0 = _dt.datetime.strptime(base, fmt)
                    return dt0.strftime("%H%M%S")
                except Exception:
                    pass

            if " " in base:
                return base.split(" ", 1)[1].replace(":", "")[:6]

            if "T" in base:
                return base.split("T", 1)[1].replace(":", "")[:6]

            return ""

        rovs = [str(r).strip() for r in (rovs or []) if str(r).strip()]

        if not rovs:
            return {"error": "No ROVs selected"}

        if output_dir is None:
            return {"error": "Missing output_dir"}

        has_line_station_filter = any([
            line_from,
            line_to,
            station_from,
            station_to,
        ])

        if not first_day and not ts_from and not ts_to and not has_line_station_filter:
            return {"error": "Missing date/time or Line/Station filter"}

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # ---------------------------------------------------------
        # Status controls ROV column, timestamp column, MODE
        # ---------------------------------------------------------
        if int(export_type) == 0:
            field_date = "Day"
            rov_col = "ROV"
            ts_col = "TimeStamp"
            mode_txt = "DEPLOYED"
            op_tag = "deploy"
            export_flag_col = "isExported"
        else:
            field_date = "Day1"
            rov_col = "ROV1"
            ts_col = "TimeStamp1"
            mode_txt = "RETRIEVED"
            op_tag = "recovery"
            export_flag_col = "isRecExported"

        # ---------------------------------------------------------
        # Coordinates only
        # ---------------------------------------------------------
        if always_primary_deployment:
            x_col = "PrimaryEasting"
            y_col = "PrimaryNorthing"
            z_col = "PrimaryElevation"
            coord_tag = "primary_deployment_coords"
        else:
            if int(export_type) == 0:
                x_col = "PrimaryEasting"
                y_col = "PrimaryNorthing"
                z_col = "PrimaryElevation"
                coord_tag = "deployment"
            else:
                x_col = "PrimaryEasting1"
                y_col = "PrimaryNorthing1"
                z_col = "PrimaryElevation1"
                coord_tag = "recovery"

        where = [
            f"{x_col} IS NOT NULL",
            f"{y_col} IS NOT NULL",
            f"{z_col} IS NOT NULL",
            f"{ts_col} IS NOT NULL",
            f"TRIM({ts_col}) <> ''",
            f"{rov_col} IS NOT NULL",
            f"TRIM({rov_col}) <> ''",
        ]
        params = []

        # ---------------------------------------------------------
        # Date/time filter optional when line/station is selected
        # ---------------------------------------------------------
        if ts_from and ts_to:
            where.append(f"{ts_col} >= ?")
            where.append(f"{ts_col} < ?")
            params.extend([ts_from, ts_to])
            label = f"{ts_from[:16].replace(' ', '_')}-{ts_to[:16].replace(' ', '_')}"
            label = _safe_file_part(label)

        elif first_day and last_day:
            where.append(f"{field_date} BETWEEN ? AND ?")
            params.extend([first_day, last_day])
            label = f"{first_day}_{last_day}"

        elif first_day:
            where.append(f"{field_date} = ?")
            params.append(first_day)
            label = first_day

        else:
            label = "selected_lines"

        # ---------------------------------------------------------
        # ROV filter
        # ---------------------------------------------------------
        placeholders = ",".join(["?"] * len(rovs))
        where.append(f"TRIM({rov_col}) IN ({placeholders})")
        params.extend(rovs)

        rov_tag = "_".join([r.replace(" ", "_") for r in rovs])
        rov_tag = _safe_file_part(rov_tag)

        # ---------------------------------------------------------
        # Line / Station filter
        # ---------------------------------------------------------
        try:
            lf = _clean_int(line_from)
            lt = _clean_int(line_to)
            sf = _clean_int(station_from)
            st = _clean_int(station_to)
        except ValueError as exc:
            return {"error": str(exc)}

        if lf is not None and lt is None:
            lt = lf
        if lt is not None and lf is None:
            lf = lt
        if sf is not None and st is None:
            st = sf
        if st is not None and sf is None:
            sf = st

        if lf is not None and lt is not None:
            if lf > lt:
                lf, lt = lt, lf
            where.append("CAST(Line AS INTEGER) BETWEEN ? AND ?")
            params.extend([lf, lt])

        if sf is not None and st is not None:
            if sf > st:
                sf, st = st, sf
            where.append("CAST(Station AS INTEGER) BETWEEN ? AND ?")
            params.extend([sf, st])

        where_sql = " AND ".join(where)

        filter_label_parts = []

        if lf is not None and lt is not None:
            filter_label_parts.append(f"L{lf}" if lf == lt else f"L{lf}-{lt}")

        if sf is not None and st is not None:
            filter_label_parts.append(f"S{sf}" if sf == st else f"S{sf}-{st}")

        filter_label = "_".join(filter_label_parts)

        # ---------------------------------------------------------
        # SQL
        # ---------------------------------------------------------
        sql = f"""
            SELECT
                ID,
                Node,
                TRIM(Line) AS Line,
                TRIM(Station) AS Station,
                CAST(NULLIF({x_col}, '') AS REAL) AS Easting,
                CAST(NULLIF({y_col}, '') AS REAL) AS Northing,
                CAST(NULLIF({z_col}, '') AS REAL) AS Depth,
                {field_date} AS D,
                {ts_col} AS TS,
                {rov_col} AS ROVName
            FROM {table}
            WHERE {where_sql}
            ORDER BY
                CAST(Line AS INTEGER),
                CAST(Station AS INTEGER),
                {ts_col}
        """

        try:
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(sql, params).fetchall()

                if not rows:
                    return {
                        "error": (
                            "No DSR rows found for selected export filters. "
                            f"Status={mode_txt}, ROV column={rov_col}, "
                            f"Timestamp column={ts_col}, coordinates={x_col}/{y_col}/{z_col}"
                        )
                    }

                if mark_exported:
                    try:
                        conn.executemany(
                            f"UPDATE {table} SET {export_flag_col} = 1 WHERE ID = ?",
                            [(r["ID"],) for r in rows],
                        )
                        conn.commit()
                    except Exception:
                        pass

        except Exception as exc:
            return {"error": f"export_dsr_to_sm: sqlite error: {exc}"}

        # ---------------------------------------------------------
        # Output filename
        # ---------------------------------------------------------
        if filename:
            safe_filename = str(filename).strip()
            safe_filename = re.sub(r'[<>:"/\\|?*]+', "_", safe_filename)
            if not safe_filename.lower().endswith(".csv"):
                safe_filename += ".csv"
            out_filename = safe_filename
        else:
            if int(zexp) == 1:
                parts = [label, rov_tag, "zexp_SM"]
            else:
                parts = [label, rov_tag, op_tag, "SM"]

            if filter_label:
                parts.insert(-1, filter_label)

            if always_primary_deployment:
                parts.insert(-1, coord_tag)

            out_filename = "_".join(parts) + ".csv"
            out_filename = _safe_file_part(out_filename)

        out_path = output_dir / out_filename

        # ---------------------------------------------------------
        # Z-NODES EXPORT FORMAT
        # ---------------------------------------------------------
        if int(zexp) == 1:
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
                    easting = float(r["Easting"] or 0.0)
                    northing = float(r["Northing"] or 0.0)
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
                        f"EASTING:{easting:.1f},"
                        f"NORTHING:{northing:.1f},"
                        f"DEPTH:{depth:.1f},"
                        f"DAY:{day_str},"
                        f"HHMMSS:{hhmmss},"
                        "survey\n"
                    )

            return {
                "success": str(out_path),
                "rows": len(rows),
                "filename": out_path.name,
                "format": "zexp",
                "mode": mode_txt,
                "rov_col": rov_col,
                "ts_col": ts_col,
                "x_col": x_col,
                "y_col": y_col,
                "z_col": z_col,
                "always_primary_deployment": bool(always_primary_deployment),
            }

        # ---------------------------------------------------------
        # NORMAL SM CSV FORMAT
        # ---------------------------------------------------------
        headers = [
            "QRCODE",
            "RFID",
            "LINE",
            "STATION",
            "CF",
            "MODE",
            "EASTING",
            "NORTHING",
            "DEPTH",
            "DAY",
            "HHMMSS",
        ]

        with out_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

            for r in rows:
                depth = float(r["Depth"] or 0.0)

                if int(export_abs) == 1:
                    depth = abs(depth)

                writer.writerow([
                    r["Node"] or "",
                    "",
                    r["Line"] or "",
                    r["Station"] or "",
                    "",
                    mode_txt,
                    f"{float(r['Easting'] or 0.0):.1f}",
                    f"{float(r['Northing'] or 0.0):.1f}",
                    f"{depth:.1f}",
                    _mmddyyyy(r["D"]),
                    _hhmmss(r["TS"]),
                ])

        return {
            "success": str(out_path),
            "rows": len(rows),
            "filename": out_path.name,
            "format": "normal_csv",
            "mode": mode_txt,
            "rov_col": rov_col,
            "ts_col": ts_col,
            "x_col": x_col,
            "y_col": y_col,
            "z_col": z_col,
            "always_primary_deployment": bool(always_primary_deployment),
        }

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
            xy_unit="m",
            z_unit="m",
            coord_unit=None,
    ):
        """
        Export selected DSR lines to SPS R-file.

        kind:
            0 = deployment coordinates from DSR PrimaryEasting/PrimaryNorthing/PrimaryElevation
            1 = recovery coordinates from DSR PrimaryEasting1/PrimaryNorthing1/PrimaryElevation1
            2 = REC_DB / First Break coordinates from REC_X/REC_Y/REC_Z

        If kind=2 and REC_DB row is missing, the point is exported as KL using deployment coordinates.

        Unit conversion is applied only during export.
        Database values remain unchanged.
        """
        if not selected_lines:
            return {"ok": False, "message": "No lines selected.", "files": [], "errors": {}}

        export_dir = Path(export_dir)
        export_dir.mkdir(parents=True, exist_ok=True)

        pcode = (pcode or "R1").strip()[:4]
        seq = (seq or "").strip()

        if use_seq and not seq:
            seq = "01"

        if coord_unit:
            xy_unit = coord_unit
            z_unit = coord_unit

        xy_factor = self._coordinate_unit_factor(xy_unit)
        z_factor = self._coordinate_unit_factor(z_unit)

        header_lines = None
        if export_header:
            header_lines = self._read_header_lines(header_file_path)

        df = self._fetch_dsr_for_lines(selected_lines)

        if df.empty:
            return {"ok": False, "message": "No DSR data found.", "files": [], "errors": {}}

        df["TS"] = pd.to_datetime(df["TimeStamp"], errors="coerce")
        df = df.dropna(subset=["TS"])

        if df.empty:
            return {"ok": False, "message": "No valid timestamps.", "files": [], "errors": {}}

        df["Line"] = pd.to_numeric(df["Line"], errors="coerce")
        df["LinePoint"] = pd.to_numeric(df["LinePoint"], errors="coerce")
        df = df.dropna(subset=["Line", "LinePoint"])

        df["Line"] = df["Line"].astype("int64")
        df["LinePoint"] = df["LinePoint"].astype("int64")

        if "Station" in df.columns:
            df["Station"] = pd.to_numeric(df["Station"], errors="coerce")
            df = df.dropna(subset=["Station"])
            df["Station"] = df["Station"].astype("int64")
        else:
            df["Station"] = df["LinePoint"].astype("int64")

        df = df.sort_values(["Line", "Station", "TS"])

        rec = self._fetch_rec_db_for_lines(selected_lines)

        if rec is None:
            rec = pd.DataFrame()

        if not rec.empty:
            rec["Line"] = pd.to_numeric(rec["Line"], errors="coerce")
            rec["LinePoint"] = pd.to_numeric(rec["LinePoint"], errors="coerce")
            rec = rec.dropna(subset=["Line", "LinePoint"])

            rec["Line"] = rec["Line"].astype("int64")
            rec["LinePoint"] = rec["LinePoint"].astype("int64")

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
                ]
                if c in rec.columns
            ]

            rec = rec[keep_cols].copy()

            merged = rec.merge(
                df,
                how="left",
                on=["Line", "LinePoint"],
                suffixes=("", "_DSR"),
            )

            rec_keys = rec[["Line", "LinePoint"]].drop_duplicates()

            dsr_only = df.merge(
                rec_keys,
                how="left",
                on=["Line", "LinePoint"],
                indicator=True,
            )

            dsr_only = dsr_only[dsr_only["_merge"] == "left_only"].drop(columns=["_merge"]).copy()

            dsr_only["Point"] = pd.NA
            dsr_only["REC_ID"] = pd.NA
            dsr_only["REC_X"] = pd.NA
            dsr_only["REC_Y"] = pd.NA
            dsr_only["REC_Z"] = pd.NA
            dsr_only["RPI"] = 0
            dsr_only["DEPLOY"] = pd.NA
            dsr_only["PointIdx"] = pd.NA

            df = pd.concat([merged, dsr_only], ignore_index=True)

        else:
            df["Point"] = pd.NA
            df["REC_ID"] = pd.NA
            df["REC_X"] = pd.NA
            df["REC_Y"] = pd.NA
            df["REC_Z"] = pd.NA
            df["RPI"] = 0
            df["DEPLOY"] = pd.NA
            df["PointIdx"] = pd.NA

        df["TS"] = pd.to_datetime(df.get("TS"), errors="coerce")
        df = df.dropna(subset=["TS"])

        if df.empty:
            return {
                "ok": False,
                "message": "After merge, no rows have valid timestamps.",
                "files": [],
                "errors": {},
            }

        df["PointCode"] = pcode
        df["Static"] = 0
        df["Datum"] = 0
        df["Elevation"] = 0
        df["Uphole"] = 0

        df["RPI"] = pd.to_numeric(df.get("RPI"), errors="coerce").fillna(0).astype("int64")

        df["JDay"] = df["TS"].dt.strftime("%j").astype(int)
        df["Hour"] = df["TS"].dt.hour.astype(int)
        df["Minute"] = df["TS"].dt.minute.astype(int)
        df["Second"] = df["TS"].dt.second.astype(int)

        if int(kind) == 0:
            df["X"] = pd.to_numeric(df["PrimaryEasting"], errors="coerce")
            df["Y"] = pd.to_numeric(df["PrimaryNorthing"], errors="coerce")
            df["Z"] = pd.to_numeric(df["PrimaryElevation"], errors="coerce")
            sub = "dep"

            df["DEPLOY"] = pd.to_numeric(df.get("DEPLOY"), errors="coerce")
            miss_dep = df["DEPLOY"].isna()

            if miss_dep.any():
                df.loc[miss_dep, "DEPLOY"] = (
                    df[miss_dep]
                    .groupby(["Line", "Station"])
                    .cumcount()
                    .add(1)
                )

            df["DEPLOY"] = df["DEPLOY"].fillna(1).astype("int64")

        elif int(kind) == 1:
            df["X"] = pd.to_numeric(df["PrimaryEasting1"], errors="coerce")
            df["Y"] = pd.to_numeric(df["PrimaryNorthing1"], errors="coerce")
            df["Z"] = pd.to_numeric(df["PrimaryElevation1"], errors="coerce")
            sub = "rec"

            if "ROV1" in df.columns:
                mask = df["ROV1"].isna() | (df["ROV1"].astype(str).str.strip() == "")
                df.loc[mask, "PointCode"] = "KL"
                df.loc[mask, "X"] = pd.to_numeric(df.loc[mask, "PrimaryEasting"], errors="coerce")
                df.loc[mask, "Y"] = pd.to_numeric(df.loc[mask, "PrimaryNorthing"], errors="coerce")
                df.loc[mask, "Z"] = pd.to_numeric(df.loc[mask, "PrimaryElevation"], errors="coerce")

            df["DEPLOY"] = pd.to_numeric(df.get("DEPLOY"), errors="coerce")
            miss_dep = df["DEPLOY"].isna()

            if miss_dep.any():
                df.loc[miss_dep, "DEPLOY"] = (
                    df[miss_dep]
                    .groupby(["Line", "Station"])
                    .cumcount()
                    .add(1)
                )

            df["DEPLOY"] = df["DEPLOY"].fillna(1).astype("int64")

        else:
            # REC_DB / First Break export.
            # Main coordinates MUST come from REC_DB: REC_X, REC_Y, REC_Z.
            # Only missing REC_DB rows fall back to deployment coordinates and become KL.
            df["X"] = pd.to_numeric(df.get("REC_X"), errors="coerce")
            df["Y"] = pd.to_numeric(df.get("REC_Y"), errors="coerce")
            df["Z"] = pd.to_numeric(df.get("REC_Z"), errors="coerce")
            sub = "fb"

            has_rec = (
                df["REC_ID"].notna()
                if "REC_ID" in df.columns
                else df["X"].notna() & df["Y"].notna()
            )

            no_rec = ~has_rec
            df.loc[no_rec, "PointCode"] = "KL"

            fallback = df["X"].isna() | df["Y"].isna() | df["Z"].isna()

            df.loc[fallback, "X"] = pd.to_numeric(
                df.loc[fallback, "PrimaryEasting"],
                errors="coerce",
            )
            df.loc[fallback, "Y"] = pd.to_numeric(
                df.loc[fallback, "PrimaryNorthing"],
                errors="coerce",
            )
            df.loc[fallback, "Z"] = pd.to_numeric(
                df.loc[fallback, "PrimaryElevation"],
                errors="coerce",
            )

            if "PointIdx" in df.columns:
                point_idx = pd.to_numeric(df["PointIdx"], errors="coerce")
            else:
                point_idx = pd.Series(pd.NA, index=df.index)

            fallback_deploy = (
                df.groupby(["Line", "Station"])
                .cumcount()
                .add(1)
                .astype("int64")
            )

            df["DEPLOY"] = point_idx.fillna(fallback_deploy)
            df["DEPLOY"] = pd.to_numeric(df["DEPLOY"], errors="coerce").fillna(1).astype("int64")

        df["X"] = pd.to_numeric(df["X"], errors="coerce") * xy_factor
        df["Y"] = pd.to_numeric(df["Y"], errors="coerce") * xy_factor
        df["Z"] = pd.to_numeric(df["Z"], errors="coerce") * z_factor

        df = df.dropna(subset=["X", "Y"])

        if df.empty:
            return {
                "ok": False,
                "message": "No rows have valid X/Y coordinates after fallback and unit conversion.",
                "files": [],
                "errors": {},
            }

        df["Z"] = pd.to_numeric(df["Z"], errors="coerce").fillna(0).abs().round(1)

        def format_z(val):
            if pd.isna(val):
                val = 0
            val = float(val)
            if val > 100:
                return str(int(round(val))).rjust(4)
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

        created_files = []

        try:
            selected_lines_int = [int(x) for x in selected_lines]

            if int(how_exp) == 1:
                for line in selected_lines_int:
                    part = df[df["Line"] == line].copy()

                    if part.empty:
                        continue

                    rec_name = record_line_name(line)
                    fname_line = file_line_name(line)
                    fname = f"{line_code}{fname_line}.R01" if use_line_code else f"{fname_line}.R01"
                    fpath = export_dir / fname

                    write_file(fpath, [build_lines_v1(part, rec_name)])
                    created_files.append(str(fpath))

            else:
                if len(selected_lines_int) > 1:
                    base = f"{selected_lines_int[0]}-{selected_lines_int[-1]}"
                else:
                    base = f"{selected_lines_int[0]}"

                if use_seq and seq:
                    if len(selected_lines_int) > 1:
                        base = f"{selected_lines_int[0]}{seq}-{selected_lines_int[-1]}{seq}"
                    else:
                        base = f"{selected_lines_int[0]}{seq}"

                fname = f"{line_code}{base}.R01" if use_line_code else f"{base}.R01"
                fpath = export_dir / fname

                blocks = []

                for line in selected_lines_int:
                    part = df[df["Line"] == line].copy()

                    if part.empty:
                        continue

                    rec_name = record_line_name(line)
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
            "kind": kind,
            "sub": sub,
            "xy_unit": xy_unit,
            "z_unit": z_unit,
            "xy_factor": xy_factor,
            "z_factor": z_factor,
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

    def export_all_bbox_configs(self, dir: str) -> dict:
        """
        Export BBox_Configs_List and BBox_Config to JSON
        """

        try:
            self.ensure_bbox_config_schema()

            out_file = Path(dir) / "bbox_configs.json"
            Path(dir).mkdir(parents=True, exist_ok=True)

            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()

                cfg_list = cur.execute("""
                    SELECT *
                    FROM BBox_Configs_List
                    ORDER BY ID
                """).fetchall()

                cfg_fields = cur.execute("""
                    SELECT *
                    FROM BBox_Config
                    ORDER BY CONFIG_FK, ID
                """).fetchall()

            payload = {
                "BBox_Configs_List": [dict(r) for r in cfg_list],
                "BBox_Config": [dict(r) for r in cfg_fields],
            }

            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)

            print("EXPORT:")
            print(" configs:", len(cfg_list))
            print(" fields :", len(cfg_fields))
            print(" file   :", out_file)

            return {
                "ok": True,
                "file": str(out_file),
                "configs_count": len(cfg_list),
                "fields_count": len(cfg_fields),
            }

        except Exception as e:
            return {"ok": False, "error": str(e)}

    def import_all_bbox_configs(self, dir: str) -> dict:
        """
        Import configs from <dir>/bbox_configs.json

        Rules:
        - keep parent ID only if it is free
        - if parent ID already exists -> insert parent without ID, SQLite generates new one
        - if parent Name already exists -> rename to Name (2), Name (3), ...
        - child ID is never imported
        - child CONFIG_FK is remapped to the inserted parent ID
        """
        try:
            self.ensure_bbox_config_schema()

            in_file = Path(dir) / "bbox_configs.json"
            if not in_file.exists():
                return {"ok": False, "error": f"File not found: {in_file}"}

            with open(in_file, "r", encoding="utf-8") as f:
                payload = json.load(f)

            cfg_list = payload.get("BBox_Configs_List", [])
            cfg_fields = payload.get("BBox_Config", [])

            print("IMPORT START")
            print(" parents in json :", len(cfg_list))
            print(" children in json:", len(cfg_fields))

            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()

                conn.execute("PRAGMA foreign_keys = ON;")
                conn.execute("BEGIN IMMEDIATE;")

                try:
                    parent_db_cols = [r["name"] for r in cur.execute("PRAGMA table_info(BBox_Configs_List)").fetchall()]
                    child_db_cols = [r["name"] for r in cur.execute("PRAGMA table_info(BBox_Config)").fetchall()]

                    parent_cols_lut = {c.lower(): c for c in parent_db_cols}
                    child_cols_lut = {c.lower(): c for c in child_db_cols}

                    parent_id_col = parent_cols_lut.get("id", "ID")
                    parent_name_col = parent_cols_lut.get("name", "Name")
                    child_id_col = child_cols_lut.get("id", "ID")
                    child_fk_col = child_cols_lut.get("config_fk", "CONFIG_FK")

                    existing_ids = {
                        int(r[0])
                        for r in cur.execute(f"SELECT {parent_id_col} FROM BBox_Configs_List").fetchall()
                        if r[0] is not None
                    }

                    existing_names = {
                        str(r[0]).strip()
                        for r in cur.execute(
                            f"SELECT {parent_name_col} FROM BBox_Configs_List "
                            f"WHERE {parent_name_col} IS NOT NULL AND TRIM({parent_name_col}) <> ''"
                        ).fetchall()
                    }

                    fk_map = {}

                    imported_parents = 0
                    imported_children = 0
                    renamed_parents = 0
                    reused_parent_ids = 0
                    new_parent_ids = 0
                    skipped_parents_invalid = 0
                    skipped_children = 0

                    def unique_name(name):
                        name = (name or "").strip()
                        if not name:
                            name = "Unnamed Config"

                        if name not in existing_names:
                            existing_names.add(name)
                            return name, False

                        i = 2
                        while True:
                            cand = f"{name} ({i})"
                            if cand not in existing_names:
                                existing_names.add(cand)
                                return cand, True
                            i += 1

                    def filter_row(row_dict, lut):
                        out = {}
                        for k, v in row_dict.items():
                            real_col = lut.get(str(k).lower())
                            if real_col:
                                out[real_col] = v
                        return out

                    # -------------------------
                    # parents
                    # -------------------------
                    for i, row in enumerate(cfg_list, start=1):
                        if not isinstance(row, dict):
                            skipped_parents_invalid += 1
                            continue

                        old_id = row.get("ID", row.get("id"))
                        if old_id is None:
                            skipped_parents_invalid += 1
                            continue

                        try:
                            old_id = int(old_id)
                        except Exception:
                            skipped_parents_invalid += 1
                            continue

                        row_new = filter_row(dict(row), parent_cols_lut)
                        if not row_new:
                            skipped_parents_invalid += 1
                            continue

                        new_name, was_renamed = unique_name(row_new.get(parent_name_col))
                        row_new[parent_name_col] = new_name
                        if was_renamed:
                            renamed_parents += 1

                        keep_original_id = old_id not in existing_ids

                        if keep_original_id:
                            row_new[parent_id_col] = old_id
                        else:
                            row_new.pop(parent_id_col, None)

                        cols = list(row_new.keys())
                        vals = [row_new[c] for c in cols]

                        sql = f"""
                            INSERT INTO BBox_Configs_List ({",".join(cols)})
                            VALUES ({",".join("?" for _ in cols)})
                        """
                        cur.execute(sql, vals)

                        if keep_original_id:
                            new_id = old_id
                            reused_parent_ids += 1
                            print(f" parent #{i}: inserted with original ID={new_id}")
                        else:
                            new_id = int(cur.lastrowid)
                            new_parent_ids += 1
                            print(f" parent #{i}: inserted with new ID={new_id}")

                        existing_ids.add(int(new_id))
                        fk_map[old_id] = int(new_id)
                        imported_parents += 1

                    print(" parents imported:", imported_parents)
                    print(" parents renamed :", renamed_parents)

                    # -------------------------
                    # children
                    # -------------------------
                    for i, row in enumerate(cfg_fields, start=1):
                        if not isinstance(row, dict):
                            skipped_children += 1
                            continue

                        old_fk = row.get("CONFIG_FK", row.get("config_fk"))
                        if old_fk is None:
                            skipped_children += 1
                            continue

                        try:
                            old_fk = int(old_fk)
                        except Exception:
                            skipped_children += 1
                            continue

                        new_fk = fk_map.get(old_fk)
                        if new_fk is None:
                            skipped_children += 1
                            continue

                        row_new = filter_row(dict(row), child_cols_lut)
                        row_new.pop(child_id_col, None)
                        row_new[child_fk_col] = new_fk

                        cols = list(row_new.keys())
                        vals = [row_new[c] for c in cols]

                        sql = f"""
                            INSERT INTO BBox_Config ({",".join(cols)})
                            VALUES ({",".join("?" for _ in cols)})
                        """
                        cur.execute(sql, vals)
                        imported_children += 1

                    conn.commit()

                    print(" children imported:", imported_children)
                    print("IMPORT DONE")

                    return {
                        "ok": True,
                        "file": str(in_file),
                        "imported_parents": imported_parents,
                        "renamed_parents": renamed_parents,
                        "reused_parent_ids": reused_parent_ids,
                        "new_parent_ids": new_parent_ids,
                        "skipped_parents_invalid": skipped_parents_invalid,
                        "imported_children": imported_children,
                        "skipped_children": skipped_children,
                    }

                except Exception as e:
                    conn.rollback()
                    print("IMPORT FAILED:", str(e))
                    return {"ok": False, "error": str(e)}

        except Exception as e:
            print("IMPORT FAILED:", str(e))
            return {"ok": False, "error": str(e)}

    def delete_bbox_config(self, config_id: int) -> dict:
        """
        Delete one config from BBox_Configs_List and all related rows from BBox_Config.
        """
        try:
            config_id = int(config_id)
        except Exception:
            return {
                "ok": False,
                "error": "Invalid config_id",
            }

        self.ensure_bbox_config_schema()

        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("BEGIN IMMEDIATE;")

            try:
                row = cur.execute("""
                    SELECT ID, Name
                    FROM BBox_Configs_List
                    WHERE ID = ?
                """, (config_id,)).fetchone()

                if not row:
                    conn.rollback()
                    return {
                        "ok": False,
                        "error": f"BBox config ID {config_id} not found",
                    }

                cfg_name = row["Name"]

                child_count = cur.execute("""
                    SELECT COUNT(*)
                    FROM BBox_Config
                    WHERE CONFIG_FK = ?
                """, (config_id,)).fetchone()[0]

                cur.execute("""
                    DELETE FROM BBox_Config
                    WHERE CONFIG_FK = ?
                """, (config_id,))

                cur.execute("""
                    DELETE FROM BBox_Configs_List
                    WHERE ID = ?
                """, (config_id,))

                conn.commit()

                return {
                    "ok": True,
                    "deleted_config_id": config_id,
                    "deleted_config_name": cfg_name,
                    "deleted_fields_count": int(child_count),
                }

            except Exception as e:
                conn.rollback()
                return {
                    "ok": False,
                    "error": str(e),
                }

    def get_blackbox_for_line(
            self,
            line: int,
            config_id: int,
            each_point: int = 1,
    ):
        """
        Return BlackBox data for selected DSR line and config_id.

        Filter:
            BlackBox.TimeStamp between MIN/MAX DSR.TimeStamp for selected DSR.Line
            BlackBox.File_FK -> BlackBox_Files.ID
            BlackBox_Files.Config_FK == config_id
        """
        each_point = max(1, int(each_point))

        sql = """
        WITH dsr_time AS (
            SELECT
                MIN(TimeStamp) AS ts_min,
                MAX(TimeStamp) AS ts_max
            FROM DSR
            WHERE Line = ?
              AND TimeStamp IS NOT NULL
              AND TRIM(TimeStamp) <> ''
        )
        SELECT
            bb.ID,
            bb.TimeStamp,
            bb.VesselEasting,
            bb.VesselNorthing,
            bb.VesselHDG,
            bb.ROV1_INS_Easting,
            bb.ROV1_INS_Northing,
            bb.ROV1_USBL_Easting,
            bb.ROV1_USBL_Northing,
            bb.ROV2_INS_Easting,
            bb.ROV2_INS_Northing,
            bb.ROV2_USBL_Easting,
            bb.ROV2_USBL_Northing,
            bb.File_FK,
            bf.Config_FK
        FROM BlackBox bb
        JOIN dsr_time dt
          ON bb.TimeStamp >= dt.ts_min
         AND bb.TimeStamp <= dt.ts_max
        JOIN BlackBox_Files bf
          ON bb.File_FK = bf.ID
        WHERE dt.ts_min IS NOT NULL
          AND dt.ts_max IS NOT NULL
          AND bf.Config_FK = ?
          AND (bb.ID % ?) = 0
        ORDER BY bb.TimeStamp, bb.ID
        """

        try:
            with self._connect() as conn:
                df = pd.read_sql_query(sql, conn, params=(line, config_id, each_point))
        except Exception as e:
            print(f"get_blackbox_for_line error: {e}")
            return pd.DataFrame()

        if df.empty:
            return df

        numeric_cols = [
            "VesselEasting", "VesselNorthing", "VesselHDG",
            "ROV1_INS_Easting", "ROV1_INS_Northing",
            "ROV1_USBL_Easting", "ROV1_USBL_Northing",
            "ROV2_INS_Easting", "ROV2_INS_Northing",
            "ROV2_USBL_Easting", "ROV2_USBL_Northing",
            "File_FK", "Config_FK",
        ]
        for c in numeric_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        return df

    def get_bbox_config_for_line(self, df):
        """
        Detect BBox configuration for a line based on df.ROV values.

        Returns
        -------
        dict | None
            {
                "config_id": int,
                "rov1_name": str,
                "rov2_name": str,
                "vessel_name": str
            }
        """

        if df is None or df.empty or "ROV" not in df.columns:
            return None

        rov_values = (
            df["ROV"]
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )

        if not rov_values:
            return None

        sql = """
        SELECT
            ID,
            rov1_name,
            rov2_name,
            Vessel_name
        FROM BBox_Configs_List
        """

        try:
            with self._connect() as conn:
                cfg = pd.read_sql(sql, conn)
        except Exception as e:
            print("BBox config lookup error:", e)
            return None

        if cfg.empty:
            return None

        for r in cfg.itertuples():

            if (
                    r.rov1_name in rov_values
                    or r.rov2_name in rov_values
            ):
                return {
                    "config_id": r.ID,
                    "rov1_name": r.rov1_name,
                    "rov2_name": r.rov2_name,
                    "vessel_name": r.Vessel_name,
                }

        return None

    def ensure_blackbox_file_stats_schema(self):
        ddl = """
        CREATE TABLE IF NOT EXISTS BlackBox_FileStats (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            File_FK INTEGER NOT NULL UNIQUE,

            -- TIME
            StartTime TEXT,
            EndTime TEXT,
            RowCount INTEGER DEFAULT 0,
            DurationSec INTEGER,
            MaxTimeGapSec REAL,

            -- CONFIG
            Config_FK INTEGER,

            -- VESSEL
            VesselEasting_Min REAL,
            VesselEasting_Max REAL,
            VesselNorthing_Min REAL,
            VesselNorthing_Max REAL,
            VesselElevation_Min REAL,
            VesselElevation_Max REAL,

            VesselHDG_Min REAL,
            VesselHDG_Max REAL,
            VesselSOG_Min REAL,
            VesselSOG_Max REAL,
            VesselCOG_Min REAL,
            VesselCOG_Max REAL,

            -- ROV MOTION
            ROV1_SOG_Min REAL,
            ROV1_SOG_Max REAL,
            ROV2_SOG_Min REAL,
            ROV2_SOG_Max REAL,

            -- ROV DEPTH
            ROV1_Depth_Min REAL,
            ROV1_Depth_Max REAL,
            ROV2_Depth_Min REAL,
            ROV2_Depth_Max REAL,

            ROV1_Depth1_Min REAL,
            ROV1_Depth1_Max REAL,
            ROV1_Depth2_Min REAL,
            ROV1_Depth2_Max REAL,

            ROV2_Depth1_Min REAL,
            ROV2_Depth1_Max REAL,
            ROV2_Depth2_Min REAL,
            ROV2_Depth2_Max REAL,

            -- GNSS GEOMETRY QC
            GNSS1_HDOP_Min REAL,
            GNSS1_HDOP_Max REAL,
            GNSS1_PDOP_Min REAL,
            GNSS1_PDOP_Max REAL,
            GNSS1_VDOP_Min REAL,
            GNSS1_VDOP_Max REAL,

            GNSS2_HDOP_Min REAL,
            GNSS2_HDOP_Max REAL,
            GNSS2_PDOP_Min REAL,
            GNSS2_PDOP_Max REAL,
            GNSS2_VDOP_Min REAL,
            GNSS2_VDOP_Max REAL,

            -- GNSS QUALITY QC
            GNSS1_NOS_Min INTEGER,
            GNSS1_NOS_Max INTEGER,
            GNSS1_DiffAge_Min REAL,
            GNSS1_DiffAge_Max REAL,
            GNSS1_FixQuality_Min INTEGER,
            GNSS1_FixQuality_Max INTEGER,

            GNSS2_NOS_Min INTEGER,
            GNSS2_NOS_Max INTEGER,
            GNSS2_DiffAge_Min REAL,
            GNSS2_DiffAge_Max REAL,
            GNSS2_FixQuality_Min INTEGER,
            GNSS2_FixQuality_Max INTEGER,

            -- ENV
            Barometer_Min REAL,
            Barometer_Max REAL,

            -- INS vs USBL POSITION QC
            ROV1_PosDiff_Min REAL,
            ROV1_PosDiff_Max REAL,
            ROV1_PosDiff_Avg REAL,

            ROV2_PosDiff_Min REAL,
            ROV2_PosDiff_Max REAL,
            ROV2_PosDiff_Avg REAL,

            FOREIGN KEY (File_FK) REFERENCES BlackBox_Files(ID) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_blackbox_filestats_file_fk
            ON BlackBox_FileStats(File_FK);
        """

        with self._connect() as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.executescript(ddl)
            conn.commit()

    def refresh_blackbox_file_stats(self, file_fk: int, conn=None) -> bool:
        """
        Recalculate BlackBox_FileStats for one file.
        Call this right after the file has been fully loaded into BlackBox.
        """
        if not file_fk:
            return False

        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.create_function("sqrt", 1, lambda x: None if x is None else math.sqrt(x))
            self.ensure_blackbox_file_stats_schema()

            cur = conn.cursor()

            # Check file exists
            row = cur.execute(
                "SELECT ID, Config_FK FROM BlackBox_Files WHERE ID = ?",
                (file_fk,)
            ).fetchone()
            if row is None:
                return False

            # Remove old stats row first, then insert fresh one
            cur.execute("DELETE FROM BlackBox_FileStats WHERE File_FK = ?", (file_fk,))

            sql = """
            INSERT INTO BlackBox_FileStats (
                File_FK,

                StartTime,
                EndTime,
                RowCount,
                DurationSec,
                MaxTimeGapSec,

                Config_FK,

                VesselEasting_Min,
                VesselEasting_Max,
                VesselNorthing_Min,
                VesselNorthing_Max,
                VesselElevation_Min,
                VesselElevation_Max,

                VesselHDG_Min,
                VesselHDG_Max,
                VesselSOG_Min,
                VesselSOG_Max,
                VesselCOG_Min,
                VesselCOG_Max,

                ROV1_SOG_Min,
                ROV1_SOG_Max,
                ROV2_SOG_Min,
                ROV2_SOG_Max,

                ROV1_Depth_Min,
                ROV1_Depth_Max,
                ROV2_Depth_Min,
                ROV2_Depth_Max,

                ROV1_Depth1_Min,
                ROV1_Depth1_Max,
                ROV1_Depth2_Min,
                ROV1_Depth2_Max,

                ROV2_Depth1_Min,
                ROV2_Depth1_Max,
                ROV2_Depth2_Min,
                ROV2_Depth2_Max,

                GNSS1_HDOP_Min,
                GNSS1_HDOP_Max,
                GNSS1_PDOP_Min,
                GNSS1_PDOP_Max,
                GNSS1_VDOP_Min,
                GNSS1_VDOP_Max,

                GNSS2_HDOP_Min,
                GNSS2_HDOP_Max,
                GNSS2_PDOP_Min,
                GNSS2_PDOP_Max,
                GNSS2_VDOP_Min,
                GNSS2_VDOP_Max,

                GNSS1_NOS_Min,
                GNSS1_NOS_Max,
                GNSS1_DiffAge_Min,
                GNSS1_DiffAge_Max,
                GNSS1_FixQuality_Min,
                GNSS1_FixQuality_Max,

                GNSS2_NOS_Min,
                GNSS2_NOS_Max,
                GNSS2_DiffAge_Min,
                GNSS2_DiffAge_Max,
                GNSS2_FixQuality_Min,
                GNSS2_FixQuality_Max,

                Barometer_Min,
                Barometer_Max,

                ROV1_PosDiff_Min,
                ROV1_PosDiff_Max,
                ROV1_PosDiff_Avg,

                ROV2_PosDiff_Min,
                ROV2_PosDiff_Max,
                ROV2_PosDiff_Avg
            )
            WITH
            bb AS (
                SELECT *
                FROM BlackBox
                WHERE File_FK = :file_fk
            ),
            gap_cte AS (
                SELECT
                    CAST(strftime('%s', TimeStamp) AS REAL)
                    - CAST(strftime('%s', LAG(TimeStamp) OVER (ORDER BY TimeStamp)) AS REAL) AS gap_sec
                FROM bb
                WHERE TimeStamp IS NOT NULL AND TimeStamp != ''
            ),
            pos_cte AS (
                SELECT
                    CASE
                        WHEN ROV1_INS_Easting IS NOT NULL
                         AND ROV1_INS_Northing IS NOT NULL
                         AND ROV1_USBL_Easting IS NOT NULL
                         AND ROV1_USBL_Northing IS NOT NULL
                        THEN sqrt(
                            ((ROV1_INS_Easting - ROV1_USBL_Easting) * (ROV1_INS_Easting - ROV1_USBL_Easting)) +
                            ((ROV1_INS_Northing - ROV1_USBL_Northing) * (ROV1_INS_Northing - ROV1_USBL_Northing))
                        )
                    END AS rov1_posdiff,
                    CASE
                        WHEN ROV2_INS_Easting IS NOT NULL
                         AND ROV2_INS_Northing IS NOT NULL
                         AND ROV2_USBL_Easting IS NOT NULL
                         AND ROV2_USBL_Northing IS NOT NULL
                        THEN sqrt(
                            ((ROV2_INS_Easting - ROV2_USBL_Easting) * (ROV2_INS_Easting - ROV2_USBL_Easting)) +
                            ((ROV2_INS_Northing - ROV2_USBL_Northing) * (ROV2_INS_Northing - ROV2_USBL_Northing))
                        )
                    END AS rov2_posdiff
                FROM bb
            ),
            agg AS (
                SELECT
                    :file_fk AS File_FK,

                    MIN(TimeStamp) AS StartTime,
                    MAX(TimeStamp) AS EndTime,
                    COUNT(*) AS RowCount,
                    CASE
                        WHEN COUNT(*) > 0
                         AND MIN(TimeStamp) IS NOT NULL
                         AND MAX(TimeStamp) IS NOT NULL
                        THEN CAST(strftime('%s', MAX(TimeStamp)) AS INTEGER)
                           - CAST(strftime('%s', MIN(TimeStamp)) AS INTEGER)
                    END AS DurationSec,

                    MIN(VesselEasting) AS VesselEasting_Min,
                    MAX(VesselEasting) AS VesselEasting_Max,
                    MIN(VesselNorthing) AS VesselNorthing_Min,
                    MAX(VesselNorthing) AS VesselNorthing_Max,
                    MIN(VesselElevation) AS VesselElevation_Min,
                    MAX(VesselElevation) AS VesselElevation_Max,

                    MIN(VesselHDG) AS VesselHDG_Min,
                    MAX(VesselHDG) AS VesselHDG_Max,
                    MIN(VesselSOG) AS VesselSOG_Min,
                    MAX(VesselSOG) AS VesselSOG_Max,
                    MIN(VesselCOG) AS VesselCOG_Min,
                    MAX(VesselCOG) AS VesselCOG_Max,

                    MIN(ROV1_SOG) AS ROV1_SOG_Min,
                    MAX(ROV1_SOG) AS ROV1_SOG_Max,
                    MIN(ROV2_SOG) AS ROV2_SOG_Min,
                    MAX(ROV2_SOG) AS ROV2_SOG_Max,

                    MIN(ROV1_Depth) AS ROV1_Depth_Min,
                    MAX(ROV1_Depth) AS ROV1_Depth_Max,
                    MIN(ROV2_Depth) AS ROV2_Depth_Min,
                    MAX(ROV2_Depth) AS ROV2_Depth_Max,

                    MIN(ROV1_Depth1) AS ROV1_Depth1_Min,
                    MAX(ROV1_Depth1) AS ROV1_Depth1_Max,
                    MIN(ROV1_Depth2) AS ROV1_Depth2_Min,
                    MAX(ROV1_Depth2) AS ROV1_Depth2_Max,

                    MIN(ROV2_Depth1) AS ROV2_Depth1_Min,
                    MAX(ROV2_Depth1) AS ROV2_Depth1_Max,
                    MIN(ROV2_Depth2) AS ROV2_Depth2_Min,
                    MAX(ROV2_Depth2) AS ROV2_Depth2_Max,

                    MIN(GNSS1_HDOP) AS GNSS1_HDOP_Min,
                    MAX(GNSS1_HDOP) AS GNSS1_HDOP_Max,
                    MIN(GNSS1_PDOP) AS GNSS1_PDOP_Min,
                    MAX(GNSS1_PDOP) AS GNSS1_PDOP_Max,
                    MIN(GNSS1_VDOP) AS GNSS1_VDOP_Min,
                    MAX(GNSS1_VDOP) AS GNSS1_VDOP_Max,

                    MIN(GNSS2_HDOP) AS GNSS2_HDOP_Min,
                    MAX(GNSS2_HDOP) AS GNSS2_HDOP_Max,
                    MIN(GNSS2_PDOP) AS GNSS2_PDOP_Min,
                    MAX(GNSS2_PDOP) AS GNSS2_PDOP_Max,
                    MIN(GNSS2_VDOP) AS GNSS2_VDOP_Min,
                    MAX(GNSS2_VDOP) AS GNSS2_VDOP_Max,

                    MIN(GNSS1_NOS) AS GNSS1_NOS_Min,
                    MAX(GNSS1_NOS) AS GNSS1_NOS_Max,
                    MIN(GNSS1_DiffAge) AS GNSS1_DiffAge_Min,
                    MAX(GNSS1_DiffAge) AS GNSS1_DiffAge_Max,
                    MIN(GNSS1_FixQuality) AS GNSS1_FixQuality_Min,
                    MAX(GNSS1_FixQuality) AS GNSS1_FixQuality_Max,

                    MIN(GNSS2_NOS) AS GNSS2_NOS_Min,
                    MAX(GNSS2_NOS) AS GNSS2_NOS_Max,
                    MIN(GNSS2_DiffAge) AS GNSS2_DiffAge_Min,
                    MAX(GNSS2_DiffAge) AS GNSS2_DiffAge_Max,
                    MIN(GNSS2_FixQuality) AS GNSS2_FixQuality_Min,
                    MAX(GNSS2_FixQuality) AS GNSS2_FixQuality_Max,

                    MIN(Barometer) AS Barometer_Min,
                    MAX(Barometer) AS Barometer_Max
                FROM bb
            ),
            pos_agg AS (
                SELECT
                    MIN(rov1_posdiff) AS ROV1_PosDiff_Min,
                    MAX(rov1_posdiff) AS ROV1_PosDiff_Max,
                    AVG(rov1_posdiff) AS ROV1_PosDiff_Avg,

                    MIN(rov2_posdiff) AS ROV2_PosDiff_Min,
                    MAX(rov2_posdiff) AS ROV2_PosDiff_Max,
                    AVG(rov2_posdiff) AS ROV2_PosDiff_Avg
                FROM pos_cte
            ),
            gap_agg AS (
                SELECT MAX(gap_sec) AS MaxTimeGapSec
                FROM gap_cte
            )
            SELECT
                agg.File_FK,

                agg.StartTime,
                agg.EndTime,
                agg.RowCount,
                agg.DurationSec,
                gap_agg.MaxTimeGapSec,

                bf.Config_FK,

                agg.VesselEasting_Min,
                agg.VesselEasting_Max,
                agg.VesselNorthing_Min,
                agg.VesselNorthing_Max,
                agg.VesselElevation_Min,
                agg.VesselElevation_Max,

                agg.VesselHDG_Min,
                agg.VesselHDG_Max,
                agg.VesselSOG_Min,
                agg.VesselSOG_Max,
                agg.VesselCOG_Min,
                agg.VesselCOG_Max,

                agg.ROV1_SOG_Min,
                agg.ROV1_SOG_Max,
                agg.ROV2_SOG_Min,
                agg.ROV2_SOG_Max,

                agg.ROV1_Depth_Min,
                agg.ROV1_Depth_Max,
                agg.ROV2_Depth_Min,
                agg.ROV2_Depth_Max,

                agg.ROV1_Depth1_Min,
                agg.ROV1_Depth1_Max,
                agg.ROV1_Depth2_Min,
                agg.ROV1_Depth2_Max,

                agg.ROV2_Depth1_Min,
                agg.ROV2_Depth1_Max,
                agg.ROV2_Depth2_Min,
                agg.ROV2_Depth2_Max,

                agg.GNSS1_HDOP_Min,
                agg.GNSS1_HDOP_Max,
                agg.GNSS1_PDOP_Min,
                agg.GNSS1_PDOP_Max,
                agg.GNSS1_VDOP_Min,
                agg.GNSS1_VDOP_Max,

                agg.GNSS2_HDOP_Min,
                agg.GNSS2_HDOP_Max,
                agg.GNSS2_PDOP_Min,
                agg.GNSS2_PDOP_Max,
                agg.GNSS2_VDOP_Min,
                agg.GNSS2_VDOP_Max,

                agg.GNSS1_NOS_Min,
                agg.GNSS1_NOS_Max,
                agg.GNSS1_DiffAge_Min,
                agg.GNSS1_DiffAge_Max,
                agg.GNSS1_FixQuality_Min,
                agg.GNSS1_FixQuality_Max,

                agg.GNSS2_NOS_Min,
                agg.GNSS2_NOS_Max,
                agg.GNSS2_DiffAge_Min,
                agg.GNSS2_DiffAge_Max,
                agg.GNSS2_FixQuality_Min,
                agg.GNSS2_FixQuality_Max,

                agg.Barometer_Min,
                agg.Barometer_Max,

                pos_agg.ROV1_PosDiff_Min,
                pos_agg.ROV1_PosDiff_Max,
                pos_agg.ROV1_PosDiff_Avg,

                pos_agg.ROV2_PosDiff_Min,
                pos_agg.ROV2_PosDiff_Max,
                pos_agg.ROV2_PosDiff_Avg
            FROM agg
            CROSS JOIN pos_agg
            CROSS JOIN gap_agg
            LEFT JOIN BlackBox_Files bf
                ON bf.ID = agg.File_FK
            WHERE agg.RowCount > 0
            """

            cur.execute(sql, {"file_fk": int(file_fk)})

            if own_conn:
                conn.commit()

            return True

        except Exception:
            if own_conn:
                conn.rollback()
            raise
        finally:
            if own_conn:
                conn.close()

    def refresh_all_blackbox_file_stats(self, conn=None) -> int:
        """
        Recalculate stats for all files from BlackBox_Files.
        """
        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            self.ensure_blackbox_file_stats_schema()
            cur = conn.cursor()
            rows = cur.execute("SELECT ID FROM BlackBox_Files ORDER BY ID").fetchall()

            count = 0
            for row in rows:
                file_fk = int(row["ID"] if hasattr(row, "keys") else row[0])
                ok = self.refresh_blackbox_file_stats(file_fk, conn=conn)
                if ok:
                    count += 1

            if own_conn:
                conn.commit()

            return count

        except Exception:
            if own_conn:
                conn.rollback()
            raise
        finally:
            if own_conn:
                conn.close()

    def get_bbox_vessel_options(self):
        conn = self._connect()
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT COALESCE(Vessel_name, '') AS VesselName
                FROM BBox_Configs_List
                WHERE COALESCE(Vessel_name, '') <> ''
                ORDER BY Vessel_name
            """)
            return [row["VesselName"] for row in cur.fetchall()]
        finally:
            conn.close()
    # --------------------------------------------------
    # DSR Line Summary SQL / table
    # --------------------------------------------------
    def dsr_line_summary_sql(self) -> str:
        """
        Source SQL used to build DSR_LineSummary.
        """
        return """
        WITH
        rec_by_line AS (
            SELECT
                rl.Line AS Line,
                COUNT(*) AS ProcessedCount
            FROM REC_DB r
            JOIN RLPreplot rl
              ON rl.ID = r.Preplot_FK
            GROUP BY rl.Line
        ),

        dsr_by_line AS (
            SELECT
                d.Line AS Line,

                -- Planned points
                MAX(rl.Points) AS PlannedPoints,

                -- RL line flags
                MAX(rl.isLineClicked)  AS isLineClicked,
                MAX(rl.isLineDeployed) AS isLineDeployed,
                MAX(rl.isValidated)    AS isValidated,

                -- Basic counts
                COUNT(*)                  AS DSRRows,
                COUNT(DISTINCT d.Station) AS Stations,
                COUNT(DISTINCT d.Node)    AS Nodes,

                MIN(d.Station) AS MinStation,
                MAX(d.Station) AS MaxStation,

                -- ROV Deployment / Retrieval counts
                SUM(CASE
                    WHEN d.TimeStamp IS NOT NULL AND TRIM(d.TimeStamp) <> ''
                    THEN 1 ELSE 0
                END) AS DeployedCount,

                SUM(CASE
                    WHEN d.TimeStamp1 IS NOT NULL AND TRIM(d.TimeStamp1) <> ''
                    THEN 1 ELSE 0
                END) AS RetrievedCount,

                -- ROV lists
                REPLACE(
                    GROUP_CONCAT(
                        DISTINCT CASE
                            WHEN d.ROV IS NOT NULL AND TRIM(d.ROV) <> '' THEN TRIM(d.ROV)
                        END
                    ),
                    ',', ', '
                ) AS DepROVs,

                REPLACE(
                    GROUP_CONCAT(
                        DISTINCT CASE
                            WHEN d.ROV1 IS NOT NULL AND TRIM(d.ROV1) <> '' THEN TRIM(d.ROV1)
                        END
                    ),
                    ',', ', '
                ) AS RecROVs,

                -- SM flags
                SUM(CASE WHEN UPPER(TRIM(d.Deployed)) = 'YES' OR UPPER(TRIM(d.PickedUp)) = 'YES' THEN 1 ELSE 0 END) AS SMCount,
                SUM(CASE WHEN UPPER(TRIM(d.PickedUp)) = 'YES' THEN 1 ELSE 0 END) AS SMRCount,

                -- Timing (deployment)
                MIN(CASE WHEN d.TimeStamp  IS NOT NULL AND TRIM(d.TimeStamp)  <> '' THEN d.TimeStamp  END) AS FirstDeployTime,
                MAX(CASE WHEN d.TimeStamp  IS NOT NULL AND TRIM(d.TimeStamp)  <> '' THEN d.TimeStamp  END) AS LastDeployTime,
                ROUND((
                    julianday(MAX(CASE WHEN d.TimeStamp IS NOT NULL AND TRIM(d.TimeStamp) <> '' THEN d.TimeStamp END)) -
                    julianday(MIN(CASE WHEN d.TimeStamp IS NOT NULL AND TRIM(d.TimeStamp) <> '' THEN d.TimeStamp END))
                ) * 24, 2) AS DeploymentHours,

                -- Timing (retrieval)
                MIN(CASE WHEN d.TimeStamp1 IS NOT NULL AND TRIM(d.TimeStamp1) <> '' THEN d.TimeStamp1 END) AS StartOfRec,
                MAX(CASE WHEN d.TimeStamp1 IS NOT NULL AND TRIM(d.TimeStamp1) <> '' THEN d.TimeStamp1 END) AS EndOfRec,
                ROUND((
                    julianday(MAX(CASE WHEN d.TimeStamp1 IS NOT NULL AND TRIM(d.TimeStamp1) <> '' THEN d.TimeStamp1 END)) -
                    julianday(MIN(CASE WHEN d.TimeStamp1 IS NOT NULL AND TRIM(d.TimeStamp1) <> '' THEN d.TimeStamp1 END))
                ) * 24, 2) AS RecDuration,

                -- Keep old names too
                MIN(CASE WHEN d.TimeStamp1 IS NOT NULL AND TRIM(d.TimeStamp1) <> '' THEN d.TimeStamp1 END) AS FirstRetrieveTime,
                MAX(CASE WHEN d.TimeStamp1 IS NOT NULL AND TRIM(d.TimeStamp1) <> '' THEN d.TimeStamp1 END) AS LastRetrieveTime,
                ROUND((
                    julianday(MAX(CASE WHEN d.TimeStamp1 IS NOT NULL AND TRIM(d.TimeStamp1) <> '' THEN d.TimeStamp1 END)) -
                    julianday(MIN(CASE WHEN d.TimeStamp1 IS NOT NULL AND TRIM(d.TimeStamp1) <> '' THEN d.TimeStamp1 END))
                ) * 24, 2) AS RetrievalHours,

                -- Total operation time
                ROUND((
                    julianday(MAX(CASE WHEN d.TimeStamp1 IS NOT NULL AND TRIM(d.TimeStamp1) <> '' THEN d.TimeStamp1 END)) -
                    julianday(MIN(CASE WHEN d.TimeStamp  IS NOT NULL AND TRIM(d.TimeStamp)  <> '' THEN d.TimeStamp  END))
                ) * 24, 2) AS TotalOperationHours,

                -- Solution counts
                SUM(CASE WHEN d.Solution_FK = 1 THEN 1 ELSE 0 END) AS Normal,
                SUM(CASE WHEN d.Solution_FK = 2 THEN 1 ELSE 0 END) AS CoDeployed,
                SUM(CASE WHEN d.Solution_FK = 3 THEN 1 ELSE 0 END) AS Losted,
                SUM(CASE WHEN d.Solution_FK = 4 THEN 1 ELSE 0 END) AS Missplaced,
                SUM(CASE WHEN d.Solution_FK = 5 THEN 1 ELSE 0 END) AS WrongID,
                SUM(CASE WHEN d.Solution_FK = 6 THEN 1 ELSE 0 END) AS Overlap,

                -- Delta statistics
                AVG(d.DeltaEprimarytosecondary)  AS AvgDeltaE,
                MIN(d.DeltaEprimarytosecondary)  AS MinDeltaE,
                MAX(d.DeltaEprimarytosecondary)  AS MaxDeltaE,

                AVG(d.DeltaNprimarytosecondary)  AS AvgDeltaN,
                MIN(d.DeltaNprimarytosecondary)  AS MinDeltaN,
                MAX(d.DeltaNprimarytosecondary)  AS MaxDeltaN,

                AVG(d.DeltaEprimarytosecondary1) AS AvgDeltaE1,
                MIN(d.DeltaEprimarytosecondary1) AS MinDeltaE1,
                MAX(d.DeltaEprimarytosecondary1) AS MaxDeltaE1,

                AVG(d.DeltaNprimarytosecondary1) AS AvgDeltaN1,
                MIN(d.DeltaNprimarytosecondary1) AS MinDeltaN1,
                MAX(d.DeltaNprimarytosecondary1) AS MaxDeltaN1,

                -- Sigma statistics
                AVG(d.Sigma)  AS AvgSigma,
                MIN(d.Sigma)  AS MinSigma,
                MAX(d.Sigma)  AS MaxSigma,

                AVG(d.Sigma1) AS AvgSigma1,
                MIN(d.Sigma1) AS MinSigma1,
                MAX(d.Sigma1) AS MaxSigma1,

                AVG(d.Sigma2) AS AvgSigma2,
                MIN(d.Sigma2) AS MinSigma2,
                MAX(d.Sigma2) AS MaxSigma2,

                AVG(d.Sigma3) AS AvgSigma3,
                MIN(d.Sigma3) AS MinSigma3,
                MAX(d.Sigma3) AS MaxSigma3,

                -- Radial Offset
                AVG(d.RangetoPrePlot) AS AvgRadOffset,
                MIN(d.RangetoPrePlot) AS MinRadOffset,
                MAX(d.RangetoPrePlot) AS MaxRadOffset,

                -- Range Primary to Secondary
                AVG(d.Rangeprimarytosecondary) AS AvgRangePrimToSec,
                MIN(d.Rangeprimarytosecondary) AS MinRangePrimToSec,
                MAX(d.Rangeprimarytosecondary) AS MaxRangePrimToSec,

                -- Elevation stats
                AVG(d.PrimaryElevation)   AS AvgPrimaryElevation,
                MIN(d.PrimaryElevation)   AS MinPrimaryElevation,
                MAX(d.PrimaryElevation)   AS MaxPrimaryElevation,

                AVG(d.SecondaryElevation) AS AvgSecondaryElevation,
                MIN(d.SecondaryElevation) AS MinSecondaryElevation,
                MAX(d.SecondaryElevation) AS MaxSecondaryElevation

            FROM DSR d
            LEFT JOIN RLPreplot rl
              ON rl.Line = d.Line
            GROUP BY d.Line
        ),

        config_per_line AS (
            SELECT
                d.Line,
                MIN(bcl.ID) AS ConfigID
            FROM DSR d
            JOIN BBox_Configs_List bcl
              ON TRIM(d.ROV) = bcl.rov1_name
              OR TRIM(d.ROV) = bcl.rov2_name
            WHERE d.ROV IS NOT NULL AND TRIM(d.ROV) <> ''
            GROUP BY d.Line
        ),

        -- Deployment stats by ROV (unique stations)
        dep_rov_counts AS (
            SELECT
                d.Line,
                TRIM(d.ROV) AS ROV,
                COUNT(DISTINCT d.Station) AS Cnt
            FROM DSR d
            WHERE d.ROV IS NOT NULL
              AND TRIM(d.ROV) <> ''
              AND d.TimeStamp IS NOT NULL
              AND TRIM(d.TimeStamp) <> ''
            GROUP BY d.Line, TRIM(d.ROV)
        ),

        dep_rov_agg AS (
            SELECT
                Line,
                REPLACE(
                    GROUP_CONCAT(ROV || '(' || Cnt || ')'),
                    ',', ', '
                ) AS DepROVStats
            FROM dep_rov_counts
            GROUP BY Line
        ),

        -- Recovery stats by ROV1 (unique stations)
        rec_rov_counts AS (
            SELECT
                d.Line,
                TRIM(d.ROV1) AS ROV1,
                COUNT(DISTINCT d.Station) AS Cnt
            FROM DSR d
            WHERE d.ROV1 IS NOT NULL
              AND TRIM(d.ROV1) <> ''
              AND d.TimeStamp1 IS NOT NULL
              AND TRIM(d.TimeStamp1) <> ''
            GROUP BY d.Line, TRIM(d.ROV1)
        ),

        rec_rov_agg AS (
            SELECT
                Line,
                REPLACE(
                    GROUP_CONCAT(ROV1 || '(' || Cnt || ')'),
                    ',', ', '
                ) AS RecROVStats
            FROM rec_rov_counts
            GROUP BY Line
        )

        SELECT
            s.Line,
            s.PlannedPoints,

            s.isLineClicked,
            s.isLineDeployed,
            s.isValidated,

            s.DSRRows,
            s.Stations,
            s.Nodes,
            s.MinStation,
            s.MaxStation,

            s.DeployedCount,
            s.RetrievedCount,

            s.DepROVs,
            s.RecROVs,
            dra.DepROVStats,
            rra.RecROVStats,

            s.SMCount,
            s.SMRCount,

            COALESCE(r.ProcessedCount, 0) AS ProcessedCount,

            s.FirstDeployTime,
            s.LastDeployTime,
            s.DeploymentHours,

            s.StartOfRec,
            s.EndOfRec,
            s.RecDuration,

            s.FirstRetrieveTime,
            s.LastRetrieveTime,
            s.RetrievalHours,

            s.TotalOperationHours,

            ROUND(100.0 * s.DeployedCount / NULLIF(s.PlannedPoints, 0), 1) AS DeployedPct,
            ROUND(100.0 * s.RetrievedCount / NULLIF(s.PlannedPoints, 0), 1) AS RetrievedPct,
            ROUND(100.0 * COALESCE(r.ProcessedCount, 0) / NULLIF(s.PlannedPoints, 0), 1) AS ProcessedPct,

            s.Normal,
            s.CoDeployed,
            s.Losted,
            s.Missplaced,
            s.WrongID,
            s.Overlap,

            s.AvgDeltaE,  s.MinDeltaE,  s.MaxDeltaE,
            s.AvgDeltaN,  s.MinDeltaN,  s.MaxDeltaN,
            s.AvgDeltaE1, s.MinDeltaE1, s.MaxDeltaE1,
            s.AvgDeltaN1, s.MinDeltaN1, s.MaxDeltaN1,

            s.AvgSigma,  s.MinSigma,  s.MaxSigma,
            s.AvgSigma1, s.MinSigma1, s.MaxSigma1,
            s.AvgSigma2, s.MinSigma2, s.MaxSigma2,
            s.AvgSigma3, s.MinSigma3, s.MaxSigma3,

            (s.MaxSigma  * 2.44774683068) AS Primary_e95,
            (s.MaxSigma1 * 2.44774683068) AS Primary_n95,

            s.AvgRadOffset, s.MinRadOffset, s.MaxRadOffset,
            s.AvgRangePrimToSec, s.MinRangePrimToSec, s.MaxRangePrimToSec,

            bcl.*

        FROM dsr_by_line s
        LEFT JOIN rec_by_line r
          ON r.Line = s.Line
        LEFT JOIN config_per_line cpl
          ON cpl.Line = s.Line
        LEFT JOIN BBox_Configs_List bcl
          ON bcl.ID = cpl.ConfigID
        LEFT JOIN dep_rov_agg dra
          ON dra.Line = s.Line
        LEFT JOIN rec_rov_agg rra
          ON rra.Line = s.Line
        """

    def _get_query_column_names(self, conn, sql: str) -> list[str]:
        cur = conn.execute(f"SELECT * FROM ({sql}) q LIMIT 0")
        return [d[0] for d in (cur.description or [])]

    def _get_table_column_names(self, conn, table_name: str) -> list[str]:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return [row["name"] for row in rows]

    def ensure_dsr_line_summary_table(self, conn=None, rebuild_if_schema_changed: bool = True):
        """
        Ensures DSR_LineSummary exists and matches current SELECT output columns.
        Because the SELECT includes bcl.*, we create schema dynamically from the query itself.
        """
        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            sql = self.dsr_line_summary_sql()
            expected_cols = self._get_query_column_names(conn, sql)

            row = conn.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type='table' AND name='DSR_LineSummary'
            """).fetchone()

            table_exists = row is not None

            if table_exists:
                actual_cols = self._get_table_column_names(conn, "DSR_LineSummary")
                if actual_cols != expected_cols and rebuild_if_schema_changed:
                    conn.execute("DROP TABLE IF EXISTS DSR_LineSummary")
                    table_exists = False

            if not table_exists:
                conn.execute(f"""
                    CREATE TABLE DSR_LineSummary AS
                    SELECT * FROM ({sql}) q
                    WHERE 1 = 0
                """)

                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_dsr_linesummary_line
                    ON DSR_LineSummary(Line)
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS ix_dsr_linesummary_config_id
                    ON DSR_LineSummary(ID)
                """)

            if own_conn:
                conn.commit()

        finally:
            if own_conn:
                conn.close()

    def refresh_dsr_line_summary_table(self, conn=None) -> int:
        """
        Full rebuild of DSR_LineSummary.
        Safe and simple.
        """
        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            self.ensure_dsr_line_summary_table(conn=conn)
            sql = self.dsr_line_summary_sql()

            conn.execute("DELETE FROM DSR_LineSummary")
            conn.execute(f"""
                INSERT INTO DSR_LineSummary
                SELECT * FROM ({sql}) q
            """)

            row = conn.execute("SELECT COUNT(*) AS cnt FROM DSR_LineSummary").fetchone()
            count = int(row["cnt"]) if row else 0

            if own_conn:
                conn.commit()

            return count

        finally:
            if own_conn:
                conn.close()


    def refresh_dsr_line_summary_lines(self, lines, conn=None) -> int:
        """
        Incremental refresh for only changed lines.
        Use this after DSR upload.
        """
        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            self.ensure_dsr_line_summary_table(conn=conn)

            if lines is None:
                return 0

            cleaned = []
            seen = set()
            for x in lines:
                if x is None:
                    continue
                try:
                    val = int(x)
                except Exception:
                    continue
                if val in seen:
                    continue
                seen.add(val)
                cleaned.append(val)

            if not cleaned:
                return 0

            placeholders = ",".join("?" for _ in cleaned)
            sql = self.dsr_line_summary_sql()

            conn.execute(
                f"DELETE FROM DSR_LineSummary WHERE Line IN ({placeholders})",
                cleaned
            )

            conn.execute(f"""
                INSERT INTO DSR_LineSummary
                SELECT *
                FROM ({sql}) q
                WHERE q.Line IN ({placeholders})
            """, cleaned)

            if own_conn:
                conn.commit()

            return len(cleaned)

        finally:
            if own_conn:
                conn.close()

    def dsr_line_summary_rowcount(self, conn=None) -> int:
        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            row = conn.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type='table' AND name='DSR_LineSummary'
            """).fetchone()

            if row is None:
                return 0

            row = conn.execute("SELECT COUNT(*) AS cnt FROM DSR_LineSummary").fetchone()
            return int(row["cnt"]) if row else 0

        finally:
            if own_conn:
                conn.close()

    def ensure_dsr_line_summary_ready(self, conn=None, rebuild_if_empty: bool = True) -> dict:
        """
        Ensure DSR_LineSummary exists.
        If table exists but is empty, rebuild it.
        Returns small status dict for logging/debugging.
        """
        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            row = conn.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type='table' AND name='DSR_LineSummary'
            """).fetchone()

            created = False
            refreshed = False
            rowcount = 0

            if row is None:
                self.ensure_dsr_line_summary_table(conn=conn)
                created = True

            rowcount = self.dsr_line_summary_rowcount(conn=conn)

            if rebuild_if_empty and rowcount == 0:
                rowcount = self.refresh_dsr_line_summary_table(conn=conn)
                refreshed = True

            if own_conn:
                conn.commit()

            return {
                "ok": True,
                "created": created,
                "refreshed": refreshed,
                "rowcount": rowcount,
            }

        finally:
            if own_conn:
                conn.close()

    def get_daily_sm_comparison(self, date, day_field="Day",rov_field = "ROV"):
        if day_field not in ("Day", "Day1"):
            raise ValueError("Invalid day field")

        query = f"""
            SELECT
                    Line,
                    Station,
                    Node AS DSRNode,
                
                    CASE
                        WHEN AUQRCode IS NOT NULL AND TRIM(AUQRCode) <> ''
                            THEN AUQRCode
                        ELSE RemoteUnit
                    END AS SMNode,
                
                    AUQRCode,
                    {rov_field} as ROV,
                
                    PrimaryEasting AS DsrEasting,
                    PrimaryNorthing AS DsrNorthing,
                    PrimaryElevation AS DsrElevation,
                
                    ActualX AS SMEasting,
                    ActualY AS SMNorthing,
                    ActualZ AS SMElevation,
                
                    PrimaryEasting - ActualX AS dX,
                    PrimaryNorthing - ActualY AS dY,
                    ABS(ABS(PrimaryElevation) - ABS(ActualZ)) AS dZ,
                
                    Comments AS DSRComments
                
                FROM DSR
                WHERE {day_field} = ?
                ORDER BY Line, Station;
        """

        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, (date,)).fetchall()

        return [dict(row) for row in rows]

    def get_sm_comparison_by_line(self, line):
        query = """
            SELECT
                Line,
                Station,
                Node AS DSRNode,

                CASE
                    WHEN AUQRCode IS NOT NULL
                         AND TRIM(AUQRCode) <> ''
                    THEN AUQRCode
                    ELSE RemoteUnit
                END AS SMNode,

                ROV,

                PrimaryEasting AS DsrEasting,
                PrimaryNorthing AS DsrNorthing,
                PrimaryElevation AS DsrElevation,

                ActualX AS SMEasting,
                ActualY AS SMNorthing,
                ActualZ AS SMElevation,

                PrimaryEasting - ActualX AS dX,
                PrimaryNorthing - ActualY AS dY,
                ABS(PrimaryElevation) - ABS(ActualZ) AS dZ,

                Comments AS DSRComments

            FROM DSR
            WHERE Line = ?
            ORDER BY Line, Station
        """

        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, (line,)).fetchall()

        return [dict(row) for row in rows]

    def get_rlsolution_lines(self):
        query = """
            SELECT DISTINCT Line
            FROM DSR_LineSummary 
            ORDER BY Line
        """

        with self._connect() as conn:
            rows = conn.execute(query).fetchall()

        return [row[0] for row in rows]













