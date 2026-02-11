import csv
import io
import re
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple
import traceback

import pandas as pd
from django.utils.html import escape
from django.template.loader import render_to_string

from core.projectdb import ProjectDB


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
    def linescaler(self):
        mask = getattr(self.pdb.get_geometry(), "rl_mask", "")
        return 10 ** (mask.count("L") + 1) if "L" in mask else 0

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
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                return datetime.fromisoformat(ts)
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
            solution_name: str = "Normal",
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
            "Solution_FK", "RLPreplot_FK", "RecIdx", "TIER",
            "LinePointIdx", "LinePoint",
            "Year", "Month", "Week", "Day", "JDay",
            "Year1", "Month1", "Week1", "Day1", "JDay1",
            *file_cols,
        ]

        placeholders = ",".join("?" * len(insert_cols))
        update_cols = [c for c in insert_cols if c not in ("Line", "Station", "Node", "Solution_FK")]
        update_sql = ", ".join(f'"{c}"=excluded."{c}"' for c in update_cols)

        sql_upsert = f"""
        INSERT INTO DSR ({",".join(insert_cols)})
        VALUES ({placeholders})
        ON CONFLICT(Line,Station,Node) DO UPDATE SET
        {update_sql};
        """

        NORMAL_ID = 1
        OVERLAP_ID = 6

        processed = upserted = 0
        batch = []

        scaler = int(getattr(self, "linescaler", 0) or 0)

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")

            conn.executescript("""
            INSERT OR IGNORE INTO DSRSolution(ID,NAME) VALUES (1,'Normal');
            INSERT OR IGNORE INTO DSRSolution(ID,NAME) VALUES (6,'Overlap');
            """)

            sql_ls_exists = "SELECT 1 FROM DSR WHERE Line=? AND Station=? LIMIT 1"
            sql_same_exists = "SELECT Solution_FK FROM DSR WHERE Line=? AND Station=? AND Node=? LIMIT 1"

            # RLPreplot lookup
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

                line = self._to_int(row[0])
                station = self._to_int(row[1])
                if line is None or station is None:
                    continue

                node = self._to_node(row[2])

                dt = self._parse_ts(row[6] if len(row) > 6 else "")
                dt1 = self._parse_ts(row[26] if len(row) > 26 else "")
                y, m, w, d, j = self._dt_parts(dt)
                y1, m1, w1, d1, j1 = self._dt_parts(dt1)

                same = conn.execute(sql_same_exists, (line, station, node)).fetchone()
                if same:
                    solution_fk = int(same[0])
                else:
                    exists_ls = conn.execute(sql_ls_exists, (line, station)).fetchone()
                    solution_fk = OVERLAP_ID if exists_ls else NORMAL_ID

                lp = (line * scaler + station) if scaler > 0 else station
                lp_idx = (lp * 10 + rec_idx)

                rl_row = conn.execute(sql_rl_fk, (line, tier)).fetchone()
                rl_fk = int(rl_row[0]) if rl_row else None

                values = {
                    "Solution_FK": solution_fk,
                    "RLPreplot_FK": rl_fk,
                    "RecIdx": rec_idx,
                    "TIER": tier,
                    "LinePoint": lp,
                    "LinePointIdx": lp_idx,
                    "Year": y, "Month": m, "Week": w, "Day": d, "JDay": j,
                    "Year1": y1, "Month1": m1, "Week1": w1, "Day1": d1, "JDay1": j1,
                }

                for i, col in enumerate(file_cols):
                    raw = row[i] if i < len(row) else ""
                    if col == "Node":
                        values[col] = node
                    elif col in {"ROV", "TimeStamp", "Quality", "ROV1", "TimeStamp1", "Quality1", "Comments"}:
                        values[col] = self._to_text(raw)
                    elif col in {"Line", "Station"}:
                        values[col] = self._to_int(raw)
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

        return processed, upserted

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
            name: str,
            rov1_name: str = "",
            rov2_name: str = "",
            gnss1_name: str = "",
            gnss2_name: str = "",
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
                INSERT INTO BBox_Configs_List (Name, IsDefault, rov1_name, rov2_name, gnss1_name, gnss2_name)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(Name) DO UPDATE SET
                    IsDefault = excluded.IsDefault,
                    rov1_name = excluded.rov1_name,
                    rov2_name = excluded.rov2_name,
                    gnss1_name = excluded.gnss1_name,
                    gnss2_name = excluded.gnss2_name
                """,
                (
                    name,
                    1 if is_default else 0,
                    rov1_name,
                    rov2_name,
                    gnss1_name,
                    gnss2_name,
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
                    IsDefault,
                    rov1_name,
                    rov2_name,
                    gnss1_name,
                    gnss2_name
                FROM BBox_Configs_List
                ORDER BY IsDefault DESC, Name COLLATE NOCASE
                """
            ).fetchall()

        return [
            {
                "id": r["ID"],
                "name": r["Name"],
                "is_default": bool(r["IsDefault"]),
                "rov1_name": r["rov1_name"],
                "rov2_name": r["rov2_name"],
                "gnss1_name": r["gnss1_name"],
                "gnss2_name": r["gnss2_name"],
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
            default_node: str = "NA",
    ) -> dict:
        """
        Import SM and UPDATE DSR. No Solution_FK. Table fixed to DSR.

        update_key:
          - "unique"      -> WHERE Line=? AND Station=? AND Node=? (recommended)
          - "linepointidx"-> WHERE LinePointIdx=? (legacy)
        """

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

        # -------------------------
        # 2) Normalize columns
        # -------------------------
        df.columns = [re.sub(r"\W", "", str(c)).strip() for c in df.columns]
        df.columns = [re.sub(r"line", "Line", c, flags=re.IGNORECASE) for c in df.columns]

        # Station aliases
        lower_map = {c.lower(): c for c in df.columns}
        if "station" not in lower_map:
            for alias in ("point", "stn", "sta", "stationno", "stationnumber"):
                if alias in lower_map:
                    df.rename(columns={lower_map[alias]: "Station"}, inplace=True)
                    break

        # Node aliases (optional)
        lower_map = {c.lower(): c for c in df.columns}
        if "node" not in lower_map:
            for alias in ("nodeid", "node_id", "receiver", "receiverid"):
                if alias in lower_map:
                    df.rename(columns={lower_map[alias]: "Node"}, inplace=True)
                    break

        if "Line" not in df.columns or "Station" not in df.columns:
            return {"error": f"SM must contain Line and Station. Columns: {list(df.columns)}"}

        if "Node" not in df.columns:
            df["Node"] = default_node

        df["Node"] = df["Node"].fillna(default_node).astype(str)
        df["Line"] = pd.to_numeric(df["Line"], errors="coerce").fillna(0).astype("int64")
        df["Station"] = pd.to_numeric(df["Station"], errors="coerce").fillna(0).astype("int64")

        # -------------------------
        # 3) Compute LinePoint / RecIdx / LinePointIdx
        # -------------------------
        scaler = int(getattr(self, "linescaler", 0) or 0)
        if scaler <= 0:
            scaler = 100000  # fallback

        df["LinePoint"] = (df["Line"] * scaler + df["Station"]).astype("int64")
        df["RecIdx"] = df.groupby(["LinePoint", "Node"]).cumcount().add(1).astype("int64")
        df["LinePointIdx"] = (df["LinePoint"] * 10 + df["RecIdx"]).astype("int64")

        if "ROV" in df.columns:
            df.drop(columns=["ROV"], inplace=True)

        # -------------------------
        # 4) Update DSR (fixed table)
        # -------------------------
        with self._connect() as conn:
            info = conn.execute("PRAGMA table_info(DSR)").fetchall()
            table_cols = [r["name"] for r in info]
            table_cols_lc = {c.lower(): c for c in table_cols}

            # map casing to DB
            df.rename(columns={c: table_cols_lc[c.lower()] for c in df.columns if c.lower() in table_cols_lc},
                      inplace=True)

            update_key = update_key.lower().strip()
            if update_key not in ("unique", "linepointidx"):
                return {"error": "update_key must be 'unique' or 'linepointidx'"}

            if update_key == "linepointidx":
                where_cols = ["LinePointIdx"]
                if "LinePointIdx" not in df.columns:
                    return {"error": "Missing LinePointIdx for linepointidx update."}
            else:
                where_cols = ["Line", "Station", "Node"]

            set_cols = []
            for c in df.columns:
                lc = c.lower()
                if lc == "id":
                    continue
                if c in where_cols:
                    continue
                if lc in table_cols_lc:
                    set_cols.append(c)

            if not set_cols:
                return {"error": "No matching DSR columns to update from SM."}

            sql = (
                "UPDATE DSR "
                f"SET {', '.join(f'{c}=?' for c in set_cols)} "
                f"WHERE {' AND '.join(f'{c}=?' for c in where_cols)}"
            )

            values = []
            for r in df.itertuples(index=False):
                d = r._asdict()
                values.append(tuple(d[c] for c in set_cols) + tuple(d[c] for c in where_cols))

            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany(sql, values)
                conn.commit()
            except Exception as e:
                return {"error": f"DSR update failed: {e}"}

        return {
            "success": True,
            "rows_in_file": int(len(df)),
            "update_key": update_key,
            "updated_attempted": int(len(values)),
            "set_cols": set_cols,
        }

    def load_fb_from_file(self, file_obj_or_path, *, chunk_rows: int = 50000) -> dict:
        """
        FB/RECDB whitespace-delimited loader.
        Updates DSR by LinePointIdx.

        file_obj_or_path:
          - str / Path
          - Django UploadedFile (InMemoryUploadedFile / TemporaryUploadedFile)

        Required columns:
          - REC_ID
          - RPI
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
        # mask / positions
        # -------------------------
        geom = self.pdb.get_geometry()
        mask = getattr(geom, "rl_mask", "") or ""
        if not mask or "L" not in mask or "P" not in mask:
            return {"error": "rl_mask/RLineMask missing or invalid"}

        line_pos0 = mask.index("L")
        line_pos1 = mask.rfind("L")
        point_pos0 = mask.index("P")
        point_pos1 = mask.rfind("P")

        num_point_digits = point_pos1 - point_pos0 + 1
        scalar_point = int("1" + ("0" * num_point_digits))

        fin_rename = {
            "FINPITCH": "PITCHFIN",
            "FINROLL": "ROLLFIN",
            "FINYAW": "YAWFIN",
        }

        # -------------------------
        # read DSR schema once
        # -------------------------
        with self._connect() as conn:
            dsr_info = conn.execute("PRAGMA table_info(DSR)").fetchall()
            dsr_cols = {r["name"].lower(): r["name"] for r in dsr_info}
            key_col = dsr_cols.get("linepointidx", "LinePointIdx")

        # -------------------------
        # build reader (always whitespace)
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
                return {"error": "File is empty or unreadable"}

            reader = pd.read_csv(
                io.StringIO(text),
                sep=r"\s+",
                chunksize=chunk_rows,
                low_memory=False,
                engine="python",
            )
            src_name = getattr(file_obj_or_path, "name", "uploaded_file")

        # -------------------------
        # process chunks
        # -------------------------
        total_rows = 0
        total_updates_attempted = 0

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for df in reader:
                    if df is None or df.empty:
                        continue

                    total_rows += len(df)

                    df.columns = [re.sub(r"\W", "", str(c)).strip() for c in df.columns]

                    for k, v in fin_rename.items():
                        if k in df.columns:
                            df.rename(columns={k: v}, inplace=True)

                    if "REC_ID" not in df.columns or "RPI" not in df.columns:
                        raise ValueError("FB file must contain REC_ID and RPI")

                    rec_str = df["REC_ID"].astype(str)

                    line_str = rec_str.str.slice(line_pos0, line_pos1 + 1)
                    point_str = rec_str.str.slice(point_pos0, point_pos1 + 1)

                    line_val = pd.to_numeric(line_str, errors="coerce").fillna(0).astype("int64")
                    point_val = pd.to_numeric(point_str, errors="coerce").fillna(0).astype("int64")
                    rpi_val = pd.to_numeric(df["RPI"], errors="coerce").fillna(0).astype("int64")

                    df["LinePoint"] = line_val * scalar_point + point_val
                    df[key_col] = df["LinePoint"].astype("int64") * 10 + rpi_val
                    df.drop(columns=["LinePoint"], inplace=True, errors="ignore")

                    keep_cols = [c for c in df.columns if c.lower() in dsr_cols and c.lower() != "id"]
                    if key_col not in keep_cols:
                        keep_cols.append(key_col)

                    set_cols = [c for c in keep_cols if c != key_col]
                    if not set_cols:
                        continue

                    rename_to_dsr = {c: dsr_cols[c.lower()] for c in keep_cols if c.lower() in dsr_cols}
                    df.rename(columns=rename_to_dsr, inplace=True)

                    key_db = rename_to_dsr.get(key_col, key_col)
                    set_db = [rename_to_dsr.get(c, c) for c in set_cols]

                    sql = (
                            "UPDATE DSR SET "
                            + ", ".join(f'"{c}"=?' for c in set_db)
                            + f' WHERE "{key_db}"=?'
                    )

                    sub = df[set_db + [key_db]].where(pd.notnull(df), None)
                    values = list(sub.itertuples(index=False, name=None))

                    conn.executemany(sql, values)
                    total_updates_attempted += len(values)

                conn.commit()

            except Exception as e:
                conn.rollback()
                return {"error": f"load_fb_from_file error: {e}", "file": src_name}

        return {
            "success": f"File {src_name} processed",
            "rows_read": int(total_rows),
            "updates_attempted": int(total_updates_attempted),
        }










