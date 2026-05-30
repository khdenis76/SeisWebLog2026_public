import math
import os
import sqlite3
from datetime import datetime, timedelta

from core.models import SPSRevision


class ReceiverSPS:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 120000")
        return conn

    def load_file(
        self,
        file_path: str,
        *,
        sps_revision_id: int,
        solution_fk: int,
        year: int,
        node_vessel_fk: int | None = None,
        vessel_name: str = "",
        tier: int = 1,
        line_mask: str | None = None,
        chunk_size: int = 20000,
    ) -> dict:

        revision = SPSRevision.objects.get(id=sps_revision_id)
        conn = self._connect()

        try:
            self.ensure_tables(conn)

            file_id = self._get_or_create_sps_file(conn, file_path)
            conn.commit()

            if node_vessel_fk and not vessel_name:
                vessel_name = self._get_vessel_name(conn, node_vessel_fk)

            pp_lines = self._load_rlpreplot_lookup(conn)
            pp_points = self._load_rppreplot_lookup(conn)

            if not pp_lines:
                return {
                    "ok": False,
                    "error": "RLPreplot is empty. Load receiver preplot first.",
                }

            encoding = self._detect_encoding(file_path)

            rows_buffer = []
            line_stats = {}

            total_rows = 0
            skipped_rows = 0

            with conn:
                with open(file_path, "rt", encoding=encoding, errors="ignore") as f:
                    for raw in f:
                        if not raw or raw.startswith("H"):
                            continue

                        parsed = self._parse_record(
                            raw_line=raw,
                            revision=revision,
                            year=year,
                            tier=tier,
                            solution_fk=solution_fk,
                            file_id=file_id,
                            vessel_name=vessel_name,
                            node_vessel_fk=node_vessel_fk,
                            line_mask=line_mask,
                        )

                        if not parsed:
                            skipped_rows += 1
                            continue

                        line = parsed["Line"]
                        point = parsed["Point"]

                        pp_line = pp_lines.get(line)

                        if not pp_line:
                            skipped_rows += 1
                            continue

                        parsed["PP_Line_FK"] = pp_line["ID"]

                        pp_point = pp_points.get((pp_line["ID"], point))

                        if pp_point:
                            parsed["PP_Point_FK"] = pp_point["ID"]
                            parsed["PP_X"] = pp_point["X"]
                            parsed["PP_Y"] = pp_point["Y"]

                            dx = float(parsed["Easting"] or 0) - float(pp_point["X"] or 0)
                            dy = float(parsed["Northing"] or 0) - float(pp_point["Y"] or 0)

                            parsed["dX"] = dx
                            parsed["dY"] = dy
                            parsed["RadialOffset"] = math.hypot(dx, dy)

                            bearing = float(
                                pp_line.get("LineBearing")
                                or pp_line.get("CalcLineBearing")
                                or 0
                            )
                            theta = math.radians(bearing)

                            parsed["ILOffset"] = dx * math.sin(theta) + dy * math.cos(theta)
                            parsed["XLOffset"] = dx * math.cos(theta) - dy * math.sin(theta)

                            parsed["isPreplotCompared"] = 1
                            parsed["isCompared"] = 1

                        self._update_line_stat(
                            line_stats=line_stats,
                            row=parsed,
                            pp_line=pp_line,
                            solution_fk=solution_fk,
                        )

                        rows_buffer.append(parsed)
                        total_rows += 1

                        if len(rows_buffer) >= chunk_size:
                            self._flush_rpsolution(conn, rows_buffer)
                            rows_buffer.clear()

                if rows_buffer:
                    self._flush_rpsolution(conn, rows_buffer)

                self._flush_rlsolution(conn, line_stats)

            return {
                "ok": True,
                "file": os.path.basename(file_path),
                "rows": total_rows,
                "skipped": skipped_rows,
                "lines": len(line_stats),
            }

        except Exception as exc:
            return {
                "ok": False,
                "file": os.path.basename(file_path),
                "error": str(exc),
            }

        finally:
            conn.close()

    def ensure_tables(self, conn):
        """
        Safe table initialization.

        Does not drop tables on every load.
        Creates tables only if missing.
        """
        self.ensure_sps_files_table(conn)
        self.ensure_rlsolution_table(conn)
        self.ensure_rpsolution_table(conn)
        self.ensure_indexes(conn)

    def _table_exists(self, conn, table_name: str) -> bool:
        row = conn.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name = ?
        """, (table_name,)).fetchone()

        return row is not None

    def ensure_rlsolution_table(self, conn):
        """
        Current RLSolution schema.

        Changes:
        - SRP removed
        - ERP removed
        - PP_Count added from RLPreplot.Points
        - Vessel_FK added from project_fleet.id
        """

        if self._table_exists(conn, "RLSolution"):
            return

        conn.execute("""
            CREATE TABLE RLSolution (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,

                PPLine_FK INTEGER,
                File_FK INTEGER,
                FileName_FK INTEGER,

                LineName TEXT,
                Line INTEGER,

                Seq INTEGER DEFAULT 1,
                Attempt TEXT,

                Tier INTEGER,
                TierLine INTEGER,

                FRP INTEGER,
                LRP INTEGER,

                PP_Count INTEGER DEFAULT 0,

                StartX REAL DEFAULT 0,
                StartY REAL DEFAULT 0,
                EndX REAL DEFAULT 0,
                EndY REAL DEFAULT 0,

                Vessel TEXT,
                Vessel_FK INTEGER,

                StartYear INTEGER,
                StartMonth INTEGER,
                StartJDay INTEGER,
                StartDay INTEGER,
                StartHour INTEGER,
                StartMinute INTEGER,
                StartSecond INTEGER,
                StartMSecond REAL,

                EndYear INTEGER,
                EndMonth INTEGER,
                EndJDay INTEGER,
                EndDay INTEGER,
                EndHour INTEGER,
                EndMinute INTEGER,
                EndSecond REAL,
                EndMSecond REAL,

                Solution_FK INTEGER NOT NULL,

                PercentOfLineDone REAL DEFAULT 0,
                SeqProdCount REAL DEFAULT 0,
                PercentOFSeqDone REAL DEFAULT 0,

                Count_All INTEGER DEFAULT 0,

                is_clicked INTEGER DEFAULT 0,
                is_recovered INTEGER DEFAULT 0,
                is_processed INTEGER DEFAULT 0,
                is_fbloaded INTEGER DEFAULT 0,

                MinRadialOffset REAL DEFAULT 0,
                AvgRadialOffset REAL DEFAULT 0,
                MaxRadialOffset REAL DEFAULT 0,

                MinILOffset REAL DEFAULT 0,
                AvgILOffset REAL DEFAULT 0,
                MaxILOffset REAL DEFAULT 0,

                MinXLOffset REAL DEFAULT 0,
                AvgXLOffset REAL DEFAULT 0,
                MaxXLOffset REAL DEFAULT 0,

                MindX REAL DEFAULT 0,
                AvgdX REAL DEFAULT 0,
                MaxdX REAL DEFAULT 0,

                MindY REAL DEFAULT 0,
                AvgdY REAL DEFAULT 0,
                MaxdY REAL DEFAULT 0,

                MinWaterDepth REAL DEFAULT 0,
                AvgWaterDepth REAL DEFAULT 0,
                MaxWaterDepth REAL DEFAULT 0,

                MinElevation REAL DEFAULT 0,
                AvgElevation REAL DEFAULT 0,
                MaxElevation REAL DEFAULT 0,

                FOREIGN KEY (PPLine_FK)
                    REFERENCES RLPreplot(ID)
                    ON DELETE CASCADE,

                FOREIGN KEY (Solution_FK)
                    REFERENCES Solutions(ID)
                    ON DELETE CASCADE,

                FOREIGN KEY (File_FK)
                    REFERENCES SPS_Files(ID)
                    ON DELETE CASCADE,

                FOREIGN KEY (FileName_FK)
                    REFERENCES SPS_Files(ID)
                    ON DELETE CASCADE,

                FOREIGN KEY (Vessel_FK)
                    REFERENCES project_fleet(id)
                    ON DELETE SET NULL,

                UNIQUE(Line, Solution_FK)
            )
        """)

    def ensure_rpsolution_table(self, conn):
        """
        Current RPSolution schema.

        Does not drop existing table.
        Adds Vessel_FK and stores calculated IL / XL offsets.
        """

        if self._table_exists(conn, "RPSolution"):
            return

        conn.execute("""
            CREATE TABLE RPSolution (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,

                LineName_FK INTEGER,
                Line INTEGER,

                PP_Point_FK INTEGER,
                PP_Line_FK INTEGER,

                File_FK INTEGER,
                Solution_FK INTEGER,

                Tier INTEGER DEFAULT 0,
                TierLinePoint INTEGER DEFAULT 0,

                LinePoint INTEGER DEFAULT 0,
                LinePointIdx INTEGER DEFAULT 0,
                LinePointIdxSol INTEGER NOT NULL,

                Point INTEGER DEFAULT 0,
                PointIdx INTEGER,

                FireCode TEXT,

                Seq INTEGER DEFAULT 1,

                ArrayNumber INTEGER,
                FCodeIdx INTEGER DEFAULT 0,

                PointCode TEXT,

                Static REAL DEFAULT 0,
                PointDepth REAL DEFAULT 0,

                Datum INTEGER DEFAULT 0,
                Uphole REAL DEFAULT 0,

                WaterDepth INTEGER DEFAULT 0,

                Easting REAL DEFAULT 0,
                Northing REAL DEFAULT 0,
                Elevation REAL DEFAULT 0,

                JDay INTEGER DEFAULT 0,
                Hour INTEGER DEFAULT 0,
                Minute INTEGER DEFAULT 0,
                Second INTEGER DEFAULT 0,
                Msecond REAL DEFAULT 0,

                Month INTEGER DEFAULT 0,
                Week INTEGER DEFAULT 0,
                Day INTEGER DEFAULT 0,
                Year INTEGER DEFAULT 0,

                TimeStamp TEXT,
                Date DATETIME,
                YearDay TEXT,

                Vessel TEXT,
                Vessel_FK INTEGER,

                RadialOffset REAL DEFAULT 0,
                ILOffset REAL DEFAULT 0,
                XLOffset REAL DEFAULT 0,

                isCompared INTEGER DEFAULT 0,
                isInSpec INTEGER DEFAULT 0,
                isILInSpec INTEGER DEFAULT 0,
                isXLInSpec INTEGER DEFAULT 0,

                PP_X REAL DEFAULT 0,
                PP_Y REAL DEFAULT 0,

                dX REAL DEFAULT 0,
                dY REAL DEFAULT 0,

                isPreplotCompared INTEGER DEFAULT 0,

                NODE_ID TEXT,

                DEPLOY INTEGER DEFAULT 1,
                RPI INTEGER DEFAULT 1,

                REC_X REAL DEFAULT 0,
                REC_Y REAL DEFAULT 0,
                REC_Z REAL DEFAULT 0,

                NEARILIN INTEGER DEFAULT 0,
                NEARXLIN INTEGER DEFAULT 0,

                TIMECORR REAL DEFAULT 0,
                BULKSHIFT REAL DEFAULT 0,
                TIMINGEQ REAL DEFAULT 0,
                QDRIFT REAL DEFAULT 0,
                LDRIFT REAL DEFAULT 0,

                TRIMPTCH REAL DEFAULT 0,
                TRIMROLL REAL DEFAULT 0,
                TRIMYAW REAL DEFAULT 0,

                PITCHFIN REAL DEFAULT 0,
                ROLLFIN REAL DEFAULT 0,
                YAWFIN REAL DEFAULT 0,

                TOTDAYS REAL DEFAULT 0,

                NODSTART INTEGER DEFAULT 0,
                DEPLOYTM INTEGER DEFAULT 0,
                PICKUPTM INTEGER DEFAULT 0,
                RUNTIME INTEGER DEFAULT 0,

                EC2_CD1 INTEGER DEFAULT 0,
                CLKFLAG INTEGER DEFAULT 0,

                EC1_RUS0 REAL DEFAULT 0,
                EC1_RUS1 REAL DEFAULT 0,
                EC1_EDT0 REAL DEFAULT 0,
                EC1_EDT1 REAL DEFAULT 0,
                EC1_EPT0 REAL DEFAULT 0,
                EC1_EPT1 REAL DEFAULT 0,

                TOTSHOTS INTEGER DEFAULT 0,
                TOTPROD INTEGER DEFAULT 0,
                SPSK INTEGER DEFAULT 0,

                Spare1 INTEGER DEFAULT 0,
                Spare2 INTEGER DEFAULT 0,
                Spare3 INTEGER DEFAULT 0,

                UNIQUE(LinePointIdxSol),

                FOREIGN KEY (LineName_FK)
                    REFERENCES RLSolution(ID)
                    ON DELETE CASCADE,

                FOREIGN KEY (PP_Point_FK)
                    REFERENCES RPPreplot(ID)
                    ON DELETE CASCADE,

                FOREIGN KEY (Solution_FK)
                    REFERENCES Solutions(ID)
                    ON DELETE CASCADE,

                FOREIGN KEY (PP_Line_FK)
                    REFERENCES RLPreplot(ID)
                    ON DELETE CASCADE,

                FOREIGN KEY (File_FK)
                    REFERENCES SPS_Files(ID)
                    ON DELETE CASCADE,

                FOREIGN KEY (Vessel_FK)
                    REFERENCES project_fleet(id)
                    ON DELETE SET NULL
            )
        """)

    def ensure_sps_files_table(self, conn):
        conn.execute("""
            CREATE TABLE IF NOT EXISTS SPS_Files (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                FileName TEXT NOT NULL UNIQUE,
                FileSize INTEGER DEFAULT 0,
                FileType TEXT DEFAULT 'NOAR_R_SPS',
                CreatedAt TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def ensure_indexes(self, conn):
        indexes = [
            """
            CREATE INDEX IF NOT EXISTS idx_sps_files_filename
            ON SPS_Files(FileName)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_rlsolution_line_solution
            ON RLSolution(Line, Solution_FK)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_rlsolution_vessel_fk
            ON RLSolution(Vessel_FK)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_rpsolution_line_solution
            ON RPSolution(Line, Solution_FK)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_rpsolution_line_point
            ON RPSolution(Line, Point)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_rpsolution_linename_fk
            ON RPSolution(LineName_FK)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_rpsolution_vessel_fk
            ON RPSolution(Vessel_FK)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_rppreplot_line_point
            ON RPPreplot(Line_Fk, Point)
            """,
        ]

        for sql in indexes:
            conn.execute(sql)

    def _parse_record(
        self,
        *,
        raw_line: str,
        revision,
        year: int,
        tier: int,
        solution_fk: int,
        file_id: int,
        vessel_name: str,
        node_vessel_fk: int | None,
        line_mask: str | None,
    ) -> dict | None:

        def txt(start, end):
            return raw_line[start:end].strip()

        def to_int(value, default=0):
            try:
                if value in ("", None):
                    return default
                return int(float(str(value).strip()))
            except Exception:
                return default

        def to_float(value, default=0.0):
            try:
                if value in ("", None):
                    return default
                return float(str(value).strip())
            except Exception:
                return default

        line_name = txt(revision.line_start, revision.line_end)

        if not line_name:
            return None

        line, seq, attempt = self._parse_line_name(line_name, line_mask)

        if line <= 0:
            return None

        point = to_int(txt(revision.point_start, revision.point_end))
        point_idx = to_int(txt(revision.point_idx_start, revision.point_idx_end))
        point_code = txt(revision.point_code_start, revision.point_code_end)

        jday = to_int(txt(revision.jday_start, revision.jday_end))
        hour = to_int(txt(revision.hour_start, revision.hour_end))
        minute = to_int(txt(revision.minute_start, revision.minute_end))
        second = to_int(txt(revision.second_start, revision.second_end))
        msecond = to_float(txt(revision.msecond_start, revision.msecond_end))

        try:
            base_date = datetime(year, 1, 1) + timedelta(days=jday - 1)
            timestamp = (
                base_date
                + timedelta(hours=hour)
                + timedelta(minutes=minute)
                + timedelta(seconds=second)
                + timedelta(milliseconds=msecond)
            )
        except Exception:
            base_date = datetime(year, 1, 1)
            timestamp = base_date

        line_point, tier_line_point, line_point_idx, line_point_idx_sol = (
            self._build_receiver_keys(
                line=line,
                point=point,
                point_idx=point_idx,
                tier=tier,
                solution_fk=solution_fk,
                line_mask=line_mask,
            )
        )

        return {
            "LineName_FK": None,
            "Line": line,
            "PP_Point_FK": None,
            "PP_Line_FK": None,
            "File_FK": file_id,
            "Solution_FK": int(solution_fk),

            "Tier": int(tier),
            "TierLinePoint": tier_line_point,
            "LinePoint": line_point,
            "LinePointIdx": line_point_idx,
            "LinePointIdxSol": line_point_idx_sol,

            "Point": point,
            "PointIdx": point_idx,
            "FireCode": point_code[:1],
            "Seq": seq,
            "ArrayNumber": None,
            "FCodeIdx": 0,
            "PointCode": point_code,

            "Static": to_float(txt(revision.static_start, revision.static_end)),
            "PointDepth": to_float(txt(revision.point_depth_start, revision.point_depth_end)),
            "Datum": to_int(txt(revision.datum_start, revision.datum_end)),
            "Uphole": to_float(txt(revision.uphole_start, revision.uphole_end)),
            "WaterDepth": to_int(txt(revision.water_depth_start, revision.water_depth_end)),
            "Easting": to_float(txt(revision.easting_start, revision.easting_end)),
            "Northing": to_float(txt(revision.northing_start, revision.northing_end)),
            "Elevation": to_float(txt(revision.elevation_start, revision.elevation_end)),

            "JDay": jday,
            "Hour": hour,
            "Minute": minute,
            "Second": second,
            "Msecond": msecond,

            "Month": timestamp.month,
            "Week": int(timestamp.strftime("%W")),
            "Day": timestamp.day,
            "Year": int(year),
            "Date": base_date.strftime("%Y-%m-%d"),
            "TimeStamp": timestamp.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "YearDay": f"{year}{jday:03d}",

            "Vessel": vessel_name,
            "Vessel_FK": node_vessel_fk,

            "RadialOffset": 0,
            "ILOffset": 0,
            "XLOffset": 0,
            "isCompared": 0,
            "isInSpec": 0,
            "isILInSpec": 0,
            "isXLInSpec": 0,

            "PP_X": 0,
            "PP_Y": 0,
            "dX": 0,
            "dY": 0,
            "isPreplotCompared": 0,

            "NODE_ID": None,
            "DEPLOY": 1,
            "RPI": 1,
            "REC_X": 0,
            "REC_Y": 0,
            "REC_Z": 0,

            "NEARILIN": 0,
            "NEARXLIN": 0,
            "TIMECORR": 0,
            "BULKSHIFT": 0,
            "TIMINGEQ": 0,
            "QDRIFT": 0,
            "LDRIFT": 0,
            "TRIMPTCH": 0,
            "TRIMROLL": 0,
            "TRIMYAW": 0,
            "PITCHFIN": 0,
            "ROLLFIN": 0,
            "YAWFIN": 0,
            "TOTDAYS": 0,
            "NODSTART": 0,
            "DEPLOYTM": 0,
            "PICKUPTM": 0,
            "RUNTIME": 0,
            "EC2_CD1": 0,
            "CLKFLAG": 0,
            "EC1_RUS0": 0,
            "EC1_RUS1": 0,
            "EC1_EDT0": 0,
            "EC1_EDT1": 0,
            "EC1_EPT0": 0,
            "EC1_EPT1": 0,
            "TOTSHOTS": 0,
            "TOTPROD": 0,
            "SPSK": 0,
            "Spare1": 0,
            "Spare2": 0,
            "Spare3": 0,

            "Attempt": attempt,
        }

    def _build_receiver_keys(
        self,
        *,
        line: int,
        point: int,
        point_idx: int,
        tier: int,
        solution_fk: int,
        line_mask: str,
    ):
        if not line_mask:
            raise ValueError("Receiver line mask is required.")

        line_digits = line_mask.count("L")
        point_digits = line_mask.count("P")

        if line_digits <= 0 or point_digits <= 0:
            raise ValueError(f"Invalid receiver line mask: {line_mask}")

        point_scalar = 10 ** point_digits
        tier_scalar = 10 ** (line_digits + point_digits)

        point_idx_digits = max(1, len(str(abs(int(point_idx or 0)))))
        point_idx_scalar = 10 ** point_idx_digits

        solution_digits = max(1, len(str(abs(int(solution_fk or 0)))))
        solution_scalar = 10 ** solution_digits

        line_point = int(line) * point_scalar + int(point)
        tier_line_point = int(tier) * tier_scalar + line_point
        line_point_idx = line_point * point_idx_scalar + int(point_idx)
        line_point_idx_sol = line_point_idx * solution_scalar + int(solution_fk)

        return line_point, tier_line_point, line_point_idx, line_point_idx_sol

    def _parse_line_name(self, line_name: str, line_mask: str | None):
        if not line_mask:
            return 0, 1, ""

        if "L" not in line_mask:
            return 0, 1, ""

        start = line_mask.index("L")
        end = line_mask.rfind("L") + 1

        line_txt = line_name[start:end].strip()

        try:
            line = int(line_txt)
        except Exception:
            line = 0

        return line, 1, ""

    def _flush_rpsolution(self, conn, rows):
        columns = [
            "LineName_FK",
            "Line",
            "PP_Point_FK",
            "PP_Line_FK",
            "File_FK",
            "Solution_FK",
            "Tier",
            "TierLinePoint",
            "LinePoint",
            "LinePointIdx",
            "LinePointIdxSol",
            "Point",
            "PointIdx",
            "FireCode",
            "Seq",
            "ArrayNumber",
            "FCodeIdx",
            "PointCode",
            "Static",
            "PointDepth",
            "Datum",
            "Uphole",
            "WaterDepth",
            "Easting",
            "Northing",
            "Elevation",
            "JDay",
            "Hour",
            "Minute",
            "Second",
            "Msecond",
            "Month",
            "Week",
            "Day",
            "Year",
            "TimeStamp",
            "Date",
            "YearDay",
            "Vessel",
            "Vessel_FK",
            "RadialOffset",
            "ILOffset",
            "XLOffset",
            "isCompared",
            "isInSpec",
            "isILInSpec",
            "isXLInSpec",
            "PP_X",
            "PP_Y",
            "dX",
            "dY",
            "isPreplotCompared",
            "NODE_ID",
            "DEPLOY",
            "RPI",
            "REC_X",
            "REC_Y",
            "REC_Z",
            "NEARILIN",
            "NEARXLIN",
            "TIMECORR",
            "BULKSHIFT",
            "TIMINGEQ",
            "QDRIFT",
            "LDRIFT",
            "TRIMPTCH",
            "TRIMROLL",
            "TRIMYAW",
            "PITCHFIN",
            "ROLLFIN",
            "YAWFIN",
            "TOTDAYS",
            "NODSTART",
            "DEPLOYTM",
            "PICKUPTM",
            "RUNTIME",
            "EC2_CD1",
            "CLKFLAG",
            "EC1_RUS0",
            "EC1_RUS1",
            "EC1_EDT0",
            "EC1_EDT1",
            "EC1_EPT0",
            "EC1_EPT1",
            "TOTSHOTS",
            "TOTPROD",
            "SPSK",
            "Spare1",
            "Spare2",
            "Spare3",
        ]

        sql = f"""
            INSERT OR REPLACE INTO RPSolution (
                {",".join(columns)}
            )
            VALUES (
                {",".join(["?"] * len(columns))}
            )
        """

        values = [
            tuple(row.get(col) for col in columns)
            for row in rows
        ]

        conn.executemany(sql, values)

    def _flush_rlsolution(self, conn, line_stats):
        columns = [
            "PPLine_FK",
            "File_FK",
            "FileName_FK",
            "LineName",
            "Line",
            "Seq",
            "Attempt",
            "Tier",
            "TierLine",
            "FRP",
            "LRP",
            "PP_Count",
            "StartX",
            "StartY",
            "EndX",
            "EndY",
            "Vessel",
            "Vessel_FK",
            "StartYear",
            "StartMonth",
            "StartJDay",
            "StartDay",
            "StartHour",
            "StartMinute",
            "StartSecond",
            "StartMSecond",
            "EndYear",
            "EndMonth",
            "EndJDay",
            "EndDay",
            "EndHour",
            "EndMinute",
            "EndSecond",
            "EndMSecond",
            "Solution_FK",
            "PercentOfLineDone",
            "SeqProdCount",
            "PercentOFSeqDone",
            "Count_All",
            "is_recovered",
            "is_processed",
            "MinRadialOffset",
            "AvgRadialOffset",
            "MaxRadialOffset",
            "MinILOffset",
            "AvgILOffset",
            "MaxILOffset",
            "MinXLOffset",
            "AvgXLOffset",
            "MaxXLOffset",
            "MindX",
            "AvgdX",
            "MaxdX",
            "MindY",
            "AvgdY",
            "MaxdY",
            "MinWaterDepth",
            "AvgWaterDepth",
            "MaxWaterDepth",
            "MinElevation",
            "AvgElevation",
            "MaxElevation",
        ]

        update_columns = [
            col
            for col in columns
            if col not in ("Line", "Solution_FK")
        ]

        update_set = ",\n".join([f"{col} = ?" for col in update_columns])
        insert_cols = ", ".join(columns)
        insert_marks = ", ".join(["?"] * len(columns))

        for stat in line_stats.values():
            stat["FileName_FK"] = stat["File_FK"]
            stat["LineName"] = str(stat["Line"])

            existing = conn.execute("""
                SELECT ID
                FROM RLSolution
                WHERE Line = ? AND Solution_FK = ?
            """, (
                stat["Line"],
                stat["Solution_FK"],
            )).fetchone()

            if existing:
                rl_id = existing["ID"]

                update_values = tuple(stat.get(col) for col in update_columns)

                conn.execute(f"""
                    UPDATE RLSolution
                    SET {update_set}
                    WHERE ID = ?
                """, update_values + (rl_id,))

            else:
                values = tuple(stat.get(col) for col in columns)

                cur = conn.execute(f"""
                    INSERT INTO RLSolution (
                        {insert_cols}
                    )
                    VALUES (
                        {insert_marks}
                    )
                """, values)

                rl_id = cur.lastrowid

            conn.execute("""
                UPDATE RPSolution
                SET LineName_FK = ?
                WHERE Line = ? AND Solution_FK = ?
            """, (
                rl_id,
                stat["Line"],
                stat["Solution_FK"],
            ))

    def _update_line_stat(self, line_stats, row, pp_line, solution_fk):
        def _safe_float(value):
            try:
                if value is None or value == "":
                    return None
                return float(value)
            except Exception:
                return None

        def _min_avg_max(values):
            clean = [v for v in values if v is not None]

            if not clean:
                return 0, 0, 0

            return (
                round(min(clean), 3),
                round(sum(clean) / len(clean), 3),
                round(max(clean), 3),
            )

        line = row["Line"]

        if line not in line_stats:
            line_stats[line] = {
                "Line": line,
                "PPLine_FK": pp_line["ID"],
                "File_FK": row["File_FK"],
                "FileName_FK": row["File_FK"],
                "Solution_FK": int(solution_fk),

                "Seq": row["Seq"],
                "Attempt": row.get("Attempt", ""),
                "Tier": row["Tier"],
                "TierLine": pp_line.get("TierLine"),

                "FRP": row["Point"],
                "LRP": row["Point"],

                "PP_Count": pp_line.get("Points", 0) or 0,

                "StartX": row["Easting"],
                "StartY": row["Northing"],
                "EndX": row["Easting"],
                "EndY": row["Northing"],

                "Vessel": row["Vessel"],
                "Vessel_FK": row.get("Vessel_FK"),

                "StartYear": row["Year"],
                "StartMonth": row["Month"],
                "StartJDay": row["JDay"],
                "StartDay": row["Day"],
                "StartHour": row["Hour"],
                "StartMinute": row["Minute"],
                "StartSecond": row["Second"],
                "StartMSecond": row["Msecond"],

                "EndYear": row["Year"],
                "EndMonth": row["Month"],
                "EndJDay": row["JDay"],
                "EndDay": row["Day"],
                "EndHour": row["Hour"],
                "EndMinute": row["Minute"],
                "EndSecond": row["Second"],
                "EndMSecond": row["Msecond"],

                "Count_All": 0,

                "PercentOfLineDone": 0,
                "SeqProdCount": 0,
                "PercentOFSeqDone": 0,

                "is_recovered": 0,
                "is_processed": 0,

                "_radial_values": [],
                "_il_values": [],
                "_xl_values": [],
                "_dx_values": [],
                "_dy_values": [],
                "_water_depth_values": [],
                "_elevation_values": [],

                "MinRadialOffset": 0,
                "AvgRadialOffset": 0,
                "MaxRadialOffset": 0,

                "MinILOffset": 0,
                "AvgILOffset": 0,
                "MaxILOffset": 0,

                "MinXLOffset": 0,
                "AvgXLOffset": 0,
                "MaxXLOffset": 0,

                "MindX": 0,
                "AvgdX": 0,
                "MaxdX": 0,

                "MindY": 0,
                "AvgdY": 0,
                "MaxdY": 0,

                "MinWaterDepth": 0,
                "AvgWaterDepth": 0,
                "MaxWaterDepth": 0,

                "MinElevation": 0,
                "AvgElevation": 0,
                "MaxElevation": 0,
            }

        stat = line_stats[line]

        stat["Count_All"] += 1
        stat["SeqProdCount"] = stat["Count_All"]

        point = row["Point"]

        if point < stat["FRP"]:
            stat["FRP"] = point
            stat["StartX"] = row["Easting"]
            stat["StartY"] = row["Northing"]

            stat["StartYear"] = row["Year"]
            stat["StartMonth"] = row["Month"]
            stat["StartJDay"] = row["JDay"]
            stat["StartDay"] = row["Day"]
            stat["StartHour"] = row["Hour"]
            stat["StartMinute"] = row["Minute"]
            stat["StartSecond"] = row["Second"]
            stat["StartMSecond"] = row["Msecond"]

        if point > stat["LRP"]:
            stat["LRP"] = point
            stat["EndX"] = row["Easting"]
            stat["EndY"] = row["Northing"]

            stat["EndYear"] = row["Year"]
            stat["EndMonth"] = row["Month"]
            stat["EndJDay"] = row["JDay"]
            stat["EndDay"] = row["Day"]
            stat["EndHour"] = row["Hour"]
            stat["EndMinute"] = row["Minute"]
            stat["EndSecond"] = row["Second"]
            stat["EndMSecond"] = row["Msecond"]

        for list_key, row_key in [
            ("_radial_values", "RadialOffset"),
            ("_il_values", "ILOffset"),
            ("_xl_values", "XLOffset"),
            ("_dx_values", "dX"),
            ("_dy_values", "dY"),
            ("_water_depth_values", "WaterDepth"),
            ("_elevation_values", "Elevation"),
        ]:
            value = _safe_float(row.get(row_key))

            if value is not None:
                stat[list_key].append(value)

        (
            stat["MinRadialOffset"],
            stat["AvgRadialOffset"],
            stat["MaxRadialOffset"],
        ) = _min_avg_max(stat["_radial_values"])

        (
            stat["MinILOffset"],
            stat["AvgILOffset"],
            stat["MaxILOffset"],
        ) = _min_avg_max(stat["_il_values"])

        (
            stat["MinXLOffset"],
            stat["AvgXLOffset"],
            stat["MaxXLOffset"],
        ) = _min_avg_max(stat["_xl_values"])

        (
            stat["MindX"],
            stat["AvgdX"],
            stat["MaxdX"],
        ) = _min_avg_max(stat["_dx_values"])

        (
            stat["MindY"],
            stat["AvgdY"],
            stat["MaxdY"],
        ) = _min_avg_max(stat["_dy_values"])

        (
            stat["MinWaterDepth"],
            stat["AvgWaterDepth"],
            stat["MaxWaterDepth"],
        ) = _min_avg_max(stat["_water_depth_values"])

        (
            stat["MinElevation"],
            stat["AvgElevation"],
            stat["MaxElevation"],
        ) = _min_avg_max(stat["_elevation_values"])

        planned = stat["PP_Count"]

        if planned:
            stat["PercentOfLineDone"] = round(
                stat["Count_All"] / planned * 100.0,
                2,
            )

            stat["PercentOFSeqDone"] = stat["PercentOfLineDone"]

    def _load_rlpreplot_lookup(self, conn):
        rows = conn.execute("""
            SELECT
                ID,
                Line,
                Points,
                TierLine,
                LineBearing,
                CalcLineBearing
            FROM RLPreplot
        """).fetchall()

        return {
            int(row["Line"]): dict(row)
            for row in rows
        }

    def _load_rppreplot_lookup(self, conn):
        rows = conn.execute("""
            SELECT ID, Line_Fk, Point, X, Y
            FROM RPPreplot
        """).fetchall()

        return {
            (int(row["Line_Fk"]), int(row["Point"])): dict(row)
            for row in rows
        }

    def _get_or_create_sps_file(self, conn, file_path):
        file_name = os.path.basename(file_path)

        try:
            file_size = os.path.getsize(file_path)
        except OSError:
            file_size = 0

        row = conn.execute("""
            SELECT ID
            FROM SPS_Files
            WHERE FileName = ?
        """, (file_name,)).fetchone()

        if row:
            sps_file_id = row["ID"]

            conn.execute("""
                UPDATE SPS_Files
                SET FileSize = ?,
                    FileType = ?
                WHERE ID = ?
            """, (
                file_size,
                "NOAR_R_SPS",
                sps_file_id,
            ))

            conn.commit()
            return sps_file_id

        cur = conn.execute("""
            INSERT INTO SPS_Files (
                FileName,
                FileSize,
                FileType
            )
            VALUES (?, ?, ?)
        """, (
            file_name,
            file_size,
            "NOAR_R_SPS",
        ))

        conn.commit()
        return cur.lastrowid

    def _get_vessel_name(self, conn, vessel_id):
        row = conn.execute("""
            SELECT vessel_name
            FROM project_fleet
            WHERE id = ?
        """, (
            vessel_id,
        )).fetchone()

        if not row:
            return ""

        return row["vessel_name"]

    def _detect_encoding(self, file_path):
        for enc in (
            "utf-8-sig",
            "utf-8",
            "cp1252",
            "latin1",
        ):
            try:
                with open(file_path, "rt", encoding=enc) as f:
                    f.read(4096)
                return enc
            except UnicodeDecodeError:
                continue

        return "latin1"

    def list_rlsolutions(
        self,
        *,
        search: str = "",
        line_from: int | None = None,
        line_to: int | None = None,
        seq_from: int | None = None,
        seq_to: int | None = None,
        solution_fk: int | None = None,
        sort_by: str = "Line",
        sort_dir: str = "asc",
    ) -> list[dict]:

        conn = self._connect()

        try:
            cur = conn.cursor()

            allowed_sort = {
                "Line": "rl.Line",
                "LineName": "rl.LineName",
                "Seq": "rl.Seq",
                "Attempt": "rl.Attempt",
                "Tier": "rl.Tier",
                "TierLine": "rl.TierLine",
                "FRP": "rl.FRP",
                "LRP": "rl.LRP",
                "Vessel": "rl.Vessel",
                "PP_Count": "rl.PP_Count",
                "Count_All": "rl.Count_All",
                "PercentOfLineDone": "rl.PercentOfLineDone",
                "PercentOFSeqDone": "rl.PercentOFSeqDone",
                "AvgRadialOffset": "rl.AvgRadialOffset",
                "AvgILOffset": "rl.AvgILOffset",
                "AvgXLOffset": "rl.AvgXLOffset",
                "AvgdX": "rl.AvgdX",
                "AvgdY": "rl.AvgdY",
                "AvgWaterDepth": "rl.AvgWaterDepth",
                "AvgElevation": "rl.AvgElevation",
                "Solution": "s.Solution",
                "FileName": "f.FileName",
            }

            sort_column = allowed_sort.get(sort_by, "rl.Line")
            sort_direction = "DESC" if str(sort_dir).lower() == "desc" else "ASC"

            where = []
            params = []

            if search:
                where.append("""
                    (
                        CAST(rl.Line AS TEXT) LIKE ?
                        OR COALESCE(rl.LineName, '') LIKE ?
                        OR COALESCE(rl.Vessel, '') LIKE ?
                        OR COALESCE(s.Solution, '') LIKE ?
                        OR COALESCE(f.FileName, '') LIKE ?
                    )
                """)
                s = f"%{search}%"
                params.extend([s, s, s, s, s])

            if line_from is not None:
                where.append("rl.Line >= ?")
                params.append(line_from)

            if line_to is not None:
                where.append("rl.Line <= ?")
                params.append(line_to)

            if seq_from is not None:
                where.append("rl.Seq >= ?")
                params.append(seq_from)

            if seq_to is not None:
                where.append("rl.Seq <= ?")
                params.append(seq_to)

            if solution_fk is not None:
                where.append("rl.Solution_FK = ?")
                params.append(solution_fk)

            where_sql = ""
            if where:
                where_sql = "WHERE " + " AND ".join(where)

            sql = f"""
                SELECT
                    rl.ID,

                    rl.LineName,
                    rl.Line,
                    rl.Seq,
                    rl.Attempt,
                    rl.Tier,
                    rl.TierLine,

                    rl.FRP,
                    rl.LRP,
                    rl.PP_Count,

                    rl.Vessel,
                    rl.Vessel_FK,
                    pf.vessel_name AS VesselName,

                    rl.StartYear,
                    rl.StartMonth,
                    rl.StartJDay,
                    rl.StartDay,
                    rl.StartHour,
                    rl.StartMinute,
                    rl.StartSecond,
                    rl.StartMSecond,

                    rl.EndYear,
                    rl.EndMonth,
                    rl.EndJDay,
                    rl.EndDay,
                    rl.EndHour,
                    rl.EndMinute,
                    rl.EndSecond,
                    rl.EndMSecond,

                    rl.PercentOfLineDone,
                    rl.SeqProdCount,
                    rl.PercentOFSeqDone,
                    rl.Count_All,

                    rl.is_clicked,
                    rl.is_recovered,
                    rl.is_processed,
                    rl.is_fbloaded,

                    rl.MinRadialOffset,
                    rl.AvgRadialOffset,
                    rl.MaxRadialOffset,

                    rl.MinILOffset,
                    rl.AvgILOffset,
                    rl.MaxILOffset,

                    rl.MinXLOffset,
                    rl.AvgXLOffset,
                    rl.MaxXLOffset,

                    rl.MindX,
                    rl.AvgdX,
                    rl.MaxdX,

                    rl.MindY,
                    rl.AvgdY,
                    rl.MaxdY,

                    rl.MinWaterDepth,
                    rl.AvgWaterDepth,
                    rl.MaxWaterDepth,

                    rl.MinElevation,
                    rl.AvgElevation,
                    rl.MaxElevation,

                    rl.Solution_FK,
                    s.Solution,
                    s.Comments AS SolutionComments,

                    rl.FileName_FK,
                    f.FileName,

                    COALESCE(rl.Count_All, 0) AS PointCount

                FROM RLSolution rl

                LEFT JOIN Solutions s
                    ON s.ID = rl.Solution_FK

                LEFT JOIN SPS_Files f
                    ON f.ID = rl.FileName_FK

                LEFT JOIN project_fleet pf
                    ON pf.id = rl.Vessel_FK

                {where_sql}

                ORDER BY
                    {sort_column} {sort_direction},
                    rl.Seq ASC,
                    rl.Line ASC,
                    rl.ID ASC
            """

            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

        finally:
            conn.close()

    def delete_selected_rlsolutions(self, *, ids, delete_type: str) -> dict:
        ids = [int(x) for x in ids if str(x).isdigit()]

        if not ids:
            return {"rows": 0}

        placeholders = ",".join("?" for _ in ids)

        conn = self._connect()

        try:
            cur = conn.cursor()

            cur.execute(
                f"SELECT DISTINCT Line FROM RLSolution WHERE ID IN ({placeholders})",
                ids,
            )

            lines = [
                row["Line"]
                for row in cur.fetchall()
                if row["Line"] is not None
            ]

            line_placeholders = ",".join("?" for _ in lines) if lines else ""

            deleted = {}

            if delete_type == "all":
                cur.execute(
                    f"DELETE FROM RPSolution WHERE LineName_FK IN ({placeholders})",
                    ids,
                )
                deleted["RPSolution"] = cur.rowcount

                if lines:
                    cur.execute(
                        f"DELETE FROM REC_DB WHERE Line IN ({line_placeholders})",
                        lines,
                    )
                    deleted["REC_DB"] = cur.rowcount

                cur.execute(
                    f"DELETE FROM RLSolution WHERE ID IN ({placeholders})",
                    ids,
                )
                deleted["RLSolution"] = cur.rowcount

            elif delete_type == "nav_log":
                cur.execute(
                    f"DELETE FROM RPSolution WHERE LineName_FK IN ({placeholders})",
                    ids,
                )
                deleted["RPSolution"] = cur.rowcount

            elif delete_type == "rec_db":
                if lines:
                    cur.execute(
                        f"DELETE FROM REC_DB WHERE Line IN ({line_placeholders})",
                        lines,
                    )
                    deleted["REC_DB"] = cur.rowcount
                else:
                    deleted["REC_DB"] = 0

            elif delete_type == "pinger_log":
                cur.execute(
                    f"""
                    UPDATE RLSolution
                    SET is_fbloaded = 0
                    WHERE ID IN ({placeholders})
                    """,
                    ids,
                )
                deleted["RLSolution_updated"] = cur.rowcount

            elif delete_type == "sm_deployed":
                cur.execute(
                    f"""
                    UPDATE RLSolution
                    SET is_clicked = 0
                    WHERE ID IN ({placeholders})
                    """,
                    ids,
                )
                deleted["RLSolution_updated"] = cur.rowcount

            elif delete_type == "sm_recovered":
                cur.execute(
                    f"""
                    UPDATE RLSolution
                    SET is_recovered = 0
                    WHERE ID IN ({placeholders})
                    """,
                    ids,
                )
                deleted["RLSolution_updated"] = cur.rowcount

            else:
                raise ValueError(f"Unknown delete_type: {delete_type}")

            conn.commit()
            return deleted

        except Exception:
            conn.rollback()
            raise

        finally:
            conn.close()