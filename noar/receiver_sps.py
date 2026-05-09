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
                            parsed["dX"] = pp_point["X"] - parsed["Easting"]
                            parsed["dY"] = pp_point["Y"] - parsed["Northing"]
                            parsed["isPreplotCompared"] = 1

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
        self.ensure_sps_files_table(conn)

        self.recreate_rlsolution_table(conn)
        self.recreate_rpsolution_table(conn)

        self.ensure_indexes(conn)

    def recreate_rlsolution_table(self, conn):

        conn.execute("DROP TABLE IF EXISTS RLSolution")

        conn.execute("""
            CREATE TABLE RLSolution (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,

                PPLine_FK INTEGER,
                File_FK INTEGER,

                LineName TEXT,
                Line INTEGER,
                LineSolution INTEGER NOT NULL,

                Seq INTEGER DEFAULT 1,
                Attempt TEXT,

                Tier INTEGER,
                TierLine INTEGER,

                FRP INTEGER,
                LRP INTEGER,

                StartX REAL DEFAULT 0,
                StartY REAL DEFAULT 0,

                EndX REAL DEFAULT 0,
                EndY REAL DEFAULT 0,

                SRP INTEGER,
                ERP INTEGER,

                Vessel TEXT,

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

                Solution_FK INTEGER,

                PercentOfLineDone REAL,
                SeqProdCount REAL,
                PercentOFSeqDone REAL,

                Count_All INTEGER DEFAULT 0,

                is_clicked INTEGER DEFAULT 0,
                is_recovered INTEGER DEFAULT 0,
                is_fbloaded INTEGER DEFAULT 0,

                FileName_FK INTEGER,

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

                UNIQUE(LineSolution),
                UNIQUE(ID, LineSolution)
            )
        """)

    def recreate_rpsolution_table(self, conn):

        conn.execute("DROP TABLE IF EXISTS RPSolution")

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
                UNIQUE(ID, LinePointIdxSol),

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
                    ON DELETE CASCADE
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
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sps_files_filename
            ON SPS_Files(FileName)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_rlsolution_line_solution
            ON RLSolution(Line, Solution_FK)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_rpsolution_line_solution
            ON RPSolution(Line, Solution_FK)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_rpsolution_line_point
            ON RPSolution(Line, Point)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_rppreplot_line_point
            ON RPPreplot(Line_Fk, Point)
        """)

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

        line_point = line * 1_000_000 + point
        tier_line_point = tier * 1_000_000_000_000 + line_point
        line_point_idx = line_point * 10 + point_idx
        line_point_idx_sol = line_point_idx * 10 + int(solution_fk)

        return {
            "LineName_FK": 0,
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

    def _parse_line_name(self, line_name: str, line_mask: str | None):
        """
        Receiver mask example:
            AAAAALLLLPPPPEE

        A = prefix
        L = line number
        P = point number
        E = suffix

        For receiver SPS loading we use:
            L -> Line

        Point still comes from SPSRevision point_start/point_end,
        not from mask.
        """

        if not line_mask:
            return 0, 1, ""

        def extract(mask_char, default=""):
            if mask_char not in line_mask:
                return default

            start = line_mask.index(mask_char)
            end = line_mask.rfind(mask_char) + 1

            if start >= len(line_name):
                return default

            return line_name[start:end].strip()

        line_txt = extract("L", "0")
        suffix = extract("E", "")

        try:
            line = int(line_txt)
        except Exception:
            line = 0

        seq = 1
        attempt = suffix

        return line, seq, attempt

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
        for stat in line_stats.values():

            line_solution = stat["Line"] * 10 + stat["Solution_FK"]

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

                conn.execute("""
                    UPDATE RLSolution
                    SET
                        PPLine_FK = ?,
                        File_FK = ?,
                        LineName = ?,
                        LineSolution = ?,
                        Seq = ?,
                        Attempt = ?,
                        Tier = ?,
                        FRP = ?,
                        LRP = ?,
                        StartX = ?,
                        StartY = ?,
                        EndX = ?,
                        EndY = ?,
                        SRP = ?,
                        ERP = ?,
                        Vessel = ?,
                        StartYear = ?,
                        StartMonth = ?,
                        StartJDay = ?,
                        StartDay = ?,
                        StartHour = ?,
                        StartMinute = ?,
                        StartSecond = ?,
                        StartMSecond = ?,
                        EndYear = ?,
                        EndMonth = ?,
                        EndJDay = ?,
                        EndDay = ?,
                        EndHour = ?,
                        EndMinute = ?,
                        EndSecond = ?,
                        EndMSecond = ?,
                        PercentOfLineDone = ?,
                        Count_All = ?,
                        FileName_FK = ?
                    WHERE ID = ?
                """, (
                    stat["PPLine_FK"],
                    stat["File_FK"],
                    str(stat["Line"]),
                    line_solution,
                    stat["Seq"],
                    stat["Attempt"],
                    stat["Tier"],
                    stat["FRP"],
                    stat["LRP"],
                    stat["StartX"],
                    stat["StartY"],
                    stat["EndX"],
                    stat["EndY"],
                    stat["SRP"],
                    stat["ERP"],
                    stat["Vessel"],
                    stat["StartYear"],
                    stat["StartMonth"],
                    stat["StartJDay"],
                    stat["StartDay"],
                    stat["StartHour"],
                    stat["StartMinute"],
                    stat["StartSecond"],
                    stat["StartMSecond"],
                    stat["EndYear"],
                    stat["EndMonth"],
                    stat["EndJDay"],
                    stat["EndDay"],
                    stat["EndHour"],
                    stat["EndMinute"],
                    stat["EndSecond"],
                    stat["EndMSecond"],
                    stat["PercentOfLineDone"],
                    stat["Count_All"],
                    stat["File_FK"],
                    rl_id,
                ))

            else:
                cur = conn.execute("""
                    INSERT INTO RLSolution (
                        PPLine_FK,
                        File_FK,
                        LineName,
                        Line,
                        LineSolution,
                        Seq,
                        Attempt,
                        Tier,
                        FRP,
                        LRP,
                        StartX,
                        StartY,
                        EndX,
                        EndY,
                        SRP,
                        ERP,
                        Vessel,
                        StartYear,
                        StartMonth,
                        StartJDay,
                        StartDay,
                        StartHour,
                        StartMinute,
                        StartSecond,
                        StartMSecond,
                        EndYear,
                        EndMonth,
                        EndJDay,
                        EndDay,
                        EndHour,
                        EndMinute,
                        EndSecond,
                        EndMSecond,
                        Solution_FK,
                        PercentOfLineDone,
                        Count_All,
                        FileName_FK
                    )
                    VALUES (
                        ?, ?, ?, ?, ?,
                        ?, ?, ?,
                        ?, ?,
                        ?, ?, ?, ?,
                        ?, ?,
                        ?,
                        ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?,
                        ?,
                        ?,
                        ?
                    )
                """, (
                    stat["PPLine_FK"],
                    stat["File_FK"],
                    str(stat["Line"]),
                    stat["Line"],
                    line_solution,
                    stat["Seq"],
                    stat["Attempt"],
                    stat["Tier"],
                    stat["FRP"],
                    stat["LRP"],
                    stat["StartX"],
                    stat["StartY"],
                    stat["EndX"],
                    stat["EndY"],
                    stat["SRP"],
                    stat["ERP"],
                    stat["Vessel"],
                    stat["StartYear"],
                    stat["StartMonth"],
                    stat["StartJDay"],
                    stat["StartDay"],
                    stat["StartHour"],
                    stat["StartMinute"],
                    stat["StartSecond"],
                    stat["StartMSecond"],
                    stat["EndYear"],
                    stat["EndMonth"],
                    stat["EndJDay"],
                    stat["EndDay"],
                    stat["EndHour"],
                    stat["EndMinute"],
                    stat["EndSecond"],
                    stat["EndMSecond"],
                    stat["Solution_FK"],
                    stat["PercentOfLineDone"],
                    stat["Count_All"],
                    stat["File_FK"],
                ))

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
        line = row["Line"]

        if line not in line_stats:
            line_stats[line] = {
                "Line": line,
                "PPLine_FK": pp_line["ID"],
                "File_FK": row["File_FK"],
                "Solution_FK": int(solution_fk),
                "Seq": row["Seq"],
                "Attempt": row.get("Attempt", ""),
                "Tier": row["Tier"],
                "FRP": row["Point"],
                "LRP": row["Point"],
                "SRP": row["Point"],
                "ERP": row["Point"],
                "StartX": row["Easting"],
                "StartY": row["Northing"],
                "EndX": row["Easting"],
                "EndY": row["Northing"],
                "Vessel": row["Vessel"],
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
                "PlannedPoints": pp_line.get("Points", 0) or 0,
                "PercentOfLineDone": 0,
            }

        stat = line_stats[line]

        stat["Count_All"] += 1
        stat["LRP"] = row["Point"]

        if row["Point"] < stat["SRP"]:
            stat["SRP"] = row["Point"]
            stat["StartX"] = row["Easting"]
            stat["StartY"] = row["Northing"]

        if row["Point"] > stat["ERP"]:
            stat["ERP"] = row["Point"]
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

        planned = stat["PlannedPoints"]

        if planned:
            stat["PercentOfLineDone"] = round(
                stat["Count_All"] / planned * 100.0,
                2,
            )

    def _load_rlpreplot_lookup(self, conn):
        rows = conn.execute("""
            SELECT ID, Line, Points
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