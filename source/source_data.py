import csv
import datetime
import io
import re
import sqlite3
from pathlib import Path
from typing import Optional
from core.models import SPSRevision
from core.project_dataclasses import *
class SourceData:
    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def drop_shot_table_indexes(self) -> int:
        """
        Drops all user-created indexes for SHOT_TABLE.
        Returns number of dropped indexes.
        """
        conn = self._connect()
        try:
            cur = conn.cursor()

            # Get all user-created indexes (exclude SQLite auto indexes)
            indexes = cur.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type = 'index'
                  AND tbl_name = 'SHOT_TABLE'
                  AND name NOT LIKE 'sqlite_autoindex%';
            """).fetchall()

            dropped = 0

            for row in indexes:
                index_name = row["name"]
                cur.execute(f'DROP INDEX IF EXISTS "{index_name}"')
                dropped += 1

            conn.commit()
            return dropped

        finally:
            conn.close()

    @staticmethod
    def decode_nav_line(code: str):
        """
        Decode LLLLLXSSSS
        Example: 99999R9036 -> (99999, 'R', 9036)
        """
        if not code:
            return None, None, None

        code = code.strip()
        m = re.match(r'^(\d{5})([A-Za-z0-9])(\d{4})$', code)

        if not m:
            return None, None, None

        line = int(m.group(1))
        attempt = m.group(2)
        seq = int(m.group(3))

        return line, attempt, seq

    def insert_file_record(self, file_name: str, file_type: str | None = None) -> int:
        conn = self._connect()
        try:
            cur = conn.cursor()

            # 1️⃣ Check if file already exists
            cur.execute(
                "SELECT id FROM Files WHERE FileName = ?",
                (file_name,)
            )
            row = cur.fetchone()

            if row:
                # File already exists → reuse ID
                return int(row["id"])

            # 2️⃣ Insert new record
            cur.execute(
                "INSERT INTO Files (FileName) VALUES (?)",
                (file_name,)
            )
            conn.commit()

            file_fk = cur.lastrowid
            if not file_fk:
                raise RuntimeError("Files insert returned empty lastrowid.")

            return int(file_fk)

        finally:
            conn.close()

    def create_shot_table_indexes(self):
        conn = self._connect()
        try:
            cur = conn.cursor()

            cur.executescript("""
            CREATE INDEX IF NOT EXISTS idx_shot_file_fk
            ON SHOT_TABLE (File_FK);

            CREATE INDEX IF NOT EXISTS idx_shot_line
            ON SHOT_TABLE (sail_line);

            CREATE INDEX IF NOT EXISTS idx_shot_line_attempt_seq
            ON SHOT_TABLE (nav_line, attempt, seq);

            CREATE INDEX IF NOT EXISTS idx_shot_seq
            ON SHOT_TABLE (seq);

            CREATE INDEX IF NOT EXISTS idx_shot_time
            ON SHOT_TABLE (shot_year, shot_day);

            CREATE INDEX IF NOT EXISTS idx_shot_xy
            ON SHOT_TABLE (shot_x, shot_y);
            """)

            conn.commit()

        finally:
            conn.close()



    def load_shot_table_h26_stream(self, file_obj, file_fk: int, chunk_size: int = 20000) -> int:

        if not file_fk:
            raise ValueError("file_fk is required")

        conn = self._connect()
        try:
            cur = conn.cursor()

            # Discover real columns in table (excluding id and created_at)
            cur.execute("PRAGMA table_info(SHOT_TABLE)")
            cols = [r["name"] for r in cur.fetchall()]

            # We will insert only columns that exist
            wanted = [
                "File_FK",
                "sail_line", "shot_station", "shot_index", "shot_status",
                "nav_line_code", "nav_line", "attempt", "seq",
                "post_point_code", "fire_code",
                "gun_depth", "water_depth",
                "shot_x", "shot_y",
                "shot_day", "shot_hour", "shot_minute", "shot_second", "shot_microsecond", "shot_year",
                "vessel", "array_id", "source_id",
                "nav_station", "shot_group_id", "elevation",
            ]

            insert_cols = [c for c in wanted if c in cols]
            placeholders = ",".join(["?"] * len(insert_cols))
            col_list = ", ".join(insert_cols)

            insert_sql = f"INSERT INTO SHOT_TABLE ({col_list}) VALUES ({placeholders})"

            def to_int(x):
                x = (x or "").strip()
                if not x:
                    return None
                try:
                    return int(x)
                except ValueError:
                    return None

            def to_float(x):
                x = (x or "").strip()
                if not x:
                    return None
                try:
                    return float(x)
                except ValueError:
                    return None

            # text wrapper if binary
            probe = file_obj.read(0)
            if isinstance(probe, (bytes, bytearray)):
                text_stream = io.TextIOWrapper(file_obj, encoding="utf-8", errors="ignore", newline="")
            else:
                text_stream = file_obj

            reader = csv.reader(text_stream, delimiter=",")

            inserted = 0
            batch = []

            for row in reader:
                if not row:
                    continue

                row = [c.strip() for c in row]
                if not row[0]:
                    continue

                if row[0].startswith(("H", "h")):
                    continue

                if len(row) < 22:
                    continue

                # parse
                first = row[0]
                parts = first.split()
                if len(parts) == 2 and parts[0].upper() == "S":
                    sail_line = to_int(parts[1])
                else:
                    sail_line = to_int(first)

                shot_station = to_int(row[1])
                shot_index = to_int(row[2])
                shot_status = to_int(row[3])

                post_point_code = (row[4] or "").strip() or None
                fire_code = post_point_code[0].upper() if post_point_code else None

                gun_depth = to_float(row[5])
                water_depth = to_float(row[6])

                shot_x = to_float(row[7])
                shot_y = to_float(row[8])

                shot_day = to_int(row[9])
                shot_hour = to_int(row[10])
                shot_minute = to_int(row[11])
                shot_second = to_int(row[12])
                shot_microsecond = to_int(row[13])
                shot_year = to_int(row[14])

                vessel = (row[15] or "").strip() or None
                array_id = (row[16] or "").strip() or None
                source_id = to_int(row[17])

                nav_line_code = (row[18] or "").strip() or None
                nav_line, attempt, seq = self.decode_nav_line(nav_line_code or "")

                nav_station = to_int(row[19])
                shot_group_id = to_int(row[20])
                elevation = to_float(row[21])

                # build dict of parsed values
                data = {
                    "File_FK": int(file_fk),
                    "sail_line": sail_line,
                    "shot_station": shot_station,
                    "shot_index": shot_index,
                    "shot_status": shot_status,
                    "nav_line_code": nav_line_code,
                    "nav_line": nav_line,
                    "attempt": attempt,
                    "seq": seq,
                    "post_point_code": post_point_code,
                    "fire_code": fire_code,
                    "gun_depth": gun_depth,
                    "water_depth": water_depth,
                    "shot_x": shot_x,
                    "shot_y": shot_y,
                    "shot_day": shot_day,
                    "shot_hour": shot_hour,
                    "shot_minute": shot_minute,
                    "shot_second": shot_second,
                    "shot_microsecond": shot_microsecond,
                    "shot_year": shot_year,
                    "vessel": vessel,
                    "array_id": array_id,
                    "source_id": source_id,
                    "nav_station": nav_station,
                    "shot_group_id": shot_group_id,
                    "elevation": elevation,
                }

                # emit tuple in exactly the insert_cols order
                batch.append(tuple(data[c] for c in insert_cols))

                if len(batch) >= chunk_size:
                    cur.executemany(insert_sql, batch)
                    inserted += len(batch)
                    batch.clear()

            if batch:
                cur.executemany(insert_sql, batch)
                inserted += len(batch)

            conn.commit()
            return inserted

        finally:
            try:
                if "text_stream" in locals() and hasattr(text_stream, "detach"):
                    text_stream.detach()
            except Exception:
                pass
            conn.close()

    def create_shot_table(self):
        conn = self._connect()
        try:
            cur = conn.cursor()

            cur.executescript("""
            CREATE TABLE IF NOT EXISTS SHOT_TABLE (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                File_FK INTEGER NOT NULL,

                sail_line INTEGER,
                shot_station INTEGER,
                shot_index INTEGER,
                shot_status INTEGER,

                attempt TEXT,
                seq INTEGER,

                post_point_code TEXT,

                gun_depth REAL,
                water_depth REAL,

                shot_x REAL,
                shot_y REAL,

                shot_day INTEGER,
                shot_hour INTEGER,
                shot_minute INTEGER,
                shot_second INTEGER,
                shot_microsecond INTEGER,
                shot_year INTEGER,

                vessel TEXT,
                array_id TEXT,
                source_id INTEGER,

                nav_line_code TEXT,
                nav_line INTEGER,
                nav_station INTEGER,

                shot_group_id INTEGER,
                elevation REAL,

                created_at TEXT DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (File_FK)
                    REFERENCES Files(id)
                    ON DELETE CASCADE
            );
            """)

            conn.commit()

        finally:
            conn.close()

    def list_shot_table_summary(self) -> list[dict]:
        """
        Returns rows from V_SHOT_TABLE_SUMMARY as list[dict].
        """
        conn = self._connect()
        try:
            cur = conn.cursor()
            rows = cur.execute("""
                SELECT
                    nav_line,
                    attempt,
                    seq,
                    nav_station_count,
                    nav_station_distinct_count,
                    SourceCount,
                    ArraysCount,
                    min_gun_depth,
                    max_gun_depth,
                    min_water_depth,
                    max_water_depth,
                    ProdCount,
                    NonProdCount,
                    KillCount,
                    ProdPercent,
                    NonProdPercent,
                    KillPercent
                FROM V_SHOT_TABLE_SUMMARY
                ORDER BY nav_line, attempt, seq
            """).fetchall()

            return [dict(r) for r in rows]

        finally:
            conn.close()
    def list_sps_files_summary(self) -> list[dict]:
        """
        Returns rows from V_SHOT_TABLE_SUMMARY as list[dict].
        """
        conn = self._connect()
        try:
            cur = conn.cursor()
            rows = cur.execute("""
                SELECT
                    ID,
                    PPLine_FK,
                    File_FK,
                    SailLine,
                    Line,
                    Seq,
                    Attempt,
                    Tier,
                    TierLine,
                    FSP,
                    LSP,
                    FGSP,
                    LGSP,
                    StartX,
                    StartY,
                    EndX,
                    EndY,
                    Vessel_FK,
                    Start_Time,
                    End_Time,
                    LineLength,
                    Start_Production_Time,
                    End_Production_Time,
                    PercentOfLineCompleted,
                    PercentOfSeqCompleted,
                    ProductionCount,
                    NonProductionCount,
                    KillCount,
                    MinGunDepth,
                    MaxGunDepth,
                    MinWaterDepth,
                    MaxWaterDepth,
                    PP_Length,
                    SeqLenPercentage,
                    MaxSPI,
                    MaxSeq
                FROM SLSolution
                ORDER BY Seq, attempt
            """).fetchall()

            return [dict(r) for r in rows]

        finally:
            conn.close()
    #=============================================================================================
    #             LOAD SOURCE SPS
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    def _begin_fast_import(self, aggressive: bool = False) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute("PRAGMA temp_store = MEMORY;")
        cur.execute("PRAGMA cache_size = -200000;")  # ~200MB cache (negative => KB)
        if aggressive:
            cur.execute("PRAGMA journal_mode = OFF;")
            cur.execute("PRAGMA synchronous = OFF;")
        else:
            cur.execute("PRAGMA journal_mode = WAL;")
            cur.execute("PRAGMA synchronous = NORMAL;")

    def _end_fast_import(self) -> None:
        # keep WAL/NORMAL usually; nothing mandatory here
        pass

    def _get_or_create_sl_solution_id(
            self,
            conn: sqlite3.Connection,
            *,
            file_fk: int,
            sail_line: str,
            line: int,
            seq: int,
            attempt: str,
            tier: int,
            vessel_fk: int | None,
    ) -> int:
        sail_line = (sail_line or "").strip()
        attempt = (attempt or "").strip()[:1].upper() or "X"

        # TierLine (simple stable encoding)
        tierline = int(tier) * 100000 + int(line)  # or your own encoding

        row = conn.execute(
            "SELECT ID FROM SLSolution WHERE SailLine=?",
            (sail_line,),
        ).fetchone()
        if row:
            return int(row[0])

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO SLSolution
            (PPLine_FK, File_FK, SailLine, Line, Seq, Attempt, Tier, TierLine, Vessel_FK)
            VALUES (0, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(file_fk), sail_line, int(line), int(seq), attempt, int(tier), int(tierline), vessel_fk),
        )
        return int(cur.lastrowid)

    @staticmethod
    def _detect_text_encoding(sample: bytes) -> str:
        """
            SPS-optimized encoding detection.

                    SPS files are typically ASCII / UTF-8 compatible. We keep detection fast:
                      1) BOM check (utf-8-sig / utf-16)
                      2) strict UTF-8 decode attempt
                      3) fallback to single-byte encoding (cp1252 or cp1251) based on heuristic
                      4) final fallback latin1 (never fails)

                    Returns: encoding name for bytes.decode()
        """

        if not sample:
            return "utf-8"

        # 1) BOM checks (fast and reliable)
        if sample.startswith(b"\xef\xbb\xbf"):
            return "utf-8-sig"
        if sample.startswith(b"\xff\xfe") or sample.startswith(b"\xfe\xff"):
            # Rare for SPS, but handle if it happens
            return "utf-16"

        # 2) Strict UTF-8 test (no errors allowed)
        try:
            sample.decode("utf-8", errors="strict")
            return "utf-8"
        except UnicodeDecodeError:
            pass

        # 3) Heuristic: choose cp1251 if it "looks like" Cyrillic text
        # We decode with cp1251 and count Cyrillic letters.
        try:
            s1251 = sample.decode("cp1251", errors="ignore")
            cyr = sum(1 for ch in s1251 if "\u0400" <= ch <= "\u04FF")  # Cyrillic range
            letters = sum(1 for ch in s1251 if ch.isalpha())

            # If there are some letters and a noticeable share is Cyrillic,
            # it's probably cp1251.
            if letters >= 20 and (cyr / max(letters, 1)) >= 0.2:
                return "cp1251"
        except Exception:
            pass

        # 4) Default single-byte fallback:
        # cp1252 is a good "western" default; latin1 always works too.
        return "cp1252"

    @staticmethod
    def _to_int(s: str, default: Optional[int] = None) -> Optional[int]:
        s = (s or "").strip()
        if not s:
            return default
        try:
            return int(float(s))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _to_float(s: str, default: Optional[float] = None) -> Optional[float]:
        s = (s or "").strip()
        if not s:
            return default
        try:
            return float(s.replace(",", "."))
        except (ValueError, TypeError):
            return default

    def decode_sps_string(
            self,
            s: str,
            *,
            sps_revision,
            geom,
            default: int | None,
            tier: int = 1,
            year: int | None = None,
            line_bearing: float = 0.0,
    ) -> SourceSPSData | None:

        if year is None:
            year = date.today().year

        point_len = geom.sou_point_length
        line_len = geom.sou_line_length
        line_point_len = geom.sou_linepoint_length

        sail_line = (s[sps_revision.line_start:sps_revision.line_end] or "").strip()
        sail_line_mask = (geom.sail_line_mask or "").strip()

        if "L" not in sail_line_mask or "X" not in sail_line_mask or "S" not in sail_line_mask:
            return None

        l_first = sail_line_mask.find("L")
        l_last = sail_line_mask.rfind("L") + 1

        x_first = sail_line_mask.find("X")
        x_last = sail_line_mask.rfind("X") + 1

        s_first = sail_line_mask.find("S")
        s_last = sail_line_mask.rfind("S") + 1

        line = self._to_int(sail_line[l_first:l_last], default=default)
        attempt = (sail_line[x_first:x_last] or "").strip()[:1].upper() or "X"
        seq = self._to_int(sail_line[s_first:s_last], default=default)

        point = self._to_int(s[sps_revision.point_start:sps_revision.point_end], default=default)
        point_depth = self._to_float(s[sps_revision.point_depth_start:sps_revision.point_depth_end], default=default)
        water_depth = self._to_float(s[sps_revision.water_depth_start:sps_revision.water_depth_end], default=default)
        easting = self._to_float(s[sps_revision.easting_start:sps_revision.easting_end], default=default)
        northing = self._to_float(s[sps_revision.northing_start:sps_revision.northing_end], default=default)
        elevation = self._to_float(s[sps_revision.elevation_start:sps_revision.elevation_end], default=default)

        point_code = (s[sps_revision.point_code_start:sps_revision.point_code_end] or "").strip()
        fire_code = (point_code[:1] or "").upper()  # "A" from "A8"
        array_code = self._to_int(point_code[1:2], default=0) if len(point_code) >= 2 else 0

        jday = self._to_int(s[sps_revision.jday_start:sps_revision.jday_end], default=default)
        hour = self._to_int(s[sps_revision.hour_start:sps_revision.hour_end], default=default)
        minute = self._to_int(s[sps_revision.minute_start:sps_revision.minute_end], default=default)
        second = self._to_int(s[sps_revision.second_start:sps_revision.second_end], default=default)
        microsecond = self._to_int(s[sps_revision.msecond_start:sps_revision.msecond_end], default=0)

        point_idx = self._to_int(s[sps_revision.point_idx_start:sps_revision.point_idx_end], default=default)
        if not point_idx:
            point_idx = 1

        line_point = line * point_len + point
        tier_line = tier * line_len + line
        tier_line_point = tier * line_point_len + line_point

        return SourceSPSData(
            sail_line=sail_line,
            line=line,
            attempt=attempt,
            seq=seq,
            tier=tier,

            point_idx=point_idx,
            point=point,
            point_code=point_code,
            fire_code=fire_code,
            array_code=array_code,

            point_depth=point_depth or 0.0,
            water_depth=water_depth or 0.0,

            easting=easting or 0.0,
            northing=northing or 0.0,
            elevation=elevation or 0.0,

            line_point=line_point,
            tier_line_point=tier_line_point,

            jday=jday or 1,
            hour=hour or 0,
            minute=minute or 0,
            second=second or 0,
            microsecond=microsecond or 0,
            year=year,
        )
    def load_shot_table_h26_stream_fast(self, file_obj, file_fk: int, chunk_size: int = 50000) -> int:
        """
        High-speed loader for H26 comma-delimited shot table with padded spaces.
        - big chunks
        - fast pragmas
        - single transaction
        """
        if not file_fk:
            raise ValueError("file_fk is required (NOT NULL)")

        conn = self._connect()
        try:
            self._set_fast_import_pragmas(conn, aggressive=False)

            cur = conn.cursor()
            cur.execute("BEGIN;")  # explicit transaction

            insert_sql = """
            INSERT INTO SHOT_TABLE (
                File_FK,
                sail_line, shot_station, shot_index, shot_status,
                nav_line_code, nav_line, attempt, seq,
                post_point_code, fire_code,
                gun_depth, water_depth,
                shot_x, shot_y,
                shot_day, shot_hour, shot_minute, shot_second, shot_microsecond, shot_year,
                vessel, array_id, source_id,
                nav_station, shot_group_id, elevation
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """

            def to_int(x):
                x = x.strip()
                return int(x) if x else None

            def to_float(x):
                x = x.strip()
                return float(x) if x else None

            # Make sure we read text
            probe = file_obj.read(0)
            if isinstance(probe, (bytes, bytearray)):
                text_stream = io.TextIOWrapper(file_obj, encoding="utf-8", errors="ignore", newline="")
            else:
                text_stream = file_obj

            reader = csv.reader(text_stream, delimiter=",", skipinitialspace=True)

            inserted = 0
            batch = []

            for row in reader:
                if not row:
                    continue

                # Trim only when needed (skipinitialspace already helps)
                # Also: row[0] can include "S       99999"
                first = (row[0] or "").strip()
                if not first:
                    continue

                # skip H headers
                if first[:1] in ("H", "h"):
                    continue

                if len(row) < 22:
                    continue

                # Sail line: "S 99999" or "99999"
                parts = first.split()
                if len(parts) == 2 and parts[0].upper() == "S":
                    sail_line = to_int(parts[1])
                else:
                    sail_line = to_int(first)

                shot_station = to_int((row[1] or "").strip())
                shot_index = to_int((row[2] or "").strip())
                shot_status = to_int((row[3] or "").strip())

                post_point_code = (row[4] or "").strip() or None
                fire_code = post_point_code[0].upper() if post_point_code else None

                gun_depth = to_float((row[5] or "").strip())
                water_depth = to_float((row[6] or "").strip())

                shot_x = to_float((row[7] or "").strip())
                shot_y = to_float((row[8] or "").strip())

                shot_day = to_int((row[9] or "").strip())
                shot_hour = to_int((row[10] or "").strip())
                shot_minute = to_int((row[11] or "").strip())
                shot_second = to_int((row[12] or "").strip())
                shot_microsecond = to_int((row[13] or "").strip())
                shot_year = to_int((row[14] or "").strip())

                vessel = (row[15] or "").strip() or None
                array_id = (row[16] or "").strip() or None
                source_id = to_int((row[17] or "").strip())

                nav_line_code = (row[18] or "").strip() or None
                nav_line, attempt, seq = self.decode_nav_line(nav_line_code or "")

                nav_station = to_int((row[19] or "").strip())
                shot_group_id = to_int((row[20] or "").strip())
                elevation = to_float((row[21] or "").strip())

                batch.append((
                    int(file_fk),
                    sail_line, shot_station, shot_index, shot_status,
                    nav_line_code, nav_line, attempt, seq,
                    post_point_code, fire_code,
                    gun_depth, water_depth,
                    shot_x, shot_y,
                    shot_day, shot_hour, shot_minute, shot_second, shot_microsecond, shot_year,
                    vessel, array_id, source_id,
                    nav_station, shot_group_id, elevation
                ))

                if len(batch) >= chunk_size:
                    cur.executemany(insert_sql, batch)
                    inserted += len(batch)
                    batch.clear()

            if batch:
                cur.executemany(insert_sql, batch)
                inserted += len(batch)

            conn.commit()
            return inserted

        finally:
            try:
                if "text_stream" in locals() and hasattr(text_stream, "detach"):
                    text_stream.detach()
            except Exception:
                pass
            conn.close()

    def load_source_sps_uploaded_file_fast(
            self,
            uploaded_file,  # Django UploadedFile
            *,
            sps_revision,
            geometry,
            vessel_fk: int | None,
            tier: int = 1,
            line_bearing: float = 0.0,
            default: int | None = None,
            year: int | None = None,
            batch_size: int = 50000,
    ) -> dict:

        file_name = uploaded_file.name

        uploaded_file.seek(0)
        sample = uploaded_file.read(4096)
        encoding = self._detect_text_encoding(sample)
        uploaded_file.seek(0)

        file_fk = self.insert_file_record(file_name, file_type="SPS")

        stream = io.TextIOWrapper(uploaded_file.file, encoding=encoding, errors="replace", newline="")

        conn = self._connect()
        try:
            self._begin_fast_import(aggressive=False)
            cur = conn.cursor()
            cur.execute("BEGIN;")

            insert_cols = [
                "SailLine_FK", "PPLine_FK", "Vessel_FK", "File_FK",
                "SailLine", "Line", "Attempt", "Seq", "Tier",
                "TierLinePoint", "LinePoint", "PointIdx", "Point",
                "PointCode", "FireCode", "ArrayCode",
                "PointDepth", "WaterDepth", "Easting", "Northing", "Elevation",
                "JDay", "Hour", "Minute", "Second", "Microsecond",
                "Month", "Week", "Day", "Year", "YearDay",
                "TimeStamp",
            ]
            placeholders = ",".join("?" for _ in insert_cols)

            insert_sql = f"""
            INSERT INTO SPSolution ({",".join(insert_cols)})
            VALUES ({placeholders})
            """

            sl_cache: dict[str, int] = {}

            batch_tuples: list[tuple] = []
            total = 0
            skipped = 0
            lines_touched: set[int] = set()
            pplines_touched: set[int] = set()

            for text_line in stream:
                if not text_line:
                    continue
                if text_line and text_line[0] == "H":
                    continue

                p = self.decode_sps_string(
                    text_line,
                    sps_revision=sps_revision,
                    geom=geometry,
                    default=default,
                    tier=tier,
                    year=year,
                    line_bearing=line_bearing,
                )
                if p is None:
                    skipped += 1
                    continue

                # get/create SLSolution.ID
                sl_id = sl_cache.get(p.sail_line)
                if sl_id is None:
                    sl_id = self._get_or_create_sl_solution_id(
                        conn,
                        file_fk=int(file_fk),
                        sail_line=p.sail_line,
                        line=p.line,
                        seq=p.seq,
                        attempt=p.attempt,
                        tier=p.tier,
                        vessel_fk=vessel_fk,
                    )
                    sl_cache[p.sail_line] = sl_id

                lines_touched.add(sl_id)


                # fill FK/meta into point object
                p.sail_line_fk = sl_id
                p.ppline_fk = 0
                p.vessel_fk = vessel_fk
                p.file_fk = int(file_fk)

                batch_tuples.append(p.to_db_tuple())
                total += 1

                if len(batch_tuples) >= batch_size:
                    cur.executemany(insert_sql, batch_tuples)
                    batch_tuples.clear()

            if batch_tuples:
                cur.executemany(insert_sql, batch_tuples)
                batch_tuples.clear()

            conn.commit()
            self._end_fast_import()

            return {
                "file_fk": int(file_fk),
                "points": int(total),
                "skipped": int(skipped),
                "lines": int(len(lines_touched)),
                "source_line":int(p.line),
            }

        except Exception:
            conn.rollback()
            raise
        finally:
            try:
                if hasattr(stream, "detach"):
                    stream.detach()
            except Exception:
                pass
            conn.close()

    def update_line_maxspi_maxseq(self, line: int, file_fk: int | None = None) -> dict:
        """
        Updates SLSolution.MaxSPI and SLSolution.MaxSeq for a given Line.

        MaxSPI = max distance between neighbouring production points
                 (FireCode in project_geometry.production_code),
                 ordered by Point, PointIdx, computed within each SailLine_FK,
                 then max across all SailLine_FK of this Line.

        MaxSeq = number of distinct SailLine_FK (i.e. SLSolution.ID) for this Line
                 (optionally limited to file_fk).
        """
        line = int(line)

        conn = self._connect()
        try:
            cur = conn.cursor()
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("BEGIN IMMEDIATE;")

            # production codes
            r = cur.execute(
                "SELECT COALESCE(production_code,'') FROM project_geometry LIMIT 1"
            ).fetchone()
            prod_codes = (r[0] if r else "") or ""

            if not prod_codes:
                # still update MaxSeq
                if file_fk is None:
                    maxseq = cur.execute(
                        "SELECT COUNT(*) FROM SLSolution WHERE Line=?",
                        (line,),
                    ).fetchone()[0]
                    cur.execute(
                        "UPDATE SLSolution SET MaxSPI=0, MaxSeq=? WHERE Line=?",
                        (int(maxseq), line),
                    )
                else:
                    fk = int(file_fk)
                    maxseq = cur.execute(
                        "SELECT COUNT(*) FROM SLSolution WHERE Line=? AND File_FK=?",
                        (line, fk),
                    ).fetchone()[0]
                    cur.execute(
                        "UPDATE SLSolution SET MaxSPI=0, MaxSeq=? WHERE Line=? AND File_FK=?",
                        (int(maxseq), line, fk),
                    )

                conn.commit()
                return {"line": line, "MaxSPI": 0.0, "MaxSeq": int(maxseq)}

            fk_filter_sql = ""
            fk_params: tuple = ()
            if file_fk is not None:
                fk = int(file_fk)
                fk_filter_sql = "AND l.File_FK = ?"
                fk_params = (fk,)

            # Compute MaxSPI (SQL window)
            # - filter by Line via JOIN to SLSolution (l.Line)
            # - keep only production points
            # - order by Point, PointIdx within each SailLine_FK
            # - compute distance to previous point
            # - take max gap per sail line, then max across line
            sql_maxspi = f"""
            WITH prod AS (
                SELECT
                    s.SailLine_FK,
                    s.Easting AS x,
                    s.Northing AS y,
                    LAG(s.Easting) OVER (
                        PARTITION BY s.SailLine_FK
                        ORDER BY s.Point, COALESCE(s.PointIdx,0)
                    ) AS px,
                    LAG(s.Northing) OVER (
                        PARTITION BY s.SailLine_FK
                        ORDER BY s.Point, COALESCE(s.PointIdx,0)
                    ) AS py
                FROM SPSolution s
                JOIN SLSolution l ON l.ID = s.SailLine_FK
                WHERE l.Line = ?
                  {fk_filter_sql}
                  AND s.FireCode IS NOT NULL
                  AND instr(?, s.FireCode) > 0
                  AND s.Easting IS NOT NULL
                  AND s.Northing IS NOT NULL
            ),
            gaps AS (
                SELECT
                    SailLine_FK,
                    sqrt((x-px)*(x-px) + (y-py)*(y-py)) AS d
                FROM prod
                WHERE px IS NOT NULL
            )
            SELECT COALESCE(MAX(d), 0) FROM gaps;
            """

            params_maxspi = (line, *fk_params, prod_codes)
            maxspi = cur.execute(sql_maxspi, params_maxspi).fetchone()[0]
            maxspi = float(maxspi or 0.0)

            # MaxSeq = number of SailLine_FK rows for this Line
            if file_fk is None:
                maxseq = cur.execute(
                    "SELECT COUNT(*) FROM SLSolution WHERE Line=?",
                    (line,),
                ).fetchone()[0]
                cur.execute(
                    "UPDATE SLSolution SET MaxSPI=?, MaxSeq=? WHERE Line=?",
                    (maxspi, int(maxseq), line),
                )
            else:
                fk = int(file_fk)
                maxseq = cur.execute(
                    "SELECT COUNT(*) FROM SLSolution WHERE Line=? AND File_FK=?",
                    (line, fk),
                ).fetchone()[0]
                cur.execute(
                    "UPDATE SLSolution SET MaxSPI=?, MaxSeq=? WHERE Line=? AND File_FK=?",
                    (maxspi, int(maxseq), line, fk),
                )

            conn.commit()
            return {"line": line, "MaxSPI": round(maxspi, 3), "MaxSeq": int(maxseq)}

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_slsolution_from_spsolution_timebased(self, file_fk: int) -> str:
        """
        Update SLSolution for one file only (File_FK).

        Definitions (per SailLine_FK == SLSolution.ID):
          FSP  = Point at earliest TimeStamp
          LSP  = Point at latest TimeStamp
          FGSP = Point at earliest TimeStamp where FireCode in project_geometry.production_code
          LGSP = Point at latest TimeStamp where FireCode in project_geometry.production_code

        Also fills:
          - Start/End coords from FGSP/LGSP (fallback to FSP/LSP)
          - Start/End times (min/max TimeStamp)
          - Start/End production times (min/max prod TimeStamp)
          - ProductionCount / NonProductionCount / KillCount (distinct Point)
          - PercentOfLineCompleted / PercentOfSeqCompleted (based on prod_points / all_points)
          - LineLength (distance between Start and End coords)

          - Min/Max GunDepth (all points)
          - Min/Max WaterDepth (all points)

          - Min/Max GunDepth and WaterDepth split by:
            * Production FireCode (project_geometry.production_code)
            * NonProduction FireCode (project_geometry.non_production_code)
            * Kill FireCode (project_geometry.kill_code)

        NEW output fields in SLSolution (must exist in table):
            MinProdGunDepth REAL DEFAULT 0,
            MaxProdGunDepth REAL DEFAULT 0,
            MinNonProdGunDepth REAL DEFAULT 0,
            MaxNonProdGunDepth REAL DEFAULT 0,
            MinKillGunDepth REAL DEFAULT 0,
            MaxKillGunDepth REAL DEFAULT 0,

            MinProdWaterDepth REAL DEFAULT 0,
            MaxProdWaterDepth REAL DEFAULT 0,
            MinNonProdWaterDepth REAL DEFAULT 0,
            MaxNonProdWaterDepth REAL DEFAULT 0,
            MinKillWaterDepth REAL DEFAULT 0,
            MaxKillWaterDepth REAL DEFAULT 0
        """
        file_fk = int(file_fk)
        conn = self._connect()
        try:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("BEGIN IMMEDIATE;")
            cur = conn.cursor()

            # Get codes (fallback to empty if missing)
            pg = cur.execute(
                "SELECT COALESCE(production_code,''), "
                "       COALESCE(non_production_code,''), "
                "       COALESCE(kill_code,'') "
                "FROM project_geometry LIMIT 1"
            ).fetchone()

            prod_codes = (pg[0] if pg else "") or ""
            nonprod_codes = (pg[1] if pg else "") or ""
            kill_codes = (pg[2] if pg else "") or ""

            # Core update driven by CTEs (NO VIEW: SQLite doesn't allow params in views)
            cur.execute(
                """
                WITH
                base AS (
                    SELECT
                        s.SailLine_FK AS line_id,

                        -- counts (distinct points)
                        COUNT(DISTINCT s.Point) AS count_all_points,

                        COUNT(DISTINCT CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                            THEN s.Point END) AS prod_points,

                        COUNT(DISTINCT CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                            THEN s.Point END) AS nonprod_points,

                        COUNT(DISTINCT CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                            THEN s.Point END) AS kill_points,

                        -- min/max timestamps
                        MIN(s.TimeStamp) AS min_ts,
                        MAX(s.TimeStamp) AS max_ts,

                        MIN(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                            THEN s.TimeStamp END) AS min_prod_ts,

                        MAX(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                            THEN s.TimeStamp END) AS max_prod_ts,

                        -- depth ranges (ALL points)
                        MIN(CASE WHEN s.PointDepth IS NOT NULL THEN s.PointDepth END) AS min_gun_depth,
                        MAX(CASE WHEN s.PointDepth IS NOT NULL THEN s.PointDepth END) AS max_gun_depth,

                        MIN(CASE WHEN s.WaterDepth IS NOT NULL THEN s.WaterDepth END) AS min_water_depth,
                        MAX(CASE WHEN s.WaterDepth IS NOT NULL THEN s.WaterDepth END) AS max_water_depth,

                        -- depth ranges (PRODUCTION points)
                        MIN(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                             AND s.PointDepth IS NOT NULL
                            THEN s.PointDepth END) AS min_prod_gun_depth,
                        MAX(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                             AND s.PointDepth IS NOT NULL
                            THEN s.PointDepth END) AS max_prod_gun_depth,

                        MIN(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                             AND s.WaterDepth IS NOT NULL
                            THEN s.WaterDepth END) AS min_prod_water_depth,
                        MAX(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                             AND s.WaterDepth IS NOT NULL
                            THEN s.WaterDepth END) AS max_prod_water_depth,

                        -- depth ranges (NON-PRODUCTION points)
                        MIN(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                             AND s.PointDepth IS NOT NULL
                            THEN s.PointDepth END) AS min_nonprod_gun_depth,
                        MAX(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                             AND s.PointDepth IS NOT NULL
                            THEN s.PointDepth END) AS max_nonprod_gun_depth,

                        MIN(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                             AND s.WaterDepth IS NOT NULL
                            THEN s.WaterDepth END) AS min_nonprod_water_depth,
                        MAX(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                             AND s.WaterDepth IS NOT NULL
                            THEN s.WaterDepth END) AS max_nonprod_water_depth,

                        -- depth ranges (KILL points)
                        MIN(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                             AND s.PointDepth IS NOT NULL
                            THEN s.PointDepth END) AS min_kill_gun_depth,
                        MAX(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                             AND s.PointDepth IS NOT NULL
                            THEN s.PointDepth END) AS max_kill_gun_depth,

                        MIN(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                             AND s.WaterDepth IS NOT NULL
                            THEN s.WaterDepth END) AS min_kill_water_depth,
                        MAX(CASE
                            WHEN s.FireCode IS NOT NULL AND instr(?, s.FireCode) > 0
                             AND s.WaterDepth IS NOT NULL
                            THEN s.WaterDepth END) AS max_kill_water_depth

                    FROM SPSolution s
                    JOIN SLSolution l ON l.ID = s.SailLine_FK
                    WHERE l.File_FK = ?
                      AND s.TimeStamp IS NOT NULL
                      AND trim(s.TimeStamp) <> ''
                    GROUP BY s.SailLine_FK
                ),
                picks AS (
                    SELECT
                        b.*,

                        -- FSP point: earliest timestamp
                        (SELECT Point FROM SPSolution p
                         WHERE p.SailLine_FK=b.line_id AND p.TimeStamp=b.min_ts
                         ORDER BY p.Point ASC, COALESCE(p.PointIdx,0) ASC
                         LIMIT 1) AS fsp,

                        -- LSP point: latest timestamp
                        (SELECT Point FROM SPSolution p
                         WHERE p.SailLine_FK=b.line_id AND p.TimeStamp=b.max_ts
                         ORDER BY p.Point DESC, COALESCE(p.PointIdx,0) DESC
                         LIMIT 1) AS lsp,

                        -- FGSP point: earliest production timestamp
                        (SELECT Point FROM SPSolution p
                         WHERE p.SailLine_FK=b.line_id
                           AND p.TimeStamp=b.min_prod_ts
                           AND p.FireCode IS NOT NULL AND instr(?, p.FireCode) > 0
                         ORDER BY p.Point ASC, COALESCE(p.PointIdx,0) ASC
                         LIMIT 1) AS fgsp,

                        -- LGSP point: latest production timestamp
                        (SELECT Point FROM SPSolution p
                         WHERE p.SailLine_FK=b.line_id
                           AND p.TimeStamp=b.max_prod_ts
                           AND p.FireCode IS NOT NULL AND instr(?, p.FireCode) > 0
                         ORDER BY p.Point DESC, COALESCE(p.PointIdx,0) DESC
                         LIMIT 1) AS lgsp
                    FROM base b
                )
                UPDATE SLSolution
                SET
                    FSP  = COALESCE((SELECT fsp  FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    LSP  = COALESCE((SELECT lsp  FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    FGSP = COALESCE((SELECT fgsp FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    LGSP = COALESCE((SELECT lgsp FROM picks x WHERE x.line_id=SLSolution.ID), 0),

                    Start_Time = (SELECT min_ts FROM picks x WHERE x.line_id=SLSolution.ID),
                    End_Time   = (SELECT max_ts FROM picks x WHERE x.line_id=SLSolution.ID),

                    Start_Production_Time = (SELECT min_prod_ts FROM picks x WHERE x.line_id=SLSolution.ID),
                    End_Production_Time   = (SELECT max_prod_ts FROM picks x WHERE x.line_id=SLSolution.ID),

                    -- depth ranges (ALL)
                    MinGunDepth   = (SELECT min_gun_depth    FROM picks x WHERE x.line_id=SLSolution.ID),
                    MaxGunDepth   = (SELECT max_gun_depth    FROM picks x WHERE x.line_id=SLSolution.ID),
                    MinWaterDepth = (SELECT min_water_depth  FROM picks x WHERE x.line_id=SLSolution.ID),
                    MaxWaterDepth = (SELECT max_water_depth  FROM picks x WHERE x.line_id=SLSolution.ID),

                    -- depth ranges (PROD / NONPROD / KILL) (default 0 if NULL)
                    MinProdGunDepth     = COALESCE((SELECT min_prod_gun_depth     FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    MaxProdGunDepth     = COALESCE((SELECT max_prod_gun_depth     FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    MinNonProdGunDepth  = COALESCE((SELECT min_nonprod_gun_depth  FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    MaxNonProdGunDepth  = COALESCE((SELECT max_nonprod_gun_depth  FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    MinKillGunDepth     = COALESCE((SELECT min_kill_gun_depth     FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    MaxKillGunDepth     = COALESCE((SELECT max_kill_gun_depth     FROM picks x WHERE x.line_id=SLSolution.ID), 0),

                    MinProdWaterDepth    = COALESCE((SELECT min_prod_water_depth    FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    MaxProdWaterDepth    = COALESCE((SELECT max_prod_water_depth    FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    MinNonProdWaterDepth = COALESCE((SELECT min_nonprod_water_depth FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    MaxNonProdWaterDepth = COALESCE((SELECT max_nonprod_water_depth FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    MinKillWaterDepth    = COALESCE((SELECT min_kill_water_depth    FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    MaxKillWaterDepth    = COALESCE((SELECT max_kill_water_depth    FROM picks x WHERE x.line_id=SLSolution.ID), 0),

                    -- Start coords: FGSP else FSP
                    StartX = COALESCE(
                        (SELECT Easting FROM SPSolution p
                         WHERE p.SailLine_FK=SLSolution.ID
                           AND p.Point=(SELECT fgsp FROM picks x WHERE x.line_id=SLSolution.ID)
                         ORDER BY COALESCE(p.PointIdx,0) ASC LIMIT 1),
                        (SELECT Easting FROM SPSolution p
                         WHERE p.SailLine_FK=SLSolution.ID
                           AND p.Point=(SELECT fsp FROM picks x WHERE x.line_id=SLSolution.ID)
                         ORDER BY COALESCE(p.PointIdx,0) ASC LIMIT 1)
                    ),
                    StartY = COALESCE(
                        (SELECT Northing FROM SPSolution p
                         WHERE p.SailLine_FK=SLSolution.ID
                           AND p.Point=(SELECT fgsp FROM picks x WHERE x.line_id=SLSolution.ID)
                         ORDER BY COALESCE(p.PointIdx,0) ASC LIMIT 1),
                        (SELECT Northing FROM SPSolution p
                         WHERE p.SailLine_FK=SLSolution.ID
                           AND p.Point=(SELECT fsp FROM picks x WHERE x.line_id=SLSolution.ID)
                         ORDER BY COALESCE(p.PointIdx,0) ASC LIMIT 1)
                    ),

                    -- End coords: LGSP else LSP
                    EndX = COALESCE(
                        (SELECT Easting FROM SPSolution p
                         WHERE p.SailLine_FK=SLSolution.ID
                           AND p.Point=(SELECT lgsp FROM picks x WHERE x.line_id=SLSolution.ID)
                         ORDER BY COALESCE(p.PointIdx,0) DESC LIMIT 1),
                        (SELECT Easting FROM SPSolution p
                         WHERE p.SailLine_FK=SLSolution.ID
                           AND p.Point=(SELECT lsp FROM picks x WHERE x.line_id=SLSolution.ID)
                         ORDER BY COALESCE(p.PointIdx,0) DESC LIMIT 1)
                    ),
                    EndY = COALESCE(
                        (SELECT Northing FROM SPSolution p
                         WHERE p.SailLine_FK=SLSolution.ID
                           AND p.Point=(SELECT lgsp FROM picks x WHERE x.line_id=SLSolution.ID)
                         ORDER BY COALESCE(p.PointIdx,0) DESC LIMIT 1),
                        (SELECT Northing FROM SPSolution p
                         WHERE p.SailLine_FK=SLSolution.ID
                           AND p.Point=(SELECT lsp FROM picks x WHERE x.line_id=SLSolution.ID)
                         ORDER BY COALESCE(p.PointIdx,0) DESC LIMIT 1)
                    ),

                    -- counts
                    ProductionCount    = COALESCE((SELECT prod_points    FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    NonProductionCount = COALESCE((SELECT nonprod_points FROM picks x WHERE x.line_id=SLSolution.ID), 0),
                    KillCount          = COALESCE((SELECT kill_points    FROM picks x WHERE x.line_id=SLSolution.ID), 0),

                    -- percents (based on prod_points / all_points)
                    PercentOfLineCompleted = ROUND(
                        100.0 * COALESCE((SELECT prod_points FROM picks x WHERE x.line_id=SLSolution.ID), 0)
                        / NULLIF(COALESCE((SELECT count_all_points FROM picks x WHERE x.line_id=SLSolution.ID), 0), 0),
                    2),

                    PercentOfSeqCompleted = ROUND(
                        100.0 * COALESCE((SELECT prod_points FROM picks x WHERE x.line_id=SLSolution.ID), 0)
                        / NULLIF(COALESCE((SELECT count_all_points FROM picks x WHERE x.line_id=SLSolution.ID), 0), 0),
                    2)

                WHERE File_FK = ?
                  AND EXISTS (SELECT 1 FROM picks x WHERE x.line_id=SLSolution.ID);
                """,
                (
                    # counts
                    prod_codes,  # prod_points
                    nonprod_codes,  # nonprod_points
                    kill_codes,  # kill_points

                    # prod timestamps
                    prod_codes,  # min_prod_ts
                    prod_codes,  # max_prod_ts

                    # prod depths
                    prod_codes,  # min_prod_gun_depth
                    prod_codes,  # max_prod_gun_depth
                    prod_codes,  # min_prod_water_depth
                    prod_codes,  # max_prod_water_depth

                    # nonprod depths
                    nonprod_codes,  # min_nonprod_gun_depth
                    nonprod_codes,  # max_nonprod_gun_depth
                    nonprod_codes,  # min_nonprod_water_depth
                    nonprod_codes,  # max_nonprod_water_depth

                    # kill depths
                    kill_codes,  # min_kill_gun_depth
                    kill_codes,  # max_kill_gun_depth
                    kill_codes,  # min_kill_water_depth
                    kill_codes,  # max_kill_water_depth

                    # file filter for base
                    file_fk,

                    # fgsp/lgsp selection uses production codes
                    prod_codes,  # fgsp instr
                    prod_codes,  # lgsp instr

                    # update where File_FK
                    file_fk,
                ),
            )

            # LineLength from Start/End coords
            cur.execute(
                """
                UPDATE SLSolution
                SET LineLength =
                    CASE
                      WHEN StartX IS NULL OR StartY IS NULL OR EndX IS NULL OR EndY IS NULL THEN 0
                      ELSE sqrt((EndX-StartX)*(EndX-StartX) + (EndY-StartY)*(EndY-StartY))
                    END
                WHERE File_FK = ?;
                """,
                (file_fk,),
            )

            conn.commit()

            updated = cur.execute(
                "SELECT COUNT(*) FROM SLSolution WHERE File_FK=? AND Start_Time IS NOT NULL",
                (file_fk,),
            ).fetchone()[0]

            return f"SLSolution updated={int(updated)} (file_fk={file_fk})"

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    def update_slsolution_from_preplot_timebased(self, file_fk: int) -> str:
        file_fk = int(file_fk)
        conn = self._connect()
        try:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("BEGIN IMMEDIATE;")
            cur = conn.cursor()

            # ---- your existing: codes + V_SL_TIME_AGG_FILE + update FSP/LSP/FGSP/LGSP + coords + counts + LineLength ----
            # (keep everything you already have)

            # ------------------------------------------------------------
            # NEW PART 1: set PPLine_FK from SLPreplot by Line match
            # ------------------------------------------------------------
            cur.execute("""
            UPDATE SLSolution
            SET PPLine_FK = (
                SELECT p.ID
                FROM SLPreplot p
                WHERE p.Line = SLSolution.Line
                ORDER BY p.ID
                LIMIT 1
            )
            WHERE File_FK = ?;
            """, (file_fk,))

            # ------------------------------------------------------------
            # NEW PART 2: store PP_Length (SLPreplot.Length)
            # Requires SLSolution.PP_Length column exists
            # ------------------------------------------------------------
            cur.execute("""
            UPDATE SLSolution
            SET PP_Length = COALESCE((
                SELECT p.LineLength
                FROM SLPreplot p
                WHERE p.ID = SLSolution.PPLine_FK
                LIMIT 1
            ), 0)
            WHERE File_FK = ?;
            """, (file_fk,))

            # ------------------------------------------------------------
            # NEW PART 3: SeqLenPercentage = LineLength / PP_Length * 100
            # Requires SLSolution.SeqLenPercentage column exists
            # ------------------------------------------------------------
            cur.execute("""
            UPDATE SLSolution
            SET SeqLenPercentage =
                CASE
                  WHEN COALESCE(PP_Length, 0) > 0
                  THEN ROUND(100.0 * COALESCE(LineLength, 0) / PP_Length, 2)
                  ELSE 0
                END
            WHERE File_FK = ?;
            """, (file_fk,))

            conn.commit()

            updated = cur.execute(
                "SELECT COUNT(*) FROM SLSolution WHERE File_FK=?",
                (file_fk,),
            ).fetchone()[0]

            return f"SLSolution updated={int(updated)} (file_fk={file_fk})"

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
