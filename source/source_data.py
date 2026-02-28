import csv
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
            *,
            line: int,
            seq: int,
            attempt: str,
            tier: int,
            tierline: int,
            vessel: str | None,
            file_fk: int,
    ) -> int:
        """
        Gets/creates SLSolution row by LineName (unique).
        LineName format: LLLLLXSSSS (same idea as your nav line code).
        """
        conn = self._connect()
        cur = conn.cursor()
        attempt = (attempt or "").strip() or "A"
        attempt = attempt[:1]  # ensure 1 char
        line_name = f"{int(line):05d}{attempt}{int(seq):04d}"

        row = conn.execute(
            "SELECT ID FROM SLSolution WHERE LineName=?",
            (line_name,),
        ).fetchone()
        if row:
            return int(row[0])

        conn.execute(
            """
            INSERT INTO SLSolution
            (File_FK, FileName_FK, LineName, Line, Seq, Attempt, Tier, TierLine, Vessel)
            VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (file_fk, file_fk, line_name, int(line), int(seq), attempt, int(tier), int(tierline), vessel),
        )
        return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

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
    def decode_sps_string(self,s:str,
                          sps_revision:SPSRevision,
                          geom:GeometrySettings,
                          default:int|None,
                          tier:int=1,
                          line_bearing:float=0)->SourceSPSData:

        point_len = geom.sou_point_length
        line_len = geom.sou_line_length
        line_point_len = geom.sou_linepoint_length

        sail_line=s[sps_revision.line_start:sps_revision.line_end] or ""
        sail_line_mask=geom.sail_line_mask
        if "L" in sail_line_mask:
            l_first = sail_line_mask.find("L")  # 1
            l_last = sail_line_mask.rfind("L")+1  # 5
        else:
            return None
        if "X" in sail_line_mask:
            x_first = sail_line_mask.find("X")  # 1
            x_last = sail_line_mask.rfind("X")+1  # 5
        else:
            return None
        if "S" in sail_line_mask:
            s_first = sail_line_mask.find("S")  # 1
            s_last = sail_line_mask.rfind("S")+1  # 5
        else:
            return None

        line  = self._to_int(sail_line[l_first:l_last],default=default)
        attempt = sail_line[x_first:x_last] or ""
        seq = self._to_int(sail_line[s_first:s_last],default=default)
        point = self._to_int(s[sps_revision.point_start:sps_revision.point_end],default=default)
        gun_depth =  self._to_float(s[sps_revision.point_depth_start:sps_revision.point_depth_end],default=default)
        water_depth = self._to_float(s[sps_revision.water_depth_start:sps_revision.water_depth_end], default=default)
        easting = self._to_float(s[sps_revision.easting_start:sps_revision.easting_end],default=default)
        northing = self._to_float(s[sps_revision.northing_start:sps_revision.northing_end],default=default)
        elevation = self._to_float(s[sps_revision.elevation_start:sps_revision.elevation_end],default=default)
        point_code =s[sps_revision.point_code_start:sps_revision.point_code_end] or ""
        array_code = self._to_int(point_code[1],default=default)
        point_index= self._to_int(s[sps_revision.point_idx_start:sps_revision.point_idx_end],default=default)
        if not point_index:
            point_index = 1
        line_point = line*point_len+point
        line_point_idx = line_point*10+point_index
        tier_line=tier*line_len+line
        tier_line_point = tier*line_point_len+line_point
        tier_line_point_idx = tier*(10**(len(str(line_point_idx)))*10)+line_point_idx


        return SourceSPSData(
            sail_line=sail_line,
            attempt=attempt,
            seq=seq,
            line=line,
            point=point,
            gun_depth=gun_depth,
            water_depth=water_depth,
            array_code=array_code,
            easting=easting,
            northing=northing,
            elevation=elevation,
            point_code=point_code,
            point_index=point_index,
            line_point=line_point,
            #line_point_idx=line_point_idx,
            tier_line=tier_line,
            #tier_line_point=tier_line_point,
            #tier_line_point_idx=tier_line_point_idx,
            line_bearing=line_bearing

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
            sps_revision,  # SPSRevision
            geometry:GeometrySettings,
            vessel: str | None,
            tier: int = 1,
            line_bearing: float = 0.0,
            default: int | None = None,
            batch_size: int = 50000,
    ) -> dict:
        """
        Fast streaming SPS loader into:
          - SLSolution (line table)
          - SPSolution (point table)

        Requires: Files table + SLSolution + SPSolution already exist.
        """

        import io

        file_name = uploaded_file.name

        # Detect encoding (reuse your ProjectDB method)
        uploaded_file.seek(0)
        sample = uploaded_file.read(4096)
        encoding = self._detect_text_encoding(sample)
        uploaded_file.seek(0)



        # Files.ID
        file_fk = self.insert_file_record(file_name, file_type="SPS")

        # Text stream wrapper (fast)
        stream = io.TextIOWrapper(uploaded_file.file, encoding=encoding, errors="replace", newline="")

        conn = self._connect()
        try:
            self._begin_fast_import(aggressive=False)
            cur = conn.cursor()
            cur.execute("BEGIN;")

            # Cache line_name -> sl_id to avoid hitting DB each point
            sl_cache: dict[str, int] = {}

            insert_sql = """
            INSERT INTO SPSolution (
                LineName_FK,
                FileName_FK,
                Tier,
                TierLinePoint,
                LinePoint,
                Point,
                PointIdx,
                FireCode,
                ArrayNumber,
                PointCode,
                WaterDepth,
                Easting,
                Northing,
                Elevation,
                JDay, Hour, Minute, Second, Microsecond,
                Day, Month, Week, Year,
                YearDay,
                TimeStamp,
                Vessel
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """

            batch: list[tuple] = []
            total = 0
            skipped = 0
            lines_touched: set[int] = set()

            for text_line in stream:
                if not text_line:
                    continue
                if text_line[0] == "H":
                    continue

                # Decode SPS line -> your existing decoder
                p = self.decode_sps_string(
                    text_line,
                    sps_revision=sps_revision,
                    default=default,
                    tier=tier,
                    geom=geometry,
                    line_bearing=line_bearing,
                )
                if p is None:
                    skipped += 1
                    continue

                # --- build line identifiers ---
                # p.line must exist. If not, adjust to your decoded object.


                sl_id = sl_cache.get(p.sail_line)
                if sl_id is None:
                    sl_id = self._get_or_create_sl_solution_id(
                        conn,
                        line=line,
                        seq=int(seq),
                        attempt=attempt1,
                        tier=int(tier),
                        tierline=int(tierline),
                        vessel=vessel,
                        file_fk=int(file_fk),
                    )
                    sl_cache[line_name] = sl_id

                lines_touched.add(sl_id)

                # --- FireCode ---
                # If your decoded object has fire code -> use it.
                # Otherwise, you can derive from some field; placeholder:
                fire_code = None
                if hasattr(p, "fire_code") and p.fire_code:
                    fire_code = str(p.fire_code).strip()[:1].upper()
                elif hasattr(p, "post_point_code") and p.post_point_code:
                    fire_code = str(p.post_point_code).strip()[:1].upper()

                # --- timestamps / date parts (adapt to what your p contains) ---
                # Use safe getters; if your p has different names, map accordingly.
                jday = int(getattr(p, "jday", 0) or 0)
                hour = int(getattr(p, "hour", 0) or 0)
                minute = int(getattr(p, "minute", 0) or 0)
                second = int(getattr(p, "second", 0) or 0)
                micro = float(getattr(p, "microsecond", 0) or 0)

                day = int(getattr(p, "day", 0) or 0)
                month = int(getattr(p, "month", 0) or 0)
                week = int(getattr(p, "week", 0) or 0)
                year = int(getattr(p, "year", 0) or 0)

                # YearDay (text) + TimeStamp (datetime text) if you have them
                yearday = getattr(p, "year_day", None) or None
                ts = getattr(p, "timestamp", None) or getattr(p, "time_stamp", None) or None

                # core point
                point = int(getattr(p, "point", 0) or 0)
                point_idx = int(getattr(p, "point_index", 0) or 0)

                # offsets / depths / coords
                wd = int(getattr(p, "water_depth", 0) or 0)
                x = float(getattr(p, "easting", 0.0) or 0.0)
                y = float(getattr(p, "northing", 0.0) or 0.0)
                z = float(getattr(p, "elevation", 0.0) or 0.0)

                # optional
                array_num = int(getattr(p, "array_number", 0) or 0)

                # linepoint fields (set something useful; change if you have exact logic)
                tier_line_point = int(getattr(p, "tier_line_point", 0) or 0)
                line_point = int(getattr(p, "line_point", 0) or 0)

                batch.append((
                    sl_id,
                    int(file_fk),
                    int(tier),
                    tier_line_point,
                    line_point,
                    point,
                    point_idx,
                    fire_code,
                    array_num,
                    point_code,
                    wd,
                    x, y, z,
                    jday, hour, minute, second, micro,
                    day, month, week, year,
                    yearday,
                    ts,
                    vessel,
                ))
                total += 1

                if len(batch) >= batch_size:
                    cur.executemany(insert_sql, batch)
                    batch.clear()

            if batch:
                cur.executemany(insert_sql, batch)
                batch.clear()

            conn.commit()
            self._end_fast_import(conn)

            return {
                "file_fk": int(file_fk),
                "points": int(total),
                "skipped": int(skipped),
                "lines": int(len(lines_touched)),
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

    def update_slsolution_from_spsolution_fast(self) -> str:
        """
        Fast aggregate update:
          SLSolution <- SPSolution + project_geometry (production/non-production codes)
        Uses SQL GROUP BY (very fast).
        """
        conn = self._connect()
        try:
            self._begin_fast_import(conn, aggressive=False)

            conn.execute("BEGIN IMMEDIATE;")
            try:
                # If project_geometry can be empty, provide defaults (prevents empty joins)
                conn.executescript("""
                DROP VIEW IF EXISTS V_PG_ONE;
                CREATE VIEW V_PG_ONE AS
                WITH pg AS (
                    SELECT production_code, non_production_code
                    FROM project_geometry
                    LIMIT 1
                    UNION ALL
                    SELECT '' AS production_code, '' AS non_production_code
                    WHERE NOT EXISTS (SELECT 1 FROM project_geometry)
                )
                SELECT production_code, non_production_code FROM pg LIMIT 1;
                """)

                # Main aggregate per line
                # Start/End by MIN/MAX(Point) with correlated subqueries for X/Y
                conn.executescript("""
                DROP VIEW IF EXISTS V_SPSOLUTION_AGG;
                CREATE VIEW V_SPSOLUTION_AGG AS
                WITH pg AS (SELECT production_code, non_production_code FROM V_PG_ONE),
                a AS (
                    SELECT
                        LineName_FK AS line_id,
                        MIN(Point) AS min_point,
                        MAX(Point) AS max_point,
                        COUNT(*) AS count_all,

                        SUM(CASE WHEN FireCode='A' THEN 1 ELSE 0 END) AS count_a,
                        SUM(CASE WHEN FireCode='P' THEN 1 ELSE 0 END) AS count_p,
                        SUM(CASE WHEN FireCode='L' THEN 1 ELSE 0 END) AS count_l,
                        SUM(CASE WHEN FireCode='R' THEN 1 ELSE 0 END) AS count_r,
                        SUM(CASE WHEN FireCode='X' THEN 1 ELSE 0 END) AS count_x,
                        SUM(CASE WHEN FireCode='M' THEN 1 ELSE 0 END) AS count_m,
                        SUM(CASE WHEN FireCode='K' THEN 1 ELSE 0 END) AS count_k,
                        SUM(CASE WHEN FireCode='W' THEN 1 ELSE 0 END) AS count_w,
                        SUM(CASE WHEN FireCode='T' THEN 1 ELSE 0 END) AS count_t,

                        COUNT(DISTINCT CASE
                            WHEN FireCode IS NOT NULL
                             AND instr((SELECT production_code FROM pg), FireCode) > 0
                            THEN Point
                        END) AS prod_distinct_points,

                        COUNT(DISTINCT CASE
                            WHEN FireCode IS NOT NULL
                             AND instr((SELECT non_production_code FROM pg), FireCode) > 0
                            THEN Point
                        END) AS nonprod_distinct_points

                    FROM SPSolution
                    GROUP BY LineName_FK
                )
                SELECT
                    a.*,

                    (SELECT Easting  FROM SPSolution p WHERE p.LineName_FK=a.line_id AND p.Point=a.min_point LIMIT 1) AS start_x,
                    (SELECT Northing FROM SPSolution p WHERE p.LineName_FK=a.line_id AND p.Point=a.min_point LIMIT 1) AS start_y,
                    (SELECT Easting  FROM SPSolution p WHERE p.LineName_FK=a.line_id AND p.Point=a.max_point LIMIT 1) AS end_x,
                    (SELECT Northing FROM SPSolution p WHERE p.LineName_FK=a.line_id AND p.Point=a.max_point LIMIT 1) AS end_y
                FROM a;
                """)

                # Apply updates
                conn.executescript("""
                UPDATE SLSolution
                SET
                    FSP = (SELECT min_point FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID),
                    LSP = (SELECT max_point FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID),

                    StartX = (SELECT start_x FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID),
                    StartY = (SELECT start_y FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID),
                    EndX   = (SELECT end_x   FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID),
                    EndY   = (SELECT end_y   FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID),

                    Count_All = COALESCE((SELECT count_all FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0),
                    Count_A   = COALESCE((SELECT count_a   FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0),
                    Count_P   = COALESCE((SELECT count_p   FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0),
                    Count_L   = COALESCE((SELECT count_l   FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0),
                    Count_R   = COALESCE((SELECT count_r   FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0),
                    Count_X   = COALESCE((SELECT count_x   FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0),
                    Count_M   = COALESCE((SELECT count_m   FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0),
                    Count_K   = COALESCE((SELECT count_k   FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0),
                    Count_W   = COALESCE((SELECT count_w   FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0),
                    Count_T   = COALESCE((SELECT count_t   FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0),

                    SeqProdCount = COALESCE((SELECT prod_distinct_points FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0),

                    PercentOfSeqDone = ROUND(
                        100.0 * COALESCE((SELECT prod_distinct_points FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0)
                        / NULLIF(COALESCE((SELECT count_all FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0), 0),
                    2),

                    PercentOfLineDone = ROUND(
                        100.0 * COALESCE((SELECT prod_distinct_points FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0)
                        / NULLIF(COALESCE((SELECT count_all FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID), 0), 0),
                    2)

                WHERE EXISTS (SELECT 1 FROM V_SPSOLUTION_AGG WHERE line_id=SLSolution.ID);
                """)

                conn.commit()
            except Exception:
                conn.rollback()
                raise

            # how many lines updated?
            updated = conn.execute("""
                SELECT COUNT(*) FROM SLSolution
                WHERE Count_All IS NOT NULL AND Count_All > 0
            """).fetchone()[0]

            return f"SLSolution updated={int(updated)}"

        finally:
            conn.close()