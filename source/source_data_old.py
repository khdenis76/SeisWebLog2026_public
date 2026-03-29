import bisect
import csv
import datetime
import io
import re
import sqlite3
import threading
import time
import os
import traceback
from pathlib import Path
from typing import Optional
from core.models import SPSRevision
from core.project_dataclasses import *
from datetime import datetime, timezone



class SourceData:
    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.seq_ranges = []  # [(seq_first, seq_last, id, vessel_id)]
        self.seq_starts = []  # [seq_first,...]

    def _connect(self):
        conn = sqlite3.connect(
            self.db_path,
            timeout=20,
            isolation_level=None,
            check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")

        # debug only
        # print(f"[DB_CONNECT] db={self.db_path}", flush=True)
        # traceback.print_stack(limit=6)

        return conn

    def load_sequence_mapping(self):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, seq_first, seq_last, vessel_id
                FROM sequence_vessel_assignment
                WHERE is_active = 1
                ORDER BY seq_first
            """)
            rows = cur.fetchall()

            self.seq_ranges = [(r[1], r[2], r[0], r[3]) for r in rows]
            self.seq_starts = [r[1] for r in rows]
        finally:
            conn.close()

    def get_seq_info(self, seq):

        idx = bisect.bisect_right(self.seq_starts, seq) - 1
        if idx >= 0:
            seq_first, seq_last, rec_id, vessel_id = self.seq_ranges[idx]
            if seq_first <= seq <= seq_last:
                return {
                    "id": rec_id,
                    "vessel_id": vessel_id,
                }

        return {
            "id": None,
            "vessel_id": None,
        }

    def get_seq_fk(self, seq):
        return self.get_seq_info(seq)["id"]

    def get_vessel_id(self, seq):
        return self.get_seq_info(seq)["vessel_id"]
    def drop_shot_table_indexes(self, conn=None) -> int:
        """
        Drops all user-created indexes for SHOT_TABLE.
        Returns number of dropped indexes.
        """
        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            cur = conn.cursor()

            indexes = cur.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type = 'index'
                  AND tbl_name = 'SHOT_TABLE'
                  AND name NOT LIKE 'sqlite_autoindex%';
            """).fetchall()

            dropped = 0

            for row in indexes:
                index_name = row["name"] if hasattr(row, "keys") else row[0]
                cur.execute(f'DROP INDEX IF EXISTS "{index_name}"')
                dropped += 1

            if own_conn:
                conn.commit()

            return dropped

        except Exception:
            if own_conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise

        finally:
            if own_conn:
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

    def to_int(self,x):
        x = (x or "").strip()
        if not x:
            return None
        try:
            return int(x)
        except ValueError:
            return None

    def to_float(self, x):
        x = (x or "").strip()
        if not x:
            return None
        try:
            return float(x)
        except ValueError:
            return None

    def insert_file_record(self, file_name: str, file_type: str | None = None, conn=None,
                           force_new: bool = False) -> int:
        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            cur = conn.cursor()

            filename = getattr(file_name, "name", None)
            if filename is None:
                filename = str(file_name)
            filename = os.path.basename(filename)

            if not force_new:
                cur.execute(
                    "SELECT id FROM Files WHERE FileName = ? ORDER BY id DESC LIMIT 1",
                    (filename,)
                )
                row = cur.fetchone()
                if row:
                    return int(row["id"] if hasattr(row, "keys") else row[0])

            cur.execute(
                "INSERT INTO Files (FileName) VALUES (?)",
                (filename,)
            )

            if own_conn:
                conn.commit()

            return int(cur.lastrowid)

        except Exception:
            if own_conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise

        finally:
            if own_conn:
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

    def create_shot_table(self, conn=None):
        own_conn = conn is None
        if own_conn:
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

            if own_conn:
                conn.commit()

        except Exception:
            if own_conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise

        finally:
            if own_conn:
                conn.close()

    def create_shot_table_indexes(self, conn=None) -> int:
        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            cur = conn.cursor()

            # remove duplicates first so UNIQUE index can be created
            cur.execute("""
                DELETE FROM SHOT_TABLE
                WHERE id NOT IN (
                    SELECT MAX(id)
                    FROM SHOT_TABLE
                    GROUP BY nav_line_code, nav_station, post_point_code
                );
            """)

            sql_list = [
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_shot_unique
                ON SHOT_TABLE(nav_line_code, nav_station, post_point_code)
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_shot_line_attempt_seq
                ON SHOT_TABLE(nav_line, attempt, seq)
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_shot_compare
                ON SHOT_TABLE(nav_line_code, seq, nav_station, shot_index)
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_shot_file_fk
                ON SHOT_TABLE(File_FK)
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_shot_seq_fk
                ON SHOT_TABLE(Seq_FK)
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_shot_line_station
                ON SHOT_TABLE(nav_line_code, nav_station)
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_sps_compare
                ON SPSolution(SailLine, Seq, Point, Attempt)
                """
            ]

            for sql in sql_list:
                cur.execute(sql)

            if own_conn:
                conn.commit()

            return len(sql_list)

        except Exception:
            if own_conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise

        finally:
            if own_conn:
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

    def list_sps_files_summary(
            self,
            *,
            search: str = "",
            vessel_fk: int | None = None,
            purpose: str = "",
            sailline: str = "",
            attempt: str = "",
            line_from: int | None = None,
            line_to: int | None = None,
            seq_from: int | None = None,
            seq_to: int | None = None,
            wd: str = "",  # "", "Ok", "Error"
            gd: str = "",  # "", "Ok", "Error"
            min_depth_limit: float | None = None,
            max_depth_limit: float | None = None,
            sort_by: str = "seq",
            sort_dir: str = "asc",
    ) -> list[dict]:
        conn = self._connect()
        try:
            cur = conn.cursor()

            where = []
            params = []

            # -----------------------------
            # text search
            # -----------------------------
            search = (search or "").strip().lower()
            if search:
                like = f"%{search}%"
                where.append("""
                    (
                        lower(COALESCE(sl.SailLine, '')) LIKE ?
                        OR lower(COALESCE(pv.vessel_name, '')) LIKE ?
                        OR lower(COALESCE(p.purpose, '')) LIKE ?
                        OR CAST(COALESCE(sl.Line, '') AS TEXT) LIKE ?
                        OR CAST(COALESCE(sl.Seq, '') AS TEXT) LIKE ?
                        OR CAST(COALESCE(sl.Attempt, '') AS TEXT) LIKE ?
                        OR CAST(COALESCE(sl.MaxSPI, '') AS TEXT) LIKE ?
                        OR CAST(COALESCE(sl.ProductionCount, '') AS TEXT) LIKE ?
                        OR CAST(COALESCE(sl.NonProductionCount, '') AS TEXT) LIKE ?
                        OR CAST(COALESCE(sl.KillCount, '') AS TEXT) LIKE ?
                    )
                """)
                params.extend([like, like, like, like, like, like, like, like, like, like])

            # -----------------------------
            # exact filters
            # -----------------------------
            if vessel_fk is not None:
                where.append("COALESCE(sl.Vessel_FK, 0) = ?")
                params.append(int(vessel_fk))

            purpose = (purpose or "").strip()
            if purpose:
                where.append("COALESCE(p.purpose, '') = ?")
                params.append(purpose)

            sailline = (sailline or "").strip()
            if sailline:
                where.append("COALESCE(sl.SailLine, '') = ?")
                params.append(sailline)

            attempt = (attempt or "").strip()
            if attempt:
                where.append("CAST(COALESCE(sl.Attempt, '') AS TEXT) = ?")
                params.append(attempt)

            if line_from is not None:
                where.append("COALESCE(sl.Line, 0) >= ?")
                params.append(int(line_from))

            if line_to is not None:
                where.append("COALESCE(sl.Line, 0) <= ?")
                params.append(int(line_to))

            if seq_from is not None:
                where.append("COALESCE(sl.Seq, 0) >= ?")
                params.append(int(seq_from))

            if seq_to is not None:
                where.append("COALESCE(sl.Seq, 0) <= ?")
                params.append(int(seq_to))

            # -----------------------------
            # WD status
            # template logic:
            # if MinProdWaterDepth > 0 => Ok else Error
            # -----------------------------
            wd = (wd or "").strip()
            if wd == "Ok":
                where.append("COALESCE(sl.MinProdWaterDepth, 0) > 0")
            elif wd == "Error":
                where.append("COALESCE(sl.MinProdWaterDepth, 0) <= 0")

            # -----------------------------
            # GD status
            # template logic:
            # min_depth_limit <= MinProdGunDepth <= max_depth_limit => Ok
            # -----------------------------
            gd = (gd or "").strip()
            if gd and min_depth_limit is not None and max_depth_limit is not None:
                if gd == "Ok":
                    where.append("COALESCE(sl.MinProdGunDepth, 0) BETWEEN ? AND ?")
                    params.extend([float(min_depth_limit), float(max_depth_limit)])
                elif gd == "Error":
                    where.append("""
                        (
                            COALESCE(sl.MinProdGunDepth, 0) < ?
                            OR COALESCE(sl.MinProdGunDepth, 0) > ?
                        )
                    """)
                    params.extend([float(min_depth_limit), float(max_depth_limit)])

            # -----------------------------
            # safe order by map
            # -----------------------------
            sort_map = {
                "id": "sl.ID",
                "ppline": "sl.PPLine_FK",
                "sailline": "sl.SailLine",
                "line": "sl.Line",
                "seq": "sl.Seq",
                "attempt": "sl.Attempt",
                "tier": "sl.Tier",
                "tierline": "sl.TierLine",
                "fsp": "sl.FSP",
                "lsp": "sl.LSP",
                "fgsp": "sl.FGSP",
                "lgsp": "sl.LGSP",
                "vessel": "pv.vessel_name",
                "source": "pv.vessel_name",
                "purpose": "p.purpose",
                "start_time": "sl.Start_Time",
                "end_time": "sl.End_Time",
                "linelength": "sl.LineLength",
                "prodstart": "sl.Start_Production_Time",
                "prodend": "sl.End_Production_Time",
                "percentline": "sl.PercentOfLineCompleted",
                "percentseq": "sl.PercentOfSeqCompleted",
                "production": "sl.ProductionCount",
                "nonproduction": "sl.NonProductionCount",
                "kill": "sl.KillCount",
                "gdmin": "sl.MinProdGunDepth",
                "gdmax": "sl.MaxProdGunDepth",
                "wdmin": "sl.MinProdWaterDepth",
                "wdmax": "sl.MaxProdWaterDepth",
                "pplength": "sl.PP_Length",
                "seqlenpercentage": "sl.SeqLenPercentage",
                "maxspi": "sl.MaxSPI",
                "maxseq": "sl.MaxSeq",
            }

            order_col = sort_map.get((sort_by or "").strip().lower(), "sl.Seq")
            order_dir = "DESC" if (sort_dir or "").strip().lower() == "desc" else "ASC"

            sql = f"""
                SELECT
                    sl.ID,
                    sl.PPLine_FK,
                    sl.File_FK,
                    sl.SailLine,
                    sl.Line,
                    sl.Seq,
                    sl.Attempt,
                    sl.Tier,
                    sl.TierLine,
                    sl.FSP,
                    sl.LSP,
                    sl.FGSP,
                    sl.LGSP,
                    sl.StartX,
                    sl.StartY,
                    sl.EndX,
                    sl.EndY,
                    sl.Vessel_FK,
                    pv.vessel_name AS VesselName,
                    sl.Start_Time,
                    sl.End_Time,
                    sl.LineLength,
                    sl.Start_Production_Time,
                    sl.End_Production_Time,
                    sl.PercentOfLineCompleted,
                    sl.PercentOfSeqCompleted,
                    sl.ProductionCount,
                    sl.NonProductionCount,
                    sl.KillCount,
                    sl.MinProdGunDepth,
                    sl.MaxProdGunDepth,
                    sl.MinProdWaterDepth,
                    sl.MaxProdWaterDepth,
                    sl.PP_Length,
                    sl.SeqLenPercentage,
                    sl.MaxSPI,
                    sl.MaxSeq,
                    sl.purpose_id,
                    p.purpose AS purpose
                FROM SLSolution sl
                LEFT JOIN (
                    SELECT purpose_id, MAX(purpose) AS purpose
                    FROM sequence_vessel_assignment
                    GROUP BY purpose_id
                ) p
                  ON sl.purpose_id = p.purpose_id
                LEFT JOIN project_fleet pv
                  ON sl.Vessel_FK = pv.id
                {"WHERE " + " AND ".join(where) if where else ""}
                ORDER BY {order_col} {order_dir}, sl.Seq ASC, sl.Attempt ASC;
            """

            rows = cur.execute(sql, params).fetchall()
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
            purpose_id: int | None,
    ) -> int:
        sail_line = (sail_line or "").strip()
        attempt = (attempt or "").strip()[:1].upper() or "X"
        tierline = int(tier) * 100000 + int(line)

        row = conn.execute(
            """
            SELECT
                ID,
                COALESCE(Vessel_FK, 0)   AS Vessel_FK,
                COALESCE(purpose_id, 0)  AS purpose_id,
                COALESCE(File_FK, 0)     AS File_FK
            FROM SLSolution
            WHERE SailLine=?
            """,
            (sail_line,),
        ).fetchone()

        # ✅ if exists — update vessel_fk / purpose_id / file_fk (only if changed)
        if row:
            sl_id = int(row["ID"])
            old_vessel = int(row["Vessel_FK"] or 0)
            old_purpose = int(row["purpose_id"] or 0)
            old_file = int(row["File_FK"] or 0)

            updates = []
            params = []

            if vessel_fk is not None and int(vessel_fk) != old_vessel:
                updates.append("Vessel_FK=?")
                params.append(int(vessel_fk))

            if purpose_id is not None and int(purpose_id) != old_purpose:
                updates.append("purpose_id=?")
                params.append(int(purpose_id))

            if int(file_fk) != old_file:
                updates.append("File_FK=?")
                params.append(int(file_fk))

            if updates:
                params.append(sl_id)
                conn.execute(
                    f"UPDATE SLSolution SET {', '.join(updates)} WHERE ID=?",
                    tuple(params),
                )

            return sl_id

        # ✅ insert new
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO SLSolution
            (PPLine_FK, File_FK, SailLine, Line, Seq, Attempt, Tier, TierLine, Vessel_FK, purpose_id)
            VALUES (0, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(file_fk), sail_line, int(line), int(seq), attempt, int(tier), int(tierline), vessel_fk, purpose_id),
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
        static = self._to_int(s[sps_revision.static_start:sps_revision.static_end], default=default)
        point_depth = self._to_float(s[sps_revision.point_depth_start:sps_revision.point_depth_end], default=default)
        datum = self._to_int(s[sps_revision.datum_start:sps_revision.datum_end], default=default)
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
            static=static,
            datum=datum,
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

    def _set_fast_import_pragmas(self, conn, aggressive=False):
        """
        Set SQLite PRAGMA settings for faster bulk inserts.

        aggressive=False  -> Safe mode (recommended for production)
        aggressive=True   -> Maximum speed (risk of DB corruption if crash)
        """
        cur = conn.cursor()

        # Always safe speed-ups
        cur.execute("PRAGMA temp_store = MEMORY;")
        cur.execute("PRAGMA cache_size = -200000;")  # ~200MB cache
        cur.execute("PRAGMA locking_mode = EXCLUSIVE;")

        if aggressive:
            # 🚀 FASTEST (but unsafe if crash happens)
            cur.execute("PRAGMA synchronous = OFF;")
            cur.execute("PRAGMA journal_mode = OFF;")
        else:
            # ⚖ Balanced (safe for normal use)
            cur.execute("PRAGMA synchronous = NORMAL;")
            cur.execute("PRAGMA journal_mode = WAL;")

        conn.commit()

    def _ensure_shot_unique_index(self, conn, tries=10, sleep_s=0.5):
        conn.execute("PRAGMA busy_timeout = 60000;")

        for _ in range(tries):
            try:
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_shot_table_nav_station_ppc
                    ON SHOT_TABLE (nav_line_code, nav_station, post_point_code);
                """)
                conn.commit()
                return
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower():
                    raise
                time.sleep(sleep_s)

        raise sqlite3.OperationalError("database is locked while creating unique index")


    def load_shot_table_h26_stream_fast(self, file_obj, file_fk: int, chunk_size: int = 50000) -> int:
        """
        High-speed loader for H26 comma-delimited shot table with padded spaces.

        Behavior:
          - Ensures UNIQUE index exists on (nav_line_code, nav_station, post_point_code)
          - UPSERT: insert new rows, update existing rows by that unique key
          - Uses busy_timeout + retry to reduce "database is locked" failures
          - Single transaction for the import

        NOTE:
          - If duplicates already exist for (nav_line_code, nav_station, post_point_code),
            CREATE UNIQUE INDEX will fail. Clean duplicates first.
        """
        if not file_fk:
            raise ValueError("file_fk is required (NOT NULL)")
        conn = self._connect()
        text_stream = None
        try:
            # Helps SQLite wait rather than instantly error
            conn.execute("PRAGMA busy_timeout = 60000;")  # 60s

            # ---------- 1) Ensure UNIQUE index exists (outside import transaction) ----------
            # Retry index creation if DB is temporarily locked
            for _ in range(20):  # ~10 seconds total with 0.5s sleep
                try:
                    conn.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS ux_shot_table_nav_station_ppc
                        ON SHOT_TABLE (nav_line_code, nav_station, post_point_code);
                    """)
                    conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    if "locked" not in str(e).lower():
                        raise
                    time.sleep(0.5)
            else:
                raise sqlite3.OperationalError("database is locked while creating unique index")

            # ---------- 2) Fast pragmas + start import transaction ----------
            self._set_fast_import_pragmas(conn, aggressive=False)
            cur = conn.cursor()

            # Retry acquiring write lock
            for _ in range(20):  # ~10 seconds
                try:
                    cur.execute("BEGIN IMMEDIATE;")
                    break
                except sqlite3.OperationalError as e:
                    if "locked" not in str(e).lower():
                        raise
                    time.sleep(0.5)
            else:
                raise sqlite3.OperationalError("database is locked (could not start write transaction)")

            upsert_sql = """
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
            ON CONFLICT(nav_line_code, nav_station, post_point_code)
            DO UPDATE SET
                File_FK          = excluded.File_FK,
                sail_line        = excluded.sail_line,
                shot_station     = excluded.shot_station,
                shot_index       = excluded.shot_index,
                shot_status      = excluded.shot_status,
                nav_line_code    = excluded.nav_line_code,
                nav_line         = excluded.nav_line,
                attempt          = excluded.attempt,
                seq              = excluded.seq,
                post_point_code  = excluded.post_point_code,
                fire_code        = excluded.fire_code,
                gun_depth        = excluded.gun_depth,
                water_depth      = excluded.water_depth,
                shot_x           = excluded.shot_x,
                shot_y           = excluded.shot_y,
                shot_day         = excluded.shot_day,
                shot_hour        = excluded.shot_hour,
                shot_minute      = excluded.shot_minute,
                shot_second      = excluded.shot_second,
                shot_microsecond = excluded.shot_microsecond,
                shot_year        = excluded.shot_year,
                vessel           = excluded.vessel,
                array_id         = excluded.array_id,
                source_id        = excluded.source_id,
                nav_station      = excluded.nav_station,
                shot_group_id    = excluded.shot_group_id,
                elevation        = excluded.elevation
            ;
            """

            # ---------- 3) Read file as text stream ----------
            probe = file_obj.read(0)
            if isinstance(probe, (bytes, bytearray)):
                text_stream = io.TextIOWrapper(file_obj, encoding="utf-8", errors="ignore", newline="")
            else:
                text_stream = file_obj

            reader = csv.reader(text_stream, delimiter=",", skipinitialspace=True)

            processed = 0
            batch = []

            for row in reader:
                if not row:
                    continue

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
                    sail_line = self.to_int(parts[1])
                else:
                    sail_line = self.to_int(first)

                shot_station = self.to_int(row[1])
                shot_index = self.to_int(row[2])
                shot_status = self.to_int(row[3])

                post_point_code = (row[4] or "").strip() or None
                fire_code = post_point_code[0].upper() if post_point_code else None

                gun_depth = self.to_float(row[5])
                water_depth = self.to_float(row[6])

                shot_x = self.to_float(row[7])
                shot_y = self.to_float(row[8])

                shot_day = self.to_int(row[9])
                shot_hour = self.to_int(row[10])
                shot_minute = self.to_int(row[11])
                shot_second = self.to_int(row[12])
                shot_microsecond = self.to_int(row[13])
                shot_year = self.to_int(row[14])

                vessel = (row[15] or "").strip() or None
                array_id = (row[16] or "").strip() or None
                source_id = self.to_int(row[17])

                nav_line_code = (row[18] or "").strip() or None
                nav_line, attempt, seq = self.decode_nav_line(nav_line_code or "")

                nav_station = self.to_int(row[19])
                shot_group_id = self.to_int(row[20])
                elevation = self.to_float(row[21])

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
                    cur.executemany(upsert_sql, batch)
                    processed += len(batch)
                    batch.clear()

            if batch:
                cur.executemany(upsert_sql, batch)
                processed += len(batch)

            conn.commit()
            return processed

        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise

        finally:
            try:
                if text_stream is not None and hasattr(text_stream, "detach"):
                    text_stream.detach()
            except Exception:
                pass
            conn.close()
        """
        H26 loader with UPSERT (insert or update) using SQLite ON CONFLICT.
        Requires UNIQUE index already exists (run once outside this function):
          ux_shot_table_h26 ON (File_FK, sail_line, shot_station, shot_index)
        """
        if not file_fk:
            raise ValueError("file_fk is required (NOT NULL)")

        conn = self._connect()
        try:
            # Important for "database is locked"
            conn.execute("PRAGMA busy_timeout = 30000;")  # wait 30s for lock
            self._set_fast_import_pragmas(conn, aggressive=False)

            cur = conn.cursor()

            # Grab write lock early (prevents failing mid-way)
            cur.execute("BEGIN IMMEDIATE;")

            upsert_sql = """
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
            ON CONFLICT(File_FK, sail_line, shot_station, shot_index)
            DO UPDATE SET
                shot_status      = excluded.shot_status,
                nav_line_code    = excluded.nav_line_code,
                nav_line         = excluded.nav_line,
                attempt          = excluded.attempt,
                seq              = excluded.seq,
                post_point_code  = excluded.post_point_code,
                fire_code        = excluded.fire_code,
                gun_depth        = excluded.gun_depth,
                water_depth      = excluded.water_depth,
                shot_x           = excluded.shot_x,
                shot_y           = excluded.shot_y,
                shot_day         = excluded.shot_day,
                shot_hour        = excluded.shot_hour,
                shot_minute      = excluded.shot_minute,
                shot_second      = excluded.shot_second,
                shot_microsecond = excluded.shot_microsecond,
                shot_year        = excluded.shot_year,
                vessel           = excluded.vessel,
                array_id         = excluded.array_id,
                source_id        = excluded.source_id,
                nav_station      = excluded.nav_station,
                shot_group_id    = excluded.shot_group_id,
                elevation        = excluded.elevation
            ;
            """

            def to_int(x):
                x = (x or "").strip()
                return int(x) if x else None

            def to_float(x):
                x = (x or "").strip()
                return float(x) if x else None

            probe = file_obj.read(0)
            if isinstance(probe, (bytes, bytearray)):
                text_stream = io.TextIOWrapper(file_obj, encoding="utf-8", errors="ignore", newline="")
            else:
                text_stream = file_obj

            reader = csv.reader(text_stream, delimiter=",", skipinitialspace=True)

            processed = 0
            batch = []

            for row in reader:
                if not row:
                    continue

                first = (row[0] or "").strip()
                if not first or first[:1] in ("H", "h"):
                    continue
                if len(row) < 22:
                    continue

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
                    cur.executemany(upsert_sql, batch)
                    processed += len(batch)
                    batch.clear()

            if batch:
                cur.executemany(upsert_sql, batch)
                processed += len(batch)

            conn.commit()
            return processed

        except sqlite3.OperationalError as e:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                if "text_stream" in locals() and hasattr(text_stream, "detach"):
                    text_stream.detach()
            except Exception:
                pass
            conn.close()

    def import_shot_files_locked(self, files):
        conn = self._connect()
        if not getattr(self, "seq_ranges", None):
            self.load_sequence_mapping()
        try:
            cur = conn.cursor()

            cur.execute("PRAGMA journal_mode=DELETE;")
            cur.execute("PRAGMA synchronous=OFF;")
            cur.execute("PRAGMA foreign_keys=OFF;")
            cur.execute("PRAGMA temp_store=MEMORY;")
            cur.execute("PRAGMA cache_size=-300000;")
            cur.execute("PRAGMA busy_timeout=120000;")
            cur.execute("PRAGMA locking_mode=EXCLUSIVE;")

            cur.execute("BEGIN EXCLUSIVE;")

            cur.execute("DELETE FROM SHOT_TABLE")
            total = 0
            for f in files:
                file_fk = self.insert_file_record(f, conn=conn)
                inserted = self.load_shot_table_h26_replace_all_fast(
                    f.file,
                    file_fk=file_fk,
                    chunk_size=200000,
                    conn=conn,
                )
                total += inserted

            conn.commit()
            return total

        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()
    def test_db_lock(self):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode=DELETE;")
            cur.execute("BEGIN EXCLUSIVE;")
            conn.rollback()
            print("DB is FREE")
        except Exception as e:
            print(f"DB is LOCKED:{e}")
        finally:
            conn.close()
    def load_shot_table_h26_replace_all_fast(
            self,
            file_obj,
            file_fk: int,
            chunk_size: int = 50000,
            lock_timeout_s: int = 120,
            conn=None,
    ) -> int:
        """
        Fast full replace SHOT_TABLE from one H26 CSV file.

        Optimizations:
        - one exclusive transaction
        - one final commit only
        - File_FK from input
        - Seq_FK recalculated only when seq changes
        """
        print("\n[SHOT_IMPORT] started", flush=True)
        t0 = time.time()

        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        text_stream = None

        def _execute_retry(cursor, sql, params=None, timeout_s=120, sleep_s=0.5):
            t1 = time.time()
            while True:
                try:
                    if params is None:
                        return cursor.execute(sql)
                    return cursor.execute(sql, params)
                except sqlite3.OperationalError as e:
                    if "locked" not in str(e).lower():
                        raise
                    waited = time.time() - t1
                    if waited >= timeout_s:
                        raise sqlite3.OperationalError(
                            f"database is locked (waited {waited:.0f}s) during execute."
                        )
                    time.sleep(sleep_s)

        def _executemany_retry(cursor, sql, rows, timeout_s=120, sleep_s=0.5):
            t1 = time.time()
            while True:
                try:
                    return cursor.executemany(sql, rows)
                except sqlite3.OperationalError as e:
                    if "locked" not in str(e).lower():
                        raise
                    waited = time.time() - t1
                    if waited >= timeout_s:
                        raise sqlite3.OperationalError(
                            f"database is locked (waited {waited:.0f}s) during batch insert."
                        )
                    time.sleep(sleep_s)

        try:
            self.ensure_shot_table_schema(conn=conn)
            cur = conn.cursor()

            # best for full bulk reload
            try:
                cur.execute("PRAGMA journal_mode=DELETE;")
            except Exception:
                pass

            try:
                cur.execute("PRAGMA synchronous=OFF;")
            except Exception:
                pass

            try:
                cur.execute("PRAGMA foreign_keys=OFF;")
            except Exception:
                pass

            try:
                cur.execute("PRAGMA temp_store=MEMORY;")
            except Exception:
                pass

            try:
                cur.execute("PRAGMA cache_size=-300000;")
            except Exception:
                pass

            try:
                cur.execute("PRAGMA locking_mode=EXCLUSIVE;")
            except Exception:
                pass

            try:
                cur.execute(f"PRAGMA busy_timeout={int(lock_timeout_s * 1000)};")
            except Exception:
                pass



            insert_sql = """
            INSERT INTO SHOT_TABLE (
                sail_line,
                shot_station,
                shot_index,
                shot_status,
                nav_line_code,
                nav_line,
                attempt,
                seq,
                post_point_code,
                fire_code,
                gun_depth,
                water_depth,
                shot_x,
                shot_y,
                shot_day,
                shot_hour,
                shot_minute,
                shot_second,
                shot_microsecond,
                shot_year,
                vessel,
                array_id,
                source_id,
                nav_station,
                shot_group_id,
                elevation,
                File_FK,
                Seq_FK
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """

            probe = file_obj.read(0)
            if isinstance(probe, (bytes, bytearray)):
                text_stream = io.TextIOWrapper(
                    file_obj,
                    encoding="utf-8",
                    errors="ignore",
                    newline=""
                )
            else:
                text_stream = file_obj

            try:
                file_obj.seek(0)
            except Exception:
                pass

            reader = csv.reader(text_stream, delimiter=",", skipinitialspace=True)

            processed = 0
            attempted = 0
            skipped = 0
            batch = []

            decode_nav = self.decode_nav_line
            to_int = self.to_int
            to_float = self.to_float
            batch_append = batch.append
            get_seq_info = self.get_seq_info

            last_print = time.time()
            last_seq = object()
            last_seq_fk = None

            print("[SHOT_IMPORT] begin exclusive transaction...", flush=True)
            _execute_retry(cur, "BEGIN EXCLUSIVE", timeout_s=lock_timeout_s)

            print("[SHOT_IMPORT] deleting old rows...", flush=True)
            _execute_retry(cur, "DELETE FROM SHOT_TABLE", timeout_s=lock_timeout_s)

            for row in reader:
                if not row:
                    continue

                first = (row[0] or "").strip()
                if not first:
                    continue

                if first[:1] in ("H", "h"):
                    continue

                if len(row) < 22:
                    skipped += 1
                    continue

                parts = first.split()
                sail_line = (
                    to_int(parts[1])
                    if (len(parts) == 2 and parts[0].upper() == "S")
                    else to_int(first)
                )

                shot_station = to_int(row[1])
                shot_index = to_int(row[2])
                shot_status = to_int(row[3])

                post_point_code = (row[4] or "").strip().upper()
                if not post_point_code:
                    post_point_code = ""

                fire_code = post_point_code[:1] if post_point_code else ""

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

                nav_line_code = (row[18] or "").strip() or ""
                nav_line, attempt, seq = decode_nav(nav_line_code)
                attempt = "" if attempt is None else str(attempt)

                nav_station = to_int(row[19])
                if nav_station is None:
                    nav_station = 0

                shot_group_id = to_int(row[20])
                elevation = to_float(row[21])

                if seq != last_seq:
                    seq_fk, _vessel_id = get_seq_info(seq)
                    last_seq_fk = seq_fk
                    last_seq = seq

                batch_append((
                    sail_line,
                    shot_station,
                    shot_index,
                    shot_status,
                    nav_line_code,
                    nav_line,
                    attempt,
                    seq,
                    post_point_code,
                    fire_code,
                    gun_depth,
                    water_depth,
                    shot_x,
                    shot_y,
                    shot_day,
                    shot_hour,
                    shot_minute,
                    shot_second,
                    shot_microsecond,
                    shot_year,
                    vessel,
                    array_id,
                    source_id,
                    nav_station,
                    shot_group_id,
                    elevation,
                    file_fk,
                    last_seq_fk
                ))
                attempted += 1

                if len(batch) >= chunk_size:
                    _executemany_retry(cur, insert_sql, batch, timeout_s=lock_timeout_s)
                    processed += len(batch)
                    batch.clear()

                    now = time.time()
                    if now - last_print >= 2.0:
                        elapsed = now - t0
                        print(
                            f"[SHOT_IMPORT] progress inserted={processed:,} attempted={attempted:,} "
                            f"skipped={skipped:,} time={elapsed:.1f}s "
                            f"speed={attempted / max(elapsed, 1):,.0f} rows/sec",
                            flush=True
                        )
                        last_print = now

            if batch:
                _executemany_retry(cur, insert_sql, batch, timeout_s=lock_timeout_s)
                processed += len(batch)
                batch.clear()

            conn.commit()

            total_time = time.time() - t0
            print(
                f"[SHOT_IMPORT] FINISHED inserted={processed:,} attempted={attempted:,} "
                f"skipped={skipped:,} time={total_time:.1f}s "
                f"speed={attempted / max(total_time, 1):,.0f} rows/sec",
                flush=True
            )
            return processed

        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise

        finally:
            try:
                if text_stream is not None and hasattr(text_stream, "detach"):
                    text_stream.detach()
            except Exception:
                pass

            try:
                conn.execute("PRAGMA foreign_keys=ON;")
            except Exception:
                pass

            if own_conn:
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
            detect_vessel_by_seq: bool = False,
            auto_year_by_jday: bool = False,
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
                "PointCode", "Static","FireCode", "ArrayCode",
                "PointDepth", "Datum","WaterDepth", "Easting", "Northing", "Elevation",
                "JDay", "Hour", "Minute", "Second", "Microsecond",
                "Month", "Week", "Day", "Year", "YearDay",
                "TimeStamp",
            ]
            placeholders = ",".join("?" for _ in insert_cols)

            insert_sql = f"""
            INSERT INTO SPSolution ({",".join(insert_cols)})
            VALUES ({placeholders})
            """

            # Cache SLSolution IDs by SailLine (string)
            sl_cache: dict[str, int] = {}

            # Cache vessel id by seq (int) when detect mode is ON
            vessel_by_seq_cache: dict[int, int | None] = {}

            # Cache (vessel_id, purpose_id) by seq
            assignment_by_seq_cache: dict[int, tuple[int | None, int | None]] = {}

            def _lookup_assignment_by_seq_in_conn(seq_num: int) -> tuple[int | None, int | None]:
                seq_num = int(seq_num)
                if seq_num in assignment_by_seq_cache:
                    return assignment_by_seq_cache[seq_num]

                row = conn.execute(
                    """
                    SELECT vessel_id, purpose_id
                    FROM sequence_vessel_assignment
                    WHERE is_active = 1
                      AND ? BETWEEN seq_first AND seq_last
                    ORDER BY (seq_last - seq_first) ASC, id DESC
                    LIMIT 1
                    """,
                    (seq_num,),
                ).fetchone()

                vessel_id = int(row[0]) if row and row[0] is not None else None
                purpose_id = int(row[1]) if row and row[1] is not None else None

                assignment_by_seq_cache[seq_num] = (vessel_id, purpose_id)
                return vessel_id, purpose_id

            batch_tuples: list[tuple] = []
            total = 0
            skipped = 0
            lines_touched: set[int] = set()

            last_source_line: int | None = None
            last_seq: int | None = None
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


                if auto_year_by_jday:
                    now = datetime.now(timezone.utc).astimezone()  # local tz OK
                    today_year = now.year
                    today_jday = int(now.strftime("%j"))

                    try:
                        pj = int(p.jday or 0)
                    except Exception:
                        pj = 0

                    if pj > 0:
                        p.year = (today_year - 1) if pj > today_jday else today_year
                # Remember last decoded line for return payload
                last_source_line = int(p.line)
                last_seq=int(p.seq)

                # Resolve vessel FK:
                # - if detect mode ON => lookup by p.seq (decoded from SailLine)
                # - else => use passed vessel_fk
                point_vessel_fk = vessel_fk
                point_purpose_id = None

                if detect_vessel_by_seq:
                    if p.seq is None:
                        raise ValueError(f"SPS decode produced empty Seq for SailLine '{p.sail_line}'.")
                    point_vessel_fk, point_purpose_id = _lookup_assignment_by_seq_in_conn(int(p.seq))
                    if not point_vessel_fk:
                        raise ValueError(f"No vessel assignment found for Seq {int(p.seq)} (SailLine '{p.sail_line}').")

                # get/create SLSolution.ID (per SailLine)
                sl_id = sl_cache.get(p.sail_line)
                if sl_id is None:
                    sl_id = self._get_or_create_sl_solution_id(
                        conn,
                        file_fk=int(file_fk),
                        sail_line=p.sail_line,
                        line=int(p.line),
                        seq=int(p.seq or 0),
                        attempt=p.attempt,
                        tier=int(p.tier),
                        vessel_fk=point_vessel_fk,
                        purpose_id=point_purpose_id,
                    )
                    sl_cache[p.sail_line] = sl_id

                lines_touched.add(sl_id)

                # fill FK/meta into point object
                p.sail_line_fk = sl_id
                p.ppline_fk = None
                p.vessel_fk = point_vessel_fk
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
                "source_line": int(last_source_line or 0),
                "seq": int(last_seq or 0),
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

    def update_seq_maxspi(self, seq: int, production_code: str) -> dict:
        """
        Update ONLY SLSolution.MaxSPI for a selected Seq (not Line).

        MaxSPI = max distance between neighbouring production points
                 (FireCode contained in `production_code`),
                 ordered by Point, PointIdx, computed within each SailLine_FK,
                 then max across all SailLine_FK that belong to this Seq.

        Inputs:
          - seq: sequence number (SLSolution.Seq)
          - production_code: string containing allowed production fire codes
                             (same "instr(production_code, s.FireCode) > 0" logic
                              as your old function)

        Returns:
          {"seq": <seq>, "MaxSPI": <rounded max gap>}
        """
        seq = int(seq)
        prod_codes = (production_code or "").strip()

        conn = self._connect()
        try:
            cur = conn.cursor()
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("BEGIN IMMEDIATE;")

            # If no production codes -> set MaxSPI = 0 for this Seq
            if not prod_codes:
                cur.execute(
                    "UPDATE SLSolution SET MaxSPI=0 WHERE Seq=?",
                    (seq,),
                )
                conn.commit()
                return {"seq": seq, "MaxSPI": 0.0}

            # Compute MaxSPI across all sail lines for this Seq
            sql_maxspi = """
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
                WHERE l.Seq = ?
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

            maxspi = cur.execute(sql_maxspi, (seq, prod_codes)).fetchone()[0]
            maxspi = float(maxspi or 0.0)

            # Update only the selected Seq
            cur.execute(
                "UPDATE SLSolution SET MaxSPI=? WHERE Seq=?",
                (maxspi, seq),
            )

            conn.commit()
            return {"seq": seq, "MaxSPI": round(maxspi, 3)}

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
                           AND p.Point=(SELECT FGSP FROM picks x WHERE x.line_id=SLSolution.ID)
                         ORDER BY COALESCE(p.PointIdx,0) ASC LIMIT 1),
                        (SELECT Easting FROM SPSolution p
                         WHERE p.SailLine_FK=SLSolution.ID
                           AND p.Point=(SELECT fsp FROM picks x WHERE x.line_id=SLSolution.ID)
                         ORDER BY COALESCE(p.PointIdx,0) ASC LIMIT 1)
                    ),
                    StartY = COALESCE(
                        (SELECT Northing FROM SPSolution p
                         WHERE p.SailLine_FK=SLSolution.ID
                           AND p.Point=(SELECT FGSP FROM picks x WHERE x.line_id=SLSolution.ID)
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

    def delete_sps_by_ids(self, ids):
        """
        Delete SPS rows from SLSolution by primary key IDs.
        Ensures SQLite foreign key enforcement is ON for this connection.

        Args:
            ids: iterable of int/str IDs (SLSolution.ID)

        Returns:
            int: number of IDs requested for deletion (not necessarily rows deleted if some missing)
        """
        if not ids:
            return 0

        # normalize + dedupe (keep stable order)
        seen = set()
        clean_ids = []
        for x in ids:
            try:
                xi = int(x)
            except Exception:
                continue
            if xi not in seen:
                seen.add(xi)
                clean_ids.append(xi)

        if not clean_ids:
            return 0

        placeholders = ",".join("?" for _ in clean_ids)
        sql = f"DELETE FROM SLSolution WHERE ID IN ({placeholders})"

        with self._connect() as conn:
            # IMPORTANT: FK enforcement is per-connection in SQLite
            conn.execute("PRAGMA foreign_keys = ON;")

            # Do the delete inside one transaction
            cur = conn.execute(sql, clean_ids)
            conn.commit()

            # sqlite rowcount is reliable for DELETE
            return cur.rowcount

    def get_source_vessel_id_by_seq(self, seq_num: int):
        con = self._connect()
        cur = con.cursor()
        cur.execute("""
            SELECT vessel_id
            FROM sequence_vessel_assignment
            WHERE is_active = 1
              AND ? BETWEEN seq_first AND seq_last
            ORDER BY (seq_last - seq_first) ASC, id DESC
            LIMIT 1
        """, (int(seq_num),))
        row = cur.fetchone()
        con.close()
        return int(row[0]) if row else None

    def read_vessel_purpose_summary(self, file_fk: int | None = None) -> dict:
        """
        Read V_SLSolution_VesselPurposeSummary and project totals.
        Returns dict ready for Django templates.
        """

        sql_rows = """
        SELECT *
        FROM V_SLSolution_VesselPurposeSummary
        ORDER BY vessel_name, purpose_id
        """

        sql_totals = """
        SELECT
            COUNT(*) AS SailLines,
            COUNT(DISTINCT Line) AS DistinctLines,
            COUNT(DISTINCT Seq)  AS DistinctSeqs,
            COUNT(DISTINCT printf('%d|%d', COALESCE(Line,-1), COALESCE(Seq,-1))) AS DistinctLineSeq,

            SUM(COALESCE(ProductionCount,0))    AS ProductionTotal,
            SUM(COALESCE(NonProductionCount,0)) AS NonProductionTotal,
            SUM(COALESCE(KillCount,0))          AS KillTotal,

            MIN(Start_Time) AS FirstStartTime,
            MAX(End_Time)   AS LastEndTime,

            SUM(COALESCE(LineLength,0)) AS LineLengthTotal,
            MAX(COALESCE(MaxSPI,0))     AS MaxSPI,

            MIN(NULLIF(MinProdGunDepth,0))   AS MinProdGunDepth,
            MAX(NULLIF(MaxProdGunDepth,0))   AS MaxProdGunDepth,

            MIN(NULLIF(MinProdWaterDepth,0)) AS MinProdWaterDepth,
            MAX(NULLIF(MaxProdWaterDepth,0)) AS MaxProdWaterDepth

        FROM SLSolution
        """

        with self._connect() as conn:
            conn.row_factory = None
            cur = conn.cursor()

            # rows
            cur.execute(sql_rows)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]

            # totals
            cur.execute(sql_totals)
            tcols = [d[0] for d in cur.description]
            trow = cur.fetchone()
            totals = dict(zip(tcols, trow)) if trow else {}

        return {
            "rows": rows,
            "totals": totals
        }
    def check_db_lock(self):
        conn = sqlite3.connect(self.db_path, timeout=1)
        cur = conn.cursor()

        try:
            cur.execute("BEGIN IMMEDIATE")
            conn.rollback()
            print("DB is FREE")
        except sqlite3.OperationalError as e:
            print("DB LOCKED:", e)

        conn.close()
    def ensure_shot_table_schema(self, conn=None):
        """
        Ensure SHOT_TABLE exists with the expected schema.
        If schema differs, the table will be dropped and recreated.
        """

        expected_cols = [
            "id",
            "sail_line",
            "shot_station",
            "shot_index",
            "shot_status",
            "nav_line_code",
            "nav_line",
            "attempt",
            "seq",
            "post_point_code",
            "fire_code",
            "gun_depth",
            "water_depth",
            "shot_x",
            "shot_y",
            "shot_day",
            "shot_hour",
            "shot_minute",
            "shot_second",
            "shot_microsecond",
            "shot_year",
            "vessel",
            "array_id",
            "source_id",
            "nav_station",
            "shot_group_id",
            "elevation",
            "File_FK",
            "Seq_FK"
        ]

        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            cur = conn.cursor()

            # check if table exists
            row = cur.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                  AND name='SHOT_TABLE'
            """).fetchone()

            recreate = False

            if row:
                # read existing columns
                cols = cur.execute("PRAGMA table_info(SHOT_TABLE)").fetchall()
                existing_cols = [c[1] for c in cols]

                if existing_cols != expected_cols:
                    print("[SHOT_TABLE] schema mismatch, recreating table")
                    recreate = True
            else:
                recreate = True

            if recreate:
                cur.execute("DROP TABLE IF EXISTS SHOT_TABLE")

                cur.execute("""
                CREATE TABLE IF NOT EXISTS SHOT_TABLE (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      sail_line INTEGER,
                      shot_station INTEGER,
                      shot_index INTEGER,
                      shot_status INTEGER,
                      
                      nav_line_code TEXT,
                      
                      nav_line INTEGER,
                      attempt TEXT,
                      seq INTEGER,
                      
                      post_point_code TEXT,
                      fire_code TEXT,
                      
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
                      
                      nav_station INTEGER,
                      shot_group_id INTEGER,
                      elevation REAL,
                      
                      File_FK INTEGER,
                      Seq_FK INTEGER,
                      
                      FOREIGN KEY (File_FK) REFERENCES Files(ID) ON DELETE SET NULL,
                      FOREIGN KEY (Seq_FK) REFERENCES sequence_vessel_assignment(ID) ON DELETE SET NULL);                    
                """)

                print("[SHOT_TABLE] created")

            conn.commit()

        finally:
            if own_conn:
                conn.close()

    def create_v_shot_linesummary_view(self, conn=None) -> None:
        """
        Rebuild V_SHOT_LineSummary view.

        Adds:
            - sequence_vessel_assignment.purpose
            - sequence_vessel_assignment.vessel_id
            - project_fleet.vessel_name

        Join logic:
            a.seq BETWEEN sva.seq_first AND sva.seq_last

        Notes:
            - If sequence_vessel_assignment has overlapping seq ranges,
              the view can return duplicate rows for the same shot line.
            - In SQLite, unicode(text) uses only the first character.
              Kept as-is because it matches your existing logic.
        """
        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            cur = conn.cursor()

            cur.executescript("""
            DROP VIEW IF EXISTS V_SHOT_LineSummary;

            CREATE VIEW V_SHOT_LineSummary AS
            WITH
            pg AS (
                SELECT
                    UPPER(COALESCE(production_code,''))      AS prod_codes,
                    UPPER(COALESCE(non_production_code,''))  AS nonprod_codes,
                    UPPER(COALESCE(kill_code,''))            AS kill_codes
                FROM project_geometry
                LIMIT 1
            ),

            -- ===============================
            -- SHOT base
            -- ===============================
            shot_base AS (
                SELECT
                    s.nav_line_code,
                    s.nav_line,
                    s.attempt,
                    s.seq,

                    s.shot_station,
                    s.shot_index,
                    s.shot_status,

                    s.post_point_code,
                    s.fire_code,

                    s.gun_depth,
                    s.water_depth,

                    s.shot_x,
                    s.shot_y,

                    s.shot_day,
                    s.shot_hour,
                    s.shot_minute,
                    s.shot_second,
                    s.shot_microsecond,
                    s.shot_year,

                    s.array_id,
                    s.source_id,
                    s.nav_station,
                    s.shot_group_id,
                    s.elevation,

                    CASE WHEN INSTR(pg.prod_codes,    UPPER(COALESCE(s.fire_code,''))) > 0 THEN 1 ELSE 0 END AS is_prod,
                    CASE WHEN INSTR(pg.nonprod_codes, UPPER(COALESCE(s.fire_code,''))) > 0 THEN 1 ELSE 0 END AS is_nonprod,
                    CASE WHEN INSTR(pg.kill_codes,    UPPER(COALESCE(s.fire_code,''))) > 0 THEN 1 ELSE 0 END AS is_kill
                FROM SHOT_TABLE s
                CROSS JOIN pg
                WHERE s.nav_line_code IS NOT NULL
                  AND TRIM(s.nav_line_code) <> ''
            ),

            -- ===============================
            -- SHOT aggregation
            -- ===============================
            shot_agg AS (
                SELECT
                    nav_line_code,

                    MAX(nav_line) AS nav_line,
                    MAX(attempt)  AS attempt,
                    MAX(seq)      AS seq,

                    COUNT(*) AS ShotCount,

                    SUM(is_prod)    AS ProdShots,
                    SUM(is_nonprod) AS NonProdShots,
                    SUM(is_kill)    AS KillShots,

                    MIN(nav_station) AS FSP,
                    MAX(nav_station) AS LSP,
                    MIN(CASE WHEN is_prod=1 THEN nav_station END) AS FGSP,
                    MAX(CASE WHEN is_prod=1 THEN nav_station END) AS LGSP,

                    -- SHOT sums / checksums
                    SUM(shot_station)     AS Sum_shot_station,
                    SUM(shot_index)       AS Sum_shot_index,
                    SUM(shot_status)      AS Sum_shot_status,

                    SUM(attempt)          AS Sum_attempt,
                    SUM(seq)              AS Sum_seq,

                    SUM(unicode(post_point_code)) AS Sum_post_point_code,
                    SUM(unicode(fire_code))       AS Sum_fire_code,

                    SUM(gun_depth)        AS Sum_gun_depth,
                    SUM(water_depth)      AS Sum_water_depth,

                    SUM(shot_x)           AS Sum_shot_x,
                    SUM(shot_y)           AS Sum_shot_y,

                    SUM(shot_day)         AS Sum_shot_day,
                    SUM(shot_hour)        AS Sum_shot_hour,
                    SUM(shot_minute)      AS Sum_shot_minute,
                    SUM(shot_second)      AS Sum_shot_second,
                    SUM(shot_microsecond) AS Sum_shot_microsecond,
                    SUM(shot_year)        AS Sum_shot_year,

                    SUM(unicode(array_id)) AS Sum_array_id,

                    SUM(source_id)     AS Sum_source_id,
                    SUM(nav_station)   AS Sum_nav_station,
                    SUM(shot_group_id) AS Sum_shot_group_id,
                    SUM(elevation)     AS Sum_elevation

                FROM shot_base
                GROUP BY nav_line_code
            ),

            -- ===============================
            -- SPSolution aggregation
            -- ===============================
            sps_agg AS (
                SELECT
                    SailLine,
                    Line,
                    Attempt,
                    Seq,

                    SUM(Line)    AS sps_Sum_Line,
                    SUM(Attempt) AS sps_Sum_Attempt,
                    SUM(Seq)     AS sps_Sum_Seq,
                    SUM(Point)   AS sps_Sum_Point,

                    SUM(unicode(PointCode)) AS sps_Sum_PointCode,
                    SUM(unicode(FireCode))  AS sps_Sum_FireCode,
                    SUM(unicode(ArrayCode)) AS sps_Sum_ArrayCode,

                    SUM(Static)     AS sps_Sum_Static,
                    SUM(PointDepth) AS sps_Sum_PointDepth,
                    SUM(Datum)      AS sps_Sum_Datum,
                    SUM(Uphole)     AS sps_Sum_Uphole,
                    SUM(WaterDepth) AS sps_Sum_WaterDepth,

                    SUM(Easting)   AS sps_Sum_Easting,
                    SUM(Northing)  AS sps_Sum_Northing,
                    SUM(Elevation) AS sps_Sum_Elevation,

                    SUM(JDay)        AS sps_Sum_JDay,
                    SUM(Hour)        AS sps_Sum_Hour,
                    SUM(Minute)      AS sps_Sum_Minute,
                    SUM(Second)      AS sps_Sum_Second,
                    SUM(Microsecond) AS sps_Sum_Microsecond

                FROM SPSolution
                GROUP BY SailLine, Line, Attempt, Seq
            )

            -- ===============================
            -- FINAL SELECT
            -- ===============================
            SELECT
                a.nav_line_code,
                a.nav_line,
                a.attempt,
                a.seq,

                sva.purpose,
                sva.vessel_id,
                pf.vessel_name,

                CASE WHEN EXISTS (
                    SELECT 1
                    FROM SLSolution sl
                    WHERE sl.SailLine = a.nav_line_code
                ) THEN 1 ELSE 0 END AS IsInSLSolution,

                a.ShotCount,
                a.ProdShots,
                a.NonProdShots,
                a.KillShots,

                a.FSP,
                a.LSP,
                a.FGSP,
                a.LGSP,

                -- SHOT sums
                a.Sum_shot_station,
                a.Sum_shot_index,
                a.Sum_shot_status,
                a.Sum_attempt,
                a.Sum_seq,
                a.Sum_post_point_code,
                a.Sum_fire_code,
                a.Sum_gun_depth,
                a.Sum_water_depth,
                a.Sum_shot_x,
                a.Sum_shot_y,
                a.Sum_shot_day,
                a.Sum_shot_hour,
                a.Sum_shot_minute,
                a.Sum_shot_second,
                a.Sum_shot_microsecond,
                a.Sum_shot_year,
                a.Sum_array_id,
                a.Sum_source_id,
                a.Sum_nav_station,
                a.Sum_shot_group_id,
                a.Sum_elevation,

                -- SPS sums
                s.SailLine AS sps_SailLine,
                s.sps_Sum_Line,
                s.sps_Sum_Attempt,
                s.sps_Sum_Seq,
                s.sps_Sum_Point,
                s.sps_Sum_PointCode,
                s.sps_Sum_FireCode,
                s.sps_Sum_ArrayCode,
                s.sps_Sum_Static,
                s.sps_Sum_PointDepth,
                s.sps_Sum_Datum,
                s.sps_Sum_Uphole,
                s.sps_Sum_WaterDepth,
                s.sps_Sum_Easting,
                s.sps_Sum_Northing,
                s.sps_Sum_Elevation,
                s.sps_Sum_JDay,
                s.sps_Sum_Hour,
                s.sps_Sum_Minute,
                s.sps_Sum_Second,
                s.sps_Sum_Microsecond,

                -- ---------------------------------
                -- Comparisons (1=match, 0=mismatch)
                -- ---------------------------------
                CASE WHEN COALESCE(a.nav_line,0)             = COALESCE(s.sps_Sum_Line, -999999999)        THEN 1 ELSE 0 END AS cmp_Line,
                CASE WHEN COALESCE(a.Sum_attempt,0)          = COALESCE(s.sps_Sum_Attempt, -999999999)     THEN 1 ELSE 0 END AS cmp_Attempt,
                CASE WHEN COALESCE(a.Sum_seq,0)              = COALESCE(s.sps_Sum_Seq, -999999999)         THEN 1 ELSE 0 END AS cmp_Seq,

                CASE WHEN COALESCE(a.Sum_nav_station,0)      = COALESCE(s.sps_Sum_Point, -999999999)       THEN 1 ELSE 0 END AS cmp_Point,

                CASE WHEN COALESCE(a.Sum_post_point_code,0)  = COALESCE(s.sps_Sum_PointCode, -999999999)   THEN 1 ELSE 0 END AS cmp_PointCode,
                CASE WHEN COALESCE(a.Sum_fire_code,0)        = COALESCE(s.sps_Sum_FireCode, -999999999)    THEN 1 ELSE 0 END AS cmp_FireCode,

                CASE WHEN COALESCE(a.Sum_water_depth,0)      = COALESCE(s.sps_Sum_WaterDepth, -999999999)  THEN 1 ELSE 0 END AS cmp_WaterDepth,
                CASE WHEN COALESCE(a.Sum_shot_x,0)           = COALESCE(s.sps_Sum_Easting, -999999999)     THEN 1 ELSE 0 END AS cmp_Easting,
                CASE WHEN COALESCE(a.Sum_shot_y,0)           = COALESCE(s.sps_Sum_Northing, -999999999)    THEN 1 ELSE 0 END AS cmp_Northing,
                CASE WHEN COALESCE(a.Sum_elevation,0)        = COALESCE(s.sps_Sum_Elevation, -999999999)   THEN 1 ELSE 0 END AS cmp_Elevation,

                CASE WHEN COALESCE(a.Sum_shot_day,0)         = COALESCE(s.sps_Sum_JDay, -999999999)        THEN 1 ELSE 0 END AS cmp_JDay,
                CASE WHEN COALESCE(a.Sum_shot_hour,0)        = COALESCE(s.sps_Sum_Hour, -999999999)        THEN 1 ELSE 0 END AS cmp_Hour,
                CASE WHEN COALESCE(a.Sum_shot_minute,0)      = COALESCE(s.sps_Sum_Minute, -999999999)      THEN 1 ELSE 0 END AS cmp_Minute,
                CASE WHEN COALESCE(a.Sum_shot_second,0)      = COALESCE(s.sps_Sum_Second, -999999999)      THEN 1 ELSE 0 END AS cmp_Second,
                CASE WHEN COALESCE(a.Sum_shot_microsecond,0) = COALESCE(s.sps_Sum_Microsecond, -999999999) THEN 1 ELSE 0 END AS cmp_Microsecond,

                -- diffs (SHOT - SPS)
                (COALESCE(a.Sum_attempt,0)          - COALESCE(s.sps_Sum_Attempt,0))      AS diff_Attempt,
                (COALESCE(a.Sum_seq,0)              - COALESCE(s.sps_Sum_Seq,0))          AS diff_Seq,
                (COALESCE(a.Sum_nav_station,0)      - COALESCE(s.sps_Sum_Point,0))        AS diff_Point,
                (COALESCE(a.Sum_post_point_code,0)  - COALESCE(s.sps_Sum_PointCode,0))    AS diff_PointCode,
                (COALESCE(a.Sum_fire_code,0)        - COALESCE(s.sps_Sum_FireCode,0))     AS diff_FireCode,
                (COALESCE(a.Sum_water_depth,0)      - COALESCE(s.sps_Sum_WaterDepth,0))   AS diff_WaterDepth,
                (COALESCE(a.Sum_shot_x,0)           - COALESCE(s.sps_Sum_Easting,0))      AS diff_Easting,
                (COALESCE(a.Sum_shot_y,0)           - COALESCE(s.sps_Sum_Northing,0))     AS diff_Northing,
                (COALESCE(a.Sum_elevation,0)        - COALESCE(s.sps_Sum_Elevation,0))    AS diff_Elevation,
                (COALESCE(a.Sum_shot_day,0)         - COALESCE(s.sps_Sum_JDay,0))         AS diff_JDay,
                (COALESCE(a.Sum_shot_hour,0)        - COALESCE(s.sps_Sum_Hour,0))         AS diff_Hour,
                (COALESCE(a.Sum_shot_minute,0)      - COALESCE(s.sps_Sum_Minute,0))       AS diff_Minute,
                (COALESCE(a.Sum_shot_second,0)      - COALESCE(s.sps_Sum_Second,0))       AS diff_Second,
                (COALESCE(a.Sum_shot_microsecond,0) - COALESCE(s.sps_Sum_Microsecond,0))  AS diff_Microsecond,

                -- Sum of absolute diffs
                ABS(COALESCE(a.Sum_attempt,0)          - COALESCE(s.sps_Sum_Attempt,0)) +
                ABS(COALESCE(a.Sum_seq,0)              - COALESCE(s.sps_Sum_Seq,0)) +
                ABS(COALESCE(a.Sum_nav_station,0)      - COALESCE(s.sps_Sum_Point,0)) +
                ABS(COALESCE(a.Sum_post_point_code,0)  - COALESCE(s.sps_Sum_PointCode,0)) +
                ABS(COALESCE(a.Sum_fire_code,0)        - COALESCE(s.sps_Sum_FireCode,0)) +
                ABS(COALESCE(a.Sum_water_depth,0)      - COALESCE(s.sps_Sum_WaterDepth,0)) +
                ABS(COALESCE(a.Sum_shot_x,0)           - COALESCE(s.sps_Sum_Easting,0)) +
                ABS(COALESCE(a.Sum_shot_y,0)           - COALESCE(s.sps_Sum_Northing,0)) +
                ABS(COALESCE(a.Sum_elevation,0)        - COALESCE(s.sps_Sum_Elevation,0)) +
                ABS(COALESCE(a.Sum_shot_day,0)         - COALESCE(s.sps_Sum_JDay,0)) +
                ABS(COALESCE(a.Sum_shot_hour,0)        - COALESCE(s.sps_Sum_Hour,0)) +
                ABS(COALESCE(a.Sum_shot_minute,0)      - COALESCE(s.sps_Sum_Minute,0)) +
                ABS(COALESCE(a.Sum_shot_second,0)      - COALESCE(s.sps_Sum_Second,0)) +
                ABS(COALESCE(a.Sum_shot_microsecond,0) - COALESCE(s.sps_Sum_Microsecond,0))
                AS SumDiff,

                CASE WHEN
                    (COALESCE(a.nav_line,0)             = COALESCE(s.sps_Sum_Line, -999999999)) AND
                    (COALESCE(a.Sum_attempt,0)          = COALESCE(s.sps_Sum_Attempt, -999999999)) AND
                    (COALESCE(a.Sum_seq,0)              = COALESCE(s.sps_Sum_Seq, -999999999)) AND
                    (COALESCE(a.Sum_nav_station,0)      = COALESCE(s.sps_Sum_Point, -999999999)) AND
                    (COALESCE(a.Sum_post_point_code,0)  = COALESCE(s.sps_Sum_PointCode, -999999999)) AND
                    (COALESCE(a.Sum_fire_code,0)        = COALESCE(s.sps_Sum_FireCode, -999999999)) AND
                    (COALESCE(a.Sum_water_depth,0)      = COALESCE(s.sps_Sum_WaterDepth, -999999999)) AND
                    (COALESCE(a.Sum_shot_x,0)           = COALESCE(s.sps_Sum_Easting, -999999999)) AND
                    (COALESCE(a.Sum_shot_y,0)           = COALESCE(s.sps_Sum_Northing, -999999999)) AND
                    (COALESCE(a.Sum_elevation,0)        = COALESCE(s.sps_Sum_Elevation, -999999999)) AND
                    (COALESCE(a.Sum_shot_day,0)         = COALESCE(s.sps_Sum_JDay, -999999999)) AND
                    (COALESCE(a.Sum_shot_hour,0)        = COALESCE(s.sps_Sum_Hour, -999999999)) AND
                    (COALESCE(a.Sum_shot_minute,0)      = COALESCE(s.sps_Sum_Minute, -999999999)) AND
                    (COALESCE(a.Sum_shot_second,0)      = COALESCE(s.sps_Sum_Second, -999999999)) AND
                    (COALESCE(a.Sum_shot_microsecond,0) = COALESCE(s.sps_Sum_Microsecond, -999999999))
                THEN 1 ELSE 0 END AS QC_AllMatch,

                CASE WHEN
                     (COALESCE(a.nav_line,0)             = COALESCE(s.sps_Sum_Line, -999999999))
                  OR (COALESCE(a.Sum_attempt,0)          = COALESCE(s.sps_Sum_Attempt, -999999999))
                  OR (COALESCE(a.Sum_seq,0)              = COALESCE(s.sps_Sum_Seq, -999999999))
                  OR (COALESCE(a.Sum_nav_station,0)      = COALESCE(s.sps_Sum_Point, -999999999))
                  OR (COALESCE(a.Sum_post_point_code,0)  = COALESCE(s.sps_Sum_PointCode, -999999999))
                  OR (COALESCE(a.Sum_fire_code,0)        = COALESCE(s.sps_Sum_FireCode, -999999999))
                  OR (COALESCE(a.Sum_water_depth,0)      = COALESCE(s.sps_Sum_WaterDepth, -999999999))
                  OR (COALESCE(a.Sum_shot_x,0)           = COALESCE(s.sps_Sum_Easting, -999999999))
                  OR (COALESCE(a.Sum_shot_y,0)           = COALESCE(s.sps_Sum_Northing, -999999999))
                  OR (COALESCE(a.Sum_elevation,0)        = COALESCE(s.sps_Sum_Elevation, -999999999))
                  OR (COALESCE(a.Sum_shot_day,0)         = COALESCE(s.sps_Sum_JDay, -999999999))
                  OR (COALESCE(a.Sum_shot_hour,0)        = COALESCE(s.sps_Sum_Hour, -999999999))
                  OR (COALESCE(a.Sum_shot_minute,0)      = COALESCE(s.sps_Sum_Minute, -999999999))
                  OR (COALESCE(a.Sum_shot_second,0)      = COALESCE(s.sps_Sum_Second, -999999999))
                  OR (COALESCE(a.Sum_shot_microsecond,0) = COALESCE(s.sps_Sum_Microsecond, -999999999))
                THEN 1 ELSE 0 END AS QC_AnyMatch

            FROM shot_agg a
            LEFT JOIN sps_agg s
                ON s.Line    = a.nav_line
               AND s.Attempt = a.attempt
               AND s.Seq     = a.seq
            LEFT JOIN sequence_vessel_assignment sva
                ON a.seq BETWEEN sva.seq_first AND sva.seq_last
            LEFT JOIN project_fleet pf
                ON pf.id = sva.vessel_id;
            """)

            if own_conn:
                conn.commit()

        finally:
            if own_conn:
                conn.close()

    def get_sps_shot_compare_by_sailline(self, sailline: str, mismatches_only: bool = False):
        sql = """
        SELECT
            sps.SailLine,
            sps.Line,
            sps.Attempt,
            sps.Seq,
            sps.Point,

            sps.SailLine       AS sps_SailLine,
            st.nav_line_code   AS shot_nav_line_code,
            CASE
                WHEN sps.SailLine IS NULL AND st.nav_line_code IS NULL THEN 0
                WHEN sps.SailLine IS NULL OR st.nav_line_code IS NULL THEN 1
                WHEN sps.SailLine = st.nav_line_code THEN 0
                ELSE 1
            END AS diff_SailLine,

            sps.Line           AS sps_Line,
            st.nav_line        AS shot_nav_line,
            CASE
                WHEN sps.Line IS NULL AND st.nav_line IS NULL THEN 0
                WHEN sps.Line IS NULL OR st.nav_line IS NULL THEN 1
                WHEN sps.Line = st.nav_line THEN 0
                ELSE 1
            END AS diff_Line,

            sps.Attempt        AS sps_Attempt,
            st.shot_index      AS shot_shot_index,
            CASE
                WHEN sps.Attempt IS NULL AND st.shot_index IS NULL THEN 0
                WHEN sps.Attempt IS NULL OR st.shot_index IS NULL THEN 1
                WHEN sps.Attempt = st.shot_index THEN 0
                ELSE 1
            END AS diff_Attempt,

            sps.Seq            AS sps_Seq,
            st.seq             AS shot_seq,
            CASE
                WHEN sps.Seq IS NULL AND st.seq IS NULL THEN 0
                WHEN sps.Seq IS NULL OR st.seq IS NULL THEN 1
                WHEN sps.Seq = st.seq THEN 0
                ELSE 1
            END AS diff_Seq,

            sps.PointIdx       AS sps_PointIdx,
            st.array_id        AS shot_array_id,
            CASE
                WHEN sps.PointIdx IS NULL AND st.array_id IS NULL THEN 0
                WHEN sps.PointIdx IS NULL OR st.array_id IS NULL THEN 1
                WHEN sps.PointIdx = st.array_id THEN 0
                ELSE 1
            END AS diff_PointIdx,

            sps.Point          AS sps_Point,
            st.nav_station     AS shot_nav_station,
            CASE
                WHEN sps.Point IS NULL AND st.nav_station IS NULL THEN 0
                WHEN sps.Point IS NULL OR st.nav_station IS NULL THEN 1
                WHEN sps.Point = st.nav_station THEN 0
                ELSE 1
            END AS diff_Point

        FROM SPSolution sps
        LEFT JOIN SHOT_TABLE st
            ON sps.SailLine = st.nav_line_code
            AND sps.Seq = st.seq
            AND sps.Point =  st.nav_station 
        WHERE sps.SailLine = ?
        """

        if mismatches_only:
            sql += """
            AND (
                st.id IS NULL OR
                sps.Line != st.nav_line OR
                sps.Attempt != st.shot_index OR
                sps.Seq != st.seq OR
                sps.Point != st.nav_station OR
                sps.PointCode != st.post_point_code OR
                sps.FireCode != st.fire_code OR
                sps.ArrayCode != st.source_id
            )
            """

        sql += " ORDER BY sps.Line, sps.Seq, sps.Point, sps.Attempt"

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(sql, [sailline])
            columns = [col[0] for col in cur.description]
            rows = cur.fetchall()
        finally:
            try:
                cur.close()
            except:
                pass
            conn.close()

        return [dict(zip(columns, row)) for row in rows]

    def create_shot_linesummary_table(self, conn=None) -> None:
        """
        Full rebuild of SHOT_LineSummary materialized table.

        Fix:
            - diff_Attempt compares actual grouped attempt value
              instead of SUM(attempt), which was wrong.
        """
        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            cur = conn.cursor()

            cur.executescript("""
           CREATE TABLE IF NOT EXISTS SHOT_LineSummary AS
            WITH
            pg AS (
                SELECT
                    UPPER(COALESCE(production_code,''))      AS prod_codes,
                    UPPER(COALESCE(non_production_code,''))  AS nonprod_codes,
                    UPPER(COALESCE(kill_code,''))            AS kill_codes
                FROM project_geometry
                LIMIT 1
            ),

            shot_base AS (
                SELECT
                    s.nav_line_code,
                    s.nav_line,
                    s.attempt,
                    s.seq,

                    s.shot_station,
                    s.shot_index,
                    s.shot_status,

                    s.post_point_code,
                    s.fire_code,

                    s.gun_depth,
                    s.water_depth,

                    s.shot_x,
                    s.shot_y,

                    s.shot_day,
                    s.shot_hour,
                    s.shot_minute,
                    s.shot_second,
                    s.shot_microsecond,
                    s.shot_year,

                    s.array_id,
                    s.source_id,
                    s.nav_station,
                    s.shot_group_id,
                    s.elevation,

                    CASE WHEN INSTR(pg.prod_codes,    UPPER(COALESCE(s.fire_code,''))) > 0 THEN 1 ELSE 0 END AS is_prod,
                    CASE WHEN INSTR(pg.nonprod_codes, UPPER(COALESCE(s.fire_code,''))) > 0 THEN 1 ELSE 0 END AS is_nonprod,
                    CASE WHEN INSTR(pg.kill_codes,    UPPER(COALESCE(s.fire_code,''))) > 0 THEN 1 ELSE 0 END AS is_kill
                FROM SHOT_TABLE s
                CROSS JOIN pg
                WHERE s.nav_line_code IS NOT NULL
                  AND TRIM(s.nav_line_code) <> ''
            ),

            shot_agg AS (
                SELECT
                    nav_line_code,

                    MAX(nav_line) AS nav_line,
                    MAX(attempt)  AS attempt,
                    MAX(seq)      AS seq,

                    COUNT(*) AS ShotCount,

                    SUM(is_prod)    AS ProdShots,
                    SUM(is_nonprod) AS NonProdShots,
                    SUM(is_kill)    AS KillShots,

                    MIN(nav_station) AS FSP,
                    MAX(nav_station) AS LSP,
                    MIN(CASE WHEN is_prod=1 THEN nav_station END) AS FGSP,
                    MAX(CASE WHEN is_prod=1 THEN nav_station END) AS LGSP,

                    SUM(shot_station)     AS Sum_shot_station,
                    SUM(shot_index)       AS Sum_shot_index,
                    SUM(shot_status)      AS Sum_shot_status,
                    SUM(seq)              AS Sum_seq,

                    SUM(unicode(post_point_code)) AS Sum_post_point_code,
                    SUM(unicode(fire_code))       AS Sum_fire_code,

                    SUM(gun_depth)        AS Sum_gun_depth,
                    SUM(water_depth)      AS Sum_water_depth,
                    SUM(shot_x)           AS Sum_shot_x,
                    SUM(shot_y)           AS Sum_shot_y,

                    SUM(shot_day)         AS Sum_shot_day,
                    SUM(shot_hour)        AS Sum_shot_hour,
                    SUM(shot_minute)      AS Sum_shot_minute,
                    SUM(shot_second)      AS Sum_shot_second,
                    SUM(shot_microsecond) AS Sum_shot_microsecond,
                    SUM(shot_year)        AS Sum_shot_year,

                    SUM(unicode(array_id)) AS Sum_array_id,

                    SUM(source_id)     AS Sum_source_id,
                    SUM(nav_station)   AS Sum_nav_station,
                    SUM(shot_group_id) AS Sum_shot_group_id,
                    SUM(elevation)     AS Sum_elevation

                FROM shot_base
                GROUP BY nav_line_code
            ),

            sps_agg AS (
                SELECT
                    SailLine,
                    Line,
                    Attempt,
                    Seq,

                    SUM(Line)    AS sps_Sum_Line,
                    SUM(Seq)     AS sps_Sum_Seq,
                    SUM(Point)   AS sps_Sum_Point,

                    SUM(unicode(PointCode)) AS sps_Sum_PointCode,
                    SUM(unicode(FireCode))  AS sps_Sum_FireCode,
                    SUM(unicode(ArrayCode)) AS sps_Sum_ArrayCode,

                    SUM(Static)     AS sps_Sum_Static,
                    SUM(PointDepth) AS sps_Sum_PointDepth,
                    SUM(Datum)      AS sps_Sum_Datum,
                    SUM(Uphole)     AS sps_Sum_Uphole,
                    SUM(WaterDepth) AS sps_Sum_WaterDepth,

                    SUM(Easting)   AS sps_Sum_Easting,
                    SUM(Northing)  AS sps_Sum_Northing,
                    SUM(Elevation) AS sps_Sum_Elevation,

                    SUM(JDay)        AS sps_Sum_JDay,
                    SUM(Hour)        AS sps_Sum_Hour,
                    SUM(Minute)      AS sps_Sum_Minute,
                    SUM(Second)      AS sps_Sum_Second,
                    SUM(Microsecond) AS sps_Sum_Microsecond
                FROM SPSolution
                GROUP BY SailLine, Line, Attempt, Seq
            )

            SELECT
                a.nav_line_code,
                a.nav_line,
                a.attempt,
                a.seq,

                sva.purpose,
                sva.vessel_id,
                pf.vessel_name,

                CASE WHEN EXISTS (
                    SELECT 1
                    FROM SLSolution sl
                    WHERE sl.SailLine = a.nav_line_code
                ) THEN 1 ELSE 0 END AS IsInSLSolution,

                a.ShotCount,
                a.ProdShots,
                a.NonProdShots,
                a.KillShots,

                a.FSP,
                a.LSP,
                a.FGSP,
                a.LGSP,

                a.Sum_shot_station,
                a.Sum_shot_index,
                a.Sum_shot_status,
                a.Sum_seq,
                a.Sum_post_point_code,
                a.Sum_fire_code,
                a.Sum_gun_depth,
                a.Sum_water_depth,
                a.Sum_shot_x,
                a.Sum_shot_y,
                a.Sum_shot_day,
                a.Sum_shot_hour,
                a.Sum_shot_minute,
                a.Sum_shot_second,
                a.Sum_shot_microsecond,
                a.Sum_shot_year,
                a.Sum_array_id,
                a.Sum_source_id,
                a.Sum_nav_station,
                a.Sum_shot_group_id,
                a.Sum_elevation,

                s.SailLine AS sps_SailLine,
                s.Line AS sps_Line,
                s.Attempt AS sps_Attempt,
                s.Seq AS sps_Seq,
                s.sps_Sum_Line,
                s.sps_Sum_Seq,
                s.sps_Sum_Point,
                s.sps_Sum_PointCode,
                s.sps_Sum_FireCode,
                s.sps_Sum_ArrayCode,
                s.sps_Sum_Static,
                s.sps_Sum_PointDepth,
                s.sps_Sum_Datum,
                s.sps_Sum_Uphole,
                s.sps_Sum_WaterDepth,
                s.sps_Sum_Easting,
                s.sps_Sum_Northing,
                s.sps_Sum_Elevation,
                s.sps_Sum_JDay,
                s.sps_Sum_Hour,
                s.sps_Sum_Minute,
                s.sps_Sum_Second,
                s.sps_Sum_Microsecond,

                CASE WHEN COALESCE(a.nav_line,0) = COALESCE(s.sps_Sum_Line, -999999999)
                    THEN 1 ELSE 0 END AS cmp_Line,

                CASE WHEN COALESCE(CAST(a.attempt AS TEXT), '') = COALESCE(CAST(s.Attempt AS TEXT), '')
                    THEN 1 ELSE 0 END AS cmp_Attempt,

                CASE WHEN COALESCE(a.Sum_seq,0) = COALESCE(s.sps_Sum_Seq, -999999999)
                    THEN 1 ELSE 0 END AS cmp_Seq,

                CASE WHEN COALESCE(a.Sum_nav_station,0) = COALESCE(s.sps_Sum_Point, -999999999)
                    THEN 1 ELSE 0 END AS cmp_Point,

                CASE WHEN COALESCE(a.Sum_post_point_code,0) = COALESCE(s.sps_Sum_PointCode, -999999999)
                    THEN 1 ELSE 0 END AS cmp_PointCode,

                CASE WHEN COALESCE(a.Sum_fire_code,0) = COALESCE(s.sps_Sum_FireCode, -999999999)
                    THEN 1 ELSE 0 END AS cmp_FireCode,

                CASE WHEN COALESCE(a.Sum_water_depth,0) = COALESCE(s.sps_Sum_WaterDepth, -999999999)
                    THEN 1 ELSE 0 END AS cmp_WaterDepth,

                CASE WHEN COALESCE(a.Sum_shot_x,0) = COALESCE(s.sps_Sum_Easting, -999999999)
                    THEN 1 ELSE 0 END AS cmp_Easting,

                CASE WHEN COALESCE(a.Sum_shot_y,0) = COALESCE(s.sps_Sum_Northing, -999999999)
                    THEN 1 ELSE 0 END AS cmp_Northing,

                CASE WHEN COALESCE(a.Sum_elevation,0) = COALESCE(s.sps_Sum_Elevation, -999999999)
                    THEN 1 ELSE 0 END AS cmp_Elevation,

                CASE WHEN COALESCE(a.Sum_shot_day,0) = COALESCE(s.sps_Sum_JDay, -999999999)
                    THEN 1 ELSE 0 END AS cmp_JDay,

                CASE WHEN COALESCE(a.Sum_shot_hour,0) = COALESCE(s.sps_Sum_Hour, -999999999)
                    THEN 1 ELSE 0 END AS cmp_Hour,

                CASE WHEN COALESCE(a.Sum_shot_minute,0) = COALESCE(s.sps_Sum_Minute, -999999999)
                    THEN 1 ELSE 0 END AS cmp_Minute,

                CASE WHEN COALESCE(a.Sum_shot_second,0) = COALESCE(s.sps_Sum_Second, -999999999)
                    THEN 1 ELSE 0 END AS cmp_Second,

                CASE WHEN COALESCE(a.Sum_shot_microsecond,0) = COALESCE(s.sps_Sum_Microsecond, -999999999)
                    THEN 1 ELSE 0 END AS cmp_Microsecond,

                CASE
                    WHEN s.Line IS NULL THEN 1
                    WHEN COALESCE(CAST(a.attempt AS TEXT), '') = COALESCE(CAST(s.Attempt AS TEXT), '')
                    THEN 0 ELSE 1
                END AS diff_Attempt,

                (COALESCE(a.Sum_seq,0)              - COALESCE(s.sps_Sum_Seq,0))          AS diff_Seq,
                (COALESCE(a.Sum_nav_station,0)      - COALESCE(s.sps_Sum_Point,0))        AS diff_Point,
                (COALESCE(a.Sum_post_point_code,0)  - COALESCE(s.sps_Sum_PointCode,0))    AS diff_PointCode,
                (COALESCE(a.Sum_fire_code,0)        - COALESCE(s.sps_Sum_FireCode,0))     AS diff_FireCode,
                (COALESCE(a.Sum_water_depth,0)      - COALESCE(s.sps_Sum_WaterDepth,0))   AS diff_WaterDepth,
                (COALESCE(a.Sum_shot_x,0)           - COALESCE(s.sps_Sum_Easting,0))      AS diff_Easting,
                (COALESCE(a.Sum_shot_y,0)           - COALESCE(s.sps_Sum_Northing,0))     AS diff_Northing,
                (COALESCE(a.Sum_elevation,0)        - COALESCE(s.sps_Sum_Elevation,0))    AS diff_Elevation,
                (COALESCE(a.Sum_shot_day,0)         - COALESCE(s.sps_Sum_JDay,0))         AS diff_JDay,
                (COALESCE(a.Sum_shot_hour,0)        - COALESCE(s.sps_Sum_Hour,0))         AS diff_Hour,
                (COALESCE(a.Sum_shot_minute,0)      - COALESCE(s.sps_Sum_Minute,0))       AS diff_Minute,
                (COALESCE(a.Sum_shot_second,0)      - COALESCE(s.sps_Sum_Second,0))       AS diff_Second,
                (COALESCE(a.Sum_shot_microsecond,0) - COALESCE(s.sps_Sum_Microsecond,0))  AS diff_Microsecond,

                CASE
                    WHEN s.Line IS NULL THEN 1
                    WHEN COALESCE(CAST(a.attempt AS TEXT), '') = COALESCE(CAST(s.Attempt AS TEXT), '')
                    THEN 0 ELSE 1
                END +
                ABS(COALESCE(a.Sum_seq,0)              - COALESCE(s.sps_Sum_Seq,0)) +
                ABS(COALESCE(a.Sum_nav_station,0)      - COALESCE(s.sps_Sum_Point,0)) +
                ABS(COALESCE(a.Sum_post_point_code,0)  - COALESCE(s.sps_Sum_PointCode,0)) +
                ABS(COALESCE(a.Sum_fire_code,0)        - COALESCE(s.sps_Sum_FireCode,0)) +
                ABS(COALESCE(a.Sum_water_depth,0)      - COALESCE(s.sps_Sum_WaterDepth,0)) +
                ABS(COALESCE(a.Sum_shot_x,0)           - COALESCE(s.sps_Sum_Easting,0)) +
                ABS(COALESCE(a.Sum_shot_y,0)           - COALESCE(s.sps_Sum_Northing,0)) +
                ABS(COALESCE(a.Sum_elevation,0)        - COALESCE(s.sps_Sum_Elevation,0)) +
                ABS(COALESCE(a.Sum_shot_day,0)         - COALESCE(s.sps_Sum_JDay,0)) +
                ABS(COALESCE(a.Sum_shot_hour,0)        - COALESCE(s.sps_Sum_Hour,0)) +
                ABS(COALESCE(a.Sum_shot_minute,0)      - COALESCE(s.sps_Sum_Minute,0)) +
                ABS(COALESCE(a.Sum_shot_second,0)      - COALESCE(s.sps_Sum_Second,0)) +
                ABS(COALESCE(a.Sum_shot_microsecond,0) - COALESCE(s.sps_Sum_Microsecond,0))
                AS SumDiff,

                CASE WHEN
                    (COALESCE(a.nav_line,0) = COALESCE(s.sps_Sum_Line, -999999999)) AND
                    (COALESCE(CAST(a.attempt AS TEXT), '') = COALESCE(CAST(s.Attempt AS TEXT), '')) AND
                    (COALESCE(a.Sum_seq,0) = COALESCE(s.sps_Sum_Seq, -999999999)) AND
                    (COALESCE(a.Sum_nav_station,0) = COALESCE(s.sps_Sum_Point, -999999999)) AND
                    (COALESCE(a.Sum_post_point_code,0) = COALESCE(s.sps_Sum_PointCode, -999999999)) AND
                    (COALESCE(a.Sum_fire_code,0) = COALESCE(s.sps_Sum_FireCode, -999999999)) AND
                    (COALESCE(a.Sum_water_depth,0) = COALESCE(s.sps_Sum_WaterDepth, -999999999)) AND
                    (COALESCE(a.Sum_shot_x,0) = COALESCE(s.sps_Sum_Easting, -999999999)) AND
                    (COALESCE(a.Sum_shot_y,0) = COALESCE(s.sps_Sum_Northing, -999999999)) AND
                    (COALESCE(a.Sum_elevation,0) = COALESCE(s.sps_Sum_Elevation, -999999999)) AND
                    (COALESCE(a.Sum_shot_day,0) = COALESCE(s.sps_Sum_JDay, -999999999)) AND
                    (COALESCE(a.Sum_shot_hour,0) = COALESCE(s.sps_Sum_Hour, -999999999)) AND
                    (COALESCE(a.Sum_shot_minute,0) = COALESCE(s.sps_Sum_Minute, -999999999)) AND
                    (COALESCE(a.Sum_shot_second,0) = COALESCE(s.sps_Sum_Second, -999999999)) AND
                    (COALESCE(a.Sum_shot_microsecond,0) = COALESCE(s.sps_Sum_Microsecond, -999999999))
                THEN 1 ELSE 0 END AS QC_AllMatch,

                CASE WHEN
                     (COALESCE(a.nav_line,0) = COALESCE(s.sps_Sum_Line, -999999999))
                  OR (COALESCE(CAST(a.attempt AS TEXT), '') = COALESCE(CAST(s.Attempt AS TEXT), ''))
                  OR (COALESCE(a.Sum_seq,0) = COALESCE(s.sps_Sum_Seq, -999999999))
                  OR (COALESCE(a.Sum_nav_station,0) = COALESCE(s.sps_Sum_Point, -999999999))
                  OR (COALESCE(a.Sum_post_point_code,0) = COALESCE(s.sps_Sum_PointCode, -999999999))
                  OR (COALESCE(a.Sum_fire_code,0) = COALESCE(s.sps_Sum_FireCode, -999999999))
                  OR (COALESCE(a.Sum_water_depth,0) = COALESCE(s.sps_Sum_WaterDepth, -999999999))
                  OR (COALESCE(a.Sum_shot_x,0) = COALESCE(s.sps_Sum_Easting, -999999999))
                  OR (COALESCE(a.Sum_shot_y,0) = COALESCE(s.sps_Sum_Northing, -999999999))
                  OR (COALESCE(a.Sum_elevation,0) = COALESCE(s.sps_Sum_Elevation, -999999999))
                  OR (COALESCE(a.Sum_shot_day,0) = COALESCE(s.sps_Sum_JDay, -999999999))
                  OR (COALESCE(a.Sum_shot_hour,0) = COALESCE(s.sps_Sum_Hour, -999999999))
                  OR (COALESCE(a.Sum_shot_minute,0) = COALESCE(s.sps_Sum_Minute, -999999999))
                  OR (COALESCE(a.Sum_shot_second,0) = COALESCE(s.sps_Sum_Second, -999999999))
                  OR (COALESCE(a.Sum_shot_microsecond,0) = COALESCE(s.sps_Sum_Microsecond, -999999999))
                THEN 1 ELSE 0 END AS QC_AnyMatch

            FROM shot_agg a
            LEFT JOIN sps_agg s
                ON s.Line    = a.nav_line
               AND COALESCE(CAST(s.Attempt AS TEXT), '') = COALESCE(CAST(a.attempt AS TEXT), '')
               AND s.Seq     = a.seq
            LEFT JOIN sequence_vessel_assignment sva
                ON a.seq BETWEEN sva.seq_first AND sva.seq_last
            LEFT JOIN project_fleet pf
                ON pf.id = sva.vessel_id;
            """)

            cur.executescript("""
            CREATE INDEX IF NOT EXISTS idx_shot_linesummary_linecode
                ON SHOT_LineSummary(nav_line_code);

            CREATE INDEX IF NOT EXISTS idx_shot_linesummary_seq
                ON SHOT_LineSummary(seq);

            CREATE INDEX IF NOT EXISTS idx_shot_linesummary_navline
                ON SHOT_LineSummary(nav_line);
            """)

            if own_conn:
                conn.commit()

        finally:
            if own_conn:
                conn.close()

    def ensure_shot_linesummary_table(self, conn=None) -> None:
        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            cur = conn.cursor()
            row = cur.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type='table' AND name='SHOT_LineSummary'
            """).fetchone()

            if row is None:
                self.create_shot_line_summary_table(conn=conn)

            if own_conn:
                conn.commit()

        finally:
            if own_conn:
                conn.close()

    def get_shot_line_summary(self) -> dict:
        """
        Read SHOT_LineSummary table and return:
          - rows: list[dict]
          - totals: dict
          - total_count: int
        """
        conn = self._connect()
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            rows = cur.execute("""
                SELECT *
                FROM SHOT_LineSummary
                ORDER BY nav_line, attempt, seq
            """).fetchall()

            rows = [dict(r) for r in rows]
            total_count = len(rows)

            totals = {}
            if rows:
                for k, v in rows[0].items():
                    if isinstance(v, (int, float)) and k not in ("nav_line", "attempt", "seq"):
                        totals[k] = 0

                for r in rows:
                    for k in list(totals.keys()):
                        val = r.get(k)
                        if isinstance(val, (int, float)):
                            totals[k] += val

            return {
                "rows": rows,
                "totals": totals,
                "total_count": total_count,
            }

        finally:
            conn.close()

    def list_v_shot_linesummary(self, filters: dict | None = None) -> list[dict]:
        filters = filters or {}

        sql = """
            SELECT *
            FROM SHOT_LineSummary
            WHERE 1=1
        """
        params = []

        nav_line_code = (filters.get("nav_line_code") or "").strip()
        if nav_line_code:
            sql += " AND nav_line_code LIKE ? "
            params.append(f"%{nav_line_code}%")

        seq = filters.get("seq")
        if seq not in (None, ""):
            sql += " AND seq = ? "
            params.append(seq)

        attempt = filters.get("attempt")
        if attempt not in (None, ""):
            sql += " AND attempt = ? "
            params.append(attempt)

        vessel_name = (filters.get("vessel_name") or "").strip()
        if vessel_name:
            sql += " AND COALESCE(vessel_name, '') LIKE ? "
            params.append(f"%{vessel_name}%")

        purpose = (filters.get("purpose") or "").strip()
        if purpose:
            sql += " AND COALESCE(purpose, '') LIKE ? "
            params.append(f"%{purpose}%")

        is_in_sl = filters.get("is_in_sl")
        if is_in_sl in (0, 1, "0", "1"):
            sql += " AND IsInSLSolution = ? "
            params.append(int(is_in_sl))

        qc_allmatch = filters.get("qc_allmatch")
        if qc_allmatch in (0, 1, "0", "1"):
            sql += " AND QC_AllMatch = ? "
            params.append(int(qc_allmatch))

        diff_status = (filters.get("diff_status") or "").strip().lower()
        if diff_status == "ok":
            sql += " AND COALESCE(SumDiff, 0) = 0 "
        elif diff_status == "error":
            sql += " AND COALESCE(SumDiff, 0) <> 0 "

        shotcount_min = filters.get("shotcount_min")
        if shotcount_min not in (None, ""):
            sql += " AND COALESCE(ShotCount, 0) >= ? "
            params.append(shotcount_min)

        shotcount_max = filters.get("shotcount_max")
        if shotcount_max not in (None, ""):
            sql += " AND COALESCE(ShotCount, 0) <= ? "
            params.append(shotcount_max)

        sql += " ORDER BY seq, attempt, nav_line_code "

        conn = self._connect()
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            rows = cur.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def refresh_shot_linesummary_lines(self, changed_lines, conn=None) -> int:
        """
        Incrementally refresh SHOT_LineSummary only for selected nav_line_code values.

        Parameters
        ----------
        changed_lines : list[str] | tuple[str] | set[str] | str
            nav_line_code values to rebuild.
        conn : sqlite3.Connection | None

        Returns
        -------
        int
            Number of line codes requested for refresh.

        Notes
        -----
        - Requires SHOT_LineSummary table to already exist.
        - Deletes old rows for changed lines, then inserts rebuilt rows.
        - Safe to call after SHOT_TABLE / SPSolution updates for limited lines.
        """

        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        try:
            cur = conn.cursor()

            # normalize input
            if changed_lines is None:
                return 0

            if isinstance(changed_lines, str):
                changed_lines = [changed_lines]

            changed_lines = [
                str(x).strip()
                for x in changed_lines
                if x is not None and str(x).strip() != ""
            ]

            # unique preserve order
            seen = set()
            changed_lines = [x for x in changed_lines if not (x in seen or seen.add(x))]

            if not changed_lines:
                return 0

            # make sure table exists
            cur.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type='table' AND name='SHOT_LineSummary'
            """)
            if cur.fetchone() is None:
                raise RuntimeError(
                    "SHOT_LineSummary table does not exist. "
                    "Run create_shot_line_summary_table() first."
                )

            placeholders = ",".join("?" for _ in changed_lines)

            # delete only changed lines
            cur.execute(
                f"DELETE FROM SHOT_LineSummary WHERE nav_line_code IN ({placeholders})",
                changed_lines
            )

            # insert rebuilt rows only for changed lines
            sql = f"""
            INSERT INTO SHOT_LineSummary (
                nav_line_code,
                nav_line,
                attempt,
                seq,

                ShotCount,
                ProdShots,
                NonProdShots,
                KillShots,

                FSP,
                LSP,
                FGSP,
                LGSP,

                Sum_shot_station,
                Sum_shot_index,
                Sum_shot_status,
                Sum_attempt,
                Sum_seq,
                Sum_post_point_code,
                Sum_fire_code,
                Sum_gun_depth,
                Sum_water_depth,
                Sum_shot_x,
                Sum_shot_y,
                Sum_shot_day,
                Sum_shot_hour,
                Sum_shot_minute,
                Sum_shot_second,
                Sum_shot_microsecond,
                Sum_shot_year,
                Sum_array_id,
                Sum_source_id,
                Sum_nav_station,
                Sum_shot_group_id,
                Sum_elevation,

                SailLine,
                Line,
                Attempt,
                Seq,
                sps_Sum_Line,
                sps_Sum_Attempt,
                sps_Sum_Seq,
                sps_Sum_Point,
                sps_Sum_PointCode,
                sps_Sum_FireCode,
                sps_Sum_ArrayCode,
                sps_Sum_Static,
                sps_Sum_PointDepth,
                sps_Sum_Datum,
                sps_Sum_Uphole,
                sps_Sum_WaterDepth,
                sps_Sum_Easting,
                sps_Sum_Northing,
                sps_Sum_Elevation,
                sps_Sum_JDay,
                sps_Sum_Hour,
                sps_Sum_Minute,
                sps_Sum_Second,
                sps_Sum_Microsecond,

                purpose,
                vessel_id,
                vessel_name,
                IsInSLSolution,

                cmp_Line,
                cmp_Attempt,
                cmp_Seq,
                cmp_Point,
                cmp_PointCode,
                cmp_FireCode,
                cmp_WaterDepth,
                cmp_Easting,
                cmp_Northing,
                cmp_Elevation,
                cmp_JDay,
                cmp_Hour,
                cmp_Minute,
                cmp_Second,
                cmp_Microsecond,

                diff_Attempt,
                diff_Seq,
                diff_Point,
                diff_PointCode,
                diff_FireCode,
                diff_WaterDepth,
                diff_Easting,
                diff_Northing,
                diff_Elevation,
                diff_JDay,
                diff_Hour,
                diff_Minute,
                diff_Second,
                diff_Microsecond,

                SumDiff,
                QC_AllMatch,
                QC_AnyMatch
            )
            WITH
            pg AS (
                SELECT
                    UPPER(COALESCE(production_code,''))      AS prod_codes,
                    UPPER(COALESCE(non_production_code,''))  AS nonprod_codes,
                    UPPER(COALESCE(kill_code,''))            AS kill_codes
                FROM project_geometry
                LIMIT 1
            ),

            shot_base AS (
                SELECT
                    s.nav_line_code,
                    s.nav_line,
                    s.attempt,
                    s.seq,

                    s.shot_station,
                    s.shot_index,
                    s.shot_status,

                    s.post_point_code,
                    s.fire_code,

                    s.gun_depth,
                    s.water_depth,

                    s.shot_x,
                    s.shot_y,

                    s.shot_day,
                    s.shot_hour,
                    s.shot_minute,
                    s.shot_second,
                    s.shot_microsecond,
                    s.shot_year,

                    s.array_id,
                    s.source_id,
                    s.nav_station,
                    s.shot_group_id,
                    s.elevation,

                    CASE WHEN INSTR(pg.prod_codes,    UPPER(COALESCE(s.fire_code,''))) > 0 THEN 1 ELSE 0 END AS is_prod,
                    CASE WHEN INSTR(pg.nonprod_codes, UPPER(COALESCE(s.fire_code,''))) > 0 THEN 1 ELSE 0 END AS is_nonprod,
                    CASE WHEN INSTR(pg.kill_codes,    UPPER(COALESCE(s.fire_code,''))) > 0 THEN 1 ELSE 0 END AS is_kill
                FROM SHOT_TABLE s
                CROSS JOIN pg
                WHERE s.nav_line_code IN ({placeholders})
                  AND s.nav_line_code IS NOT NULL
                  AND TRIM(s.nav_line_code) <> ''
            ),

            shot_agg AS (
                SELECT
                    nav_line_code,

                    MAX(nav_line) AS nav_line,
                    MAX(attempt)  AS attempt,
                    MAX(seq)      AS seq,

                    COUNT(*) AS ShotCount,

                    SUM(is_prod)    AS ProdShots,
                    SUM(is_nonprod) AS NonProdShots,
                    SUM(is_kill)    AS KillShots,

                    MIN(nav_station) AS FSP,
                    MAX(nav_station) AS LSP,
                    MIN(CASE WHEN is_prod=1 THEN nav_station END) AS FGSP,
                    MAX(CASE WHEN is_prod=1 THEN nav_station END) AS LGSP,

                    SUM(shot_station)     AS Sum_shot_station,
                    SUM(shot_index)       AS Sum_shot_index,
                    SUM(shot_status)      AS Sum_shot_status,

                    SUM(attempt)          AS Sum_attempt,
                    SUM(seq)              AS Sum_seq,

                    SUM(unicode(post_point_code)) AS Sum_post_point_code,
                    SUM(unicode(fire_code))       AS Sum_fire_code,

                    SUM(gun_depth)        AS Sum_gun_depth,
                    SUM(water_depth)      AS Sum_water_depth,

                    SUM(shot_x)           AS Sum_shot_x,
                    SUM(shot_y)           AS Sum_shot_y,

                    SUM(shot_day)         AS Sum_shot_day,
                    SUM(shot_hour)        AS Sum_shot_hour,
                    SUM(shot_minute)      AS Sum_shot_minute,
                    SUM(shot_second)      AS Sum_shot_second,
                    SUM(shot_microsecond) AS Sum_shot_microsecond,
                    SUM(shot_year)        AS Sum_shot_year,

                    SUM(unicode(array_id)) AS Sum_array_id,

                    SUM(source_id)     AS Sum_source_id,
                    SUM(nav_station)   AS Sum_nav_station,
                    SUM(shot_group_id) AS Sum_shot_group_id,
                    SUM(elevation)     AS Sum_elevation

                FROM shot_base
                GROUP BY nav_line_code
            ),

            sps_agg AS (
                SELECT
                    SailLine,
                    Line,
                    Attempt,
                    Seq,

                    SUM(Line)    AS sps_Sum_Line,
                    SUM(Attempt) AS sps_Sum_Attempt,
                    SUM(Seq)     AS sps_Sum_Seq,
                    SUM(Point)   AS sps_Sum_Point,

                    SUM(unicode(PointCode)) AS sps_Sum_PointCode,
                    SUM(unicode(FireCode))  AS sps_Sum_FireCode,
                    SUM(unicode(ArrayCode)) AS sps_Sum_ArrayCode,

                    SUM(Static)     AS sps_Sum_Static,
                    SUM(PointDepth) AS sps_Sum_PointDepth,
                    SUM(Datum)      AS sps_Sum_Datum,
                    SUM(Uphole)     AS sps_Sum_Uphole,
                    SUM(WaterDepth) AS sps_Sum_WaterDepth,

                    SUM(Easting)   AS sps_Sum_Easting,
                    SUM(Northing)  AS sps_Sum_Northing,
                    SUM(Elevation) AS sps_Sum_Elevation,

                    SUM(JDay)        AS sps_Sum_JDay,
                    SUM(Hour)        AS sps_Sum_Hour,
                    SUM(Minute)      AS sps_Sum_Minute,
                    SUM(Second)      AS sps_Sum_Second,
                    SUM(Microsecond) AS sps_Sum_Microsecond

                FROM SPSolution
                GROUP BY SailLine, Line, Attempt, Seq
            )

            SELECT
                a.nav_line_code,
                a.nav_line,
                a.attempt,
                a.seq,

                a.ShotCount,
                a.ProdShots,
                a.NonProdShots,
                a.KillShots,

                a.FSP,
                a.LSP,
                a.FGSP,
                a.LGSP,

                a.Sum_shot_station,
                a.Sum_shot_index,
                a.Sum_shot_status,
                a.Sum_attempt,
                a.Sum_seq,
                a.Sum_post_point_code,
                a.Sum_fire_code,
                a.Sum_gun_depth,
                a.Sum_water_depth,
                a.Sum_shot_x,
                a.Sum_shot_y,
                a.Sum_shot_day,
                a.Sum_shot_hour,
                a.Sum_shot_minute,
                a.Sum_shot_second,
                a.Sum_shot_microsecond,
                a.Sum_shot_year,
                a.Sum_array_id,
                a.Sum_source_id,
                a.Sum_nav_station,
                a.Sum_shot_group_id,
                a.Sum_elevation,

                s.SailLine,
                s.Line,
                s.Attempt,
                s.Seq,
                s.sps_Sum_Line,
                s.sps_Sum_Attempt,
                s.sps_Sum_Seq,
                s.sps_Sum_Point,
                s.sps_Sum_PointCode,
                s.sps_Sum_FireCode,
                s.sps_Sum_ArrayCode,
                s.sps_Sum_Static,
                s.sps_Sum_PointDepth,
                s.sps_Sum_Datum,
                s.sps_Sum_Uphole,
                s.sps_Sum_WaterDepth,
                s.sps_Sum_Easting,
                s.sps_Sum_Northing,
                s.sps_Sum_Elevation,
                s.sps_Sum_JDay,
                s.sps_Sum_Hour,
                s.sps_Sum_Minute,
                s.sps_Sum_Second,
                s.sps_Sum_Microsecond,

                sva.purpose,
                sva.vessel_id,
                pf.vessel_name,

                CASE WHEN EXISTS (
                    SELECT 1
                    FROM SLSolution sl
                    WHERE sl.SailLine = a.nav_line_code
                ) THEN 1 ELSE 0 END AS IsInSLSolution,

                CASE WHEN COALESCE(a.nav_line,0)             = COALESCE(s.sps_Sum_Line, -999999999)        THEN 1 ELSE 0 END AS cmp_Line,
                CASE WHEN COALESCE(a.Sum_attempt,0)          = COALESCE(s.sps_Sum_Attempt, -999999999)     THEN 1 ELSE 0 END AS cmp_Attempt,
                CASE WHEN COALESCE(a.Sum_seq,0)              = COALESCE(s.sps_Sum_Seq, -999999999)         THEN 1 ELSE 0 END AS cmp_Seq,
                CASE WHEN COALESCE(a.Sum_nav_station,0)      = COALESCE(s.sps_Sum_Point, -999999999)       THEN 1 ELSE 0 END AS cmp_Point,
                CASE WHEN COALESCE(a.Sum_post_point_code,0)  = COALESCE(s.sps_Sum_PointCode, -999999999)   THEN 1 ELSE 0 END AS cmp_PointCode,
                CASE WHEN COALESCE(a.Sum_fire_code,0)        = COALESCE(s.sps_Sum_FireCode, -999999999)    THEN 1 ELSE 0 END AS cmp_FireCode,
                CASE WHEN COALESCE(a.Sum_water_depth,0)      = COALESCE(s.sps_Sum_WaterDepth, -999999999)  THEN 1 ELSE 0 END AS cmp_WaterDepth,
                CASE WHEN COALESCE(a.Sum_shot_x,0)           = COALESCE(s.sps_Sum_Easting, -999999999)     THEN 1 ELSE 0 END AS cmp_Easting,
                CASE WHEN COALESCE(a.Sum_shot_y,0)           = COALESCE(s.sps_Sum_Northing, -999999999)    THEN 1 ELSE 0 END AS cmp_Northing,
                CASE WHEN COALESCE(a.Sum_elevation,0)        = COALESCE(s.sps_Sum_Elevation, -999999999)   THEN 1 ELSE 0 END AS cmp_Elevation,
                CASE WHEN COALESCE(a.Sum_shot_day,0)         = COALESCE(s.sps_Sum_JDay, -999999999)        THEN 1 ELSE 0 END AS cmp_JDay,
                CASE WHEN COALESCE(a.Sum_shot_hour,0)        = COALESCE(s.sps_Sum_Hour, -999999999)        THEN 1 ELSE 0 END AS cmp_Hour,
                CASE WHEN COALESCE(a.Sum_shot_minute,0)      = COALESCE(s.sps_Sum_Minute, -999999999)      THEN 1 ELSE 0 END AS cmp_Minute,
                CASE WHEN COALESCE(a.Sum_shot_second,0)      = COALESCE(s.sps_Sum_Second, -999999999)      THEN 1 ELSE 0 END AS cmp_Second,
                CASE WHEN COALESCE(a.Sum_shot_microsecond,0) = COALESCE(s.sps_Sum_Microsecond, -999999999) THEN 1 ELSE 0 END AS cmp_Microsecond,

                (COALESCE(a.Sum_attempt,0)          - COALESCE(s.sps_Sum_Attempt,0))      AS diff_Attempt,
                (COALESCE(a.Sum_seq,0)              - COALESCE(s.sps_Sum_Seq,0))          AS diff_Seq,
                (COALESCE(a.Sum_nav_station,0)      - COALESCE(s.sps_Sum_Point,0))        AS diff_Point,
                (COALESCE(a.Sum_post_point_code,0)  - COALESCE(s.sps_Sum_PointCode,0))    AS diff_PointCode,
                (COALESCE(a.Sum_fire_code,0)        - COALESCE(s.sps_Sum_FireCode,0))     AS diff_FireCode,
                (COALESCE(a.Sum_water_depth,0)      - COALESCE(s.sps_Sum_WaterDepth,0))   AS diff_WaterDepth,
                (COALESCE(a.Sum_shot_x,0)           - COALESCE(s.sps_Sum_Easting,0))      AS diff_Easting,
                (COALESCE(a.Sum_shot_y,0)           - COALESCE(s.sps_Sum_Northing,0))     AS diff_Northing,
                (COALESCE(a.Sum_elevation,0)        - COALESCE(s.sps_Sum_Elevation,0))    AS diff_Elevation,
                (COALESCE(a.Sum_shot_day,0)         - COALESCE(s.sps_Sum_JDay,0))         AS diff_JDay,
                (COALESCE(a.Sum_shot_hour,0)        - COALESCE(s.sps_Sum_Hour,0))         AS diff_Hour,
                (COALESCE(a.Sum_shot_minute,0)      - COALESCE(s.sps_Sum_Minute,0))       AS diff_Minute,
                (COALESCE(a.Sum_shot_second,0)      - COALESCE(s.sps_Sum_Second,0))       AS diff_Second,
                (COALESCE(a.Sum_shot_microsecond,0) - COALESCE(s.sps_Sum_Microsecond,0))  AS diff_Microsecond,

                ABS(COALESCE(a.Sum_attempt,0)          - COALESCE(s.sps_Sum_Attempt,0)) +
                ABS(COALESCE(a.Sum_seq,0)              - COALESCE(s.sps_Sum_Seq,0)) +
                ABS(COALESCE(a.Sum_nav_station,0)      - COALESCE(s.sps_Sum_Point,0)) +
                ABS(COALESCE(a.Sum_post_point_code,0)  - COALESCE(s.sps_Sum_PointCode,0)) +
                ABS(COALESCE(a.Sum_fire_code,0)        - COALESCE(s.sps_Sum_FireCode,0)) +
                ABS(COALESCE(a.Sum_water_depth,0)      - COALESCE(s.sps_Sum_WaterDepth,0)) +
                ABS(COALESCE(a.Sum_shot_x,0)           - COALESCE(s.sps_Sum_Easting,0)) +
                ABS(COALESCE(a.Sum_shot_y,0)           - COALESCE(s.sps_Sum_Northing,0)) +
                ABS(COALESCE(a.Sum_elevation,0)        - COALESCE(s.sps_Sum_Elevation,0)) +
                ABS(COALESCE(a.Sum_shot_day,0)         - COALESCE(s.sps_Sum_JDay,0)) +
                ABS(COALESCE(a.Sum_shot_hour,0)        - COALESCE(s.sps_Sum_Hour,0)) +
                ABS(COALESCE(a.Sum_shot_minute,0)      - COALESCE(s.sps_Sum_Minute,0)) +
                ABS(COALESCE(a.Sum_shot_second,0)      - COALESCE(s.sps_Sum_Second,0)) +
                ABS(COALESCE(a.Sum_shot_microsecond,0) - COALESCE(s.sps_Sum_Microsecond,0))
                AS SumDiff,

                CASE WHEN
                    (COALESCE(a.nav_line,0)             = COALESCE(s.sps_Sum_Line, -999999999)) AND
                    (COALESCE(a.Sum_attempt,0)          = COALESCE(s.sps_Sum_Attempt, -999999999)) AND
                    (COALESCE(a.Sum_seq,0)              = COALESCE(s.sps_Sum_Seq, -999999999)) AND
                    (COALESCE(a.Sum_nav_station,0)      = COALESCE(s.sps_Sum_Point, -999999999)) AND
                    (COALESCE(a.Sum_post_point_code,0)  = COALESCE(s.sps_Sum_PointCode, -999999999)) AND
                    (COALESCE(a.Sum_fire_code,0)        = COALESCE(s.sps_Sum_FireCode, -999999999)) AND
                    (COALESCE(a.Sum_water_depth,0)      = COALESCE(s.sps_Sum_WaterDepth, -999999999)) AND
                    (COALESCE(a.Sum_shot_x,0)           = COALESCE(s.sps_Sum_Easting, -999999999)) AND
                    (COALESCE(a.Sum_shot_y,0)           = COALESCE(s.sps_Sum_Northing, -999999999)) AND
                    (COALESCE(a.Sum_elevation,0)        = COALESCE(s.sps_Sum_Elevation, -999999999)) AND
                    (COALESCE(a.Sum_shot_day,0)         = COALESCE(s.sps_Sum_JDay, -999999999)) AND
                    (COALESCE(a.Sum_shot_hour,0)        = COALESCE(s.sps_Sum_Hour, -999999999)) AND
                    (COALESCE(a.Sum_shot_minute,0)      = COALESCE(s.sps_Sum_Minute, -999999999)) AND
                    (COALESCE(a.Sum_shot_second,0)      = COALESCE(s.sps_Sum_Second, -999999999)) AND
                    (COALESCE(a.Sum_shot_microsecond,0) = COALESCE(s.sps_Sum_Microsecond, -999999999))
                THEN 1 ELSE 0 END AS QC_AllMatch,

                CASE WHEN
                     (COALESCE(a.nav_line,0)             = COALESCE(s.sps_Sum_Line, -999999999))
                  OR (COALESCE(a.Sum_attempt,0)          = COALESCE(s.sps_Sum_Attempt, -999999999))
                  OR (COALESCE(a.Sum_seq,0)              = COALESCE(s.sps_Sum_Seq, -999999999))
                  OR (COALESCE(a.Sum_nav_station,0)      = COALESCE(s.sps_Sum_Point, -999999999))
                  OR (COALESCE(a.Sum_post_point_code,0)  = COALESCE(s.sps_Sum_PointCode, -999999999))
                  OR (COALESCE(a.Sum_fire_code,0)        = COALESCE(s.sps_Sum_FireCode, -999999999))
                  OR (COALESCE(a.Sum_water_depth,0)      = COALESCE(s.sps_Sum_WaterDepth, -999999999))
                  OR (COALESCE(a.Sum_shot_x,0)           = COALESCE(s.sps_Sum_Easting, -999999999))
                  OR (COALESCE(a.Sum_shot_y,0)           = COALESCE(s.sps_Sum_Northing, -999999999))
                  OR (COALESCE(a.Sum_elevation,0)        = COALESCE(s.sps_Sum_Elevation, -999999999))
                  OR (COALESCE(a.Sum_shot_day,0)         = COALESCE(s.sps_Sum_JDay, -999999999))
                  OR (COALESCE(a.Sum_shot_hour,0)        = COALESCE(s.sps_Sum_Hour, -999999999))
                  OR (COALESCE(a.Sum_shot_minute,0)      = COALESCE(s.sps_Sum_Minute, -999999999))
                  OR (COALESCE(a.Sum_shot_second,0)      = COALESCE(s.sps_Sum_Second, -999999999))
                  OR (COALESCE(a.Sum_shot_microsecond,0) = COALESCE(s.sps_Sum_Microsecond, -999999999))
                THEN 1 ELSE 0 END AS QC_AnyMatch

            FROM shot_agg a
            LEFT JOIN sps_agg s
                ON s.Line    = a.nav_line
               AND s.Attempt = a.attempt
               AND s.Seq     = a.seq
            LEFT JOIN sequence_vessel_assignment sva
                ON a.seq BETWEEN sva.seq_first AND sva.seq_last
            LEFT JOIN project_fleet pf
                ON pf.id = sva.vessel_id
            """
            cur.execute(sql, changed_lines)

            if own_conn:
                conn.commit()

            return len(changed_lines)

        finally:
            if own_conn:
                conn.close()

    def load_shot_table_h26_stream_new_only(
            self,
            file_obj,
            file_fk: int,
            chunk_size: int = 50000,
            conn=None,
    ) -> dict:
        """
        Insert only NEW SHOT rows.
        Existing rows are ignored based on UNIQUE key:
            (nav_line_code, nav_station, post_point_code)

        Returns
        -------
        {
            "inserted": int,
            "duplicates": int,
            "changed_lines": list[str]
        }
        """
        if not file_fk:
            raise ValueError("file_fk is required")

        own_conn = conn is None
        if own_conn:
            conn = self._connect()

        text_stream = None

        try:
            cur = conn.cursor()
            conn.execute("PRAGMA busy_timeout = 60000;")
            self._ensure_shot_unique_index(conn)
            self._set_fast_import_pragmas(conn, aggressive=False)

            cur.execute("BEGIN IMMEDIATE;")

            insert_sql = """
            INSERT OR IGNORE INTO SHOT_TABLE (
                sail_line,
                shot_station,
                shot_index,
                shot_status,
                nav_line_code,
                nav_line,
                attempt,
                seq,
                post_point_code,
                fire_code,
                gun_depth,
                water_depth,
                shot_x,
                shot_y,
                shot_day,
                shot_hour,
                shot_minute,
                shot_second,
                shot_microsecond,
                shot_year,
                vessel,
                array_id,
                source_id,
                nav_station,
                shot_group_id,
                elevation,
                File_FK,
                Seq_FK
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """

            probe = file_obj.read(0)
            if isinstance(probe, (bytes, bytearray)):
                text_stream = io.TextIOWrapper(
                    file_obj,
                    encoding="utf-8",
                    errors="ignore",
                    newline=""
                )
            else:
                text_stream = file_obj

            try:
                file_obj.seek(0)
            except Exception:
                pass

            reader = csv.reader(text_stream, delimiter=",", skipinitialspace=True)

            batch = []
            inserted = 0
            duplicates = 0
            changed_lines = set()

            decode_nav = self.decode_nav_line
            to_int = self.to_int
            to_float = self.to_float
            get_seq_info = self.get_seq_info

            last_seq = object()
            last_seq_fk = None

            for row in reader:
                if not row:
                    continue

                first = (row[0] or "").strip()
                if not first:
                    continue

                if first[:1] in ("H", "h"):
                    continue

                if len(row) < 22:
                    continue

                parts = first.split()
                sail_line = (
                    to_int(parts[1])
                    if (len(parts) == 2 and parts[0].upper() == "S")
                    else to_int(first)
                )

                shot_station = to_int(row[1])
                shot_index = to_int(row[2])
                shot_status = to_int(row[3])

                post_point_code = (row[4] or "").strip().upper()
                if not post_point_code:
                    post_point_code = ""

                fire_code = post_point_code[:1] if post_point_code else ""

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

                nav_line_code = (row[18] or "").strip() or ""
                nav_line, attempt, seq = decode_nav(nav_line_code)
                attempt = "" if attempt is None else str(attempt)

                nav_station = to_int(row[19])
                if nav_station is None:
                    nav_station = 0

                shot_group_id = to_int(row[20])
                elevation = to_float(row[21])

                if seq != last_seq:
                    seq_info = get_seq_info(seq)
                    last_seq_fk = seq_info.get("id")
                    last_seq = seq

                batch.append((
                    sail_line,
                    shot_station,
                    shot_index,
                    shot_status,
                    nav_line_code,
                    nav_line,
                    attempt,
                    seq,
                    post_point_code,
                    fire_code,
                    gun_depth,
                    water_depth,
                    shot_x,
                    shot_y,
                    shot_day,
                    shot_hour,
                    shot_minute,
                    shot_second,
                    shot_microsecond,
                    shot_year,
                    vessel,
                    array_id,
                    source_id,
                    nav_station,
                    shot_group_id,
                    elevation,
                    int(file_fk),
                    last_seq_fk
                ))

                if nav_line_code:
                    changed_lines.add(nav_line_code)

                if len(batch) >= chunk_size:
                    before = conn.total_changes
                    cur.executemany(insert_sql, batch)
                    delta = conn.total_changes - before
                    inserted += delta
                    duplicates += (len(batch) - delta)
                    batch.clear()

            if batch:
                before = conn.total_changes
                cur.executemany(insert_sql, batch)
                delta = conn.total_changes - before
                inserted += delta
                duplicates += (len(batch) - delta)
                batch.clear()

            if own_conn:
                conn.commit()

            return {
                "inserted": int(inserted),
                "duplicates": int(duplicates),
                "changed_lines": sorted(changed_lines),
            }

        except Exception:
            if own_conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise

        finally:
            try:
                if text_stream is not None and hasattr(text_stream, "detach"):
                    text_stream.detach()
            except Exception:
                pass

            if own_conn:
                conn.close()


