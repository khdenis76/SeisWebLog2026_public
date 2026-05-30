from __future__ import annotations

import csv
import math
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from django.template.loader import render_to_string

PAGE_RE = re.compile(
    r"^#Page\s+(?P<page_no>\d+)\s+(?P<title>.*?)(?:\s+First Online Shot\s+(?P<first_shot>\d+)\s+Last Online Shot\s+(?P<last_shot>\d+))?\s*$"
)
META_COLUMNS = {
    "Preplot Name",
    "Line Name",
    "Vessel Name",
    "Shot",
    "Raw Time",
    "Time",
    "Date",
    "Number FFIDs",
}

@dataclass
class MFAPage:
    page_no: int
    title: str
    first_online_shot: int | None
    last_online_shot: int | None
    header: list[str]
    types: list[str]
    rows: list[list[str]]


def clean_name(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch.isprintable()).strip()


def to_float(value):
    try:
        value = str(value).strip()
        return float(value) if value else None
    except Exception:
        return None


def to_int(value):
    number = to_float(value)
    return int(number) if number is not None and not math.isnan(number) else None


def read_mfa_meta(path: str | Path) -> dict:
    meta = {"file_version": None, "uuid": None, "line_name": None}
    path = Path(path)

    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line.startswith("#Page"):
                break
            if line.startswith("#FileVersion"):
                meta["file_version"] = line.replace("#FileVersion", "", 1).strip()
            elif line.startswith("#UUID"):
                meta["uuid"] = line.replace("#UUID", "", 1).strip()
            elif line.startswith("#Line Name"):
                meta["line_name"] = line.replace("#Line Name", "", 1).strip()

    return meta

def iter_mfa_pages(path: str | Path) -> Iterable[MFAPage]:
    """
    Read MFA pages from Gator MFA file.

    Supports both formats:

        #Page 1 Some Title First Online Shot 100 Last Online Shot 200

    and:

        #Page 1 GPS Quality All Shots
    """

    path = Path(path)

    current_page = None
    header = []
    types = []
    rows = []

    def flush_page():
        nonlocal current_page, header, types, rows

        if current_page and header:
            yield MFAPage(
                page_no=current_page["page_no"],
                title=current_page["title"],
                first_online_shot=current_page["first_online_shot"],
                last_online_shot=current_page["last_online_shot"],
                header=header,
                types=types,
                rows=rows,
            )

        current_page = None
        header = []
        types = []
        rows = []

    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:

        for raw_line in f:
            line = raw_line.rstrip("\r\n")

            if not line:
                continue

            page_match = PAGE_RE.match(line)

            if page_match:
                yield from flush_page()

                first_shot = page_match.group("first_shot")
                last_shot = page_match.group("last_shot")

                current_page = {
                    "page_no": int(page_match.group("page_no")),
                    "title": clean_name(page_match.group("title")),
                    "first_online_shot": int(first_shot) if first_shot else None,
                    "last_online_shot": int(last_shot) if last_shot else None,
                }

                continue

            if current_page is None:
                continue

            parts = next(csv.reader([line]))

            if not parts:
                continue

            rec_type = parts[0].strip()
            values = [clean_name(v) for v in parts[1:]]

            if rec_type == "H":
                header = values

            elif rec_type == "T":
                types = values

            elif rec_type == "S":
                if len(values) < len(header):
                    values += [""] * (len(header) - len(values))

                rows.append(values[:len(header)])

    yield from flush_page()


class MFADB:
    def __init__(self, db_path):
        self.db_path = Path(db_path)

    def connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 60000")
        return conn

    def ensure_tables(self):
        with self.connect() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS MFA_Files (
                                   ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                
                                   FileName TEXT NOT NULL,
                                   FilePath TEXT,
                                   FileSize INTEGER DEFAULT 0,
                                
                                   FileVersion TEXT,
                                   UUID TEXT,
                                
                                   LineName TEXT,
                                
                                   Line INTEGER,
                                   Seq INTEGER,
                                   Attempt TEXT,
                                
                                   Vessel_FK INTEGER,
                                   SailLine_FK INTEGER,
                                
                                   TotalShots INTEGER DEFAULT 0,
                                   FirstOnlineShot INTEGER,
                                   LastOnlineShot INTEGER,
                                
                                   ImportedAt TEXT DEFAULT CURRENT_TIMESTAMP,
                                
                                   UNIQUE(FileName, FileSize, UUID),
                                
                                   FOREIGN KEY (Vessel_FK)
                                       REFERENCES project_fleet(id)
                                       ON UPDATE CASCADE
                                       ON DELETE SET NULL,
                                
                                   FOREIGN KEY (SailLine_FK)
                                       REFERENCES SLSolution(ID)
                                       ON UPDATE CASCADE
                                       ON DELETE SET NULL
                                );
            CREATE TABLE IF NOT EXISTS MFA_Pages (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                File_FK INTEGER NOT NULL,
                PageNo INTEGER NOT NULL,
                PageTitle TEXT NOT NULL,
                FirstOnlineShot INTEGER,
                LastOnlineShot INTEGER,
                ColumnCount INTEGER DEFAULT 0,
                RowCount INTEGER DEFAULT 0,
                FOREIGN KEY(File_FK) REFERENCES MFA_Files(ID) ON DELETE CASCADE,
                UNIQUE(File_FK, PageNo)
            );
            CREATE TABLE IF NOT EXISTS MFA_Rows (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                Page_FK INTEGER NOT NULL,
                RowNo INTEGER NOT NULL,
                PreplotName TEXT,
                LineName TEXT,
                VesselName TEXT,
                Shot INTEGER,
                RawTime REAL,
                Time TEXT,
                DateText TEXT,
                FOREIGN KEY(Page_FK) REFERENCES MFA_Pages(ID) ON DELETE CASCADE,
                UNIQUE(Page_FK, RowNo)
            );
            CREATE TABLE IF NOT EXISTS MFA_Values (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                Row_FK INTEGER NOT NULL,
                ColumnIndex INTEGER NOT NULL,
                ColumnName TEXT NOT NULL,
                ColumnType TEXT,
                ValueText TEXT,
                ValueReal REAL,
                Quality INTEGER,
                FOREIGN KEY(Row_FK) REFERENCES MFA_Rows(ID) ON DELETE CASCADE,
                UNIQUE(Row_FK, ColumnIndex)
            );
            CREATE INDEX IF NOT EXISTS idx_mfa_pages_file_page ON MFA_Pages(File_FK, PageNo);
            CREATE INDEX IF NOT EXISTS idx_mfa_rows_page_shot ON MFA_Rows(Page_FK, Shot);
            CREATE INDEX IF NOT EXISTS idx_mfa_values_col ON MFA_Values(ColumnName);
            """)

    def parse_mfa_filename(self,file_name):
        """
        Parse MFA filename.

        Example:
            134117000.0.mfa

        Returns:
            line=1341
            seq=17
            attempt='0'
        """

        stem = Path(file_name).stem

        line = None
        seq = None
        attempt = None

        try:
            left, right = stem.split(".")

            if len(left) >= 6:
                line = int(left[:4])
                seq = int(left[4:6])

            attempt = right

        except Exception:
            pass

        return line, seq, attempt

    def detect_sailline_fk(self,conn, line_name):
        """
        Find matching SLSolution.ID using SailLine.

        MFA:
            LineName

        SLSolution:
            SailLine
        """

        if not line_name:
            return None

        row = conn.execute("""
            SELECT ID
            FROM SLSolution
            WHERE TRIM(LOWER(SailLine)) = TRIM(LOWER(?))
            LIMIT 1
        """, (
            line_name,
        )).fetchone()

        return row["ID"] if row else None

    def import_file(
            self,
            mfa_path,
            replace_existing=True,
            original_file_name=None,
    ):
        """
        Import one MFA file into SQLite.

        Imports:
        - MFA_Files
        - MFA_Pages
        - MFA_Rows
        - MFA_Values

        Links:
        - MFA_Files.LineName -> SLSolution.SailLine
        """

        self.ensure_tables()

        mfa_path = Path(mfa_path)
        file_name = original_file_name or mfa_path.name
        file_size = mfa_path.stat().st_size

        meta = read_mfa_meta(mfa_path)
        line, seq, attempt = self.parse_mfa_filename(file_name)

        pages = list(iter_mfa_pages(mfa_path))

        if not pages:
            raise ValueError(f"No MFA pages found in file: {file_name}")

        page_count = 0
        row_count = 0
        value_count = 0

        with self.connect() as conn:

            sailline_fk = self.detect_sailline_fk(
                conn=conn,
                line_name=meta.get("line_name"),
            )

            vessel_fk = None

            if sailline_fk:
                sl_row = conn.execute("""
                    SELECT
                        ID,
                        Line,
                        Seq,
                        Attempt,
                        Vessel_FK
                    FROM SLSolution
                    WHERE ID = ?
                """, (sailline_fk,)).fetchone()

                if sl_row:
                    line = sl_row["Line"]
                    seq = sl_row["Seq"]
                    attempt = sl_row["Attempt"]
                    vessel_fk = sl_row["Vessel_FK"]

            if replace_existing:
                old = conn.execute("""
                    SELECT ID
                    FROM MFA_Files
                    WHERE FileName = ?
                """, (file_name,)).fetchone()

                if old:
                    conn.execute("""
                        DELETE FROM MFA_Files
                        WHERE ID = ?
                    """, (old["ID"],))

            cur = conn.execute("""
                INSERT INTO MFA_Files
                (
                    FileName,
                    FilePath,
                    FileSize,

                    FileVersion,
                    UUID,

                    LineName,

                    Line,
                    Seq,
                    Attempt,

                    Vessel_FK,
                    SailLine_FK,

                    TotalShots,
                    FirstOnlineShot,
                    LastOnlineShot
                )
                VALUES
                (
                    ?, ?, ?,
                    ?, ?,
                    ?,
                    ?, ?, ?,
                    ?, ?,
                    0, NULL, NULL
                )
            """, (
                file_name,
                file_name,
                file_size,

                meta.get("file_version"),
                meta.get("uuid"),

                meta.get("line_name"),

                line,
                seq,
                attempt,

                vessel_fk,
                sailline_fk,
            ))

            file_id = cur.lastrowid

            for page in pages:

                page_count += 1

                page_cur = conn.execute("""
                    INSERT INTO MFA_Pages
                    (
                        File_FK,
                        PageNo,
                        PageTitle,

                        FirstOnlineShot,
                        LastOnlineShot,

                        ColumnCount,
                        RowCount
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    file_id,
                    page.page_no,
                    page.title,

                    page.first_online_shot,
                    page.last_online_shot,

                    len(page.header),
                    len(page.rows),
                ))

                page_id = page_cur.lastrowid

                common = {
                    name: idx
                    for idx, name in enumerate(page.header)
                }

                row_payload = []

                for row_no, row in enumerate(page.rows, start=1):
                    row_payload.append((
                        page_id,
                        row_no,

                        row[common["Preplot Name"]]
                        if "Preplot Name" in common else None,

                        row[common["Line Name"]]
                        if "Line Name" in common else None,

                        row[common["Vessel Name"]]
                        if "Vessel Name" in common else None,

                        to_int(row[common["Shot"]])
                        if "Shot" in common else None,

                        to_float(row[common["Raw Time"]])
                        if "Raw Time" in common else None,

                        row[common["Time"]]
                        if "Time" in common else None,

                        row[common["Date"]]
                        if "Date" in common else None,
                    ))

                if row_payload:
                    conn.executemany("""
                        INSERT INTO MFA_Rows
                        (
                            Page_FK,
                            RowNo,

                            PreplotName,
                            LineName,
                            VesselName,

                            Shot,
                            RawTime,
                            Time,
                            DateText
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, row_payload)

                row_count += len(row_payload)

                row_ids = conn.execute("""
                    SELECT ID, RowNo
                    FROM MFA_Rows
                    WHERE Page_FK = ?
                    ORDER BY RowNo
                """, (page_id,)).fetchall()

                row_id_by_no = {
                    r["RowNo"]: r["ID"]
                    for r in row_ids
                }

                value_payload = []

                for row_no, row in enumerate(page.rows, start=1):

                    row_id = row_id_by_no.get(row_no)

                    if not row_id:
                        continue

                    for col_idx, col_name in enumerate(page.header):

                        if col_name in META_COLUMNS:
                            continue

                        raw_value = (
                            row[col_idx]
                            if col_idx < len(row)
                            else ""
                        )

                        col_type = (
                            page.types[col_idx]
                            if col_idx < len(page.types)
                            else ""
                        )

                        is_quality = col_name.lower().endswith(" quality")

                        value_payload.append((
                            row_id,
                            col_idx,

                            col_name,
                            col_type,

                            raw_value,
                            to_float(raw_value),

                            to_int(raw_value)
                            if is_quality
                            else None,
                        ))

                if value_payload:
                    conn.executemany("""
                        INSERT INTO MFA_Values
                        (
                            Row_FK,
                            ColumnIndex,

                            ColumnName,
                            ColumnType,

                            ValueText,
                            ValueReal,

                            Quality
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, value_payload)

                value_count += len(value_payload)

            conn.execute("""
                UPDATE MFA_Files
                SET
                    TotalShots = COALESCE((
                        SELECT COUNT(DISTINCT r.Shot)
                        FROM MFA_Rows r
                        JOIN MFA_Pages p
                            ON p.ID = r.Page_FK
                        WHERE p.File_FK = MFA_Files.ID
                          AND r.Shot IS NOT NULL
                    ), 0),

                    FirstOnlineShot = COALESCE(
                        (
                            SELECT MIN(p.FirstOnlineShot)
                            FROM MFA_Pages p
                            WHERE p.File_FK = MFA_Files.ID
                              AND p.FirstOnlineShot IS NOT NULL
                        ),
                        (
                            SELECT MIN(r.Shot)
                            FROM MFA_Rows r
                            JOIN MFA_Pages p
                                ON p.ID = r.Page_FK
                            WHERE p.File_FK = MFA_Files.ID
                              AND r.Shot IS NOT NULL
                        )
                    ),

                    LastOnlineShot = COALESCE(
                        (
                            SELECT MAX(p.LastOnlineShot)
                            FROM MFA_Pages p
                            WHERE p.File_FK = MFA_Files.ID
                              AND p.LastOnlineShot IS NOT NULL
                        ),
                        (
                            SELECT MAX(r.Shot)
                            FROM MFA_Rows r
                            JOIN MFA_Pages p
                                ON p.ID = r.Page_FK
                            WHERE p.File_FK = MFA_Files.ID
                              AND r.Shot IS NOT NULL
                        )
                    )
                WHERE ID = ?
            """, (file_id,))

            conn.commit()

        return {
            "file_id": file_id,
            "file_name": file_name,
            "pages": page_count,
            "rows": row_count,
            "values": value_count,
        }

    def update_mfa_file_summary(self):
        """
        Refresh MFA_Files summary and links.

        Updates:
        - SailLine_FK from SLSolution.SailLine = MFA_Files.LineName
        - Line / Seq / Attempt from SLSolution
        - Vessel_FK from SLSolution
        - TotalShots from MFA_Rows
        - FirstOnlineShot / LastOnlineShot from MFA_Pages,
          fallback to MFA_Rows.Shot when page header does not have them.
        """

        self.ensure_tables()

        with self.connect() as conn:
            conn.execute("""
                UPDATE MFA_Files
                SET SailLine_FK = (
                    SELECT sl.ID
                    FROM SLSolution sl
                    WHERE TRIM(LOWER(sl.SailLine)) =
                          TRIM(LOWER(MFA_Files.LineName))
                    LIMIT 1
                )
                WHERE LineName IS NOT NULL
                  AND TRIM(LineName) <> ''
            """)

            conn.execute("""
                UPDATE MFA_Files
                SET
                    Line = (
                        SELECT sl.Line
                        FROM SLSolution sl
                        WHERE sl.ID = MFA_Files.SailLine_FK
                    ),

                    Seq = (
                        SELECT sl.Seq
                        FROM SLSolution sl
                        WHERE sl.ID = MFA_Files.SailLine_FK
                    ),

                    Attempt = (
                        SELECT sl.Attempt
                        FROM SLSolution sl
                        WHERE sl.ID = MFA_Files.SailLine_FK
                    ),

                    Vessel_FK = (
                        SELECT sl.Vessel_FK
                        FROM SLSolution sl
                        WHERE sl.ID = MFA_Files.SailLine_FK
                    )
                WHERE SailLine_FK IS NOT NULL
            """)

            conn.execute("""
                UPDATE MFA_Files
                SET
                    TotalShots = COALESCE((
                        SELECT COUNT(DISTINCT r.Shot)
                        FROM MFA_Rows r
                        JOIN MFA_Pages p
                            ON p.ID = r.Page_FK
                        WHERE p.File_FK = MFA_Files.ID
                          AND r.Shot IS NOT NULL
                    ), 0),

                    FirstOnlineShot = COALESCE(
                        (
                            SELECT MIN(p.FirstOnlineShot)
                            FROM MFA_Pages p
                            WHERE p.File_FK = MFA_Files.ID
                              AND p.FirstOnlineShot IS NOT NULL
                        ),
                        (
                            SELECT MIN(r.Shot)
                            FROM MFA_Rows r
                            JOIN MFA_Pages p
                                ON p.ID = r.Page_FK
                            WHERE p.File_FK = MFA_Files.ID
                              AND r.Shot IS NOT NULL
                        )
                    ),

                    LastOnlineShot = COALESCE(
                        (
                            SELECT MAX(p.LastOnlineShot)
                            FROM MFA_Pages p
                            WHERE p.File_FK = MFA_Files.ID
                              AND p.LastOnlineShot IS NOT NULL
                        ),
                        (
                            SELECT MAX(r.Shot)
                            FROM MFA_Rows r
                            JOIN MFA_Pages p
                                ON p.ID = r.Page_FK
                            WHERE p.File_FK = MFA_Files.ID
                              AND r.Shot IS NOT NULL
                        )
                    )
            """)

            conn.commit()

    def list_files(self):
        self.ensure_tables()
        with self.connect() as conn:
            return conn.execute("""
                SELECT
                    f.ID,
                    f.FileName,
                    f.LineName,
                    f.ImportedAt,
                    COUNT(DISTINCT p.ID) AS Pages,
                    COALESCE(SUM(p.RowCount), 0) AS Rows
                FROM MFA_Files f
                LEFT JOIN MFA_Pages p ON p.File_FK = f.ID
                GROUP BY f.ID
                ORDER BY f.ID DESC
            """).fetchall()

    def render_mfa_files_table_body(self, template_file, request=None):
        """
        Render only MFA files table body.

        Useful for AJAX refresh.
        """

        self.update_mfa_file_summary()

        mfa_files = self.list_files()

        return render_to_string(
            template_file,
            {
                "mfa_files": mfa_files,
            },
            request=request,
        )