from __future__ import annotations

import csv
import math
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import pandas as pd


PAGE_RE = re.compile(
    r"^#Page\s+(?P<page_no>\d+)\s+(?P<title>.*?)\s+First Online Shot\s+(?P<first_shot>\d+)\s+Last Online Shot\s+(?P<last_shot>\d+)\s*$"
)


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
    """Remove non-printable characters and trim whitespace from MFA headers."""
    if value is None:
        return ""
    return "".join(ch for ch in str(value) if ch.isprintable()).strip()


def to_float(value: str) -> float | None:
    """Convert an MFA value to float; blank/non-numeric values become None."""
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def to_int(value: str) -> int | None:
    """Convert an MFA value to int through float; blank/non-numeric values become None."""
    number = to_float(value)
    if number is None or math.isnan(number):
        return None
    return int(number)


def iter_mfa_pages(path: str | Path) -> Iterable[MFAPage]:
    """
    Parse a Gator MFA file.

    MFA structure in your samples:
    - global metadata lines start with #FileVersion, #UUID, #Line Name
    - each plot page starts with #Page ... First Online Shot ... Last Online Shot ...
    - H row contains column names
    - T row contains column types
    - S rows contain samples
    """
    path = Path(path)
    current_page = None
    header: list[str] = []
    types: list[str] = []
    rows: list[list[str]] = []

    def flush_page():
        nonlocal current_page, header, types, rows
        if current_page is not None and header:
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
                current_page = {
                    "page_no": int(page_match.group("page_no")),
                    "title": clean_name(page_match.group("title")),
                    "first_online_shot": int(page_match.group("first_shot")),
                    "last_online_shot": int(page_match.group("last_shot")),
                }
                continue

            if current_page is None:
                continue

            parts = next(csv.reader([line]))
            rec_type = parts[0].strip() if parts else ""
            values = [clean_name(v) for v in parts[1:]]

            if rec_type == "H":
                header = values
            elif rec_type == "T":
                types = values
            elif rec_type == "S":
                # pad short rows, trim long rows
                if len(values) < len(header):
                    values += [""] * (len(header) - len(values))
                rows.append(values[: len(header)])

    yield from flush_page()


def read_mfa_meta(path: str | Path) -> dict:
    """Read the MFA global metadata found before the first page."""
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


class MFADB:
    """SQLite loader and plotter for Gator MFA files."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    def ensure_tables(self) -> None:
        """Create normalized MFA tables. This does not touch existing SeisWebLog tables."""
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS MFA_Files (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    FileName TEXT NOT NULL,
                    FilePath TEXT,
                    FileSize INTEGER DEFAULT 0,
                    FileVersion TEXT,
                    UUID TEXT,
                    LineName TEXT,
                    ImportedAt TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(FileName, FileSize, UUID)
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
                """
            )

    def import_file(self, mfa_path: str | Path, replace_existing: bool = True) -> int:
        """Import one MFA file into normalized SQLite tables and return MFA_Files.ID."""
        self.ensure_tables()
        mfa_path = Path(mfa_path)
        meta = read_mfa_meta(mfa_path)

        with self.connect() as conn:
            if replace_existing:
                old = conn.execute(
                    "SELECT ID FROM MFA_Files WHERE FileName=? AND FileSize=?",
                    (mfa_path.name, mfa_path.stat().st_size),
                ).fetchone()
                if old:
                    conn.execute("DELETE FROM MFA_Files WHERE ID=?", (old["ID"],))

            cur = conn.execute(
                """
                INSERT OR IGNORE INTO MFA_Files
                    (FileName, FilePath, FileSize, FileVersion, UUID, LineName)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    mfa_path.name,
                    str(mfa_path),
                    mfa_path.stat().st_size,
                    meta.get("file_version"),
                    meta.get("uuid"),
                    meta.get("line_name"),
                ),
            )
            file_id = cur.lastrowid or conn.execute(
                "SELECT ID FROM MFA_Files WHERE FileName=? AND FileSize=?",
                (mfa_path.name, mfa_path.stat().st_size),
            ).fetchone()["ID"]

            for page in iter_mfa_pages(mfa_path):
                page_cur = conn.execute(
                    """
                    INSERT INTO MFA_Pages
                        (File_FK, PageNo, PageTitle, FirstOnlineShot, LastOnlineShot, ColumnCount, RowCount)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        file_id,
                        page.page_no,
                        page.title,
                        page.first_online_shot,
                        page.last_online_shot,
                        len(page.header),
                        len(page.rows),
                    ),
                )
                page_id = page_cur.lastrowid

                # first 8 fields are common metadata in your samples
                common_cols = {name: idx for idx, name in enumerate(page.header)}
                value_start_idx = 8

                row_payload = []
                value_payload = []

                for row_no, row in enumerate(page.rows, start=1):
                    row_payload.append(
                        (
                            page_id,
                            row_no,
                            row[common_cols.get("Preplot Name", -1)] if "Preplot Name" in common_cols else None,
                            row[common_cols.get("Line Name", -1)] if "Line Name" in common_cols else None,
                            row[common_cols.get("Vessel Name", -1)] if "Vessel Name" in common_cols else None,
                            to_int(row[common_cols.get("Shot", -1)]) if "Shot" in common_cols else None,
                            to_float(row[common_cols.get("Raw Time", -1)]) if "Raw Time" in common_cols else None,
                            row[common_cols.get("Time", -1)] if "Time" in common_cols else None,
                            row[common_cols.get("Date", -1)] if "Date" in common_cols else None,
                        )
                    )

                conn.executemany(
                    """
                    INSERT INTO MFA_Rows
                        (Page_FK, RowNo, PreplotName, LineName, VesselName, Shot, RawTime, Time, DateText)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    row_payload,
                )

                row_ids = conn.execute(
                    "SELECT ID, RowNo FROM MFA_Rows WHERE Page_FK=? ORDER BY RowNo", (page_id,)
                ).fetchall()
                row_id_by_no = {r["RowNo"]: r["ID"] for r in row_ids}

                for row_no, row in enumerate(page.rows, start=1):
                    row_id = row_id_by_no[row_no]
                    for col_idx in range(value_start_idx, len(page.header)):
                        col_name = page.header[col_idx]
                        raw_value = row[col_idx] if col_idx < len(row) else ""
                        col_type = page.types[col_idx] if col_idx < len(page.types) else ""
                        is_quality = col_name.lower().endswith(" quality")
                        value_payload.append(
                            (
                                row_id,
                                col_idx,
                                col_name,
                                col_type,
                                raw_value,
                                to_float(raw_value),
                                to_int(raw_value) if is_quality else None,
                            )
                        )

                conn.executemany(
                    """
                    INSERT INTO MFA_Values
                        (Row_FK, ColumnIndex, ColumnName, ColumnType, ValueText, ValueReal, Quality)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    value_payload,
                )

        return file_id

    def import_folder(self, folder: str | Path, pattern: str = "*.mfa") -> list[int]:
        """Import all MFA files from a folder."""
        ids = []
        for path in sorted(Path(folder).glob(pattern)):
            ids.append(self.import_file(path))
        return ids

    def plot_pages(self, output_dir: str | Path, file_id: int | None = None, max_series: int = 12) -> list[Path]:
        """
        Create one PNG per MFA page.

        Plot logic:
        - x-axis = Shot
        - y-series = all non-quality numeric columns, limited by max_series
        - Quality columns are stored in DB, but not plotted as lines here
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        made: list[Path] = []

        with self.connect() as conn:
            file_filter = "WHERE f.ID=?" if file_id else ""
            params = (file_id,) if file_id else ()
            pages = conn.execute(
                f"""
                SELECT p.ID, p.PageNo, p.PageTitle, f.FileName, f.LineName
                FROM MFA_Pages p
                JOIN MFA_Files f ON f.ID = p.File_FK
                {file_filter}
                ORDER BY f.FileName, p.PageNo
                """,
                params,
            ).fetchall()

            for page in pages:
                df = pd.read_sql_query(
                    """
                    SELECT r.Shot, v.ColumnName, v.ValueReal
                    FROM MFA_Rows r
                    JOIN MFA_Values v ON v.Row_FK = r.ID
                    WHERE r.Page_FK = ?
                      AND v.ValueReal IS NOT NULL
                      AND LOWER(v.ColumnName) NOT LIKE '% quality'
                    ORDER BY r.Shot
                    """,
                    conn,
                    params=(page["ID"],),
                )
                if df.empty:
                    continue

                pivot = df.pivot_table(index="Shot", columns="ColumnName", values="ValueReal", aggfunc="first")
                # remove mostly empty columns and limit to keep plot readable
                pivot = pivot.dropna(axis=1, how="all")
                columns = list(pivot.columns)[:max_series]
                if not columns:
                    continue

                fig, ax = plt.subplots(figsize=(14, 7))
                for col in columns:
                    ax.plot(pivot.index, pivot[col], linewidth=1.2, marker=".", markersize=2, label=col)

                ax.set_title(f"{page['FileName']} | Page {page['PageNo']} - {page['PageTitle']}")
                ax.set_xlabel("Shot")
                ax.set_ylabel("Value")
                ax.grid(True, alpha=0.3)
                ax.legend(loc="best", fontsize=7)
                fig.tight_layout()

                safe_file = re.sub(r"[^A-Za-z0-9_.-]+", "_", page["FileName"])
                safe_title = re.sub(r"[^A-Za-z0-9_.-]+", "_", page["PageTitle"])
                out = output_dir / f"{safe_file}_page_{page['PageNo']:02d}_{safe_title}.png"
                fig.savefig(out, dpi=150)
                plt.close(fig)
                made.append(out)

        return made


if __name__ == "__main__":
    # Standalone test/example:
    # python mfa_import_plot.py
    db = MFADB("mfa_gator.sqlite3")
    for mfa in Path(".").glob("*.mfa"):
        fid = db.import_file(mfa)
        db.plot_pages("mfa_plots", file_id=fid)
        print(f"Imported and plotted: {mfa}")
