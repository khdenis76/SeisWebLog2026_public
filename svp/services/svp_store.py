import csv
import io
import sqlite3
from pathlib import Path

DEPTH_HEADERS = {"depth", "depth_m", "z", "dep"}
VELOCITY_HEADERS = {"velocity", "soundvelocity", "sound_velocity", "sv", "vel", "speed"}


def _connect(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_svp_tables(db_path: str):
    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS svp_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                survey_time TEXT,
                source TEXT,
                location TEXT,
                comment TEXT,
                point_count INTEGER DEFAULT 0,
                max_depth REAL,
                min_velocity REAL,
                max_velocity REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS svp_points (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER NOT NULL,
                depth REAL NOT NULL,
                velocity REAL NOT NULL,
                point_order INTEGER NOT NULL,
                FOREIGN KEY(profile_id) REFERENCES svp_profiles(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_svp_points_profile_id ON svp_points(profile_id);
            """
        )
        conn.commit()


def list_profiles(db_path: str):
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                id,
                name,
                survey_time,
                source,
                location,
                comment,
                point_count,
                max_depth,
                min_velocity,
                max_velocity,
                created_at
            FROM svp_profiles
            ORDER BY COALESCE(survey_time, created_at) DESC, id DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def get_profile_points(db_path: str, profile_id: int):
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT depth, velocity, point_order
            FROM svp_points
            WHERE profile_id = ?
            ORDER BY point_order ASC, depth ASC
            """,
            (profile_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def _normalize_header(value: str) -> str:
    return (value or "").strip().lower().replace(" ", "").replace("-", "").replace("/", "")


def _detect_columns(headers):
    depth_idx = None
    velocity_idx = None
    for i, h in enumerate(headers):
        hh = _normalize_header(h)
        if hh in DEPTH_HEADERS and depth_idx is None:
            depth_idx = i
        if hh in VELOCITY_HEADERS and velocity_idx is None:
            velocity_idx = i
    if depth_idx is None or velocity_idx is None:
        raise ValueError("CSV must contain depth and velocity columns.")
    return depth_idx, velocity_idx


def import_svp_csv(db_path: str, csv_text: str, profile_name: str, source: str = "CSV", survey_time=None, location=None, comment=None):
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows:
        raise ValueError("Uploaded CSV is empty.")

    depth_idx, velocity_idx = _detect_columns(rows[0])

    points = []
    for row in rows[1:]:
        if not row:
            continue
        try:
            depth_raw = (row[depth_idx] if depth_idx < len(row) else "").strip()
            velocity_raw = (row[velocity_idx] if velocity_idx < len(row) else "").strip()
            if not depth_raw or not velocity_raw:
                continue
            depth = float(depth_raw.replace(",", "."))
            velocity = float(velocity_raw.replace(",", "."))
            points.append((depth, velocity))
        except ValueError:
            continue

    if not points:
        raise ValueError("No valid depth/velocity rows were found.")

    point_count = len(points)
    max_depth = max(p[0] for p in points)
    min_velocity = min(p[1] for p in points)
    max_velocity = max(p[1] for p in points)

    with _connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO svp_profiles (
                name, survey_time, source, location, comment,
                point_count, max_depth, min_velocity, max_velocity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                profile_name,
                survey_time,
                source,
                location,
                comment,
                point_count,
                max_depth,
                min_velocity,
                max_velocity,
            ),
        )
        profile_id = cur.lastrowid
        conn.executemany(
            """
            INSERT INTO svp_points (profile_id, depth, velocity, point_order)
            VALUES (?, ?, ?, ?)
            """,
            [(profile_id, depth, velocity, idx) for idx, (depth, velocity) in enumerate(points, start=1)],
        )
        conn.commit()

    return {
        "profile_id": profile_id,
        "profile_name": profile_name,
        "point_count": point_count,
    }
