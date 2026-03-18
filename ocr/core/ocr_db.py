from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List

TABLE_NAME = "ocr_results"


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                image_path TEXT NOT NULL UNIQUE,
                image_name TEXT,
                resolution TEXT,
                config_used TEXT,
                file_role TEXT,
                file_line TEXT,
                file_station TEXT,
                file_index TEXT,
                rov TEXT,
                dive INTEGER,
                date TEXT,
                time TEXT,
                line TEXT,
                station TEXT,
                east TEXT,
                north TEXT,
                dsr_line TEXT,
                dsr_station TEXT,
                dsr_x TEXT,
                dsr_y TEXT,
                dsr_timestamp TEXT,
                dsr_timestamp1 TEXT,
                dsr_rov TEXT,
                dsr_rov1 TEXT,
                delta_m REAL,
                ocr_vs_file TEXT,
                file_vs_dsr TEXT,
                status TEXT,
                station_image_count INTEGER,
                expected_images TEXT,
                station_status TEXT,
                message TEXT,
                checked INTEGER NOT NULL DEFAULT 1,
                processed_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        for sql in [
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_path ON {TABLE_NAME}(image_path)",
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_line_station ON {TABLE_NAME}(file_line, file_station)",
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_status ON {TABLE_NAME}(status)",
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_checked ON {TABLE_NAME}(checked)",
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_delta ON {TABLE_NAME}(delta_m)",
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_ts ON {TABLE_NAME}(dsr_timestamp)",
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_ts1 ON {TABLE_NAME}(dsr_timestamp1)",
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_rov ON {TABLE_NAME}(dsr_rov)",
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_rov1 ON {TABLE_NAME}(dsr_rov1)",
        ]:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


_COLUMNS = [
    "image_path", "image_name", "resolution", "config_used", "file_role", "file_line",
    "file_station", "file_index", "rov", "dive", "date", "time", "line", "station",
    "east", "north", "dsr_line", "dsr_station", "dsr_x", "dsr_y", "dsr_timestamp",
    "dsr_timestamp1", "dsr_rov", "dsr_rov1", "delta_m", "ocr_vs_file", "file_vs_dsr",
    "status", "station_image_count", "expected_images", "station_status", "message", "checked",
]


def upsert_result(db_path: str, row: Dict[str, Any]) -> None:
    ensure_schema(db_path)
    conn = _connect(db_path)
    try:
        data = {
            "image_path": row.get("image_path", ""),
            "image_name": row.get("image", ""),
            "resolution": row.get("resolution", ""),
            "config_used": row.get("config_used", ""),
            "file_role": row.get("file_role", ""),
            "file_line": row.get("file_line", ""),
            "file_station": row.get("file_station", ""),
            "file_index": row.get("file_index", ""),
            "rov": row.get("rov", ""),
            "dive": _safe_int(row.get("dive")),
            "date": row.get("date", ""),
            "time": row.get("time", ""),
            "line": row.get("line", ""),
            "station": row.get("station", ""),
            "east": row.get("east", ""),
            "north": row.get("north", ""),
            "dsr_line": row.get("dsr_line", ""),
            "dsr_station": row.get("dsr_station", ""),
            "dsr_x": row.get("dsr_x", ""),
            "dsr_y": row.get("dsr_y", ""),
            "dsr_timestamp": row.get("dsr_timestamp", ""),
            "dsr_timestamp1": row.get("dsr_timestamp1", ""),
            "dsr_rov": row.get("dsr_rov", ""),
            "dsr_rov1": row.get("dsr_rov1", ""),
            "delta_m": _safe_float(row.get("delta_m")),
            "ocr_vs_file": row.get("ocr_vs_file", ""),
            "file_vs_dsr": row.get("file_vs_dsr", ""),
            "status": row.get("status", ""),
            "station_image_count": _safe_int(row.get("station_image_count")),
            "expected_images": row.get("expected_images", ""),
            "station_status": row.get("station_status", ""),
            "message": row.get("message", ""),
            "checked": 1 if bool(row.get("checked", True)) else 0,
        }
        cols = ", ".join(_COLUMNS)
        placeholders = ", ".join(["?"] * len(_COLUMNS))
        updates = ", ".join([f"{c}=excluded.{c}" for c in _COLUMNS if c != "image_path"])
        conn.execute(
            f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({placeholders}) "
            f"ON CONFLICT(image_path) DO UPDATE SET {updates}, processed_at=CURRENT_TIMESTAMP",
            [data[c] for c in _COLUMNS],
        )
        conn.commit()
    finally:
        conn.close()


def fetch_results(
    db_path: str,
    dsr_line: str = "",
    dsr_station: str = "",
    status: str = "",
    checked: str = "",
    min_delta: str = "",
    max_delta: str = "",
    min_images: str = "",
    max_images: str = "",
    dsr_rov: str = "",
    dsr_rov1: str = "",
    deploy_day: str = "",
    deploy_from: str = "",
    deploy_to: str = "",
    recover_day: str = "",
    recover_from: str = "",
    recover_to: str = "",
    sort_by: str = "file_line",
    descending: bool = False,
) -> List[Dict[str, Any]]:
    ensure_schema(db_path)
    conn = _connect(db_path)
    try:
        where = []
        params: List[Any] = []
        if dsr_line:
            where.append("(file_line = ? OR line = ? OR dsr_line = ?)")
            params += [dsr_line, dsr_line, dsr_line]
        if dsr_station:
            where.append("(file_station = ? OR station = ? OR dsr_station = ?)")
            params += [dsr_station, dsr_station, dsr_station]
        if status:
            where.append("(status = ? OR station_status = ?)")
            params += [status, status]
        if checked in ("0", "1"):
            where.append("checked = ?")
            params.append(int(checked))
        if min_delta != "":
            where.append("COALESCE(delta_m, 0) >= ?")
            params.append(float(min_delta))
        if max_delta != "":
            where.append("COALESCE(delta_m, 0) <= ?")
            params.append(float(max_delta))
        if min_images != "":
            where.append("COALESCE(station_image_count, 0) >= ?")
            params.append(int(min_images))
        if max_images != "":
            where.append("COALESCE(station_image_count, 0) <= ?")
            params.append(int(max_images))
        if dsr_rov:
            where.append("dsr_rov = ?")
            params.append(dsr_rov)
        if dsr_rov1:
            where.append("dsr_rov1 = ?")
            params.append(dsr_rov1)
        if deploy_day:
            where.append("substr(COALESCE(dsr_timestamp,''), 1, 10) = ?")
            params.append(deploy_day)
        else:
            if deploy_from:
                where.append("substr(COALESCE(dsr_timestamp,''), 1, 10) >= ?")
                params.append(deploy_from)
            if deploy_to:
                where.append("substr(COALESCE(dsr_timestamp,''), 1, 10) <= ?")
                params.append(deploy_to)
        if recover_day:
            where.append("substr(COALESCE(dsr_timestamp1,''), 1, 10) = ?")
            params.append(recover_day)
        else:
            if recover_from:
                where.append("substr(COALESCE(dsr_timestamp1,''), 1, 10) >= ?")
                params.append(recover_from)
            if recover_to:
                where.append("substr(COALESCE(dsr_timestamp1,''), 1, 10) <= ?")
                params.append(recover_to)

        safe_sort = sort_by if sort_by in {
            "file_line", "file_station", "status", "delta_m", "date", "time", "station_image_count",
            "processed_at", "line", "station", "dsr_timestamp", "dsr_timestamp1", "dsr_rov", "dsr_rov1"
        } else "file_line"
        order = "DESC" if descending else "ASC"
        sql = f"SELECT * FROM {TABLE_NAME}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += f" ORDER BY {safe_sort} {order}, file_station ASC, file_index ASC"
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) | {"image": dict(r).get("image_name", "")} for r in rows]
    finally:
        conn.close()


def set_checked(db_path: str, image_paths: Iterable[str], checked: bool) -> None:
    ensure_schema(db_path)
    conn = _connect(db_path)
    try:
        for image_path in image_paths:
            conn.execute(
                f"UPDATE {TABLE_NAME} SET checked=?, processed_at=CURRENT_TIMESTAMP WHERE image_path=?",
                (1 if checked else 0, image_path),
            )
        conn.commit()
    finally:
        conn.close()


def is_checked(db_path: str, image_path: str) -> bool:
    ensure_schema(db_path)
    conn = _connect(db_path)
    try:
        row = conn.execute(f"SELECT checked FROM {TABLE_NAME} WHERE image_path=?", (image_path,)).fetchone()
        return bool(row["checked"]) if row else False
    finally:
        conn.close()


def _safe_int(v: Any):
    try:
        if v in ("", None):
            return None
        return int(str(v).strip())
    except Exception:
        return None


def _safe_float(v: Any):
    try:
        if v in ("", None):
            return None
        return float(str(v).strip())
    except Exception:
        return None
