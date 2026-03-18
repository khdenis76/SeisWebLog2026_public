from __future__ import annotations

import sqlite3
from typing import Dict, Tuple, Any


def load_dsr(db_path: str) -> Dict[Tuple[str, str], dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT
            COALESCE(Line, ''),
            COALESCE(Station, ''),
            PrimaryEasting,
            PrimaryNorthing,
            COALESCE(TimeStamp, ''),
            COALESCE(TimeStamp1, ''),
            COALESCE(ROV, ''),
            COALESCE(ROV1, '')
        FROM DSR
        """
    ).fetchall()
    conn.close()

    dsr: Dict[Tuple[str, str], dict[str, Any]] = {}
    for line, station, e, n, ts, ts1, rov, rov1 in rows:
        key = (str(line).strip(), str(station).strip())
        dsr[key] = {
            "line": key[0],
            "station": key[1],
            "x": float(e) if e not in (None, "") else None,
            "y": float(n) if n not in (None, "") else None,
            "timestamp": str(ts or ""),
            "timestamp1": str(ts1 or ""),
            "rov": str(rov or ""),
            "rov1": str(rov1 or ""),
        }
    return dsr


def load_distinct_rov_values(db_path: str) -> dict[str, list[str]]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        rov_rows = cur.execute(
            """
            SELECT DISTINCT TRIM(COALESCE(ROV, ''))
            FROM DSR
            WHERE TRIM(COALESCE(ROV, '')) <> ''
            ORDER BY 1
            """
        ).fetchall()
        rov1_rows = cur.execute(
            """
            SELECT DISTINCT TRIM(COALESCE(ROV1, ''))
            FROM DSR
            WHERE TRIM(COALESCE(ROV1, '')) <> ''
            ORDER BY 1
            """
        ).fetchall()
        return {
            "rov": [str(r[0]) for r in rov_rows],
            "rov1": [str(r[0]) for r in rov1_rows],
        }
    except sqlite3.Error:
        return {"rov": [], "rov1": []}
    finally:
        conn.close()
