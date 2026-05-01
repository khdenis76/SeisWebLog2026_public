from __future__ import annotations

import sqlite3


def load_preplot_points(db_path, line=None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    sql = """
        SELECT
            Line as line,
            Point as station,
            X as x,
            Y as y
        FROM main.RPPreplot
    """
    params = []
    if line:
        sql += " WHERE Line=?"
        params.append(line)
    rows = cur.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def load_dsr_station_points_from_ocr(db_path, line=None):
    """Return one DSR/OCR point per DSR station with image counts.

    v5 fix: image count is calculated from OCR result rows. Earlier map code sometimes
    showed empty values because station_image_count was not selected/grouped correctly.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    sql = """
       SELECT
            COALESCE(NULLIF(dsr_line,''), file_line, line) AS dsr_line,
            COALESCE(NULLIF(dsr_station,''), file_station, station) AS dsr_station,
            MAX(dsr_x) AS dsr_x,
            MAX(dsr_y) AS dsr_y,
            MAX(dsr_rov) AS dsr_rov,
            MAX(dsr_rov1) AS dsr_rov1,
            MAX(status) AS status,
            MAX(station_status) AS station_status,
            COUNT(*) AS images,
            MAX(COALESCE(station_image_count, 0)) AS station_image_count,
            MAX(message) AS message,
            MAX(dsr_timestamp) AS dsr_timestamp,
            MAX(dsr_timestamp1) AS dsr_timestamp1
        FROM ocr_results
        WHERE dsr_x IS NOT NULL
          AND dsr_y IS NOT NULL
          AND TRIM(COALESCE(dsr_x,'')) <> ''
          AND TRIM(COALESCE(dsr_y,'')) <> ''
    """
    params = []
    if line:
        sql += " AND (dsr_line=? OR file_line=? OR line=?)"
        params.extend([line, line, line])
    sql += """
        GROUP BY COALESCE(NULLIF(dsr_line,''), file_line, line),
                 COALESCE(NULLIF(dsr_station,''), file_station, station)
        ORDER BY dsr_line, dsr_station
    """
    rows = cur.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]
