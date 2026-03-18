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
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    sql = """
       SELECT
            dsr_line,
            dsr_station,
            dsr_x,
            dsr_y,
            rov,
            dsr_rov1,
            status,
            station_status,
            station_image_count as images
        FROM ocr_results
        WHERE dsr_x IS NOT NULL
          AND dsr_y IS NOT NULL
    """

    params = []
    if line:
        sql += " AND dsr_line=?"
        params.append(line)

    sql += """
        GROUP BY dsr_line, dsr_station
        ORDER BY dsr_line, dsr_station
    """

    rows = cur.execute(sql, params).fetchall()

    conn.close()
    return [dict(r) for r in rows]