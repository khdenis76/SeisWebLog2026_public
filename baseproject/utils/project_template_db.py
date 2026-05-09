import sqlite3
from pathlib import Path


class ProjectTemplateDB:
    def __init__(self, db_path):
        self.db_path = Path(db_path)

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 60000")
        return conn

    def ensure_schema(self):
        conn = self._connect()
        try:
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS project_template (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    FirstSL INTEGER,
                    LastSL INTEGER,
                    LNum INTEGER,
                    RLine INTEGER,
                    Tier INTEGER,
                    deployed_by_vessel INTEGER,
                    recovered_by_vessel INTEGER,
                    FOREIGN KEY (deployed_by_vessel) REFERENCES project_fleet(ID),
                    FOREIGN KEY (recovered_by_vessel) REFERENCES project_fleet(ID)
                )
            """)

            existing_cols = {
                row["name"]
                for row in cur.execute("PRAGMA table_info(project_template)").fetchall()
            }

            if "deployed_by_vessel" not in existing_cols:
                cur.execute("ALTER TABLE project_template ADD COLUMN deployed_by_vessel INTEGER")

            if "recovered_by_vessel" not in existing_cols:
                cur.execute("ALTER TABLE project_template ADD COLUMN recovered_by_vessel INTEGER")

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_project_template_sl_range
                ON project_template (FirstSL, LastSL)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_project_template_tier
                ON project_template (Tier)
            """)

            conn.commit()
        finally:
            conn.close()

    def clear_all(self):
        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM project_template")
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

    def insert_rows(self, rows, replace=False):
        """
        rows format:
        [
            {
                "FirstSL": 1001,
                "LastSL": 1010,
                "LNum": 10,
                "RLine": 4517,
                "Tier": 1,
                "deployed_by_vessel": 1,
                "recovered_by_vessel": 2,
            }
        ]
        """

        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()

            if replace:
                cur.execute("DELETE FROM project_template")

            values = []
            for row in rows:
                values.append((
                    row.get("FirstSL"),
                    row.get("LastSL"),
                    row.get("LNum"),
                    row.get("RLine"),
                    row.get("Tier"),
                    row.get("deployed_by_vessel"),
                    row.get("recovered_by_vessel"),
                ))

            cur.executemany("""
                INSERT INTO project_template (
                    FirstSL,
                    LastSL,
                    LNum,
                    RLine,
                    Tier,
                    deployed_by_vessel,
                    recovered_by_vessel
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, values)

            conn.commit()
            return len(values)
        finally:
            conn.close()

    def list_rows(self, limit=None, offset=0):
        self.ensure_schema()

        sql = """
            SELECT
                pt.ID,
                pt.FirstSL,
                pt.LastSL,
                pt.LNum,
                pt.RLine,
                pt.Tier,
                pt.deployed_by_vessel,
                pt.recovered_by_vessel,
                dv.Name AS deployed_vessel_name,
                rv.Name AS recovered_vessel_name
            FROM project_template pt
            LEFT JOIN project_fleet dv
                ON dv.ID = pt.deployed_by_vessel
            LEFT JOIN project_fleet rv
                ON rv.ID = pt.recovered_by_vessel
            ORDER BY pt.FirstSL, pt.LastSL, pt.RLine
        """

        params = []

        if limit:
            sql += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])

        conn = self._connect()
        try:
            cur = conn.cursor()
            return [dict(row) for row in cur.execute(sql, params).fetchall()]
        finally:
            conn.close()

    def get_by_id(self, template_id):
        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()
            row = cur.execute("""
                SELECT *
                FROM project_template
                WHERE ID = ?
            """, (template_id,)).fetchone()

            return dict(row) if row else None
        finally:
            conn.close()

    def delete_by_ids(self, ids):
        self.ensure_schema()

        ids = [int(x) for x in ids if str(x).strip()]
        if not ids:
            return 0

        placeholders = ",".join("?" for _ in ids)

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                f"DELETE FROM project_template WHERE ID IN ({placeholders})",
                ids,
            )
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

    def update_row(self, template_id, data):
        self.ensure_schema()

        allowed = {
            "FirstSL",
            "LastSL",
            "LNum",
            "RLine",
            "Tier",
            "deployed_by_vessel",
            "recovered_by_vessel",
        }

        fields = []
        values = []

        for key, value in data.items():
            if key in allowed:
                fields.append(f"{key} = ?")
                values.append(value)

        if not fields:
            return 0

        values.append(template_id)

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(f"""
                UPDATE project_template
                SET {", ".join(fields)}
                WHERE ID = ?
            """, values)

            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

    def summary_by_tier(self):
        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()
            return [dict(row) for row in cur.execute("""
                SELECT
                    Tier,
                    COUNT(*) AS TemplateRows,
                    SUM(COALESCE(LNum, 0)) AS TotalLines,
                    MIN(FirstSL) AS FirstSL,
                    MAX(LastSL) AS LastSL
                FROM project_template
                GROUP BY Tier
                ORDER BY Tier
            """).fetchall()]
        finally:
            conn.close()

    def summary_by_rline(self):
        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()
            return [dict(row) for row in cur.execute("""
                SELECT
                    RLine,
                    Tier,
                    COUNT(*) AS TemplateRows,
                    SUM(COALESCE(LNum, 0)) AS TotalLines,
                    MIN(FirstSL) AS FirstSL,
                    MAX(LastSL) AS LastSL
                FROM project_template
                GROUP BY RLine, Tier
                ORDER BY RLine, Tier
            """).fetchall()]
        finally:
            conn.close()

    def to_table_rows(self):
        """
        For Django table / JSON endpoint.
        """
        return self.list_rows()

    def to_bokeh_source_rows(self):
        """
        Later usable directly with ColumnDataSource.
        """
        rows = self.list_rows()

        return {
            "ID": [r["ID"] for r in rows],
            "FirstSL": [r["FirstSL"] for r in rows],
            "LastSL": [r["LastSL"] for r in rows],
            "LNum": [r["LNum"] for r in rows],
            "RLine": [r["RLine"] for r in rows],
            "Tier": [r["Tier"] for r in rows],
            "deployed_vessel_name": [r["deployed_vessel_name"] or "" for r in rows],
            "recovered_vessel_name": [r["recovered_vessel_name"] or "" for r in rows],
        }