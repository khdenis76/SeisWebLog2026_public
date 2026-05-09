import sqlite3
from pathlib import Path


BASE_SOLUTIONS = [
    (1, "USBL Deploy", "Default NOAR solution"),
    (2, "PING A", "Default NOAR solution"),
    (3, "PING B", "Default NOAR solution"),
    (4, "RETRIEVE", "Default NOAR solution"),
    (5, "FB", "Default NOAR solution"),
]


class SolutionsDB:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_table(self):

        with self._connect() as conn:

            cur = conn.cursor()

            table_exists = cur.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                  AND name='Solutions'
            """).fetchone()

            old_rows = []

            if table_exists:

                cols = [
                    r["name"]
                    for r in cur.execute(
                        "PRAGMA table_info(Solutions)"
                    ).fetchall()
                ]

                # OLD BROKEN TABLE STRUCTURE
                if "Comments" not in cols:

                    try:
                        old_rows = cur.execute("""
                            SELECT ID, Solution
                            FROM Solutions
                        """).fetchall()

                    except Exception:
                        old_rows = []

                    cur.execute("""
                        DROP TABLE Solutions
                    """)

                    conn.commit()

            # CREATE NEW STRUCTURE
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Solutions (

                    ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,

                    Solution TEXT NOT NULL UNIQUE,

                    Comments TEXT DEFAULT '',

                    SpareInt1 INTEGER DEFAULT NULL,
                    SpareInt2 INTEGER DEFAULT NULL,

                    SpareText1 TEXT DEFAULT '',
                    SpareText2 TEXT DEFAULT '',

                    IsBase INTEGER NOT NULL DEFAULT 0
                )
            """)

            # RESTORE OLD DATA
            for row in old_rows:

                try:

                    cur.execute("""
                        INSERT OR IGNORE INTO Solutions (
                            ID,
                            Solution,
                            Comments,
                            IsBase
                        )
                        VALUES (?, ?, '', 0)
                    """, (
                        row["ID"],
                        row["Solution"],
                    ))

                except Exception as e:
                    print("[SOLUTIONS] restore warning:", e)

            # DEFAULT BASE SOLUTIONS
            base_solutions = [
                ("USBL Deploy", "Default NOAR solution"),
                ("PING A", "Default NOAR solution"),
                ("PING B", "Default NOAR solution"),
                ("RETRIEVE", "Default NOAR solution"),
                ("FB", "Default NOAR solution"),
            ]

            for solution_name, comments in base_solutions:
                cur.execute("""
                    INSERT OR IGNORE INTO Solutions (
                        Solution,
                        Comments,
                        IsBase
                    )
                    VALUES (?, ?, 1)
                """, (
                    solution_name,
                    comments,
                ))

                cur.execute("""
                    UPDATE Solutions
                    SET
                        IsBase = 1,
                        Comments = CASE
                            WHEN Comments IS NULL
                                 OR TRIM(Comments) = ''
                            THEN ?
                            ELSE Comments
                        END
                    WHERE Solution = ?
                """, (
                    comments,
                    solution_name,
                ))

            conn.commit()

    def list_solutions(self):
        self.ensure_table()

        with self._connect() as conn:
            rows = conn.execute("""
                SELECT
                    ID,
                    Solution,
                    COALESCE(Comments, '') AS Comments,
                    SpareInt1,
                    SpareInt2,
                    COALESCE(SpareText1, '') AS SpareText1,
                    COALESCE(SpareText2, '') AS SpareText2,
                    IsBase
                FROM Solutions
                ORDER BY IsBase DESC, ID ASC
            """).fetchall()

            return [dict(row) for row in rows]

    def add_solution(self, solution: str, comments: str = ""):
        self.ensure_table()

        solution = (solution or "").strip()
        comments = (comments or "").strip()

        if not solution:
            raise ValueError("Solution name is empty.")

        with self._connect() as conn:
            conn.execute("""
                INSERT INTO Solutions
                    (Solution, Comments, IsBase)
                VALUES (?, ?, 0)
            """, (solution, comments))
            conn.commit()

    def delete_solution(self, solution_id: int):
        self.ensure_table()

        with self._connect() as conn:
            row = conn.execute("""
                SELECT ID, IsBase
                FROM Solutions
                WHERE ID = ?
            """, (solution_id,)).fetchone()

            if not row:
                raise ValueError("Solution not found.")

            if int(row["IsBase"]) == 1:
                raise ValueError("Base solutions cannot be deleted.")

            conn.execute("""
                DELETE FROM Solutions
                WHERE ID = ?
            """, (solution_id,))

            conn.commit()