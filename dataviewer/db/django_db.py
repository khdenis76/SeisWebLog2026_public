import sqlite3
from pathlib import Path
import pandas as pd


class DjangoDbError(Exception):
    pass


class DjangoDb:
    def __init__(self, db_dir: str = "..", db_name: str = "db.sqlite", timeout_sec: float = 5.0):
        self.db_path = (Path(db_dir).expanduser().resolve() / db_name)
        self.timeout_sec = float(timeout_sec)

    def _ensure_db_file(self) -> None:
        if not self.db_path.parent.exists():
            raise DjangoDbError(f"DB folder not found: {self.db_path.parent}")
        if not self.db_path.exists():
            raise DjangoDbError(f"DB file not found: {self.db_path}")
        if not self.db_path.is_file():
            raise DjangoDbError(f"DB path is not a file: {self.db_path}")

    def _connect(self) -> sqlite3.Connection:
        self._ensure_db_file()
        try:
            conn = sqlite3.connect(
                str(self.db_path),
                timeout=self.timeout_sec,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA schema_version;").fetchone()  # validate sqlite
            return conn
        except sqlite3.DatabaseError as e:
            raise DjangoDbError(f"SQLite open error: {e}") from e

    def read_projects(self) -> pd.DataFrame:
        with self._connect() as conn:
            # table existence
            ok = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='core_project' LIMIT 1"
            ).fetchone()
            if not ok:
                raise DjangoDbError("Table 'core_project' not found in DB.")

            df = pd.read_sql_query(
                """
                SELECT id, name, root_path, folder_name, note, owner_id, created_at
                FROM core_project
                ORDER BY id
                """,
                conn,
            )
        return df
