from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


class ProjectsDatabaseError(RuntimeError):
    pass


@dataclass(frozen=True)
class ProjectEntry:
    id: int
    name: str
    root_path: str
    folder_name: str

    @property
    def project_dir(self) -> Path:
        return Path(self.root_path).expanduser() / self.folder_name


class ProjectsDatabase:
    """Read SeisWebLog projects from the root Django db.sqlite3."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        if not self.db_path.is_file():
            raise ProjectsDatabaseError(f"Root database not found: {self.db_path}")

    def read_projects(self) -> list[ProjectEntry]:
        try:
            connection = sqlite3.connect(str(self.db_path), timeout=10.0)
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT id, name, root_path, folder_name
                FROM core_project
                ORDER BY id
                """
            ).fetchall()
        except sqlite3.Error as exc:
            raise ProjectsDatabaseError(
                f"Cannot read core_project from {self.db_path}: {exc}"
            ) from exc
        finally:
            try:
                connection.close()
            except Exception:
                pass

        return [
            ProjectEntry(
                id=int(row["id"]),
                name=str(row["name"]),
                root_path=str(row["root_path"]),
                folder_name=str(row["folder_name"]),
            )
            for row in rows
        ]


def is_projects_database(path: str | Path) -> bool:
    db_path = Path(path).expanduser().resolve()
    if not db_path.is_file():
        return False
    try:
        with sqlite3.connect(str(db_path), timeout=5.0) as connection:
            return connection.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='table' AND name='core_project' LIMIT 1"
            ).fetchone() is not None
    except sqlite3.Error:
        return False
