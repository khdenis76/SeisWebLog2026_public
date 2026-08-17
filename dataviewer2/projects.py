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

    def read_active_project(self) -> ProjectEntry | None:
        """Return the active project stored in this installation's root DB.

        DataViewer has no authenticated Django request, so in the normal
        single-user desktop installation the first UserSettings row with an
        active project is used.  The project is returned only when it still
        exists in ``core_project`` (the JOIN enforces that condition).
        """
        try:
            with sqlite3.connect(str(self.db_path), timeout=10.0) as connection:
                connection.row_factory = sqlite3.Row
                row = connection.execute(
                    """
                    SELECT p.id, p.name, p.root_path, p.folder_name
                    FROM core_usersettings AS s
                    JOIN core_project AS p ON p.id = s.active_project_id
                    WHERE s.active_project_id IS NOT NULL
                    ORDER BY s.id
                    LIMIT 1
                    """
                ).fetchone()
        except sqlite3.Error as exc:
            raise ProjectsDatabaseError(
                f"Cannot read the active project from {self.db_path}: {exc}"
            ) from exc

        if row is None:
            return None
        return ProjectEntry(
            id=int(row["id"]),
            name=str(row["name"]),
            root_path=str(row["root_path"]),
            folder_name=str(row["folder_name"]),
        )

    @staticmethod
    def is_project_available(project: ProjectEntry) -> bool:
        """A selectable project must have its project SQLite database."""
        return (project.project_dir.expanduser().resolve() / "data" / "project.sqlite3").is_file()

    def read_available_projects(self) -> list[ProjectEntry]:
        return [project for project in self.read_projects() if self.is_project_available(project)]


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
