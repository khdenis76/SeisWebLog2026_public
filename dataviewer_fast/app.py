from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyqtgraph as pg
from PySide6 import QtWidgets

from .main_window import MainWindow
from .projects import ProjectsDatabase, ProjectsDatabaseError, is_projects_database


def choose_project(root_db: Path) -> Path | None:
    try:
        projects = ProjectsDatabase(root_db).read_projects()
    except ProjectsDatabaseError as exc:
        QtWidgets.QMessageBox.critical(None, "SeisWebLog DataViewer", str(exc))
        return None

    if not projects:
        QtWidgets.QMessageBox.warning(
            None,
            "SeisWebLog DataViewer",
            f"No projects found in {root_db}",
        )
        return None

    labels = [f"{project.name}  —  {project.project_dir}" for project in projects]
    selected, accepted = QtWidgets.QInputDialog.getItem(
        None,
        "Select SeisWebLog project",
        "Project:",
        labels,
        0,
        False,
    )
    if not accepted:
        return None

    return projects[labels.index(selected)].project_dir


def resolve_start_path(argument: str | None) -> Path | None:
    # No argument: use the same root db.sqlite3 convention as original DataViewer.
    if not argument:
        root_db = Path.cwd() / "db.sqlite3"
        return choose_project(root_db)

    supplied = Path(argument).expanduser().resolve()

    # Explicit root Django database.
    if is_projects_database(supplied):
        return choose_project(supplied)

    # Root SeisWebLog folder containing db.sqlite3.
    root_db = supplied / "db.sqlite3" if supplied.is_dir() else None
    if root_db and is_projects_database(root_db):
        return choose_project(root_db)

    # Direct project directory or direct project.sqlite3 remains supported.
    return supplied


def main() -> int:
    parser = argparse.ArgumentParser(description="Fast SeisWebLog DataViewer")
    parser.add_argument(
        "path",
        nargs="?",
        help=(
            "Optional SeisWebLog root folder/root db.sqlite3, project folder, "
            "or project.sqlite3. Without it, ./db.sqlite3 is used."
        ),
    )
    args = parser.parse_args()

    pg.setConfigOptions(
        antialias=False,
        useOpenGL=True,
        background="k",
        foreground="w",
    )

    application = QtWidgets.QApplication(sys.argv)
    project_path = resolve_start_path(args.path)
    if project_path is None:
        return 0

    try:
        window = MainWindow(project_path)
    except Exception as exc:
        QtWidgets.QMessageBox.critical(None, "SeisWebLog DataViewer", str(exc))
        return 1

    window.show()
    return application.exec()


if __name__ == "__main__":
    raise SystemExit(main())
