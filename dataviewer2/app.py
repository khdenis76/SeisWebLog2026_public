from __future__ import annotations

import os

# Keep PROJ deterministic and local. Shape reprojection is serialized in the
# main thread because some Windows PROJ builds are unstable under concurrent use.
os.environ.setdefault("PROJ_NETWORK", "OFF")

import argparse
import sys
import traceback
from datetime import datetime
from pathlib import Path
import pyqtgraph as pg
from PySide6 import QtCore, QtWidgets

from .main_window import MainWindow
from .projects import ProjectsDatabase, ProjectsDatabaseError, is_projects_database
from .startup_settings import (
    forget_project,
    remember_project,
    remember_projects_database,
    remembered_project,
)


def choose_project(root_db: Path) -> Path | None:
    try:
        projects = ProjectsDatabase(root_db).read_projects()
    except ProjectsDatabaseError as exc:
        QtWidgets.QMessageBox.critical(None, "DataViewer 2.0", str(exc))
        return None
    remember_projects_database(root_db)
    labels = [f"{p.name} — {p.project_dir}" for p in projects]
    if not labels:
        QtWidgets.QMessageBox.warning(None, "DataViewer 2.0", "No projects found.")
        return None
    selected, ok = QtWidgets.QInputDialog.getItem(None, "DataViewer 2.0", "Project:", labels, 0, False)
    return projects[labels.index(selected)].project_dir if ok else None


def find_projects_database() -> Path:
    """Return the SeisWebLog database located beside the dataviewer2 package."""
    return Path(__file__).resolve().parents[1] / "db.sqlite3"


def resolve_start_path(argument: str | None) -> tuple[Path | None, bool]:
    if not argument:
        previous = remembered_project()
        if previous is not None and previous.exists():
            return previous.resolve(), True
        if previous is not None:
            forget_project()
        return choose_project(find_projects_database()), False
    supplied = Path(argument).expanduser().resolve()
    if is_projects_database(supplied):
        return choose_project(supplied), False
    root_db = supplied / "db.sqlite3" if supplied.is_dir() else None
    if root_db and is_projects_database(root_db):
        return choose_project(root_db), False
    return supplied, False


def show_startup_error(project_path: Path, text: str) -> None:
    log_locations = [
        Path.cwd() / "dataviewer2_startup_error.log",
        project_path.parent / "dataviewer2_startup_error.log"
        if project_path.is_file()
        else project_path / "dataviewer2_startup_error.log",
    ]
    saved_to = None
    for log_path in log_locations:
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text(text, encoding="utf-8")
            saved_to = log_path
            break
        except Exception:
            continue
    message = text[-6000:]
    if saved_to is not None:
        message += f"\n\nFull traceback saved to:\n{saved_to}"
    QtWidgets.QMessageBox.critical(None, "DataViewer startup error", message)


def main() -> int:
    parser = argparse.ArgumentParser(description="SeisWebLog DataViewer 2.0")
    parser.add_argument("path", nargs="?")
    args = parser.parse_args()

    QtCore.QCoreApplication.setOrganizationName("SeisWebLog")
    QtCore.QCoreApplication.setApplicationName("DataViewer 2.0")
    # OpenGL is disabled by default because some Windows/Qt/OpenGL driver
    # combinations terminate the process without a Python traceback.
    pg.setConfigOptions(antialias=False, useOpenGL=False, background="#111317", foreground="#e8e8e8")
    app = QtWidgets.QApplication(sys.argv)

    def exception_hook(exc_type, exc_value, exc_tb):
        text = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        try:
            log_path = Path.cwd() / "dataviewer2_crash.log"
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(f"\n--- {datetime.now().isoformat(timespec='seconds')} ---\n")
                handle.write(text)
        except Exception:
            pass
        QtWidgets.QMessageBox.critical(None, "DataViewer 2.0 error", text[-5000:])

    sys.excepthook = exception_hook
    app.setStyle("Fusion")
    project_path, is_remembered = resolve_start_path(args.path)
    if project_path is None:
        return 0
    try:
        window = MainWindow(project_path)
    except Exception:
        text = traceback.format_exc()
        if not is_remembered:
            show_startup_error(project_path, text)
            return 1
        forget_project()
        QtWidgets.QMessageBox.warning(
            None,
            "DataViewer 2.0",
            "The last project is unavailable or cannot be opened. Please select another project.",
        )
        project_path = choose_project(find_projects_database())
        if project_path is None:
            return 0
        try:
            window = MainWindow(project_path)
        except Exception:
            show_startup_error(project_path, traceback.format_exc())
            return 1
    remember_project(project_path)
    window.showMaximized()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
