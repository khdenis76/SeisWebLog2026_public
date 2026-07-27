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


def choose_project(root_db: Path) -> Path | None:
    try:
        projects = ProjectsDatabase(root_db).read_projects()
    except ProjectsDatabaseError as exc:
        QtWidgets.QMessageBox.critical(None, "DataViewer 2.0", str(exc))
        return None
    labels = [f"{p.name} — {p.project_dir}" for p in projects]
    if not labels:
        QtWidgets.QMessageBox.warning(None, "DataViewer 2.0", "No projects found.")
        return None
    selected, ok = QtWidgets.QInputDialog.getItem(None, "DataViewer 2.0", "Project:", labels, 0, False)
    return projects[labels.index(selected)].project_dir if ok else None


def resolve_start_path(argument: str | None) -> Path | None:
    if not argument:
        return choose_project(Path.cwd() / "db.sqlite3")
    supplied = Path(argument).expanduser().resolve()
    if is_projects_database(supplied):
        return choose_project(supplied)
    root_db = supplied / "db.sqlite3" if supplied.is_dir() else None
    if root_db and is_projects_database(root_db):
        return choose_project(root_db)
    return supplied


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
    project_path = resolve_start_path(args.path)
    if project_path is None:
        return 0
    try:
        window = MainWindow(project_path)
    except Exception as exc:
        QtWidgets.QMessageBox.critical(None, "DataViewer 2.0", str(exc))
        return 1
    window.showMaximized()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
