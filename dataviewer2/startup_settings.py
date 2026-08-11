from __future__ import annotations

from pathlib import Path

from PySide6 import QtCore


LAST_PROJECT_KEY = "startup/last_project"
PROJECTS_DATABASE_KEY = "startup/projects_database"


def remembered_project() -> Path | None:
    value = QtCore.QSettings().value(LAST_PROJECT_KEY, "", type=str).strip()
    return Path(value).expanduser() if value else None


def remember_project(path: str | Path) -> None:
    settings = QtCore.QSettings()
    settings.setValue(LAST_PROJECT_KEY, str(Path(path).expanduser().resolve()))
    settings.sync()


def forget_project() -> None:
    settings = QtCore.QSettings()
    settings.remove(LAST_PROJECT_KEY)
    settings.sync()


def remembered_projects_database() -> Path | None:
    value = QtCore.QSettings().value(PROJECTS_DATABASE_KEY, "", type=str).strip()
    return Path(value).expanduser() if value else None


def remember_projects_database(path: str | Path) -> None:
    settings = QtCore.QSettings()
    settings.setValue(PROJECTS_DATABASE_KEY, str(Path(path).expanduser().resolve()))
    settings.sync()
