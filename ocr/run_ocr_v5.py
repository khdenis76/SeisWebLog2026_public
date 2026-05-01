from __future__ import annotations

import os
import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication, QFileDialog, QMessageBox
from PySide6.QtGui import QIcon

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR.parent) not in sys.path:
    sys.path.insert(0, str(BASE_DIR.parent))

IMAGE_PATH = BASE_DIR / "images" / "swl_ocr_studio.png"

from ocr.ui.main_window import OCRMainWindow  # noqa: E402
from ocr.ui.splash import FadeSplash  # noqa: E402


def choose_django_db() -> str:
    return QFileDialog.getOpenFileName(
        None,
        "Choose Django DB",
        "",
        "SQLite DB (*.sqlite *.sqlite3 *.db);;All files (*)",
    )[0]


def find_django_db() -> str:
    env_db = os.environ.get("SEISWEBLOG_DJANGO_DB", "")
    if env_db and os.path.exists(env_db):
        return env_db

    db_path = BASE_DIR.parent / "db.sqlite3"
    if db_path.exists():
        return str(db_path)

    db_path = BASE_DIR.parent.parent / "db.sqlite3"
    if db_path.exists():
        return str(db_path)

    return ""


def main() -> int:
    app = QApplication(sys.argv)
    if IMAGE_PATH.exists():
        app.setWindowIcon(QIcon(str(IMAGE_PATH)))

    django_db = find_django_db()
    if not django_db:
        django_db = choose_django_db()
    if not django_db:
        QMessageBox.warning(None, "Canceled", "No Django DB selected.")
        return 1

    win = OCRMainWindow(django_db)

    def start_main():
        win.show()

    if IMAGE_PATH.exists():
        splash = FadeSplash(image_path=str(IMAGE_PATH), duration_ms=2500, fade_ms=800, on_finish=start_main)
        splash.show()
    else:
        win.show()

    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
