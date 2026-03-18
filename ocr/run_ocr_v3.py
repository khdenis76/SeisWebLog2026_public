from __future__ import annotations

import os
import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication, QFileDialog, QMessageBox

# Allow running as: python ocr/run_ocr_v3.py
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR.parent) not in sys.path:
    sys.path.insert(0, str(BASE_DIR.parent))
IMAGE_PATH = BASE_DIR / "images" / "swl_ocr_studio.png"

from ocr.ui.main_window import OCRMainWindow  # noqa: E402
from ocr.ui.splash import FadeSplash


def choose_django_db() -> str:
    return QFileDialog.getOpenFileName(
        None,
        "Choose Django DB",
        "",
        "SQLite DB (*.sqlite *.sqlite3 *.db);;All files (*)",
    )[0]


def find_django_db() -> str:
    # 1. environment variable
    env_db = os.environ.get("SEISWEBLOG_DJANGO_DB", "")
    if env_db and os.path.exists(env_db):
        return env_db

    # 2. try SeisWebLog project root
    db_path = BASE_DIR.parent / "db.sqlite3"
    if db_path.exists():
        return str(db_path)

    # 3. try parent folder
    db_path = BASE_DIR.parent.parent / "db.sqlite3"
    if db_path.exists():
        return str(db_path)

    return ""


def main() -> int:
    app = QApplication(sys.argv)

    # =========================
    # FIND DB (your logic stays)
    # =========================
    django_db = find_django_db()

    if not django_db:
        django_db = choose_django_db()

    if not django_db:
        QMessageBox.warning(None, "Canceled", "No Django DB selected.")
        return 1

    # =========================
    # CREATE MAIN WINDOW
    # =========================
    win = OCRMainWindow(django_db)

    # =========================
    # FUNCTION TO START MAIN UI
    # =========================
    def start_main():
        win.show()

    # =========================
    # SHOW SPLASH FIRST
    # =========================
    splash = FadeSplash(
        image_path=IMAGE_PATH,
        duration_ms=4000,
        fade_ms=1200,
        on_finish=start_main
    )

    splash.show()

    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())