# ocr/core/project_loader.py

import sqlite3
from pathlib import Path


def load_projects(django_db_path):
    """
    Load active projects from Django DB and build path to project.sqlite3
    """

    conn = sqlite3.connect(django_db_path)
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT
            id,
            name,
            root_path,
            folder_name,
            is_deleted
        FROM core_project
        ORDER BY name
    """).fetchall()

    conn.close()

    projects = []

    for pid, name, root_path, folder_name, is_deleted in rows:
        if is_deleted:
            continue

        db_path = Path(root_path) / folder_name / "data" / "project.sqlite3"

        projects.append({
            "id": pid,
            "name": name,
            "root_path": root_path,
            "folder_name": folder_name,
            "db_path": str(db_path),
        })

    return projects