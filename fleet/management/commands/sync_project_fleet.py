import sqlite3
from datetime import datetime

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from fleet.models import Vessel
from core.models import Project


PROJECT_FLEET_SCHEMA = """
CREATE TABLE IF NOT EXISTS project_fleet (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vessel_name TEXT NOT NULL,
    imo TEXT,
    mmsi TEXT,
    call_sign TEXT,
    vessel_type TEXT,
    owner TEXT,
    is_active INTEGER DEFAULT 1,
    is_retired INTEGER DEFAULT 0,
    notes TEXT,
    source_vessel_id INTEGER,
    created_at TEXT,
    updated_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_project_fleet_source_vessel_id
ON project_fleet(source_vessel_id);
"""


UPSERT_SQL = """
INSERT INTO project_fleet (
    vessel_name, imo, mmsi, call_sign, vessel_type, owner,
    is_active, is_retired, notes, source_vessel_id, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source_vessel_id) DO UPDATE SET
    vessel_name=excluded.vessel_name,
    imo=excluded.imo,
    mmsi=excluded.mmsi,
    call_sign=excluded.call_sign,
    vessel_type=excluded.vessel_type,
    owner=excluded.owner,
    is_active=excluded.is_active,
    is_retired=excluded.is_retired,
    notes=excluded.notes,
    updated_at=excluded.updated_at
"""


class Command(BaseCommand):
    help = "Copy/sync master Vessel list from Django into a project's ProjectDB table project_fleet."

    def add_arguments(self, parser):
        parser.add_argument("--project-id", type=int, required=True)
        parser.add_argument("--only-active", action="store_true", help="Copy only is_active=True vessels")

    def handle(self, *args, **opts):
        project_id = opts["project_id"]
        only_active = opts["only_active"]

        try:
            project = Project.objects.get(pk=project_id)
        except Project.DoesNotExist:
            raise CommandError(f"Project id={project_id} not found")

        db_path = project.project_db_path
        if not db_path:
            raise CommandError("Project.project_db_path is empty")

        qs = Vessel.objects.all()
        if only_active:
            qs = qs.filter(is_active=True)

        now = timezone.now().isoformat(timespec="seconds")

        conn = sqlite3.connect(db_path)
        try:
            cur = conn.cursor()
            cur.executescript(PROJECT_FLEET_SCHEMA)

            rows = 0
            for v in qs.iterator():
                cur.execute(
                    UPSERT_SQL,
                    (
                        v.name,
                        v.imo,
                        v.mmsi,
                        v.call_sign,
                        v.vessel_type,
                        v.owner,
                        1 if v.is_active else 0,
                        1 if v.is_retired else 0,
                        v.notes,
                        v.id,
                        v.created_at.isoformat(timespec="seconds") if v.created_at else now,
                        now,
                    ),
                )
                rows += 1

            conn.commit()
        finally:
            conn.close()

        self.stdout.write(self.style.SUCCESS(f"Synced {rows} vessels to project_fleet for project id={project_id}"))