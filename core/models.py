# core/models.py
from __future__ import annotations
from pathlib import Path
import sqlite3

from django.db import models
from django.core.exceptions import ValidationError
from django.contrib.auth.models import User
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone

from .project_dataclasses import MainSettings, GeometrySettings, NodeQCSettings, GunQCSettings, FolderSettings




def path_exists_or_raise(p: Path):
    """
    Validate that path exists and is a directory.
    """
    if not p.exists() or not p.is_dir():
        raise ValidationError(f"Root path does not exist or is not a directory: {p}")

class ProjectQuerySet(models.QuerySet):
    def alive(self):
        return self.filter(is_deleted=False)

    def deleted(self):
        return self.filter(is_deleted=True)
class Project(models.Model):
    """
    Represents a user-created project stored outside the Django project folder.
    """
    objects = ProjectQuerySet.as_manager()
    name = models.CharField("Name", max_length=200, unique=True)
    root_path = models.CharField("Path", max_length=500)
    folder_name = models.CharField("Project folder name", max_length=200)

    # Free text comments about project
    note = models.TextField("Comments", blank=True, default="")

    owner = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="owned_projects",
        verbose_name="Owner",
    )
    owner_readonly = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name

    def soft_delete(self):
        self.is_deleted = True
        self.deleted_at = timezone.now()
        self.save(update_fields=["is_deleted", "deleted_at"])

    def restore(self):
        self.is_deleted = False
        self.deleted_at = None
        self.save(update_fields=["is_deleted", "deleted_at"])
    # ---------------------- PATH HELPERS ----------------------

    @property
    def abs_path(self) -> Path:
        """
        Absolute path to project root directory: <root_path>/<folder_name>.
        """
        return Path(self.root_path).expanduser().resolve() / self.folder_name

    @property
    def data_dir(self) -> Path:
        """
        Data folder inside project directory.
        """
        return self.abs_path / "data"

    @property
    def db_path(self) -> Path:
        """
        Path to per-project SQLite database.
        """
        return self.data_dir / "project.sqlite3"
    @property
    def hdr_dir(self)->Path:
        """
         Path to folder with headers.
        """
        return self.abs_path / "headers"
    @property
    def export_dir(self) -> Path:
        return self.abs_path / "export"

    @property
    def export_csv(self) -> Path:
        return self.export_dir / "csv"

    @property
    def export_sps1(self) -> Path:
        return self.export_dir / "sps_v1"

    @property
    def export_sps21(self) -> Path:
        return self.export_dir / "sps_v21"

    @property
    def export_shapes(self)->Path:
        return self.export_dir / "shapes"

    @property
    def export_gpkg(self)->Path:
        return self.export_dir / "gpkg"
    @property
    def export_rline_shapes(self)->Path:
        return self.export_shapes / "rec_line_shapes"

    @property
    def export_rpoint_shapes(self) -> Path:
        return self.export_shapes / "rec_point_shapes"

    @property
    def export_sline_shapes(self) -> Path:
        return self.export_shapes / "sou_line_shapes"

    @property
    def export_spoint_shapes(self) -> Path:
        return self.export_shapes / "sou_point_shapes"

    @property
    def export_sm(self) -> Path:
        return self.export_dir / "sm"

    @property
    def logs_dir(self) -> Path:
        return self.abs_path / "logs"

    @property
    def reports_dir(self) -> Path:
        return self.abs_path / "reports"

    @property
    def plots_dir(self) -> Path:
        return self.abs_path / "plots"

    @property
    def tmp_dir(self) -> Path:
        return self.abs_path / "tmp"

    # ---------------------- VALIDATION ----------------------

    def clean(self):
        path_exists_or_raise(Path(self.root_path).expanduser().resolve())

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    # ---------------------- PERMISSIONS ----------------------

    def can_view(self, user: User) -> bool:
        if not user or not user.is_authenticated:
            return False

        # ✅ superuser = full access
        if user.is_superuser:
            return True

        if user == self.owner:
            return True

        return ProjectMember.objects.filter(project=self, user=user).exists()

    def can_edit(self, user: User) -> bool:
        if not user or not user.is_authenticated:
            return False

        # ✅ superuser = full access
        if user.is_superuser:
            return True

        if user == self.owner:
            return not getattr(self, "owner_readonly", False)

        return ProjectMember.objects.filter(
            project=self,
            user=user,
            can_edit=True,
        ).exists()


class UserSettings(models.Model):
    """
    Per-user settings, including currently active project.
    """

    user = models.OneToOneField(
        User,
        on_delete=models.CASCADE,
        related_name="settings",
    )
    active_project = models.ForeignKey(
        Project,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="active_for_users",
    )
    theme_mode = models.CharField(max_length=8, default="dark")
    def __str__(self) -> str:
        return f"Settings for {self.user.username}"


class ProjectMember(models.Model):
    """
    Membership in a project with optional edit permission.
    """

    project = models.ForeignKey(
        Project,
        on_delete=models.CASCADE,
        related_name="memberships",
    )
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="project_memberships",
    )
    can_edit = models.BooleanField(default=False)

    class Meta:
        unique_together = ("project", "user")
        verbose_name = "Project member"
        verbose_name_plural = "Project members"

    def __str__(self) -> str:
        return f"{self.user.username} → {self.project.name} ({'edit' if self.can_edit else 'view'})"
# core/models.py
class SPSRevision(models.Model):
    """
    SPS revision definition.
    All positions in SPS file start from 0.
    """

    rev_name = models.CharField(max_length=255, unique=True)

    record_start = models.IntegerField(default=0)
    record_end = models.IntegerField(default=0)

    line_start = models.IntegerField(default=0)
    line_end = models.IntegerField(default=0)

    point_start = models.IntegerField(default=0)
    point_end = models.IntegerField(default=0)

    point_idx_start = models.IntegerField(default=0)
    point_idx_end = models.IntegerField(default=0)

    point_code_start = models.IntegerField(default=0)
    point_code_end = models.IntegerField(default=0)

    static_start = models.IntegerField(default=0)
    static_end = models.IntegerField(default=0)

    point_depth_start = models.IntegerField(default=0)
    point_depth_end = models.IntegerField(default=0)

    datum_start = models.IntegerField(default=0)
    datum_end = models.IntegerField(default=0)

    uphole_start = models.IntegerField(default=0)
    uphole_end = models.IntegerField(default=0)

    water_depth_start = models.IntegerField(default=0)
    water_depth_end = models.IntegerField(default=0)

    easting_start = models.IntegerField(default=0)
    easting_end = models.IntegerField(default=0)

    northing_start = models.IntegerField(default=0)
    northing_end = models.IntegerField(default=0)

    elevation_start = models.IntegerField(default=0)
    elevation_end = models.IntegerField(default=0)

    jday_start = models.IntegerField(default=0)
    jday_end = models.IntegerField(default=0)

    hour_start = models.IntegerField(default=0)
    hour_end = models.IntegerField(default=0)

    minute_start = models.IntegerField(default=0)
    minute_end = models.IntegerField(default=0)

    second_start = models.IntegerField(default=0)
    second_end = models.IntegerField(default=0)

    msecond_start = models.IntegerField(default=0)
    msecond_end = models.IntegerField(default=0)

    default_format = models.BooleanField(default=False)

    def __str__(self):
        return self.rev_name


# ---------------------- SIGNAL: CREATE FOLDERS & DB ----------------------



@receiver(post_save, sender=Project)
def create_project_folder(sender, instance: Project, created, **kwargs):
    """
    When a new Project is created, ensure that its folder structure and
    empty project SQLite DB exist.
    """

    def init_db(connect) -> None:
        """
        Create tables if not exist and ensure one default row in each table.
        """
        with connect() as conn:
            cur = conn.cursor()

            # -------- project_main --------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS project_main (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    name TEXT NOT NULL,
                    location TEXT NOT NULL,
                    area TEXT NOT NULL,
                    client TEXT NOT NULL,
                    contractor TEXT NOT NULL,
                    project_client_id TEXT NOT NULL,
                    project_contractor_id TEXT NOT NULL,
                    epsg TEXT NOT NULL,
                    line_code TEXT NOT NULL,
                    start_project TEXT NOT NULL,
                    project_duration INTEGER NOT NULL,
                    color_scheme TEXT DEFAULT 'dark'
                );
                """
            )
            cur.execute("SELECT COUNT(*) AS cnt FROM project_main;")
            if cur.fetchone()["cnt"] == 0:
                m = MainSettings()
                cur.execute(
                    """
                    INSERT INTO project_main
                        (id, name, location, area, client, contractor,
                         project_client_id, project_contractor_id,
                         epsg, line_code, start_project, project_duration,color_scheme)
                    VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        m.name, m.location, m.area,
                        m.client, m.contractor,
                        m.project_client_id, m.project_contractor_id,
                        m.epsg, m.line_code,
                        m.start_project, m.project_duration, m.color_scheme
                    ),
                )

            # -------- project_geometry --------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS project_geometry (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    rpi REAL NOT NULL,
                    rli REAL NOT NULL,
                    spi REAL NOT NULL,
                    sli REAL NOT NULL,
                    rl_heading REAL NOT NULL,
                    sl_heading REAL NOT NULL,
                    production_code TEXT NOT NULL,
                    non_production_code TEXT NOT NULL,
                    kill_code TEXT NOT NULL,
                    rl_mask TEXT NOT NULL,
                    sl_mask TEXT NOT NULL,
                    sail_line_mask TEXT NOT NULL
                    
                );
                """
            )
            cur.execute("SELECT COUNT(*) AS cnt FROM project_geometry;")
            if cur.fetchone()["cnt"] == 0:
                g = GeometrySettings()
                cur.execute(
                    """
                    INSERT INTO project_geometry
                        (id, rpi, rli, spi, sli,
                         rl_heading, sl_heading,
                         production_code, non_production_code,kill_code,
                         rl_mask, sl_mask,sail_line_mask)
                    VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        g.rpi, g.rli, g.spi, g.sli,
                        g.rl_heading, g.sl_heading,
                        g.production_code, g.non_production_code,g.kill_code,
                        g.rl_mask, g.sl_mask,g.sail_line_mask
                    ),
                )

            # -------- project_node_qc --------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS "project_node_qc" (
                         "id"	INTEGER CHECK("id" = 1),
                         "max_il_offset"	REAL NOT NULL,
                         "max_xl_offset"	REAL NOT NULL,
                         "max_radial_offset"	REAL NOT NULL,
                         "percent_of_depth"	REAL NOT NULL,
                         "use_offset"	INTEGER NOT NULL,
                         "battery_life"	INTEGER DEFAULT 0,
                         "gnss_diffage_warning"	INTEGER DEFAULT 0,
                         "gnss_diffage_error"	INTEGER DEFAULT 0,
                         "gnss_fixed_quality"	INTEGER DEFAULT 0,
                         PRIMARY KEY("id")
                )
                """
            )
            cur.execute("SELECT COUNT(*) AS cnt FROM project_node_qc;")
            if cur.fetchone()["cnt"] == 0:
                n = NodeQCSettings()
                cur.execute(
                    """
                    INSERT INTO project_node_qc
                        (id, max_il_offset, max_xl_offset, max_radial_offset,
                         percent_of_depth, use_offset,battery_life,gnss_diffage_warning,gnss_diffage_error,gnss_fixed_quality)
                    VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        n.max_il_offset, n.max_xl_offset, n.max_radial_offset,
                        n.percent_of_depth, n.use_offset, n.battery_life, n.gnss_diffage_warning, n.gnss_diffage_error,
                        n.gnss_fixed_quality
                    ),
                )

            # -------- project_gun_qc --------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS project_gun_qc (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    num_of_arrays INTEGER NOT NULL,
                    num_of_strings INTEGER NOT NULL,
                    num_of_guns INTEGER NOT NULL,
                    depth REAL NOT NULL,
                    depth_tolerance REAL NOT NULL,
                    time_warning REAL NOT NULL,
                    time_error REAL NOT NULL,
                    pressure REAL NOT NULL,
                    pressure_drop REAL NOT NULL,
                    volume REAL NOT NULL,
                    max_il_offset REAL NOT NULL,
                    max_xl_offset REAL NOT NULL,
                    max_radial_offset REAL NOT NULL,
                    kill_shots_cons INTEGER,
                    percentage_of_kill INTEGER
                );
                """
            )
            cur.execute("SELECT COUNT(*) AS cnt FROM project_gun_qc;")
            if cur.fetchone()["cnt"] == 0:
                q = GunQCSettings()
                cur.execute(
                    """
                    INSERT INTO project_gun_qc
                        (id, num_of_arrays, num_of_strings, num_of_guns,
                         depth, depth_tolerance,
                         time_warning, time_error,
                         pressure, pressure_drop, volume,
                         max_il_offset, max_xl_offset, max_radial_offset,kill_shots_cons,percentage_of_kill)
                    VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        q.num_of_arrays, q.num_of_strings, q.num_of_guns,
                        q.depth, q.depth_tolerance,
                        q.time_warning, q.time_error,
                        q.pressure, q.pressure_drop, q.volume,
                        q.max_il_offset, q.max_xl_offset, q.max_radial_offset,q.kill_shots_cons,q.percentage_of_kill
                    ),
                )
            cur.execute(
                """
                  CREATE TABLE IF NOT EXISTS project_folders (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    shapes_folder TEXT,
                    image_folder TEXT,
                    local_prj_folder TEXT,
                    bb_folder TEXT,                
                    segy_folder TEXT);
                """)
            cur.execute("SELECT COUNT(*) AS cnt FROM project_folders;")
            if cur.fetchone()["cnt"] == 0:
                f = FolderSettings()
                cur.execute(
                    """
                    INSERT INTO project_folders
                        (id, shapes_folder,image_folder,local_prj_folder,bb_folder,segy_folder)
                    VALUES (1, ?, ?, ?, ?, ?)
                    """,
                    (f.shapes_folder, f.image_folder, f.local_prj_folder, f.bb_folder, f.segy_folder),
                )
            sql = """
                       CREATE TABLE IF NOT EXISTS project_shapes (
                                "id" INTEGER,
                                "FullName" TEXT UNIQUE NOT NULL,
                                "FileName" TEXT,
                                "isFilled" INTEGER DEFAULT 0,
                                "FillColor" TEXT DEFAULT '#000000',
                                "LineColor" TEXT DEFAULT '#000000',
                                "LineWidth" INTEGER DEFAULT 1,
                                "LineStyle" TEXT DEFAULT '',
                                "HatchPattern" TEXT DEFAULT '',
                                "FileCheck" INT DEFAULT 1,
	                            PRIMARY KEY(id,FullName));
                """
            cur.execute(sql)
            conn.commit()
    def run_sql_file(connect, path):
        with open(path, "r", encoding="utf-8") as file:
            sql = file.read()

        with connect() as conn:
            cursor = conn.cursor()
            cursor.executescript(sql)  # для SQLite
        conn.commit()
        conn.close()
        print("SQL file executed.")

    if not created:
        return

    # Project root
    project_dir = instance.abs_path
    project_dir.parent.mkdir(parents=True, exist_ok=True)
    project_dir.mkdir(parents=True, exist_ok=True)

    # data/
    instance.data_dir.mkdir(parents=True, exist_ok=True)

    # export and subfolders
    instance.export_dir.mkdir(parents=True, exist_ok=True)
    instance.export_csv.mkdir(parents=True, exist_ok=True)
    instance.export_sps1.mkdir(parents=True, exist_ok=True)
    instance.export_sps21.mkdir(parents=True, exist_ok=True)
    instance.export_shapes.mkdir(parents=True, exist_ok=True)
    instance.export_sm.mkdir(parents=True, exist_ok=True)

    instance.export_rline_shapes.mkdir(parents=True, exist_ok=True)
    instance.export_rpoint_shapes.mkdir(parents=True, exist_ok=True)
    instance.export_sline_shapes.mkdir(parents=True, exist_ok=True)
    instance.export_spoint_shapes.mkdir(parents=True, exist_ok=True)
    instance.export_gpkg.mkdir(parents=True, exist_ok=True)
    # other folders
    instance.logs_dir.mkdir(parents=True, exist_ok=True)
    instance.reports_dir.mkdir(parents=True, exist_ok=True)
    instance.plots_dir.mkdir(parents=True, exist_ok=True)
    instance.hdr_dir.mkdir(parents=True,exist_ok=True)
    instance.tmp_dir.mkdir(parents=True, exist_ok=True)


    # SQLite DB file
    db_path = instance.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect():
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        return conn

    if not db_path.exists() or db_path.stat().st_size == 0:
        init_db(connect)
        run_sql_file(connect,"core/newproject.sql")



    # Optional: if you also have SQL scripts that must run:
    # pdb.run_sql_file(instance.some_sql_path)
