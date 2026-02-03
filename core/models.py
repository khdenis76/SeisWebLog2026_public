# core/models.py
from __future__ import annotations
from pathlib import Path
import sqlite3

from django.db import models
from django.core.exceptions import ValidationError
from django.contrib.auth.models import User
from django.db.models.signals import post_save
from django.dispatch import receiver


def path_exists_or_raise(p: Path):
    """
    Validate that path exists and is a directory.
    """
    if not p.exists() or not p.is_dir():
        raise ValidationError(f"Root path does not exist or is not a directory: {p}")


class Project(models.Model):
    """
    Represents a user-created project stored outside the Django project folder.
    """

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

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name

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
        """
        User can view if:
          - he is the owner
          - or he is in ProjectMember (view or edit)
        """
        if not user.is_authenticated:
            return False
        if user == self.owner:
            return True
        return ProjectMember.objects.filter(project=self, user=user).exists()

    def can_edit(self, user: User) -> bool:
        """
        User can edit if:
          - he is the owner
          - or ProjectMember.can_edit is True
        """
        if not user.is_authenticated:
            return False
        if user == self.owner:
            return True
        return ProjectMember.objects.filter(
            project=self, user=user, can_edit=True
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
    if not db_path.exists():
        conn = sqlite3.connect(str(db_path))
        conn.close()
