# baseproject/models.py
from django.db import models
from django.contrib.auth.models import User

from core.models import Project, SPSRevision


class BaseProjectFile(models.Model):
    """
    Uploaded base project files stored in Django DB.
    For SPS files you can optionally specify SPSRevision.
    """

    TYPE_SOURCE_SPS = "SRC_SPS"
    TYPE_RECEIVER_SPS = "REC_SPS"
    TYPE_HEADER_SPS = "HDR_SPS"


    FILE_TYPE_CHOICES = [
        (TYPE_SOURCE_SPS, "Source SPS"),
        (TYPE_RECEIVER_SPS, "Receiver SPS"),
        (TYPE_HEADER_SPS, "Header SPS"),
    ]

    project = models.ForeignKey(
        Project,
        on_delete=models.CASCADE,
        related_name="base_files",
    )

    file_type = models.CharField(
        max_length=16,
        choices=FILE_TYPE_CHOICES,
    )

    # SPS revision definition (only for SPS files; optional)
    sps_revision = models.ForeignKey(
        SPSRevision,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="base_files",
    )

    file_name = models.CharField(max_length=255)
    file_size = models.BigIntegerField()
    uploaded_at = models.DateTimeField(auto_now_add=True)
    uploaded_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="base_files_uploaded",
    )

    class Meta:
        ordering = ["project", "file_type", "file_name", "-uploaded_at"]

    def __str__(self):
        return f"{self.project.name}: {self.file_type} → {self.file_name}"
class AuditLog(models.Model):
    LEVEL_CHOICES = [
        ("INFO", "Info"),
        ("SUCCESS", "Success"),
        ("WARNING", "Warning"),
        ("ERROR", "Error"),
        ("CRITICAL", "Critical"),
    ]

    project_name = models.CharField(max_length=255, null=True, blank=True)
    project_id_ref = models.IntegerField(null=True, blank=True)

    user = models.ForeignKey(
        User,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="audit_logs"
    )

    username_text = models.CharField(max_length=150, null=True, blank=True)

    action = models.CharField(max_length=255)          # e.g. "upload_sps"
    function_name = models.CharField(max_length=255)   # e.g. "rov_upload_black_box"
    module_name = models.CharField(max_length=255, null=True, blank=True)

    level = models.CharField(max_length=10, choices=LEVEL_CHOICES, default="INFO")
    message = models.TextField()

    request_id = models.CharField(max_length=64, null=True, blank=True)

    ip_address = models.GenericIPAddressField(null=True, blank=True)
    method = models.CharField(max_length=10, null=True, blank=True)
    path = models.TextField(null=True, blank=True)

    status_code = models.IntegerField(null=True, blank=True)
    duration_ms = models.FloatField(null=True, blank=True)

    object_type = models.CharField(max_length=100, null=True, blank=True)  # "DSR", "SPS", "SHOT_TABLE"
    object_id = models.CharField(max_length=100, null=True, blank=True)

    details = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "audit_log"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["created_at"]),
            models.Index(fields=["level"]),
            models.Index(fields=["action"]),
            models.Index(fields=["project_name"]),
            models.Index(fields=["username_text"]),
            models.Index(fields=["request_id"]),
        ]

    def __str__(self):
        return f"{self.created_at} [{self.level}] {self.action}: {self.message[:80]}"
