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
