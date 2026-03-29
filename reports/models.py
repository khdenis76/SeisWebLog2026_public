"""
Data models for the SeisWebLog Reports app.

Design notes
------------
1. The reports app stores only report metadata and snapshots.
2. Operational data remains in the active project DB selected in SeisWebLog.
3. Saving payload snapshots is important because project DB content can change
   later while old weekly/monthly/final reports should remain reproducible.
"""

from django.conf import settings
from django.db import models


class ReportTemplate(models.Model):
    """Optional user-defined template record."""
    REPORT_TYPE_CHOICES = [
        ("weekly", "Weekly"),
        ("monthly", "Monthly"),
        ("survey", "Survey Wide"),
    ]

    name = models.CharField(max_length=150, unique=True)
    report_type = models.CharField(max_length=20, choices=REPORT_TYPE_CHOICES)
    title_template = models.CharField(max_length=255, blank=True, default="")
    include_summary = models.BooleanField(default=True)
    include_activity = models.BooleanField(default=True)
    include_qc = models.BooleanField(default=True)
    include_maps = models.BooleanField(default=True)
    include_fleet = models.BooleanField(default=True)
    include_narrative = models.BooleanField(default=True)
    logo_left = models.ImageField(upload_to="report_logos/", blank=True, null=True)
    logo_right = models.ImageField(upload_to="report_logos/", blank=True, null=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["name"]

    def __str__(self):
        return self.name


class GeneratedReport(models.Model):
    """Snapshot of a generated report."""
    REPORT_TYPE_CHOICES = [
        ("weekly", "Weekly"),
        ("monthly", "Monthly"),
        ("survey", "Survey Wide"),
    ]

    STATUS_CHOICES = [
        ("draft", "Draft"),
        ("final", "Final"),
    ]

    title = models.CharField(max_length=255)
    report_type = models.CharField(max_length=20, choices=REPORT_TYPE_CHOICES)
    start_date = models.DateField()
    end_date = models.DateField()
    project_name = models.CharField(max_length=255, blank=True, default="")
    template = models.ForeignKey(
        ReportTemplate,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="generated_reports",
    )
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="seisweblog_generated_reports",
    )
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="draft")
    json_payload = models.JSONField(default=dict, blank=True)
    html_snapshot = models.TextField(blank=True, default="")
    pdf_file = models.FileField(upload_to="reports/pdf/", blank=True, null=True)
    notes = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return self.title
