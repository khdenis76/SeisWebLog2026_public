"""
Admin registrations for the Reports app.

Even if you do not plan to use Django admin heavily, registering the core
models is useful for debugging saved report payloads and generated output.
"""

from django.contrib import admin
from .models import GeneratedReport, ReportTemplate


@admin.register(ReportTemplate)
class ReportTemplateAdmin(admin.ModelAdmin):
    """Simple admin for report template records."""
    list_display = ("name", "report_type", "is_active", "created_at")
    list_filter = ("report_type", "is_active")
    search_fields = ("name", "title_template")


@admin.register(GeneratedReport)
class GeneratedReportAdmin(admin.ModelAdmin):
    """Admin view for generated reports."""
    list_display = ("title", "report_type", "status", "start_date", "end_date", "created_at")
    list_filter = ("report_type", "status", "created_at")
    search_fields = ("title", "project_name")
    readonly_fields = ("created_at", "updated_at")
