"""
SeisWebLog Reports - views.py

This module handles:
- Reports home page
- Report generation form
- Report preview (interactive)
- Report detail (saved report)
- PDF export
- Report delete

Follows SeisWebLog standard:
- Uses UserSettings.active_project
- Uses ProjectDB
- Uses @log_action decorator
- Uses Bootstrap + templates for UI
"""

from pathlib import Path

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import HttpResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages

# --- SeisWebLog core imports (confirmed from your project) ---
from core.models import UserSettings
from core.projectdb import ProjectDB
from utils.decorators import log_action

# --- Local app imports ---
from .forms import ReportGenerateForm
from .models import GeneratedReport, ReportTemplate
from .services.report_builder import SurveyReportBuilder
from .services.pdf_export import PDFExporter


# ============================================================
# Helper: Active Project Resolver (same pattern as your apps)
# ============================================================

def _get_active_project(request):
    """
    Resolve active project from UserSettings.

    Returns:
        project (object) OR redirect response
    """
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project

    if not project:
        messages.warning(request, "No active project selected.")
        return None, redirect("projects")

    # Validate DB path exists
    if not project.db_path or not Path(project.db_path).exists():
        user_settings.active_project = None
        user_settings.save(update_fields=["active_project"])
        messages.warning(request, "Active project not found. Please select again.")
        return None, redirect("projects")

    # Permission check
    if not project.can_view(request.user):
        raise PermissionDenied("You are not a member of this project.")

    return project, None


# ============================================================
# Reports Home
# ============================================================

@login_required
@log_action("show_reports_page", object_type="REP")
def report_home(request):
    """
    Main Reports page.

    Shows:
    - Generate report button
    - List of generated reports
    """
    project, redirect_resp = _get_active_project(request)
    if redirect_resp:
        return redirect_resp

    reports = GeneratedReport.objects.all().order_by("-created_at")[:100]

    return render(
        request,
        "reports/report_home.html",
        {
            "project": project,
            "reports": reports,
        },
    )


# ============================================================
# Generate Report
# ============================================================

@login_required
@log_action("generate_report", object_type="REP")
def report_generate(request):
    """
    Report generation form.

    GET:
        Show form with default values

    POST:
        Validate input
        Build report
        Save report
        Redirect to preview
    """
    project, redirect_resp = _get_active_project(request)
    if redirect_resp:
        return redirect_resp

    if request.method == "POST":
        form = ReportGenerateForm(request.POST)

        if form.is_valid():
            start_date = form.cleaned_data["start_date"]
            end_date = form.cleaned_data["end_date"]
            report_type = form.cleaned_data["report_type"]
            title = form.cleaned_data.get("title") or f"{report_type.title()} Report"

            # Initialize ProjectDB
            pdb = ProjectDB(project.db_path)

            # Build report using service layer
            builder = SurveyReportBuilder(pdb=pdb, project=project)
            payload = builder.build(
                start_date=start_date,
                end_date=end_date,
                report_type=report_type,
            )

            # Save report
            report = GeneratedReport.objects.create(
                title=title,
                report_type=report_type,
                start_date=start_date,
                end_date=end_date,
                project_name=str(project),
                created_by=request.user,
                json_payload=payload,
            )

            messages.success(request, "Report generated successfully.")

            return redirect("reports:preview", pk=report.pk)

    else:
        form = ReportGenerateForm()

    return render(
        request,
        "reports/report_form.html",
        {
            "form": form,
            "project": project,
        },
    )


# ============================================================
# Preview Report (INTERACTIVE)
# ============================================================

@login_required
@log_action("preview_report", object_type="REP")
def report_preview(request, pk):
    """
    Interactive report preview.

    Displays:
    - Bokeh maps
    - Plotly charts
    - Summary sections
    """
    project, redirect_resp = _get_active_project(request)
    if redirect_resp:
        return redirect_resp

    report = get_object_or_404(GeneratedReport, pk=pk)

    payload = report.json_payload or {}

    return render(
        request,
        "reports/report_preview.html",
        {
            "project": project,
            "report": report,
            "payload": payload,
        },
    )


# ============================================================
# Report Detail (same as preview but for history)
# ============================================================

@login_required
@log_action("view_report_detail", object_type="REP")
def report_detail(request, pk):
    """
    View saved report.

    Can be identical to preview or simplified version.
    """
    return report_preview(request, pk)


# ============================================================
# PDF Export
# ============================================================

@login_required
@log_action("download_report_pdf", object_type="REP")
def report_pdf(request, pk):
    """
    Generate and download PDF report.

    Uses:
    - HTML template
    - Static chart images
    """
    project, redirect_resp = _get_active_project(request)
    if redirect_resp:
        return redirect_resp

    report = get_object_or_404(GeneratedReport, pk=pk)

    exporter = PDFExporter()

    pdf_bytes = exporter.render_report_to_pdf(
        template_name="reports/report_pdf.html",
        context={
            "report": report,
            "payload": report.json_payload,
            "project": project,
        },
    )

    response = HttpResponse(pdf_bytes, content_type="application/pdf")
    response["Content-Disposition"] = f'attachment; filename="report_{report.pk}.pdf"'

    return response


# ============================================================
# Delete Report
# ============================================================

@login_required
@log_action("delete_report", object_type="REP")
def report_delete(request, pk):
    """
    Delete generated report.
    """
    project, redirect_resp = _get_active_project(request)
    if redirect_resp:
        return redirect_resp

    report = get_object_or_404(GeneratedReport, pk=pk)

    report.delete()

    messages.success(request, "Report deleted.")

    return redirect("reports:home")