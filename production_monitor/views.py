from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.template.loader import render_to_string

from core.models import UserSettings
from production_monitor.utils.project_template_db import ProjectTemplateDB


@login_required
def production_monitor_home(request):
    """
    Main Production Monitor page.

    XLSX/template loading remains inside this app.
    Matrix visualization is rendered here directly.
    """

    user_settings, _ = UserSettings.objects.get_or_create(
        user=request.user
    )

    project = user_settings.active_project

    template_rows_html = ""
    matrix_html = ""

    if project:
        ptdb = ProjectTemplateDB(project.db_path)

        ptdb.ensure_schema()

        # table body
        template_rows_html = ptdb.render_table_body()

        # matrix
        matrix_data = ptdb.visual_offset_status_table_data()

        matrix_html = render_to_string(
            "production_monitor/partials/project_template_status_matrix.html",
            matrix_data,
            request=request,
        )

    context = {
        "project": project,
        "template_rows_html": template_rows_html,
        "matrix_html": matrix_html,
    }

    return render(
        request,
        "production_monitor/production_monitor_home.html",
        context,
    )