import json
from pathlib import Path

from django.http import JsonResponse, FileResponse
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_POST
from django.core.exceptions import PermissionDenied

from core.models import UserSettings


@login_required
@require_POST
def eol_generate_reports(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project

    if not project:
        return JsonResponse({"error": "No active project"}, status=400)

    if hasattr(project, "can_view") and not project.can_view(request.user):
        raise PermissionDenied("You are not a member of this project.")

    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"error": "Invalid JSON payload"}, status=400)

    lines = payload.get("lines") or []
    sections = payload.get("sections") or []
    output_mode = payload.get("output_mode") or "auto"

    if not lines:
        return JsonResponse({"error": "No lines selected"}, status=400)

    if not sections:
        return JsonResponse({"error": "No report sections selected"}, status=400)

    from .generator import EOLReportGenerator
    options = dict(payload)
    options["reports_dir"] = str(project.reports_dir)
    generator = EOLReportGenerator(
        db_path=project.db_path,
        request_user=request.user,
        options=options,
    )

    try:
        if len(lines) == 1 and output_mode != "zip":
            pdf_path = generator.build_line_report(line=lines[0])
            return FileResponse(
                open(pdf_path, "rb"),
                as_attachment=True,
                filename=Path(pdf_path).name,
                content_type="application/pdf",
            )

        zip_path = generator.build_zip_for_lines(lines)
        return FileResponse(
            open(zip_path, "rb"),
            as_attachment=True,
            filename=Path(zip_path).name,
            content_type="application/zip",
        )

    except Exception as e:
        return JsonResponse(
            {"error": f"EOL report generation failed: {str(e)}"},
            status=500
        )