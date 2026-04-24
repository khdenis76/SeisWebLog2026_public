import json
from pathlib import Path

from django.http import JsonResponse, FileResponse
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_POST
from django.core.exceptions import PermissionDenied

from core.models import UserSettings
from rov.eol.generator import EOLReportGenerator


# ---------------------------------------------------------
# helpers
# ---------------------------------------------------------
def _as_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _as_int(value, default=0, min_value=None):
    try:
        out = int(value)
    except Exception:
        out = default

    if min_value is not None:
        out = max(min_value, out)
    return out


def _clean_lines(lines):
    if not isinstance(lines, (list, tuple)):
        return []

    out = []
    seen = set()

    for item in lines:
        txt = str(item).strip()
        if not txt:
            continue
        if txt in seen:
            continue
        seen.add(txt)
        out.append(txt)

    return out


def _clean_sections(sections):
    """
    Normalize sections received from modal.

    Rules:
    - keep order
    - remove duplicates
    - if child selected without parent, parent is added automatically
    - only keep known section keys
    """
    if not isinstance(sections, (list, tuple)):
        return []

    allowed = [
        "front_page",
        "table_of_contents",
        "line_summary",
        "project_map",
        "dsr_info_table",

        "deployment",
        "deployment_summary_statistic",
        "deployment_primary_secondary",
        "deployment_preplot",
        "deployment_single_node_map",
        "deployment_water_depth",

        "bbox_qc",
        "bbox_qc_gnss",
        "bbox_qc_motion",
        "deployment_vs_bbox",

        "source",
        "source_summary",

        "recovery",
        "recovery_qc_package",

        "final_comparison",
        "comments",
    ]

    allowed_set = set(allowed)

    child_to_parent = {
        "deployment_summary_statistic": "deployment",
        "deployment_primary_secondary": "deployment",
        "deployment_preplot": "deployment",
        "deployment_single_node_map": "deployment",
        "deployment_water_depth": "deployment",

        "bbox_qc_gnss": "bbox_qc",
        "bbox_qc_motion": "bbox_qc",
        "deployment_vs_bbox": "bbox_qc",

        "source_summary": "source",

        "recovery_qc_package": "recovery",
    }

    raw = []
    seen = set()

    for item in sections:
        key = str(item).strip()
        if not key or key not in allowed_set:
            continue
        if key in seen:
            continue
        seen.add(key)
        raw.append(key)

    # auto-add parents if only child selected
    final_list = list(raw)
    final_seen = set(final_list)

    for key in raw:
        parent = child_to_parent.get(key)
        if parent and parent not in final_seen:
            final_list.append(parent)
            final_seen.add(parent)

    # canonical order
    canonical_order = [
        "front_page",
        "table_of_contents",
        "line_summary",
        "project_map",
        "dsr_info_table",

        "deployment",
        "deployment_summary_statistic",
        "deployment_primary_secondary",
        "deployment_preplot",
        "deployment_single_node_map",
        "deployment_water_depth",

        "bbox_qc",
        "bbox_qc_gnss",
        "bbox_qc_motion",
        "deployment_vs_bbox",

        "source",
        "source_summary",

        "recovery",
        "recovery_qc_package",

        "final_comparison",
        "comments",
    ]

    ordered = [k for k in canonical_order if k in final_seen]
    return ordered


def _normalize_output_mode(value, lines_count=1):
    value = str(value or "auto").strip().lower()

    if value in {"single_pdf", "pdf"}:
        return "single_pdf"

    if value == "zip":
        return "zip"

    # auto
    if lines_count == 1:
        return "single_pdf"
    return "zip"


def _build_generator_options(project, payload):
    lines = _clean_lines(payload.get("lines") or [])
    sections = _clean_sections(payload.get("sections") or [])

    options = {
        "reports_dir": str(project.reports_dir),

        "lines": lines,
        "sections": sections,

        "prepared_by": str(payload.get("prepared_by") or "").strip(),
        "comments_text": str(payload.get("comments_text") or "").strip(),

        "output_mode": _normalize_output_mode(payload.get("output_mode"), len(lines)),
        "page_size": str(payload.get("page_size") or "A4").strip() or "A4",

        "include_tgs_logo": _as_bool(payload.get("include_tgs_logo"), True),
        "include_page_numbers": _as_bool(payload.get("include_page_numbers"), True),
        "auto_orientation": _as_bool(payload.get("auto_orientation"), True),

        "bbox_hours_per_page": _as_int(
            payload.get("bbox_hours_per_page"),
            default=6,
            min_value=1,
        ),
    }

    # optional fields, if later added in modal/backend
    for key in [
        "project_name",
        "client_name",
        "vessel_name",
    ]:
        if key in payload:
            options[key] = str(payload.get(key) or "").strip()

    return options


# ---------------------------------------------------------
# main generate view
# ---------------------------------------------------------
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

    lines = _clean_lines(payload.get("lines") or [])
    if not lines:
        return JsonResponse({"error": "No lines selected"}, status=400)

    sections = _clean_sections(payload.get("sections") or [])
    if not sections:
        return JsonResponse({"error": "No report sections selected"}, status=400)

    bbox_hours_per_page = _as_int(
        payload.get("bbox_hours_per_page"),
        default=6,
        min_value=1,
    )
    if bbox_hours_per_page < 1:
        return JsonResponse(
            {"error": "BBox QC: hours per page must be 1 or greater."},
            status=400,
        )


    options = _build_generator_options(project, payload)
    generator = EOLReportGenerator(
        db_path=project.db_path,
        request_user=request.user,
        options=options,
    )

    output_mode = options["output_mode"]

    try:
        if len(lines) == 1 and output_mode == "single_pdf":
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
        print(e)
        return JsonResponse(
            {"error": f"EOL report generation failed: {str(e)}"},
            status=500
        )