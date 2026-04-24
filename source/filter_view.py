from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.template.loader import render_to_string

from core.models import UserSettings
from source.source_data import SourceData
from utils.decorators import log_action


@login_required
@log_action("filter_shot_table", object_type="ST")
def shot_line_summary_table(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return redirect("projects")

    if not project.can_view(request.user):
        raise PermissionDenied("You are not a member of this project.")

    sd = SourceData(project.db_path)

    filters = {
        "nav_line_code": request.GET.get("nav_line_code", "").strip(),
        "seq": request.GET.get("seq", "").strip(),
        "attempt": request.GET.get("attempt", "").strip(),
        "vessel_name": request.GET.get("vessel_name", "").strip(),
        "purpose": request.GET.get("purpose", "").strip(),
        "is_in_sl": request.GET.get("is_in_sl", "").strip(),
        "qc_allmatch": request.GET.get("qc_allmatch", "").strip(),
        "diff_status": request.GET.get("diff_status", "").strip(),
        "shotcount_min": request.GET.get("shotcount_min", "").strip(),
        "shotcount_max": request.GET.get("shotcount_max", "").strip(),
    }

    rows = sd.list_v_shot_linesummary(filters=filters)

    # tbody html
    tbody_html = render_to_string(
        "source/partials/_shot_line_summary_tbody.html",
        {"rows": rows},
        request=request,
    )

    is_ajax = request.headers.get("x-requested-with") == "XMLHttpRequest"
    if is_ajax:
        return JsonResponse({
            "ok": True,
            "tbody_html": tbody_html,
            "count": len(rows),
        })

    return render(request, "source/shot_line_summary_table.html", {
        "shot_line_summary": tbody_html,
        "rows_count": len(rows),
        "filters": filters,
    })

@login_required
@log_action("filter_shot_table", object_type="ST")
def shot_line_summary_filter(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project

    if not project:
        return JsonResponse({"ok": False, "error": "No active project"})

    if not project.can_view(request.user):
        raise PermissionDenied("You are not a member of this project.")

    sd = SourceData(project.db_path)

    filters = {
        "nav_line_code": request.GET.get("nav_line_code"),
        "seq": request.GET.get("seq"),
        "attempt": request.GET.get("attempt"),
        "vessel_name": request.GET.get("vessel_name"),
        "purpose": request.GET.get("purpose"),
        "is_in_sl": request.GET.get("is_in_sl"),
        "qc_allmatch": request.GET.get("qc_allmatch"),
        "diff_status": request.GET.get("diff_status"),
        "shotcount_min": request.GET.get("shotcount_min"),
        "shotcount_max": request.GET.get("shotcount_max"),
    }

    rows = sd.list_v_shot_linesummary(filters)

    html = render_to_string(
        "source/partials/_shot_line_summary_tbody.html",
        {"rows": rows},
        request=request
    )

    return JsonResponse({
        "ok": True,
        "tbody_html": html,
        "count": len(rows)
    })