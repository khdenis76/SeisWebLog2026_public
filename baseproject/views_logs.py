import csv

from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db.models import Q
from django.http import HttpResponse
from django.shortcuts import render

from baseproject.models import AuditLog
from urllib.parse import urlencode


@login_required
def log_viewer(request):
    qs = AuditLog.objects.all().order_by("-created_at")

    # filters
    project = (request.GET.get("project") or "").strip()
    level = (request.GET.get("level") or "").strip()
    action = (request.GET.get("action") or "").strip()
    username = (request.GET.get("username") or "").strip()
    q = (request.GET.get("q") or "").strip()
    date_from = (request.GET.get("date_from") or "").strip()
    date_to = (request.GET.get("date_to") or "").strip()

    if project:
        qs = qs.filter(project_name__icontains=project)

    if level:
        qs = qs.filter(level=level)

    if action:
        qs = qs.filter(action__icontains=action)

    if username:
        qs = qs.filter(username_text__icontains=username)

    if date_from:
        qs = qs.filter(created_at__date__gte=date_from)

    if date_to:
        qs = qs.filter(created_at__date__lte=date_to)

    if q:
        qs = qs.filter(
            Q(message__icontains=q) |
            Q(function_name__icontains=q) |
            Q(action__icontains=q) |
            Q(request_id__icontains=q) |
            Q(object_type__icontains=q) |
            Q(object_id__icontains=q)
        )

    paginator = Paginator(qs, 50)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    context = {
        "page_obj": page_obj,
        "project": project,
        "level": level,
        "action": action,
        "username": username,
        "q": q,
        "date_from": date_from,
        "date_to": date_to,
        "levels": ["INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"],
    }
    params = request.GET.copy()
    if "page" in params:
        params.pop("page")

    context["querystring"] = urlencode(params)
    return render(request, "baseproject/log_viewer.html", context)

def _filtered_logs(request):
    qs = AuditLog.objects.all().order_by("-created_at")

    project = (request.GET.get("project") or "").strip()
    level = (request.GET.get("level") or "").strip()
    action = (request.GET.get("action") or "").strip()
    username = (request.GET.get("username") or "").strip()
    q = (request.GET.get("q") or "").strip()
    date_from = (request.GET.get("date_from") or "").strip()
    date_to = (request.GET.get("date_to") or "").strip()

    if project:
        qs = qs.filter(project_name__icontains=project)
    if level:
        qs = qs.filter(level=level)
    if action:
        qs = qs.filter(action__icontains=action)
    if username:
        qs = qs.filter(username_text__icontains=username)
    if date_from:
        qs = qs.filter(created_at__date__gte=date_from)
    if date_to:
        qs = qs.filter(created_at__date__lte=date_to)
    if q:
        qs = qs.filter(
            Q(message__icontains=q) |
            Q(function_name__icontains=q) |
            Q(action__icontains=q) |
            Q(request_id__icontains=q) |
            Q(object_type__icontains=q) |
            Q(object_id__icontains=q)
        )
    return qs


@login_required
def export_logs_csv(request):
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="seisweblog_logs.csv"'

    writer = csv.writer(response)
    writer.writerow([
        "time", "level", "project", "user", "action", "function",
        "object_type", "object_id", "status_code", "method", "path", "message"
    ])

    for log in _filtered_logs(request)[:10000]:
        writer.writerow([
            log.created_at,
            log.level,
            log.project_name,
            log.username_text,
            log.action,
            log.function_name,
            log.object_type,
            log.object_id,
            log.status_code,
            log.method,
            log.path,
            log.message,
        ])

    return response


@login_required
def export_logs_txt(request):
    response = HttpResponse(content_type="text/plain")
    response["Content-Disposition"] = 'attachment; filename="seisweblog_logs.txt"'

    lines = []
    for log in _filtered_logs(request)[:10000]:
        lines.append(
            f"{log.created_at} | {log.level} | {log.project_name or '-'} | "
            f"{log.username_text or '-'} | {log.action or '-'} | "
            f"{log.function_name or '-'} | {log.message}"
        )

    response.write("\n".join(lines))
    return response