import logging
from typing import Any

from baseproject.models import AuditLog

tech_logger = logging.getLogger("seisweblog.tech")
audit_logger = logging.getLogger("seisweblog.audit")


def get_client_ip(request) -> str | None:
    if not request:
        return None
    xff = request.META.get("HTTP_X_FORWARDED_FOR")
    if xff:
        return xff.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR")


def get_project_info(request=None, project=None):
    """
    Returns (project_name, project_id)
    """
    if project is not None:
        name = getattr(project, "name", str(project))
        pid = getattr(project, "id", None)
        return name, pid

    if request and hasattr(request, "user") and request.user.is_authenticated:
        try:
            user_settings = request.user.usersettings
            active_project = user_settings.active_project
            if active_project:
                return active_project.name, active_project.id
        except Exception:
            pass

    return None, None


def audit_event(
    *,
    request=None,
    action: str,
    function_name: str = "",
    message: str = "",
    level: str = "INFO",
    project=None,
    object_type: str | None = None,
    object_id: str | None = None,
    status_code: int | None = None,
    duration_ms: float | None = None,
    details: dict[str, Any] | None = None,
    write_to_db: bool = True,
    write_to_file: bool = True,
):
    user = None
    username = None
    method = None
    path = None
    request_id = None
    ip_address = None

    if request is not None:
        method = getattr(request, "method", None)
        path = getattr(request, "path", None)
        request_id = getattr(request, "request_id", None)
        ip_address = get_client_ip(request)

        if hasattr(request, "user") and request.user.is_authenticated:
            user = request.user
            username = request.user.username

    project_name, project_id_ref = get_project_info(request=request, project=project)

    payload = {
        "request_id": request_id,
        "username": username,
        "method": method,
        "path": path,
        "ip_address": ip_address,
        "project_name": project_name,
        "project_id_ref": project_id_ref,
        "action": action,
        "function_name": function_name,
        "level": level,
        "object_type": object_type,
        "object_id": object_id,
        "status_code": status_code,
        "duration_ms": duration_ms,
        "details": details or {},
    }

    if write_to_file:
        log_message = f"{action} | {message}"
        if level in ("ERROR", "CRITICAL"):
            audit_logger.error(log_message, extra=payload)
        elif level == "WARNING":
            audit_logger.warning(log_message, extra=payload)
        else:
            audit_logger.info(log_message, extra=payload)

    if write_to_db:
        try:
            AuditLog.objects.create(
                project_name=project_name,
                project_id_ref=project_id_ref,
                user=user,
                username_text=username,
                action=action,
                function_name=function_name,
                module_name="",
                level=level,
                message=message,
                request_id=request_id,
                ip_address=ip_address,
                method=method,
                path=path,
                status_code=status_code,
                duration_ms=duration_ms,
                object_type=object_type,
                object_id=object_id,
                details=details or {},
            )
        except Exception as exc:
            tech_logger.exception("Failed to write AuditLog row: %s", exc)