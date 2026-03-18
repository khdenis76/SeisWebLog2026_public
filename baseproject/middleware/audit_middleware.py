import time
import logging

logger = logging.getLogger("seisweblog.audit")


class AuditRequestMiddleware:
    """
    Logs one summary line for every request.
    Good for tracing who called what and how long it took.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        start = getattr(request, "_start_time", time.perf_counter())
        response = None

        try:
            response = self.get_response(request)
            return response

        finally:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)

            user = getattr(request, "user", None)
            username = None
            if user and user.is_authenticated:
                username = user.username

            logger.info(
                "HTTP request complete",
                extra={
                    "request_id": getattr(request, "request_id", None),
                    "username": username,
                    "method": request.method,
                    "path": request.path,
                    "status_code": getattr(response, "status_code", None),
                    "duration_ms": duration_ms,
                    "ip_address": _get_client_ip(request),
                },
            )


def _get_client_ip(request):
    xff = request.META.get("HTTP_X_FORWARDED_FOR")
    if xff:
        return xff.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR")