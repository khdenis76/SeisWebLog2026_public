import logging
import traceback

from utils.audit import audit_event

tech_logger = logging.getLogger("seisweblog.tech")


class ExceptionAuditMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        try:
            return self.get_response(request)
        except Exception as exc:
            audit_event(
                request=request,
                action="unhandled_exception",
                function_name="middleware",
                message=str(exc),
                level="CRITICAL",
                status_code=500,
                details={
                    "exception_class": exc.__class__.__name__,
                    "traceback": traceback.format_exc(),
                },
            )
            tech_logger.exception("Unhandled exception")
            raise