import time
import functools
import logging

from utils.audit import audit_event

tech_logger = logging.getLogger("seisweblog.tech")


def log_action(action_name: str, object_type: str | None = None):
    """
    Decorator for view or service function logging.
    Assumes request may be in kwargs or first arg for views.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()

            request = kwargs.get("request", None)
            if request is None and args:
                possible_request = args[0]
                if hasattr(possible_request, "META") and hasattr(possible_request, "method"):
                    request = possible_request

            try:
                result = func(*args, **kwargs)
                duration_ms = round((time.perf_counter() - start) * 1000, 2)

                audit_event(
                    request=request,
                    action=action_name,
                    function_name=func.__name__,
                    message=f"{func.__name__} completed successfully",
                    level="SUCCESS",
                    object_type=object_type,
                    duration_ms=duration_ms,
                )
                return result

            except Exception as exc:
                duration_ms = round((time.perf_counter() - start) * 1000, 2)

                audit_event(
                    request=request,
                    action=action_name,
                    function_name=func.__name__,
                    message=str(exc),
                    level="ERROR",
                    object_type=object_type,
                    duration_ms=duration_ms,
                    details={"exception_class": exc.__class__.__name__},
                )

                tech_logger.exception("Error in %s", func.__name__)
                raise

        return wrapper
    return decorator