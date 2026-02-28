from django.core.exceptions import PermissionDenied
from django.utils.deprecation import MiddlewareMixin

from .models import UserSettings


class ActiveProjectMiddleware(MiddlewareMixin):
    """
    Adds:
      request.active_project (or None)
      request.active_project_can_edit (bool)
    Optionally blocks if active project is not viewable.
    """
    def process_request(self, request):
        request.active_project = None
        request.active_project_can_edit = False

        u = getattr(request, "user", None)
        if not u or not u.is_authenticated:
            return None

        try:
            settings = u.settings  # related_name="settings" :contentReference[oaicite:4]{index=4}
        except Exception:
            return None

        project = getattr(settings, "active_project", None)
        if not project:
            return None

        # if soft delete exists and project deleted -> ignore
        if getattr(project, "is_deleted", False):
            request.active_project = None
            request.active_project_can_edit = False
            return None

        # Must be viewable, otherwise treat as forbidden (or silently clear)
        if not project.can_view(u):  # :contentReference[oaicite:5]{index=5}
            raise PermissionDenied("Active project is not accessible.")

        request.active_project = project
        request.active_project_can_edit = project.can_edit(u)  # :contentReference[oaicite:6]{index=6}
        return None