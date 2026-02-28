from functools import wraps
from django.core.exceptions import PermissionDenied
from django.shortcuts import get_object_or_404

from .models import Project


def project_view_required(project_kw="project_id", *, allow_deleted=False):
    """
    Expects URL kwarg like project_id (or set project_kw).
    Loads Project and checks Project.can_view(user).
    Adds request.project.
    """
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            pk = kwargs.get(project_kw)
            project = get_object_or_404(Project, pk=pk)

            if not allow_deleted and getattr(project, "is_deleted", False):
                raise PermissionDenied("Project is deleted.")

            if not project.can_view(request.user):
                raise PermissionDenied("No access to this project.")

            request.project = project
            return view_func(request, *args, **kwargs)
        return _wrapped
    return decorator


def project_edit_required(project_kw="project_id", *, allow_deleted=False):
    """
    Same, but checks Project.can_edit(user).
    """
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            pk = kwargs.get(project_kw)
            project = get_object_or_404(Project, pk=pk)

            if not allow_deleted and getattr(project, "is_deleted", False):
                raise PermissionDenied("Project is deleted.")

            if not project.can_edit(request.user):
                raise PermissionDenied("Edit not allowed for this project.")

            request.project = project
            return view_func(request, *args, **kwargs)
        return _wrapped
    return decorator