from pathlib import Path

from django.contrib import messages

from core.models import UserSettings


def get_valid_active_project(request, warn=True):
    """
    Returns the user's active project if it exists and its db file is present.
    If invalid, clears active_project and returns None.
    """
    if not request.user.is_authenticated:
        return None

    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project

    if not project:
        if warn:
            messages.warning(request, "Please select a project first.")
        return None

    db_path = getattr(project, "db_path", None)

    if not db_path or not Path(db_path).exists():
        user_settings.active_project = None
        user_settings.save(update_fields=["active_project"])

        if warn:
            messages.warning(
                request,
                "Your active project was not found. Please select a project again."
            )
        return None

    return project