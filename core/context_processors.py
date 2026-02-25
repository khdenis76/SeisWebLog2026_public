from __future__ import annotations

from django.conf import settings
from .models import UserSettings
from .projectdb import ProjectDB
from .version_checker import check_new_version

def theme_context(request):
    """
    Adds:
      theme_mode: "dark" | "light"
    to every template.
    """
    theme_mode = "dark"

    if request.user.is_authenticated:
        settings, _ = UserSettings.objects.get_or_create(user=request.user)

        # If active project exists, read from project SQLite
        if settings.active_project:
            try:
                pdb = ProjectDB(settings.active_project.db_path)
                main = pdb.get_main()
                # adapt this mapping to your DB values
                cs = (main.color_scheme or "").strip().lower()

                if cs in ("light", "white", "day"):
                    theme_mode = "light"
                else:
                    theme_mode = "dark"
            except Exception:
                theme_mode = "dark"

    return {"theme_mode": theme_mode}
def app_version(request):
    return {"APP_VERSION": getattr(settings, "APP_VERSION", "dev")}
def version_info(request):
    info = check_new_version()
    return {
        "VERSION_OK": info["ok"],
        "NEW_VERSION_AVAILABLE": info["new_available"],
        "LOCAL_VERSION": info["local"],
        "REMOTE_VERSION": info["remote"],
        "VERSION_DOWNLOAD_URL": info["download_url"],
    }
