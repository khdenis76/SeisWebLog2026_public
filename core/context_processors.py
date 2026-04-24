from __future__ import annotations

from django.conf import settings
from .models import UserSettings
from .projectdb import ProjectDB
from .version_checker import check_new_version

def theme_context(request):
    mode = "dark"
    user = getattr(request, "user", None)
    if user and user.is_authenticated:
        try:
            mode = user.settings.theme_mode or "dark"
        except Exception:
            pass
    return {"theme_mode": mode}
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
