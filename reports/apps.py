"""
Django app configuration for the SeisWebLog interactive reporting module.
"""

from django.apps import AppConfig


class ReportsConfig(AppConfig):
    """
    Register the app under the simple name "reports".

    The app is intentionally generic because the actual project integration
    happens at runtime by using the currently active SeisWebLog project DB.
    """
    default_auto_field = "django.db.models.BigAutoField"
    name = "reports"
    verbose_name = "SeisWebLog Reports"
