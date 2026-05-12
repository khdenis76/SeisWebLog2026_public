# production_monitor/apps.py

from django.apps import AppConfig


class ProductionMonitorConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "production_monitor"
    verbose_name = "Production Monitor"