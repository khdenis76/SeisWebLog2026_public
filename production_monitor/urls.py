from django.urls import path
from . import views

app_name = "production_monitor"

urlpatterns = [
    path("", views.production_monitor_home, name="production_monitor_home"),


]