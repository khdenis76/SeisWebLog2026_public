from django.urls import path
from . import views

app_name = "noar"

urlpatterns = [
    path("", views.noar_home, name="noar_home"),
    path("api/dashboard/", views.noar_dashboard_api, name="noar_dashboard_api"),
    path("load-sps/", views.noar_load_sps, name="noar_load_sps"),
]