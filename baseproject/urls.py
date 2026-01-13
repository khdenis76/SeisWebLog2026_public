# baseproject/urls.py
from django.urls import path
from .views import base_project_settings_view
from .views import upload_preplots

urlpatterns = [
    path("", base_project_settings_view, name="base_project_settings"),
    path("upload/preplots/", upload_preplots, name="upload_preplots"),
]
