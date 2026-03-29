"""
URL configuration for the reports app.
"""
from django.urls import path

from . import views

app_name = "reports"

urlpatterns = [
    path("", views.report_home, name="home"),
    path("generate/", views.report_generate, name="generate"),
    path("preview/", views.report_preview, name="preview"),
    path("<int:pk>/", views.report_detail, name="detail"),
    path("<int:pk>/pdf/", views.report_pdf, name="pdf"),
    path("<int:pk>/delete/", views.report_delete, name="delete"),
]
