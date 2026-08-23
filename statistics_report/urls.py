from django.urls import path

from . import views

app_name = "statistics_report"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("export/csv/", views.export_csv, name="export_csv"),
    path("export/excel/", views.export_excel, name="export_excel"),
    path("export/pdf/", views.export_pdf, name="export_pdf"),
    path("export/html/", views.export_html, name="export_html"),
]
