from django.urls import path
from . import views

urlpatterns = [
    path("", views.source_home, name="source_home"),
    path("upload/",views.source_upload_files,name="source_upload_files"),
    path("delete/sps",views.sps_delete_selected,name="sps_delete_selected"),
    path("qc/progress-map-json/", views.source_qc_progress_map_json, name="source_qc_progress_map_json"),
    path("qc/sunburst-json/",views.source_qc_sunburst_json,name="source_qc_sunburst_json",),
    path("daybyday-production/json/",views.source_daybyday_production_json,name="source_daybyday_production_json",),
    path("sps/table-data/", views.source_sps_table_data, name="source_sps_table_data"),
    path("sp-solution-vs-preplot-json/",views.source_sp_solution_vs_preplot_json,name="source_sp_solution_vs_preplot_json",),
]