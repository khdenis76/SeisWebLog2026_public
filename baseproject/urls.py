# baseproject/urls.py
from django.urls import path

from .views import *
urlpatterns = [
    path("", base_project_settings_view, name="base_project_settings"),
    path("upload/preplots/", upload_preplots, name="upload_preplots"),
    path("upload/source-sps/", upload_source_sps, name="upload_source_sps"),
    path("upload/receiver-sps/", upload_receiver_sps, name="upload_receiver_sps"),
    path("upload/header-sps/", upload_header_sps, name="upload_header_sps"),
    path("upload/delete/rl",delete_selected_receiver_lines,name="delete_selected_receiver_lines"),
    path("upload/delete/sl",delete_selected_source_lines,name="delete_selected_source_lines"),
    path("upload/delete/hdr",upload_header_sps,name="upload_header_sps"),
    path("upload/shapesearch/",shape_search,name="shape_search"),
    path("add/shapes/",add_shape_to_db,name="add_shape_to_db"),
    path("shapes/update/",project_shapes_update, name="project_shapes_update"),
    path("shapes/delete/", project_shapes_delete, name="project_shapes_delete"),
    path("shapes/searchinfolder/", update_shape_folder_view, name="update_shape_folder_view"),
    path("export/soleol/",export_sol_eol_to_csv,name="export_sol_eol_to_csv"),
    path('export/csv',export_splited_csv,name="export_splited_csv"),
    path('export/solidcsv',export_to_csv,name="export_to_csv"),
    path('export/gpkg',export_gpkg,name="export_gpkg"),
    path('export/shapes',export_to_shapes,name="export_to_shapes"),
    path('export/sps1',export_to_sps,name="export_to_sps"),
    path ('detect/headers',csv_headers,name="csv_headers"),
    path('upload/csv',upload_csv_layer_ajax,name="upload_csv_layer_ajax")
]
