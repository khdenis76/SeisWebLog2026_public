# baseproject/urls.py
from django.urls import path

from .views import *
urlpatterns = [
    path("", base_project_settings_view, name="base_project_settings"),
    path("upload/source-sps/", upload_source_sps, name="upload_source_sps"),
    path("upload/receiver-sps/", upload_receiver_sps, name="upload_receiver_sps"),
    path("upload/header-sps/", upload_header_sps, name="upload_header_sps"),
    path("upload/delete/rl",delete_selected_receiver_lines,name="delete_selected_receiver_lines"),
    path("upload/delete/sl",delete_selected_source_lines,name="delete_selected_source_lines"),
    path("upload/delete/hdr",upload_header_sps,name="upload_header_sps"),
    #path("upload/shapesearch/",shape_search,name="shape_search"),
    path("add/shapes/",add_shape_to_db,name="add_shape_to_db"),
    path("shapes/update/",project_shapes_update, name="project_shapes_update"),
    path("shapes/delete/", project_shapes_delete, name="project_shapes_delete"),
    path("layers/update/", project_layers_update, name="project_layers_update"),

    path("shapes/searchinfolder/", update_shape_folder_view, name="update_shape_folder_view"),
    path("export/soleol/",export_sol_eol_to_csv,name="export_sol_eol_to_csv"),
    path('export/csv',export_splited_csv,name="export_splited_csv"),
    path('export/solidcsv',export_to_csv,name="export_to_csv"),
    path('export/gpkg',export_gpkg,name="export_gpkg"),
    path('export/shapes',export_to_shapes,name="export_to_shapes"),
    path('export/sps1',export_to_sps,name="export_to_sps"),
    path ('detect/headers',csv_headers,name="csv_headers"),
    path('upload/csv',upload_csv_layer_ajax,name="upload_csv_layer_ajax"),
    path("layers/delete", delete_csv_layers, name="project_layers_delete"),
    path("rlines/select",rl_line_click,name="rl_line_click"),
    path("slines/select",sl_line_click,name="sl_line_click"),
    path("rlines/point_delete/",rp_points_delete,name="rp_points_delete"),
    path("slines/point_delete/",sp_points_delete,name="sp_points_delete"),
    path("api/projects/<int:project_id>/fleet/list/", api_project_fleet_list, name="api_project_fleet_list"),
    path("api/fleet/django/list/", api_django_vessels_list, name="api_django_vessels_list"),
    path("api/projects/<int:project_id>/fleet/add/", api_project_fleet_add_from_django, name="api_project_fleet_add_from_django"),
    path("api/projects/<int:project_id>/fleet/remove/", api_project_fleet_remove, name="api_project_fleet_remove"),

]
