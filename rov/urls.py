# baseproject/urls.py
from django.urls import path

from .views import *
urlpatterns = [
    path("", rov_main_view, name="rov_main_view"),
    path("load/drs", rov_upload_dsr,name="rov_upload_dsr"),
    path("load/sm", rov_upload_survey_manager, name="rov_upload_survey_manager"),
    path("load/bbox",rov_upload_black_box,name="rov_upload_black_box"),
    path("load/rec_db",rov_upload_rec_db,name="rov_upload_rec_db"),
    path('select/line',rov_dsr_line_click,name='rov_dsr_line_click'),
    path('bbox_config/save',save_bbox_config,name='save_bbox_config'),
    path('bbox_config/set_default',set_default_bbox_config,name='set_default_bbox_config'),
    path('dsr/delete_line',delete_selected_dsr_lines,name="delete_selected_dsr_lines"),
    path('delete/bbox',delete_bbox_files,name="delete_bbox_files"),
    path('bbox/file_selected',bbox_file_selected,name='bbox_file_selected'),
    path("rov/bbox/configs/", bbox_configs_list, name="bbox_configs_list"),
    path("rov/bbox/config/<int:config_id>/", bbox_config_detail, name="bbox_config_detail"),
    path("rov/bbox/read-headers/",read_bbox_headers,name="csv_headers",),
    path("export/sm/",dsr_export_sm,name="dsr_export_sm"),
    path("dsr/rovs-for-timeframe/",dsr_rovs_for_timeframe,name="dsr_rovs_for_timeframe"),
    path("dsr/production/day",select_prod_day,name="select_prod_day"),
    path("dsr/export/sps",export_dsr_to_sps,name="export_dsr_to_sps"),
    path("dsr/line.selected",dsr_line_onclick,name="dsr_line_onclick"),
]
