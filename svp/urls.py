from django.urls import path
from . import views

app_name = "svp"

urlpatterns = [
    path("", views.svp_home, name="home"),
    #path("api/profiles/", views.svp_profiles_json, name="profiles_json"),
    #path("api/profiles/<int:profile_id>/points/", views.svp_profile_points_json, name="profile_points_json"),
    #path("api/upload-csv/", views.svp_upload_csv, name="upload_csv"),
    path("api/list/", views.svp_api_list, name="api_list"),
    path("api/details/<int:profile_id>/", views.svp_api_details, name="api_details"),
    path("api/upload/", views.svp_api_upload, name="api_upload"),
    path("api/config/save/", views.svp_api_config_save, name="api_config_save"),
    path("api/config/preview/", views.svp_api_config_preview, name="api_config_preview"),
    path("api/config/export/<int:config_id>/", views.svp_api_config_export, name="api_config_export"),
    path("api/config/import/", views.svp_api_config_import, name="api_config_import"),
    path("api/config/list/", views.svp_api_config_list, name="api_config_list"),
    path("api/config/list/", views.svp_api_config_list, name="api_config_list"),
    path("api/config/get/<int:config_id>/", views.svp_api_config_get, name="api_config_get"),

    path("api/config/delete/<int:config_id>/", views.svp_api_config_delete, name="api_config_delete"),

]
