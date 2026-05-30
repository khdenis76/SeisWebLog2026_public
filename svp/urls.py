from django.urls import path
from . import views

app_name = "svp"

urlpatterns = [
    path("", views.svp_home, name="home"),
    path("api/list/", views.svp_api_list, name="api_list"),
    path("api/details/<int:profile_id>/", views.svp_api_details, name="api_details"),
    path("api/plot/<int:profile_id>/", views.svp_api_plot, name="api_plot"),
    path("api/upload/", views.svp_api_upload, name="api_upload"),
    path("api/delete/<int:profile_id>/", views.svp_api_delete, name="api_delete"),
    path("api/delete-selected/", views.svp_api_delete_selected, name="api_delete_selected"),

    path("api/config/save/", views.svp_api_config_save, name="api_config_save"),
    path("api/config/preview/", views.svp_api_config_preview, name="api_config_preview"),
    path("api/config/export/<int:config_id>/", views.svp_api_config_export, name="api_config_export"),
    path("api/config/import/", views.svp_api_config_import, name="api_config_import"),
    path("api/config/list/", views.svp_api_config_list, name="api_config_list"),
    path("api/config/get/<int:config_id>/", views.svp_api_config_get, name="api_config_get"),
    path("api/config/delete/<int:config_id>/", views.svp_api_config_delete, name="api_config_delete"),
]
