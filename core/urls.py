from django.urls import path
from .views_version import version_status
from .views import (
    dashboard_view,
    project_list_view,
    project_create_view,
    project_detail_view,
    project_delete_view,
    project_members_view,
    project_set_active_view,
    project_settings_view,
    signup_view,
    logout_view,
    set_theme_view,
)

urlpatterns = [
    path("", dashboard_view, name="dashboard"),
    path("projects/", project_list_view, name="projects"),
    path("projects/new/", project_create_view, name="project_create"),
    path("projects/<int:pk>/", project_detail_view, name="project_detail"),
    path("projects/<int:pk>/delete/", project_delete_view, name="project_delete"),
    path("projects/<int:pk>/members/", project_members_view, name="project_members"),
    path("project/<int:pk>/set_active/", project_set_active_view, name="project_set_active"),
    path("project/settings/", project_settings_view, name="project_settings"),
    path("signup/", signup_view, name="signup"),
    path("logout/", logout_view, name="logout"),
    path("ui/theme/", set_theme_view, name="set_theme"),
    path("api/version/", version_status, name="api_version"),
]
