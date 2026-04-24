from django.urls import path
from . import views

urlpatterns = [
    path("", views.members_page, name="project_users_members"),
    path("create/", views.create_user, name="project_users_create"),
    path("add/", views.add_member, name="project_users_add"),
    path("member/<int:member_id>/update/", views.update_member, name="project_users_update_member"),
    path("member/<int:member_id>/remove/", views.remove_member, name="project_users_remove_member"),
    path("user/<int:user_id>/toggle-active/", views.toggle_user_active, name="project_users_toggle_active"),
]