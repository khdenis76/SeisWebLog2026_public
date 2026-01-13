from django.contrib import admin
from django.urls import path, include
from django.contrib.auth import views as auth_views
from django.contrib.auth.views import LoginView, LogoutView
from core.views import project_members_view

from core.views import (
    logout_view,
    signup_view,
    dashboard_view,
    project_list_view,
    project_create_view,
    project_detail_view,
    project_delete_view,
    project_members_view,
    project_set_active_view,
)

urlpatterns = [
    path('admin/', admin.site.urls),

    path('logout/', logout_view, name='logout'),
    path('signup/', signup_view, name='signup'),

    path('', dashboard_view, name='dashboard'),

    path('projects/', project_list_view, name='projects'),
    path('projects/new/', project_create_view, name='project_create'),
    path('projects/<int:pk>/', project_detail_view, name='project_detail'),
    path('projects/<int:pk>/delete/', project_delete_view, name='project_delete'),
    path('projects/<int:pk>/members/', project_members_view, name='project_members'),
    path('projects/<int:pk>/set-active/', project_set_active_view, name='project_set_active'),
    path('login/', auth_views.LoginView.as_view(template_name='registration/login.html'), name='login'),
    path("project/base/", include("baseproject.urls")),


    path('', include('core.urls')),
]
