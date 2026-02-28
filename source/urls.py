from django.urls import path
from . import views

urlpatterns = [
    path("", views.source_home, name="source_home"),
    path("upload/",views.source_upload_files,name="source_upload_files"),
]