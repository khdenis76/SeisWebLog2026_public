from django.urls import path
from . import views

app_name = "noar"

urlpatterns = [
    path("", views.noar_home, name="noar_home"),
]