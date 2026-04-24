from django.urls import path
from .views import *

app_name = "fleet"

urlpatterns = [
    path("vessels/", vessel_page, name="vessel_page"),

    # AJAX endpoints
    path("api/vessels/list/", api_vessel_list, name="api_vessel_list"),
    path("api/vessels/create/", api_vessel_create, name="api_vessel_create"),
    path("api/vessels/<int:pk>/update/", api_vessel_update, name="api_vessel_update"),
    path("api/vessels/<int:pk>/delete/", api_vessel_delete, name="api_vessel_delete"),
    path("api/vessels/import-csv/", api_import_fleet_from_csv, name="api_import_fleet_from_csv"),
# Page
    path("sequence-assignments/",sequence_assignments_page,name="sequence_assignments_page"),

    # API
    path("api/fleet/sequence-assignments/list/",api_seq_assign_list,name="api_seq_assign_list"),

    path("api/fleet/sequence-assignments/add/",api_seq_assign_add,name="api_seq_assign_add"),

    path("api/fleet/sequence-assignments/update/",api_seq_assign_update,name="api_seq_assign_update"),

    path("api/fleet/sequence-assignments/delete/",api_seq_assign_delete,name="api_seq_assign_delete"),
]