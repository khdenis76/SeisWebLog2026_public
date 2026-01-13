from django.contrib import admin
from .models import Project
@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    list_display = ('name','root_path','folder_name')
    search_fields = ('name','root_path','folder_name')
