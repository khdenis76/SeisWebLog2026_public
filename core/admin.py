from django.contrib import admin
from .models import Project, ProjectMember, UserSettings
from django.contrib.auth.models import User
from django.contrib.auth.admin import UserAdmin as DjangoUserAdmin
from django.contrib.auth import get_user_model
User = get_user_model()
# Unregister default User admin (auth.UserAdmin)
try:
    admin.site.unregister(User)
except admin.sites.NotRegistered:
    pass

@admin.register(User)
class UserAdmin(DjangoUserAdmin):
    list_display = ("username", "email", "is_staff", "is_superuser", "is_active")
    list_filter = ("is_staff", "is_superuser", "is_active")
    search_fields = ("username", "email")
    ordering = ("username",)


@admin.register(ProjectMember)
class ProjectMemberAdmin(admin.ModelAdmin):
    list_display = ("project", "user", "can_edit")
    list_filter = ("can_edit", "project")
    search_fields = ("project__name", "user__username", "user__email")
    autocomplete_fields = ("project", "user")


@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    list_display = ("name", "owner", "created_at")
    search_fields = ("name", "owner__username")
    autocomplete_fields = ("owner",)
    # Optional: show memberships inline
    # (inline class below)


class ProjectMemberInline(admin.TabularInline):
    model = ProjectMember
    extra = 0
    autocomplete_fields = ("user",)


ProjectAdmin.inlines = [ProjectMemberInline]


@admin.register(UserSettings)
class UserSettingsAdmin(admin.ModelAdmin):
    list_display = ("user", "active_project")
    autocomplete_fields = ("user", "active_project")