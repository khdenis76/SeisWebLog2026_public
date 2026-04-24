from django.contrib import admin
from .models import AuditLog

@admin.register(AuditLog)
class AuditLogAdmin(admin.ModelAdmin):
    list_display = (
        "created_at",
        "level",
        "project_name",
        "username_text",
        "action",
        "function_name",
        "status_code",
        "message_short",
    )
    list_filter = ("level", "project_name", "action", "username_text", "created_at")
    search_fields = ("message", "action", "function_name", "username_text", "request_id")
    date_hierarchy = "created_at"
    readonly_fields = [f.name for f in AuditLog._meta.fields]

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser

    @admin.display(description="Message")
    def message_short(self, obj):
        return obj.message[:120]