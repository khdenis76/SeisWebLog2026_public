from django.db import connections
from django.utils.deprecation import MiddlewareMixin

from core.models import UserSettings


class ActiveProjectDBMiddleware(MiddlewareMixin):

    def process_request(self, request):

        if not request.user.is_authenticated:
            return

        try:
            settings_obj, _ = UserSettings.objects.get_or_create(
                user=request.user
            )

            project = settings_obj.active_project

            if not project:
                return

            db_path = project.db_path

            conn = connections["project_db"]

            current_name = conn.settings_dict.get("NAME")

            # switch only if changed
            if current_name != db_path:

                conn.close()

                conn.settings_dict["NAME"] = db_path

        except Exception as e:
            print(f"[PROJECT_DB_MIDDLEWARE] {e}")