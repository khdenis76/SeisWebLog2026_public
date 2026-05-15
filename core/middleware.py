from pathlib import Path


from django.shortcuts import redirect
from django.contrib import messages


class ActiveProjectMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        request.active_project = None

        if not request.user.is_authenticated:
            return self.get_response(request)

        allowed_names = {"projects", "login", "logout"}
        match = getattr(request, "resolver_match", None)
        current_url_name = match.url_name if match else None

        try:
            from core.models import UserSettings

            settings_obj, _ = UserSettings.objects.get_or_create(user=request.user)
            project = settings_obj.active_project

            if not project:
                return self.get_response(request)

            db_path = getattr(project, "db_path", None)

            if not db_path or not Path(db_path).exists():
                settings_obj.active_project = None
                settings_obj.save(update_fields=["active_project"])

                if current_url_name not in allowed_names:
                    messages.warning(
                        request,
                        "Your active project was not found. Please select a project again."
                    )
                    return redirect("projects")

                return self.get_response(request)

            request.active_project = project

            return self.get_response(request)

        except Exception as e:
            print(f"[ActiveProjectMiddleware] {e}")

            try:
                from core.models import UserSettings

                settings_obj, _ = UserSettings.objects.get_or_create(user=request.user)
                settings_obj.active_project = None
                settings_obj.save(update_fields=["active_project"])
            except Exception:
                pass

            if current_url_name not in allowed_names:
                messages.error(
                    request,
                    "Could not open active project. Please select a project again."
                )
                return redirect("projects")

            return self.get_response(request)