from django.http import JsonResponse
from django.shortcuts import render
from django.template.loader import render_to_string
from django.views.decorators.http import require_GET, require_POST
from utils.decorators import log_action
from core.models import UserSettings
from baseproject.utils.solutions_db import SolutionsDB


def _get_active_project_db(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project

    if not project:
        raise ValueError("No active project selected.")
    return project.db_path


@require_GET
@log_action("Open solutions tab (solutions_tab)", object_type="BP_SOL")
def solutions_tab(request):
    try:
        db_path = _get_active_project_db(request)
        db = SolutionsDB(db_path)
        solutions = db.list_solutions()
        html = render_to_string(
            "baseproject/partials/solutions_tab.html",
            {
                "solutions": solutions
            },
            request=request,
        )

        return JsonResponse({
            "ok": True,
            "html": html
        })

    except Exception as e:
        return JsonResponse({
            "ok": False,
            "error": str(e)
        }, status=400)

@require_POST
@log_action("add new solution (solutions_add)", object_type="BP_SOL")
def solution_add(request):
    try:
        db_path = _get_active_project_db(request)
        print(db_path)
        db = SolutionsDB(db_path)

        db.add_solution(
            solution=request.POST.get("solution", ""),
            comments=request.POST.get("comments", ""),
        )

        solutions = db.list_solutions()

        html = render_to_string(
            "baseproject/partials/solutions_table_body.html",
            {
                "solutions": solutions
            },
            request=request,
        )

        return JsonResponse({
            "ok": True,
            "html": html
        })

    except Exception as e:
        return JsonResponse({
            "ok": False,
            "error": str(e)
        }, status=400)


@require_POST
@log_action("Delete new solution (solutions_delete)", object_type="BP_SOL")
def solution_delete(request):
    try:
        db_path = _get_active_project_db(request)

        solution_id = int(request.POST.get("solution_id"))

        db = SolutionsDB(db_path)

        db.delete_solution(solution_id)

        solutions = db.list_solutions()

        html = render_to_string(
            "baseproject/partials/solutions_table_body.html",
            {
                "solutions": solutions
            },
            request=request,
        )

        return JsonResponse({
            "ok": True,
            "html": html
        })

    except Exception as e:
        return JsonResponse({
            "ok": False,
            "error": str(e)
        }, status=400)