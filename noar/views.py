import os
import tempfile
from pathlib import Path

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import JsonResponse
from django.shortcuts import render, redirect
from datetime import datetime

from django.views.decorators.http import require_POST

from core.models import UserSettings, SPSRevision
from core.projectdb import ProjectDB
from baseproject.utils.solutions_db import SolutionsDB
from utils.decorators import log_action
from noar.receiver_sps import ReceiverSPS

@login_required
@log_action("show noar home page", object_type="NOAR")
def noar_home(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project

    if not project:
        # No active project → go to project list
        return redirect("projects")
    if not project.can_view(request.user):
        raise PermissionDenied("You are not a member of this project.")
    pdb=ProjectDB(project.db_path)
    sps_revisions = SPSRevision.objects.all().order_by("rev_name")
    prjsol = SolutionsDB(project.db_path)
    prjsol.ensure_table()
    solutions = prjsol.list_solutions()
    project_fleet = pdb.list_project_fleet()


    return render(request, "noar/noar_home.html", {
        "project": project,
        "current_year": datetime.now().year,
        "project_fleet": project_fleet,
        "solutions": solutions,
        "sps_revisions": sps_revisions,

    })

def noar_dashboard_api(request):
    return JsonResponse({
        "ok": True,
        "html": """
        <div class="seis-empty-state">
          <div>
            <div class="seis-empty-state-icon">
              <i class="fas fa-water"></i>
            </div>
            <div class="fw-semibold mb-1">NOAR dashboard loaded</div>
            <div class="text-muted small">Add NOAR plots and tables here.</div>
          </div>
        </div>
        """
    })
@require_POST
@login_required
@log_action("Load SPS file(s)", object_type="NOAR")
def noar_load_sps(request):
    files = request.FILES.getlist("files")

    # Modal field name is sps_revision_id
    sps_revision_id = request.POST.get("sps_revision_id", "")
    year = request.POST.get("year", "")
    node_vessel_fk = request.POST.get("node_vessel_fk", "")
    solution_fk = request.POST.get("solution_fk", "")

    if not files:
        return JsonResponse({
            "ok": False,
            "error": "No SPS files selected."
        }, status=400)

    if not sps_revision_id:
        return JsonResponse({
            "ok": False,
            "error": "SPS Revision is required."
        }, status=400)

    if not year:
        return JsonResponse({
            "ok": False,
            "error": "Year is required."
        }, status=400)

    if not node_vessel_fk:
        return JsonResponse({
            "ok": False,
            "error": "Node Vessel is required."
        }, status=400)

    if not solution_fk:
        return JsonResponse({
            "ok": False,
            "error": "Solution is required."
        }, status=400)

    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project

    if not project:
        return JsonResponse({
            "ok": False,
            "error": "No active project selected."
        }, status=400)

    if not project.can_view(request.user):
        raise PermissionDenied("You are not a member of this project.")
    pdb=ProjectDB(project.db_path)
    geometry = pdb.get_geometry()
    receiver_line_mask = geometry.rl_mask
    try:

        sps_revision_id = int(sps_revision_id)
        year = int(year)
        node_vessel_fk = int(node_vessel_fk)
        solution_fk = int(solution_fk)
    except ValueError:
        return JsonResponse({
            "ok": False,
            "error": "Invalid SPS Revision, Year, Node Vessel, or Solution."
        }, status=400)

    loader = ReceiverSPS(project.db_path)

    results = []
    loaded_count = 0
    failed_count = 0
    total_rows = 0
    total_skipped = 0
    total_lines = 0

    tmp_paths = []

    try:
        with tempfile.TemporaryDirectory(prefix="noar_sps_") as tmp_dir:

            for uploaded_file in files:
                safe_name = os.path.basename(uploaded_file.name)
                tmp_path = Path(tmp_dir) / safe_name

                with open(tmp_path, "wb+") as destination:
                    for chunk in uploaded_file.chunks():
                        destination.write(chunk)

                tmp_paths.append(str(tmp_path))

                result = loader.load_file(
                    str(tmp_path),
                    sps_revision_id=sps_revision_id,
                    solution_fk=solution_fk,
                    year=year,
                    node_vessel_fk=node_vessel_fk,
                    tier=1,
                    line_mask=receiver_line_mask,
                    chunk_size=20000,
                )

                results.append(result)

                if result.get("ok"):
                    loaded_count += 1
                    total_rows += int(result.get("rows", 0) or 0)
                    total_skipped += int(result.get("skipped", 0) or 0)
                    total_lines += int(result.get("lines", 0) or 0)
                else:
                    failed_count += 1

    except Exception as exc:
        return JsonResponse({
            "ok": False,
            "error": str(exc),
            "results": results,
        }, status=500)

    ok = failed_count == 0

    return JsonResponse({
        "ok": ok,
        "message": (
            f"Loaded {loaded_count} SPS file(s), "
            f"{total_rows} point rows, "
            f"{total_lines} line(s). "
            f"Skipped rows: {total_skipped}."
        ),
        "files_count": len(files),
        "loaded_count": loaded_count,
        "failed_count": failed_count,
        "total_rows": total_rows,
        "total_skipped": total_skipped,
        "total_lines": total_lines,
        "sps_revision_id": sps_revision_id,
        "year": year,
        "node_vessel_fk": node_vessel_fk,
        "solution_fk": solution_fk,
        "results": results,
    }, status=200 if ok else 400)