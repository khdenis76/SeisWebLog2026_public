import time

from django.shortcuts import render

# Create your views here.
# baseproject/views.py
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import render, redirect

from core.models import UserSettings, SPSRevision
from core.models import Project
from core.projectdb import ProjectDB
from .models import BaseProjectFile
from .forms import BaseProjectUploadForm
from django.http import JsonResponse

@login_required
def base_project_settings_view(request):
    """
    Upload base project files (SPS, headers, shapefiles, etc.)
    for the currently active project.
    Files are stored directly in the SQLite database.
    """

    # 1. Get active project
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    #WelcomeToTGSV3ss3!$
    if not project:
        # No active project → go to project list
        return redirect("projects")

    if not project.can_edit(request.user):
        raise PermissionDenied

    if request.method == "POST":
        form = BaseProjectUploadForm(request.POST, request.FILES)
        if form.is_valid():
            file_type = form.cleaned_data["file_type"]

            # All selected files have the same type
            uploaded_files = request.FILES.getlist("files")

            for f in uploaded_files:
                # f is an UploadedFile; read its bytes once

                BaseProjectFile.objects.create(
                    project=project,
                    file_type=file_type,
                    file_name=f.name,
                    file_size=f.size,
                    uploaded_by=request.user,
                )

            return redirect("base_project_settings")
    else:
        form = BaseProjectUploadForm()

    # 2. Group existing files by type for display
    files_qs = BaseProjectFile.objects.filter(project=project)

    grouped = {
        "source_sps": files_qs.filter(file_type=BaseProjectFile.TYPE_SOURCE_SPS),
        "receiver_sps": files_qs.filter(file_type=BaseProjectFile.TYPE_RECEIVER_SPS),
        "header_sps": files_qs.filter(file_type=BaseProjectFile.TYPE_HEADER_SPS),

    }
    pdb = ProjectDB(project.db_path)
    rows = pdb.select_rlpreplot()
    return render(
        request,
        "baseproject/base_settings.html",
        {
            "project": project,
            "form": form,
            "grouped": grouped,
            "rows":rows
        },
    )

@login_required
def upload_preplots(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST only"}, status=405)

    start = time.perf_counter()

    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)

    dup_mode = request.POST.get("dup_mode", "add")

    pdb = ProjectDB(project.db_path)

    files = request.FILES.getlist("files")
    if not files:
        return JsonResponse({"error": "No files uploaded"}, status=400)

    # --- params ---
    sps_revision = SPSRevision.objects.get(id=request.POST["sps_revision"])
    tier = int(request.POST.get("tier", 1))
    bearing = float(request.POST.get("bearing", 0))

    total_points = 0
    total_lines = 0
    processed_files = []

    for f in files:
        # 🚀 ВАЖНО: используем FAST loader
        result = pdb.load_sps_uploaded_file_fast(
            uploaded_file=f,
            sps_revision=sps_revision,
            default=None,
            tier=tier,
            line_bearing=bearing,
            point_type="R",
            dup_mode=dup_mode,
            batch_size=20000,   # можно 10k–50k
        )

        processed_files.append({
            "name": f.name,
            "points": result["points"],
            "lines": result["lines"],
            "skipped": result["skipped"],
        })

        total_points += result["points"]
        total_lines += result["lines"]

    # RLPreplot уже актуален — просто читаем
    rows = pdb.select_rlpreplot()

    elapsed = round(time.perf_counter() - start, 2)

    return JsonResponse({
        "status": "ok",
        "files": processed_files,
        "total_points": total_points,
        "total_lines": total_lines,
        "elapsed_sec": elapsed,
        "rows": rows,
    })

