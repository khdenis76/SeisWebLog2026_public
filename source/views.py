from datetime import date

from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect
from django.core.exceptions import PermissionDenied
from django.template.loader import render_to_string

from core.projectdb import ProjectDB   # <-- change to your real import
from core.models import UserSettings,SPSRevision
from django.http import JsonResponse
from django.views.decorators.http import require_POST
import re
from .source_data import SourceData   # adjust path

@login_required
def source_home(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        # No active project → go to project list
        return redirect("projects")

    if not project.can_view(request.user):
        raise PermissionDenied("You are not a member of this project.")


    pdb = ProjectDB(project.db_path)
    sd = SourceData(project.db_path)
    shot_table_rows = sd.list_shot_table_summary()
    st_summary = render_to_string("source/partials/shot_table_tbody.html",{"rows":shot_table_rows})


    # Project fleet (project-specific)
    fleet = pdb.list_project_fleet()  # list[dict]

    # Filter: active + Source
    fleet_vessels = []
    for v in fleet:
        vtype = str(v.get("vessel_type") or "").strip().lower()
        is_active = int(v.get("is_active") or 0)
        if is_active == 1 and vtype == "source":
            fleet_vessels.append(v)

    years = list(range(date.today().year, 1999, -1))  # now..2000
    tiers = list(range(1, 11))  # 1..10

    sps_revisions = SPSRevision.objects.all()  # adjust field

    return render(
        request,
        "source/source_home.html",
        {
            "fleet_vessels": fleet_vessels,
            "years": years,
            "tiers": tiers,
            "sps_revisions": sps_revisions,
            "st_summary": st_summary,
        },)



@login_required
@require_POST
def source_upload_files(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"ok": False, "error": "No active project."}, status=400)
    if not project.can_edit(request.user):
        return JsonResponse({"ok": False, "error": "Permission denied."}, status=403)

    file_type = (request.POST.get("file_type") or "").strip().upper()
    files = request.FILES.getlist("files")  # matches <input name="files" ... multiple>
    if not files:
        return JsonResponse({"ok": False, "error": "No files selected."}, status=400)

    sd = SourceData(project.db_path)
    pdb = ProjectDB(project.db_path)


    # =========================
    # SPS metadata (from modal)
    # =========================
    sps_revision = None
    tier = 1
    year = None
    vessel_name = None  # we will resolve from source_vessel_id

    if file_type == "SPS":
        # modal fields: source_vessel_id, sps_revision_id, tier, year
        source_vessel_id = (request.POST.get("source_vessel_id") or "").strip()
        sps_revision_id = (request.POST.get("sps_revision_id") or "").strip()
        tier_raw = (request.POST.get("tier") or "").strip()
        year_raw = (request.POST.get("year") or "").strip()

        if not source_vessel_id:
            return JsonResponse({"ok": False, "error": "Missing Source Vessel."}, status=400)
        if not sps_revision_id:
            return JsonResponse({"ok": False, "error": "Missing SPS Revision."}, status=400)
        if not tier_raw:
            return JsonResponse({"ok": False, "error": "Missing TIER."}, status=400)
        if not year_raw:
            return JsonResponse({"ok": False, "error": "Missing Year."}, status=400)

        try:
            sps_revision = SPSRevision.objects.get(id=int(sps_revision_id))
        except Exception:
            return JsonResponse({"ok": False, "error": "Invalid SPS Revision."}, status=400)

        try:
            tier = int(tier_raw)
        except Exception:
            return JsonResponse({"ok": False, "error": "Invalid TIER."}, status=400)
        if tier < 1 or tier > 10:
            return JsonResponse({"ok": False, "error": "TIER must be 1..10."}, status=400)

        try:
            year = int(year_raw)
        except Exception:
            return JsonResponse({"ok": False, "error": "Invalid Year."}, status=400)

        # Resolve vessel_name from project_fleet (ProjectDB)
        fleet = pdb.list_project_fleet()
        vessel_row = None
        try:
            src_id = int(source_vessel_id)
        except Exception:
            return JsonResponse({"ok": False, "error": "Invalid Source Vessel id."}, status=400)

        for v in fleet:
            if int(v.get("id") or 0) == src_id:
                vessel_row = v
                break

        if not vessel_row:
            return JsonResponse({"ok": False, "error": "Source Vessel not found in project fleet."}, status=400)

        vessel_name = (vessel_row.get("vessel_name") or "").strip() or None

    # =========================
    # Speed: drop indexes once for SHOT
    # =========================
    dropped_shot = False
    if file_type == "SHOT":
        try:
            sd.create_shot_table()
            sd.drop_shot_table_indexes()
            dropped_shot = True
        except Exception:
            dropped_shot = False

    try:
        total_inserted= 0
        file_results =[]
        for f in files:
            # Files record (unique by FileName) – your function already handles duplicates now
            file_fk = sd.insert_file_record(file_name=f.name, file_type=file_type)
            if not file_fk:
                return JsonResponse({"ok": False, "error": f"Failed to create Files record for {f.name}."}, status=500)

            if file_type == "SHOT":
                inserted = sd.load_shot_table_h26_stream_fast(
                    f.file,
                    file_fk=file_fk,
                    chunk_size=50000,
                )
                total_inserted += int(inserted)
                file_results.append({"name": f.name, "file_fk": int(file_fk), "inserted": int(inserted)})

            elif file_type == "SPS":


                res = sd.load_source_sps_uploaded_file_fast(
                    f,  # UploadedFile
                    sps_revision=sps_revision,
                    vessel=vessel_name,   # resolved from project_fleet
                    tier=tier,
                    geometry=pdb.get_geometry(),
                    line_bearing=0.0,
                    default=year,
                    batch_size=50000,
                )
                # res should include points/skipped/lines/file_fk etc.
                total_inserted += int(res.get("points") or 0)
                file_results.append({"name": f.name, **res})

            else:
                return JsonResponse({"ok": False, "error": "Unsupported file type."}, status=400)

    finally:
        if dropped_shot:
            try:
                sd.create_shot_table_indexes()
            except Exception:
                pass

    return JsonResponse({
        "ok": True,
        "file_type": file_type,
        "files": file_results,
        "rows_inserted": int(total_inserted),
    })