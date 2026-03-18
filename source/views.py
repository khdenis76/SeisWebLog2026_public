import json
from datetime import date

from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect
from django.core.exceptions import PermissionDenied
from django.template.loader import render_to_string
from django.views.decorators.csrf import csrf_protect

from core.projectdb import ProjectDB   # <-- change to your real import
from core.models import UserSettings,SPSRevision
from django.http import JsonResponse
from django.views.decorators.http import require_POST
import re

from utils.decorators import log_action
from .source_data import SourceData   # adjust path
from .source_map_graph import SourceMapGraphics


@login_required
@log_action("open source home", object_type="SOU")
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
    #sd.ensure_shot_table_schema()
    sd.create_v_shot_linesummary_view()
    gun_qc = pdb.get_gun_qc()
    min_depth_limit = gun_qc.depth - gun_qc.depth_tolerance
    max_depth_limit = gun_qc.depth + gun_qc.depth_tolerance
    shot_table_rows = sd.list_shot_table_summary()

    st_summary = render_to_string("source/partials/shot_table_tbody.html",{"rows":shot_table_rows})
    sps_table_rows = sd.list_sps_files_summary()
    sps_summary = render_to_string("source/partials/sps_table_tbody.html",
                                   {"rows":sps_table_rows,
                                    "min_depth_limit":min_depth_limit,
                                    "max_depth_limit":max_depth_limit,
                                    "gun_qc":gun_qc,
                                    })
    res = sd.read_vessel_purpose_summary()
    project_summary = render_to_string(
        "source/partials/vessel_purpose_summary.html",
        {"rows": res["rows"], "totals": res["totals"]},
        request=request)
    data = sd.get_shot_line_summary()  # or your existing read method
    shot_line_summary = render_to_string(
        "source/partials/_shot_line_summary_tbody.html",{"rows": data["rows"]})


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
            "sps_summary": sps_summary,
            "gun_qc":gun_qc,
            "project_summary":project_summary,
            "shot_line_summary":shot_line_summary,

        },)
@login_required
@require_POST
@log_action("upload source files", object_type="SOU_UPL")
def source_upload_files(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"ok": False, "error": "No active project."}, status=400)
    if not project.can_edit(request.user):
        return JsonResponse({"ok": False, "error": "Permission denied."}, status=403)

    file_type = (request.POST.get("file_type") or "").strip().upper()
    files = request.FILES.getlist("files")
    if not files:
        return JsonResponse({"ok": False, "error": "No files selected."}, status=400)

    sd = SourceData(project.db_path)
    pdb = ProjectDB(project.db_path)

    sps_revision = None
    tier = 1
    year = None
    src_id = None

    detect_by_seq = str(request.POST.get("detect_vessel_by_seq") or "").strip().lower() in ("1", "true", "on", "yes")
    auto_year_by_jday = str(request.POST.get("auto_year_by_jday") or "").strip().lower() in ("1", "true", "on", "yes")

    if file_type == "SPS":
        source_vessel_id = (request.POST.get("source_vessel_id") or "").strip()
        sps_revision_id = (request.POST.get("sps_revision_id") or "").strip()
        tier_raw = (request.POST.get("tier") or "").strip()
        year_raw = (request.POST.get("year") or "").strip()

        if (not detect_by_seq) and (not source_vessel_id):
            return JsonResponse({"ok": False, "error": "Missing Source Vessel."}, status=400)
        if not sps_revision_id:
            return JsonResponse({"ok": False, "error": "Missing SPS Revision."}, status=400)
        if not tier_raw:
            return JsonResponse({"ok": False, "error": "Missing TIER."}, status=400)

        if (not auto_year_by_jday) and (not year_raw):
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

        if not auto_year_by_jday:
            try:
                year = int(year_raw)
            except Exception:
                return JsonResponse({"ok": False, "error": "Invalid Year."}, status=400)

        if not detect_by_seq:
            fleet = pdb.list_project_fleet()
            try:
                src_id = int(source_vessel_id)
            except Exception:
                return JsonResponse({"ok": False, "error": "Invalid Source Vessel id."}, status=400)

            vessel_row = next((v for v in fleet if int(v.get("id") or 0) == src_id), None)
            if not vessel_row:
                return JsonResponse({"ok": False, "error": "Source Vessel not found in project fleet."}, status=400)

    try:
        total_inserted = 0
        file_results = []
        target_tbody = None
        tbody_html = None
        extras = {}

        if file_type == "SHOT":
            # full replace shot table
            inserted = 0
            for f in files:
                inserted = sd.load_shot_table_h26_replace_all_fast(
                    f.file,
                    chunk_size=10000,
                    conn=None,
                )
                total_inserted += int(inserted)
                file_results.append({
                    "name": f.name,
                    "inserted": int(inserted),
                })

            shot_table_rows = sd.list_shot_table_summary()
            tbody_html = render_to_string(
                "source/partials/shot_table_tbody.html",
                {"rows": shot_table_rows},
                request=request,
            )
            target_tbody = "shot-table-tbody"

        elif file_type == "SPS":
            for f in files:
                res = sd.load_source_sps_uploaded_file_fast(
                    f,
                    sps_revision=sps_revision,
                    geometry=pdb.get_geometry(),
                    vessel_fk=(None if detect_by_seq else src_id),
                    tier=tier,
                    line_bearing=0.0,
                    default=year,
                    batch_size=50000,
                    detect_vessel_by_seq=detect_by_seq,
                    auto_year_by_jday=auto_year_by_jday,
                )

                sd.update_slsolution_from_spsolution_timebased(file_fk=res["file_fk"])
                sd.update_seq_maxspi(
                    production_code=pdb.get_geometry().production_code,
                    seq=res["seq"],
                )
                sd.update_slsolution_from_preplot_timebased(file_fk=res["file_fk"])

                total_inserted += int(res.get("points") or 0)
                file_results.append({"name": f.name, **res})

            gun_qc = pdb.get_gun_qc()
            min_depth_limit = None
            max_depth_limit = None
            if gun_qc and getattr(gun_qc, "depth", None) is not None and getattr(gun_qc, "depth_tolerance", None) is not None:
                min_depth_limit = gun_qc.depth - gun_qc.depth_tolerance
                max_depth_limit = gun_qc.depth + gun_qc.depth_tolerance

            sps_table_rows = sd.list_sps_files_summary()
            tbody_html = render_to_string(
                "source/partials/sps_table_tbody.html",
                {
                    "rows": sps_table_rows,
                    "gun_qc": gun_qc,
                    "min_depth_limit": min_depth_limit,
                    "max_depth_limit": max_depth_limit,
                },
                request=request,
            )
            target_tbody = "sps-table-tbody"

            extras = {
                "gun_qc": {
                    "depth": getattr(gun_qc, "depth", None) if gun_qc else None,
                    "depth_tolerance": getattr(gun_qc, "depth_tolerance", None) if gun_qc else None,
                },
                "min_depth_limit": min_depth_limit,
                "max_depth_limit": max_depth_limit,
            }

        else:
            return JsonResponse({"ok": False, "error": "Unsupported file type."}, status=400)

        out = {
            "ok": True,
            "file_type": file_type,
            "files": file_results,
            "rows_inserted": int(total_inserted),
            "target_tbody": target_tbody,
            "tbody_html": tbody_html,
        }
        out.update(extras)

        return JsonResponse(out)

    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)

@login_required
@require_POST
@log_action("delete sps", object_type="SOU_DEL")
def sps_delete_selected(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        # No active project → go to project list
        return redirect("projects")

    if not project.can_edit(request.user):
        raise PermissionDenied("You are not a member of this project.")

    pdb = ProjectDB(project.db_path)
    sd = SourceData(project.db_path)
    try:
        payload = json.loads(request.body or "{}")
        payload = json.loads(request.body or "{}")
        ids = payload.get("ids", [])
        if not ids:
            return JsonResponse({"ok": False, "error": "No rows selected"}, status=400)
        # convert to int
        try:
            ids = [int(i) for i in ids]
            del_count = sd.delete_sps_by_ids(ids)
            rows = sd.list_sps_files_summary()
            sps_summary = render_to_string("source/partials/sps_table_tbody.html",{"rows": rows})
            return JsonResponse({"ok": True,"sps_summary": sps_summary})

        except ValueError as e:
            return JsonResponse({"ok": False, "error": f"Invalid IDs {e}"}, status=400)


    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=400)

@login_required
@log_action("upload QC map", object_type="SOU")
def source_qc_progress_map_json(request):
    # build your SourceData / whatever class you use
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        # No active project → go to project list
        return redirect("projects")

    if not project.can_view(request.user):
        raise PermissionDenied("You are not a member of this project.")
    pdb = ProjectDB(project.db_path)
    smd = SourceMapGraphics(project.db_path)  # adapt to your project
    item = smd.build_source_progress_map(
        production_only=True,
        use_tiles=True,          # or False
        is_show=False,
        json_return=True,
        show_shapes=True,
        show_layers=True,
        default_epsg=pdb.get_main().epsg,  # adapt
        max_csv_labels=5,
    )
    return JsonResponse(item)
@login_required
@log_action("upload source chart", object_type="SOU")
def source_qc_sunburst_json(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return redirect("projects")
    if not project.can_view(request.user):
        raise PermissionDenied("You are not a member of this project.")

    # optional theme from querystring: ?theme=dark|light
    theme = (request.GET.get("theme") or "light").strip().lower()
    if theme not in ("light", "dark"):
        theme = "light"

    smd = SourceMapGraphics(project.db_path)

    item = smd.build_source_sunburst(
        is_show=False,
        json_return=True,
        theme=theme,      # <-- you added this earlier
        drop_zeros=True,
        title="Source — Vessel → Purpose → Shot totals",
    )
    return JsonResponse(item)
@login_required
@log_action("upload source dbd graph", object_type="SOU")
def source_daybyday_production_json(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project

    if not project:
        return JsonResponse({"error": "No active project selected."}, status=400)

    if not project.can_view(request.user):
        raise PermissionDenied("You are not a member of this project.")

    production_code = request.GET.get("production_code", "K")
    non_production_code = request.GET.get("non_production_code", "")
    kill_code = request.GET.get("kill_code", "X")
    include_other = request.GET.get("include_other", "0") in ("1", "true", "True", "yes")
    max_days = request.GET.get("max_days")

    try:
        max_days = int(max_days) if max_days not in (None, "", "null") else None
    except ValueError:
        max_days = None

    smd = SourceMapGraphics(project.db_path)
    pdb = ProjectDB(project.db_path)
    item = smd.build_daybyday_source_production(
        production_code=pdb.get_geometry().production_code,
        non_production_code=pdb.get_geometry().non_production_code,
        kill_code=pdb.get_geometry().kill_code,
        is_show=False,
        include_other=False,
        json_return=True,
        title="Day-by-Day Source Production",
    )

    return JsonResponse(item)
@log_action("convert to int", object_type="AUX_MATH")
def _to_int_or_none(v):
    try:
        v = str(v or "").strip()
        return int(v) if v != "" else None
    except Exception:
        return None

@log_action("convert to float", object_type="AUX_MATH")
def _to_float_or_none(v):
    try:
        s = str(v).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _norm_str(v):
    return str(v or "").strip()


def _row_text_for_search(row):
    parts = []
    for val in row.values():
        if val is None:
            continue
        parts.append(str(val))
    return " ".join(parts).lower()


@login_required
@log_action("get sps data", object_type="SOU")
def source_sps_table_data(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"ok": False, "error": "No active project."}, status=400)

    if not project.can_view(request.user):
        raise PermissionDenied("You are not a member of this project.")

    pdb = ProjectDB(project.db_path)
    sd = SourceData(project.db_path)

    gun_qc = pdb.get_gun_qc()
    min_depth_limit = None
    max_depth_limit = None
    if gun_qc:
        min_depth_limit = gun_qc.depth - gun_qc.depth_tolerance
        max_depth_limit = gun_qc.depth + gun_qc.depth_tolerance

    sps_table_rows = sd.list_sps_files_summary(
        search=request.GET.get("search", ""),
        vessel_fk=_to_int_or_none(request.GET.get("vessel")),
        purpose=request.GET.get("purpose", ""),
        sailline=request.GET.get("sailline", ""),
        attempt=request.GET.get("attempt", ""),
        line_from=_to_int_or_none(request.GET.get("line_from")),
        line_to=_to_int_or_none(request.GET.get("line_to")),
        seq_from=_to_int_or_none(request.GET.get("seq_from")),
        seq_to=_to_int_or_none(request.GET.get("seq_to")),
        wd=request.GET.get("wd", ""),
        gd=request.GET.get("gd", ""),
        min_depth_limit=min_depth_limit,
        max_depth_limit=max_depth_limit,
        sort_by=request.GET.get("sort_by", "seq"),
        sort_dir=request.GET.get("sort_dir", "asc"),
    )

    sps_summary = render_to_string(
        "source/partials/sps_table_tbody.html",
        {
            "rows": sps_table_rows,
            "min_depth_limit": min_depth_limit,
            "max_depth_limit": max_depth_limit,
            "gun_qc": gun_qc,
        },
        request=request,
    )

    return JsonResponse({
        "ok": True,
        "count": len(sps_table_rows),
        "sps_summary": sps_summary,
    })

@login_required
@log_action("source_sp_solution_vs_preplot", object_type="SOU")
def source_sp_solution_vs_preplot_json(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"ok": False, "error": "No active project."}, status=400)

    if not project.can_view(request.user):
        raise PermissionDenied("You are not a member of this project.")

    line_raw = request.GET.get("line")
    if line_raw in (None, ""):
        return JsonResponse({"ok": False, "error": "Missing line."}, status=400)

    try:
        line = int(line_raw)
    except (TypeError, ValueError):
        return JsonResponse({"ok": False, "error": "Invalid line."}, status=400)

    label_every = request.GET.get("label_every", 10)
    point_size = request.GET.get("point_size", 7)

    try:
        label_every = max(1, min(20, int(label_every)))
    except (TypeError, ValueError):
        label_every = 10

    try:
        point_size = max(2, int(point_size))
    except (TypeError, ValueError):
        point_size = 7

    try:
        sm = SourceMapGraphics(project.db_path)

        item = sm.build_sp_solution_vs_preplot(
            line=line,
            is_show=False,
            json_return=True,
            label_every=label_every,
            point_size=point_size,
        )

        return JsonResponse({
            "ok": True,
            "line": line,
            "item": item,
        })

    except Exception as e:
        return JsonResponse({
            "ok": False,
            "error": str(e),
        }, status=500)


def shot_line_summary_table(request):
    return None