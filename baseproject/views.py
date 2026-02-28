import glob
import io
import os.path
import time
import json
from pathlib import Path

import pandas as pd

from bokeh.layouts import column, row
from bokeh.models import Button, CustomJS
from django.shortcuts import render

# Create your views here.
# baseproject/views.py
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import render, redirect
from django.views.decorators.http import require_POST, require_GET
from dataclasses import dataclass, field

from pygments.lexer import default

from core.models import UserSettings, SPSRevision
from core.models import Project
from core.projectdb import ProjectDB
from core.projectlayers import ProjectLayer
from core.projectshp import ProjectShape
from baseproject.preplot_graphics import PreplotGraphics
from fleet.models import Vessel
from fleet.utils import import_vessels_from_csv_if_missing
from .models import BaseProjectFile
from .forms import BaseProjectUploadForm
from django.http import JsonResponse
from django.template.loader import render_to_string
from bokeh.embed import components, json_item


@dataclass
class ShapeFile:
    file_name:str =""
    full_name:str =""
    file_size:int=0
    is_indb:int=0
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
    pgr =PreplotGraphics(project.db_path)


    folders =pdb.get_folders()
    shp_list = get_shape_list(folders.shapes_folder)
    prj_shapes = pdb.get_shapes()
    prj_full_names = {s.full_name for s in prj_shapes}
    for shp in shp_list:
        shp.is_indb = 1 if shp.full_name in prj_full_names else 0

    rl_rows = pdb.select_rlpreplot("R")
    sl_rows = pdb.select_rlpreplot("S")

    layers_body = render_to_string("baseproject/partials/layers_body.html",{"layers_list":pdb.get_csv_layers()})
    header1_path = project.hdr_dir / "header1.txt"
    header2_path = project.hdr_dir / "header2.txt"
    header1_text = header1_path.read_text() if header1_path.exists() else ""
    header2_text = header2_path.read_text() if header2_path.exists() else ""
    summary = pdb.get_preplot_summary_allfiles()
    layout = pgr.preplot_map(
        src_epsg=pdb.get_main().epsg,
        show_shapes=True,  # or False
        show_layers=True,  # or False
        show_scale_bar=True,  # or False
    )
    pp_map_script,pp_map_div = components(layout)
    return render(
        request,
        "baseproject/base_settings.html",
        {
            "project": project,
            "form": form,
            "grouped": grouped,
            "rl_rows":rl_rows,
            "sl_rows": sl_rows,
            "hdr1":header1_text,
            "hdr2":header2_text,
            "folders":folders,
            "shp_list":shp_list,
            "prj_shapes":prj_shapes,
            "layers_body":layers_body,
            "sou_preplot_summary": summary.get("SLPreplot") or {},
            "rec_preplot_summary": summary.get("RLPreplot") or {},
            "pp_map_script":pp_map_script,
            "pp_map_div":pp_map_div,
        },
    )
#================================================= UPLOAD SPS FILES====================================================
@login_required
def upload_source_sps(request):
    """ view for upload source preplot from sps file"""
    if request.method != "POST":
        return JsonResponse({"error": "POST only"}, status=405)

    start = time.perf_counter()

    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)
    if not project.can_edit(request.user):
        raise PermissionDenied
    dup_mode = request.POST.get("dup_mode", "add")

    pdb = ProjectDB(project.db_path)
    pgr=PreplotGraphics(project.db_path)

    files = request.FILES.getlist("files")
    if not files:
        return JsonResponse({"error": "No files uploaded"}, status=400)
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
            point_type="S",
            dup_mode=dup_mode,
            batch_size=20000,   # можно 10k–50k
        )
        pdb.update_line_real_geometry_fast(point_type="S")
        layout = pgr.preplot_map(
            src_epsg=pdb.get_main().epsg,
            show_shapes=True,  # or False
            show_layers=True,  # or False
            show_scale_bar=True,  # or False
        )
        processed_files.append({
            "name": f.name,
            "points": result["points"],
            "lines": result["lines"],
            "skipped": result["skipped"],
        })

        total_points += result["points"]
        total_lines += result["lines"]
        rows = pdb.select_rlpreplot("S")
        elapsed = round(time.perf_counter() - start, 2)
        summary = pdb.get_preplot_summary_allfiles()
        prep_stat = render_to_string("baseproject/partials/preplot_stat_body.html",
                                    {"sou_preplot_summary": summary.get("SLPreplot") or {},
                                     "rec_preplot_summary": summary.get("RLPreplot") or {},
                                     })
    return JsonResponse({
            "status": "ok",
            "files": processed_files,
            "total_points": total_points,
            "total_lines": total_lines,
            "elapsed_sec": elapsed,
            "rows": rows,
            "point_type": "S",
            "preplot_map": json_item(layout),
            "upload_type": "SOU PREPLOT",
            "prep_stat": prep_stat,
        })
@login_required
def upload_receiver_sps(request):
    """ view for upload receiver preplot from sps file"""
    if request.method != "POST":
        return JsonResponse({"error": "POST only"}, status=405)

    start = time.perf_counter()

    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)
    if not project.can_edit(request.user):
        raise PermissionDenied
    dup_mode = request.POST.get("dup_mode", "add")

    pdb = ProjectDB(project.db_path)
    pgr = PreplotGraphics(project.db_path)

    files = request.FILES.getlist("files")
    if not files:
        return JsonResponse({"error": "No files uploaded"}, status=400)
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
        pdb.update_line_real_geometry_fast(point_type="R")
        processed_files.append({
            "name": f.name,
            "points": result["points"],
            "lines": result["lines"],
            "skipped": result["skipped"],
        })

        total_points += result["points"]
        total_lines += result["lines"]
        rows = pdb.select_rlpreplot("R")
        elapsed = round(time.perf_counter() - start, 2)
    summary = pdb.get_preplot_summary_allfiles()
    prep_stat = render_to_string("baseproject/partials/preplot_stat_body.html",
                     {"sou_preplot_summary": summary.get("SLPreplot") or {},
                              "rec_preplot_summary": summary.get("RLPreplot") or {},
                     })
    layout = pgr.preplot_map(
        src_epsg=pdb.get_main().epsg,
        show_shapes=True,  # or False
        show_layers=True,  # or False
        show_scale_bar=True,  # or False
    )


    return JsonResponse({
            "status": "ok",
            "files": processed_files,
            "total_points": total_points,
            "total_lines": total_lines,
            "elapsed_sec": elapsed,
            "rows": rows,
            "point_type":"R",
            "upload_type": "REC PREPLOT",
            "preplot_map":json_item(layout),
            "prep_stat":prep_stat
        })
@login_required
@require_POST
def upload_header_sps(request):
    # 1) file
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)
    if not project.can_edit(request.user):
        raise PermissionDenied
    pdb = ProjectDB(project.db_path)
    uploaded = request.FILES.get("files")
    if not uploaded:
        return JsonResponse({"error": "No file uploaded (field name must be 'file')"}, status=400)

    # 2) sps_revision from form
    sps_revision_raw = request.POST.get("sps_revision")
    if not sps_revision_raw:
        return JsonResponse({"error": "Missing sps_revision"}, status=400)

    try:
        sps_revision = int(sps_revision_raw)
    except ValueError:
        return JsonResponse({"error": f"Invalid sps_revision: {sps_revision_raw}"}, status=400)

    # 3) read file text (decode safely)
    raw_bytes = uploaded.read()

    encoding = pdb._detect_text_encoding(raw_bytes)
    try:
        text = raw_bytes.decode(encoding)
    except UnicodeDecodeError:
        # часто SPS бывает в latin-1/cp1252; можно поменять на cp1251 если нужно
        text = raw_bytes.decode("latin-1", errors="replace")

    # 4) choose output file
    out_name = project.hdr_dir / "header2.txt" if sps_revision == 1 else project.hdr_dir / "header1.txt"
    out_name.write_text(text,newline="\n")
    # 9) read both headers to return
    header1_path = project.hdr_dir / "header1.txt"
    header2_path = project.hdr_dir / "header2.txt"

    header1_text = header1_path.read_text() if header1_path.exists() else ""
    header2_text = header2_path.read_text() if header2_path.exists() else ""
    return JsonResponse({
        "status": "ok",
        "header1_text": header1_text,
        "header2_text": header2_text,
    })
@login_required
@require_POST
def delete_selected_receiver_lines(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)
    if not project.can_edit(request.user):
        raise PermissionDenied
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"error": "Bad JSON"}, status=400)

    ids = payload.get("ids") or []
    try:
        ids = [int(x) for x in ids]
    except Exception:
        return JsonResponse({"error": "Bad ids"}, status=400)

    if not ids:
        return JsonResponse({"error": "No ids"}, status=400)

    pdb = ProjectDB(project.db_path)
    pgr=PreplotGraphics(project.db_path)
    # 1) delete
    deleted = pdb.delete_preplot_lines(ids,"R")

    # 2) return fresh rows
    rl_rows = pdb.select_rlpreplot("R")
    summary = pdb.get_preplot_summary_allfiles()
    prep_stat = render_to_string("baseproject/partials/preplot_stat_body.html",
                                 {"sou_preplot_summary": summary.get("SLPreplot") or {},
                                  "rec_preplot_summary": summary.get("RLPreplot") or {},
                                  })
    layout = pgr.preplot_map(
        src_epsg=pdb.get_main().epsg,
        show_shapes=True,  # or False
        show_layers=True,  # or False
        show_scale_bar=True,  # or False
    )

    return JsonResponse({
        "status": "ok",
        "deleted": deleted,
        "rl_rows": rl_rows,
        "preplot_map": json_item(layout),
        "prep_stat": prep_stat
    })
@login_required
@require_POST
def delete_selected_source_lines(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)
    if not project.can_edit(request.user):
        raise PermissionDenied
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"error": "Bad JSON"}, status=400)

    ids = payload.get("ids") or []
    try:
        ids = [int(x) for x in ids]
    except Exception:
        return JsonResponse({"error": "Bad ids"}, status=400)

    if not ids:
        return JsonResponse({"error": "No ids"}, status=400)

    pdb = ProjectDB(project.db_path)
    pgr=PreplotGraphics(project.db_path)
    # 1) delete
    deleted = pdb.delete_preplot_lines(ids,"S")

    # 2) return fresh rows
    sl_rows = pdb.select_rlpreplot("S")
    summary = pdb.get_preplot_summary_allfiles()
    prep_stat = render_to_string("baseproject/partials/preplot_stat_body.html",
                                 {"sou_preplot_summary": summary.get("SLPreplot") or {},
                                  "rec_preplot_summary": summary.get("RLPreplot") or {},
                                  })
    layout = pgr.preplot_map(
        src_epsg=pdb.get_main().epsg,
        show_shapes=True,  # or False
        show_layers=True,  # or False
        show_scale_bar=True,  # or False
    )
    return JsonResponse({
        "status": "ok",
        "deleted": deleted,
        "sl_rows": sl_rows,
        "preplot_map": json_item(layout),
        "prep_stat": prep_stat
    })
def get_shape_list(folder_name)->list[ShapeFile]:
    flist = glob.glob(folder_name + "/*.shp")
    shp_list:list[ShapeFile] = []

    for f in flist:
        size_bytes = Path(f).stat().st_size
        size_kb = size_bytes / 1024
        a = ShapeFile(
            full_name=f,
            file_name=os.path.basename(f),
            file_size=size_kb
        )
        if a is not None:
           shp_list.append(a)
    return shp_list
@login_required
@require_POST
def shape_search(request):
    try:
        payload = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    value = payload.get("input_value")
    if not value:
        return JsonResponse({"error": "No input_value"}, status=400)
    shp_list=get_shape_list(value)
    return JsonResponse({
        "ok": True,
        "shp_list": shp_list,
    })
@require_POST
def add_shape_to_db(request):
    """Add shape file from the folder to project database with default values for line_width & colors"""
    try:
        payload = json.loads(request.body.decode("utf-8"))
        full_names = payload.get("full_names", [])
        if not isinstance(full_names, list) or not full_names:
            return JsonResponse({"error": "No shapes selected"}, status=400)

        # Example: your project db init (adapt to your code)
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"error": "No active project"}, status=400)
        if not project.can_edit(request.user):
            raise PermissionDenied
        pdb = ProjectDB(project.db_path)
        pgr =PreplotGraphics(project.db_path)

        upserted = 0
        for fn in full_names:
            if not isinstance(fn, str) or not fn.strip():
                continue

            shape = ProjectShape(
                full_name=fn.strip(),
                # keep defaults for style fields:
                # is_filled=0, fill_color="#000000", ...
            )
            pdb.upsert_shape(shape)  # the UPSERT you created earlier
            upserted += 1
        shp_list = get_shape_list(pdb.get_folders().shapes_folder)
        prj_shapes = pdb.get_shapes()
        prj_full_names = {s.full_name for s in prj_shapes}
        for shp in shp_list:
            shp.is_indb = 1 if shp.full_name in prj_full_names else 0
        layout = pgr.preplot_map(
            src_epsg=pdb.get_main().epsg,
            show_shapes=True,  # or False
            show_layers=True,  # or False
            show_scale_bar=True,  # or False
        )
        html = render_to_string(
            "baseproject/partials/shape_folder_rows.html",
            {"shp_list": shp_list},
            request=request,
        )
        shp_html = render_to_string("baseproject/partials/prj_shp_body.html",{"prj_shapes":prj_shapes})
        return JsonResponse({"ok": True,
                             "shapes_in_folder": html,
                             "prj_shp_body": shp_html,
                             "upserted": upserted,
                             "preplot_map": json_item(layout),})
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
@login_required
@require_POST
def project_shapes_update(request):
    try:
        data = json.loads(request.body.decode("utf-8"))

        full_name = (data.get("full_name") or "").strip()
        if not full_name:
            return JsonResponse({"error": "full_name is required"}, status=400)

        is_filled = int(data.get("is_filled", 0))
        fill_color = data.get("fill_color", "#000000")
        line_color = data.get("line_color", "#000000")
        line_width = int(data.get("line_width", 1))
        hatch_pattern = data.get("hatch_pattern","")
        line_dashed = data.get("line_dashed","")
        # active project -> your way
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"error": "No active project"}, status=400)
        if not project.can_edit(request.user):
            raise PermissionDenied
        pdb = ProjectDB(project.db_path)

        shape = ProjectShape(
            full_name=full_name,
            is_filled=is_filled,
            fill_color=fill_color,
            line_color=line_color,
            line_width=line_width,
            line_style=line_dashed,
            hatch_pattern=hatch_pattern
            # line_style=data.get("line_style", "solid"),
        )

        pdb.upsert_shape(shape)

        return JsonResponse({"ok": True})

    except (ValueError, TypeError):
        return JsonResponse({"error": "Invalid field types"}, status=400)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
@login_required
@require_POST
def project_layers_update(request):
    try:
        data = json.loads(request.body.decode("utf-8"))

        layer_id = (data.get("layer_id") or "").strip()
        if not layer_id:
            return JsonResponse({"error": "id is required"}, status=400)


        fill_color = data.get("point_color", "#000000")
        point_style = data.get("point_style", "circle")
        point_size = int(data.get("point_size", 1))
        #layer_id = int(data.get("layer_id"))
        # active project -> your way
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"error": "No active project"}, status=400)
        if not project.can_edit(request.user):
            raise PermissionDenied
        pdb = ProjectDB(project.db_path)
        pgr =PreplotGraphics(project.db_path)
        layout = pgr.preplot_map(
            src_epsg=pdb.get_main().epsg,
            show_shapes=True,  # or False
            show_layers=True,  # or False
            show_scale_bar=True,  # or False
        )
        layer = ProjectLayer(
            layer_id=layer_id,
            fill_color=fill_color,
            point_style=point_style,
            point_size=point_size,
        )
        pdb.upsert_layer(layer)
        return JsonResponse({"ok": True, "preplot_map": json_item(layout),})

    except (ValueError, TypeError):
        return JsonResponse({"error": "Invalid field types"}, status=400)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
@login_required
@require_POST
def project_shapes_delete(request):
    try:
        data = json.loads(request.body.decode("utf-8"))
        full_names = data.get("full_names", [])

        if not isinstance(full_names, list) or not full_names:
            return JsonResponse({"error": "No shapes selected"}, status=400)

        # active project -> your way
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"error": "No active project"}, status=400)
        if not project.can_edit(request.user):
            raise PermissionDenied
        # sanitize
        full_names = [str(x).strip() for x in full_names if str(x).strip()]
        if not full_names:
            return JsonResponse({"error": "No valid FullName values"}, status=400)

        pdb = ProjectDB(project.db_path)
        pgr = PreplotGraphics(project.db_path)
        deleted = pdb.delete_shapes(full_names)
        shp_list = get_shape_list(pdb.get_folders().shapes_folder)
        prj_shapes = pdb.get_shapes()
        prj_full_names = {s.full_name for s in prj_shapes}
        for shp in shp_list:
            shp.is_indb = 1 if shp.full_name in prj_full_names else 0

        html = render_to_string(
            "baseproject/partials/shape_folder_rows.html",
            {"shp_list": shp_list,},
            request=request,
        )
        shp_html = render_to_string("baseproject/partials/prj_shp_body.html", {"prj_shapes": prj_shapes})
        layout = pgr.preplot_map(
            src_epsg=pdb.get_main().epsg,
            show_shapes=True,  # or False
            show_layers=True,  # or False
            show_scale_bar=True,  # or False
        )
        return JsonResponse({"ok": True,
                             "deleted": deleted,
                             "shapes_in_folder": html,
                             "prj_shp_body": shp_html,
                             "preplot_map": json_item(layout),
                             })

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

@login_required
@require_POST
def update_shape_folder_view(request):
    try:
        data = json.loads(request.body.decode("utf-8"))
        folder = data.get("folder", "").strip()
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"error": "No active project"}, status=400)
        pdb = ProjectDB(project.db_path)
        if not folder:
            return JsonResponse({"error": "Folder path is empty"}, status=400)

        # 👉 твоя логика чтения shp файлов
        shp_list = get_shape_list(folder)
        pdb.update_shapes_folder(folder)
        prj_shapes = pdb.get_shapes()
        prj_full_names = {s.full_name for s in prj_shapes}
        for shp in shp_list:
            shp.is_indb = 1 if shp.full_name in prj_full_names else 0

        html = render_to_string(
            "baseproject/partials/shape_folder_rows.html",
            {"shp_list": shp_list},
            request=request,
        )

        return JsonResponse({"ok": True, "shapes_in_folder": html})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
#============================================================ EXPORTS=============================================
@login_required
@require_POST
def export_sol_eol_to_csv(request):
    try:
        data = json.loads(request.body.decode("utf-8"))
        point_type = data.get("point_type", "")

        if not point_type:
            return JsonResponse({"error": "No point type selected"}, status=400)

        # active project -> your way
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"error": "No active project"}, status=400)
        pdb = ProjectDB(project.db_path)
        if point_type == 'R':
            exp_file = pdb.export_sol_and_eol_to_csv(csv_path=project.export_csv / "REC_SOL_EOL.csv",
                                      point_type=point_type,)
        else:
            exp_file = pdb.export_sol_and_eol_to_csv(csv_path=project.export_csv / "SOU_SOL_EOL.csv",
                                                     point_type=point_type, )
        return JsonResponse({"message": exp_file}, status=200)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
@login_required
@require_POST
def export_to_csv(request):
    try:
        data = json.loads(request.body.decode("utf-8"))
        point_type = data.get("point_type", "")

        if not point_type:
            return JsonResponse({"error": "No point type selected"}, status=400)

        # active project
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"error": "No active project"}, status=400)
        pdb = ProjectDB(project.db_path)
        text =pdb.export_preplot_to_one_csv(point_type=point_type,out_dir=project.export_csv)

        return JsonResponse({"ok": True, "message": text}, status=200)

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
@login_required
@require_POST
def export_splited_csv(request):
    try:
        data = json.loads(request.body.decode("utf-8"))
        point_type = data.get("point_type", "")

        if not point_type:
            return JsonResponse({"error": "No point type selected"}, status=400)

        # active project -> your way
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"error": "No active project"}, status=400)
        pdb = ProjectDB(project.db_path)
        text=pdb.export_preplot_by_line(point_type=point_type,out_dir=project.export_csv)
        return JsonResponse({"ok": True, "message": text}, status=200)

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
@login_required
@require_POST
def export_gpkg(request):
    """ This function does not work so far """
    try:
        data = json.loads(request.body.decode("utf-8"))
        point_type = data.get("point_type", "")

        if not point_type:
            return JsonResponse({"error": "No point type selected"}, status=400)

        # active project -> your way
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"error": "No active project"}, status=400)
        pdb = ProjectDB(project.db_path)
        text = pdb.export_all_tierlines_to_one_gpkg (out_dir=project.export_gpkg,
                                                     point_type=point_type,
                                                     epsg=pdb.get_main().epsg,
                                                     filename=f"{point_type}LINES_PREPLOT")
        return JsonResponse({"ok": True, "message": text}, status=200)

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
@login_required
@require_POST
def export_to_shapes(request):
    try:
        data = json.loads(request.body.decode("utf-8"))
        point_type = data.get("point_type", "")

        if not point_type:
            return JsonResponse({"error": "No point type selected"}, status=400)

        # active project -> your way
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        point_dir = project.export_rpoint_shapes if point_type == 'R' else project.export_spoint_shapes
        line_dir = project.export_rline_shapes if point_type == 'R' else project.export_sline_shapes
        if not project:
            return JsonResponse({"error": "No active project"}, status=400)
        pdb = ProjectDB(project.db_path)
        text = pdb.export_preplot_to_shapes(points_dir=point_dir,
                                            lines_dir=line_dir,
                                            point_type=point_type,
                                            epsg=pdb.get_main().epsg,
                                            )
        return JsonResponse({"ok": True, "message": text}, status=200)

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
@login_required
@require_POST
def export_to_sps(request):
    try:
        data = json.loads(request.body.decode("utf-8"))
        point_type = data.get("point_type", "")

        if not point_type:
            return JsonResponse({"error": "No point type selected"}, status=400)

        # active project -> your way
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        point_dir = project.export_rpoint_shapes if point_type == 'R' else project.export_spoint_shapes
        line_dir = project.export_rline_shapes if point_type == 'R' else project.export_sline_shapes
        if not project:
            return JsonResponse({"error": "No active project"}, status=400)
        pdb = ProjectDB(project.db_path)
        text = pdb.export_splited_sps(point_type=point_type,out_dir=project.export_sps1)
        return JsonResponse({"ok": True, "message": text}, status=200)

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
#==========================================================================================================================
#====================================LOAD CSV LAYERS=====================================================================
@require_POST
def csv_headers(request):
    f = request.FILES.get("csv_file")
    if not f:
        return JsonResponse({"error": "No file"}, status=400)

    try:
        # IMPORTANT: convert uploaded bytes stream -> text stream
        text_stream = io.TextIOWrapper(f.file, encoding="utf-8-sig", newline="")

        df = pd.read_csv(
            text_stream,
            sep=None,        # auto-detect delimiter
            engine="python",
            nrows=0          # headers only
        )

    except Exception as e:
        return JsonResponse({"error": f"CSV read error: {str(e)}"}, status=400)

    headers = list(df.columns)
    if not headers:
        return JsonResponse({"error": "No headers detected"}, status=400)

    def esc(val):
        return (
            str(val)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    options = ['<option value="" disabled selected>— choose column —</option>']
    for h in headers:
        eh = esc(h)
        options.append(f'<option value="{eh}">{eh}</option>')

    return JsonResponse({
        "ok": True,
        "headers": headers,
        "options_html": "".join(options),
    })
def upload_csv_layer_ajax(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST only"}, status=405)

    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)
    if not project.can_edit(request.user):
        raise PermissionDenied
    f = request.FILES.get("csv_file")
    if not f:
        return JsonResponse({"error": "No file"}, status=400)

    layer_name = (request.POST.get("layer_name") or f.name).strip()
    comments = (request.POST.get("comments") or "").strip()

    pointfield = request.POST.get("pointfield")
    xfield = request.POST.get("xfield")
    yfield = request.POST.get("yfield")


    if not all([pointfield, xfield, yfield]):
        return JsonResponse({"error": "Missing mapping (point/x/y/z)"}, status=400)

    # optional attributes
    zfield = (request.POST.get("zfield") or "").strip() or None
    attr1_field = (request.POST.get("attr1") or "").strip() or None
    attr2_field = (request.POST.get("attr2") or "").strip() or None
    attr3_field = (request.POST.get("attr3") or "").strip() or None

    attr1_name = (request.POST.get("attr1_name") or "").strip()
    attr2_name = (request.POST.get("attr2_name") or "").strip()
    attr3_name = (request.POST.get("attr3_name") or "").strip()

    pdb = ProjectDB(project.db_path)
    pgr = PreplotGraphics(project.db_path)

    try:
        result = pdb.load_csv_layer_from_upload(
            uploaded_file=f,
            layer_name=layer_name,
            comments=comments,
            pointfield=pointfield,
            xfield=xfield,
            yfield=yfield,
            zfield=zfield,
            attr1_name=attr1_name,
            attr2_name=attr2_name,
            attr3_name=attr3_name,
            attr1_field=attr1_field,
            attr2_field=attr2_field,
            attr3_field=attr3_field,
        )
    except ValueError as e:
        return JsonResponse({"error": str(e)}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

    # render updated tbody

    layers_body = pdb.get_layers_table()
    layout = pgr.preplot_map(
        src_epsg=pdb.get_main().epsg,
        show_shapes=True,  # or False
        show_layers=True,  # or False
        show_scale_bar=True,  # or False
    )

    return JsonResponse({
        "ok": True,
        "layer_id": result["layer_id"],
        "points_inserted": result["points_inserted"],
        "layers_body": layers_body,
        "preplot_map": json_item(layout),
    })
@login_required
@require_POST
def delete_csv_layers(request):
    try:
        data = json.loads(request.body.decode("utf-8"))
        ids = data.get("ids", [])

        # validate ids
        ids = [int(x) for x in ids if str(x).isdigit()]
        if not ids:
            return JsonResponse({"ok": False, "error": "No layer IDs provided"}, status=400)

        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"ok": False, "error": "No active project"}, status=400)

        pdb = ProjectDB(project.db_path)
        pgr=PreplotGraphics(project.db_path)

        deleted = pdb.delete_csv_layers(ids)
        layout = pgr.preplot_map(
            src_epsg=pdb.get_main().epsg,
            show_shapes=True,  # or False
            show_layers=True,  # or False
            show_scale_bar=True,  # or False
        )

        return JsonResponse({"ok": True,
                             "deleted_ids": ids,
                             "deleted": deleted,
                             "preplot_map": json_item(layout),})

    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)
#==========================================================================================================================
#===================================Line CLick ===========================================================================
@login_required
@require_POST
def rl_line_click(request):
    try:
        payload = json.loads(request.body.decode("utf-8"))
        line_id = payload.get("line_id")
        tier_line = payload.get("tier_line")  # optional

        if not line_id:
            return JsonResponse({"ok": False, "error": "Missing line_id"}, status=400)

        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"ok": False, "error": "No active project"}, status=400)

        pdb = ProjectDB(project.db_path)

        line_point_list = pdb.get_preplot_points_by_line("RPPreplot", line_fk=int(line_id))

        point_table = render_to_string(
            "baseproject/partials/rl_point_list.html",
            {"point_list": line_point_list, "line_id": line_id, "tier_line": tier_line},
            request=request,
        )

        return JsonResponse({
            "ok": True,
            "line_id": int(line_id),
            "tier_line": tier_line,
            "point_table": point_table,
            "count": len(line_point_list),
        })

    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)
@login_required
@require_POST
def sl_line_click(request):
    try:
        payload = json.loads(request.body.decode("utf-8"))
        line_id = payload.get("line_id")
        tier_line = payload.get("tier_line")  # optional

        if not line_id:
            return JsonResponse({"ok": False, "error": "Missing line_id"}, status=400)

        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"ok": False, "error": "No active project"}, status=400)

        pdb = ProjectDB(project.db_path)

        line_point_list = pdb.get_preplot_points_by_line("SPPreplot", line_fk=int(line_id))

        point_table = render_to_string(
            "baseproject/partials/sl_point_list.html",
            {"point_list": line_point_list, "line_id": line_id, "tier_line": tier_line},
            request=request,
        )

        return JsonResponse({
            "ok": True,
            "line_id": int(line_id),
            "tier_line": tier_line,
            "point_table": point_table,
            "count": len(line_point_list),
        })

    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)
@login_required
@require_POST
def rp_points_delete(request):
    try:
        payload = json.loads(request.body.decode("utf-8"))
        point_ids = payload.get("point_ids", [])

        point_ids = [int(x) for x in point_ids if str(x).isdigit()]
        if not point_ids:
            return JsonResponse({"ok": False, "error": "No point_ids provided"}, status=400)

        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"ok": False, "error": "No active project"}, status=400)
        if not project.can_edit(request.user):
            raise PermissionDenied
        pdb = ProjectDB(project.db_path)
        deleted = pdb.delete_preplot_points(point_ids,table_name="SPPreplot")

        return JsonResponse({"ok": True, "deleted": deleted, "deleted_ids": point_ids})

    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)
@login_required
@require_POST
def sp_points_delete(request):
    try:
        payload = json.loads(request.body.decode("utf-8"))
        point_ids = payload.get("point_ids", [])

        point_ids = [int(x) for x in point_ids if str(x).isdigit()]
        if not point_ids:
            return JsonResponse({"ok": False, "error": "No point_ids provided"}, status=400)

        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"ok": False, "error": "No active project"}, status=400)
        if not project.can_edit(request.user):
            raise PermissionDenied
        pdb = ProjectDB(project.db_path)
        deleted = pdb.delete_preplot_points(point_ids,table_name="SPPreplot")

        return JsonResponse({"ok": True, "deleted": deleted, "deleted_ids": point_ids})

    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)
def _json_body(request):
    try:
        return json.loads(request.body.decode("utf-8"))
    except Exception:
        return None


def _get_project_or_404(project_id: int) -> Project:
    # You can replace with get_object_or_404 if you prefer
    return Project.objects.get(pk=project_id)


def _vessel_to_dict(v: Vessel) -> dict:
    return {
        "id": v.id,
        "name": v.name,
        "vessel_type": v.vessel_type or "",
        "imo": v.imo or "",
        "mmsi": v.mmsi or "",
        "call_sign": v.call_sign or "",
        "owner": v.owner or "",
        "is_active": bool(v.is_active),
        "is_retired": bool(v.is_retired),
        "notes": v.notes or "",
    }


# -------------------------
# Django (master) fleet API
# -------------------------

@require_GET
@login_required
def api_django_vessels_list(request):
    """
    Returns Django master fleet list for right table.
    Supports ?q= for name search.
    """
    q = (request.GET.get("q") or "").strip()
    qs = Vessel.objects.all()

    if q:
        qs = qs.filter(name__icontains=q)

    # Optional filters
    only_active = (request.GET.get("only_active") or "").strip().lower() in ("1", "true", "yes")
    if only_active:
        qs = qs.filter(is_active=True)

    items = [_vessel_to_dict(v) for v in qs.order_by("name")[:5000]]
    return JsonResponse({"ok": True, "items": items})


@require_POST
@login_required
def api_django_vessel_create(request):
    payload = _json_body(request)
    if not payload:
        return JsonResponse({"ok": False, "error": "Invalid JSON"}, status=400)

    name = (payload.get("name") or "").strip()
    if not name:
        return JsonResponse({"ok": False, "error": "Name is required"}, status=400)

    v = Vessel(
        name=name,
        vessel_type=(payload.get("vessel_type") or "").strip() or None,
        imo=(payload.get("imo") or "").strip() or None,
        mmsi=(payload.get("mmsi") or "").strip() or None,
        call_sign=(payload.get("call_sign") or "").strip() or None,
        owner=(payload.get("owner") or "").strip() or None,
        is_active=bool(payload.get("is_active", True)),
        is_retired=bool(payload.get("is_retired", False)),
        notes=(payload.get("notes") or "").strip() or None,
    )

    try:
        v.save()
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=400)

    return JsonResponse({"ok": True, "id": v.id})


@require_POST
@login_required
def api_django_vessel_update(request, pk: int):
    payload = _json_body(request)
    if not payload:
        return JsonResponse({"ok": False, "error": "Invalid JSON"}, status=400)

    try:
        v = Vessel.objects.get(pk=pk)
    except Vessel.DoesNotExist:
        return JsonResponse({"ok": False, "error": "Vessel not found"}, status=404)

    name = (payload.get("name") or "").strip()
    if not name:
        return JsonResponse({"ok": False, "error": "Name is required"}, status=400)

    v.name = name
    v.vessel_type = (payload.get("vessel_type") or "").strip() or None
    v.imo = (payload.get("imo") or "").strip() or None
    v.mmsi = (payload.get("mmsi") or "").strip() or None
    v.call_sign = (payload.get("call_sign") or "").strip() or None
    v.owner = (payload.get("owner") or "").strip() or None
    v.is_active = bool(payload.get("is_active", True))
    v.is_retired = bool(payload.get("is_retired", False))
    v.notes = (payload.get("notes") or "").strip() or None

    try:
        v.save()
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=400)

    return JsonResponse({"ok": True})


@require_POST
@login_required
def api_django_vessel_delete(request, pk: int):
    try:
        v = Vessel.objects.get(pk=pk)
    except Vessel.DoesNotExist:
        return JsonResponse({"ok": False, "error": "Vessel not found"}, status=404)

    v.delete()
    return JsonResponse({"ok": True})


# -----------------------------------
# ProjectDB project_fleet management
# -----------------------------------

@require_GET
@login_required
def api_project_fleet_list(request, project_id: int):
    project = _get_project_or_404(project_id)
    pdb = ProjectDB(project.db_path)

    try:
        items = pdb.list_project_fleet()
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=400)

    return JsonResponse({"ok": True, "items": items})


@require_POST
@login_required
def api_project_fleet_add_from_django(request, project_id: int):
    """
    Adds a Django vessel into ProjectDB project_fleet by vessel_id.
    Body: { "vessel_id": 123 }
    """
    payload = _json_body(request)
    if not payload or not payload.get("vessel_id"):
        return JsonResponse({"ok": False, "error": "vessel_id required"}, status=400)

    project = _get_project_or_404(project_id)
    pdb = ProjectDB(project.db_path)

    vessel_id = int(payload["vessel_id"])
    try:
        v = Vessel.objects.get(pk=vessel_id)
    except Vessel.DoesNotExist:
        return JsonResponse({"ok": False, "error": "Django vessel not found"}, status=404)

    try:
        status = pdb.add_vessel_to_project(_vessel_to_dict(v))
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=400)

    # status: "inserted" or "exists"
    return JsonResponse({"ok": True, "status": status})


@require_POST
@login_required
def api_project_fleet_remove(request, project_id: int):
    """
    Removes a project_fleet row by project_fleet_id.
    Body: { "project_fleet_id": 10 }
    """
    payload = _json_body(request)
    if not payload or not payload.get("project_fleet_id"):
        return JsonResponse({"ok": False, "error": "project_fleet_id required"}, status=400)

    project = _get_project_or_404(project_id)
    pdb = ProjectDB(project.db_path)

    try:
        deleted = pdb.remove_project_vessel(int(payload["project_fleet_id"]))
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=400)

    return JsonResponse({"ok": True, "deleted": deleted})


# -------------------------
# Optional: CSV import master fleet
# -------------------------

@require_POST
@login_required
def api_import_master_fleet_csv(request):
    """
    Imports missing vessels into Django master fleet from fleet/vessels_list.csv
    (your Update Fleet button).
    """
    try:
        result = import_vessels_from_csv_if_missing("fleet/vessels_list.csv")
        return JsonResponse({"ok": True, "result": result})
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=400)
