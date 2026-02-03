import glob
import io
import os.path
import time
import json
from pathlib import Path

import pandas as pd
from django.shortcuts import render

# Create your views here.
# baseproject/views.py
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import render, redirect
from django.views.decorators.http import require_POST
from dataclasses import dataclass, field

from pygments.lexer import default

from core.models import UserSettings, SPSRevision
from core.models import Project
from core.projectdb import ProjectDB
from core.projectshp import ProjectShape
from baseproject.preplot_graphics import PreplotGraphics
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
    preplot_map = pgr.preplot_map(src_epsg=pdb.get_main().epsg)
    preplot_map = pgr.add_project_shapes_layers(preplot_map,default_src_epsg=pdb.get_main().epsg)
    pp_map_script,pp_map_div = components(preplot_map)
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
    file_type = request.POST["file_type"]
    if file_type == "SRC_SPS":
       point_code ="S"
    elif file_type == "REC_SPS":
       point_code = "R"

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
            point_type=point_code,
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
    if point_code == "R":
        rows = pdb.select_rlpreplot(point_code)
        upload_type=1 # Uploaded receviers
    elif point_code == "S":
        rows = pdb.select_rlpreplot(point_code)
        upload_type = 2  # Uploaded receviers
    else:
        rows = []
        upload_type = 3  # Uploaded receviers



    elapsed = round(time.perf_counter() - start, 2)

    return JsonResponse({
        "status": "ok",
        "files": processed_files,
        "total_points": total_points,
        "total_lines": total_lines,
        "elapsed_sec": elapsed,
        "rows": rows,
        "upload_type": upload_type,
    })
def upload_source_sps(request):
    """ view for upload source preplot from sps file"""
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

    return JsonResponse({
            "status": "ok",
            "files": processed_files,
            "total_points": total_points,
            "total_lines": total_lines,
            "elapsed_sec": elapsed,
            "rows": rows,
            "point_type": "S",
            "upload_type": "SOU PREPLOT",
        })


def upload_receiver_sps(request):
    """ view for upload receiver preplot from sps file"""
    if request.method != "POST":
        return JsonResponse({"error": "POST only"}, status=405)

    start = time.perf_counter()

    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)

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
    preplot_map = pgr.preplot_map(src_epsg=pdb.get_main().epsg)
    preplot_map = pgr.add_project_shapes_layers(preplot_map, default_src_epsg=pdb.get_main().epsg)


    return JsonResponse({
            "status": "ok",
            "files": processed_files,
            "total_points": total_points,
            "total_lines": total_lines,
            "elapsed_sec": elapsed,
            "rows": rows,
            "point_type":"R",
            "upload_type": "REC PREPLOT",
            "preplot_map":json_item(preplot_map),
            "prep_stat":prep_stat
        })

@login_required
@require_POST
def upload_header_sps(request):
    # 1) file
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
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
    out_name = project.hdr_dir / "header1.txt" if sps_revision == 1 else project.hdr_dir / "header2.txt"
    out_name.write_text(text)
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
    preplot_map = pgr.preplot_map(src_epsg=pdb.get_main().epsg)
    preplot_map = pgr.add_project_shapes_layers(preplot_map, default_src_epsg=pdb.get_main().epsg)

    return JsonResponse({
        "status": "ok",
        "deleted": deleted,
        "rl_rows": rl_rows,
        "preplot_map": json_item(preplot_map),
        "prep_stat": prep_stat
    })
@login_required
@require_POST
def delete_selected_source_lines(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)

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
    preplot_map = pgr.preplot_map(src_epsg=pdb.get_main().epsg)
    preplot_map = pgr.add_project_shapes_layers(preplot_map, default_src_epsg=pdb.get_main().epsg)
    return JsonResponse({
        "status": "ok",
        "deleted": deleted,
        "sl_rows": sl_rows,
        "preplot_map": json_item(preplot_map),
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

        pdb = ProjectDB(project.db_path)

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

        return JsonResponse({"ok": True, "upserted": upserted})

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

        # active project -> your way
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"error": "No active project"}, status=400)

        pdb = ProjectDB(project.db_path)

        shape = ProjectShape(
            full_name=full_name,
            is_filled=is_filled,
            fill_color=fill_color,
            line_color=line_color,
            line_width=line_width,
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

        # sanitize
        full_names = [str(x).strip() for x in full_names if str(x).strip()]
        if not full_names:
            return JsonResponse({"error": "No valid FullName values"}, status=400)

        pdb = ProjectDB(project.db_path)
        deleted = pdb.delete_shapes(full_names)

        return JsonResponse({"ok": True, "deleted": deleted})

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
        prj_shapes = pdb.get_shapes()
        prj_full_names = {s.full_name for s in prj_shapes}
        for shp in shp_list:
            shp.is_indb = 1 if shp.full_name in prj_full_names else 0

        html = render_to_string(
            "partials/shape_folder_rows.html",
            {"shp_list": shp_list},
            request=request,
        )

        return JsonResponse({"ok": True, "html": html})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
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

    f = request.FILES.get("csv_file")
    if not f:
        return JsonResponse({"error": "No file"}, status=400)

    layer_name = (request.POST.get("layer_name") or f.name).strip()
    comments = (request.POST.get("comments") or "").strip()

    pointfield = request.POST.get("pointfield")
    xfield = request.POST.get("xfield")
    yfield = request.POST.get("yfield")
    zfield = request.POST.get("zfield")

    if not all([pointfield, xfield, yfield, zfield]):
        return JsonResponse({"error": "Missing mapping (point/x/y/z)"}, status=400)

    # optional attributes
    attr1_field = (request.POST.get("attr1") or "").strip() or None
    attr2_field = (request.POST.get("attr2") or "").strip() or None
    attr3_field = (request.POST.get("attr3") or "").strip() or None

    attr1_name = (request.POST.get("attr1_name") or "").strip()
    attr2_name = (request.POST.get("attr2_name") or "").strip()
    attr3_name = (request.POST.get("attr3_name") or "").strip()

    pdb = ProjectDB(project.db_path)

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
    layers_body = render_to_string(
        "baseproject/partials/layers_body.html",
        {"layer_list": pdb.get_csv_layers()},
        request=request
    )

    return JsonResponse({
        "ok": True,
        "layer_id": result["layer_id"],
        "points_inserted": result["points_inserted"],
        "layers_body": layers_body,
    })


