import io
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from urllib import request
import pickle

from bokeh.embed import json_item, components
from bokeh.layouts import column, gridplot
from bokeh.palettes import Turbo256
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import JsonResponse
from django.shortcuts import render, redirect
from django.template.loader import render_to_string
from django.views.decorators.http import require_POST, require_GET

from core.models import UserSettings
from core.projectdb import ProjectDB
from rov.dsr_map_graphics import DSRMapPlots
from rov.dsrclass import DSRDB
from rov.bbox_graphics import BlackBoxGraphics





from django.core.cache import cache




# Create your views here.
@login_required
def rov_main_view(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        # No active project → go to project list
        return redirect("projects")
    if not project.can_edit(request.user):
        raise PermissionDenied
    dsrdb = DSRDB(project.db_path)
    pdb=ProjectDB(project.db_path)
    dsrdb.pdb.update_days_in_water()
    dsr_map_plot = DSRMapPlots(project.db_path,default_epsg=dsrdb.pdb.get_main().epsg,use_tiles=True)
    plotly_template="plotly_dark" if pdb.get_main().color_scheme == "dark" else "plotly_white"
    rp_data = dsr_map_plot.read_rp_preplot()
    dsr_data = dsr_map_plot.read_dsr()
    rec_db_data = dsr_map_plot.read_recdb()
    layers = [
       dict(
            name="Deployment",
            df='dsr',
            x_col="PrimaryEasting",
            y_col="PrimaryNorthing",
            marker="circle",
            size=6,
            alpha=0.9,
            color='blue',
            # color_col="ROV",                       # categorical color mapping
            where="ROV.notna() and ROV1 != ''",  # filter: ROV not empty
        ),
        dict(
            name="Recovered Nodes",
            df='dsr',
            x_col="PrimaryEasting1",
            y_col="PrimaryNorthing1",
            marker="circle",
            size=6,
            alpha=0.9,
            color='orange',
            # color_col="ROV",                       # categorical color mapping
            where="ROV1.notna() and ROV1 != ''",  # filter: ROV not empty
        ),
        dict(
            name="Processed Nodes",
            df='rec',
            x_col="REC_X",
            y_col="REC_Y",
            marker="circle",
            size=6,
            alpha=0.9,
            color='red',
            # color_col="ROV",                       # categorical color mapping
            where=None,  # filter: ROV not empty
        ),
    ]
    progress_map = dsr_map_plot.make_map_multi_layers(
        rp_df=rp_data,  # your RPPreplot dataframe
        dsr_df=dsr_data,  # your DSR dataframe
        rec_db_df=rec_db_data,
        title="PROJECT PROGRESS MAP",
        layers=layers,
        show_preplot=True,
        show_shapes=True,
        show_tiles=True,  # if using mercator tiles
    )
    d_dep = dsr_map_plot.day_by_day_deployment(json_return=False)
    d_rec = dsr_map_plot.day_by_day_recovery(json_return=False)
    d_dep_script, d_dep_div= components(d_dep)
    d_rec_script, d_rec_div= components(d_rec)
    pp_map_script, pp_map_div = components(progress_map)
    deployment_pie = dsr_map_plot.sunburst_prod_3layers_plotly(metric="Stations",
                                                               title="Node Deployment",
                                                               labels={"total": "Deployment"},
                                                               json_return=False,
                                                               template=plotly_template)
    recovery_pie = dsr_map_plot.sunburst_prod_3layers_plotly(metric="RECStations",
                                                             title="Node Recovery",
                                                             labels={"total": "Recovery"},
                                                             json_return=False,
                                                             template=plotly_template)
    dsr_lines_body = dsrdb.render_dsr_line_summary_body()
    bbox_fields_selectors = dsrdb.get_config_selector_table()
    bbox_config_list = dsrdb.get_bbox_configs_list()
    bbox_file_tbody = dsrdb.get_bbox_file_table()
    rows = dsrdb.get_bbox_configs_list()  # you already have this
    sm_file_name = Path(project.export_csv / "sm.csv")
    dsrdb.export_dsr_to_csv(file_name=sm_file_name, sql=dsrdb.build_dsr_export_sql())
    dsr_statistics_table =dsrdb.get_dsr_html_stat()
    return render(request,
                  "rov/rovpage.html",
                  {"project": project,
                   "dsr_lines_body": dsr_lines_body,
                   "bbox_fields_selectors": bbox_fields_selectors,
                   "bbox_config_list": bbox_config_list,
                   "bbox_file_tbody": bbox_file_tbody,
                   "ok": True,
                   "configs": rows,
                   "dsr_statistics_table":dsr_statistics_table,
                   "pp_map_script":pp_map_script,
                   "pp_map_div":pp_map_div,
                   "d_dep_script":d_dep_script,
                   "d_dep_div":d_dep_div,
                   "d_rec_div":d_rec_div,
                   "d_rec_script":d_rec_script,
                   "deployment_pie":deployment_pie,
                   "recovery_pie":recovery_pie,
                   })
@require_POST
@login_required
def rov_upload_dsr(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    if not user_settings or not user_settings.active_project:
        return JsonResponse({"error": "No active project"}, status=400)

    project = user_settings.active_project

    files = request.FILES.getlist("files")

    if not files:
        return JsonResponse({"error": "No files uploaded"}, status=400)

    tier = int(request.POST.get("tier", 1))
    rec_idx = int(request.POST.get("rec_idx", 1))
    solution_name = request.POST.get("solution", "Normal")

    dsrdb = DSRDB(project.db_path)

    total_processed = 0
    total_upserted = 0
    total_skipped = 0
    result_files = []

    for f in files:
        try:

            processed, upserted,skipped = dsrdb.upsert_ip_stream(
                file_obj=f.file,
                rec_idx=rec_idx,
                tier=tier,
            )

            total_processed += processed
            total_upserted += upserted
            total_skipped += skipped

            result_files.append({
                "file": f.name,
                "processed": processed,
                "upserted": upserted,
                "skipped": skipped,
            })

        except Exception as e:
            return JsonResponse(
                {"error": str(e), "file": f.name},
                status=500,
            )
    dsr_lines_body = dsrdb.render_dsr_line_summary_body()
    dsr_statistics_table = dsrdb.get_dsr_html_stat()
    file_name = Path(project.export_csv / "dsr.csv")
    dsrdb.export_dsr_to_csv(file_name=file_name)




    return JsonResponse({
        "status": "ok",
        "tier": tier,
        "rec_idx": rec_idx,
        "solution": solution_name,
        "total_processed": total_processed,
        "total_upserted": total_upserted,
        "total_skipped": total_skipped,
        "files": result_files,
        "dsr_lines_body": dsr_lines_body,
        "dsr_statistics_table":dsr_statistics_table,
    })
@require_POST
@login_required
def rov_upload_survey_manager(request):
    """
    Upload Survey Manager files (CSV/TXT) directly from memory
    and UPDATE DSR table.
    """

    # ---- active project ----
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)

    files = request.FILES.getlist("files")
    if not files:
        return JsonResponse({"error": "No files uploaded"}, status=400)
    dsrdb=DSRDB(project.db_path)
    pdb =ProjectDB(project.db_path)
    # optional params from JS
    solution_fk = request.POST.get("solution_fk")
    solution_fk = int(solution_fk) if solution_fk and solution_fk.isdigit() else None

    default_node = request.POST.get("default_node", "NA")
    rline_mask = pdb.get_geometry().rl_mask

    results = []
    updated_total = 0

    for f in files:
        try:
            # ---- read uploaded file IN MEMORY ----
            # Django gives InMemoryUploadedFile or TemporaryUploadedFile
            # Both support .read()
            raw = f.read()

            # decode safely (SM files are usually text)
            try:
                text = raw.decode("utf-8-sig")
            except UnicodeDecodeError:
                text = raw.decode("cp1252", errors="ignore")

            # pandas can read from StringIO
            buffer = io.StringIO(text)

            # reuse your core loader by passing buffer instead of filename
            res = dsrdb.load_sm_file_to_db(f,update_key="unique")
            res["original_name"] = f.name
            results.append(res)

            if "updated_attempted" in res:
                updated_total += int(res["updated_attempted"])

        except Exception as e:
            results.append({
                "original_name": f.name,
                "error": str(e),
            })

    errors = [r for r in results if "error" in r]
    sm_file_name = Path(project.export_csv / "sm.csv")
    dsrdb.export_dsr_to_csv(file_name=sm_file_name, sql=dsrdb.build_dsr_export_sql())
    dsr_lines_body = dsrdb.render_dsr_line_summary_body()
    dsr_statistics_table=dsrdb.get_dsr_html_stat()
    if errors:
        return JsonResponse(
            {
                "error": "Some files failed",
                "results": results,
                "updated_total": updated_total,
            },
            status=400,
        )

    return JsonResponse(
        {
            "success": f"Survey Manager imported ({len(results)} file(s))",
            "results": results,
            "updated_total": updated_total,
            "dsr_lines_body": dsr_lines_body,
            "dsr_statistics_table":dsr_statistics_table,
        }
    )
@require_POST
@login_required
def rov_upload_black_box(request):
    try:
        # --- read inputs ---
        try:
            config_id = int(request.POST.get("config_id", "0") or 0)
        except ValueError:
            return JsonResponse({"error": "config_id must be integer"}, status=400)

        if not config_id:
            return JsonResponse({"error": "config_id is required"}, status=400)

        files = request.FILES.getlist("files")
        if not files:
            f1 = request.FILES.get("file")
            if f1:
                files = [f1]

        if not files:
            return JsonResponse({"error": "No files uploaded"}, status=400)

        # --- active project ---
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        project = user_settings.active_project
        if not project:
            return JsonResponse({"error": "No active project"}, status=400)

        pdb = DSRDB(project.db_path)

        # --- mapping ---
        mapping = pdb.get_bbox_config_mapping(config_id)
        if not mapping:
            return JsonResponse(
                {"error": "Selected config has no active field mapping (inUse=1)."},
                status=400
            )

        # --- import files ---
        inserted_total = 0
        processed = []

        for f in files:
            # 1) create/get file FK
            file_fk = pdb.upsert_blackbox_file(f.name, config_id)

            # 2) load CSV into BlackBox
            n = pdb.load_blackbox_csv(
                uploaded_file=f,
                mapping=mapping,
                file_fk=file_fk,
                chunk_rows=5000,
            )

            inserted_total += n
            processed.append({"file": f.name, "rows": n, "file_fk": file_fk})
        bbox_file_tbody = pdb.get_bbox_file_table()

        return JsonResponse({
            "ok": True,
            "message": f"BlackBox imported: {inserted_total} rows from {len(processed)} file(s)",
            "rows": inserted_total,
            "files": processed,
            "bbox_file_tbody": bbox_file_tbody
        })

    except Exception as e:
        # optional: add traceback in console for debugging
        import traceback
        traceback.print_exc()
        return JsonResponse({"error": str(e)}, status=500)
@require_POST
@login_required
def rov_upload_rec_db(request):
    """
    Upload FB/REC_DB (whitespace-delimited) files using the SAME modal logic:
      - JS sends: FormData { file_type, files[] }
    This view reads uploaded files directly (no temp files) and updates DSR via DSRDB.load_fb_from_file().
    """
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)

    files = request.FILES.getlist("files")
    if not files:
        return JsonResponse({"error": "No files uploaded"}, status=400)

    # optional chunk size from UI (not required)
    chunk_rows = request.POST.get("chunk_rows")
    try:
        chunk_rows = int(chunk_rows) if chunk_rows else 50000
    except ValueError:
        chunk_rows = 50000

    dsrdb = DSRDB(project.db_path)

    results = []
    total_rows_read = 0
    total_updates_attempted = 0

    for f in files:
        res = dsrdb.load_fb_from_file(f, chunk_rows=chunk_rows)
        res["original_name"] = getattr(f, "name", "")
        results.append(res)

        if "rows_read" in res:
            total_rows_read += int(res["rows_read"])
        if "updates_attempted" in res:
            total_updates_attempted += int(res["updates_attempted"])

    errors = [r for r in results if "error" in r]
    if errors:
        return JsonResponse(
            {
                "error": "Some files failed",
                "results": results,
                "rows_read_total": total_rows_read,
                "updates_attempted_total": total_updates_attempted,
            },
            status=400,
        )

    # If you want to refresh the DSR line summary table after upload:
    # (your JS can inject this HTML into the table body)
    dsr_line_body_html = dsrdb.render_dsr_line_summary_body(request=request)
    dsr_statistics_table = dsrdb.get_dsr_html_stat()
    return JsonResponse(
        {
            "success": f"REC_DB uploaded: {len(results)} file(s)",
            "results": results,
            "rows_read_total": total_rows_read,
            "updates_attempted_total": total_updates_attempted,
            "dsr_line_body_html": dsr_line_body_html,
            "dsr_statistics_table": dsr_statistics_table,
        }
    )


@require_POST
@login_required
def rov_dsr_line_click(request):
    user_settings = getattr(request.user, "usersettings", None)
    if not user_settings or not user_settings.active_project:
        return JsonResponse({"error": "No active project"}, status=400)

    line = request.POST.get("line")
    if not line:
        return JsonResponse({"error": "Missing line"}, status=400)

    try:
        line = int(line)
    except ValueError:
        return JsonResponse({"error": "Line must be integer"}, status=400)

    project = user_settings.active_project
    pdsr=DSRDB(project.db_path)
    # Optional: mark line as clicked
    pdsr.set_dsr_line_clicked(line)
    

    return JsonResponse({
        "status": "ok",
        "line": line
    })
@require_POST
@login_required
def read_bbox_headers(request):
    """This function take csv file from POST and read headers from the top and return list of them"""
    pass
@require_POST
@login_required
def save_bbox_config(request):
    try:
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        if not user_settings or not user_settings.active_project:
            return JsonResponse({"error": "No active project"}, status=400)
        project = user_settings.active_project
        if not project:
            # No active project → go to project list
            return redirect("projects")
        if not project.can_edit(request.user):
            raise PermissionDenied
        dsrd = DSRDB(project.db_path)

        cfg_name = request.POST.get("layer_name", "").strip()
        vessel_name = request.POST.get("vessel_name", "").strip()
        rov1_name = request.POST.get("rov1_name", "").strip()
        rov2_name = request.POST.get("rov2_name", "").strip()
        gnss1_name = request.POST.get("gnss1_name", "").strip()
        gnss2_name = request.POST.get("gnss2_name", "").strip()
        Depth1_name = request.POST.get("Depth1_name", "").strip()
        Depth2_name = request.POST.get("Depth2_name", "").strip()


        mapping = json.loads(request.POST.get("mapping_json", "{}"))

        if not cfg_name:
            return JsonResponse({"error": "Config Name is required"}, status=400)

        if not mapping:
            return JsonResponse({"error": "No field mapping provided"}, status=400)

        # Save config only (no CSV)
        # pdb.save_bbox_config(
        #   name=cfg_name,
        #   vessel=vessel_name,
        #   rov1=rov1_name,
        #   rov2=rov2_name,
        #   gnss1=gnss1_name,
        #   gnss2=gnss2_name,
        #   mapping=mapping,
        # )
        cfg_id = dsrd.save_bbox_config(
            name=cfg_name,
            vessel_name=vessel_name,
            rov1_name=rov1_name,
            rov2_name=rov2_name,
            gnss1_name=gnss1_name,
            gnss2_name=gnss2_name,
            depth1_name=Depth1_name,
            depth2_name=Depth2_name,
            mapping=mapping,
            is_default=False,
        )
        return JsonResponse({"ok": True, "message": "BlackBox config saved"})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
@require_POST
@login_required
def set_default_bbox_config(request):
    try:
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        if not user_settings or not user_settings.active_project:
            return JsonResponse({"error": "No active project"}, status=400)
        project = user_settings.active_project
        if not project:
            # No active project → go to project list
            return redirect("projects")
        if not project.can_edit(request.user):
            raise PermissionDenied
        id = request.POST["id"]
        dsrd = DSRDB(project.db_path)
        dsrd.set_bbox_config_default(id)
        return JsonResponse({"ok": True, "message": "BlackBox config saved"})


    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
@require_POST
@login_required
def delete_selected_dsr_lines(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)

    try:
        payload = json.loads(request.body.decode("utf-8"))
        lines = payload.get("lines", [])
        mode = payload.get("mode", "all")
    except Exception:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    if not lines:
        return JsonResponse({"error": "No lines selected"}, status=400)

    dsrdb = DSRDB(project.db_path)
    placeholders = ",".join("?" for _ in lines)

    with dsrdb._connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:

            # --------------------------------------------------
            # DELETE → ALL (hard delete)
            # --------------------------------------------------
            if mode == "all":
                conn.execute(
                    f"DELETE FROM DSR WHERE Line IN ({placeholders})",
                    lines,
                )

            # --------------------------------------------------
            # DELETE → REC DB (RESET fields)
            # --------------------------------------------------
            elif mode == "recdb":
                conn.execute(
                    f"DELETE FROM REC_DB WHERE Line IN ({placeholders})",
                    lines,
                )
            # --------------------------------------------------
            # DELETE → SM (RESET fields)
            # --------------------------------------------------
            elif mode == "sm":
                SM_NULL_COLS = [
                    "Area", "RemoteUnit", "AUQRCode", "AURFID",
                    "CUSerialNumber", "Status", "DeploymentType",

                    "StartTimeEpoch", "StartTimeUTC",
                    "DeployTimeEpoch", "DeployTimeUTC",
                    "PickupTimeEpoch", "PickupTimeUTC",
                    "StopTimeEpoch", "StopTimeUTC",

                    "SPSX", "SPSY", "SPSZ",
                    "ActualX", "ActualY", "ActualZ",

                    "Deployed", "PickedUp", "Archived",
                    "DeviceID", "BinID",

                    "ExpectedTraces", "CollectedTraces",
                    "DownloadedDatainMB", "ExpectedDatainMB",
                    "DownloadError",
                ]

                set_null = ", ".join(f"{c}=NULL" for c in SM_NULL_COLS)

                conn.execute(
                    f"""
                    UPDATE DSR
                    SET {set_null}
                    WHERE Line IN ({placeholders})
                    """,
                    lines,
                )

            conn.commit()

        except Exception as e:
            conn.rollback()
            return JsonResponse({"error": str(e)}, status=500)

    return JsonResponse({
        "success": f"'{mode}' operation applied to {len(lines)} line(s)",
        "lines": lines,
        "mode": mode,
    })
@login_required
@require_POST
def delete_bbox_files(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)
    try:
        payload = json.loads(request.body)
        ids = payload.get("ids", [])

        if not ids:
            return JsonResponse({"ok": False, "error": "No IDs"})

        placeholders = ",".join("?" for _ in ids)
        dsrdb=DSRDB(project.db_path)
        with dsrdb._connect() as conn:
            cursor=conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            # (optional but recommended for sqlite FK cascade)
            cursor.execute("PRAGMA foreign_keys = ON;")
            # delete files
            cursor.execute(
                f"DELETE FROM main.BlackBox_Files WHERE ID IN ({placeholders})",
                ids,
            )
            dsrdb._connect().commit()
        bbox_file_tbody = dsrdb.get_bbox_file_table()
        return JsonResponse({"ok": True,
                             "deleted": len(ids),
                             "bbox_file_tbody": bbox_file_tbody})

    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)
@require_POST
@login_required
def bbox_file_selected(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)

    try:
        payload = json.loads(request.body or "{}")
        file_id = int(payload.get("file_id") or 0)
        file_name = payload.get("file_name") or ""

        if not file_id and not file_name:
            return JsonResponse({"ok": False, "error": "No file_id / file_name"}, status=400)

        bbgr = BlackBoxGraphics(project.db_path)

        # ---- 1) load file meta / labels (light query)
        # (kept same logic as you already have)
        file_details = bbgr.get_bbox_config_names_by_filename(file_name) if file_name else {}

        # ---- 2) ONE heavy query for ALL plots
        # Use file_name (your existing workflow uses filename)
        data = bbgr.load_bbox_data(
            file_name=file_name if file_name else None,
            file_ids=[file_id] if (not file_name and file_id) else None,
            # columns=None -> loads the shared common package for many plots
            # you can also pass start_ts/end_ts here later if you add UI time filters
        )
        dsr_df = bbgr.dsr_points_in_bbox_timeframe(data)
        # ---- 3) build plots from same dataframe
        gnss_plot = bbgr.bokeh_gnss_qc_timeseries(
            title="GNSS QC",
            gnss1_label=file_details.get("gnss1_name"),
            gnss2_label=file_details.get("gnss2_name"),
            is_show=False,
            data=data,
        )

        rovs_depths_plot = bbgr.bokeh_bbox_depth12_diff_timeseries(df=data, diff_threshold=10, plot_kind="vbar", is_show=False)
        vessel_sog = bbgr.bokeh_bbox_sog_timeseries(df=data,plot_kind="line",is_show=False)
        hdop_plot = bbgr.bokeh_bbox_gnss_hdop_timeseries(df=data,is_show=False,return_json=False)
        cog_vs_hdg_plot = bbgr.boke_cog_hdg_timeseries_all(df=data,is_show=False)

        return JsonResponse({
            "ok": True,
            "gnss_qc_plot": json_item(gnss_plot),
            "rovs_depths_plot": json_item(rovs_depths_plot),
            "vessel_sog": json_item(vessel_sog),
            "hdop_plot":json_item(hdop_plot),
            "cog_vs_hdg_plot": json_item(cog_vs_hdg_plot),
        })

    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)

@require_GET
@login_required
def bbox_configs_list(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"ok": False, "error": "No active project"}, status=400)

    dsrdb = DSRDB(project.db_path)
    rows = dsrdb.get_bbox_configs_list()  # you already have this

    # For datalist we only need names (and maybe default marker)
    return JsonResponse({
        "ok": True,
        "configs": rows,  # each: {id,name,is_default,rov1_name,...}
    })
@require_GET
@login_required
def bbox_config_detail(request, config_id: int):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"ok": False, "error": "No active project"}, status=400)

    dsrdb = DSRDB(project.db_path)

    # header fields (from list)
    all_cfgs = dsrdb.get_bbox_configs_list()
    cfg = next((c for c in all_cfgs if int(c["id"]) == int(config_id)), None)
    if not cfg:
        return JsonResponse({"ok": False, "error": "Config not found"}, status=404)

    # mapping rows (FieldName -> FileColumn)
    mapping = dsrdb.get_bbox_config_mapping(config_id)

    return JsonResponse({
        "ok": True,
        "config": cfg,
        "mapping": mapping,  # {"VesselEasting":"IP E ...", ...}
    })
@require_POST
@login_required
def read_bbox_headers(request):
    """
    Read CSV headers from uploaded BlackBox CSV (in memory).
    Returns headers + <option> HTML for mapping selects.
    """
    f = request.FILES.get("csv_file")
    if not f:
        return JsonResponse({"ok": False, "error": "No CSV file provided"}, status=400)

    try:
        raw = f.read()
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = raw.decode("cp1252", errors="ignore")

        # detect separator from first line
        first_line = text.splitlines()[0] if text else ""
        if "," in first_line and first_line.count(",") >= first_line.count("\t"):
            sep = ","
        elif "\t" in first_line:
            sep = "\t"
        else:
            sep = None  # auto / whitespace

        from io import StringIO
        import pandas as pd

        df = pd.read_csv(
            StringIO(text),
            sep=sep,
            nrows=0,
            engine="python",
        )

        headers = [str(c).strip() for c in df.columns if str(c).strip()]
        if not headers:
            return JsonResponse({"ok": False, "error": "No headers found"}, status=400)

        options = ['<option value="">— Select column —</option>']
        options += [f'<option value="{h}">{h}</option>' for h in headers]

        return JsonResponse({
            "ok": True,
            "headers": headers,
            "options_html": "\n".join(options),
        })

    except Exception as e:
        return JsonResponse(
            {"ok": False, "error": f"Failed to read CSV headers: {e}"},
            status=500,
        )
@require_POST
@login_required
def dsr_export_sm(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"ok": False, "error": "No active project"}, status=400)

    dsrdb = DSRDB(project.db_path)

    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"ok": False, "error": "Invalid JSON"}, status=400)

    mode = (payload.get("mode") or "day").strip().lower()
    status = (payload.get("status") or "deployed").strip().lower()
    depth_mode = (payload.get("depth_mode") or "neg").strip().lower()
    fmt = (payload.get("format") or "mass_nodes").strip().lower()
    rovs = payload.get("rovs") or []

    if status not in ("deployed", "recovered"):
        return JsonResponse({"ok": False, "error": "Invalid status"}, status=400)
    if fmt not in ("z_nodes", "mass_nodes"):
        return JsonResponse({"ok": False, "error": "Invalid format"}, status=400)
    if depth_mode not in ("neg", "abs"):
        return JsonResponse({"ok": False, "error": "Invalid depth_mode"}, status=400)
    if not isinstance(rovs, list) or not rovs:
        return JsonResponse({"ok": False, "error": "Select at least one ROV"}, status=400)

    export_type = 0 if status == "deployed" else 1
    export_abs = 1 if depth_mode == "abs" else 0
    zexp = 1 if fmt == "z_nodes" else 0

    sm_folder = project.export_sm
    if not sm_folder:
        return JsonResponse({"ok": False, "error": "SM folder not configured"}, status=400)

    # Build day range + optional ts range
    ts_from = None
    ts_to = None

    if mode == "day":
        first_day = (payload.get("day") or "").strip()
        if not first_day:
            return JsonResponse({"ok": False, "error": "Missing day"}, status=400)
        last_day = None
    else:
        dt_from = (payload.get("from") or "").strip()
        dt_to = (payload.get("to") or "").strip()
        if not dt_from or not dt_to:
            return JsonResponse({"ok": False, "error": "Missing from/to"}, status=400)

        # interval timestamps: "YYYY-MM-DDTHH:MM" -> "YYYY-MM-DD HH:MM:SS"
        def _norm_dt(s: str) -> str:
            s = s.replace("T", " ")
            if len(s) == 16:
                s += ":00"
            return s

        ts_from = _norm_dt(dt_from)
        ts_to = _norm_dt(dt_to)

        # keep these for fallback/naming (not strictly required)
        first_day = dt_from[:10]
        last_day = dt_to[:10]

    result = dsrdb.export_dsr_to_sm(
        first_day=first_day,
        last_day=last_day,
        rovs=rovs,
        export_type=export_type,
        export_abs=export_abs,
        zexp=zexp,
        output_dir=sm_folder,
        mark_exported=True,
        ts_from=ts_from,
        ts_to=ts_to,
    )

    if "error" in result:
        return JsonResponse({"ok": False, "error": result["error"]}, status=400)

    return JsonResponse({
        "ok": True,
        "message": "DSR successfully exported to SM format.",
        "file": result.get("success"),
        "filename": result.get("filename"),
        "rows": int(result.get("rows", 0)),
    })



# rov/views.py

@require_POST
def dsr_rovs_for_timeframe(request):
    """
    POST JSON:
        {
            "mode": "day" | "interval",
            "day": "YYYY-MM-DD",              # required if mode=day
            "from": "YYYY-MM-DDTHH:MM",       # required if mode=interval
            "to":   "YYYY-MM-DDTHH:MM"
        }

    Returns:
        {
            "rovs": ["ROV1", "ROV2"],
            "count": 2
        }
    """
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    mode = payload.get("mode", "day")
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"ok": False, "error": "No active project"}, status=400)
    try:
        # Get current project DB path (adapt to your project structure)
        dsrdb = DSRDB(project.db_path)

        if mode == "day":
            day = payload.get("day")
            rovs = dsrdb.get_rovs_for_timeframe(
                mode="day",
                day=day,
            )
        else:
            dt_from = payload.get("from")
            dt_to = payload.get("to")
            rovs = dsrdb.get_rovs_for_timeframe(
                mode="interval",
                dt_from=dt_from,
                dt_to=dt_to,
            )

        return JsonResponse({
            "rovs": rovs,
            "count": len(rovs),
        })

    except ValueError as e:
        # raised from DSRDB validation
        return JsonResponse({"error": str(e)}, status=400)

    except Exception as e:
        return JsonResponse(
            {"error": f"Failed to load ROV list: {str(e)}"},
            status=500
        )
@require_POST
@login_required
def select_prod_day(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"ok": False, "error": "No active project"}, status=400)
    try:
        payload = json.loads(request.body.decode("utf-8"))
        day = (payload.get("day") or "").strip()
        if not day:
            return JsonResponse({"error": "Missing day"}, status=400)
    except Exception:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    dsrdb = DSRDB(project.db_path)
    deploy_rows = dsrdb.get_daily_recovery(date=day,view_name="Daily_Deployment")
    rec_rows = dsrdb.get_daily_recovery(date=day,view_name="Daily_Recovery")
    html = render_to_string("rov/partials/daily_production_tables.html",
                     {"deploy_rows": deploy_rows, "rec_rows": rec_rows})
    return JsonResponse({
        "html": html,
        "deploy_count": len(deploy_rows),
        "recovery_count": len(rec_rows),
    })
@require_POST
@login_required
def export_dsr_to_sps (request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project

    if not project:
        return JsonResponse({"ok": False, "error": "No active project"}, status=400)
    dsrdb = DSRDB(project.db_path)
    pdb = ProjectDB(project.db_path)
    try:
        selected_lines = json.loads(request.POST.get("selected_lines", "[]"))

        if not selected_lines:
            return JsonResponse({"ok": False, "message": "No lines selected."}, status=400)

        export_header = request.POST.get("export_header") in ("1", "true", "on")
        use_seq = request.POST.get("use_seq") in ("1", "true", "on")
        use_line_seq = request.POST.get("use_line_seq") in ("1", "true", "on")
        use_line_fn = request.POST.get("use_line_fn") in ("1", "true", "on")
        seq = (request.POST.get("seq") or "01").strip()
        pcode = (request.POST.get("pcode") or "R1").strip()
        rov_export = request.POST.get("rov_export")  # "0" / "1" / "2"
        sps_ver = request.POST.get("sps_ver")  # "1" / "2"
        how_exp = request.POST.get("how_exp")  # "1" / "2"

        # TODO: run your export logic here (write files on disk / DB record / etc.)
        export_dir = project.export_sps1 if sps_ver == "1" else project.export_sps21
        header_file_path = f"{project.hdr_dir}/header1.txt" if sps_ver == "1" else f"{project.hdr_dir}/header2.txt"
        if rov_export == "0":
           export_dir =f"{export_dir}/dep/"
        if rov_export == "1":
            export_dir = f"{export_dir}/rec/"
        if rov_export == "2":
            export_dir = f"{export_dir}/fb/"

        dsrdb.export_dsr_lines_to_sps(
            export_dir = export_dir,
            selected_lines=selected_lines,
            header_file_path=header_file_path,
            export_header=export_header,
            pcode=pcode,
            sps_format=sps_ver,
            kind=rov_export,
            use_seq=use_seq,
            use_line_seq=use_line_seq,
            seq=seq,
            how_exp=how_exp,
            line_code=pdb.get_main().line_code,
            use_line_code=use_line_fn,
        )
        # Example output:
        created_files = [f"SPS_{line}_{seq}.txt" for line in selected_lines]

        return JsonResponse({
            "ok": True,
            "message": f"Exported {len(selected_lines)} line(s) to SPS.",
            "files": created_files,
            "meta": {
                "export_header": export_header,
                "use_seq": use_seq,
                "use_line_seq": use_line_seq,
                "seq": seq,
                "pcode": pcode,
                "rov_export": rov_export,
                "sps_ver": sps_ver,
                "how_exp": how_exp,
            }
        })

    except Exception as e:
        return JsonResponse({"ok": False, "message": str(e)}, status=500)
@require_POST
@login_required
def dsr_line_onclick (request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"ok": False, "error": "No active project"}, status=400)
    return JsonResponse({"ok":"ok"})
@require_POST
@login_required
def load_battery_life_map(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
       return JsonResponse({"ok": False, "error": "No active project"}, status=400)
    pdb=ProjectDB(project.db_path)
    pdb.update_days_in_water()

    map_plot = DSRMapPlots(project.db_path,default_epsg=pdb.get_main().epsg)
    rp_data = map_plot.read_rp_preplot()
    dsr_data = map_plot.read_dsr()
    rec_db_data = map_plot.read_recdb()
    layers=[
        dict(
            name="Battery Life",
            df='dsr',
            x_col="PrimaryEasting",
            y_col="PrimaryNorthing",
            marker="circle",
            size=6,
            alpha=0.9,
            color_col="TodayDaysInWater",  # categorical color mapping
            bins=8,  # number of bins
            bin_method="equal",  # "equal" or "quantile"
            include_lowest=True,
            palette="Turbo256",
            where="ROV1.isna() or ROV1.str.strip() == ''",  # filter: ROV is empty
        ),
    ]
    bl_map = progress_map = map_plot.make_map_multi_layers(
        rp_df=rp_data,  # your RPPreplot dataframe
        dsr_df=dsr_data,  # your DSR dataframe
        rec_db_df=rec_db_data,
        title="BATTERY LIFE MAP",
        layers=layers,
        show_preplot=True,
        show_shapes=True,
        show_tiles=True,  # if using mercator tiles
    )
    bl_json_map = json_item(bl_map)
    return JsonResponse({"ok": True, "map": bl_json_map})
@require_POST
@login_required
def load_battery_rest_days_map(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
       return JsonResponse({"ok": False, "error": "No active project"}, status=400)
    data = json.loads(request.body)

    max_days = int(data.get("max_days_in_water", 130))
    bins_number = int(data.get("bins_number", 8))
    pdb=ProjectDB(project.db_path)
    pdb.update_days_in_water()


    map_plot = DSRMapPlots(project.db_path,default_epsg=pdb.get_main().epsg)
    rp_data = map_plot.read_rp_preplot()
    dsr_data = map_plot.read_dsr()
    dsr_data['RemDays'] = max_days - dsr_data["TodayDaysInWater"]
    rec_db_data = map_plot.read_recdb()
    layers=[
        dict(
            name="Remaining battery life (days).",
            df='dsr',
            x_col="PrimaryEasting",
            y_col="PrimaryNorthing",
            marker="circle",
            size=6,
            alpha=0.9,
            color_col='RemDays',  # categorical color mapping
            bins=bins_number,  # number of bins
            bin_method="equal",  # "equal" or "quantile"
            include_lowest=True,
            palette="Turbo256",
            where="ROV1.isna() or ROV1.str.strip() == ''",  # filter: ROV is empty
        ),
    ]
    bl_map = progress_map = map_plot.make_map_multi_layers(
        rp_df=rp_data,  # your RPPreplot dataframe
        dsr_df=dsr_data,  # your DSR dataframe
        rec_db_df=rec_db_data,
        title="Remaining battery life (days).",
        layers=layers,
        show_preplot=True,
        show_shapes=True,
        show_tiles=True,  # if using mercator tiles
    )
    bl_json_map = json_item(bl_map)
    return JsonResponse({"ok": True, "map": bl_json_map})
@login_required
@require_POST
def load_dsr_historgram (request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"ok": False, "error": "No active project"}, status=400)
    payload = json.loads(request.body.decode("utf-8"))
    bins = int(payload.get("bins", 40))
    max_offset = int(payload.get("max_offset", 150))
    kde = bool(payload.get("kde", True))
    std = bool(payload.get("std", True))
    is_show = bool(payload.get("is_show", True))

    pdb=ProjectDB(project.db_path)
    dsr_plot = DSRMapPlots(project.db_path, default_epsg=pdb.get_main().epsg, use_tiles=True)
    rp_data = dsr_plot.read_rp_preplot()
    dsr_data = dsr_plot.read_dsr()
    dsr_data = dsr_plot.add_inline_xline_offsets(
        dsr_data, rp_data,
        from_xy=("PreplotEasting", "PreplotNorthing"),
        to_xy=("PrimaryEasting", "PrimaryNorthing"),
        out_prefix="Pri"
    )
    dsr_data = dsr_plot.add_inline_xline_offsets(
        dsr_data, rp_data,
        from_xy=("PreplotEasting", "PreplotNorthing"),
        to_xy=("SecondaryEasting", "SecondaryNorthing"),
        out_prefix="Pri"
    )
    hist = dsr_plot.build_offsets_histograms_by_rov(
        dsr_data,
        rov_col="ROV",
        inline_col="PriOffInline",
        xline_col="PriOffXline",
        radial_col="RangetoPrePlot",
        bins=bins,
        show_mean_line=True,
        title_prefix="Offsets",
        is_show=False,
        json_import=False,
        target_id="dsr_offsets_hist",
        max_offset=max_offset,
    )

    return JsonResponse({"ok": True, "hist": json_item(hist)})
@require_POST
@login_required
def load_min_max_line_qc(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
       return JsonResponse({"ok": False, "error": "No active project"}, status=400)
    pdb=ProjectDB(project.db_path)
    pdb.update_days_in_water()

    map_plot = DSRMapPlots(project.db_path,default_epsg=pdb.get_main().epsg)
    line_sum = map_plot.read_line_summary()
    line_qc_plot = map_plot.build_line_summary_qc_grid(df=line_sum,json_export=False,is_show=False)
    return JsonResponse({"ok": True, "line_qc_plot": json_item(line_qc_plot)})

@require_POST
def bbox_plot_item(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"ok": False, "error": "No active project"}, status=400)

    try:
        payload = json.loads(request.body or "{}")

        file_id = int(payload.get("file_id") or 0)
        file_name = (payload.get("file_name") or "").strip()
        plot_key = (payload.get("plot_key") or "").strip()

        if not plot_key:
            return JsonResponse({"ok": False, "error": "Missing plot_key"}, status=400)

        if not file_id and not file_name:
            return JsonResponse({"ok": False, "error": "No file_id / file_name"}, status=400)

        bbgr = BlackBoxGraphics(project.db_path)

        # light meta (optional per request)
        file_details = bbgr.get_bbox_config_names_by_filename(file_name) if file_name else {}

        # ---- OPTIONAL: cache the heavy dataframe so 5 plot calls don't re-load it 5 times
        # If you don't want cache yet, just call bbgr.load_bbox_data(...) directly.
        cache_key = f"bbox_df:{project.id}:{file_name or file_id}"
        data = cache.get(cache_key)
        if data is None:
            data = bbgr.load_bbox_data(
                file_name=file_name if file_name else None,
                file_ids=[file_id] if (not file_name and file_id) else None,
            )
            # store pickled df in cache (works well with filesystem/redis cache)
            cache.set(cache_key, pickle.dumps(data), timeout=15 * 60)
        else:
            data = pickle.loads(data)

        # ---- build ONLY requested plot
        if plot_key == "gnss_qc":
            fig = bbgr.bokeh_gnss_qc_timeseries(
                title="GNSS QC",
                gnss1_label=file_details.get("gnss1_name"),
                gnss2_label=file_details.get("gnss2_name"),
                is_show=False,
                data=data,
            )
        elif plot_key == "rovs_depths":
            fig = bbgr.bokeh_bbox_depth12_diff_timeseries(
                df=data, diff_threshold=10, plot_kind="vbar", is_show=False
            )
        elif plot_key == "vessel_sog":
            fig = bbgr.bokeh_bbox_sog_timeseries(df=data, plot_kind="line", is_show=False)
        elif plot_key == "hdop":
            fig = bbgr.bokeh_bbox_gnss_hdop_timeseries(df=data, is_show=False, return_json=False)
        elif plot_key == "cog_vs_hdg":
            fig = bbgr.boke_cog_hdg_timeseries_all(df=data, is_show=False)
        else:
            return JsonResponse({"ok": False, "error": f"Unknown plot_key: {plot_key}"}, status=400)

        return JsonResponse({
            "ok": True,
            "plot_key": plot_key,
            "item": json_item(fig),
        })

    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)
