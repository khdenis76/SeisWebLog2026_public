import io
import json
import os

from bokeh.embed import json_item
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import JsonResponse
from django.shortcuts import render, redirect
from django.views.decorators.http import require_POST

from core.models import UserSettings
from core.projectdb import ProjectDB
from rov.dsrclass import DSRDB
from rov.bbox_graphics import BlackBoxGraphics



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
    dsr_lines_body = dsrdb.render_dsr_line_summary_body()
    bbox_fields_selectors = dsrdb.get_config_selector_table()
    bbox_config_list = dsrdb.get_bbox_configs_list()
    bbox_file_tbody = dsrdb.get_bbox_file_table()
    return render(request,
                  "rov/rovpage.html",
                  {"project": project,
                   "dsr_lines_body": dsr_lines_body,
                   "bbox_fields_selectors": bbox_fields_selectors,
                   "bbox_config_list": bbox_config_list,
                   "bbox_file_tbody": bbox_file_tbody})
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
    result_files = []

    for f in files:
        try:

            processed, upserted = dsrdb.upsert_ip_stream(
                file_obj=f.file,
                rec_idx=rec_idx,
                tier=tier,
            )

            total_processed += processed
            total_upserted += upserted

            result_files.append({
                "file": f.name,
                "processed": processed,
                "upserted": upserted,
            })

        except Exception as e:
            return JsonResponse(
                {"error": str(e), "file": f.name},
                status=500,
            )
    dsr_lines_body = dsrdb.render_dsr_line_summary_body()
    return JsonResponse({
        "status": "ok",
        "tier": tier,
        "rec_idx": rec_idx,
        "solution": solution_name,
        "total_processed": total_processed,
        "total_upserted": total_upserted,
        "files": result_files,
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
            res = dsrdb.load_sm_file_to_db(f,update_key="linepointidx")
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

    return JsonResponse(
        {
            "success": f"REC_DB uploaded: {len(results)} file(s)",
            "results": results,
            "rows_read_total": total_rows_read,
            "updates_attempted_total": total_updates_attempted,
            "dsr_line_body_html": dsr_line_body_html,
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
            rov1_name=rov1_name,
            rov2_name=rov2_name,
            gnss1_name=gnss1_name,
            gnss2_name=gnss2_name,
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
                RECDB_NULL_COLS = [
                    "REC_ID", "NODE_ID",
                    "RPRE_X", "RPRE_Y",
                    "RFIELD_X", "RFIELD_Y", "RFIELD_Z",
                    "REC_X", "REC_Y", "REC_Z",
                    "TIMECORR", "BULKSHFT",
                    "QDRIFT", "LDRIFT",
                    "TRIMPTCH", "TRIMROLL", "TRIMYAW",
                    "PITCHFIN", "ROLLFIN", "YAWFIN",
                ]

                RECDB_ZERO_COLS = [
                    "DEPLOY", "RPI", "PART_NO",
                    "TOTDAYS", "RECCOUNT", "CLKFLAG",
                    "EC1_RUS0", "EC1_RUS1",
                    "EC1_EDT0", "EC1_EDT1",
                    "EC1_EPT0", "EC1_EPT1",
                    "NODSTART",
                    "DEPLOYTM", "PICKUPTM", "RUNTIME",
                    "EC2_CD1", "TOTSHOTS", "TOTPROD",
                    "SPSK", "TIER",
                ]

                set_null = ", ".join(f"{c}=NULL" for c in RECDB_NULL_COLS)
                set_zero = ", ".join(f"{c}=0" for c in RECDB_ZERO_COLS)

                conn.execute(
                    f"""
                    UPDATE DSR
                    SET {set_null}, {set_zero}
                    WHERE Line IN ({placeholders})
                    """,
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
def bbox_file_selected(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project
    if not project:
        return JsonResponse({"error": "No active project"}, status=400)
    try:
        payload = json.loads(request.body or "{}")
        file_id = int(payload.get("file_id"))
        file_name = payload.get("file_name")
        if not file_id:
            return JsonResponse({"ok": False, "error": "No IDs"})
        bbgr = BlackBoxGraphics(project.db_path)
        file_details = bbgr.get_bbox_config_names_by_filename(file_name)
        layout = bbgr.bokeh_gnss_qc_timeseries(file_name=file_name,
                                      gnss1_label=file_details['gnss1_name'],
                                      gnss2_label=file_details['gnss2_name'],is_show=False)
        # build your bokeh figure p = figure(...)
        # p = make_gnss_qc_plot(file_id)

        #item = json_item(layout, "gnss-qc-plot")  # target id

        return JsonResponse({"ok": True, "gnss_qc_plot": json_item(layout)})
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)

