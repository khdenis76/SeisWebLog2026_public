from __future__ import annotations

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import JsonResponse, HttpResponse
from django.shortcuts import redirect, render
from django.views.decorators.http import require_GET, require_POST

from core.models import UserSettings
from .services.svp_data import SVPData


def _get_active_project(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project

    if not project:
        return None, JsonResponse({"success": False, "error": "No active project selected."}, status=400)

    if hasattr(project, "can_view") and not project.can_view(request.user):
        raise PermissionDenied("You are not a member of this project.")

    if not project.db_path:
        return None, JsonResponse({"success": False, "error": "Project database path is empty."}, status=400)

    return project, None


def _get_svp_data(request):
    project, error_response = _get_active_project(request)
    if error_response:
        return None, None, error_response

    svp = SVPData(project.db_path)
    svp.ensure_tables()
    return project, svp, None


@login_required
def svp_home(request):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return redirect("projects")

    return render(request, "svp/svp_home.html", {"project": project})


@login_required
@require_GET
def svp_api_list(request):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    return JsonResponse({
        "success": True,
        "rows": svp.list_profiles(),
    })


@login_required
@require_GET
def svp_api_details(request, profile_id: int):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    item = svp.get_full_profile(profile_id)
    if not item:
        return JsonResponse({
            "success": False,
            "error": f"SVP profile id={profile_id} not found.",
        }, status=404)

    profile = item.copy()
    points = profile.pop("points", [])

    return JsonResponse({
        "success": True,
        "profile": profile,
        "points": points,
    })


@login_required
@require_GET
def svp_api_plot(request, profile_id: int):
    """
    Return a complete Bokeh HTML document for one SVP profile.
    The frontend loads it inside an iframe, so it does not depend on
    BokehJS already being loaded in base.html.
    """
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    item = svp.get_full_profile(profile_id)
    if not item:
        return JsonResponse({
            "success": False,
            "error": f"SVP profile id={profile_id} not found.",
        }, status=404)

    try:
        html = _build_svp_bokeh_html(item)
        return JsonResponse({"success": True, "html": html})
    except Exception as exc:
        return JsonResponse({"success": False, "error": str(exc)}, status=400)


def _build_svp_bokeh_html(profile: dict) -> str:
    from bokeh.embed import file_html
    from bokeh.layouts import column
    from bokeh.models import HoverTool, LinearAxis, Range1d, Title
    from bokeh.plotting import figure
    from bokeh.resources import CDN

    points = profile.get("points") or []

    depth = []
    velocity = []
    salinity_depth = []
    salinity = []

    for p in points:
        d = p.get("depth_m")
        v = p.get("velocity_mps")
        s = p.get("salinity_psu")
        if d is None:
            continue
        if v is not None:
            depth.append(float(d))
            velocity.append(float(v))
        if s is not None:
            salinity_depth.append(float(d))
            salinity.append(float(s))

    if not depth or not velocity:
        raise ValueError("Profile has no valid depth / velocity points.")

    min_depth = min(depth)
    max_depth = max(depth)
    if max_depth <= min_depth:
        max_depth = min_depth + 1

    min_vel = min(velocity)
    max_vel = max(velocity)
    vel_pad = max((max_vel - min_vel) * 0.08, 1.0)

    p = figure(
        height=680,
        sizing_mode="stretch_width",
        x_axis_location="above",
        y_range=Range1d(max_depth, min_depth),
        x_range=Range1d(min_vel - vel_pad, max_vel + vel_pad),
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
        toolbar_location="right",
        title=profile.get("name") or "SVP Profile",
    )

    p.xaxis.axis_label = "Sound Velocity (m/s)"
    p.yaxis.axis_label = "Depth (m)"
    p.ygrid.grid_line_alpha = 0.45
    p.xgrid.grid_line_alpha = 0.45
    p.min_border_left = 60
    p.min_border_right = 70
    p.min_border_top = 90

    p.line(velocity, depth, line_width=2, color="red", legend_label="Sound Velocity")

    if salinity:
        min_sal = min(salinity)
        max_sal = max(salinity)
        sal_pad = max((max_sal - min_sal) * 0.08, 0.05)
        p.extra_x_ranges = {
            "salinity": Range1d(min_sal - sal_pad, max_sal + sal_pad)
        }
        p.add_layout(LinearAxis(x_range_name="salinity", axis_label="Salinity (PSU)"), "below")
        p.line(
            salinity,
            salinity_depth,
            line_width=2,
            color="blue",
            alpha=0.85,
            x_range_name="salinity",
            legend_label="Salinity",
        )

    hover = HoverTool(
        tooltips=[
            ("Depth", "@$y{0.00} m"),
            ("X", "@$x{0.000}"),
        ],
        mode="mouse",
    )
    p.add_tools(hover)
    p.legend.location = "bottom_right"
    p.legend.click_policy = "hide"

    meta_left = (
        f"Date/Time: {profile.get('timestamp') or ''}    "
        f"Location: {profile.get('location') or ''}    "
        f"ROV: {profile.get('rov') or ''}    "
        f"Instrument: {profile.get('instrument_model') or ''}"
    )
    meta_right = (
        f"Coordinates: E {profile.get('coord_e') or ''}, N {profile.get('coord_n') or ''}    "
        f"Bottom Depth: {profile.get('bottom_depth') or ''}    "
        f"Mean Velocity: {profile.get('mean_velocity') or ''}"
    )
    p.add_layout(Title(text=meta_left, text_font_size="10pt"), "above")
    p.add_layout(Title(text=meta_right, text_font_size="10pt"), "above")

    layout = column(p, sizing_mode="stretch_width")
    return file_html(layout, CDN, profile.get("name") or "SVP Profile")


@login_required
@require_POST
def svp_api_upload(request):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    custom_name = (request.POST.get("name") or "").strip() or None
    notes = (request.POST.get("notes") or "").strip() or None

    try:
        config_id = request.POST.get("config_id")
        if not config_id:
            return JsonResponse({
                "success": False,
                "error": "No .000 config selected.",
            }, status=400)

        # New batch mode: one multi-file input named svp_files.
        # Every .000 becomes one profile. Matching .svp is optional.
        batch_files = request.FILES.getlist("svp_files")
        if batch_files:
            result = svp.import_uploaded_batch(
                files=batch_files,
                name=custom_name,
                notes=notes,
                config_id=int(config_id),
                rov=request.POST.get("rov"),
                coord_e=request.POST.get("coord_e"),
                coord_n=request.POST.get("coord_n"),
                instrument_model=request.POST.get("instrument_model"),
            )

            message = (
                f"Imported {result.get('imported_count', 0)} SVP profile(s). "
                f"Missing .svp: {result.get('missing_svp_count', 0)}. "
                f"Unmatched .svp: {result.get('unmatched_svp_count', 0)}."
            )

            status_code = 200 if result.get("failed_count", 0) == 0 else 400
            return JsonResponse({
                "success": result.get("failed_count", 0) == 0,
                "message": message,
                **result,
            }, status=status_code)

        # Backward compatible single-pair mode.
        file_000 = request.FILES.get("file_000")
        file_svp = request.FILES.get("file_svp")

        svp_id = svp.import_uploaded_profile(
            file_000_obj=file_000,
            file_svp_obj=file_svp,
            name=custom_name,
            notes=notes,
            config_id=int(config_id),
            rov=request.POST.get("rov"),
            coord_e=request.POST.get("coord_e"),
            coord_n=request.POST.get("coord_n"),
            instrument_model=request.POST.get("instrument_model"),
        )

        return JsonResponse({
            "success": True,
            "message": "SVP profile imported successfully.",
            "svp_id": svp_id,
            "profile": svp.get_profile(svp_id),
        })

    except Exception as exc:
        return JsonResponse({
            "success": False,
            "error": str(exc),
        }, status=400)


@login_required
@require_POST
def svp_api_delete(request, profile_id: int):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    profile = svp.get_profile(profile_id)
    if not profile:
        return JsonResponse({
            "success": False,
            "error": f"SVP profile id={profile_id} not found.",
        }, status=404)

    try:
        svp.delete_profile(profile_id)
        return JsonResponse({
            "success": True,
            "message": "SVP profile deleted.",
            "svp_id": profile_id,
        })
    except Exception as exc:
        return JsonResponse({
            "success": False,
            "error": str(exc),
        }, status=400)


@login_required
@require_POST
def svp_api_delete_selected(request):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    ids_raw = request.POST.getlist("ids[]") or request.POST.getlist("ids")
    ids = []
    for value in ids_raw:
        try:
            ids.append(int(value))
        except Exception:
            pass

    if not ids:
        return JsonResponse({"success": False, "error": "No SVP profiles selected."}, status=400)

    try:
        for profile_id in ids:
            svp.delete_profile(profile_id)
        return JsonResponse({
            "success": True,
            "message": f"Deleted {len(ids)} SVP profile(s).",
            "deleted_ids": ids,
        })
    except Exception as exc:
        return JsonResponse({"success": False, "error": str(exc)}, status=400)


@login_required
@require_POST
def svp_api_config_save(request):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    payload = {
        "config_name": request.POST.get("config_name"),
        "file_ext": request.POST.get("file_ext"),
        "delimiter": request.POST.get("delimiter"),
        "header_line_count": request.POST.get("header_line_count"),
        "data_header_line_index": request.POST.get("data_header_line_index"),
        "data_start_line_index": request.POST.get("data_start_line_index"),
        "meta_coordinates_key": request.POST.get("meta_coordinates_key"),
        "meta_lat_key": request.POST.get("meta_lat_key"),
        "meta_lon_key": request.POST.get("meta_lon_key"),
        "meta_rov_key": request.POST.get("meta_rov_key"),
        "meta_timestamp_key": request.POST.get("meta_timestamp_key"),
        "meta_name_key": request.POST.get("meta_name_key"),
        "meta_location_key": request.POST.get("meta_location_key"),
        "meta_serial_key": request.POST.get("meta_serial_key"),
        "meta_make_key": request.POST.get("meta_make_key"),
        "meta_model_key": request.POST.get("meta_model_key"),
        "col_timestamp": request.POST.get("col_timestamp"),
        "col_depth": request.POST.get("col_depth"),
        "col_velocity": request.POST.get("col_velocity"),
        "col_temperature": request.POST.get("col_temperature"),
        "col_salinity": request.POST.get("col_salinity"),
        "col_density": request.POST.get("col_density"),
        "sort_by_depth": bool(request.POST.get("sort_by_depth")),
        "clamp_negative_depth_to_zero": bool(request.POST.get("clamp_negative_depth_to_zero")),
        "pressure_is_depth": bool(request.POST.get("pressure_is_depth")),
        "notes": request.POST.get("notes"),
    }

    try:
        config_id = svp.save_format_config(payload)
        return JsonResponse({"success": True, "message": "Config saved.", "config_id": config_id})
    except Exception as exc:
        return JsonResponse({"success": False, "error": str(exc)}, status=400)


@login_required
@require_POST
def svp_api_config_preview(request):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    f = request.FILES.get("file")
    if not f:
        return JsonResponse({"success": False, "error": "No file uploaded"}, status=400)

    try:
        raw = f.read()
        text = raw.decode("utf-8", errors="ignore")

        from .services.svp_parser import SVPParser

        setup = SVPParser.detect_setup(text, f.name)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        header_lines = lines[: setup.header_line_count or 0]

        meta_keys = []
        for ln in header_lines:
            if "=" in ln:
                key = ln.replace("[", "").replace("]", "").split("=")[0].strip()
                meta_keys.append(key)
            elif "\t" in ln:
                key = ln.split("\t")[0].strip()
                meta_keys.append(key)

        columns = []
        if setup.data_header_line_index is not None:
            header_line = lines[setup.data_header_line_index]
            delimiter = setup.delimiter or ","
            columns = [c.strip() for c in header_line.split(delimiter)]

        return JsonResponse({
            "success": True,
            "meta_keys": sorted(set(meta_keys)),
            "columns": columns,
            "detected": setup.to_dict(),
        })
    except Exception as exc:
        return JsonResponse({"success": False, "error": str(exc)}, status=400)


@login_required
@require_GET
def svp_api_config_export(request, config_id: int):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    try:
        json_text = svp.export_format_config_to_json(config_id)
        cfg = svp.get_format_config(config_id) or {}
        file_name = (cfg.get("name") or f"svp_config_{config_id}").replace(" ", "_")
        response = HttpResponse(json_text, content_type="application/json")
        response["Content-Disposition"] = f'attachment; filename="{file_name}.json"'
        return response
    except Exception as exc:
        return JsonResponse({"success": False, "error": str(exc)}, status=400)


@login_required
@require_POST
def svp_api_config_import(request):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    f = request.FILES.get("file")
    if not f:
        return JsonResponse({"success": False, "error": "No JSON file uploaded."}, status=400)

    try:
        config_id = svp.import_format_config_uploaded_file(f)
        return JsonResponse({"success": True, "message": "Config imported successfully.", "config_id": config_id})
    except Exception as exc:
        return JsonResponse({"success": False, "error": str(exc)}, status=400)


@login_required
@require_GET
def svp_api_config_list(request):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    try:
        rows = svp.list_format_configs()
        return JsonResponse({"success": True, "rows": rows})
    except Exception as exc:
        return JsonResponse({"success": False, "error": str(exc)}, status=400)


@login_required
@require_GET
def svp_api_config_get(request, config_id: int):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    try:
        cfg = svp.get_format_config(config_id)
        if not cfg:
            return JsonResponse({"success": False, "error": f"Config id={config_id} not found."}, status=404)
        return JsonResponse({"success": True, "config": cfg})
    except Exception as exc:
        return JsonResponse({"success": False, "error": str(exc)}, status=400)


@login_required
@require_POST
def svp_api_config_delete(request, config_id: int):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    try:
        cfg = svp.get_format_config(config_id)
        if not cfg:
            return JsonResponse({"success": False, "error": f"Config id={config_id} not found."}, status=404)

        svp.delete_format_config(config_id)
        return JsonResponse({"success": True, "message": "Config deleted.", "config_id": config_id})
    except Exception as exc:
        return JsonResponse({"success": False, "error": str(exc)}, status=400)
