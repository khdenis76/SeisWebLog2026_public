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
    from bokeh.models import CheckboxGroup, CustomJS, HoverTool, LinearAxis, Range1d
    from bokeh.plotting import figure
    from bokeh.resources import CDN

    points = profile.get("points") or []
    series_defs = [
        ("velocity_mps", "Sound Velocity", "m/s", "#d62728"),
        ("salinity_psu", "Salinity", "PSU", "#1f77b4"),
        ("temperature_c", "Temperature", "°C", "#ff7f0e"),
        ("conductivity_mscm", "Conductivity", "mS/cm", "#2ca02c"),
        ("density_kgm3", "Density", "kg/m³", "#9467bd"),
    ]

    valid_depths = [float(p["depth_m"]) for p in points if p.get("depth_m") is not None]
    if not valid_depths:
        raise ValueError("Profile has no valid depth points.")
    min_depth, max_depth = min(valid_depths), max(valid_depths)
    if max_depth <= min_depth:
        max_depth = min_depth + 1

    available = []
    for field, label, unit, color in series_defs:
        xs, ys = [], []
        for row in points:
            value, depth = row.get(field), row.get("depth_m")
            if value is not None and depth is not None:
                xs.append(float(value))
                ys.append(float(depth))
        if xs:
            available.append((field, label, unit, color, xs, ys))
    if not available:
        raise ValueError("Profile has no plottable SVP measurements.")

    first = available[0]
    x_min, x_max = min(first[4]), max(first[4])
    pad = max((x_max - x_min) * .08, .01)
    fig = figure(
        height=650,
        sizing_mode="stretch_width",
        x_axis_location="above",
        y_range=Range1d(max_depth, min_depth),
        x_range=Range1d(x_min - pad, x_max + pad),
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
        toolbar_location="right",
        title=profile.get("name") or "SVP Profile",
    )
    fig.yaxis.axis_label = "Depth (m)"
    fig.xaxis.axis_label = f"{first[1]} ({first[2]})"
    fig.min_border_left = 70
    fig.min_border_top = 80
    fig.grid.grid_line_alpha = .3

    renderers = []
    axes = []
    for idx, (field, label, unit, color, xs, ys) in enumerate(available):
        range_name = "default" if idx == 0 else field
        if idx == 0:
            axis = fig.xaxis[0]
            axis.axis_line_color = color
            axis.major_label_text_color = color
            axis.axis_label_text_color = color
        else:
            lo, hi = min(xs), max(xs)
            axis_pad = max((hi - lo) * .08, .01)
            fig.extra_x_ranges[field] = Range1d(lo - axis_pad, hi + axis_pad)
            axis = LinearAxis(
                x_range_name=field,
                axis_label=f"{label} ({unit})",
                axis_line_color=color,
                major_label_text_color=color,
                axis_label_text_color=color,
            )
            fig.add_layout(axis, "above")

        renderer = fig.line(
            xs,
            ys,
            line_width=2,
            color=color,
            legend_label=label,
            x_range_name=range_name,
        )
        fig.add_tools(HoverTool(
            renderers=[renderer],
            tooltips=[("Series", label), (label, "@$x{0.000}"), ("Depth", "@$y{0.00} m")],
            mode="mouse",
        ))
        renderers.append(renderer)
        axes.append(axis)

    selector = CheckboxGroup(
        labels=[f"{label} ({unit})" for _, label, unit, _, _, _ in available],
        active=list(range(len(available))),
        inline=True,
    )
    selector.js_on_change("active", CustomJS(
        args={"renderers": renderers, "axes": axes},
        code="""
            const active = new Set(cb_obj.active);
            for (let i = 0; i < renderers.length; i++) {
                const visible = active.has(i);
                renderers[i].visible = visible;
                axes[i].visible = visible;
            }
        """,
    ))

    fig.legend.location = "bottom_right"
    fig.legend.click_policy = "hide"
    layout = column(selector, fig, sizing_mode="stretch_width")
    return file_html(layout, CDN, profile.get("name") or "SVP Profile")


def _build_svp_map_html(map_data: dict) -> str:
    from bokeh.embed import file_html
    from bokeh.models import ColumnDataSource, HoverTool
    from bokeh.plotting import figure
    from bokeh.resources import CDN

    fig = figure(
        height=690,
        sizing_mode="stretch_width",
        match_aspect=True,
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
        title="SVP Positions above RP Preplot",
        x_axis_label="Easting (X)",
        y_axis_label="Northing (Y)",
    )

    preplot = map_data.get("preplot") or []
    if preplot:
        src = ColumnDataSource({
            "x": [r.get("x") for r in preplot],
            "y": [r.get("y") for r in preplot],
            "line": [str(r.get("line") or "") for r in preplot],
            "point": [str(r.get("point") or "") for r in preplot],
        })
        renderer = fig.scatter(
            "x", "y", source=src, marker="circle", size=4,
            color="#9aa0a6", alpha=.55, legend_label="RP Preplot",
        )
        fig.add_tools(HoverTool(
            renderers=[renderer],
            tooltips=[
                ("Layer", "RP Preplot"),
                ("Line", "@line"),
                ("Point", "@point"),
                ("X", "@x{0.00}"),
                ("Y", "@y{0.00}"),
            ],
        ))

    svp_rows = map_data.get("svp") or []
    if svp_rows:
        src = ColumnDataSource({
            "x": [r.get("coord_e") for r in svp_rows],
            "y": [r.get("coord_n") for r in svp_rows],
            "name": [r.get("name") or "" for r in svp_rows],
            "rov": [r.get("rov") or "" for r in svp_rows],
            "date": [r.get("timestamp") or "" for r in svp_rows],
            "depth": [r.get("bottom_depth") for r in svp_rows],
        })
        renderer = fig.scatter(
            "x", "y", source=src, marker="diamond", size=13,
            color="#dc3545", line_color="white", line_width=1.5,
            legend_label="SVP",
        )
        fig.add_tools(HoverTool(
            renderers=[renderer],
            tooltips=[
                ("Name", "@name"),
                ("ROV", "@rov"),
                ("Date", "@date"),
                ("Depth", "@depth{0.0} m"),
                ("X", "@x{0.00}"),
                ("Y", "@y{0.00}"),
            ],
        ))

    fig.legend.click_policy = "hide"
    fig.grid.grid_line_alpha = .25
    return file_html(fig, CDN, "SVP Map")



def _interpolate_at_depth(depths: list[float], values: list[float], target: float):
    """Linear interpolation without extrapolation."""
    from bisect import bisect_left

    if not depths or target < depths[0] or target > depths[-1]:
        return None
    idx = bisect_left(depths, target)
    if idx < len(depths) and depths[idx] == target:
        return values[idx]
    if idx == 0 or idx >= len(depths):
        return None
    d0, d1 = depths[idx - 1], depths[idx]
    v0, v1 = values[idx - 1], values[idx]
    if d1 == d0:
        return v0
    fraction = (target - d0) / (d1 - d0)
    return v0 + fraction * (v1 - v0)


def _build_svp_statistics_html(profiles: list[dict]) -> str:
    """
    Plot every SVP profile in gray and place the depth-binned average curves
    above them. Each measurement keeps its own horizontal range while sharing
    the common vertical depth axis.
    """
    from bokeh.embed import file_html
    from bokeh.layouts import column
    from bokeh.models import CheckboxGroup, CustomJS, HoverTool, LinearAxis, Range1d
    from bokeh.plotting import figure
    from bokeh.resources import CDN

    series_defs = [
        ("velocity_mps", "Sound Velocity", "m/s", "#d62728"),
        ("salinity_psu", "Salinity", "PSU", "#1f77b4"),
        ("temperature_c", "Temperature", "°C", "#ff7f0e"),
        ("conductivity_mscm", "Conductivity", "mS/cm", "#2ca02c"),
        ("density_kgm3", "Density", "kg/m³", "#9467bd"),
    ]

    prepared = []
    global_max_depth = 0.0
    for profile in profiles:
        points = profile.get("points") or []
        series = {}
        for field, _label, _unit, _color in series_defs:
            pairs = []
            for row in points:
                try:
                    depth = float(row.get("depth_m"))
                    value = float(row.get(field))
                except (TypeError, ValueError):
                    continue
                pairs.append((depth, value))
            pairs.sort(key=lambda item: item[0])
            # Collapse duplicate depth samples so interpolation remains stable.
            dedup = {}
            for depth, value in pairs:
                dedup[depth] = value
            if dedup:
                depths = sorted(dedup)
                values = [dedup[d] for d in depths]
                series[field] = (depths, values)
                global_max_depth = max(global_max_depth, depths[-1])
        if series:
            prepared.append({"name": profile.get("name") or f"Profile {profile.get('id', '')}", "series": series})

    if not prepared:
        raise ValueError("No SVP profiles with plottable measurements were found.")

    # One metre average grid. At each depth, only profiles that cover that depth
    # participate; values outside a profile's measured range are never extrapolated.
    max_grid_depth = int(global_max_depth)
    depth_grid = [float(i) for i in range(max_grid_depth + 1)]

    available = []
    for field, label, unit, color in series_defs:
        all_values = []
        avg_depths, avg_values, avg_counts = [], [], []
        for depth in depth_grid:
            samples = []
            for profile in prepared:
                pair = profile["series"].get(field)
                if not pair:
                    continue
                value = _interpolate_at_depth(pair[0], pair[1], depth)
                if value is not None:
                    samples.append(value)
            if samples:
                avg_depths.append(depth)
                avg_values.append(sum(samples) / len(samples))
                avg_counts.append(len(samples))
                all_values.extend(samples)

        if avg_values:
            # Include original samples in range calculation so gray profiles fit.
            for profile in prepared:
                pair = profile["series"].get(field)
                if pair:
                    all_values.extend(pair[1])
            available.append((field, label, unit, color, avg_depths, avg_values, avg_counts, all_values))

    if not available:
        raise ValueError("No common SVP measurement series were found.")

    first = available[0]
    lo, hi = min(first[7]), max(first[7])
    pad = max((hi - lo) * 0.08, 0.01)
    fig = figure(
        height=690,
        sizing_mode="stretch_width",
        x_axis_location="above",
        y_range=Range1d(global_max_depth, 0),
        x_range=Range1d(lo - pad, hi + pad),
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
        toolbar_location="right",
        title=f"Average SVP Curves ({len(prepared)} profiles)",
    )
    fig.yaxis.axis_label = "Depth (m)"
    fig.xaxis.axis_label = f"{first[1]} ({first[2]})"
    fig.min_border_left = 70
    fig.min_border_top = 90
    fig.grid.grid_line_alpha = 0.25

    renderer_groups = []
    axes = []
    for idx, (field, label, unit, color, avg_depths, avg_values, avg_counts, all_values) in enumerate(available):
        range_name = "default" if idx == 0 else field
        if idx == 0:
            axis = fig.xaxis[0]
        else:
            lo, hi = min(all_values), max(all_values)
            axis_pad = max((hi - lo) * 0.08, 0.01)
            fig.extra_x_ranges[field] = Range1d(lo - axis_pad, hi + axis_pad)
            axis = LinearAxis(x_range_name=field, axis_label=f"{label} ({unit})")
            fig.add_layout(axis, "above")
        axis.axis_line_color = color
        axis.major_label_text_color = color
        axis.axis_label_text_color = color

        group = []
        # Draw every individual cast first so the average is visually on top.
        for profile in prepared:
            pair = profile["series"].get(field)
            if not pair:
                continue
            gray = fig.line(
                pair[1], pair[0],
                x_range_name=range_name,
                line_color="#9aa0a6",
                line_alpha=0.30,
                line_width=1,
                muted_alpha=0.05,
            )
            group.append(gray)

        average = fig.line(
            avg_values,
            avg_depths,
            x_range_name=range_name,
            line_color=color,
            line_width=4,
            line_alpha=1.0,
            legend_label=f"Average {label}",
        )
        fig.add_tools(HoverTool(
            renderers=[average],
            tooltips=[
                ("Series", f"Average {label}"),
                (label, "@$x{0.000}"),
                ("Depth", "@$y{0.0} m"),
            ],
            mode="mouse",
        ))
        group.append(average)
        renderer_groups.append(group)
        axes.append(axis)

    selector = CheckboxGroup(
        labels=[f"{label} ({unit})" for _, label, unit, _, _, _, _, _ in available],
        active=list(range(len(available))),
        inline=True,
    )
    selector.js_on_change("active", CustomJS(
        args={"renderer_groups": renderer_groups, "axes": axes},
        code="""
            const active = new Set(cb_obj.active);
            for (let i = 0; i < renderer_groups.length; i++) {
                const visible = active.has(i);
                for (const renderer of renderer_groups[i]) renderer.visible = visible;
                axes[i].visible = visible;
            }
        """,
    ))

    fig.legend.location = "bottom_right"
    fig.legend.click_policy = "hide"
    layout = column(selector, fig, sizing_mode="stretch_width")
    return file_html(layout, CDN, "SVP Aggregate Statistics")


@login_required
@require_GET
def svp_api_statistics(request):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response
    try:
        profiles = []
        for row in svp.list_profiles():
            item = svp.get_full_profile(int(row["id"]))
            if item:
                profiles.append(item)
        return JsonResponse({"success": True, "html": _build_svp_statistics_html(profiles)})
    except Exception as exc:
        return JsonResponse({"success": False, "error": str(exc)}, status=400)

@login_required
@require_GET
def svp_api_map(request):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response
    try:
        return JsonResponse({"success": True, "html": _build_svp_map_html(svp.get_map_data())})
    except Exception as exc:
        return JsonResponse({"success": False, "error": str(exc)}, status=400)


@login_required
@require_POST
def svp_api_upload(request):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    custom_name = (request.POST.get("name") or "").strip() or None
    notes = (request.POST.get("notes") or "").strip() or None

    try:
        config_id_raw = (request.POST.get("config_id") or "").strip()
        config_id = int(config_id_raw) if config_id_raw else None

        # New batch mode: one multi-file input named svp_files.
        # Every .000 becomes one profile. Matching .svp is optional.
        batch_files = request.FILES.getlist("svp_files")
        if batch_files:
            result = svp.import_uploaded_batch(
                files=batch_files,
                name=custom_name,
                notes=notes,
                config_id=config_id,
                rov=request.POST.get("rov"),
                coord_e=request.POST.get("coord_e"),
                coord_n=request.POST.get("coord_n"),
                instrument_model=request.POST.get("instrument_model"),
            )

            message = f"Imported {result.get('imported_count', 0)} SVP profile(s)."
            if result.get("failed_count", 0):
                message += f" Failed: {result.get('failed_count', 0)}."
            if result.get("missing_svp_count", 0):
                message += f" Raw profiles without matching metadata .svp: {result.get('missing_svp_count', 0)}."

            status_code = 200 if result.get("failed_count", 0) == 0 else 400
            return JsonResponse({
                "success": result.get("failed_count", 0) == 0,
                "message": message,
                **result,
            }, status=status_code)

        # Backward compatible single-pair mode.
        file_000 = request.FILES.get("file_000")
        file_svp = request.FILES.get("file_svp")

        if file_000:
            svp_id = svp.import_uploaded_profile(
                file_000_obj=file_000,
                file_svp_obj=file_svp,
                name=custom_name,
                notes=notes,
                config_id=config_id,
                rov=request.POST.get("rov"),
                coord_e=request.POST.get("coord_e"),
                coord_n=request.POST.get("coord_n"),
                instrument_model=request.POST.get("instrument_model"),
            )
        elif file_svp:
            svp_id = svp.import_standalone_svp(
                file_svp,
                name=custom_name,
                notes=notes,
                config_id=config_id,
                rov=request.POST.get("rov"),
                coord_e=request.POST.get("coord_e"),
                coord_n=request.POST.get("coord_n"),
                instrument_model=request.POST.get("instrument_model"),
            )
        else:
            raise ValueError("Please select at least one .000 or .svp file.")

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
