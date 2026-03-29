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
@require_POST
def svp_api_upload(request):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    uploaded_file = request.FILES.get("file")
    if not uploaded_file:
        return JsonResponse({"success": False, "error": "No file uploaded."}, status=400)

    config_id = request.POST.get("config_id")
    if not config_id:
        return JsonResponse({"success": False, "error": "No config selected."}, status=400)

    custom_name = (request.POST.get("name") or "").strip() or None
    notes = (request.POST.get("notes") or "").strip() or None

    try:
        svp_id = svp.import_uploaded_file(
            uploaded_file,
            file_name=getattr(uploaded_file, "name", None),
            name=custom_name,
            notes=notes,
            config_id=int(config_id),
        )

        return JsonResponse({
            "success": True,
            "message": "SVP file uploaded successfully.",
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

        return JsonResponse({
            "success": True,
            "message": "Config saved.",
            "config_id": config_id,
        })
    except Exception as exc:
        return JsonResponse({
            "success": False,
            "error": str(exc),
        }, status=400)
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

        # extract meta keys
        meta_keys = []
        for ln in header_lines:
            if "=" in ln:
                key = ln.replace("[", "").replace("]", "").split("=")[0].strip()
                meta_keys.append(key)
            elif "\t" in ln:
                key = ln.split("\t")[0].strip()
                meta_keys.append(key)

        # extract columns
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
        return JsonResponse({
            "success": False,
            "error": str(exc),
        }, status=400)
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
        return JsonResponse({
            "success": True,
            "message": "Config imported successfully.",
            "config_id": config_id,
        })
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
        return JsonResponse({
            "success": True,
            "rows": rows,
        })
    except Exception as exc:
        return JsonResponse({
            "success": False,
            "error": str(exc),
        }, status=400)
@login_required
@require_GET
def svp_api_config_get(request, config_id: int):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    try:
        cfg = svp.get_format_config(config_id)
        if not cfg:
            return JsonResponse({
                "success": False,
                "error": f"Config id={config_id} not found.",
            }, status=404)

        return JsonResponse({
            "success": True,
            "config": cfg,
        })
    except Exception as exc:
        return JsonResponse({
            "success": False,
            "error": str(exc),
        }, status=400)


@login_required
@require_POST
def svp_api_config_delete(request, config_id: int):
    project, svp, error_response = _get_svp_data(request)
    if error_response:
        return error_response

    try:
        cfg = svp.get_format_config(config_id)
        if not cfg:
            return JsonResponse({
                "success": False,
                "error": f"Config id={config_id} not found.",
            }, status=404)

        svp.delete_format_config(config_id)

        return JsonResponse({
            "success": True,
            "message": "Config deleted.",
            "config_id": config_id,
        })
    except Exception as exc:
        return JsonResponse({
            "success": False,
            "error": str(exc),
        }, status=400)

@login_required
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
        return JsonResponse({
            "success": False,
            "error": str(exc),
        }, status=400)


