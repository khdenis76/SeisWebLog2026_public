import json

from django.core.exceptions import PermissionDenied
from django.http import JsonResponse
from django.shortcuts import render, get_object_or_404, redirect
from django.template.loader import render_to_string
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.http import require_GET, require_POST
from django.contrib.auth.decorators import login_required, permission_required
from django.views.decorators.http import require_POST

from core.models import UserSettings
from core.projectdb import ProjectDB
from .utils import import_vessels_from_csv_if_missing



from .models import Vessel
from .vessel_seq_relations import VesselSeqRelations
from .vessel_seq_relations import PURPOSE_ID_TO_LABEL


@login_required
def vessel_page(request):
    return render(request, "fleet/vessels.html")
@require_GET
@login_required
def api_vessel_list(request):
    q = (request.GET.get("q") or "").strip()
    qs = Vessel.objects.all()
    if q:
        qs = qs.filter(name__icontains=q)

    data = []
    for v in qs.order_by("name")[:5000]:
        data.append({
            "id": v.id,
            "name": v.name,
            "imo": v.imo or "",
            "mmsi": v.mmsi or "",
            "call_sign": v.call_sign or "",
            "vessel_type": v.vessel_type or "",
            "owner": v.owner or "",
            "is_active": bool(v.is_active),
            "is_retired": bool(v.is_retired),
            "notes": v.notes or "",
        })
    return JsonResponse({"ok": True, "items": data})


def _parse_json(request):
    try:
        return json.loads(request.body.decode("utf-8"))
    except Exception:
        return None


@require_POST
@login_required
@permission_required("fleet.add_vessel", raise_exception=True)
def api_vessel_create(request):
    payload = _parse_json(request)
    if not payload:
        return JsonResponse({"ok": False, "error": "Invalid JSON"}, status=400)

    name = (payload.get("name") or "").strip()
    if not name:
        return JsonResponse({"ok": False, "error": "Name is required"}, status=400)

    v = Vessel(
        name=name,
        imo=(payload.get("imo") or "").strip() or None,
        mmsi=(payload.get("mmsi") or "").strip() or None,
        call_sign=(payload.get("call_sign") or "").strip() or None,
        vessel_type=(payload.get("vessel_type") or "").strip() or None,
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
@permission_required("fleet.change_vessel", raise_exception=True)
def api_vessel_update(request, pk: int):
    v = get_object_or_404(Vessel, pk=pk)
    payload = _parse_json(request)
    if not payload:
        return JsonResponse({"ok": False, "error": "Invalid JSON"}, status=400)

    name = (payload.get("name") or "").strip()
    if not name:
        return JsonResponse({"ok": False, "error": "Name is required"}, status=400)

    v.name = name
    v.imo = (payload.get("imo") or "").strip() or None
    v.mmsi = (payload.get("mmsi") or "").strip() or None
    v.call_sign = (payload.get("call_sign") or "").strip() or None
    v.vessel_type = (payload.get("vessel_type") or "").strip() or None
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
@permission_required("fleet.delete_vessel", raise_exception=True)
def api_vessel_delete(request, pk: int):
    v = get_object_or_404(Vessel, pk=pk)
    v.delete()
    return JsonResponse({"ok": True})
@require_POST
@login_required
@permission_required("fleet.add_vessel", raise_exception=True)  # remove if you don't want perms
def api_import_fleet_from_csv(request):
    try:
        result = import_vessels_from_csv_if_missing("fleet/vessels_list.csv")
        return JsonResponse({"ok": True, "result": result})
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=400)

@login_required
def _get_sd(request):
    settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = settings.active_project
    if not project:
        return None, JsonResponse({"ok": False, "error": "No active project"}, status=400)

    if hasattr(project, "can_edit") and (not project.can_edit(request.user)):
        return None, JsonResponse({"ok": False, "error": "No permission"}, status=403)

    sd = VesselSeqRelations(project.db_path)
    sd.ensure_sequence_vessel_tables()
    return sd, None
@login_required
def sequence_assignments_page(request):
    sd, err = _get_sd(request)
    if err:
        return err
    return render(request, "fleet/sequence_assignments.html")


@login_required
def api_seq_assign_list(request):
    sd, err = _get_sd(request)
    if err:
        return err

    rows = sd.list_sequence_vessel_assignments(active_only=False)
    vessels = sd.list_project_fleet_simple(active_only=True)

    purposes = [
        {"id": k, "label": v}
        for k, v in PURPOSE_ID_TO_LABEL.items()
    ]

    return JsonResponse({"ok": True, "rows": rows, "vessels": vessels, "purposes": purposes})


@require_POST
@login_required
@csrf_protect
def api_seq_assign_add(request):
    sd, err = _get_sd(request)
    if err:
        return err

    payload = json.loads(request.body.decode("utf-8") or "{}")
    res = sd.add_sequence_vessel_assignment(
        seq_first=payload.get("seq_first"),
        seq_last=payload.get("seq_last"),
        vessel_id=payload.get("vessel_id"),
        purpose_id=payload.get("purpose_id"),
        comments=payload.get("comments", ""),
        is_active=payload.get("is_active", 1),
        allow_overlap=bool(payload.get("allow_overlap", False)),
    )
    return JsonResponse(res, status=200 if res.get("ok") else 400)


@require_POST
@login_required
@csrf_protect
def api_seq_assign_update(request):
    sd, err = _get_sd(request)
    if err:
        return err

    payload = json.loads(request.body.decode("utf-8") or "{}")
    print("PAYLOAD KEYS:", payload.keys())
    res = sd.update_sequence_vessel_assignment(
        row_id=payload.get("id"),
        seq_first=payload.get("seq_first"),
        seq_last=payload.get("seq_last"),
        vessel_id=payload.get("vessel_id"),
        purpose_id=payload.get("purpose_id"),
        comments=payload.get("comments", ""),
        is_active=payload.get("is_active", 1),
        allow_overlap=bool(payload.get("allow_overlap", False)),
    )
    return JsonResponse(res, status=200 if res.get("ok") else 400)


@require_POST
@login_required
@csrf_protect
def api_seq_assign_delete(request):
    sd, err = _get_sd(request)
    if err:
        return err

    payload = json.loads(request.body.decode("utf-8") or "{}")
    res = sd.delete_sequence_vessel_assignment(
        row_id=payload.get("id"),
        hard_delete=bool(payload.get("hard_delete", False)),
    )
    return JsonResponse(res, status=200 if res.get("ok") else 400)