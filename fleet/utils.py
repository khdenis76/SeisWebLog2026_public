import csv
from pathlib import Path
from django.conf import settings
from django.db import transaction
from django.db.models import Q

from .models import Vessel


def _to_bool(v, default=False) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _norm(s):
    return (s or "").strip()


@transaction.atomic
def import_vessels_from_csv_if_missing(csv_rel_path="fleet/vessels_list.csv"):
    """
    Loads vessels from a CSV file and creates ONLY missing rows.
    Existing vessels are detected by:
      - IMO (if provided) OR
      - name (case-insensitive)

    Returns dict with counts and skipped list.
    """
    csv_path = Path(settings.BASE_DIR) / csv_rel_path
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    created = 0
    skipped = 0
    created_names = []
    skipped_names = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required = {"Name", "Type", "IMO", "MMSI", "Call", "Owner", "Active", "Retired"}
        missing_cols = required - set(reader.fieldnames or [])
        if missing_cols:
            raise ValueError(f"CSV missing columns: {sorted(missing_cols)}")

        for row in reader:
            name = _norm(row.get("Name"))
            if not name:
                continue

            imo = _norm(row.get("IMO")) or None
            mmsi = _norm(row.get("MMSI")) or None
            call_sign = _norm(row.get("Call")) or None
            vessel_type = _norm(row.get("Type")) or None
            owner = _norm(row.get("Owner")) or None
            is_active = _to_bool(row.get("Active"), default=True)
            is_retired = _to_bool(row.get("Retired"), default=False)

            # Exists? prefer IMO check when available
            exists_q = Q()
            if imo:
                exists_q |= Q(imo=imo)
            exists_q |= Q(name__iexact=name)

            if Vessel.objects.filter(exists_q).exists():
                skipped += 1
                skipped_names.append(name)
                continue

            Vessel.objects.create(
                name=name,
                vessel_type=vessel_type,
                imo=imo,
                mmsi=mmsi,
                call_sign=call_sign,
                owner=owner,
                is_active=is_active,
                is_retired=is_retired,
            )
            created += 1
            created_names.append(name)

    return {
        "created": created,
        "skipped": skipped,
        "created_names": created_names,
        "skipped_names": skipped_names,
        "csv_path": str(csv_path),
    }