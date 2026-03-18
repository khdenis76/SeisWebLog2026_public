from __future__ import annotations

from pathlib import Path
from typing import Iterable, List


def scan_images(folder: str, masks: Iterable[str], recursive: bool) -> List[str]:
    base = Path(folder)
    if not base.exists():
        raise FileNotFoundError(folder)

    files = []
    for mask in masks:
        mask = (mask or "").strip()
        if not mask:
            continue
        if recursive:
            files.extend(base.rglob(mask))
        else:
            files.extend(base.glob(mask))

    unique_sorted = sorted({p.resolve() for p in files if p.is_file()})
    return [str(p) for p in unique_sorted]
