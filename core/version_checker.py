from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

from django.core.cache import cache

try:
    import requests
except Exception:  # requests not installed
    requests = None

import urllib.request


# Local version file (repo root / version.txt)
LOCAL_VERSION_FILE = Path(__file__).resolve().parent.parent / "version.txt"

# Remote version file (raw GitHub)
REMOTE_VERSION_URL = (
    "https://raw.githubusercontent.com/khdenis76/SeisWebLog2026_public/main/version.txt"
)

# Your public "download ZIP" link
DOWNLOAD_ZIP_URL = (
    "https://github.com/khdenis76/SeisWebLog2026_public/archive/refs/heads/main.zip"
)


def _parse_version(v: str) -> Tuple[int, ...]:
    """
    Converts '2026.0.9.09' -> (2026, 0, 9, 9)
    Safe for numeric compare.
    """
    v = (v or "").strip()
    parts = v.split(".")
    out = []
    for p in parts:
        try:
            out.append(int(p))
        except ValueError:
            out.append(0)
    return tuple(out)


def get_local_version() -> str:
    if LOCAL_VERSION_FILE.exists():
        return LOCAL_VERSION_FILE.read_text(encoding="utf-8").strip()
    return "0.0.0.0"


def get_remote_version(timeout_seconds: int = 5) -> Optional[str]:
    # Prefer requests if available, otherwise use urllib
    try:
        if requests is not None:
            r = requests.get(REMOTE_VERSION_URL, timeout=timeout_seconds)
            if r.status_code == 200:
                return r.text.strip()
            return None

        with urllib.request.urlopen(REMOTE_VERSION_URL, timeout=timeout_seconds) as resp:
            return resp.read().decode("utf-8").strip()

    except Exception:
        return None


def check_new_version(cache_seconds: int = 3600) -> dict:
    """
    Returns a dict usable in templates and JSON.

    Example:
      {
        "ok": True,
        "new_available": True,
        "local": "2026.0.9.09",
        "remote": "2026.0.9.10",
        "download_url": "...main.zip"
      }
    """
    cached = cache.get("version_check_result")
    if cached:
        return cached

    local = get_local_version()
    remote = get_remote_version()

    if not remote:
        result = {
            "ok": False,
            "new_available": False,
            "local": local,
            "remote": None,
            "download_url": DOWNLOAD_ZIP_URL,
        }
    else:
        result = {
            "ok": True,
            "new_available": _parse_version(remote) > _parse_version(local),
            "local": local,
            "remote": remote,
            "download_url": DOWNLOAD_ZIP_URL,
        }

    cache.set("version_check_result", result, cache_seconds)
    return result