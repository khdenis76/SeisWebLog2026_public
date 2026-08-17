from __future__ import annotations

from pathlib import Path

import json


STATE_FILE_NAME = "dataviewer2_state.json"


def _state_path(root_path: str | Path) -> Path:
    root = Path(root_path).expanduser().resolve()
    if root.is_file():
        root = root.parent
    return root / STATE_FILE_NAME


def remembered_project(root_path: str | Path) -> Path | None:
    state_path = _state_path(root_path)
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        value = str(payload.get("last_project", "")).strip()
    except (OSError, ValueError, TypeError):
        return None
    return Path(value).expanduser() if value else None


def remember_project(root_path: str | Path, project_path: str | Path) -> None:
    state_path = _state_path(root_path)
    payload = {
        "last_project": str(Path(project_path).expanduser().resolve()),
    }
    state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def forget_project(root_path: str | Path) -> None:
    state_path = _state_path(root_path)
    try:
        state_path.unlink()
    except FileNotFoundError:
        pass
