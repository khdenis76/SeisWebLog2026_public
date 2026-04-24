
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from .models import OCRConfig


def default_config() -> OCRConfig:
    return OCRConfig()


def default_bundle() -> Dict[str, Any]:
    return {
        "comment": "",
        "filename_pattern": "{role}_{linestation}_{index}.png",
        "output_pattern": "{role}_{LLLLL}{SSSSS}_{index}.png",
        "rename_mode": "copy",  # copy | rename | off
        "single_config_only": True,
        "skip_checked": True,
        "main_config": default_config().to_dict(),
        "alt_config": default_config().to_dict(),
    }


def _normalize_bundle(data: Dict[str, Any]) -> Dict[str, Any]:
    bundle = default_bundle()

    if "main_config" not in data:
        cfg = OCRConfig.from_dict(data)
        bundle["main_config"] = cfg.to_dict()
        bundle["alt_config"] = OCRConfig().to_dict()
        bundle["comment"] = str(data.get("comment", ""))
        old_comments = data.get("comments")
        if not bundle["comment"] and isinstance(old_comments, list) and old_comments:
            bundle["comment"] = str(old_comments[0])
        if data.get("filename_pattern"):
            bundle["filename_pattern"] = str(data.get("filename_pattern"))
        return bundle

    bundle.update({
        "comment": str(data.get("comment", "")),
        "filename_pattern": str(data.get("filename_pattern", bundle["filename_pattern"])),
        "output_pattern": str(data.get("output_pattern", bundle["output_pattern"])),
        "rename_mode": str(data.get("rename_mode", bundle["rename_mode"])),
        "single_config_only": bool(data.get("single_config_only", True)),
        "skip_checked": bool(data.get("skip_checked", True)),
        "main_config": OCRConfig.from_dict(data.get("main_config", {})).to_dict(),
        "alt_config": OCRConfig.from_dict(data.get("alt_config", {})).to_dict(),
    })

    if not bundle["comment"]:
        old_comments = data.get("comments")
        if isinstance(old_comments, list) and old_comments:
            bundle["comment"] = str(old_comments[0])

    return bundle


def load_config_bundle(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return default_bundle()
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return _normalize_bundle(data)


def save_config_bundle(bundle: Dict[str, Any], path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_bundle(bundle)
    with p.open("w", encoding="utf-8") as f:
        json.dump(normalized, f, indent=2, ensure_ascii=False)


def load_config(path: str) -> OCRConfig:
    return OCRConfig.from_dict(load_config_bundle(path)["main_config"])


def save_config(config: OCRConfig, path: str) -> None:
    bundle = default_bundle()
    bundle["main_config"] = config.to_dict()
    save_config_bundle(bundle, path)
