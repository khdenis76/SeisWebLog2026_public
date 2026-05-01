from __future__ import annotations

import math
import os
import re
import sqlite3
import shutil
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

from PySide6.QtCore import QObject, Signal, Slot

try:
    import cv2
except Exception:
    cv2 = None

from .config_manager import load_config_bundle
from .dsr_loader import load_dsr
from .image_scanner import scan_images
from .models import OCRConfig, ROIField
from .ocr_db import ensure_schema, is_checked, upsert_result
from .ocr_engine import OCREngine


@dataclass
class BatchSettings:
    project_db_path: str
    folder: str
    config_path: str
    include_subfolders: bool
    masks: List[str]
    explicit_files: Optional[List[str]] = None
    force_recheck: bool = False


class BatchWorker(QObject):
    progress = Signal(int, int, str)
    finished = Signal(dict)
    failed = Signal(str)

    def __init__(self, settings: BatchSettings) -> None:
        super().__init__()
        self.settings = settings
        self._cancel = False

    @Slot()
    def run(self) -> None:
        try:
            self.finished.emit(self._run_impl())
        except Exception as exc:
            self.failed.emit(str(exc))

    def cancel(self) -> None:
        self._cancel = True

    def _run_impl(self) -> Dict[str, Any]:
        bundle = load_config_bundle(self.settings.config_path)
        main_config = OCRConfig.from_dict(bundle["main_config"])
        alt_config = OCRConfig.from_dict(bundle["alt_config"])
        main_config.include_subfolders = self.settings.include_subfolders
        alt_config.include_subfolders = self.settings.include_subfolders
        if self.settings.masks:
            main_config.file_masks = self.settings.masks
            alt_config.file_masks = self.settings.masks

        ensure_schema(self.settings.project_db_path)
        dsr = load_dsr(self.settings.project_db_path)
        rl_mask = self._load_rl_mask(self.settings.project_db_path)
        files = list(self.settings.explicit_files or [])
        if not files:
            files = scan_images(self.settings.folder, main_config.file_masks, main_config.include_subfolders)
        ocr_main = OCREngine(use_easyocr_first=main_config.use_easyocr_first)
        ocr_alt = OCREngine(use_easyocr_first=alt_config.use_easyocr_first)
        skip_checked = bool(bundle.get("skip_checked", True))

        rows: List[Dict[str, Any]] = []
        station_counter: Counter[Tuple[str, str]] = Counter()
        total = len(files)

        for i, path in enumerate(files, start=1):
            if self._cancel:
                break
            self.progress.emit(i, total, os.path.basename(path))
            if skip_checked and (not self.settings.force_recheck) and is_checked(self.settings.project_db_path, path):
                continue
            row = self._process_one(path, bundle, main_config, alt_config, dsr, rl_mask, ocr_main, ocr_alt)
            rows.append(row)
            key = (str(row.get("file_line") or row.get("line") or "").strip(), str(row.get("file_station") or row.get("station") or "").strip())
            if key != ("", ""):
                station_counter[key] += 1

        deploy_expected = int(main_config.deploy_images)
        recovery_expected = int(main_config.recovery_images)
        full_expected = deploy_expected + recovery_expected
        expected_text = f"{deploy_expected}+{recovery_expected}"

        for row in rows:
            key = (str(row.get("file_line") or row.get("line") or "").strip(), str(row.get("file_station") or row.get("station") or "").strip())
            if key == ("", ""):
                row["station_status"] = "UNKNOWN"
                row["station_image_count"] = 0
                row["expected_images"] = expected_text
            else:
                count = station_counter[key]
                row["station_image_count"] = count
                row["expected_images"] = expected_text
                if count == deploy_expected:
                    row["station_status"] = "DEPLOYMENT_OK"
                elif count == full_expected:
                    row["station_status"] = "COMPLETE_OK"
                elif count < deploy_expected:
                    row["station_status"] = "DEPLOYMENT_INCOMPLETE"
                elif count < full_expected:
                    row["station_status"] = "RECOVERY_INCOMPLETE"
                else:
                    row["station_status"] = "TOO_MANY"
            upsert_result(self.settings.project_db_path, row)

        station_summary = self._build_station_summary(rows)
        return {"rows": rows, "station_summary": station_summary, "processed": len(rows), "canceled": self._cancel}

    def _build_station_summary(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        grouped: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for row in rows:
            key = (str(row.get("file_line") or row.get("line") or "").strip(), str(row.get("file_station") or row.get("station") or "").strip())
            if key == ("", ""):
                continue
            item = grouped.setdefault(key, {
                "checked": 1,
                "file_line": key[0],
                "file_station": key[1],
                "line": row.get("line", key[0]),
                "station": row.get("station", key[1]),
                "images": 0,
                "expected": row.get("expected_images", ""),
                "station_status": row.get("station_status", ""),
                "status": row.get("status", ""),
                "dsr_line": row.get("dsr_line", ""),
                "dsr_station": row.get("dsr_station", ""),
                "dsr_timestamp": row.get("dsr_timestamp", ""),
                "dsr_timestamp1": row.get("dsr_timestamp1", ""),
                "dsr_rov": row.get("dsr_rov", ""),
                "dsr_rov1": row.get("dsr_rov1", ""),
                "delta_m": row.get("delta_m", ""),
                "message": row.get("message", ""),
            })
            item["images"] += 1
            item["checked"] = 1 if item["checked"] and bool(row.get("checked", 1)) else 0
            item["status"] = self._worse_status(item["status"], row.get("status", ""))
            if self._delta_value(row.get("delta_m")) > self._delta_value(item.get("delta_m")):
                item["delta_m"] = row.get("delta_m", "")
            if row.get("message"):
                item["message"] = row.get("message")
        return sorted(grouped.values(), key=lambda r: (r["file_line"], r["file_station"]))

    @staticmethod
    def _worse_status(a: str, b: str) -> str:
        rank = {"OK": 0, "WARNING": 1, "NO_COORDS": 2, "NO_DSR": 3, "BAD_FILENAME": 4, "BAD_RESOLUTION": 5, "ERROR": 6}
        return a if rank.get(str(a), 0) >= rank.get(str(b), 0) else str(b)

    @staticmethod
    def _delta_value(v: Any) -> float:
        try:
            return float(str(v))
        except Exception:
            return -1.0

    def _process_one(self, path: str, bundle: Dict[str, Any], main_config: OCRConfig, alt_config: OCRConfig,
                     dsr: Dict[Tuple[str, str], dict[str, Any]], rl_mask: str, ocr_main: OCREngine, ocr_alt: OCREngine) -> Dict[str, Any]:
        row = {
            "image": os.path.basename(path), "image_path": path, "resolution": "", "config_used": "",
            "file_role": "", "file_line": "", "file_station": "", "file_index": "", "rov": "", "dive": "",
            "date": "", "time": "", "line": "", "station": "", "east": "", "north": "",
            "dsr_line": "", "dsr_station": "", "dsr_x": "", "dsr_y": "", "dsr_timestamp": "", "dsr_timestamp1": "",
            "dsr_rov": "", "dsr_rov1": "", "delta_m": "", "ocr_vs_file": "", "file_vs_dsr": "",
            "status": "OK", "station_image_count": 0, "expected_images": "", "station_status": "", "message": "", "checked": 1,
        }
        row.update(self._parse_filename(os.path.basename(path), str(bundle.get("filename_pattern", "")), rl_mask))
        if cv2 is None:
            row["status"] = "ERROR"; row["message"] = "OpenCV not installed"; return row
        img = cv2.imread(path)
        if img is None:
            row["status"] = "ERROR"; row["message"] = "Cannot read image"; return row
        h, w = img.shape[:2]
        row["resolution"] = f"{w}x{h}"
        config, ocr, used = self._choose_config(w, h, main_config, alt_config, bool(bundle.get("single_config_only", True)), ocr_main, ocr_alt)
        row["config_used"] = used
        if config.expected_width and config.expected_height and (w != config.expected_width or h != config.expected_height):
            row["status"] = "BAD_RESOLUTION"; row["message"] = f"Expected {config.expected_width}x{config.expected_height}, got {w}x{h}"; return row
        for roi in config.roi_fields:
            crop = self._crop(img, roi)
            row[roi.name] = ocr.read_text(crop, roi.field_type)
        file_line = str(row.get("file_line", "")).strip(); file_station = str(row.get("file_station", "")).strip()
        ocr_line = str(row.get("line", "")).strip(); ocr_station = str(row.get("station", "")).strip()
        if file_line and file_station and ocr_line and ocr_station:
            row["ocr_vs_file"] = "MATCH" if (file_line == ocr_line and file_station == ocr_station) else "MISMATCH"
        dsr_key = (file_line, file_station) if file_line and file_station else ((ocr_line, ocr_station) if ocr_line and ocr_station else None)
        if dsr_key and dsr_key in dsr:
            d = dsr[dsr_key]
            row["dsr_line"] = d["line"]; row["dsr_station"] = d["station"]
            row["dsr_timestamp"] = d.get("timestamp", ""); row["dsr_timestamp1"] = d.get("timestamp1", "")
            row["dsr_rov"] = d.get("rov", ""); row["dsr_rov1"] = d.get("rov1", "")
            if d.get("x") is not None: row["dsr_x"] = f"{d['x']:.2f}"
            if d.get("y") is not None: row["dsr_y"] = f"{d['y']:.2f}"
            row["file_vs_dsr"] = "MATCH" if file_line and file_station else ""
            try:
                east = float(str(row.get("east", "")).strip()); north = float(str(row.get("north", "")).strip())
                if d.get("x") is not None and d.get("y") is not None:
                    delta = math.hypot(east - d["x"], north - d["y"])
                    row["delta_m"] = f"{delta:.2f}"
                    row["status"] = "ERROR" if delta >= config.delta_error else ("WARNING" if delta >= config.delta_warning else "OK")
            except Exception:
                row["status"] = "NO_COORDS"; row["message"] = "OCR coordinates missing or invalid"
        else:
            row["status"] = "NO_DSR" if dsr_key else "BAD_FILENAME"
            row["message"] = "No matching DSR row" if dsr_key else "Filename line/station not parsed"
        self._apply_output_file(path, bundle, row)
        return row

    @staticmethod
    def _choose_config(w:int, h:int, main_config: OCRConfig, alt_config: OCRConfig, single: bool, ocr_main: OCREngine, ocr_alt: OCREngine):
        if single: return main_config, ocr_main, "main"
        if alt_config.expected_width and alt_config.expected_height and w == alt_config.expected_width and h == alt_config.expected_height:
            return alt_config, ocr_alt, "alt"
        return main_config, ocr_main, "main"

    @staticmethod
    def _crop(img, roi: ROIField):
        x1=max(0,int(roi.x)); y1=max(0,int(roi.y)); x2=min(img.shape[1],x1+max(1,int(roi.w))); y2=min(img.shape[0],y1+max(1,int(roi.h)))
        return img[y1:y2, x1:x2].copy()

    @staticmethod
    def _load_rl_mask(project_db_path: str) -> str:
        if not project_db_path or not os.path.exists(project_db_path): return ""
        conn=sqlite3.connect(project_db_path)
        try:
            row=conn.execute("SELECT COALESCE(rl_mask,'') FROM project_geometry LIMIT 1").fetchone()
            return str(row[0] or "") if row else ""
        finally:
            conn.close()

    def _parse_filename(self, filename: str, pattern: str, rl_mask: str) -> Dict[str, str]:
        if not pattern.strip(): return {}
        regex = re.escape(pattern)
        mapping = {
            r"\{role\}": r"(?P<file_role>[A-Za-z0-9]+)",
            r"\{line\}": r"(?P<file_line>\d+)",
            r"\{station\}": r"(?P<file_station>\d+)",
            r"\{linestation\}": r"(?P<linestation>\d+)",
            r"\{index\}": r"(?P<file_index>\d+)",
        }
        for k,v in mapping.items(): regex = regex.replace(k, v)
        m = re.match(f"^{regex}$", filename)
        if not m: return {}
        data = {k: str(v) for k,v in m.groupdict().items() if v is not None}
        if "linestation" in data and ("file_line" not in data or "file_station" not in data):
            line_digits = sum(1 for ch in str(rl_mask) if ch.upper() == "L")
            ls = data.get("linestation", "")
            if line_digits > 0 and len(ls) > line_digits:
                data["file_line"] = ls[:line_digits]
                data["file_station"] = ls[line_digits:]
        return data

    def _apply_output_file(self, path: str, bundle: Dict[str, Any], row: Dict[str, Any]) -> None:
        mode = str(bundle.get("rename_mode", "off")).strip().lower()
        if mode == "off": return
        output_pattern = str(bundle.get("output_pattern", "")).strip()
        if not output_pattern: return
        target_name = self._render_output_name(output_pattern, row)
        source = Path(path)
        target = source.parent / "renamed" / target_name if mode == "copy" else source.with_name(target_name)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.resolve() == source.resolve(): return
        ext = target.suffix.lower()
        if ext in {".png", ".jpg", ".jpeg", ".bmp"} and cv2 is not None:
            img = cv2.imread(path)
            if img is not None: cv2.imwrite(str(target), img); return
        if mode == "copy": shutil.copy2(path, target)
        elif mode == "rename": shutil.move(path, target)

    def _render_output_name(self, pattern: str, row: Dict[str, Any]) -> str:
        line = str(row.get("file_line") or row.get("line") or "")
        station = str(row.get("file_station") or row.get("station") or "")
        out = pattern
        for n in range(2, 10):
            out = out.replace("L"*n, line.zfill(n))
            out = out.replace("S"*n, station.zfill(n))
        return (out.replace("{role}", str(row.get("file_role", "")))
                   .replace("{line}", line)
                   .replace("{station}", station)
                   .replace("{linestation}", f"{line}{station}")
                   .replace("{index}", str(row.get("file_index", ""))))
