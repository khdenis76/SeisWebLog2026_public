
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Mapping


RESULT_COLUMNS = [
    "image",
    "image_path",
    "resolution",
    "config_used",
    "file_role",
    "file_line",
    "file_station",
    "file_index",
    "rov",
    "dive",
    "date",
    "time",
    "line",
    "station",
    "east",
    "north",
    "dsr_line",
    "dsr_station",
    "dsr_x",
    "dsr_y",
    "delta_m",
    "ocr_vs_file",
    "file_vs_dsr",
    "status",
    "station_image_count",
    "expected_images",
    "station_status",
    "message",
    "checked",
]

SUMMARY_COLUMNS = ["line", "station", "images", "expected", "station_status"]


def export_csv(rows: Iterable[Mapping[str, object]], path: str, columns=None) -> None:
    cols = columns or RESULT_COLUMNS
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c, "") for c in cols})


def export_txt(rows: Iterable[Mapping[str, object]], path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(f"Image: {row.get('image','')}\n")
            f.write(f"Resolution: {row.get('resolution','')}\n")
            f.write(f"Config used: {row.get('config_used','')}\n")
            f.write(f"File role: {row.get('file_role','')}\n")
            f.write(f"File line/station: {row.get('file_line','')} / {row.get('file_station','')}\n")
            f.write(f"ROV/Dive: {row.get('rov','')} / {row.get('dive','')}\n")
            f.write(f"Date/Time: {row.get('date','')} / {row.get('time','')}\n")
            f.write(f"OCR line/station: {row.get('line','')} / {row.get('station','')}\n")
            f.write(f"OCR E/N: {row.get('east','')} / {row.get('north','')}\n")
            f.write(f"DSR line/station: {row.get('dsr_line','')} / {row.get('dsr_station','')}\n")
            f.write(f"DSR E/N: {row.get('dsr_x','')} / {row.get('dsr_y','')}\n")
            f.write(f"Delta: {row.get('delta_m','')}\n")
            f.write(f"OCR vs File: {row.get('ocr_vs_file','')}\n")
            f.write(f"File vs DSR: {row.get('file_vs_dsr','')}\n")
            f.write(f"Station Count: {row.get('station_image_count','')} / {row.get('expected_images','')} ({row.get('station_status','')})\n")
            f.write(f"Status: {row.get('status','')}\n")
            if row.get("message"):
                f.write(f"Message: {row.get('message','')}\n")
            f.write("\n")
