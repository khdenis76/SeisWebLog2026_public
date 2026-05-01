# SeisWebLog OCR v3

PySide6 desktop tool for SeisWebLog that:
- reads the Django project list from the main Django SQLite database
- lets the user select a SeisWebLog project
- loads `DSR` from the selected project DB
- scans image folders with file mask support and optional subfolder recursion
- uses manual ROI configs saved as JSON
- OCRs Line / Station / E / N / Date / Time / other fields
- compares OCR coordinates with DSR coordinates
- shows image-level results and station summary tables
- validates the expected number of images per `(Line, Station)`
- exports CSV and TXT reports

## Run

```bash
python ocr/run_ocr_v3.py
```

or set the Django DB path in an environment variable:

```bash
set SEISWEBLOG_DJANGO_DB=D:\path\to\django.sqlite3
python ocr/run_ocr_v3.py
```

## Expected Django table
The tool reads projects from:
- `baseproject_project(id, name, db_path)`

## Expected project DB table
The tool reads coordinates from:
- `DSR(Line, Station, REC_X, REC_Y)`

## Notes
- The ROI editor is included for manual config creation.
- Sample config is in `ocr/configs/sample_rov_overlay.json`.
- EasyOCR is preferred; pytesseract is used only as fallback.
- Double-click a result row to open the source image.

## Current scope
This v3 package is a working structured implementation and a solid base for deeper SeisWebLog integration. The OCR engine is intentionally generic so it can work across multiple projects. You will likely want to tune ROI positions and filename regex for each project overlay.


## OCR v5 notes

This package preserves the v3 interface and workflow, including menus, toolbar/icons, project DB autoload, ROI editor, config dialog, filters, exports, visit/check workflow, and station map. v5 adds quick filters, better table sorting, summary cards, multi-select station/image rows, database delete/reset tools, and map image-count labels next to DSR points. Run with `python ocr/run_ocr_v5.py` or keep using `python ocr/run_ocr_v3.py`.
