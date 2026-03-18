from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import List
from PySide6.QtGui import QAction
from ..core.map_loader import load_preplot_points, load_dsr_station_points_from_ocr
from .map_window import StationMapWindow

from PySide6.QtCore import QThread, Qt, QDate, QSize
from PySide6.QtWidgets import (
    QAbstractItemView, QCheckBox, QComboBox, QDateEdit, QDialog, QFileDialog, QFormLayout, QFrame,
    QGridLayout, QGroupBox, QHBoxLayout, QLabel, QLineEdit, QMainWindow, QMessageBox, QPushButton,
    QSizePolicy, QSplitter, QStyle, QTableView, QTextEdit, QToolBar, QVBoxLayout, QWidget, QSpinBox,
    QDoubleSpinBox, QDialogButtonBox, QHeaderView
)

from ..core.batch_processor import BatchSettings, BatchWorker
from ..core.config_manager import default_bundle, load_config_bundle, save_config_bundle
from ..core.exporters import RESULT_COLUMNS, SUMMARY_COLUMNS, export_csv, export_txt
from ..core.image_scanner import scan_images
from ..core.models import OCRConfig
from ..core.ocr_db import fetch_results, set_checked, ensure_schema
from ..core.project_loader import load_projects
from ..core.dsr_loader import load_distinct_rov_values
from .results_model import DictTableModel
from .roi_editor import RoiEditorDialog

INPUT_PATTERN_HELP = (
    "Supported input blocks:\n"
    "{role} prefix like dep / pre / qr\n"
    "{line} line number\n"
    "{station} station number\n"
    "{linestation} combined line+station block\n"
    "{index} image index\n\n"
    "If {linestation} is used, line length is taken from project_geometry.rl_mask."
)
OUTPUT_PATTERN_HELP = (
    "Supported output blocks:\n{role} {line} {station} {linestation} {index}\n\n"
    "Padding masks for output only:\nLLLLL = line padded to 5 digits\nSSSSS = station padded to 5 digits\n"
    "Example: {role}_{LLLLL}{SSSSS}_{index}.png"
)

STATION_COLUMNS = [
    "checked", "file_line", "file_station", "images", "expected", "station_status", "status",
    "dsr_line", "dsr_station", "dsr_timestamp", "dsr_timestamp1", "dsr_rov", "dsr_rov1", "delta_m", "message"
]


class ConfigDialog(QDialog):
    def __init__(self, bundle: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Config settings")
        self.bundle = bundle
        self.main_cfg = OCRConfig.from_dict(bundle.get("main_config", {}))
        self.alt_cfg = OCRConfig.from_dict(bundle.get("alt_config", {}))
        lay = QVBoxLayout(self)
        form = QFormLayout()
        lay.addLayout(form)
        self.comment = QLineEdit(str(bundle.get("comment", "")))
        self.filename_pattern = QLineEdit(str(bundle.get("filename_pattern", "")))
        self.filename_pattern.setToolTip(INPUT_PATTERN_HELP)
        self.output_pattern = QLineEdit(str(bundle.get("output_pattern", "")))
        self.output_pattern.setToolTip(OUTPUT_PATTERN_HELP)
        self.rename_mode = QComboBox(); self.rename_mode.addItems(["off", "copy", "rename"]); self.rename_mode.setCurrentText(str(bundle.get("rename_mode", "copy")))
        self.single_only = QCheckBox("Use only main config for all files"); self.single_only.setChecked(bool(bundle.get("single_config_only", True)))
        self.skip_checked = QCheckBox("Skip already checked files"); self.skip_checked.setChecked(bool(bundle.get("skip_checked", True)))
        self.main_w = QSpinBox(); self.main_w.setMaximum(100000); self.main_w.setValue(self.main_cfg.expected_width)
        self.main_h = QSpinBox(); self.main_h.setMaximum(100000); self.main_h.setValue(self.main_cfg.expected_height)
        self.alt_w = QSpinBox(); self.alt_w.setMaximum(100000); self.alt_w.setValue(self.alt_cfg.expected_width)
        self.alt_h = QSpinBox(); self.alt_h.setMaximum(100000); self.alt_h.setValue(self.alt_cfg.expected_height)
        self.deploy = QSpinBox(); self.deploy.setRange(1, 99); self.deploy.setValue(self.main_cfg.deploy_images)
        self.recovery = QSpinBox(); self.recovery.setRange(1, 99); self.recovery.setValue(self.main_cfg.recovery_images)
        self.warn = QDoubleSpinBox(); self.warn.setRange(0, 99999); self.warn.setDecimals(2); self.warn.setValue(self.main_cfg.delta_warning)
        self.err = QDoubleSpinBox(); self.err.setRange(0, 99999); self.err.setDecimals(2); self.err.setValue(self.main_cfg.delta_error)
        self.easy = QCheckBox("Use EasyOCR first"); self.easy.setChecked(self.main_cfg.use_easyocr_first)
        form.addRow("Comment", self.comment)
        form.addRow("Input filename pattern", self.filename_pattern)
        form.addRow("Output filename mask", self.output_pattern)
        form.addRow("Rename mode", self.rename_mode)
        form.addRow("", self.single_only)
        form.addRow("", self.skip_checked)
        form.addRow("Main width", self.main_w); form.addRow("Main height", self.main_h)
        form.addRow("Alt width", self.alt_w); form.addRow("Alt height", self.alt_h)
        form.addRow("Deploy images", self.deploy); form.addRow("Recovery images", self.recovery)
        form.addRow("Delta warning", self.warn); form.addRow("Delta error", self.err)
        form.addRow("", self.easy)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject)
        lay.addWidget(buttons)

    def updated_bundle(self) -> dict:
        self.bundle["comment"] = self.comment.text().strip()
        self.bundle["filename_pattern"] = self.filename_pattern.text().strip()
        self.bundle["output_pattern"] = self.output_pattern.text().strip()
        self.bundle["rename_mode"] = self.rename_mode.currentText()
        self.bundle["single_config_only"] = self.single_only.isChecked()
        self.bundle["skip_checked"] = self.skip_checked.isChecked()
        self.main_cfg.expected_width = self.main_w.value(); self.main_cfg.expected_height = self.main_h.value()
        self.alt_cfg.expected_width = self.alt_w.value(); self.alt_cfg.expected_height = self.alt_h.value()
        self.main_cfg.deploy_images = self.deploy.value(); self.main_cfg.recovery_images = self.recovery.value()
        self.main_cfg.delta_warning = self.warn.value(); self.main_cfg.delta_error = self.err.value(); self.main_cfg.use_easyocr_first = self.easy.isChecked()
        self.alt_cfg.delta_warning = self.main_cfg.delta_warning; self.alt_cfg.delta_error = self.main_cfg.delta_error; self.alt_cfg.use_easyocr_first = self.main_cfg.use_easyocr_first
        self.bundle["main_config"] = self.main_cfg.to_dict(); self.bundle["alt_config"] = self.alt_cfg.to_dict()
        return self.bundle


class FilterDialog(QDialog):
    def __init__(self, filters: dict, rov_values: list[str] | None = None, rov1_values: list[str] | None = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Filters")
        self.resize(520, 0)

        rov_values = [v for v in (rov_values or []) if str(v).strip()]
        rov1_values = [v for v in (rov1_values or []) if str(v).strip()]

        lay = QVBoxLayout(self)
        form = QFormLayout()
        lay.addLayout(form)

        self.dsr_line = QLineEdit(filters.get("dsr_line", ""))
        self.dsr_station = QLineEdit(filters.get("dsr_station", ""))

        self.status = QComboBox()
        self.status.addItems(["", "OK", "WARNING", "ERROR", "NO_DSR", "BAD_FILENAME", "BAD_RESOLUTION", "DEPLOYMENT_OK", "COMPLETE_OK", "DEPLOYMENT_INCOMPLETE", "RECOVERY_INCOMPLETE", "TOO_MANY"])
        self.status.setCurrentText(filters.get("status", ""))

        self.checked = QComboBox()
        self.checked.addItems(["", "1", "0"])
        self.checked.setCurrentText(filters.get("checked", ""))

        self.min_delta = QLineEdit(filters.get("min_delta", ""))
        self.max_delta = QLineEdit(filters.get("max_delta", ""))

        self.dsr_rov = QComboBox()
        self.dsr_rov.addItem("")
        self.dsr_rov.addItems(rov_values)
        self.dsr_rov.setCurrentText(filters.get("dsr_rov", ""))

        self.dsr_rov1 = QComboBox()
        self.dsr_rov1.addItem("")
        self.dsr_rov1.addItems(rov1_values)
        self.dsr_rov1.setCurrentText(filters.get("dsr_rov1", ""))

        self.deploy_mode = QComboBox()
        self.deploy_mode.addItems(["any", "single day", "range"])
        self.deploy_mode.setCurrentText(filters.get("deploy_mode", "any"))

        self.deploy_day = QDateEdit()
        self.deploy_day.setCalendarPopup(True)
        self.deploy_day.setDate(QDate.fromString(filters.get("deploy_day", "2000-01-01"), "yyyy-MM-dd") or QDate.currentDate())

        self.deploy_from = QDateEdit()
        self.deploy_from.setCalendarPopup(True)
        self.deploy_from.setDate(QDate.fromString(filters.get("deploy_from", "2000-01-01"), "yyyy-MM-dd") or QDate.currentDate())

        self.deploy_to = QDateEdit()
        self.deploy_to.setCalendarPopup(True)
        self.deploy_to.setDate(QDate.fromString(filters.get("deploy_to", "2000-01-01"), "yyyy-MM-dd") or QDate.currentDate())

        self.recover_mode = QComboBox()
        self.recover_mode.addItems(["any", "single day", "range"])
        self.recover_mode.setCurrentText(filters.get("recover_mode", "any"))

        self.recover_day = QDateEdit()
        self.recover_day.setCalendarPopup(True)
        self.recover_day.setDate(QDate.fromString(filters.get("recover_day", "2000-01-01"), "yyyy-MM-dd") or QDate.currentDate())

        self.recover_from = QDateEdit()
        self.recover_from.setCalendarPopup(True)
        self.recover_from.setDate(QDate.fromString(filters.get("recover_from", "2000-01-01"), "yyyy-MM-dd") or QDate.currentDate())

        self.recover_to = QDateEdit()
        self.recover_to.setCalendarPopup(True)
        self.recover_to.setDate(QDate.fromString(filters.get("recover_to", "2000-01-01"), "yyyy-MM-dd") or QDate.currentDate())

        form.addRow("Line", self.dsr_line)
        form.addRow("Station", self.dsr_station)
        form.addRow("Status", self.status)
        form.addRow("Checked", self.checked)
        form.addRow("Min delta", self.min_delta)
        form.addRow("Max delta", self.max_delta)
        form.addRow("ROV", self.dsr_rov)
        form.addRow("ROV1", self.dsr_rov1)
        form.addRow("Deployment filter", self.deploy_mode)
        form.addRow("Deployment day", self.deploy_day)
        form.addRow("Deployment from", self.deploy_from)
        form.addRow("Deployment to", self.deploy_to)
        form.addRow("Recovery filter", self.recover_mode)
        form.addRow("Recovery day", self.recover_day)
        form.addRow("Recovery from", self.recover_from)
        form.addRow("Recovery to", self.recover_to)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel | QDialogButtonBox.Reset)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        buttons.button(QDialogButtonBox.Reset).clicked.connect(self.reset_values)
        lay.addWidget(buttons)

    def reset_values(self):
        self.dsr_line.clear()
        self.dsr_station.clear()
        self.status.setCurrentText("")
        self.checked.setCurrentText("")
        self.min_delta.clear()
        self.max_delta.clear()
        self.dsr_rov.setCurrentText("")
        self.dsr_rov1.setCurrentText("")
        self.deploy_mode.setCurrentText("any")
        self.recover_mode.setCurrentText("any")

    def values(self) -> dict:
        d = {
            "dsr_line": self.dsr_line.text().strip(),
            "dsr_station": self.dsr_station.text().strip(),
            "status": self.status.currentText().strip(),
            "checked": self.checked.currentText().strip(),
            "min_delta": self.min_delta.text().strip(),
            "max_delta": self.max_delta.text().strip(),
            "dsr_rov": self.dsr_rov.currentText().strip(),
            "dsr_rov1": self.dsr_rov1.currentText().strip(),
            "deploy_mode": self.deploy_mode.currentText(),
            "recover_mode": self.recover_mode.currentText(),
        }
        if self.deploy_mode.currentText() == "single day":
            d["deploy_day"] = self.deploy_day.date().toString("yyyy-MM-dd")
        elif self.deploy_mode.currentText() == "range":
            d["deploy_from"] = self.deploy_from.date().toString("yyyy-MM-dd")
            d["deploy_to"] = self.deploy_to.date().toString("yyyy-MM-dd")
        if self.recover_mode.currentText() == "single day":
            d["recover_day"] = self.recover_day.date().toString("yyyy-MM-dd")
        elif self.recover_mode.currentText() == "range":
            d["recover_from"] = self.recover_from.date().toString("yyyy-MM-dd")
            d["recover_to"] = self.recover_to.date().toString("yyyy-MM-dd")
        return d


class OCRMainWindow(QMainWindow):
    def __init__(self, django_db: str = "", parent=None):
        super().__init__(parent)
        self.setWindowTitle("SeisWebLog OCR v3 - ROV Overlay QC")
        self.resize(1600, 900)
        self.django_db = django_db or ""
        self.projects = []
        self.current_config_path = ""
        self.config_bundle = default_bundle()
        self.current_config = OCRConfig.from_dict(self.config_bundle["main_config"])
        self.alt_config = OCRConfig.from_dict(self.config_bundle["alt_config"])
        self.worker_thread: QThread | None = None
        self.worker: BatchWorker | None = None
        self.current_filters: dict = {}
        self.image_rows: list[dict] = []
        self.station_rows: list[dict] = []
        self.station_model = DictTableModel(STATION_COLUMNS)
        self.image_model = DictTableModel(RESULT_COLUMNS)
        self._build_ui()
        self._auto_load_default_config()
        self._auto_load_django_db()
        self._load_projects()
        self.refresh_results()

    def _build_ui(self):
        self._build_menu()
        self._build_toolbar()
        root = QWidget(); self.setCentralWidget(root); lay = QVBoxLayout(root)
        top = QWidget(); top_l = QHBoxLayout(top)
        self.project_combo = QComboBox(); self.project_combo.currentIndexChanged.connect(self._on_project_changed)
        self.project_db_edit = QLineEdit(); self.project_db_edit.setReadOnly(True)
        self.folder_edit = QLineEdit(); btn_folder = QPushButton("Folder..."); btn_folder.clicked.connect(self.browse_folder)
        self.mask_edit = QLineEdit("*.png"); self.include_subfolders_chk = QCheckBox("Include subfolders")
        for w in [QLabel("Project"), self.project_combo, QLabel("DB"), self.project_db_edit, QLabel("Folder"), self.folder_edit, btn_folder, QLabel("Mask"), self.mask_edit, self.include_subfolders_chk]: top_l.addWidget(w)
        lay.addWidget(top)
        splitter = QSplitter(Qt.Vertical)
        self.station_table = QTableView(); self.station_table.setModel(self.station_model); self.station_table.setSortingEnabled(True); self.station_table.setSelectionBehavior(QAbstractItemView.SelectRows); self.station_table.setSelectionMode(QAbstractItemView.SingleSelection); self.station_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.station_table.selectionModel().selectionChanged.connect(self._on_station_selected)
        self.image_table = QTableView(); self.image_table.setModel(self.image_model); self.image_table.setSortingEnabled(True); self.image_table.setSelectionBehavior(QAbstractItemView.SelectRows); self.image_table.setSelectionMode(QAbstractItemView.SingleSelection); self.image_table.doubleClicked.connect(self.open_image_from_row)
        self.image_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        splitter.addWidget(self.station_table); splitter.addWidget(self.image_table); splitter.setStretchFactor(0, 3); splitter.setStretchFactor(1, 2)
        lay.addWidget(splitter, 1)
        bottom = QWidget(); bottom_l = QVBoxLayout(bottom)
        self.progress_label = QLabel("Idle"); self.log_edit = QTextEdit(); self.log_edit.setReadOnly(True); self.log_edit.setMaximumHeight(150)
        bottom_l.addWidget(self.progress_label); bottom_l.addWidget(self.log_edit)
        lay.addWidget(bottom)

    def _build_menu(self):
        menu = self.menuBar()
        filem = menu.addMenu("File")
        filem.addAction("Open folder...", self.browse_folder)
        filem.addAction("Export CSV", self.export_results_csv)
        filem.addAction("Export TXT", self.export_results_txt)
        filem.addSeparator(); filem.addAction("Exit", self.close)
        settings = menu.addMenu("Settings")
        settings.addAction("Config settings", self.show_config_dialog)
        settings.addAction("ROI editor (main)", self.edit_rois)
        settings.addAction("ROI editor (alt)", self.edit_alt_rois)
        settings.addAction("Filters", self.show_filter_dialog)
        tools = menu.addMenu("Tools")
        tools.addAction("Run QC", self.run_batch)
        tools.addAction("Stop", self.stop_batch)
        tools.addAction("Mark selected station checked", lambda: self.set_selected_station_checked(True))
        tools.addAction("Mark selected station unchecked", lambda: self.set_selected_station_checked(False))
        view = menu.addMenu("View")
        view.addAction("Refresh results", self.refresh_results)
        view.addAction("Clear filters", self.clear_filters)
        view.addAction("Station map", self.show_station_map)

    def _build_toolbar(self):
        tb = QToolBar("Main")
        self.addToolBar(tb)

        self.act_run = QAction("Run", self)
        self.act_run.triggered.connect(self.run_batch)

        self.act_stop = QAction("Stop", self)
        self.act_stop.triggered.connect(self.stop_batch)

        self.act_config = QAction("Config", self)
        self.act_config.triggered.connect(self.show_config_dialog)

        self.act_filters = QAction("Filters", self)
        self.act_filters.triggered.connect(self.show_filter_dialog)

        self.act_refresh = QAction("Refresh", self)
        self.act_refresh.triggered.connect(self.refresh_results)

        self.act_map = QAction("Map", self)
        self.act_map.triggered.connect(self.show_station_map)

        tb.addAction(self.style().standardIcon(QStyle.SP_ArrowForward), "Run QC", self.run_batch)

        tb.addAction(self.style().standardIcon(QStyle.SP_BrowserStop), "Stop", self.stop_batch)

        tb.addAction(self.style().standardIcon(QStyle.SP_BrowserReload), "Refresh", self.refresh_results)

        tb.addAction(self.style().standardIcon(QStyle.SP_FileDialogDetailedView), "Filters", self.show_filter_dialog)

        tb.addAction(self.style().standardIcon(QStyle.SP_FileDialogContentsView), "Config", self.show_config_dialog)

        tb.addAction(self.style().standardIcon(QStyle.SP_DriveNetIcon), "Map", self.show_station_map)

    def _auto_load_default_config(self):
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        config_path = os.path.join(base_dir, "configs", "default_config.json")
        if os.path.exists(config_path):
            self.config_bundle = load_config_bundle(config_path)
            self.current_config = OCRConfig.from_dict(self.config_bundle["main_config"])
            self.alt_config = OCRConfig.from_dict(self.config_bundle["alt_config"])
            self.current_config_path = config_path
            self._log(f"Default config loaded: {config_path}")

    def _auto_load_django_db(self):
        if self.django_db: return
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        db_path = os.path.join(base_dir, "db.sqlite3")
        if os.path.exists(db_path):
            self.django_db = db_path; self._log(f"Django DB loaded: {db_path}")

    def _load_projects(self):
        self.project_combo.clear()
        if not self.django_db: return
        try:
            self.projects = load_projects(self.django_db)
        except Exception as e:
            self._log(f"Cannot load projects: {e}"); self.projects=[]; return
        for p in self.projects:
            self.project_combo.addItem(p["name"], p)

    def _on_project_changed(self):
        data = self.project_combo.currentData()
        self.project_db_edit.setText(data["db_path"] if data else "")
        if data and data.get("db_path"):
            ensure_schema(data["db_path"])
            self.refresh_results()

    def browse_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Choose image folder")
        if folder: self.folder_edit.setText(folder)

    def show_config_dialog(self):
        dlg = ConfigDialog(self.config_bundle.copy(), self)
        if dlg.exec():
            self.config_bundle = dlg.updated_bundle()
            self.current_config = OCRConfig.from_dict(self.config_bundle["main_config"])
            self.alt_config = OCRConfig.from_dict(self.config_bundle["alt_config"])
            if not self.current_config_path:
                self.current_config_path = str(Path(__file__).resolve().parents[1] / "configs" / "default_config.json")
            save_config_bundle(self.config_bundle, self.current_config_path)
            self._log("Config saved")

    def show_filter_dialog(self):
        rov_values, rov1_values = self._load_filter_rov_values()
        dlg = FilterDialog(self.current_filters, rov_values=rov_values, rov1_values=rov1_values, parent=self)
        if dlg.exec():
            self.current_filters = dlg.values()
            self.refresh_results()

    def clear_filters(self):
        self.current_filters = {}
        self.refresh_results()

    def edit_rois(self):
        dlg = RoiEditorDialog(self.current_config, self)
        if dlg.exec():
            self.current_config = dlg.get_config(); self.config_bundle["main_config"] = self.current_config.to_dict(); self._save_bundle(); self._log("Main ROI updated")

    def edit_alt_rois(self):
        dlg = RoiEditorDialog(self.alt_config, self)
        if dlg.exec():
            self.alt_config = dlg.get_config(); self.config_bundle["alt_config"] = self.alt_config.to_dict(); self._save_bundle(); self._log("Alt ROI updated")

    def _save_bundle(self):
        if not self.current_config_path:
            self.current_config_path = str(Path(__file__).resolve().parents[1] / "configs" / "default_config.json")
        save_config_bundle(self.config_bundle, self.current_config_path)

    def _masks_from_ui(self) -> List[str]:
        parts = [p.strip() for p in self.mask_edit.text().replace(",", ";").split(";")]
        return [p for p in parts if p] or ["*.png"]

    def run_batch(self):
        if not self.current_config_path:
            self.show_config_dialog()
        if not self.folder_edit.text().strip():
            QMessageBox.warning(self, "Folder required", "Please select a folder."); return
        settings = BatchSettings(project_db_path=self.project_db_edit.text().strip(), folder=self.folder_edit.text().strip(), config_path=self.current_config_path, include_subfolders=self.include_subfolders_chk.isChecked(), masks=self._masks_from_ui())
        self.worker_thread = QThread(self); self.worker = BatchWorker(settings); self.worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.worker.run); self.worker.progress.connect(self._on_progress); self.worker.finished.connect(self._on_finished); self.worker.failed.connect(self._on_failed)
        self.worker.finished.connect(self.worker_thread.quit); self.worker.failed.connect(self.worker_thread.quit); self.worker_thread.start(); self._log("Started batch QC...")

    def stop_batch(self):
        if self.worker: self.worker.cancel(); self._log("Stop requested...")

    def _on_progress(self, current: int, total: int, filename: str):
        self.progress_label.setText(f"{current} / {total} - {filename}")

    def _on_finished(self, payload: dict):
        self._log(f"Finished. Processed: {payload.get('processed',0)}")
        self.progress_label.setText("Finished")
        self.refresh_results()

    def _on_failed(self, message: str):
        self._log(f"Failed: {message}")
        QMessageBox.critical(self, "Batch failed", message)


    def _load_filter_rov_values(self):
        db_path = self.project_db_edit.text().strip()
        if not db_path or not os.path.exists(db_path):
            return [], []
        try:
            values = load_distinct_rov_values(db_path)
            return values.get("rov", []), values.get("rov1", [])
        except Exception as exc:
            self._log(f"Cannot load ROV filters from DSR: {exc}")
            return [], []

    def _select_first_station(self):
        if self.station_model.rowCount() <= 0:
            self.image_model.set_rows([])
            return
        self.station_table.selectRow(0)
        self._on_station_selected()

    def refresh_results(self):
        db_path = self.project_db_edit.text().strip()
        if not db_path or not os.path.exists(db_path):
            self.station_model.set_rows([])
            self.image_model.set_rows([])
            return
        rows = fetch_results(db_path, **self._db_filter_args())
        self.image_rows = rows
        self.station_rows = self._group_station_rows(rows)
        self.station_model.set_rows(self.station_rows)
        self._select_first_station()

    def _db_filter_args(self):
        f = self.current_filters.copy()
        if f.get("deploy_mode") == "any": f.pop("deploy_day", None); f.pop("deploy_from", None); f.pop("deploy_to", None)
        if f.get("recover_mode") == "any": f.pop("recover_day", None); f.pop("recover_from", None); f.pop("recover_to", None)
        f.pop("deploy_mode", None); f.pop("recover_mode", None)
        return f

    def _group_station_rows(self, rows: list[dict]) -> list[dict]:
        grouped = {}
        for row in rows:
            key = (str(row.get("file_line") or row.get("line") or ""), str(row.get("file_station") or row.get("station") or ""))
            if key == ("", ""): continue
            item = grouped.setdefault(key, {"checked": 1, "file_line": key[0], "file_station": key[1], "images": 0, "expected": row.get("expected_images", ""), "station_status": row.get("station_status", ""), "status": row.get("status", ""), "dsr_line": row.get("dsr_line", ""), "dsr_station": row.get("dsr_station", ""), "dsr_timestamp": row.get("dsr_timestamp", ""), "dsr_timestamp1": row.get("dsr_timestamp1", ""), "dsr_rov": row.get("dsr_rov", ""), "dsr_rov1": row.get("dsr_rov1", ""), "delta_m": row.get("delta_m", ""), "message": row.get("message", "")})
            item["images"] += 1; item["checked"] = 1 if item["checked"] and bool(row.get("checked", 1)) else 0
            if self._status_rank(row.get("status","")) > self._status_rank(item.get("status","")): item["status"] = row.get("status","")
            if self._status_rank(row.get("station_status","")) > self._status_rank(item.get("station_status","")): item["station_status"] = row.get("station_status","")
            try:
                if float(row.get("delta_m") or -1) > float(item.get("delta_m") or -1): item["delta_m"] = row.get("delta_m", "")
            except Exception: pass
            if row.get("message"): item["message"] = row["message"]
        return sorted(grouped.values(), key=lambda r: (r["file_line"], r["file_station"]))

    def _status_rank(self, s: str) -> int:
        order = {"OK":0, "DEPLOYMENT_OK":1, "COMPLETE_OK":2, "WARNING":3, "DEPLOYMENT_INCOMPLETE":4, "RECOVERY_INCOMPLETE":5, "NO_COORDS":6, "NO_DSR":7, "BAD_FILENAME":8, "BAD_RESOLUTION":9, "ERROR":10, "TOO_MANY":11}
        return order.get(str(s), 0)

    def _on_station_selected(self):
        idxs = self.station_table.selectionModel().selectedRows()
        if not idxs:
            self.image_model.set_rows([]); return
        row = self.station_model.rows[idxs[0].row()]
        line = str(row.get("file_line", "")); station = str(row.get("file_station", ""))
        subset = [r for r in self.image_rows if str(r.get("file_line") or r.get("line") or "") == line and str(r.get("file_station") or r.get("station") or "") == station]
        self.image_model.set_rows(subset)

    def set_selected_station_checked(self, checked: bool):
        db_path = self.project_db_edit.text().strip()
        idxs = self.station_table.selectionModel().selectedRows()
        if not idxs or not db_path: return
        row = self.station_model.rows[idxs[0].row()]
        line = str(row.get("file_line", "")); station = str(row.get("file_station", ""))
        paths = [r["image_path"] for r in self.image_rows if str(r.get("file_line") or r.get("line") or "") == line and str(r.get("file_station") or r.get("station") or "") == station]
        set_checked(db_path, paths, checked); self.refresh_results()

    def export_results_csv(self):
        if not self.image_rows: return
        path, _ = QFileDialog.getSaveFileName(self, "Export CSV", "ocr_results.csv", "CSV (*.csv)")
        if not path: return
        export_csv(self.image_rows, path)
        export_csv(self.station_rows, str(Path(path).with_name(Path(path).stem + "_stations.csv")), STATION_COLUMNS)
        self._log(f"Exported CSV: {path}")

    def export_results_txt(self):
        if not self.image_rows: return
        path, _ = QFileDialog.getSaveFileName(self, "Export TXT", "ocr_results.txt", "Text (*.txt)")
        if not path: return
        export_txt(self.image_rows, path); self._log(f"Exported TXT: {path}")

    def open_image_from_row(self, index):
        if not index.isValid(): return
        row = self.image_model.rows[index.row()]
        image_path = row.get("image_path", "")
        if not image_path or not os.path.exists(image_path): return
        try:
            if os.name == "nt": os.startfile(image_path)
            else: subprocess.Popen(["xdg-open", image_path])
        except Exception as exc: self._log(f"Cannot open image: {exc}")

    def _log(self, text: str):
        self.log_edit.append(text)

    def show_station_map(self):
        db_path = self.project_db_edit.text().strip()
        if not db_path or not os.path.exists(db_path):
            QMessageBox.warning(self, "No DB", "Project DB not found.")
            return

        selected_line = ""
        idxs = self.station_table.selectionModel().selectedRows()
        if idxs:
            row = self.station_model.rows[idxs[0].row()]
            selected_line = str(row.get("dsr_line") or row.get("file_line") or "")

        try:
            preplot_rows = load_preplot_points(db_path)
            dsr_rows = load_dsr_station_points_from_ocr(db_path)

            self.map_window = StationMapWindow(preplot_rows, dsr_rows, self)
            self.map_window.stationClicked.connect(self._select_station_from_map)
            self.map_window.show()
            self.map_window.raise_()
            self.map_window.activateWindow()

            self._log(
                f"Map opened. Preplot points: {len(preplot_rows)}, "
                f"DSR stations: {len(dsr_rows)}, line={selected_line or 'ALL'}"
            )
        except Exception as exc:
            QMessageBox.critical(self, "Map error", str(exc))
            self._log(f"Map error: {exc}")

    def _select_station_from_map(self, line: str, station: str):

        line = str(line).strip()
        station = str(station).strip()

        if not hasattr(self.station_model, "rows"):
            return

        for row_idx, row in enumerate(self.station_model.rows):

            row_line = str(
                row.get("dsr_line")
                or row.get("file_line")
                or row.get("line")
                or ""
            ).strip()

            row_station = str(
                row.get("dsr_station")
                or row.get("file_station")
                or row.get("station")
                or ""
            ).strip()

            if row_line == line and row_station == station:
                model_index = self.station_model.index(row_idx, 0)

                self.station_table.clearSelection()
                self.station_table.selectRow(row_idx)
                self.station_table.scrollTo(model_index)

                return

    def _load_images_for_station_row(self, row_idx: int):
        if row_idx < 0 or row_idx >= len(self.station_model.rows):
            return

        row = self.station_model.rows[row_idx]

        line = str(row.get("dsr_line") or row.get("file_line") or row.get("line") or "").strip()
        station = str(row.get("dsr_station") or row.get("file_station") or row.get("station") or "").strip()

        images = self.db.get_images_for_station(line, station)  # adapt to your real DB function
        self.image_model.set_rows(images)