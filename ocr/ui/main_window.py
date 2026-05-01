from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import List
from PySide6.QtGui import QAction, QIcon
from ..core.map_loader import load_preplot_points, load_dsr_station_points_from_ocr
from .map_window import StationMapWindow

from PySide6.QtCore import QThread, Qt, QDate, QSize
from PySide6.QtWidgets import (
    QAbstractItemView, QCheckBox, QComboBox, QDateEdit, QDialog, QFileDialog, QFormLayout, QFrame,
    QGridLayout, QGroupBox, QHBoxLayout, QLabel, QLineEdit, QMainWindow, QMessageBox, QPushButton,
    QSizePolicy, QSplitter, QStyle, QTableView, QTextEdit, QToolBar, QVBoxLayout, QWidget, QSpinBox,
    QDoubleSpinBox, QDialogButtonBox, QHeaderView, QInputDialog, QMenu
)

from ..core.batch_processor import BatchSettings, BatchWorker
from ..core.config_manager import default_bundle, load_config_bundle, save_config_bundle
from ..core.exporters import RESULT_COLUMNS, SUMMARY_COLUMNS, export_csv, export_txt
from ..core.image_scanner import scan_images
from ..core.models import OCRConfig
from ..core.ocr_db import fetch_results, set_checked, ensure_schema, delete_results_by_paths, delete_results_by_station_keys, delete_results_by_rov, reset_checked_for_paths, distinct_ocr_values, fetch_unchecked_existing_image_paths
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

RESULT_TABLE_COLUMNS = ["selected"] + RESULT_COLUMNS

STATION_COLUMNS = [
    "selected", "checked", "file_line", "file_station", "images", "expected", "station_status", "status",
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
        self.setWindowTitle("SeisWebLog OCR v5 - ROV Overlay QC")
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
        self.image_model = DictTableModel(RESULT_TABLE_COLUMNS)
        self.quick_filtered_station_rows: list[dict] = []
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
        self._build_summary_cards(lay)
        self._build_quick_filters(lay)
        splitter = QSplitter(Qt.Vertical)
        self.station_table = QTableView(); self.station_table.setModel(self.station_model); self.station_table.setSortingEnabled(True); self.station_table.setSelectionBehavior(QAbstractItemView.SelectRows); self.station_table.setSelectionMode(QAbstractItemView.ExtendedSelection); self.station_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.station_table.selectionModel().selectionChanged.connect(self._on_station_selected)
        self.station_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.station_table.customContextMenuRequested.connect(self._station_context_menu)
        self.image_table = QTableView(); self.image_table.setModel(self.image_model); self.image_table.setSortingEnabled(True); self.image_table.setSelectionBehavior(QAbstractItemView.SelectRows); self.image_table.setSelectionMode(QAbstractItemView.ExtendedSelection); self.image_table.doubleClicked.connect(self.open_image_from_row)
        self.image_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.image_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.image_table.customContextMenuRequested.connect(self._image_context_menu)
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
        dbm = menu.addMenu("Database")
        dbm.addAction("Delete selected station rows from DB", self.delete_selected_station_rows)
        dbm.addAction("Delete selected image rows from DB", self.delete_selected_image_rows)
        dbm.addAction("Delete all visible/filtered rows from DB", self.delete_filtered_rows)
        dbm.addAction("Delete by ROV...", self.delete_by_rov_dialog)
        dbm.addSeparator()
        dbm.addAction("Reset selected checked status", lambda: self.reset_selected_checked(False))
        dbm.addAction("Set selected checked status", lambda: self.reset_selected_checked(True))
        dbm.addSeparator()
        dbm.addAction("Re-check all unchecked files from DB", self.recheck_all_unchecked_from_db)

        selm = menu.addMenu("Selection")
        posm = selm.addMenu("Positions")
        posm.addAction("Tick all visible positions", lambda: self._tick_all_visible_positions(True))
        posm.addAction("Untick all visible positions", lambda: self._tick_all_visible_positions(False))
        posm.addAction("Invert visible position ticks", self._invert_visible_position_ticks)
        posm.addSeparator()
        posm.addAction("Tick highlighted positions", lambda: self._set_station_selected(True))
        posm.addAction("Untick highlighted positions", lambda: self._set_station_selected(False))
        posm.addSeparator()
        posm.addAction("Tick positions by status...", self._tick_positions_by_status_dialog)
        posm.addAction("Tick positions by ROV / ROV1...", self._tick_positions_by_rov_dialog)
        posm.addAction("Tick checked positions", self._tick_checked_positions)
        posm.addAction("Tick unchecked positions", self._tick_unchecked_positions)

        imgm = selm.addMenu("Images")
        imgm.addAction("Tick all visible images", lambda: self._tick_all_visible_images(True))
        imgm.addAction("Untick all visible images", lambda: self._tick_all_visible_images(False))
        imgm.addAction("Invert visible image ticks", self._invert_visible_image_ticks)
        imgm.addSeparator()
        imgm.addAction("Tick highlighted images", lambda: self._set_image_selected(True))
        imgm.addAction("Untick highlighted images", lambda: self._set_image_selected(False))
        imgm.addSeparator()
        imgm.addAction("Tick images for ticked positions", self._tick_images_for_ticked_positions)
        imgm.addAction("Tick positions for ticked images", self._tick_positions_for_ticked_images)
        imgm.addAction("Tick checked images", self._tick_checked_images)
        imgm.addAction("Tick unchecked images", self._tick_unchecked_images)

        selm.addSeparator()
        selm.addAction("Clear all position and image ticks", self._clear_all_ticks)
        selm.addAction("Show selection counts", self._show_selection_counts)
        selm.addSeparator()
        selm.addAction("Re-check ticked/selected positions", self.recheck_selected_positions)
        selm.addAction("Re-check ticked/selected images", self.recheck_selected_images)
        selm.addAction("Delete ticked/selected positions from DB", self.delete_ticked_station_rows)
        selm.addAction("Delete ticked/selected images from DB", self.delete_ticked_image_rows)
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

        tb.addSeparator()
        tb.addAction(self.style().standardIcon(QStyle.SP_TrashIcon), "Delete selected", self.delete_selected_station_rows)

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
        for r in rows:
            r.setdefault("selected", 0)
        self.image_rows = rows
        self.station_rows = self._group_station_rows(rows)
        self._apply_quick_filters()

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
            item = grouped.setdefault(key, {"selected": 0, "checked": 1, "file_line": key[0], "file_station": key[1], "images": 0, "expected": row.get("expected_images", ""), "station_status": row.get("station_status", ""), "status": row.get("status", ""), "dsr_line": row.get("dsr_line", ""), "dsr_station": row.get("dsr_station", ""), "dsr_timestamp": row.get("dsr_timestamp", ""), "dsr_timestamp1": row.get("dsr_timestamp1", ""), "dsr_rov": row.get("dsr_rov", ""), "dsr_rov1": row.get("dsr_rov1", ""), "delta_m": row.get("delta_m", ""), "message": row.get("message", "")})
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
        self._highlight_current_station_on_map(center=False)

    def set_selected_station_checked(self, checked: bool):
        db_path = self.project_db_edit.text().strip()
        keys = set(self._selected_station_keys())
        if not keys or not db_path:
            return
        paths = []
        for r in self.image_rows:
            key = (str(r.get("file_line") or r.get("line") or r.get("dsr_line") or ""), str(r.get("file_station") or r.get("station") or r.get("dsr_station") or ""))
            if key in keys and r.get("image_path"):
                paths.append(r["image_path"])
        set_checked(db_path, paths, checked)
        self._log(f"Marked {len(paths)} image rows as {'checked' if checked else 'unchecked'}")
        self.refresh_results()

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


    def _build_summary_cards(self, parent_layout):
        row = QWidget()
        lay = QHBoxLayout(row)
        lay.setContentsMargins(0, 0, 0, 0)
        self.summary_total = QLabel("Total: 0")
        self.summary_visible = QLabel("Visible: 0")
        self.summary_checked = QLabel("Checked: 0")
        self.summary_errors = QLabel("Errors: 0")
        self.summary_nodsr = QLabel("No DSR: 0")
        for w in [self.summary_total, self.summary_visible, self.summary_checked, self.summary_errors, self.summary_nodsr]:
            w.setFrameShape(QFrame.StyledPanel)
            w.setStyleSheet("QLabel { padding: 6px 10px; font-weight: 600; background: #f6f8fa; border-radius: 6px; }")
            lay.addWidget(w)
        lay.addStretch()
        parent_layout.addWidget(row)

    def _build_quick_filters(self, parent_layout):
        bar = QWidget()
        lay = QHBoxLayout(bar)
        lay.setContentsMargins(0, 0, 0, 0)
        self.quick_search_edit = QLineEdit()
        self.quick_search_edit.setPlaceholderText("Quick search: line / station / ROV / status / message")
        self.quick_status_combo = QComboBox()
        self.quick_status_combo.addItems(["All", "OK", "Warning", "Error", "Incomplete", "No DSR", "Bad filename", "Checked", "Not checked"])
        self.quick_rov_combo = QComboBox()
        self.quick_rov_combo.addItem("All ROV")
        self.quick_delta_combo = QComboBox()
        self.quick_delta_combo.addItems(["Any delta", "Delta > 1m", "Delta > 2m", "Delta > 5m", "Delta > 10m"])
        self.quick_clear_btn = QPushButton("Clear")
        self.quick_clear_btn.clicked.connect(self._clear_quick_filters)
        self.quick_search_edit.textChanged.connect(self._apply_quick_filters)
        self.quick_status_combo.currentTextChanged.connect(self._apply_quick_filters)
        self.quick_rov_combo.currentTextChanged.connect(self._apply_quick_filters)
        self.quick_delta_combo.currentTextChanged.connect(self._apply_quick_filters)
        lay.addWidget(QLabel("Filter"))
        lay.addWidget(self.quick_search_edit, 2)
        lay.addWidget(self.quick_status_combo)
        lay.addWidget(self.quick_rov_combo)
        lay.addWidget(self.quick_delta_combo)
        lay.addWidget(self.quick_clear_btn)
        parent_layout.addWidget(bar)

    def _refresh_quick_rov_values(self):
        if not hasattr(self, "quick_rov_combo"):
            return
        current = self.quick_rov_combo.currentText()
        values = sorted({str(r.get("dsr_rov") or r.get("rov") or "").strip() for r in self.station_rows if str(r.get("dsr_rov") or r.get("rov") or "").strip()})
        self.quick_rov_combo.blockSignals(True)
        self.quick_rov_combo.clear()
        self.quick_rov_combo.addItem("All ROV")
        self.quick_rov_combo.addItems(values)
        if current in ["All ROV"] + values:
            self.quick_rov_combo.setCurrentText(current)
        self.quick_rov_combo.blockSignals(False)

    def _clear_quick_filters(self):
        self.quick_search_edit.clear()
        self.quick_status_combo.setCurrentText("All")
        self.quick_rov_combo.setCurrentText("All ROV")
        self.quick_delta_combo.setCurrentText("Any delta")
        self._apply_quick_filters()

    def _apply_quick_filters(self):
        rows = list(self.station_rows or [])
        self._refresh_quick_rov_values()
        text = self.quick_search_edit.text().strip().lower() if hasattr(self, "quick_search_edit") else ""
        status_filter = self.quick_status_combo.currentText() if hasattr(self, "quick_status_combo") else "All"
        rov_filter = self.quick_rov_combo.currentText() if hasattr(self, "quick_rov_combo") else "All ROV"
        delta_filter = self.quick_delta_combo.currentText() if hasattr(self, "quick_delta_combo") else "Any delta"

        if text:
            rows = [r for r in rows if text in " ".join(str(v).lower() for v in r.values())]
        if rov_filter and rov_filter != "All ROV":
            rows = [r for r in rows if rov_filter in {str(r.get("dsr_rov") or ""), str(r.get("dsr_rov1") or ""), str(r.get("rov") or "")}]
        if status_filter != "All":
            def svalue(r): return str(r.get("station_status") or r.get("status") or "").upper()
            if status_filter == "OK": rows = [r for r in rows if svalue(r) in {"OK", "COMPLETE_OK", "DEPLOYMENT_OK"}]
            elif status_filter == "Warning": rows = [r for r in rows if "WARNING" in svalue(r)]
            elif status_filter == "Error": rows = [r for r in rows if "ERROR" in svalue(r)]
            elif status_filter == "Incomplete": rows = [r for r in rows if "INCOMPLETE" in svalue(r)]
            elif status_filter == "No DSR": rows = [r for r in rows if svalue(r) == "NO_DSR"]
            elif status_filter == "Bad filename": rows = [r for r in rows if svalue(r) == "BAD_FILENAME"]
            elif status_filter == "Checked": rows = [r for r in rows if bool(r.get("checked"))]
            elif status_filter == "Not checked": rows = [r for r in rows if not bool(r.get("checked"))]
        limits = {"Delta > 1m": 1, "Delta > 2m": 2, "Delta > 5m": 5, "Delta > 10m": 10}
        if delta_filter in limits:
            lim = limits[delta_filter]
            def delta_ok(r):
                try: return float(r.get("delta_m") or 0) > lim
                except Exception: return False
            rows = [r for r in rows if delta_ok(r)]

        self.quick_filtered_station_rows = rows
        self.station_model.set_rows(rows)
        self._update_summary_cards(rows)
        self._select_first_station()

    def _update_summary_cards(self, visible_rows=None):
        rows = self.station_rows or []
        visible_rows = visible_rows if visible_rows is not None else rows
        checked = sum(1 for r in rows if bool(r.get("checked")))
        errors = sum(1 for r in rows if "ERROR" in str(r.get("station_status") or r.get("status") or "").upper())
        nodsr = sum(1 for r in rows if str(r.get("station_status") or r.get("status") or "").upper() == "NO_DSR")
        if hasattr(self, "summary_total"):
            self.summary_total.setText(f"Total: {len(rows)}")
            self.summary_visible.setText(f"Visible: {len(visible_rows)}")
            self.summary_checked.setText(f"Checked: {checked}")
            self.summary_errors.setText(f"Errors: {errors}")
            self.summary_nodsr.setText(f"No DSR: {nodsr}")

    def _selected_station_keys(self) -> list[tuple[str, str]]:
        keys = []
        for idx in self.station_table.selectionModel().selectedRows():
            if 0 <= idx.row() < len(self.station_model.rows):
                r = self.station_model.rows[idx.row()]
                keys.append((str(r.get("file_line") or r.get("dsr_line") or r.get("line") or ""), str(r.get("file_station") or r.get("dsr_station") or r.get("station") or "")))
        return keys

    def _visible_image_paths(self) -> list[str]:
        keys = set((str(r.get("file_line") or r.get("dsr_line") or r.get("line") or ""), str(r.get("file_station") or r.get("dsr_station") or r.get("station") or "")) for r in self.station_model.rows)
        paths = []
        for r in self.image_rows:
            key = (str(r.get("file_line") or r.get("line") or r.get("dsr_line") or ""), str(r.get("file_station") or r.get("station") or r.get("dsr_station") or ""))
            if key in keys and r.get("image_path"):
                paths.append(r["image_path"])
        return paths

    def delete_selected_station_rows(self):
        db_path = self.project_db_edit.text().strip()
        keys = self._selected_station_keys()
        if not db_path or not keys:
            return
        if QMessageBox.question(self, "Delete from DB", f"Delete OCR DB rows for {len(keys)} selected station(s)?") != QMessageBox.Yes:
            return
        count = delete_results_by_station_keys(db_path, keys)
        self._log(f"Deleted {count} OCR rows for selected stations")
        self.refresh_results()

    def delete_selected_image_rows(self):
        db_path = self.project_db_edit.text().strip()
        paths = []
        for idx in self.image_table.selectionModel().selectedRows():
            if 0 <= idx.row() < len(self.image_model.rows):
                p = self.image_model.rows[idx.row()].get("image_path")
                if p: paths.append(p)
        if not db_path or not paths:
            return
        if QMessageBox.question(self, "Delete from DB", f"Delete {len(paths)} selected image row(s) from DB?") != QMessageBox.Yes:
            return
        count = delete_results_by_paths(db_path, paths)
        self._log(f"Deleted {count} selected image rows")
        self.refresh_results()

    def delete_filtered_rows(self):
        db_path = self.project_db_edit.text().strip()
        paths = self._visible_image_paths()
        if not db_path or not paths:
            return
        if QMessageBox.question(self, "Delete filtered rows", f"Delete all visible/filtered OCR image rows from DB?\nRows: {len(paths)}") != QMessageBox.Yes:
            return
        count = delete_results_by_paths(db_path, paths)
        self._log(f"Deleted {count} visible/filtered rows")
        self.refresh_results()

    def delete_by_rov_dialog(self):
        db_path = self.project_db_edit.text().strip()
        if not db_path:
            return
        values = sorted(set(distinct_ocr_values(db_path, "dsr_rov") + distinct_ocr_values(db_path, "dsr_rov1") + distinct_ocr_values(db_path, "rov")))
        if not values:
            QMessageBox.information(self, "Delete by ROV", "No ROV values found in OCR results.")
            return
        rov, ok = QInputDialog.getItem(self, "Delete by ROV", "ROV / ROV1 value", values, 0, False)
        if not ok or not rov:
            return
        if QMessageBox.question(self, "Delete by ROV", f"Delete all OCR DB rows for ROV/ROV1 '{rov}'?") != QMessageBox.Yes:
            return
        count = delete_results_by_rov(db_path, rov, include_rov1=True)
        self._log(f"Deleted {count} rows for ROV/ROV1 {rov}")
        self.refresh_results()

    def reset_selected_checked(self, checked: bool):
        db_path = self.project_db_edit.text().strip()
        paths = []
        keys = set(self._selected_station_keys())
        for r in self.image_rows:
            key = (str(r.get("file_line") or r.get("line") or r.get("dsr_line") or ""), str(r.get("file_station") or r.get("station") or r.get("dsr_station") or ""))
            if key in keys and r.get("image_path"):
                paths.append(r["image_path"])
        if not db_path or not paths:
            return
        count = reset_checked_for_paths(db_path, paths, checked=checked)
        self._log(f"Updated checked={int(checked)} for {count} selected image rows")
        self.refresh_results()

    def _selected_or_ticked_station_keys(self):
        keys = set(self._selected_station_keys())
        for r in getattr(self.station_model, 'rows', []):
            if bool(r.get('selected')):
                keys.add((str(r.get('file_line') or r.get('dsr_line') or ''), str(r.get('file_station') or r.get('dsr_station') or '')))
        return [k for k in keys if k != ('', '')]

    def _selected_or_ticked_image_paths(self):
        paths = set()
        sel = self.image_table.selectionModel().selectedRows() if self.image_table.selectionModel() else []
        for idx in sel:
            if 0 <= idx.row() < len(self.image_model.rows):
                p = self.image_model.rows[idx.row()].get('image_path')
                if p: paths.add(p)
        for r in getattr(self.image_model, 'rows', []):
            if bool(r.get('selected')) and r.get('image_path'):
                paths.add(r['image_path'])
        return list(paths)

    def _paths_for_station_keys(self, keys):
        keyset = set(keys)
        paths = []
        for r in self.image_rows:
            key = (str(r.get('file_line') or r.get('line') or r.get('dsr_line') or ''), str(r.get('file_station') or r.get('station') or r.get('dsr_station') or ''))
            if key in keyset and r.get('image_path'):
                paths.append(r['image_path'])
        return paths

    # ---------------- Selection menu helpers ----------------
    def _tick_all_visible_positions(self, selected: bool):
        for r in getattr(self.station_model, "rows", []):
            r["selected"] = 1 if selected else 0
        self.station_model.layoutChanged.emit()
        self._show_selection_counts(log_only=True)

    def _tick_all_visible_images(self, selected: bool):
        for r in getattr(self.image_model, "rows", []):
            r["selected"] = 1 if selected else 0
        self.image_model.layoutChanged.emit()
        self._show_selection_counts(log_only=True)

    def _invert_visible_position_ticks(self):
        for r in getattr(self.station_model, "rows", []):
            r["selected"] = 0 if bool(r.get("selected")) else 1
        self.station_model.layoutChanged.emit()
        self._show_selection_counts(log_only=True)

    def _invert_visible_image_ticks(self):
        for r in getattr(self.image_model, "rows", []):
            r["selected"] = 0 if bool(r.get("selected")) else 1
        self.image_model.layoutChanged.emit()
        self._show_selection_counts(log_only=True)

    def _tick_positions_by_status_dialog(self):
        statuses = sorted({str(r.get("station_status") or r.get("status") or "").strip() for r in getattr(self.station_model, "rows", []) if str(r.get("station_status") or r.get("status") or "").strip()})
        if not statuses:
            QMessageBox.information(self, "Selection", "No visible statuses found.")
            return
        status, ok = QInputDialog.getItem(self, "Tick positions by status", "Station status:", statuses, 0, False)
        if not ok or not status:
            return
        count = 0
        for r in getattr(self.station_model, "rows", []):
            if str(r.get("station_status") or r.get("status") or "").strip() == status:
                r["selected"] = 1
                count += 1
        self.station_model.layoutChanged.emit()
        self._log(f"Ticked {count} visible positions with status {status}")

    def _tick_positions_by_rov_dialog(self):
        rovs = sorted({str(v).strip() for r in getattr(self.station_model, "rows", []) for v in (r.get("dsr_rov"), r.get("dsr_rov1")) if str(v or "").strip()})
        if not rovs:
            QMessageBox.information(self, "Selection", "No visible ROV / ROV1 values found.")
            return
        rov, ok = QInputDialog.getItem(self, "Tick positions by ROV / ROV1", "ROV:", rovs, 0, False)
        if not ok or not rov:
            return
        count = 0
        for r in getattr(self.station_model, "rows", []):
            if str(r.get("dsr_rov") or "").strip() == rov or str(r.get("dsr_rov1") or "").strip() == rov:
                r["selected"] = 1
                count += 1
        self.station_model.layoutChanged.emit()
        self._log(f"Ticked {count} visible positions for ROV/ROV1 {rov}")

    def _tick_checked_positions(self):
        count = 0
        for r in getattr(self.station_model, "rows", []):
            if bool(r.get("checked")):
                r["selected"] = 1
                count += 1
        self.station_model.layoutChanged.emit()
        self._log(f"Ticked {count} checked positions")

    def _tick_unchecked_positions(self):
        count = 0
        for r in getattr(self.station_model, "rows", []):
            if not bool(r.get("checked")):
                r["selected"] = 1
                count += 1
        self.station_model.layoutChanged.emit()
        self._log(f"Ticked {count} unchecked positions")

    def _tick_checked_images(self):
        count = 0
        for r in getattr(self.image_model, "rows", []):
            if bool(r.get("checked")):
                r["selected"] = 1
                count += 1
        self.image_model.layoutChanged.emit()
        self._log(f"Ticked {count} checked images")

    def _tick_unchecked_images(self):
        count = 0
        for r in getattr(self.image_model, "rows", []):
            if not bool(r.get("checked")):
                r["selected"] = 1
                count += 1
        self.image_model.layoutChanged.emit()
        self._log(f"Ticked {count} unchecked images")

    def _tick_images_for_ticked_positions(self):
        keys = set(self._selected_or_ticked_station_keys())
        if not keys:
            QMessageBox.information(self, "Selection", "No ticked/highlighted positions.")
            return
        count = 0
        for r in getattr(self.image_model, "rows", []):
            key = (str(r.get("file_line") or r.get("line") or r.get("dsr_line") or ""), str(r.get("file_station") or r.get("station") or r.get("dsr_station") or ""))
            if key in keys:
                r["selected"] = 1
                count += 1
        self.image_model.layoutChanged.emit()
        self._log(f"Ticked {count} visible images for ticked/highlighted positions")

    def _tick_positions_for_ticked_images(self):
        keys = set()
        for r in getattr(self.image_model, "rows", []):
            if bool(r.get("selected")):
                key = (str(r.get("file_line") or r.get("line") or r.get("dsr_line") or ""), str(r.get("file_station") or r.get("station") or r.get("dsr_station") or ""))
                if key != ("", ""):
                    keys.add(key)
        if not keys:
            QMessageBox.information(self, "Selection", "No ticked images.")
            return
        count = 0
        for r in getattr(self.station_model, "rows", []):
            key = (str(r.get("file_line") or r.get("dsr_line") or ""), str(r.get("file_station") or r.get("dsr_station") or ""))
            if key in keys:
                r["selected"] = 1
                count += 1
        self.station_model.layoutChanged.emit()
        self._log(f"Ticked {count} visible positions from ticked images")

    def _clear_all_ticks(self):
        for r in getattr(self.station_model, "rows", []):
            r["selected"] = 0
        for r in getattr(self.image_model, "rows", []):
            r["selected"] = 0
        self.station_model.layoutChanged.emit()
        self.image_model.layoutChanged.emit()
        self._log("Cleared all visible position/image ticks")

    def _show_selection_counts(self, log_only: bool = False):
        pos_tick = sum(1 for r in getattr(self.station_model, "rows", []) if bool(r.get("selected")))
        img_tick = sum(1 for r in getattr(self.image_model, "rows", []) if bool(r.get("selected")))
        pos_high = len(self.station_table.selectionModel().selectedRows()) if self.station_table.selectionModel() else 0
        img_high = len(self.image_table.selectionModel().selectedRows()) if self.image_table.selectionModel() else 0
        msg = f"Positions ticked: {pos_tick}, highlighted: {pos_high}; Images ticked: {img_tick}, highlighted: {img_high}"
        self._log(msg)
        if not log_only:
            QMessageBox.information(self, "Selection counts", msg)

    def _station_context_menu(self, pos):
        menu = QMenu(self)
        menu.addAction('Tick selected positions', lambda: self._set_station_selected(True))
        menu.addAction('Untick selected positions', lambda: self._set_station_selected(False))
        menu.addSeparator()
        menu.addAction('Re-check ticked/selected positions', self.recheck_selected_positions)
        menu.addAction('Set ticked/selected positions checked', lambda: self._set_checked_for_ticked_stations(True))
        menu.addAction('Set ticked/selected positions unchecked', lambda: self._set_checked_for_ticked_stations(False))
        menu.addSeparator()
        menu.addAction('Delete ticked/selected positions from DB', self.delete_ticked_station_rows)
        menu.exec(self.station_table.viewport().mapToGlobal(pos))

    def _image_context_menu(self, pos):
        menu = QMenu(self)
        menu.addAction('Tick selected images', lambda: self._set_image_selected(True))
        menu.addAction('Untick selected images', lambda: self._set_image_selected(False))
        menu.addSeparator()
        menu.addAction('Re-check ticked/selected images', self.recheck_selected_images)
        menu.addAction('Set ticked/selected images checked', lambda: self._set_checked_for_ticked_images(True))
        menu.addAction('Set ticked/selected images unchecked', lambda: self._set_checked_for_ticked_images(False))
        menu.addSeparator()
        menu.addAction('Delete ticked/selected images from DB', self.delete_ticked_image_rows)
        menu.exec(self.image_table.viewport().mapToGlobal(pos))

    def _set_station_selected(self, selected: bool):
        rows = self.station_table.selectionModel().selectedRows() if self.station_table.selectionModel() else []
        for idx in rows:
            if 0 <= idx.row() < len(self.station_model.rows):
                self.station_model.rows[idx.row()]['selected'] = 1 if selected else 0
        self.station_model.layoutChanged.emit()

    def _set_image_selected(self, selected: bool):
        rows = self.image_table.selectionModel().selectedRows() if self.image_table.selectionModel() else []
        for idx in rows:
            if 0 <= idx.row() < len(self.image_model.rows):
                self.image_model.rows[idx.row()]['selected'] = 1 if selected else 0
        self.image_model.layoutChanged.emit()

    def _set_checked_for_ticked_stations(self, checked: bool):
        db_path = self.project_db_edit.text().strip()
        paths = self._paths_for_station_keys(self._selected_or_ticked_station_keys())
        if not db_path or not paths: return
        count = reset_checked_for_paths(db_path, paths, checked=checked)
        self._log(f'Updated checked={int(checked)} for {count} image rows from ticked/selected positions')
        self.refresh_results()

    def _set_checked_for_ticked_images(self, checked: bool):
        db_path = self.project_db_edit.text().strip()
        paths = self._selected_or_ticked_image_paths()
        if not db_path or not paths: return
        count = reset_checked_for_paths(db_path, paths, checked=checked)
        self._log(f'Updated checked={int(checked)} for {count} ticked/selected images')
        self.refresh_results()

    def delete_ticked_station_rows(self):
        db_path = self.project_db_edit.text().strip()
        keys = self._selected_or_ticked_station_keys()
        if not db_path or not keys: return
        if QMessageBox.question(self, 'Delete positions', f'Delete OCR DB rows for {len(keys)} ticked/selected positions?') != QMessageBox.Yes: return
        count = delete_results_by_station_keys(db_path, keys)
        self._log(f'Deleted {count} rows for ticked/selected positions')
        self.refresh_results()

    def delete_ticked_image_rows(self):
        db_path = self.project_db_edit.text().strip()
        paths = self._selected_or_ticked_image_paths()
        if not db_path or not paths: return
        if QMessageBox.question(self, 'Delete images', f'Delete {len(paths)} ticked/selected image rows from DB?') != QMessageBox.Yes: return
        count = delete_results_by_paths(db_path, paths)
        self._log(f'Deleted {count} ticked/selected image rows')
        self.refresh_results()

    def _start_explicit_recheck(self, files, label='Re-check'):
        if not self.current_config_path:
            self.show_config_dialog()
        files = [p for p in files if p and os.path.exists(p)]
        if not files:
            QMessageBox.information(self, label, 'No existing image files found to re-check.')
            return
        settings = BatchSettings(
            project_db_path=self.project_db_edit.text().strip(),
            folder='',
            config_path=self.current_config_path,
            include_subfolders=False,
            masks=self._masks_from_ui(),
            explicit_files=files,
            force_recheck=True,
        )
        self.worker_thread = QThread(self)
        self.worker = BatchWorker(settings)
        self.worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.worker.run)
        self.worker.progress.connect(self._on_progress)
        self.worker.finished.connect(self._on_finished)
        self.worker.failed.connect(self._on_failed)
        self.worker.finished.connect(self.worker_thread.quit)
        self.worker.failed.connect(self.worker_thread.quit)
        self.worker_thread.start()
        self._log(f'{label} started: {len(files)} files')

    def recheck_selected_positions(self):
        files = self._paths_for_station_keys(self._selected_or_ticked_station_keys())
        self._start_explicit_recheck(files, 'Re-check selected positions')

    def recheck_selected_images(self):
        self._start_explicit_recheck(self._selected_or_ticked_image_paths(), 'Re-check selected images')

    def recheck_all_unchecked_from_db(self):
        db_path = self.project_db_edit.text().strip()
        if not db_path: return
        files = fetch_unchecked_existing_image_paths(db_path)
        if QMessageBox.question(self, 'Re-check unchecked from DB', f'Re-check all unchecked image files stored in DB?\nExisting files found: {len(files)}\nThis does not use the selected folder.') != QMessageBox.Yes:
            return
        self._start_explicit_recheck(files, 'Re-check all unchecked from DB')

    def _highlight_current_station_on_map(self, center: bool = False):
        if not hasattr(self, "map_window") or self.map_window is None:
            return
        try:
            if not self.map_window.isVisible():
                return
        except Exception:
            return
        idxs = self.station_table.selectionModel().selectedRows() if self.station_table.selectionModel() else []
        if not idxs:
            return
        try:
            row = self.station_model.rows[idxs[0].row()]
        except Exception:
            return
        line = str(row.get("dsr_line") or row.get("file_line") or row.get("line") or "").strip()
        station = str(row.get("dsr_station") or row.get("file_station") or row.get("station") or "").strip()
        if line and station and hasattr(self.map_window, "highlight_station"):
            self.map_window.highlight_station(line, station, center=center)

    def show_station_map(self):
        db_path = self.project_db_edit.text().strip()
        if not db_path or not os.path.exists(db_path):
            QMessageBox.warning(self, "No DB", "Project DB not found.")
            return

        try:
            # v5.3: show the whole OCR database on the map, not only the selected line.
            preplot_rows = load_preplot_points(db_path, None)
            dsr_rows = load_dsr_station_points_from_ocr(db_path, None)

            self.map_window = StationMapWindow(preplot_rows, dsr_rows, self)
            self.map_window.stationClicked.connect(self._select_station_from_map)
            self.map_window.show()
            self.map_window.raise_()
            self.map_window.activateWindow()
            self._highlight_current_station_on_map(center=True)

            self._log(
                f"Map opened. Preplot points: {len(preplot_rows)}, "
                f"DSR stations: {len(dsr_rows)}, line=ALL"
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
