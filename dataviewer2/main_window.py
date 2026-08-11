from __future__ import annotations

from collections import OrderedDict
import csv
import hashlib
import html
from pathlib import Path

import pyqtgraph as pg
import numpy as np
from PySide6 import QtCore, QtGui, QtWidgets

from .layers import FastPointLayer, FastShapeLayer
from .measurement import MeasurementTool
from .models import PointLayerData, ShapeLayerData, BlackBoxData
from .repository import ProjectRepository, ProjectRepositoryError
from .ribbon import RibbonBar
from .shapes import load_vector_layer
from .workers import FunctionWorker
from .bbox import BlackBoxWindow
from .dsr_qc import DsrQcWindow
from .config import ProjectViewerConfig, CustomDsrLayerDefinition
from .custom_dsr_dialog import CustomDsrLayerDialog
from .icons_manager import icon
from .labels import MapLabelManager
from .radial_circles import RadialCircleItem
from .projects import ProjectsDatabase, ProjectsDatabaseError
from .surface_dialog import SurfaceCreateDialog
from .ocr_images import OcrImagesPanel
from .surface_layer import SurfaceMapLayer
from .surface3d import Surface3DWindow
from .surface_data import SurfaceDataRepository
from .theme import apply_application_theme, normalize_theme
from .geotiff_layer import GeoTiffLayer, GeoTiffDisplayOptions
from .comparison_layer import PointComparisonLayer
from .heading_panel import HeadingPanel


class LayerTreeWidget(QtWidgets.QTreeWidget):
    """Layer tree with reliable multi-layer movement between groups."""

    order_changed = QtCore.Signal()

    def _selected_layer_items(self) -> list[QtWidgets.QTreeWidgetItem]:
        """Return selected child items in their current visual tree order."""
        selected_ids = {id(item) for item in self.selectedItems() if item.parent() is not None}
        ordered: list[QtWidgets.QTreeWidgetItem] = []
        for group_index in range(self.topLevelItemCount()):
            group = self.topLevelItem(group_index)
            for child_index in range(group.childCount()):
                child = group.child(child_index)
                if id(child) in selected_ids:
                    ordered.append(child)
        return ordered

    def dragMoveEvent(self, event: QtGui.QDragMoveEvent) -> None:
        selected = self.selectedItems()
        if not selected:
            event.ignore()
            return

        moving_groups = any(item.parent() is None for item in selected)
        moving_layers = any(item.parent() is not None for item in selected)
        if moving_groups and moving_layers:
            event.ignore()
            return

        target = self.itemAt(event.position().toPoint())
        if moving_layers and target is None:
            event.ignore()
            return

        event.setDropAction(QtCore.Qt.DropAction.MoveAction)
        event.accept()

    def dropEvent(self, event: QtGui.QDropEvent) -> None:
        selected = self.selectedItems()
        if not selected:
            event.ignore()
            return

        moving_groups = any(item.parent() is None for item in selected)
        moving_layers = any(item.parent() is not None for item in selected)

        # Do not allow mixed group/layer moves.
        if moving_groups and moving_layers:
            event.ignore()
            return

        # Keep Qt's normal root-level reordering for groups.
        if moving_groups:
            target = self.itemAt(event.position().toPoint())
            if target is not None and target.parent() is not None:
                event.ignore()
                return
            super().dropEvent(event)
            self.order_changed.emit()
            return

        # QTreeWidget's InternalMove is unreliable for moving multiple children
        # across parents, so perform the child transfer explicitly.
        layer_items = self._selected_layer_items()
        target = self.itemAt(event.position().toPoint())
        if not layer_items or target is None:
            event.ignore()
            return

        if target.parent() is None:
            target_group = target
            insert_index = target_group.childCount()
        else:
            target_group = target.parent()
            target_index = target_group.indexOfChild(target)
            indicator = self.dropIndicatorPosition()
            if indicator == QtWidgets.QAbstractItemView.DropIndicatorPosition.BelowItem:
                insert_index = target_index + 1
            elif indicator == QtWidgets.QAbstractItemView.DropIndicatorPosition.OnItem:
                insert_index = target_index + 1
            else:
                insert_index = target_index

        # Correct the insertion index for selected items removed from the same
        # destination group before the insertion point.
        removed_before = sum(
            1
            for item in layer_items
            if item.parent() is target_group
            and target_group.indexOfChild(item) < insert_index
        )
        insert_index = max(0, insert_index - removed_before)

        # Detach and KEEP the exact QTreeWidgetItem objects returned by
        # takeChild().  Ignoring the return value can invalidate the Python
        # wrappers on some PySide6 builds, which makes the rows disappear when
        # insertChild() is called.
        positions: list[tuple[QtWidgets.QTreeWidgetItem, int, QtWidgets.QTreeWidgetItem]] = []
        for item in layer_items:
            parent = item.parent()
            if parent is not None:
                row = parent.indexOfChild(item)
                if row >= 0:
                    positions.append((parent, row, item))

        # Remove bottom-to-top inside every source group so row indexes remain
        # valid. Store the returned objects, then restore the original visual
        # selection order before inserting them in the destination group.
        detached_by_id: dict[int, QtWidgets.QTreeWidgetItem] = {}
        for parent, row, original_item in sorted(
            positions,
            key=lambda value: (id(value[0]), value[1]),
            reverse=True,
        ):
            detached_item = parent.takeChild(row)
            if detached_item is not None:
                detached_by_id[id(original_item)] = detached_item

        moved_items = [
            detached_by_id[id(item)]
            for item in layer_items
            if id(item) in detached_by_id
        ]
        if not moved_items:
            event.ignore()
            return

        insert_index = min(insert_index, target_group.childCount())
        for offset, item in enumerate(moved_items):
            target_group.insertChild(insert_index + offset, item)

        self.clearSelection()
        for item in moved_items:
            item.setSelected(True)
        self.setCurrentItem(moved_items[0])
        target_group.setExpanded(True)

        event.setDropAction(QtCore.Qt.DropAction.MoveAction)
        event.accept()
        self.order_changed.emit()


class MainWindow(QtWidgets.QMainWindow):
    """Main DataViewer 2.0 shell with ribbon, docks and fast map canvas."""

    def __init__(self, project_path: str | Path) -> None:
        super().__init__()
        self.project_path = Path(project_path)
        self.repository = ProjectRepository(project_path)
        self.viewer_config = ProjectViewerConfig(project_path)
        apply_application_theme(QtWidgets.QApplication.instance(), self.viewer_config.theme)
        self.thread_pool = QtCore.QThreadPool.globalInstance()
        self.thread_pool.setMaxThreadCount(2)
        self._workers: set[FunctionWorker] = set()
        self.layers: OrderedDict[str, object] = OrderedDict()
        self.layer_items: dict[str, QtWidgets.QTreeWidgetItem] = {}
        self._reloading_layers: set[str] = set()
        self._loading_initial_layers = True
        self._pending = 0
        self.bbox_window: BlackBoxWindow | None = None
        self.bbox_data: BlackBoxData | None = None
        self.bbox_data_by_file: dict[int, BlackBoxData] = {}
        self.bbox_track_layer_names: dict[tuple[int, str], str] = {}
        self.bbox_last_track_layer_name: str | None = None
        self._selected_dsr_timestamp: object | None = None
        self.dsr_qc_window: DsrQcWindow | None = None
        self.bathymetry_3d_window: Surface3DWindow | None = None
        self.dsr_qc_cache: dict[int, object] = {}
        self._dsr_qc_request_line: int | None = None
        # Last line/station requested from the Receiver QC ribbon.  The DSR QC
        # window uses this after asynchronous line loading completes.
        self._dsr_qc_requested_station: tuple[int, int] | None = None
        self.dsr_station_marker: pg.ScatterPlotItem | None = None
        self._dsr_tree_building = False
        self._dsr_station_values: list[int] = []
        self._dsr_auto_zoom = True
        self._custom_definition_by_layer: dict[str, CustomDsrLayerDefinition] = {}
        self._dsr_line_overlays: dict[tuple[str, int], FastPointLayer] = {}
        self._replacement_window: MainWindow | None = None
        self.radial_circle_item: RadialCircleItem | None = None
        self._radial_circle_style = {"radius": self.repository.max_radial_offset(), "color": "#ff5252", "width": 1.5, "line_style": "solid"}
        self._node_selection_mode: str | None = None
        self._selection_polygon_points: list[tuple[float, float]] = []
        self._build_ui()
        self._restore_state()
        self._start_loading()

    def _build_ui(self) -> None:
        self.setWindowTitle(f"SeisWebLog DataViewer 3.0 — {self.project_path.name}")
        self.setWindowIcon(icon("app", size=48))
        self.resize(1600, 950)
        self.setDockOptions(
            QtWidgets.QMainWindow.DockOption.AllowTabbedDocks
            | QtWidgets.QMainWindow.DockOption.AllowNestedDocks
            | QtWidgets.QMainWindow.DockOption.AnimatedDocks
        )

        self.ribbon = RibbonBar(self)
        self.setMenuWidget(self.ribbon)

        self.plot_widget = pg.PlotWidget()
        self.plot_item = self.plot_widget.getPlotItem()
        self.plot_item.setAspectLocked(True)
        self.plot_item.showGrid(x=True, y=True, alpha=0.2)
        self.plot_item.setLabel("bottom", "Easting", units="m")
        self.plot_item.setLabel("left", "Northing", units="m")
        self.label_manager = MapLabelManager(self.plot_item, max_labels=300, config=self.viewer_config)
        self.label_manager.enabled = bool(self.viewer_config.labels_enabled)
        self.selection_guide = pg.PlotDataItem(
            pen=pg.mkPen("#ffd740", width=2, style=QtCore.Qt.PenStyle.DashLine),
            symbol="o",
            symbolSize=7,
            symbolBrush="#ffd740",
        )
        self.selection_guide.setZValue(1000000)
        self.plot_item.addItem(self.selection_guide)
        self.setCentralWidget(self.plot_widget)

        self._build_layers_dock()
        self._build_details_dock()
        self._build_measurement_dock()
        self._build_heading_dock()
        self._build_ocr_images_dock()

        self.measurement = MeasurementTool(self.plot_item)
        self.measurement.changed.connect(self.measure_text.setPlainText)

        self.viewport_timer = QtCore.QTimer(self)
        self.viewport_timer.setSingleShot(True)
        self.viewport_timer.setInterval(120)
        self.viewport_timer.timeout.connect(self._refresh_layers)
        self.plot_item.vb.sigRangeChanged.connect(lambda *_: self.viewport_timer.start())
        self.plot_widget.scene().sigMouseMoved.connect(self._mouse_moved)
        self.plot_widget.scene().sigMouseClicked.connect(self._map_clicked)

        self.ribbon.zoom_all_requested.connect(self._zoom_all)
        self.ribbon.refresh_requested.connect(self._reload_layers)
        self.ribbon.select_all_layers_requested.connect(lambda: self._set_all_layers_visible(True))
        self.ribbon.clear_all_layers_requested.connect(lambda: self._set_all_layers_visible(False))
        self.ribbon.measurement_toggled.connect(self._toggle_measurement)
        self.ribbon.measurement_mode_requested.connect(self._set_measurement_mode)
        self.ribbon.clear_measurement_requested.connect(self.measurement.clear)
        self.ribbon.remove_last_measurement_requested.connect(self.measurement.remove_last)
        self.ribbon.grid_toggled.connect(self._set_grid_visible)
        self.ribbon.layers_panel_toggled.connect(self.layers_dock.setVisible)
        self.ribbon.feature_panel_toggled.connect(self.details_dock.setVisible)
        self.ribbon.measurement_panel_toggled.connect(self.measure_dock.setVisible)
        self.ribbon.ocr_panel_toggled.connect(self.ocr_images_dock.setVisible)
        self.ribbon.heading_panel_toggled.connect(self.heading_dock.setVisible)
        self.ribbon.status_bar_toggled.connect(self.statusBar().setVisible)
        self.ribbon.reset_layout_requested.connect(self._reset_workspace_layout)
        self.ribbon.theme_toggled.connect(self._theme_toggled_from_ribbon)
        self.ribbon.set_theme_checked(self.viewer_config.theme)

        self.layers_dock.visibilityChanged.connect(
            lambda visible: self.ribbon.set_panel_button_checked("layers", visible)
        )
        self.details_dock.visibilityChanged.connect(
            lambda visible: self.ribbon.set_panel_button_checked("feature", visible)
        )
        self.measure_dock.visibilityChanged.connect(
            lambda visible: self.ribbon.set_panel_button_checked("measurement", visible)
        )
        self.ocr_images_dock.visibilityChanged.connect(
            lambda visible: self.ribbon.set_panel_button_checked("ocr", visible)
        )
        self.ribbon.bbox_open_requested.connect(self._open_bbox_window)
        self.ribbon.bbox_reload_requested.connect(self._load_bbox_files)
        self.ribbon.bbox_track_toggle_requested.connect(self._toggle_bbox_track)
        self.ribbon.bbox_zoom_requested.connect(self._zoom_bbox_track)
        self.ribbon.dsr_open_qc_requested.connect(self._open_dsr_qc_window)
        self.ribbon.dsr_line_changed.connect(self._dsr_line_changed)
        self.ribbon.dsr_station_changed.connect(self._select_dsr_station)
        self.ribbon.dsr_zoom_line_requested.connect(self._zoom_dsr_line)
        self.ribbon.dsr_zoom_station_requested.connect(self._zoom_dsr_station)
        self.ribbon.dsr_previous_station_requested.connect(lambda: self._step_dsr_station(-1))
        self.ribbon.dsr_next_station_requested.connect(lambda: self._step_dsr_station(1))
        self.ribbon.dsr_auto_zoom_toggled.connect(self._set_dsr_auto_zoom)
        self.ribbon.dsr_create_layer_requested.connect(self._create_custom_dsr_layer)
        self.ribbon.dsr_manage_layers_requested.connect(self._manage_custom_dsr_layers)
        self.ribbon.project_change_requested.connect(self._change_project)
        self.ribbon.project_folder_requested.connect(self._open_project_folder)
        self.ribbon.exit_requested.connect(self.close)
        self.ribbon.export_map_requested.connect(self._export_map_image)
        self.ribbon.export_selected_layer_requested.connect(self._export_selected_layer)
        self.ribbon.export_visible_layers_requested.connect(self._export_visible_layers)
        self.ribbon.report_project_requested.connect(self._generate_project_report)
        self.ribbon.report_dsr_line_requested.connect(self._generate_dsr_line_report)
        self.ribbon.reports_folder_requested.connect(self._open_reports_folder)
        self.ribbon.labels_toggled.connect(self._toggle_map_labels)
        labels_blocker = QtCore.QSignalBlocker(self.ribbon.labels_button)
        self.ribbon.labels_button.setChecked(self.label_manager.enabled)
        del labels_blocker
        self.ribbon.dsr_add_bbox_requested.connect(self._add_bbox_tracks_for_selected_dsr_line)
        self.ribbon.radial_circles_toggled.connect(self._toggle_radial_circles)
        self.ribbon.radial_circle_style_changed.connect(self._set_radial_circle_style)
        self.ribbon.surface2d_open_requested.connect(self._open_surface_2d_window)
        self.ribbon.sps_overlay_requested.connect(self._add_sps_production_overlay)
        self.ribbon.slsolution_overlay_requested.connect(self._add_slsolution_overlay)
        self.ribbon.geotiff_open_requested.connect(self._load_geotiff_dialog)
        self.ribbon.point_compare_requested.connect(self._create_point_comparison)
        self.ribbon.node_selection_requested.connect(self._start_node_selection)
        self.ribbon.clear_node_selection_requested.connect(self._clear_node_selection)
        self.ribbon.view3d_open_requested.connect(self._open_bathymetry_3d_window)
        self.ribbon.set_radial_default(self._radial_circle_style["radius"])

        epsg = self.repository.project_epsg() or "unknown"
        self.coord_label = QtWidgets.QLabel("X: —   Y: —")
        self.statusBar().addPermanentWidget(self.coord_label)
        self.statusBar().showMessage(f"Project: {self.project_path} | EPSG: {epsg}")

    def _build_layers_dock(self) -> None:
        self.layers_dock = QtWidgets.QDockWidget("Layers", self)
        self.layers_dock.setObjectName("LayersDock")
        container = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(container)
        layout.setContentsMargins(5, 5, 5, 5)
        self.layer_filter = QtWidgets.QLineEdit()
        self.layer_filter.setPlaceholderText("Filter layers…")
        self.layer_filter.textChanged.connect(self._filter_layer_tree)
        layout.addWidget(self.layer_filter)
        attach_row = QtWidgets.QHBoxLayout()
        self.attach_gpkg_button = QtWidgets.QPushButton(icon("folder", size=18), "Attach GeoPackage…")
        self.attach_gpkg_button.setToolTip("Attach selected layers from a .gpkg file to this project")
        self.attach_gpkg_button.clicked.connect(self._attach_geopackage)
        attach_row.addWidget(self.attach_gpkg_button)
        attach_row.addStretch(1)
        layout.addLayout(attach_row)
        self.layer_tree = LayerTreeWidget()
        self.layer_tree.setHeaderLabels(["Layer", "Features", ""])
        self.layer_tree.setRootIsDecorated(True)
        # Allow normal Windows-style multi-selection:
        # Ctrl-click toggles individual layers and Shift-click selects ranges.
        self.layer_tree.setSelectionMode(
            QtWidgets.QAbstractItemView.SelectionMode.ExtendedSelection
        )
        self.layer_tree.setDragEnabled(True)
        self.layer_tree.setAcceptDrops(True)
        self.layer_tree.setDropIndicatorShown(True)
        self.layer_tree.setDragDropMode(QtWidgets.QAbstractItemView.DragDropMode.InternalMove)
        self.layer_tree.setDefaultDropAction(QtCore.Qt.DropAction.MoveAction)
        self.layer_tree.order_changed.connect(self._layer_tree_dropped)
        self.layer_tree.setAllColumnsShowFocus(True)
        self.layer_tree.header().setSectionResizeMode(
            2, QtWidgets.QHeaderView.ResizeMode.ResizeToContents
        )
        self.layer_tree.itemChanged.connect(self._tree_item_changed)
        self.layer_tree.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.layer_tree.customContextMenuRequested.connect(self._show_layer_context_menu)
        self.layer_tree.itemDoubleClicked.connect(self._layer_double_clicked)
        self.layer_tree.itemExpanded.connect(self._tree_item_expanded)
        self.layer_tree.itemClicked.connect(self._tree_item_clicked)
        layout.addWidget(self.layer_tree)
        self.layers_dock.setWidget(container)
        self.addDockWidget(QtCore.Qt.DockWidgetArea.LeftDockWidgetArea, self.layers_dock)
        # Recreate saved groups immediately, including currently empty custom groups.
        for saved_group in list(getattr(self.viewer_config, "group_order", []) or []):
            self._group_item(str(saved_group))

    def _theme_toggled_from_ribbon(self, night_enabled: bool) -> None:
        self._apply_theme("night" if night_enabled else "day")

    def _apply_theme(self, theme_name: str) -> None:
        theme = normalize_theme(theme_name)
        apply_application_theme(QtWidgets.QApplication.instance(), theme)
        self.viewer_config.set_theme(theme)
        self.ribbon.set_theme_checked(theme)
        background = "#ffffff" if theme == "day" else "#111317"
        foreground = "#20252b" if theme == "day" else "#e8e8e8"
        self.plot_widget.setBackground(background)
        for axis_name in ("left", "bottom", "top", "right"):
            axis = self.plot_item.getAxis(axis_name)
            axis.setPen(foreground)
            axis.setTextPen(foreground)
        self.statusBar().showMessage(f"{theme.title()} interface enabled", 2500)

    def _create_layer_group(self) -> None:
        name, ok = QtWidgets.QInputDialog.getText(
            self, "Create layer group", "Group name:"
        )
        name = str(name).strip()
        if not ok or not name:
            return
        existing = {
            self.layer_tree.topLevelItem(i).text(0).casefold()
            for i in range(self.layer_tree.topLevelItemCount())
        }
        if name.casefold() in existing:
            QtWidgets.QMessageBox.warning(self, "Layer group", "A group with this name already exists.")
            return
        item = self._group_item(name)
        self.layer_tree.setCurrentItem(item)
        item.setSelected(True)
        self._save_layer_tree_order()
        self.statusBar().showMessage(f"Created layer group: {name}", 2500)

    def _layer_tree_dropped(self) -> None:
        # Keep layer mapping/render order synchronized after cross-group drops.
        self._sync_layers_from_tree()
        self._save_layer_tree_order()
        self.statusBar().showMessage("Layer order and groups updated", 2000)

    def _build_details_dock(self) -> None:
        self.details_dock = QtWidgets.QDockWidget("Feature information", self)
        self.details_dock.setObjectName("DetailsDock")
        self.details = QtWidgets.QPlainTextEdit()
        self.details.setReadOnly(True)
        self.details_dock.setWidget(self.details)
        self.addDockWidget(QtCore.Qt.DockWidgetArea.RightDockWidgetArea, self.details_dock)

    def _build_measurement_dock(self) -> None:
        self.measure_dock = QtWidgets.QDockWidget("Measurement", self)
        self.measure_dock.setObjectName("MeasurementDock")
        body = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(body)
        self.measure_text = QtWidgets.QPlainTextEdit()
        self.measure_text.setReadOnly(True)
        self.measure_text.setPlaceholderText("Use the Measurement tab, select Distance, Area, Bearing or Angle, then click map points.")
        layout.addWidget(self.measure_text)
        self.measure_dock.setWidget(body)
        self.addDockWidget(QtCore.Qt.DockWidgetArea.RightDockWidgetArea, self.measure_dock)
        self.tabifyDockWidget(self.details_dock, self.measure_dock)
        self.details_dock.raise_()


    def _build_heading_dock(self) -> None:
        self.heading_dock = QtWidgets.QDockWidget("Heading", self)
        self.heading_dock.setObjectName("HeadingDock")
        self.heading_panel = HeadingPanel(self)
        self.heading_dock.setWidget(self.heading_panel)
        self.addDockWidget(QtCore.Qt.DockWidgetArea.RightDockWidgetArea, self.heading_dock)
        self.tabifyDockWidget(self.details_dock, self.heading_dock)
        self.heading_dock.show()

    def _build_ocr_images_dock(self) -> None:
        self.ocr_images_dock = QtWidgets.QDockWidget("OCR Images", self)
        self.ocr_images_dock.setObjectName("OcrImagesDock")
        self.ocr_images_dock.setAllowedAreas(
            QtCore.Qt.DockWidgetArea.BottomDockWidgetArea
            | QtCore.Qt.DockWidgetArea.TopDockWidgetArea
        )
        self.ocr_images_panel = OcrImagesPanel(self)
        self.ocr_images_dock.setWidget(self.ocr_images_panel)
        self.addDockWidget(QtCore.Qt.DockWidgetArea.BottomDockWidgetArea, self.ocr_images_dock)
        self.resizeDocks([self.ocr_images_dock], [210], QtCore.Qt.Orientation.Vertical)
        self.ocr_images_dock.hide()

    def _load_ocr_images_for_dsr_record(self, record: dict) -> None:
        lower = {str(key).lower(): value for key, value in record.items()}
        line = lower.get("line") or lower.get("dsr_line")
        station = lower.get("station") or lower.get("linepoint") or lower.get("dsr_station")
        if line is None or station is None:
            self.ocr_images_panel.clear_records("Selected DSR record has no Line/Station")
            return
        records = self.repository.load_ocr_results(line, station)
        self.ocr_images_panel.set_records(line, station, records)
        self.ocr_images_dock.show()
        self.resizeDocks([self.ocr_images_dock], [220], QtCore.Qt.Orientation.Vertical)



    def _root_projects_database(self) -> Path | None:
        from .startup_settings import remembered_projects_database

        candidates = [
            remembered_projects_database(),
            Path.cwd() / "db.sqlite3",
            Path(__file__).resolve().parents[1] / "db.sqlite3",
        ]
        for candidate in candidates:
            if candidate is not None and candidate.is_file():
                return candidate.resolve()
        return None

    def _change_project(self) -> None:
        root_db = self._root_projects_database()
        if root_db is None:
            QtWidgets.QMessageBox.warning(
                self,
                "Change project",
                "The root SeisWebLog db.sqlite3 could not be found.",
            )
            return
        try:
            projects = ProjectsDatabase(root_db).read_projects()
        except ProjectsDatabaseError as exc:
            QtWidgets.QMessageBox.critical(self, "Change project", str(exc))
            return
        available = [p for p in projects if p.project_dir.resolve() != self.project_path.resolve()]
        if not available:
            QtWidgets.QMessageBox.information(self, "Change project", "No other projects are available.")
            return
        labels = [f"{p.name} — {p.project_dir}" for p in available]
        selected, accepted = QtWidgets.QInputDialog.getItem(
            self, "Change project", "Project:", labels, 0, False
        )
        if not accepted:
            return
        entry = available[labels.index(selected)]
        try:
            replacement = MainWindow(entry.project_dir)
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Change project", str(exc))
            return
        from .startup_settings import remember_project, remember_projects_database

        remember_projects_database(root_db)
        remember_project(entry.project_dir)
        self._replacement_window = replacement
        app = QtWidgets.QApplication.instance()
        if app is not None:
            setattr(app, "_dataviewer_active_window", replacement)
        replacement.showMaximized()
        self.close()

    def _open_project_folder(self) -> None:
        QtGui.QDesktopServices.openUrl(
            QtCore.QUrl.fromLocalFile(str(self.project_path.resolve()))
        )

    def _reports_dir(self) -> Path:
        path = self.project_path / "reports" / "dataviewer2"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _safe_filename(value: str) -> str:
        safe = "".join(char if char.isalnum() or char in "-_." else "_" for char in value)
        return safe.strip("._") or "layer"

    def _export_map_image(self) -> None:
        default = self._reports_dir() / f"{self._safe_filename(self.project_path.name)}_map.png"
        filename, _ = QtWidgets.QFileDialog.getSaveFileName(
            self, "Export map image", str(default), "PNG image (*.png);;JPEG image (*.jpg *.jpeg)"
        )
        if not filename:
            return
        pixmap = self.plot_widget.grab()
        if not pixmap.save(filename):
            QtWidgets.QMessageBox.warning(self, "Export map", "The map image could not be saved.")
            return
        self.statusBar().showMessage(f"Map exported: {filename}", 4000)

    def _selected_layer_name(self) -> str | None:
        item = self.layer_tree.currentItem()
        if item is None:
            return None
        name = item.data(0, QtCore.Qt.ItemDataRole.UserRole)
        return str(name) if name in self.layers else None

    def _write_layer_csv(self, name: str, filename: Path) -> int:
        layer = self.layers[name]
        filename.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(layer, FastPointLayer) and layer.data is not None:
            metadata = list(layer.data.metadata.keys())
            with filename.open("w", newline="", encoding="utf-8-sig") as handle:
                writer = csv.writer(handle)
                writer.writerow(["x", "y", *metadata])
                for index in range(layer.data.count):
                    row = [layer.data.x[index], layer.data.y[index]]
                    for key in metadata:
                        value = layer.data.metadata[key][index]
                        row.append(value.item() if hasattr(value, "item") else value)
                    writer.writerow(row)
            return layer.data.count
        if isinstance(layer, FastShapeLayer):
            with filename.open("w", newline="", encoding="utf-8-sig") as handle:
                writer = csv.writer(handle)
                writer.writerow(["part", "vertex", "x", "y"])
                count = 0
                arrays = [layer.data.points] if layer.data.geometry_type == "point" else layer.data.parts
                for part_index, array in enumerate(arrays):
                    for vertex_index, point in enumerate(array):
                        writer.writerow([part_index, vertex_index, point[0], point[1]])
                        count += 1
            return count
        return 0

    def _export_selected_layer(self) -> None:
        name = self._selected_layer_name()
        if name is None:
            QtWidgets.QMessageBox.information(self, "Export layer", "Select a layer in the Layers panel first.")
            return
        default = self._reports_dir() / f"{self._safe_filename(name)}.csv"
        filename, _ = QtWidgets.QFileDialog.getSaveFileName(
            self, "Export selected layer", str(default), "CSV file (*.csv)"
        )
        if not filename:
            return
        count = self._write_layer_csv(name, Path(filename))
        self.statusBar().showMessage(f"Exported {count:,} records from {name}", 4000)

    def _export_visible_layers(self) -> None:
        directory = QtWidgets.QFileDialog.getExistingDirectory(
            self, "Export visible layers", str(self._reports_dir() / "layers")
        )
        if not directory:
            return
        exported = 0
        for name, layer in self.layers.items():
            if not layer.visible:
                continue
            self._write_layer_csv(name, Path(directory) / f"{self._safe_filename(name)}.csv")
            exported += 1
        self.statusBar().showMessage(f"Exported {exported} visible layer(s)", 4000)

    def _report_header(self, title: str) -> str:
        return f"""<!doctype html><html><head><meta charset='utf-8'><title>{html.escape(title)}</title>
<style>body{{font-family:Segoe UI,Arial,sans-serif;margin:32px;color:#263238}}
h1{{color:#1565c0}}table{{border-collapse:collapse;width:100%;margin-top:18px}}
th,td{{border:1px solid #cfd8dc;padding:6px 9px;text-align:left}}th{{background:#eceff1}}
img{{max-width:100%;border:1px solid #b0bec5}}</style></head><body><h1>{html.escape(title)}</h1>"""

    def _generate_project_report(self) -> None:
        out = self._reports_dir()
        image_path = out / "project_map.png"
        self.plot_widget.grab().save(str(image_path))
        report_path = out / "project_summary.html"
        epsg = self.repository.project_epsg() or "unknown"
        rows = "".join(
            f"<tr><td>{html.escape(name)}</td><td>{layer.count:,}</td><td>{'Yes' if layer.visible else 'No'}</td></tr>"
            for name, layer in self.layers.items()
        )
        content = self._report_header(f"Project summary — {self.project_path.name}")
        content += f"<p><b>Project folder:</b> {html.escape(str(self.project_path))}<br><b>EPSG:</b> {html.escape(str(epsg))}</p>"
        content += "<img src='project_map.png' alt='Project map'>"
        content += f"<table><thead><tr><th>Layer</th><th>Features</th><th>Visible</th></tr></thead><tbody>{rows}</tbody></table></body></html>"
        report_path.write_text(content, encoding="utf-8")
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(report_path)))

    def _generate_dsr_line_report(self) -> None:
        line = self.ribbon.current_dsr_line()
        layer = self.layers.get("DSR Primary")
        if line is None or not isinstance(layer, FastPointLayer) or layer.data is None:
            QtWidgets.QMessageBox.information(self, "Receiver-line report", "DSR Primary and a receiver line are required.")
            return
        line_values = self._numeric_values(layer.data.metadata.get("line"))
        finite = np.isfinite(line_values)
        rounded = np.zeros(line_values.size, dtype=np.int64)
        rounded[finite] = np.rint(line_values[finite]).astype(np.int64)
        indices = np.flatnonzero(finite & (rounded == int(line)))
        if indices.size == 0:
            QtWidgets.QMessageBox.information(self, "Receiver-line report", "No DSR Primary records were found for this line.")
            return
        out = self._reports_dir()
        report_path = out / f"receiver_line_{line}.html"
        fields = [field for field in ("station", "node", "rov", "ROV", "PrimaryRadial", "PrimaryElevation") if field in layer.data.metadata]
        headings = ["Easting", "Northing", *fields]
        body_rows = []
        for index in indices:
            values = [f"{layer.data.x[index]:.3f}", f"{layer.data.y[index]:.3f}"]
            for field in fields:
                value = layer.data.metadata[field][index]
                value = value.item() if hasattr(value, "item") else value
                values.append(str(value))
            body_rows.append("<tr>" + "".join(f"<td>{html.escape(value)}</td>" for value in values) + "</tr>")
        content = self._report_header(f"Receiver line {line}")
        content += f"<p><b>Stations/records:</b> {indices.size:,}</p>"
        content += "<table><thead><tr>" + "".join(f"<th>{html.escape(field)}</th>" for field in headings) + "</tr></thead><tbody>"
        content += "".join(body_rows) + "</tbody></table></body></html>"
        report_path.write_text(content, encoding="utf-8")
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(report_path)))

    def _open_reports_folder(self) -> None:
        QtGui.QDesktopServices.openUrl(
            QtCore.QUrl.fromLocalFile(str(self._reports_dir()))
        )

    def _start_worker(self, worker: FunctionWorker) -> None:
        """Keep Python worker wrappers alive until their signals finish."""
        self._workers.add(worker)

        def cleanup(*_args) -> None:
            self._workers.discard(worker)

        worker.signals.completed.connect(cleanup)
        worker.signals.failed.connect(cleanup)
        self.thread_pool.start(worker)

    def _attach_geopackage(self) -> None:
        # Import lazily so optional GeoPackage UI cannot prevent DataViewer startup.
        from .gpkg_dialog import GeoPackageAttachDialog
        dialog = GeoPackageAttachDialog(self)
        if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
            return
        try:
            self.repository.attach_geopackage(dialog.gpkg_path, dialog.selected_layers())
            definitions = [d for d in self.repository.load_geopackage_definitions()
                           if d.full_name == dialog.gpkg_path and d.source_layer in {x["name"] for x in dialog.selected_layers()}]
            project_epsg = self.repository.project_epsg()
            self.statusBar().showMessage(f"Loading {len(definitions)} GeoPackage layer(s)…")
            for definition in definitions:
                # Avoid registering duplicates when a file/layer was already attached.
                duplicate = any(
                    isinstance(existing, FastShapeLayer)
                    and existing.data.definition.source_type == "gpkg"
                    and existing.data.definition.full_name == definition.full_name
                    and existing.data.definition.source_layer == definition.source_layer
                    for existing in self.layers.values()
                )
                if duplicate:
                    continue
                QtWidgets.QApplication.processEvents()
                data = load_vector_layer(definition, project_epsg)
                self._register_shape_data(data)
            self._zoom_all()
            self.statusBar().showMessage("GeoPackage layers attached", 3500)
        except Exception as exc:
            import traceback
            self.details.appendPlainText(traceback.format_exc() + "\n")
            QtWidgets.QMessageBox.critical(self, "Attach GeoPackage", str(exc))

    def _start_loading(self) -> None:
        jobs = [
            ("Preplot", "RPPreplot", self.repository.load_rp_preplot, "#27c2ff", "#27c2ff", "line"),
            ("OCR Images", "OCR Image Counts", self.repository.load_ocr_image_counts, "#00000000", None, None),
            ("Receiver QC", "DSR Preplot", lambda: self.repository.load_dsr_layer("preplot"), "#a7a7a7", None, None),
            ("Receiver QC", "DSR Primary", lambda: self.repository.load_dsr_layer("primary"), "#41d26f", None, None),
            ("Receiver QC", "DSR Secondary", lambda: self.repository.load_dsr_layer("secondary"), "#44a7ff", None, None),
            ("Receiver QC", "DSR Recovery Primary", lambda: self.repository.load_dsr_layer("recovery_primary"), "#ff9d3d", None, None),
            ("Receiver QC", "REC_DB", self.repository.load_rec_db, "#f05cff", None, None),
            ("Survey Manager", "SM Deployment", lambda: self.repository.load_survey_manager_layer("deployment"), "#00e676", None, None),
            ("Survey Manager", "SM Recovery", lambda: self.repository.load_survey_manager_layer("recovery"), "#ffab40", None, None),
        ]
        try:
            shape_definitions = self.repository.load_shape_definitions() + self.repository.load_geopackage_definitions()
        except Exception as exc:
            shape_definitions = []
            self.details.appendPlainText(f"Shape definition error: {exc}\n")

        # IMPORTANT: pyproj/PROJ must not run concurrently in QThreadPool workers on
        # some Windows installations. It can corrupt the native PROJ heap and close
        # the whole process with 0xC0000374 before Python can create a traceback.
        # Load/reproject registered shapes serially on the GUI thread before any
        # SQLite background workers are started. Shape loading may briefly delay
        # startup, but it is deterministic and avoids the native crash.
        project_epsg = self.repository.project_epsg()
        if shape_definitions:
            self.statusBar().showMessage(f"Loading {len(shape_definitions)} shape layer(s) safely…")
            QtWidgets.QApplication.processEvents()
            for definition in shape_definitions:
                try:
                    data = load_vector_layer(definition, project_epsg)
                    self._register_shape_data(data)
                except Exception:
                    import traceback
                    self.details.appendPlainText(traceback.format_exc() + "\n")
                QtWidgets.QApplication.processEvents()

        custom_definitions = list(self.viewer_config.custom_dsr_layers)
        self._pending = len(jobs) + len(custom_definitions)
        self.statusBar().showMessage(f"Loading {self._pending} database layer(s)…")
        for group, name, function, point_color, line_color, connect_by in jobs:
            worker = FunctionWorker(function)
            worker.signals.completed.connect(
                lambda data, g=group, n=name, pc=point_color, lc=line_color, cb=connect_by:
                self._point_layer_loaded(g, n, data, pc, lc, cb)
            )
            worker.signals.failed.connect(self._load_failed)
            self._start_worker(worker)


        for definition in custom_definitions:
            worker = FunctionWorker(lambda d=definition: self.repository.load_custom_dsr_layer(d))
            worker.signals.completed.connect(
                lambda data, d=definition: self._custom_dsr_layer_loaded(d, data)
            )
            worker.signals.failed.connect(self._load_failed)
            self._start_worker(worker)

        if self._pending == 0:
            self._loading_initial_layers = False
            self._save_layer_tree_order()
            self._zoom_all()
            self.statusBar().showMessage(f"Ready — {len(self.layers)} layer(s)")

    def _group_item(self, name: str) -> QtWidgets.QTreeWidgetItem:
        for index in range(self.layer_tree.topLevelItemCount()):
            item = self.layer_tree.topLevelItem(index)
            if item.text(0) == name:
                return item
        item = QtWidgets.QTreeWidgetItem([name, ""])
        item.setIcon(0, icon("group", size=20))
        item.setData(0, QtCore.Qt.ItemDataRole.UserRole + 1, "layer_group")
        item.setData(0, QtCore.Qt.ItemDataRole.UserRole + 2, name)
        item.setFlags((item.flags() & ~QtCore.Qt.ItemFlag.ItemIsUserCheckable) | QtCore.Qt.ItemFlag.ItemIsDragEnabled | QtCore.Qt.ItemFlag.ItemIsDropEnabled)
        item.setExpanded(True)

        # Restore a project-specific group order when one has been saved. New
        # groups that are not yet in the saved order are appended normally.
        desired = list(getattr(self.viewer_config, "group_order", []) or [])
        if name in desired:
            target = 0
            wanted_index = desired.index(name)
            for index in range(self.layer_tree.topLevelItemCount()):
                existing = self.layer_tree.topLevelItem(index).text(0)
                if existing in desired and desired.index(existing) < wanted_index:
                    target += 1
            self.layer_tree.insertTopLevelItem(target, item)
        else:
            self.layer_tree.addTopLevelItem(item)
        self._add_group_visibility_buttons(item, name)
        return item

    def _add_group_visibility_buttons(
        self, item: QtWidgets.QTreeWidgetItem, group_name: str
    ) -> None:
        """Add group-level visibility and label toggle buttons."""
        actions = QtWidgets.QWidget(self.layer_tree)
        actions.setToolTip(f"Visibility controls for {group_name}")
        row = QtWidgets.QHBoxLayout(actions)
        row.setContentsMargins(2, 0, 2, 0)

        visibility_button = QtWidgets.QToolButton(actions)
        visibility_button.setObjectName("groupVisibilityButton")
        visibility_button.setAutoRaise(True)
        visibility_button.clicked.connect(
            lambda _checked=False, group=group_name: self._toggle_layer_group_visibility(group)
        )
        row.addWidget(visibility_button)

        labels_button = QtWidgets.QToolButton(actions)
        labels_button.setObjectName("groupLabelsButton")
        labels_button.setAutoRaise(True)
        labels_button.clicked.connect(
            lambda _checked=False, group=group_name: self._toggle_layer_group_labels(group)
        )
        row.addWidget(labels_button)

        reload_button = QtWidgets.QToolButton(actions)
        reload_button.setObjectName("groupReloadButton")
        reload_button.setIcon(icon("reload", size=18))
        reload_button.setAutoRaise(True)
        reload_button.setToolTip(f"Reload database layers in {group_name}")
        reload_button.clicked.connect(
            lambda _checked=False, group=group_name: self._reload_layer_group(group)
        )
        row.addWidget(reload_button)
        self.layer_tree.setItemWidget(item, 2, actions)
        self._update_group_visibility_icon(item)
        self._update_group_label_icon(item)
        self._update_group_reload_icon(item)

    def _update_group_reload_icon(self, group_item: QtWidgets.QTreeWidgetItem) -> None:
        actions = self.layer_tree.itemWidget(group_item, 2)
        if actions is None:
            return
        button = actions.findChild(QtWidgets.QToolButton, "groupReloadButton")
        if button is None:
            return
        names = self._group_layer_names(group_item)
        reloadable = [name for name in names if self._layer_reload_function(name) is not None]
        available = [name for name in reloadable if name not in self._reloading_layers]
        button.setEnabled(bool(available))
        button.setToolTip(
            f"Reload {len(reloadable)} database layer(s) in {group_item.text(0)}"
            if reloadable
            else "This group has no directly reloadable database layers"
        )

    def _update_group_visibility_icon(
        self, group_item: QtWidgets.QTreeWidgetItem
    ) -> None:
        actions = self.layer_tree.itemWidget(group_item, 2)
        if actions is None:
            return
        button = actions.findChild(QtWidgets.QToolButton, "groupVisibilityButton")
        if button is None:
            return
        names = self._group_layer_names(group_item)
        any_visible = any(bool(getattr(self.layers[name], "visible", False)) for name in names)
        group_name = str(group_item.text(0))
        button.setIcon(icon("show" if any_visible else "hide", size=18))
        button.setToolTip(
            f"Hide all layers in {group_name}"
            if any_visible
            else f"Show all layers in {group_name}"
        )

    def _group_point_layer_names(
        self, group_item: QtWidgets.QTreeWidgetItem
    ) -> list[str]:
        return [
            name
            for name in self._group_layer_names(group_item)
            if isinstance(self.layers.get(name), FastPointLayer)
        ]

    def _layer_labels_effectively_enabled(self, layer_name: str) -> bool:
        configured = bool(
            self.label_manager.style_for(layer_name).get("enabled", True)
        )
        return configured and (
            self.label_manager.enabled
            or layer_name.casefold() == "ocr image counts"
        )

    def _update_group_label_icon(
        self, group_item: QtWidgets.QTreeWidgetItem
    ) -> None:
        actions = self.layer_tree.itemWidget(group_item, 2)
        if actions is None:
            return
        button = actions.findChild(QtWidgets.QToolButton, "groupLabelsButton")
        if button is None:
            return
        names = self._group_point_layer_names(group_item)
        any_enabled = any(self._layer_labels_effectively_enabled(name) for name in names)
        group_name = str(group_item.text(0))
        button.setEnabled(bool(names))
        button.setIcon(icon("labels_show" if any_enabled else "labels_hide", size=18))
        button.setToolTip(
            f"Hide labels for all layers in {group_name}"
            if any_enabled
            else f"Show labels for all layers in {group_name}"
        )

    def _set_layer_group_labels(self, group_name: str, enabled: bool) -> None:
        group_item = self._group_item(group_name)
        names = self._group_point_layer_names(group_item)
        if enabled:
            self.label_manager.enabled = True
            blocker = QtCore.QSignalBlocker(self.ribbon.labels_button)
            self.ribbon.labels_button.setChecked(True)
            del blocker
        for name in names:
            style = self.label_manager.style_for(name)
            style["enabled"] = bool(enabled)
            self.label_manager.set_style(name, style)
            self._update_layer_label_icon(name)
        self._update_group_label_icon(group_item)
        self.label_manager.refresh(self.layers)
        self.statusBar().showMessage(
            f"Labels {'shown' if enabled else 'hidden'} for {len(names)} layer(s) in {group_name}",
            3000,
        )

    def _toggle_layer_group_labels(self, group_name: str) -> None:
        group_item = self._group_item(group_name)
        names = self._group_point_layer_names(group_item)
        any_enabled = any(self._layer_labels_effectively_enabled(name) for name in names)
        self._set_layer_group_labels(group_name, not any_enabled)

    def _toggle_layer_group_visibility(self, group_name: str) -> None:
        group_item = self._group_item(group_name)
        names = self._group_layer_names(group_item)
        any_visible = any(bool(getattr(self.layers[name], "visible", False)) for name in names)
        self._set_layer_group_visible(group_name, not any_visible)

    def _add_layer_label_button(
        self, item: QtWidgets.QTreeWidgetItem, layer_name: str
    ) -> None:
        """Add a per-layer chat-bubble toggle for map labels."""
        actions = QtWidgets.QWidget(self.layer_tree)
        row = QtWidgets.QHBoxLayout(actions)
        row.setContentsMargins(2, 0, 2, 0)
        label_button = QtWidgets.QToolButton(actions)
        label_button.setObjectName("layerLabelsButton")
        label_button.setAutoRaise(True)
        label_button.clicked.connect(
            lambda _checked=False, name=layer_name: self._toggle_layer_labels(name)
        )
        row.addWidget(label_button)
        reload_button = QtWidgets.QToolButton(actions)
        reload_button.setObjectName("layerReloadButton")
        reload_button.setIcon(icon("reload", size=18))
        reload_button.setAutoRaise(True)
        reload_button.clicked.connect(
            lambda _checked=False, name=layer_name: self._reload_layer(name)
        )
        row.addWidget(reload_button)
        self.layer_tree.setItemWidget(item, 2, actions)
        self._update_layer_label_icon(layer_name)
        self._update_layer_reload_icon(layer_name)

    def _update_layer_label_icon(self, layer_name: str) -> None:
        item = self.layer_items.get(layer_name)
        if item is None:
            return
        actions = self.layer_tree.itemWidget(item, 2)
        if actions is None:
            return
        button = actions.findChild(QtWidgets.QToolButton, "layerLabelsButton")
        if button is None:
            return
        layer = self.layers.get(layer_name)
        supported = isinstance(layer, FastPointLayer)
        enabled = self._layer_labels_effectively_enabled(layer_name)
        button.setEnabled(supported)
        button.setIcon(icon("labels_show" if enabled else "labels_hide", size=18))
        if supported:
            button.setToolTip(
                f"Hide labels for {layer_name}" if enabled else f"Show labels for {layer_name}"
            )
        else:
            button.setToolTip("Labels are available for point layers only")

    def _update_layer_reload_icon(self, layer_name: str) -> None:
        item = self.layer_items.get(layer_name)
        if item is None:
            return
        actions = self.layer_tree.itemWidget(item, 2)
        if actions is None:
            return
        button = actions.findChild(QtWidgets.QToolButton, "layerReloadButton")
        if button is None:
            return
        reloadable = self._layer_reload_function(layer_name) is not None
        busy = layer_name in self._reloading_layers
        button.setEnabled(reloadable and not busy)
        button.setToolTip(
            f"Reload {layer_name} from the database"
            if reloadable
            else "This generated layer has no direct database source"
        )

    def _layer_reload_function(self, layer_name: str):
        """Return a callable that reloads one layer's source data."""
        builtins = {
            "RPPreplot": self.repository.load_rp_preplot,
            "OCR Image Counts": self.repository.load_ocr_image_counts,
            "DSR Preplot": lambda: self.repository.load_dsr_layer("preplot"),
            "DSR Primary": lambda: self.repository.load_dsr_layer("primary"),
            "DSR Secondary": lambda: self.repository.load_dsr_layer("secondary"),
            "DSR Recovery Primary": lambda: self.repository.load_dsr_layer("recovery_primary"),
            "REC_DB": self.repository.load_rec_db,
            "SM Deployment": lambda: self.repository.load_survey_manager_layer("deployment"),
            "SM Recovery": lambda: self.repository.load_survey_manager_layer("recovery"),
        }
        if layer_name in builtins:
            return builtins[layer_name]
        definition = self._custom_definition_by_layer.get(layer_name)
        if definition is not None:
            return lambda d=definition, name=layer_name: self._reload_custom_layer_data(d, name)
        layer = self.layers.get(layer_name)
        if isinstance(layer, FastShapeLayer):
            definition = layer.data.definition
            return lambda d=definition: load_vector_layer(d, self.repository.project_epsg())
        return None

    def _reload_custom_layer_data(
        self, definition: CustomDsrLayerDefinition, layer_name: str
    ) -> PointLayerData:
        data = self.repository.load_custom_dsr_layer(definition)
        field = str(definition.category_field or "").lower()
        if not field or field not in data.metadata or layer_name == definition.name:
            data.name = layer_name
            return data
        prefix = f"{definition.name} — "
        category = layer_name[len(prefix):] if layer_name.startswith(prefix) else ""
        values = data.metadata[field]
        indices = np.flatnonzero(
            np.asarray([str(value) == category for value in values], dtype=bool)
        )
        return PointLayerData(
            layer_name,
            data.x[indices],
            data.y[indices],
            data.source_index[indices],
            {key: value[indices] for key, value in data.metadata.items()},
        )

    def _reload_layer_group(self, group_name: str) -> None:
        group_item = self._group_item(group_name)
        names = [
            name for name in self._group_layer_names(group_item)
            if self._layer_reload_function(name) is not None
        ]
        if not names:
            self.statusBar().showMessage(f"No reloadable layers in {group_name}", 3000)
            return
        for name in names:
            self._reload_layer(name)
        self.statusBar().showMessage(
            f"Reloading {len(names)} layer(s) in {group_name}…"
        )

    def _reload_layer(self, layer_name: str) -> None:
        if layer_name in self._reloading_layers:
            return
        loader = self._layer_reload_function(layer_name)
        layer = self.layers.get(layer_name)
        if loader is None or layer is None:
            self.statusBar().showMessage(f"{layer_name} cannot be reloaded directly", 3000)
            return
        self._reloading_layers.add(layer_name)
        self._update_layer_reload_icon(layer_name)
        item = self.layer_items.get(layer_name)
        if item is not None and item.parent() is not None:
            self._update_group_reload_icon(item.parent())
        self.statusBar().showMessage(f"Reloading {layer_name}…")

        # Registered vector layers use pyproj/PROJ and must remain on the GUI
        # thread on Windows, matching the safe startup path.
        if isinstance(layer, FastShapeLayer):
            try:
                self._apply_reloaded_layer(layer_name, loader())
            except Exception as exc:
                self._layer_reload_failed(layer_name, str(exc))
            return

        worker = FunctionWorker(loader)
        worker.signals.completed.connect(
            lambda data, name=layer_name: self._apply_reloaded_layer(name, data)
        )
        worker.signals.failed.connect(
            lambda error, name=layer_name: self._layer_reload_failed(name, error)
        )
        self._start_worker(worker)

    def _apply_reloaded_layer(self, layer_name: str, data) -> None:
        layer = self.layers.get(layer_name)
        item = self.layer_items.get(layer_name)
        try:
            if isinstance(layer, FastPointLayer) and isinstance(data, PointLayerData):
                visible = layer.visible
                layer.set_data(data)
                layer.set_visible(visible)
                if item is not None:
                    item.setText(1, f"{data.count:,}")
                    item.takeChildren()
                for key, overlay in list(self._dsr_line_overlays.items()):
                    if key[0] == layer_name:
                        overlay.remove()
                        self._dsr_line_overlays.pop(key, None)
                if layer_name.startswith("DSR ") or (
                    self._custom_definition_by_layer.get(layer_name) is not None
                    and self._custom_definition_by_layer[layer_name].split_by_line
                ):
                    self._attach_dsr_hierarchy(layer_name)
                if layer_name == "DSR Primary":
                    self._populate_dsr_ribbon(data)
                    self.dsr_qc_cache.clear()
                if layer_name == "RPPreplot":
                    self._prepare_radial_circles(data)
            elif isinstance(layer, FastShapeLayer) and isinstance(data, ShapeLayerData):
                visible = layer.visible
                style = layer.style_dict()
                layer.remove()
                replacement = FastShapeLayer(self.plot_item, data, style_override=style)
                replacement.name = layer_name
                replacement.set_visible(visible)
                self.layers[layer_name] = replacement
                if item is not None:
                    item.setText(1, f"{data.count:,}")
            else:
                raise TypeError("Reloaded data does not match the layer type")
            self._update_layer_z_values()
            self.label_manager.refresh(self.layers)
            self.statusBar().showMessage(f"Reloaded {layer_name}", 3000)
        except Exception as exc:
            self.details.appendPlainText(f"Reload failed for {layer_name}: {exc}\n")
            self.statusBar().showMessage(f"Reload failed for {layer_name}: {exc}", 5000)
        finally:
            self._reloading_layers.discard(layer_name)
            self._update_layer_reload_icon(layer_name)
            if item is not None and item.parent() is not None:
                self._update_group_reload_icon(item.parent())

    def _layer_reload_failed(self, layer_name: str, error: str) -> None:
        self._reloading_layers.discard(layer_name)
        self._update_layer_reload_icon(layer_name)
        item = self.layer_items.get(layer_name)
        if item is not None and item.parent() is not None:
            self._update_group_reload_icon(item.parent())
        self.details.appendPlainText(f"Reload failed for {layer_name}: {error}\n")
        self.statusBar().showMessage(f"Reload failed for {layer_name}", 5000)

    def _toggle_layer_labels(self, layer_name: str) -> None:
        layer = self.layers.get(layer_name)
        if not isinstance(layer, FastPointLayer):
            return
        style = self.label_manager.style_for(layer_name)
        enabled = not self._layer_labels_effectively_enabled(layer_name)
        if enabled and layer_name.casefold() != "ocr image counts":
            self.label_manager.enabled = True
            blocker = QtCore.QSignalBlocker(self.ribbon.labels_button)
            self.ribbon.labels_button.setChecked(True)
            del blocker
        style["enabled"] = enabled
        self.label_manager.set_style(layer_name, style)
        self._update_layer_label_icon(layer_name)
        item = self.layer_items.get(layer_name)
        if item is not None and item.parent() is not None:
            self._update_group_label_icon(item.parent())
        self.label_manager.refresh(self.layers)
        self.statusBar().showMessage(
            f"Labels {'shown' if enabled else 'hidden'} for {layer_name}", 2500
        )

    def _register_layer(self, group: str, name: str, count: int, layer: object, tooltip: str = "") -> str:
        unique = name
        suffix = 2
        while unique in self.layers:
            unique = f"{name} ({suffix})"
            suffix += 1
        layer.name = unique
        self.layers[unique] = layer
        parent = self._group_item(group)
        item = QtWidgets.QTreeWidgetItem([unique, f"{count:,}"])
        icon_key = "layer"
        lower_name = unique.lower()
        if group == "Project shapes":
            icon_key = "shape"
        elif group == "Surfaces":
            icon_key = "map"
        elif group == "BlackBox":
            icon_key = "bbox_track"
        elif group == "Custom DSR Layers":
            icon_key = "custom_layer"
        elif "rppreplot" in lower_name or "preplot" in lower_name:
            icon_key = "preplot"
        elif "rec_db" in lower_name:
            icon_key = "rec_db"
        elif lower_name.startswith("dsr"):
            icon_key = "receiver"
        elif lower_name.startswith("sm "):
            icon_key = "station"
        item.setIcon(0, icon(icon_key, size=20))
        item.setFlags((item.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable | QtCore.Qt.ItemFlag.ItemIsDragEnabled) & ~QtCore.Qt.ItemFlag.ItemIsDropEnabled)
        saved_visible = self.viewer_config.layer_visibility.get(
            unique,
            self.viewer_config.group_visibility.get(group, True),
        )
        item.setCheckState(
            0,
            QtCore.Qt.CheckState.Checked
            if saved_visible
            else QtCore.Qt.CheckState.Unchecked,
        )
        item.setToolTip(0, tooltip)
        item.setData(0, QtCore.Qt.ItemDataRole.UserRole, unique)
        desired_layers = list(
            (getattr(self.viewer_config, "layer_order", {}) or {}).get(group, [])
        )
        if unique in desired_layers:
            wanted_index = desired_layers.index(unique)
            target = 0
            for index in range(parent.childCount()):
                existing_name = parent.child(index).data(0, QtCore.Qt.ItemDataRole.UserRole)
                if existing_name in desired_layers and desired_layers.index(existing_name) < wanted_index:
                    target += 1
            parent.insertChild(target, item)
        else:
            parent.addChild(item)
        self.layer_items[unique] = item
        layer.set_visible(bool(saved_visible))
        self.viewer_config.mark_layer_present(unique)
        self._add_layer_label_button(item, unique)
        self._update_group_visibility_icon(parent)
        self._update_group_label_icon(parent)
        self._update_group_reload_icon(parent)
        self._update_layer_z_values()
        if not self._loading_initial_layers:
            self._save_layer_tree_order()
        return unique

    def _point_layer_loaded(self, group: str, name: str, data: PointLayerData, point_color: str, line_color: str | None, connect_by: str | None) -> None:
        if self.viewer_config.is_layer_removed(name):
            self._finish_load()
            return
        layer = FastPointLayer(self.plot_item, name, point_color, line_color, connect_by)
        layer.set_data(data)
        if name == "DSR Primary":
            layer.update_style(symbol="circle", marker_text="D", point_size=8.0)
        elif name == "DSR Recovery Primary":
            layer.update_style(symbol="square", marker_text="R", point_size=8.0)
        elif name == "OCR Image Counts":
            layer.update_style(
                point_color="#00000000",
                line_color="#00000000",
                line_style="none",
                point_size=1.0,
            )
        layer.selection_changed.connect(self._show_record)
        registered_name = self._register_layer(group, name, data.count, layer)
        if name.startswith("DSR "):
            self._attach_dsr_hierarchy(registered_name)
        if name == "DSR Primary":
            self._populate_dsr_ribbon(data)
        if name == "RPPreplot":
            self._prepare_radial_circles(data)
        self._finish_load()

    def _register_shape_data(self, data: ShapeLayerData) -> None:
        style_override = self.viewer_config.shape_styles.get(data.name, {})
        layer = FastShapeLayer(self.plot_item, data, style_override=style_override)
        layer_name = data.name
        if self.viewer_config.is_layer_removed(layer_name):
            return
        group = "Project shapes"
        if data.definition.source_type == "gpkg":
            group = f"GeoPackage — {data.definition.container_name or data.definition.full_name.stem}"
        tooltip = (f"{data.definition.full_name}\n"
                   f"Layer: {data.definition.source_layer or data.definition.full_name.name}\n"
                   f"Source CRS: {data.source_crs}\nProject CRS: {data.target_crs}\nStatus: {data.crs_status}")
        registered = self._register_layer(group, layer_name, data.count, layer, tooltip)
        if layer_name not in self.viewer_config.layer_visibility and not data.definition.is_visible:
            item = self.layer_items.get(registered)
            if item is not None:
                item.setCheckState(0, QtCore.Qt.CheckState.Unchecked)
            layer.set_visible(False)

    def _shape_layer_loaded(self, data: ShapeLayerData) -> None:
        # Retained for compatibility with any caller, but normal startup now uses
        # _register_shape_data synchronously to keep pyproj out of worker threads.
        self._register_shape_data(data)
        self._finish_load()

    def _finish_load(self) -> None:
        self._pending = max(0, self._pending - 1)
        if self._pending == 0:
            self._loading_initial_layers = False
            self._save_layer_tree_order()
            self._zoom_all()
            self.statusBar().showMessage(f"Ready — {len(self.layers)} layer(s)")

    def _load_failed(self, error: str) -> None:
        self.details.appendPlainText(error + "\n")
        self._finish_load()

    def _attach_dsr_hierarchy(self, layer_name: str) -> None:
        layer = self.layers.get(layer_name)
        item = self.layer_items.get(layer_name)
        if not isinstance(layer, FastPointLayer) or layer.data is None or item is None:
            return
        line_values = self._numeric_values(layer.data.metadata.get("line"))
        station_values = self._numeric_values(layer.data.metadata.get("station"))
        if line_values.size == 0 or station_values.size == 0:
            return
        finite = np.isfinite(line_values) & np.isfinite(station_values)
        lines = np.rint(line_values[finite]).astype(np.int64)
        stations = np.rint(station_values[finite]).astype(np.int64)
        original_indices = np.flatnonzero(finite)
        for line in sorted(set(lines.tolist())):
            mask = lines == line
            count = int(mask.sum())
            child = QtWidgets.QTreeWidgetItem([f"Line {line}", f"{count:,}"])
            child.setIcon(0, icon("line", size=18))
            child.setFlags(child.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
            child.setCheckState(0, QtCore.Qt.CheckState.Unchecked)
            child.setData(0, QtCore.Qt.ItemDataRole.UserRole + 1, "dsr_line")
            child.setData(0, QtCore.Qt.ItemDataRole.UserRole + 2, layer_name)
            child.setData(0, QtCore.Qt.ItemDataRole.UserRole + 3, int(line))
            child.setToolTip(0, "Expand to load stations. Click to select line; double-click to zoom.")
            dummy = QtWidgets.QTreeWidgetItem(["Loading stations…", ""])
            dummy.setIcon(0, icon("loading", size=16))
            dummy.setData(0, QtCore.Qt.ItemDataRole.UserRole + 1, "dummy")
            child.addChild(dummy)
            item.addChild(child)

    def _set_dsr_line_overlay(self, layer_name: str, line: int, visible: bool) -> None:
        key = (layer_name, int(line))
        overlay = self._dsr_line_overlays.get(key)
        if not visible:
            if overlay is not None:
                overlay.set_visible(False)
            return
        if overlay is not None:
            overlay.set_visible(True)
            return
        source = self.layers.get(layer_name)
        if not isinstance(source, FastPointLayer) or source.data is None:
            return
        lines = self._numeric_values(source.data.metadata.get("line"))
        valid = np.isfinite(lines)
        rounded = np.zeros(lines.size, dtype=np.int64)
        rounded[valid] = np.rint(lines[valid]).astype(np.int64)
        indices = np.flatnonzero(valid & (rounded == int(line)))
        if indices.size == 0:
            return
        data = PointLayerData(
            f"{layer_name} / Line {line}",
            source.data.x[indices], source.data.y[indices], source.data.source_index[indices],
            {name: values[indices] for name, values in source.data.metadata.items()},
        )
        color = self._category_color(f"{layer_name}:{line}")
        overlay = FastPointLayer(self.plot_item, data.name, color, color, "line" if "line" in data.metadata else None)
        overlay.update_style(point_color=color, line_color=color, line_width=3.0, point_size=9.0)
        overlay.set_data(data)
        overlay.curve.setZValue(5000); overlay.scatter.setZValue(5000.1)
        overlay.selection_changed.connect(
            lambda _name, index, d=data, title=data.name: self._show_external_record(title, d, index)
        )
        self._dsr_line_overlays[key] = overlay

    def _tree_item_expanded(self, item: QtWidgets.QTreeWidgetItem) -> None:
        kind = item.data(0, QtCore.Qt.ItemDataRole.UserRole + 1)
        if kind != "dsr_line" or item.childCount() != 1:
            return
        first = item.child(0)
        if first.data(0, QtCore.Qt.ItemDataRole.UserRole + 1) != "dummy":
            return
        layer_name = str(item.data(0, QtCore.Qt.ItemDataRole.UserRole + 2) or "")
        line = int(item.data(0, QtCore.Qt.ItemDataRole.UserRole + 3))
        layer = self.layers.get(layer_name)
        if not isinstance(layer, FastPointLayer) or layer.data is None:
            return
        item.takeChildren()
        line_values = self._numeric_values(layer.data.metadata.get("line"))
        station_values = self._numeric_values(layer.data.metadata.get("station"))
        valid = np.isfinite(line_values) & np.isfinite(station_values)
        indices = np.flatnonzero(valid & (np.rint(line_values).astype(np.int64) == line))
        # Keep one navigation item per station, even when the DSR table contains
        # repeated node records. The first valid coordinate is used for selection.
        seen: set[int] = set()
        for index in indices:
            station = int(round(station_values[index]))
            if station in seen:
                continue
            seen.add(station)
            child = QtWidgets.QTreeWidgetItem([f"Station {station}", ""])
            child.setIcon(0, icon("station", size=16))
            child.setData(0, QtCore.Qt.ItemDataRole.UserRole + 1, "dsr_station")
            child.setData(0, QtCore.Qt.ItemDataRole.UserRole + 2, layer_name)
            child.setData(0, QtCore.Qt.ItemDataRole.UserRole + 3, line)
            child.setData(0, QtCore.Qt.ItemDataRole.UserRole + 4, station)
            child.setData(0, QtCore.Qt.ItemDataRole.UserRole + 5, int(index))
            child.setToolTip(0, "Click to select and zoom to this station.")
            item.addChild(child)

    def _tree_item_clicked(self, item: QtWidgets.QTreeWidgetItem, _column: int) -> None:
        kind = item.data(0, QtCore.Qt.ItemDataRole.UserRole + 1)
        if kind == "dsr_line":
            line = int(item.data(0, QtCore.Qt.ItemDataRole.UserRole + 3))
            self.ribbon.select_dsr_line(line)
            return
        if kind == "dsr_station":
            layer_name = str(item.data(0, QtCore.Qt.ItemDataRole.UserRole + 2) or "")
            line = int(item.data(0, QtCore.Qt.ItemDataRole.UserRole + 3))
            station = int(item.data(0, QtCore.Qt.ItemDataRole.UserRole + 4))
            index = int(item.data(0, QtCore.Qt.ItemDataRole.UserRole + 5))
            self.ribbon.select_dsr_line(line)
            self.ribbon.select_dsr_station(station)
            layer = self.layers.get(layer_name)
            if isinstance(layer, FastPointLayer) and layer.data is not None and 0 <= index < layer.data.count:
                x, y = float(layer.data.x[index]), float(layer.data.y[index])
                self._show_record(layer_name, index)
                self._set_station_marker(x, y)
                self._set_view_extent((x - 25.0, x + 25.0, y - 25.0, y + 25.0))
            else:
                self._zoom_dsr_station(line, station)

    def _set_dsr_auto_zoom(self, enabled: bool) -> None:
        self._dsr_auto_zoom = bool(enabled)

    def _step_dsr_station(self, direction: int) -> None:
        stations = list(self._dsr_station_values)
        if not stations:
            return
        current = self.ribbon.current_dsr_station()
        try:
            index = stations.index(int(current)) if current is not None else 0
        except ValueError:
            index = 0
        new_index = max(0, min(len(stations) - 1, index + int(direction)))
        station = stations[new_index]
        self.ribbon.select_dsr_station(station)
        line = self.ribbon.current_dsr_line()
        if line is None:
            return
        if self._dsr_auto_zoom:
            self._zoom_dsr_station(line, station)
        else:
            self._select_dsr_station(line, station)

    @staticmethod
    def _category_color(value: str) -> str:
        palette = (
            "#00e5ff", "#ffd740", "#ff6e40", "#ea80fc", "#69f0ae",
            "#82b1ff", "#ff80ab", "#b2ff59", "#ffab40", "#7c4dff",
        )
        digest = hashlib.blake2b(value.encode("utf-8", errors="ignore"), digest_size=2).digest()
        return palette[int.from_bytes(digest, "little") % len(palette)]

    def _custom_dsr_layer_loaded(self, definition: CustomDsrLayerDefinition, data: PointLayerData) -> None:
        if definition.category_field and definition.category_field.lower() in data.metadata:
            values = data.metadata[definition.category_field.lower()]
            for category in sorted({str(value) for value in values if value not in {None, ""}}):
                mask = np.asarray([str(value) == category for value in values], dtype=bool)
                indices = np.flatnonzero(mask)
                if indices.size == 0:
                    continue
                category_data = PointLayerData(
                    f"{definition.name} — {category}",
                    data.x[indices], data.y[indices], data.source_index[indices],
                    {key: value[indices] for key, value in data.metadata.items()},
                )
                if self.viewer_config.is_layer_removed(category_data.name):
                    continue
                color = definition.categories.get(category, {}).get("color", self._category_color(category))
                layer = FastPointLayer(self.plot_item, category_data.name, color, color, "line" if "line" in category_data.metadata else None)
                layer.update_style(point_color=color, line_color=color, point_size=definition.point_size, line_width=1.5)
                layer.set_data(category_data)
                layer.set_visible(definition.visible)
                layer.selection_changed.connect(self._show_record)
                registered = self._register_layer(definition.group_name or "Custom DSR Layers", category_data.name, category_data.count, layer)
                self._custom_definition_by_layer[registered] = definition
                self._update_layer_reload_icon(registered)
                self._update_group_reload_icon(self.layer_items[registered].parent())
                if definition.split_by_line:
                    self._attach_dsr_hierarchy(registered)
        else:
            if self.viewer_config.is_layer_removed(definition.name):
                self._finish_load()
                return
            color = definition.color
            layer = FastPointLayer(self.plot_item, definition.name, color, color, "line" if "line" in data.metadata else None)
            layer.update_style(point_color=color, line_color=color, point_size=definition.point_size, line_width=1.5)
            layer.set_data(data)
            layer.set_visible(definition.visible)
            layer.selection_changed.connect(self._show_record)
            registered = self._register_layer(definition.group_name or "Custom DSR Layers", definition.name, data.count, layer)
            self._custom_definition_by_layer[registered] = definition
            self._update_layer_reload_icon(registered)
            self._update_group_reload_icon(self.layer_items[registered].parent())
            if definition.split_by_line:
                self._attach_dsr_hierarchy(registered)
        self._finish_load()

    def _create_custom_dsr_layer(self) -> None:
        try:
            columns = self.repository.dsr_columns()
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Custom DSR layer", str(exc))
            return
        groups = [
            self.layer_tree.topLevelItem(index).text(0)
            for index in range(self.layer_tree.topLevelItemCount())
        ]
        preferred_group = "Custom DSR Layers"
        current = self.layer_tree.currentItem()
        if current is not None:
            if current.parent() is None and current.data(0, QtCore.Qt.ItemDataRole.UserRole + 1) == "layer_group":
                preferred_group = current.text(0)
            elif current.parent() is not None:
                preferred_group = current.parent().text(0)
        dialog = CustomDsrLayerDialog(columns, groups, preferred_group, self)
        if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
            return
        definition = dialog.definition()
        self.viewer_config.add_custom_layer(definition)
        worker = FunctionWorker(lambda d=definition: self.repository.load_custom_dsr_layer(d))
        self._pending += 1
        worker.signals.completed.connect(lambda data, d=definition: self._custom_dsr_layer_loaded(d, data))
        worker.signals.failed.connect(self._load_failed)
        self._start_worker(worker)

    def _manage_custom_dsr_layers(self) -> None:
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle("Manage custom DSR layers")
        dialog.resize(540, 360)
        layout = QtWidgets.QVBoxLayout(dialog)
        list_widget = QtWidgets.QListWidget()
        for definition in self.viewer_config.custom_dsr_layers:
            item = QtWidgets.QListWidgetItem(definition.name)
            item.setData(QtCore.Qt.ItemDataRole.UserRole, definition.id)
            list_widget.addItem(item)
        layout.addWidget(list_widget)
        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.StandardButton.Close)
        delete_button = buttons.addButton("Delete saved definition", QtWidgets.QDialogButtonBox.ButtonRole.DestructiveRole)
        delete_button.clicked.connect(lambda: self._delete_saved_custom_definition(list_widget))
        buttons.rejected.connect(dialog.reject); buttons.accepted.connect(dialog.accept)
        layout.addWidget(buttons)
        dialog.exec()

    def _delete_saved_custom_definition(self, list_widget: QtWidgets.QListWidget) -> None:
        item = list_widget.currentItem()
        if item is None:
            return
        definition_id = str(item.data(QtCore.Qt.ItemDataRole.UserRole))
        self.viewer_config.remove_custom_layer(definition_id)
        list_widget.takeItem(list_widget.row(item))

    def _edit_shape_style(self, name: str, layer: FastShapeLayer) -> None:
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle(f"Shape style — {name}")
        form = QtWidgets.QFormLayout(dialog)
        outline_button = QtWidgets.QPushButton(layer.outline_color)
        fill_button = QtWidgets.QPushButton(layer.fill_color)
        width = QtWidgets.QDoubleSpinBox(); width.setRange(0.2, 20); width.setValue(layer.outline_width)
        style = QtWidgets.QComboBox(); style.addItems(["solid", "dash", "dot"]); style.setCurrentText(layer.outline_style)
        fill_enabled = QtWidgets.QCheckBox(); fill_enabled.setChecked(layer.fill_enabled)
        fill_opacity = QtWidgets.QSpinBox(); fill_opacity.setRange(0, 255); fill_opacity.setValue(layer.fill_opacity)
        layer_opacity = QtWidgets.QDoubleSpinBox(); layer_opacity.setRange(0, 1); layer_opacity.setSingleStep(0.1); layer_opacity.setValue(layer.layer_opacity)
        point_size = QtWidgets.QDoubleSpinBox(); point_size.setRange(1, 30); point_size.setValue(layer.point_size)
        selected = {"outline": QtGui.QColor(layer.outline_color), "fill": QtGui.QColor(layer.fill_color)}
        def choose(key: str, button: QtWidgets.QPushButton) -> None:
            color = QtWidgets.QColorDialog.getColor(selected[key], dialog)
            if color.isValid():
                selected[key] = color; button.setText(color.name()); button.setStyleSheet(f"background:{color.name()}")
        outline_button.clicked.connect(lambda: choose("outline", outline_button))
        fill_button.clicked.connect(lambda: choose("fill", fill_button))
        form.addRow("Outline color:", outline_button); form.addRow("Outline width:", width); form.addRow("Outline style:", style)
        form.addRow("Enable fill:", fill_enabled); form.addRow("Fill color:", fill_button); form.addRow("Fill opacity (0–255):", fill_opacity)
        form.addRow("Layer opacity:", layer_opacity); form.addRow("Point size:", point_size)
        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.StandardButton.Ok | QtWidgets.QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(dialog.accept); buttons.rejected.connect(dialog.reject); form.addRow(buttons)
        if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
            return
        layer.update_style(
            outline_color=selected["outline"].name(), outline_width=width.value(), outline_style=style.currentText(),
            fill_enabled=fill_enabled.isChecked(), fill_color=selected["fill"].name(), fill_opacity=fill_opacity.value(),
            layer_opacity=layer_opacity.value(), point_size=point_size.value(),
        )
        self.viewer_config.set_shape_style(name, layer.style_dict())
        if layer.data.definition.source_type == "gpkg":
            self.repository.update_geopackage_layer_style(layer.data.definition, layer.style_dict())

    def _tree_item_changed(self, item: QtWidgets.QTreeWidgetItem, column: int) -> None:
        if column != 0 or self._dsr_tree_building:
            return
        kind = item.data(0, QtCore.Qt.ItemDataRole.UserRole + 1)
        if kind == "dsr_line":
            layer_name = str(item.data(0, QtCore.Qt.ItemDataRole.UserRole + 2) or "")
            line = int(item.data(0, QtCore.Qt.ItemDataRole.UserRole + 3))
            self._set_dsr_line_overlay(layer_name, line, item.checkState(0) == QtCore.Qt.CheckState.Checked)
            return
        name = item.data(0, QtCore.Qt.ItemDataRole.UserRole)
        if name in self.layers:
            visible = item.checkState(0) == QtCore.Qt.CheckState.Checked
            layer = self.layers[name]
            layer.set_visible(visible)
            if isinstance(layer, FastShapeLayer) and layer.data.definition.source_type == "gpkg":
                layer.data.definition.is_visible = visible
                self.repository.set_geopackage_layer_visible(layer.data.definition, visible)
            self.label_manager.refresh(self.layers)
            if item.parent() is not None:
                self._update_group_visibility_icon(item.parent())
            if not self._loading_initial_layers:
                self._save_layer_tree_order()

    def _layer_double_clicked(self, item: QtWidgets.QTreeWidgetItem, _column: int) -> None:
        kind = item.data(0, QtCore.Qt.ItemDataRole.UserRole + 1)
        if kind == "dsr_line":
            self._zoom_dsr_line(int(item.data(0, QtCore.Qt.ItemDataRole.UserRole + 3)))
            return
        if kind == "dsr_station":
            self._zoom_dsr_station(
                int(item.data(0, QtCore.Qt.ItemDataRole.UserRole + 3)),
                int(item.data(0, QtCore.Qt.ItemDataRole.UserRole + 4)),
            )
            return
        name = item.data(0, QtCore.Qt.ItemDataRole.UserRole)
        if name in self.layers:
            self._edit_layer_style(str(name))

    def _selected_layer_names(self) -> list[str]:
        """Return selected real layer names in current render order."""
        selected = {
            str(item.data(0, QtCore.Qt.ItemDataRole.UserRole))
            for item in self.layer_tree.selectedItems()
            if item.data(0, QtCore.Qt.ItemDataRole.UserRole) in self.layers
        }
        return [name for name in self.layers.keys() if name in selected]

    def _show_layer_context_menu(self, position: QtCore.QPoint) -> None:
        item = self.layer_tree.itemAt(position)
        if item is None:
            self._show_layers_panel_context_menu(position)
            return

        # A context click on an unselected row starts a new selection. A context
        # click on one of several selected rows preserves the complete selection.
        if not item.isSelected():
            self.layer_tree.clearSelection()
            item.setSelected(True)
            self.layer_tree.setCurrentItem(item)

        kind = item.data(0, QtCore.Qt.ItemDataRole.UserRole + 1)
        if kind == "layer_group" or (
            item.parent() is None
            and item.data(0, QtCore.Qt.ItemDataRole.UserRole) is None
        ):
            self._show_layer_group_context_menu(item, position)
            return

        names = self._selected_layer_names()
        if len(names) > 1:
            self._show_multi_layer_context_menu(names, position)
            return

        name = item.data(0, QtCore.Qt.ItemDataRole.UserRole)
        if name not in self.layers:
            return
        layer = self.layers[name]
        menu = QtWidgets.QMenu(self.layer_tree)
        zoom_action = menu.addAction(icon("zoom_layer", size=18), "Zoom to layer")
        zoom_action.triggered.connect(lambda: self._zoom_to_layer(name))
        visibility_action = menu.addAction(icon("hide" if layer.visible else "show", size=18), "Hide layer" if layer.visible else "Show layer")
        visibility_action.triggered.connect(lambda: item.setCheckState(0, QtCore.Qt.CheckState.Unchecked if layer.visible else QtCore.Qt.CheckState.Checked))
        loaded = bool(getattr(layer, "loaded", True))
        memory_action = menu.addAction(
            icon("reload" if not loaded else "hide", size=18),
            "Reload layer into memory" if not loaded else "Unload layer from memory",
        )
        memory_action.setEnabled(callable(getattr(layer, "reload" if not loaded else "unload", None)))
        memory_action.triggered.connect(lambda: self._set_layer_loaded(name, not loaded))
        only_action = menu.addAction(icon("show_only", size=18), "Show only this layer")
        only_action.triggered.connect(lambda: self._show_only_layer(name))
        menu.addSeparator()
        top_action = menu.addAction(icon("move_top", size=18), "Move to top")
        up_action = menu.addAction(icon("move_up", size=18), "Move up")
        down_action = menu.addAction(icon("move_down", size=18), "Move down")
        bottom_action = menu.addAction(icon("move_bottom", size=18), "Move to bottom")
        top_action.triggered.connect(lambda: self._move_layer(name, "top"))
        up_action.triggered.connect(lambda: self._move_layer(name, "up"))
        down_action.triggered.connect(lambda: self._move_layer(name, "down"))
        bottom_action.triggered.connect(lambda: self._move_layer(name, "bottom"))
        menu.addSeparator()
        style_action = menu.addAction(icon("style", size=18), "Style…")
        style_action.triggered.connect(lambda: self._edit_layer_style(name))
        properties_action = menu.addAction(icon("properties", size=18), "Properties")
        properties_action.triggered.connect(lambda: self._show_layer_properties(name))
        reload_action = menu.addAction(icon("reload", size=18), "Reload all layers")
        reload_action.triggered.connect(self._reload_layers)
        copy_action = menu.addAction(icon("copy", size=18), "Copy extent")
        copy_action.triggered.connect(lambda: self._copy_layer_extent(name))
        menu.addSeparator()
        delete_action = menu.addAction(icon("delete", size=18), "Delete layer")
        delete_action.triggered.connect(lambda: self._delete_layer(name))
        if isinstance(layer, FastShapeLayer):
            menu.addSeparator()
            folder_action = menu.addAction(icon("open_folder", size=18), "Open source folder")
            folder_action.triggered.connect(lambda: QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(layer.data.definition.full_name.parent))))
        menu.exec(self.layer_tree.viewport().mapToGlobal(position))

    def _show_multi_layer_context_menu(
        self, names: list[str], position: QtCore.QPoint
    ) -> None:
        names = [name for name in names if name in self.layers]
        if not names:
            return
        menu = QtWidgets.QMenu(self.layer_tree)
        title = menu.addAction(f"{len(names)} layers selected")
        title.setEnabled(False)
        menu.addSeparator()

        zoom_action = menu.addAction(icon("zoom_layer", size=18), "Zoom to selected layers")
        zoom_action.triggered.connect(lambda: self._zoom_to_selected_layers(names))
        show_action = menu.addAction(icon("show", size=18), "Show selected layers")
        hide_action = menu.addAction(icon("hide", size=18), "Hide selected layers")
        only_action = menu.addAction(icon("show_only", size=18), "Show only selected layers")
        show_action.triggered.connect(lambda: self._set_selected_layers_visible(names, True))
        hide_action.triggered.connect(lambda: self._set_selected_layers_visible(names, False))
        only_action.triggered.connect(lambda: self._show_only_selected_layers(names))
        unload_action = menu.addAction(icon("hide", size=18), "Unload selected layers from memory")
        reload_memory_action = menu.addAction(icon("reload", size=18), "Reload selected layers into memory")
        unload_action.triggered.connect(lambda: self._set_selected_layers_loaded(names, False))
        reload_memory_action.triggered.connect(lambda: self._set_selected_layers_loaded(names, True))

        menu.addSeparator()
        top_action = menu.addAction(icon("move_top", size=18), "Move selected to top")
        up_action = menu.addAction(icon("move_up", size=18), "Move selected up")
        down_action = menu.addAction(icon("move_down", size=18), "Move selected down")
        bottom_action = menu.addAction(icon("move_bottom", size=18), "Move selected to bottom")
        top_action.triggered.connect(lambda: self._move_selected_layers(names, "top"))
        up_action.triggered.connect(lambda: self._move_selected_layers(names, "up"))
        down_action.triggered.connect(lambda: self._move_selected_layers(names, "down"))
        bottom_action.triggered.connect(lambda: self._move_selected_layers(names, "bottom"))

        layers = [self.layers[name] for name in names]
        menu.addSeparator()
        style_action = menu.addAction(icon("style", size=18), "Style selected layers…")
        style_action.setEnabled(
            all(isinstance(layer, FastPointLayer) for layer in layers)
            or all(isinstance(layer, FastShapeLayer) for layer in layers)
        )
        style_action.triggered.connect(lambda: self._edit_selected_layer_styles(names))
        copy_names = menu.addAction(icon("copy", size=18), "Copy selected layer names")
        copy_names.triggered.connect(
            lambda: QtWidgets.QApplication.clipboard().setText("\n".join(names))
        )
        copy_extents = menu.addAction(icon("copy", size=18), "Copy combined extent")
        copy_extents.triggered.connect(lambda: self._copy_selected_extent(names))

        menu.addSeparator()
        delete_action = menu.addAction(icon("delete", size=18), f"Delete {len(names)} selected layers")
        delete_action.triggered.connect(lambda: self._delete_selected_layers(names))
        menu.exec(self.layer_tree.viewport().mapToGlobal(position))

    def _set_selected_layers_visible(self, names: list[str], visible: bool) -> None:
        state = QtCore.Qt.CheckState.Checked if visible else QtCore.Qt.CheckState.Unchecked
        blocker = QtCore.QSignalBlocker(self.layer_tree)
        try:
            for name in names:
                layer = self.layers.get(name)
                if layer is None:
                    continue
                layer.set_visible(visible)
                item = self.layer_items.get(name)
                if item is not None:
                    item.setCheckState(0, state)
        finally:
            del blocker
        self.label_manager.refresh(self.layers)
        for index in range(self.layer_tree.topLevelItemCount()):
            self._update_group_visibility_icon(self.layer_tree.topLevelItem(index))
        self._save_layer_tree_order()
        self.statusBar().showMessage(
            f"{'Shown' if visible else 'Hidden'} {len(names)} selected layer(s)", 2500
        )

    def _update_layer_memory_item(self, name: str) -> None:
        layer = self.layers.get(name)
        item = self.layer_items.get(name)
        if layer is None or item is None:
            return
        loaded = bool(getattr(layer, "loaded", True))
        item.setText(1, f"{layer.count:,}" if loaded else f"{layer.count:,} • unloaded")
        font = item.font(0)
        font.setItalic(not loaded)
        item.setFont(0, font)
        item.setToolTip(0, (item.toolTip(0).split("\nMemory:")[0] + f"\nMemory: {'Loaded' if loaded else 'Unloaded'}").strip())

    def _set_layer_loaded(self, name: str, loaded: bool) -> None:
        layer = self.layers.get(name)
        if layer is None:
            return
        method = getattr(layer, "reload" if loaded else "unload", None)
        if not callable(method):
            self.statusBar().showMessage(f"Layer does not support {'reload' if loaded else 'unload'}: {name}", 3500)
            return
        if not loaded:
            layer.set_visible(False)
            item = self.layer_items.get(name)
            if item is not None:
                blocker = QtCore.QSignalBlocker(self.layer_tree)
                item.setCheckState(0, QtCore.Qt.CheckState.Unchecked)
                del blocker
        ok = bool(method())
        if loaded and ok:
            layer.set_visible(True)
            item = self.layer_items.get(name)
            if item is not None:
                blocker = QtCore.QSignalBlocker(self.layer_tree)
                item.setCheckState(0, QtCore.Qt.CheckState.Checked)
                del blocker
        self._update_layer_memory_item(name)
        self.label_manager.refresh(self.layers)
        self._save_layer_tree_order()
        self.statusBar().showMessage(
            f"{'Loaded' if loaded else 'Unloaded'} layer: {name}" if ok else f"Could not {'load' if loaded else 'unload'} layer: {name}",
            3000,
        )

    def _set_selected_layers_loaded(self, names: list[str], loaded: bool) -> None:
        changed = 0
        for name in names:
            layer = self.layers.get(name)
            method = getattr(layer, "reload" if loaded else "unload", None) if layer is not None else None
            if not callable(method):
                continue
            if not loaded:
                layer.set_visible(False)
            if method():
                changed += 1
                if loaded:
                    layer.set_visible(True)
            item = self.layer_items.get(name)
            if item is not None:
                blocker = QtCore.QSignalBlocker(self.layer_tree)
                item.setCheckState(0, QtCore.Qt.CheckState.Checked if loaded else QtCore.Qt.CheckState.Unchecked)
                del blocker
            self._update_layer_memory_item(name)
        self.label_manager.refresh(self.layers)
        self.statusBar().showMessage(f"{'Loaded' if loaded else 'Unloaded'} {changed} layer(s)", 3000)

    def _show_only_selected_layers(self, names: list[str]) -> None:
        selected = set(names)
        blocker = QtCore.QSignalBlocker(self.layer_tree)
        try:
            for name, layer in self.layers.items():
                visible = name in selected
                layer.set_visible(visible)
                item = self.layer_items.get(name)
                if item is not None:
                    item.setCheckState(
                        0, QtCore.Qt.CheckState.Checked if visible else QtCore.Qt.CheckState.Unchecked
                    )
        finally:
            del blocker
        self.label_manager.refresh(self.layers)
        self.statusBar().showMessage(
            f"Showing only {len(selected)} selected layer(s)", 2500
        )

    def _combined_bounds(self, names: list[str]):
        bounds = [
            self.layers[name].bounds
            for name in names
            if name in self.layers and self.layers[name].bounds is not None
        ]
        if not bounds:
            return None
        return (
            min(value[0] for value in bounds),
            max(value[1] for value in bounds),
            min(value[2] for value in bounds),
            max(value[3] for value in bounds),
        )

    def _zoom_to_selected_layers(self, names: list[str]) -> None:
        extent = self._combined_bounds(names)
        if extent is None:
            self.statusBar().showMessage("Selected layers have no valid extent", 3000)
            return
        xmin, xmax, ymin, ymax = extent
        dx = max(xmax - xmin, 1.0)
        dy = max(ymax - ymin, 1.0)
        self.plot_item.setXRange(xmin - dx * 0.05, xmax + dx * 0.05, padding=0)
        self.plot_item.setYRange(ymin - dy * 0.05, ymax + dy * 0.05, padding=0)
        self.statusBar().showMessage(f"Zoomed to {len(names)} selected layer(s)", 2500)

    def _copy_selected_extent(self, names: list[str]) -> None:
        extent = self._combined_bounds(names)
        if extent is None:
            return
        xmin, xmax, ymin, ymax = extent
        QtWidgets.QApplication.clipboard().setText(
            f"{xmin:.3f}, {xmax:.3f}, {ymin:.3f}, {ymax:.3f}"
        )
        self.statusBar().showMessage("Combined extent copied", 2500)

    def _move_selected_layers(self, names: list[str], direction: str) -> None:
        items = [self.layer_items[name] for name in names if name in self.layer_items]
        if not items:
            return
        parents = {item.parent() for item in items}
        if len(parents) != 1:
            QtWidgets.QMessageBox.information(
                self,
                "Move layers",
                "Selected layers can be moved together only when they belong to the same group.",
            )
            return
        parent = next(iter(parents))
        if parent is None:
            return
        selected_items = set(items)
        siblings = [parent.child(index) for index in range(parent.childCount())]
        if direction == "top":
            ordered = [item for item in siblings if item in selected_items] + [
                item for item in siblings if item not in selected_items
            ]
        elif direction == "bottom":
            ordered = [item for item in siblings if item not in selected_items] + [
                item for item in siblings if item in selected_items
            ]
        elif direction == "up":
            ordered = list(siblings)
            for index in range(1, len(ordered)):
                if ordered[index] in selected_items and ordered[index - 1] not in selected_items:
                    ordered[index - 1], ordered[index] = ordered[index], ordered[index - 1]
        elif direction == "down":
            ordered = list(siblings)
            for index in range(len(ordered) - 2, -1, -1):
                if ordered[index] in selected_items and ordered[index + 1] not in selected_items:
                    ordered[index], ordered[index + 1] = ordered[index + 1], ordered[index]
        else:
            return
        parent.takeChildren()
        parent.addChildren(ordered)
        for item in items:
            item.setSelected(True)
        self._sync_layers_from_tree()
        self._save_layer_tree_order()
        self.statusBar().showMessage(f"Moved {len(items)} selected layer(s)", 2500)

    def _edit_selected_layer_styles(self, names: list[str]) -> None:
        layers = [self.layers[name] for name in names if name in self.layers]
        if not layers:
            return
        if all(isinstance(layer, FastPointLayer) for layer in layers):
            first = layers[0]
            dialog = QtWidgets.QDialog(self)
            dialog.setWindowTitle(f"Style {len(layers)} selected point layers")
            form = QtWidgets.QFormLayout(dialog)
            line_button = QtWidgets.QPushButton(first.line_color)
            point_button = QtWidgets.QPushButton(first.point_color)
            line_width = QtWidgets.QDoubleSpinBox(); line_width.setRange(0.2, 20.0); line_width.setValue(first.line_width)
            line_style = QtWidgets.QComboBox(); line_style.addItems(["solid", "dash", "dot", "dash dot", "dash dot dot", "none"]); line_style.setCurrentText(str(getattr(first, "line_style", "solid")))
            point_size = QtWidgets.QDoubleSpinBox(); point_size.setRange(1.0, 30.0); point_size.setValue(first.point_size)
            selected_colors = {"line": QtGui.QColor(first.line_color), "point": QtGui.QColor(first.point_color)}
            def choose(key, button):
                color = QtWidgets.QColorDialog.getColor(selected_colors[key], dialog)
                if color.isValid():
                    selected_colors[key] = color
                    button.setText(color.name())
                    button.setStyleSheet(f"background:{color.name()}")
            line_button.clicked.connect(lambda: choose("line", line_button))
            point_button.clicked.connect(lambda: choose("point", point_button))
            form.addRow("Line color:", line_button); form.addRow("Point color:", point_button)
            form.addRow("Line thickness:", line_width); form.addRow("Line style:", line_style); form.addRow("Point size:", point_size)
            buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.StandardButton.Ok | QtWidgets.QDialogButtonBox.StandardButton.Cancel)
            buttons.accepted.connect(dialog.accept); buttons.rejected.connect(dialog.reject); form.addRow(buttons)
            if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
                return
            for layer in layers:
                layer.update_style(
                    line_color=selected_colors["line"].name(),
                    point_color=selected_colors["point"].name(),
                    line_width=line_width.value(), line_style=line_style.currentText(), point_size=point_size.value(),
                )
        elif all(isinstance(layer, FastShapeLayer) for layer in layers):
            first_name = names[0]
            first = layers[0]
            self._edit_shape_style(first_name, first)
            style = first.style_dict()
            for name, layer in zip(names[1:], layers[1:]):
                layer.update_style(**style)
                self.viewer_config.set_shape_style(name, layer.style_dict())
        if layer.data.definition.source_type == "gpkg":
            self.repository.update_geopackage_layer_style(layer.data.definition, layer.style_dict())
        self.statusBar().showMessage(f"Updated style for {len(layers)} selected layer(s)", 3000)

    def _remove_layer_without_prompt(self, name: str) -> None:
        layer = self.layers.get(name)
        if layer is None:
            return
        try:
            layer.remove()
        except Exception:
            pass
        self.layers.pop(name, None)
        self._reloading_layers.discard(name)
        self._custom_definition_by_layer.pop(name, None)
        item = self.layer_items.pop(name, None)
        if item is not None:
            parent = item.parent()
            if parent is not None:
                parent.takeChild(parent.indexOfChild(item))
                if parent.childCount() == 0:
                    index = self.layer_tree.indexOfTopLevelItem(parent)
                    if index >= 0:
                        self.layer_tree.takeTopLevelItem(index)
        for key, registered_name in list(self.bbox_track_layer_names.items()):
            if registered_name == name:
                self.bbox_track_layer_names.pop(key, None)
        if self.bbox_last_track_layer_name == name:
            self.bbox_last_track_layer_name = (
                next(reversed(self.bbox_track_layer_names.values()), None)
                if self.bbox_track_layer_names else None
            )
        self.viewer_config.mark_layer_removed(name)
        self._save_layer_tree_order()

    def _delete_selected_layers(self, names: list[str]) -> None:
        names = [name for name in names if name in self.layers]
        if not names:
            return
        preview = "\n".join(names[:12])
        if len(names) > 12:
            preview += f"\n… and {len(names) - 12} more"
        answer = QtWidgets.QMessageBox.question(
            self, "Delete selected layers",
            f"Remove {len(names)} selected layers from the current map?\n\n{preview}\n\n"
            "Database records and source files will not be deleted.",
            QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No,
            QtWidgets.QMessageBox.StandardButton.No,
        )
        if answer != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        for name in names:
            self._remove_layer_without_prompt(name)
        if not self.bbox_track_layer_names:
            self.ribbon.bbox_track_button.setChecked(False)
        self._update_layer_z_values()
        self.label_manager.refresh(self.layers)
        self.statusBar().showMessage(f"Removed {len(names)} selected layer(s)", 3000)

    def _group_layer_names(self, group_item: QtWidgets.QTreeWidgetItem) -> list[str]:
        names: list[str] = []
        for index in range(group_item.childCount()):
            child = group_item.child(index)
            name = child.data(0, QtCore.Qt.ItemDataRole.UserRole)
            if name in self.layers:
                names.append(str(name))
        return names

    def _show_layers_panel_context_menu(self, position: QtCore.QPoint) -> None:
        menu = QtWidgets.QMenu(self.layer_tree)
        create_action = menu.addAction(icon("group", size=18), "Create new group…")
        create_action.triggered.connect(self._create_layer_group)
        menu.addSeparator()
        expand_action = menu.addAction("Expand everything")
        collapse_action = menu.addAction("Collapse everything")
        expand_action.triggered.connect(self.layer_tree.expandAll)
        collapse_action.triggered.connect(self.layer_tree.collapseAll)
        menu.exec(self.layer_tree.viewport().mapToGlobal(position))

    def _show_layer_group_context_menu(
        self,
        group_item: QtWidgets.QTreeWidgetItem,
        position: QtCore.QPoint,
    ) -> None:
        group_name = str(
            group_item.data(0, QtCore.Qt.ItemDataRole.UserRole + 2)
            or group_item.text(0)
        )
        names = self._group_layer_names(group_item)
        menu = QtWidgets.QMenu(self.layer_tree)

        zoom_action = menu.addAction(icon("zoom_layer", size=18), "Zoom to group")
        zoom_action.setEnabled(bool(names))
        zoom_action.triggered.connect(lambda: self._zoom_to_layer_group(group_name))

        menu.addSeparator()
        show_action = menu.addAction(icon("show", size=18), "Show all layers in group")
        hide_action = menu.addAction(icon("hide", size=18), "Hide all layers in group")
        only_action = menu.addAction(icon("show_only", size=18), "Show only this group")
        show_action.setEnabled(bool(names))
        hide_action.setEnabled(bool(names))
        only_action.setEnabled(bool(names))
        show_action.triggered.connect(lambda: self._set_layer_group_visible(group_name, True))
        hide_action.triggered.connect(lambda: self._set_layer_group_visible(group_name, False))
        only_action.triggered.connect(lambda: self._show_only_layer_group(group_name))

        point_names = self._group_point_layer_names(group_item)
        menu.addSeparator()
        show_labels_action = menu.addAction(
            icon("labels_show", size=18), "Show labels for all layers"
        )
        hide_labels_action = menu.addAction(
            icon("labels_hide", size=18), "Hide labels for all layers"
        )
        show_labels_action.setEnabled(bool(point_names))
        hide_labels_action.setEnabled(bool(point_names))
        show_labels_action.triggered.connect(
            lambda: self._set_layer_group_labels(group_name, True)
        )
        hide_labels_action.triggered.connect(
            lambda: self._set_layer_group_labels(group_name, False)
        )

        menu.addSeparator()
        move_top_action = menu.addAction(icon("move_top", size=18), "Move group to top")
        move_up_action = menu.addAction(icon("move_up", size=18), "Move group up")
        move_down_action = menu.addAction(icon("move_down", size=18), "Move group down")
        move_bottom_action = menu.addAction(icon("move_bottom", size=18), "Move group to bottom")
        move_top_action.triggered.connect(lambda: self._move_layer_group(group_item, "top"))
        move_up_action.triggered.connect(lambda: self._move_layer_group(group_item, "up"))
        move_down_action.triggered.connect(lambda: self._move_layer_group(group_item, "down"))
        move_bottom_action.triggered.connect(lambda: self._move_layer_group(group_item, "bottom"))

        menu.addSeparator()
        expand_action = menu.addAction(icon("move_down", size=18), "Expand group")
        collapse_action = menu.addAction(icon("move_up", size=18), "Collapse group")
        expand_action.triggered.connect(lambda: group_item.setExpanded(True))
        collapse_action.triggered.connect(lambda: group_item.setExpanded(False))

        menu.addSeparator()
        copy_action = menu.addAction(icon("copy", size=18), "Copy layer names")
        copy_action.setEnabled(bool(names))
        copy_action.triggered.connect(
            lambda: QtWidgets.QApplication.clipboard().setText("\n".join(names))
        )

        menu.addSeparator()
        delete_action = menu.addAction(icon("delete", size=18), "Delete group")
        delete_action.triggered.connect(lambda: self._delete_layer_group(group_item))
        delete_all_action = menu.addAction(
            icon("delete", size=18), "Delete group and all layers"
        )
        delete_all_action.setEnabled(bool(names))
        delete_all_action.triggered.connect(
            lambda: self._delete_layer_group_with_layers(group_item)
        )
        menu.exec(self.layer_tree.viewport().mapToGlobal(position))

    def _delete_layer_group(self, group_item: QtWidgets.QTreeWidgetItem) -> None:
        group_name = str(
            group_item.data(0, QtCore.Qt.ItemDataRole.UserRole + 2)
            or group_item.text(0)
        )
        layer_count = group_item.childCount()
        message = f"Delete group '{group_name}'?"
        if layer_count:
            message += f"\n\nIts {layer_count} layer(s) will be moved to the 'Ungrouped' group."
        answer = QtWidgets.QMessageBox.question(
            self,
            "Delete layer group",
            message,
            QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No,
            QtWidgets.QMessageBox.StandardButton.No,
        )
        if answer != QtWidgets.QMessageBox.StandardButton.Yes:
            return

        if layer_count:
            target = self._group_item("Ungrouped")
            children = group_item.takeChildren()
            target.addChildren(children)
            target.setExpanded(True)

        index = self.layer_tree.indexOfTopLevelItem(group_item)
        if index >= 0:
            self.layer_tree.takeTopLevelItem(index)
        self._sync_layers_from_tree()
        self._save_layer_tree_order()
        self.statusBar().showMessage(f"Deleted layer group: {group_name}", 2500)

    def _delete_layer_group_with_layers(
        self, group_item: QtWidgets.QTreeWidgetItem
    ) -> None:
        group_name = str(
            group_item.data(0, QtCore.Qt.ItemDataRole.UserRole + 2)
            or group_item.text(0)
        )
        names = self._group_layer_names(group_item)
        if not names:
            self._delete_layer_group(group_item)
            return
        preview = "\n".join(names[:12])
        if len(names) > 12:
            preview += f"\n… and {len(names) - 12} more"
        answer = QtWidgets.QMessageBox.warning(
            self,
            "Delete group and all layers",
            f"Delete group '{group_name}' and remove all {len(names)} layer(s) from the map?\n\n"
            f"{preview}\n\nDatabase records and source files will not be deleted.",
            QtWidgets.QMessageBox.StandardButton.Yes
            | QtWidgets.QMessageBox.StandardButton.No,
            QtWidgets.QMessageBox.StandardButton.No,
        )
        if answer != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        for name in names:
            self._remove_layer_without_prompt(name)
        self._update_layer_z_values()
        self.label_manager.refresh(self.layers)
        self._save_layer_tree_order()
        self.statusBar().showMessage(
            f"Deleted group '{group_name}' and removed {len(names)} layer(s)", 3500
        )

    def _set_layer_group_visible(self, group_name: str, visible: bool) -> None:
        group_item = self._group_item(group_name)
        self.viewer_config.group_visibility[group_name] = bool(visible)

        state = (
            QtCore.Qt.CheckState.Checked
            if visible
            else QtCore.Qt.CheckState.Unchecked
        )

        blocker = QtCore.QSignalBlocker(self.layer_tree)

        try:
            for index in range(group_item.childCount()):
                child = group_item.child(index)
                name = child.data(0, QtCore.Qt.ItemDataRole.UserRole)

                if name not in self.layers:
                    continue

                child.setCheckState(0, state)

                layer = self.layers[name]
                setter = getattr(layer, "set_visible", None)

                if callable(setter):
                    setter(visible)
                if (
                    isinstance(layer, FastShapeLayer)
                    and layer.data.definition.source_type == "gpkg"
                ):
                    layer.data.definition.is_visible = visible
                    self.repository.set_geopackage_layer_visible(
                        layer.data.definition, visible
                    )
        finally:
            del blocker

        self.label_manager.refresh(self.layers)
        self._update_group_visibility_icon(group_item)
        self._save_layer_tree_order()

        try:
            self.plot_item.update()
            self.plot_item.scene().update()
        except Exception:
            pass

    def _show_only_layer_group(self, group_name: str) -> None:
        target = set(self._group_layer_names(self._group_item(group_name)))
        for name, layer in self.layers.items():
            visible = name in target
            layer.set_visible(visible)
            item = self.layer_items.get(name)
            if item is not None:
                blocker = QtCore.QSignalBlocker(self.layer_tree)
                item.setCheckState(
                    0,
                    QtCore.Qt.CheckState.Checked if visible else QtCore.Qt.CheckState.Unchecked,
                )
                del blocker
        self.label_manager.refresh(self.layers)
        for index in range(self.layer_tree.topLevelItemCount()):
            self._update_group_visibility_icon(self.layer_tree.topLevelItem(index))
        self._save_layer_tree_order()
        self.statusBar().showMessage(f"Showing only group: {group_name}", 2500)

    def _zoom_to_layer_group(self, group_name: str) -> None:
        bounds = []
        for name in self._group_layer_names(self._group_item(group_name)):
            layer = self.layers.get(name)
            if layer is None or not layer.visible:
                continue
            extent = layer.bounds
            if extent is not None:
                bounds.append(extent)
        if not bounds:
            # Include hidden layers when all group layers are hidden.
            for name in self._group_layer_names(self._group_item(group_name)):
                layer = self.layers.get(name)
                if layer is not None and layer.bounds is not None:
                    bounds.append(layer.bounds)
        if not bounds:
            self.statusBar().showMessage(f"Group '{group_name}' has no valid extent", 3500)
            return
        xmin = min(value[0] for value in bounds)
        xmax = max(value[1] for value in bounds)
        ymin = min(value[2] for value in bounds)
        ymax = max(value[3] for value in bounds)
        dx = max(xmax - xmin, 1.0)
        dy = max(ymax - ymin, 1.0)
        self.plot_item.setXRange(xmin - dx * 0.05, xmax + dx * 0.05, padding=0)
        self.plot_item.setYRange(ymin - dy * 0.05, ymax + dy * 0.05, padding=0)
        self.statusBar().showMessage(f"Zoomed to group: {group_name}", 2500)

    def _reset_workspace_layout(self) -> None:
        self.setDockNestingEnabled(True)
        self.addDockWidget(QtCore.Qt.DockWidgetArea.LeftDockWidgetArea, self.layers_dock)
        self.addDockWidget(QtCore.Qt.DockWidgetArea.RightDockWidgetArea, self.details_dock)
        self.addDockWidget(QtCore.Qt.DockWidgetArea.RightDockWidgetArea, self.measure_dock)
        self.addDockWidget(QtCore.Qt.DockWidgetArea.BottomDockWidgetArea, self.ocr_images_dock)
        self.tabifyDockWidget(self.details_dock, self.measure_dock)
        self.details_dock.raise_()
        for dock in (self.layers_dock, self.details_dock, self.measure_dock, self.ocr_images_dock):
            dock.setFloating(False)
            dock.show()
        self.resizeDocks([self.ocr_images_dock], [220], QtCore.Qt.Orientation.Vertical)
        self.statusBar().show()
        self.ribbon.set_panel_button_checked("layers", True)
        self.ribbon.set_panel_button_checked("feature", True)
        self.ribbon.set_panel_button_checked("measurement", True)
        self.ribbon.set_panel_button_checked("ocr", True)
        self.ribbon.set_panel_button_checked("status", True)
        self.statusBar().showMessage("Default workspace layout restored", 3000)

    def _delete_layer(self, name: str) -> None:
        layer = self.layers.get(name)
        if layer is None:
            return
        answer = QtWidgets.QMessageBox.question(
            self,
            "Delete layer",
            f"Remove '{name}' from the current map?\n\nThis does not delete database records or source files.",
            QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No,
            QtWidgets.QMessageBox.StandardButton.No,
        )
        if answer != QtWidgets.QMessageBox.StandardButton.Yes:
            return

        self._remove_layer_without_prompt(name)
        if not self.bbox_track_layer_names:
            self.ribbon.bbox_track_button.setChecked(False)

        self._update_layer_z_values()
        self.statusBar().showMessage(f"Removed layer: {name}", 3000)

    def _show_only_layer(self, name: str) -> None:
        for layer_name, item in self.layer_items.items():
            item.setCheckState(0, QtCore.Qt.CheckState.Checked if layer_name == name else QtCore.Qt.CheckState.Unchecked)

    def _tree_sibling_context(
        self, item: QtWidgets.QTreeWidgetItem
    ) -> tuple[QtWidgets.QTreeWidgetItem | None, int, int]:
        """Return parent, current index and sibling count for a tree item."""
        parent = item.parent()
        if parent is None:
            index = self.layer_tree.indexOfTopLevelItem(item)
            count = self.layer_tree.topLevelItemCount()
        else:
            index = parent.indexOfChild(item)
            count = parent.childCount()
        return parent, index, count

    def _move_tree_item(
        self, item: QtWidgets.QTreeWidgetItem, direction: str
    ) -> bool:
        """Move a top-level group or a layer among its current siblings."""
        parent, current_index, count = self._tree_sibling_context(item)
        if current_index < 0 or count <= 1:
            return False
        if direction == "top":
            target_index = 0
        elif direction == "bottom":
            target_index = count - 1
        elif direction == "up":
            target_index = max(0, current_index - 1)
        elif direction == "down":
            target_index = min(count - 1, current_index + 1)
        else:
            return False
        if target_index == current_index:
            return False

        if parent is None:
            moved = self.layer_tree.takeTopLevelItem(current_index)
            self.layer_tree.insertTopLevelItem(target_index, moved)
        else:
            moved = parent.takeChild(current_index)
            parent.insertChild(target_index, moved)
        self.layer_tree.setCurrentItem(moved)
        moved.setSelected(True)
        self._sync_layers_from_tree()
        self._save_layer_tree_order()
        return True

    def _move_layer(self, name: str, direction: str) -> None:
        item = self.layer_items.get(name)
        if item is None:
            return
        if self._move_tree_item(item, direction):
            self.statusBar().showMessage(f"Moved layer: {name}", 2500)

    def _move_layer_group(
        self, group_item: QtWidgets.QTreeWidgetItem, direction: str
    ) -> None:
        group_name = str(
            group_item.data(0, QtCore.Qt.ItemDataRole.UserRole + 2)
            or group_item.text(0)
        )
        if self._move_tree_item(group_item, direction):
            self.statusBar().showMessage(f"Moved group: {group_name}", 2500)

    def _tree_layer_names(self) -> list[str]:
        """Flatten direct group children in visual top-to-bottom order."""
        names: list[str] = []
        for group_index in range(self.layer_tree.topLevelItemCount()):
            group_item = self.layer_tree.topLevelItem(group_index)
            for child_index in range(group_item.childCount()):
                child = group_item.child(child_index)
                name = child.data(0, QtCore.Qt.ItemDataRole.UserRole)
                if isinstance(name, str) and name in self.layers:
                    names.append(name)
        # Keep any transient/non-tree layers safe at the end.
        names.extend(name for name in self.layers if name not in names)
        return names

    def _sync_layers_from_tree(self) -> None:
        names = self._tree_layer_names()
        self.layers = OrderedDict((name, self.layers[name]) for name in names)
        self._update_layer_z_values(sync_tree=False)

    def _save_layer_tree_order(self) -> None:
        group_order: list[str] = []
        layer_order: dict[str, list[str]] = {}
        layer_visibility: dict[str, bool] = {}
        group_visibility: dict[str, bool] = {}
        for group_index in range(self.layer_tree.topLevelItemCount()):
            group_item = self.layer_tree.topLevelItem(group_index)
            group_name = str(
                group_item.data(0, QtCore.Qt.ItemDataRole.UserRole + 2)
                or group_item.text(0)
            )
            group_order.append(group_name)
            layer_order[group_name] = []
            child_visibility: list[bool] = []
            for child_index in range(group_item.childCount()):
                child = group_item.child(child_index)
                name = child.data(
                    0, QtCore.Qt.ItemDataRole.UserRole
                )
                if isinstance(name, str) and name in self.layers:
                    layer_order[group_name].append(name)
                    visible = child.checkState(0) == QtCore.Qt.CheckState.Checked
                    layer_visibility[name] = visible
                    child_visibility.append(visible)
            group_visibility[group_name] = (
                any(child_visibility)
                if child_visibility
                else self.viewer_config.group_visibility.get(group_name, True)
            )
        setter = getattr(self.viewer_config, "set_layer_tree_state", None)
        if callable(setter):
            setter(group_order, layer_order, layer_visibility, group_visibility)
        else:
            self.viewer_config.set_layer_tree_order(group_order, layer_order)
        # Qt may discard item widgets when rows are detached for drag/drop or
        # ordering operations. Restore the group eye and layer label buttons.
        for group_index in range(self.layer_tree.topLevelItemCount()):
            group_item = self.layer_tree.topLevelItem(group_index)
            group_name = str(
                group_item.data(0, QtCore.Qt.ItemDataRole.UserRole + 2)
                or group_item.text(0)
            )
            if self.layer_tree.itemWidget(group_item, 2) is None:
                self._add_group_visibility_buttons(group_item, group_name)
            else:
                self._update_group_visibility_icon(group_item)
                self._update_group_label_icon(group_item)
                self._update_group_reload_icon(group_item)
            for child_index in range(group_item.childCount()):
                child = group_item.child(child_index)
                name = child.data(0, QtCore.Qt.ItemDataRole.UserRole)
                if name in self.layers and self.layer_tree.itemWidget(child, 2) is None:
                    self._add_layer_label_button(child, str(name))

    def _update_layer_z_values(self, *, sync_tree: bool = True) -> None:
        # Tree order is the single source of truth: first row is rendered on top.
        if sync_tree and hasattr(self, "layer_tree"):
            names = self._tree_layer_names()
            if names and set(names) == set(self.layers):
                self.layers = OrderedDict((name, self.layers[name]) for name in names)
        total = len(self.layers)
        for index, layer in enumerate(self.layers.values()):
            setter = getattr(layer, "set_z_value", None)
            if callable(setter):
                setter(float(total - index))

    def _copy_layer_extent(self, name: str) -> None:
        layer = self.layers.get(name)
        if not layer or not layer.bounds:
            return
        xmin, xmax, ymin, ymax = layer.bounds
        QtWidgets.QApplication.clipboard().setText(f"{xmin:.3f}, {xmax:.3f}, {ymin:.3f}, {ymax:.3f}")
        self.statusBar().showMessage("Layer extent copied", 2500)

    def _edit_layer_style(self, name: str) -> None:
        layer = self.layers.get(name)
        if isinstance(layer, FastShapeLayer):
            self._edit_shape_style(name, layer)
            return
        if not isinstance(layer, FastPointLayer):
            QtWidgets.QMessageBox.information(
                self,
                "Layer style",
                "This layer does not support runtime style editing.",
            )
            return

        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle(f"Layer style — {name}")
        form = QtWidgets.QFormLayout(dialog)

        line_button = QtWidgets.QPushButton(layer.line_color)
        point_button = QtWidgets.QPushButton(layer.point_color)
        line_width = QtWidgets.QDoubleSpinBox()
        line_width.setRange(0.2, 20.0)
        line_width.setDecimals(1)
        line_width.setSingleStep(0.5)
        line_width.setValue(layer.line_width)
        line_style = QtWidgets.QComboBox()
        line_style.addItems(["solid", "dash", "dot", "dash dot", "dash dot dot", "none"])
        line_style.setCurrentText(str(getattr(layer, "line_style", "solid")))
        point_size = QtWidgets.QDoubleSpinBox()
        point_size.setRange(1.0, 30.0)
        point_size.setDecimals(1)
        point_size.setSingleStep(1.0)
        point_size.setValue(layer.point_size)
        symbol_combo = QtWidgets.QComboBox()
        symbol_combo.addItems(["Circle", "Square", "Triangle", "Triangle Down", "Diamond", "Star", "Cross", "Plus", "Pentagon", "Hexagon"])
        current_symbol = str(getattr(layer, "symbol_name", "circle"))
        idx = symbol_combo.findText(current_symbol, QtCore.Qt.MatchFlag.MatchFixedString)
        if idx < 0:
            idx = symbol_combo.findText(current_symbol.title())
        symbol_combo.setCurrentIndex(max(0, idx))
        marker_text = QtWidgets.QLineEdit(str(getattr(layer, "marker_text", "")))
        marker_text.setMaxLength(2)
        marker_text.setPlaceholderText("Optional, e.g. D or R")

        label_style = self.label_manager.style_for(name)
        label_enabled = QtWidgets.QCheckBox()
        label_enabled.setChecked(bool(label_style.get("enabled", True)))
        label_format = QtWidgets.QPlainTextEdit()
        label_format.setMaximumHeight(72)
        label_format.setPlaceholderText("Example: {Line}/{Station}\n{Node}\n{ROV}")
        current_format = str(label_style.get("format") or "")
        if not current_format:
            current_format = self.label_manager.default_format(layer)
        label_format.setPlainText(current_format)
        label_size = QtWidgets.QDoubleSpinBox()
        label_size.setRange(4.0, 36.0); label_size.setDecimals(1); label_size.setValue(float(label_style.get("font_size", 9.0)))
        label_offset_x = QtWidgets.QDoubleSpinBox()
        label_offset_x.setRange(-100.0, 100.0); label_offset_x.setDecimals(1); label_offset_x.setSuffix(" px"); label_offset_x.setValue(float(label_style.get("offset_x", 5.0)))
        label_offset_y = QtWidgets.QDoubleSpinBox()
        label_offset_y.setRange(-100.0, 100.0); label_offset_y.setDecimals(1); label_offset_y.setSuffix(" px"); label_offset_y.setValue(float(label_style.get("offset_y", -5.0)))
        label_max = QtWidgets.QSpinBox()
        label_max.setRange(1, 5000); label_max.setValue(int(label_style.get("max_labels", 300)))
        label_color_button = QtWidgets.QPushButton(str(label_style.get("color", "#f3f6f8")))
        label_fields = QtWidgets.QComboBox()
        label_fields.addItem("Insert field…", "")
        if layer.data is not None:
            for field_name in layer.data.metadata.keys():
                label_fields.addItem(str(field_name), str(field_name))
        def insert_label_field(_index: int) -> None:
            field_name = str(label_fields.currentData() or "")
            if not field_name:
                return
            cursor = label_format.textCursor()
            cursor.insertText("{" + field_name + "}")
            label_format.setTextCursor(cursor)
            label_fields.setCurrentIndex(0)
        label_fields.currentIndexChanged.connect(insert_label_field)

        selected = {
            "line": QtGui.QColor(layer.line_color),
            "point": QtGui.QColor(layer.point_color),
            "label": QtGui.QColor(str(label_style.get("color", "#f3f6f8"))),
        }

        def update_button(button: QtWidgets.QPushButton, color: QtGui.QColor) -> None:
            button.setText(color.name())
            foreground = "#000000" if color.lightness() > 150 else "#ffffff"
            button.setStyleSheet(
                f"background-color: {color.name()}; color: {foreground};"
            )

        def choose(key: str, button: QtWidgets.QPushButton) -> None:
            color = QtWidgets.QColorDialog.getColor(selected[key], dialog)
            if color.isValid():
                selected[key] = color
                update_button(button, color)

        update_button(line_button, selected["line"])
        update_button(point_button, selected["point"])
        line_button.clicked.connect(lambda: choose("line", line_button))
        point_button.clicked.connect(lambda: choose("point", point_button))
        update_button(label_color_button, selected["label"])
        label_color_button.clicked.connect(lambda: choose("label", label_color_button))

        form.addRow("Line color:", line_button)
        form.addRow("Point color:", point_button)
        form.addRow("Line thickness:", line_width)
        form.addRow("Line style:", line_style)
        form.addRow("Point size:", point_size)
        form.addRow("Point symbol:", symbol_combo)
        form.addRow("Marker letter:", marker_text)
        separator = QtWidgets.QFrame(); separator.setFrameShape(QtWidgets.QFrame.Shape.HLine)
        form.addRow(separator)
        form.addRow("Show point labels:", label_enabled)
        form.addRow("Label format:", label_format)
        form.addRow("Available fields:", label_fields)
        form.addRow("Label color:", label_color_button)
        form.addRow("Label size:", label_size)
        form.addRow("Label X offset:", label_offset_x)
        form.addRow("Label Y offset:", label_offset_y)
        form.addRow("Max labels in view:", label_max)

        buttons = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.StandardButton.Ok
            | QtWidgets.QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        form.addRow(buttons)

        if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
            return

        layer.update_style(
            line_color=selected["line"].name(),
            point_color=selected["point"].name(),
            line_width=line_width.value(),
            line_style=line_style.currentText(),
            point_size=point_size.value(),
            symbol=symbol_combo.currentText(),
            marker_text=marker_text.text(),
        )
        self.label_manager.set_style(name, {
            "enabled": label_enabled.isChecked(),
            "format": label_format.toPlainText(),
            "font_size": label_size.value(),
            "color": selected["label"].name(),
            "offset_x": label_offset_x.value(),
            "offset_y": label_offset_y.value(),
            "max_labels": label_max.value(),
        })
        self.label_manager.refresh(self.layers)
        self.statusBar().showMessage(f"Updated style: {name}", 2500)

    def _load_geotiff_dialog(self) -> None:
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "Load GeoTIFF", str(self.project_path), "GeoTIFF (*.tif *.tiff);;All files (*)"
        )
        if not path:
            return
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle("GeoTIFF display")
        form = QtWidgets.QFormLayout(dialog)
        mode = QtWidgets.QComboBox(); mode.addItems(["color map", "contours", "color + contours"]); mode.setCurrentText("color + contours")
        cmap = QtWidgets.QComboBox(); cmap.addItems(["terrain", "viridis", "plasma", "turbo", "gist_earth", "ocean", "gray"])
        opacity = QtWidgets.QDoubleSpinBox(); opacity.setRange(0.0, 1.0); opacity.setSingleStep(0.05); opacity.setValue(0.85)
        interval = QtWidgets.QDoubleSpinBox(); interval.setRange(0.001, 100000.0); interval.setDecimals(3); interval.setValue(5.0)
        contour_color = QtWidgets.QPushButton("#202020"); selected = {"color": QtGui.QColor("#202020")}
        width = QtWidgets.QDoubleSpinBox(); width.setRange(0.2, 10.0); width.setValue(1.0)
        style = QtWidgets.QComboBox(); style.addItems(["solid", "dash", "dot", "dash dot"])
        group = QtWidgets.QComboBox(); group.setEditable(True); group.addItems([self.layer_tree.topLevelItem(i).text(0) for i in range(self.layer_tree.topLevelItemCount())]); group.setCurrentText("Raster / GeoTIFF")
        def choose_color():
            color = QtWidgets.QColorDialog.getColor(selected["color"], dialog)
            if color.isValid(): selected["color"] = color; contour_color.setText(color.name())
        contour_color.clicked.connect(choose_color)
        form.addRow("File:", QtWidgets.QLabel(path)); form.addRow("Display:", mode); form.addRow("Color map:", cmap); form.addRow("Opacity:", opacity)
        form.addRow("Contour interval:", interval); form.addRow("Contour color:", contour_color); form.addRow("Contour width:", width); form.addRow("Contour style:", style); form.addRow("Layer group:", group)
        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.StandardButton.Ok | QtWidgets.QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(dialog.accept); buttons.rejected.connect(dialog.reject); form.addRow(buttons)
        if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
            return
        try:
            options = GeoTiffDisplayOptions(mode=mode.currentText(), cmap=cmap.currentText(), opacity=opacity.value(), contour_interval=interval.value(), contour_color=selected["color"].name(), contour_width=width.value(), contour_style=style.currentText())
            layer = GeoTiffLayer(self.plot_item, path, self.repository.project_epsg(), options)
            registered = self._register_layer(group.currentText().strip() or "Raster / GeoTIFF", layer.name, 1, layer, f"GeoTIFF: {path}")
            self._zoom_to_layer(registered)
            self.statusBar().showMessage(f"Loaded GeoTIFF: {Path(path).name}", 4000)
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "GeoTIFF", str(exc))

    def _create_point_comparison(self) -> None:
        point_names = [name for name, layer in self.layers.items() if isinstance(layer, FastPointLayer) and layer.data is not None]
        if len(point_names) < 2:
            QtWidgets.QMessageBox.information(self, "Point comparison", "At least two loaded point layers are required.")
            return
        dialog = QtWidgets.QDialog(self); dialog.setWindowTitle("Point comparison"); form = QtWidgets.QFormLayout(dialog)
        source = QtWidgets.QComboBox(); source.addItems(point_names)
        target = QtWidgets.QComboBox(); target.addItems(point_names); target.setCurrentIndex(1)
        source_line = QtWidgets.QComboBox(); source_station = QtWidgets.QComboBox(); target_line = QtWidgets.QComboBox(); target_station = QtWidgets.QComboBox()
        def populate():
            for combo, layer_combo in ((source_line, source),(source_station,source),(target_line,target),(target_station,target)):
                combo.clear(); data=self.layers[layer_combo.currentText()].data; combo.addItems(list(data.metadata.keys()))
            def choose(combo, candidates):
                for candidate in candidates:
                    idx=combo.findText(candidate, QtCore.Qt.MatchFlag.MatchFixedString)
                    if idx<0:
                        idx=next((i for i in range(combo.count()) if combo.itemText(i).casefold()==candidate.casefold()),-1)
                    if idx>=0: combo.setCurrentIndex(idx); return
            choose(source_line,("Line","line","DSRLine")); choose(target_line,("Line","line","DSRLine")); choose(source_station,("Station","station","Point")); choose(target_station,("Station","station","Point"))
        source.currentTextChanged.connect(populate); target.currentTextChanged.connect(populate); populate()
        color_button=QtWidgets.QPushButton("#ffd740"); selected={"color":QtGui.QColor("#ffd740")}
        def choose_color():
            c=QtWidgets.QColorDialog.getColor(selected["color"],dialog)
            if c.isValid():selected["color"]=c;color_button.setText(c.name())
        color_button.clicked.connect(choose_color)
        width=QtWidgets.QDoubleSpinBox();width.setRange(0.2,10);width.setValue(1.5)
        style=QtWidgets.QComboBox();style.addItems(["solid","dash","dot","dash dot"]);style.setCurrentText("dash")
        labels=QtWidgets.QCheckBox();labels.setChecked(True)
        group=QtWidgets.QComboBox();group.setEditable(True);group.addItems([self.layer_tree.topLevelItem(i).text(0) for i in range(self.layer_tree.topLevelItemCount())]);group.setCurrentText("Point Comparisons")
        form.addRow("From layer:",source);form.addRow("To layer:",target);form.addRow("From line field:",source_line);form.addRow("From station field:",source_station);form.addRow("To line field:",target_line);form.addRow("To station field:",target_station)
        form.addRow("Line color:",color_button);form.addRow("Line width:",width);form.addRow("Line style:",style);form.addRow("Distance and bearing labels:",labels);form.addRow("Layer group:",group)
        buttons=QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.StandardButton.Ok|QtWidgets.QDialogButtonBox.StandardButton.Cancel);buttons.accepted.connect(dialog.accept);buttons.rejected.connect(dialog.reject);form.addRow(buttons)
        if dialog.exec()!=QtWidgets.QDialog.DialogCode.Accepted:return
        try:
            s_layer=self.layers[source.currentText()];t_layer=self.layers[target.currentText()]
            name=f"{source.currentText()} → {target.currentText()}"
            comparison=PointComparisonLayer(self.plot_item,name,s_layer.data,t_layer.data,(source_line.currentText(),source_station.currentText()),(target_line.currentText(),target_station.currentText()),selected["color"].name(),width.value(),style.currentText(),labels.isChecked())
            if comparison.count==0:
                comparison.remove(); raise RuntimeError("No matching points were found for the selected fields.")
            registered=self._register_layer(group.currentText().strip() or "Point Comparisons",name,comparison.count,comparison)
            self._zoom_to_layer(registered);self.statusBar().showMessage(f"Created {comparison.count:,} comparison vectors",4000)
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self,"Point comparison",str(exc))

    @staticmethod
    def _bbox_layer_color(file_id: int, source: str) -> str:
        """Return a stable high-contrast color for file + coordinate source."""
        palette = (
            "#00E5FF", "#FFD740", "#FF6E40", "#EA80FC",
            "#69F0AE", "#82B1FF", "#FF80AB", "#B2FF59",
            "#FFAB40", "#7C4DFF", "#18FFFF", "#FFFF00",
            "#FF5252", "#40C4FF", "#64FFDA", "#E040FB",
        )
        key = f"{int(file_id)}::{source}".encode("utf-8", errors="ignore")
        digest = hashlib.blake2b(key, digest_size=2).digest()
        return palette[int.from_bytes(digest, "little") % len(palette)]

    def _show_layer_properties(self, name: str) -> None:
        layer = self.layers.get(name)
        if layer is None:
            return
        bounds = layer.bounds
        extent_text = "No valid extent" if not bounds else f"X: {bounds[0]:,.3f} to {bounds[1]:,.3f}\nY: {bounds[2]:,.3f} to {bounds[3]:,.3f}"
        lines = [f"Name: {name}", f"Features/vertices: {layer.count:,}", f"Visible: {layer.visible}", extent_text]
        if isinstance(layer, FastShapeLayer):
            data = layer.data
            lines.extend([f"Geometry: {data.geometry_type}", f"Source: {data.definition.full_name}",
                          f"Source CRS: {data.source_crs}", f"Project CRS: {data.target_crs}",
                          f"CRS status: {data.crs_status}"])
        QtWidgets.QMessageBox.information(self, "Layer properties", "\n".join(lines))

    def _filter_layer_tree(self, text: str) -> None:
        text = text.strip().lower()
        for i in range(self.layer_tree.topLevelItemCount()):
            group = self.layer_tree.topLevelItem(i)
            any_visible = False
            for j in range(group.childCount()):
                child = group.child(j)
                visible = not text or text in child.text(0).lower()
                child.setHidden(not visible)
                any_visible |= visible
            group.setHidden(not any_visible)

    def _set_all_layers_visible(self, visible: bool) -> None:
        state = QtCore.Qt.CheckState.Checked if visible else QtCore.Qt.CheckState.Unchecked
        blocker = QtCore.QSignalBlocker(self.layer_tree)
        try:
            for name, item in self.layer_items.items():
                item.setCheckState(0, state)
                layer = self.layers.get(name)
                if layer is not None:
                    layer.set_visible(visible)
        finally:
            del blocker
        self.label_manager.refresh(self.layers)
        for index in range(self.layer_tree.topLevelItemCount()):
            self._update_group_visibility_icon(self.layer_tree.topLevelItem(index))
        self._save_layer_tree_order()

    def _refresh_layers(self) -> None:
        """Refresh layers that implement a view-dependent refresh hook.

        Not every layer type needs to rebuild itself after pan/zoom. Using an
        optional hook keeps older and third-party layer implementations from
        crashing the whole map refresh cycle.
        """
        for layer in tuple(self.layers.values()):
            refresh = getattr(layer, "refresh_view", None)
            if callable(refresh):
                try:
                    refresh()
                except RuntimeError:
                    # A Qt graphics item may already have been deleted while a
                    # queued range-change signal is still being processed.
                    continue
        self.label_manager.refresh(self.layers)

    def _toggle_map_labels(self, enabled: bool) -> None:
        self.label_manager.enabled = bool(enabled)
        self.viewer_config.set_labels_enabled(enabled)
        self.label_manager.refresh(self.layers)
        for name in self.layer_items:
            self._update_layer_label_icon(name)
        for index in range(self.layer_tree.topLevelItemCount()):
            self._update_group_label_icon(self.layer_tree.topLevelItem(index))
        if enabled:
            self.statusBar().showMessage(
                "Line / point labels enabled. Labels appear automatically at close zoom levels.",
                4000,
            )

    def _zoom_all(self) -> None:
        bounds = [layer.bounds for layer in self.layers.values() if layer.visible and layer.bounds]
        if not bounds:
            return
        xmin = min(b[0] for b in bounds); xmax = max(b[1] for b in bounds)
        ymin = min(b[2] for b in bounds); ymax = max(b[3] for b in bounds)
        self._set_view_extent((xmin, xmax, ymin, ymax))

    def _set_view_extent(self, extent) -> None:
        if not extent:
            return
        xmin, xmax, ymin, ymax = extent
        width = max(xmax - xmin, 1.0); height = max(ymax - ymin, 1.0)
        self.plot_item.setXRange(xmin - width * 0.05, xmax + width * 0.05, padding=0)
        self.plot_item.setYRange(ymin - height * 0.05, ymax + height * 0.05, padding=0)

    def _zoom_to_layer(self, name: str) -> None:
        layer = self.layers.get(name)
        if layer is None or not layer.bounds:
            self.statusBar().showMessage(f"Layer '{name}' has no valid extent", 4000)
            return
        self._set_view_extent(layer.bounds)
        self.statusBar().showMessage(f"Zoomed to {name}", 2500)

    def _set_grid_visible(self, visible: bool) -> None:
        self.plot_item.showGrid(x=visible, y=visible, alpha=0.2)

    def _set_measurement_mode(self, mode: str) -> None:
        self.measurement.set_mode(mode)
        self.ribbon.set_measurement_checked(True, mode)
        self.measure_dock.setVisible(True)
        self.measure_dock.raise_()
        instructions = {
            "distance": "Distance: click two or more points.",
            "area": "Area: click three or more polygon vertices.",
            "bearing": "Bearing: click start and end points.",
            "angle": "Angle: click first arm, vertex, then second arm.",
        }
        self.statusBar().showMessage(instructions.get(mode, "Measurement active") + " Backspace removes the last point; Esc clears.")

    def _toggle_measurement(self, enabled: bool) -> None:
        if enabled:
            self._set_measurement_mode(self.measurement.mode or "distance")
        else:
            self.measurement.disable()
            self.ribbon.set_measurement_checked(False)

    def _selection_point_layer(self, show_message: bool = True) -> tuple[str, FastPointLayer] | None:
        name = self._selected_layer_name()
        layer = self.layers.get(name) if name else None
        if isinstance(layer, FastPointLayer) and layer.data is not None:
            return str(name), layer
        if show_message:
            QtWidgets.QMessageBox.information(
                self,
                "Select nodes",
                "Select a loaded point layer in the Layers panel first.",
            )
        return None

    def _start_node_selection(self, mode: str) -> None:
        target = self._selection_point_layer()
        if target is None:
            self.ribbon.clear_node_selection_mode()
            return
        self.measurement.disable()
        self.ribbon.set_measurement_checked(False)
        self._selection_polygon_points.clear()
        self.selection_guide.setData([], [])
        if mode == "shape":
            self._node_selection_mode = None
            self.ribbon.clear_node_selection_mode()
            self._select_nodes_by_shape_polygon(target[0], target[1])
            return
        self._node_selection_mode = mode
        if mode == "radius":
            self.statusBar().showMessage(
                f"Radius selection: click the map center ({self.ribbon.selection_radius():,.2f} m)."
            )
        else:
            self.statusBar().showMessage(
                "Polygon selection: left-click vertices; right-click or double-click to finish."
            )

    @staticmethod
    def _points_in_polygon(
        x: np.ndarray, y: np.ndarray, parts: list[np.ndarray]
    ) -> np.ndarray:
        """Vectorized odd-even point-in-polygon test supporting multipart shapes."""
        inside = np.zeros(x.shape, dtype=bool)
        for polygon in parts:
            points = np.asarray(polygon, dtype=np.float64)
            if points.ndim != 2 or points.shape[0] < 3:
                continue
            part_inside = np.zeros(x.shape, dtype=bool)
            x1, y1 = points[-1]
            for x2, y2 in points:
                crosses = (y1 > y) != (y2 > y)
                denominator = y2 - y1
                intersection = (x2 - x1) * (y - y1) / (
                    denominator if denominator != 0 else np.finfo(float).eps
                ) + x1
                part_inside ^= crosses & (x < intersection)
                x1, y1 = x2, y2
            inside ^= part_inside
        return inside

    def _apply_node_selection(
        self, name: str, layer: FastPointLayer, mask: np.ndarray, description: str
    ) -> None:
        if layer.data is None:
            return
        finite = np.isfinite(layer.data.x) & np.isfinite(layer.data.y)
        selected_mask = np.asarray(mask, dtype=bool) & finite
        if not self.ribbon.selection_inside():
            selected_mask = (~selected_mask) & finite
        indices = np.flatnonzero(selected_mask)
        layer.set_selected_indices(indices)
        location = "inside" if self.ribbon.selection_inside() else "outside"
        self.details.setPlainText(
            f"Node selection\nLayer: {name}\nMethod: {description}\n"
            f"Location: {location}\nSelected: {indices.size:,} of {layer.data.count:,} nodes"
        )
        self.statusBar().showMessage(
            f"Selected {indices.size:,} node(s) {location} {description} on {name}", 5000
        )

    def _select_nodes_by_radius(
        self, name: str, layer: FastPointLayer, x: float, y: float
    ) -> None:
        if layer.data is None:
            return
        radius = self.ribbon.selection_radius()
        mask = (layer.data.x - x) ** 2 + (layer.data.y - y) ** 2 <= radius ** 2
        self._apply_node_selection(name, layer, mask, f"a {radius:,.2f} m radius")

    def _select_nodes_by_shape_polygon(
        self, name: str, layer: FastPointLayer
    ) -> None:
        polygon_layers = [
            (layer_name, candidate)
            for layer_name, candidate in self.layers.items()
            if isinstance(candidate, FastShapeLayer)
            and candidate.data.geometry_type == "polygon"
            and candidate.data.parts
        ]
        if not polygon_layers:
            QtWidgets.QMessageBox.information(
                self, "Select by shape polygon", "No polygon shape layer is loaded."
            )
            return
        labels = [item[0] for item in polygon_layers]
        selected, accepted = QtWidgets.QInputDialog.getItem(
            self, "Select by shape polygon", "Polygon layer:", labels, 0, False
        )
        if not accepted:
            return
        shape_layer = polygon_layers[labels.index(selected)][1]
        mask = self._points_in_polygon(layer.data.x, layer.data.y, shape_layer.data.parts)
        self._apply_node_selection(name, layer, mask, f"polygon layer '{selected}'")

    def _finish_drawn_polygon_selection(self) -> None:
        target = self._selection_point_layer()
        if target is None or len(self._selection_polygon_points) < 3:
            self.statusBar().showMessage("A selection polygon needs at least three vertices.", 3500)
            return
        polygon = np.asarray(self._selection_polygon_points, dtype=np.float64)
        mask = self._points_in_polygon(target[1].data.x, target[1].data.y, [polygon])
        self.selection_guide.setData(
            x=np.append(polygon[:, 0], polygon[0, 0]),
            y=np.append(polygon[:, 1], polygon[0, 1]),
        )
        self._apply_node_selection(target[0], target[1], mask, "the drawn polygon")
        self._node_selection_mode = None
        self.ribbon.clear_node_selection_mode()

    def _clear_node_selection(self) -> None:
        for layer in self.layers.values():
            if isinstance(layer, FastPointLayer):
                layer.clear_selection()
        self._node_selection_mode = None
        self._selection_polygon_points.clear()
        self.selection_guide.setData([], [])
        self.ribbon.clear_node_selection_mode()
        self.statusBar().showMessage("Node selection cleared", 2500)

    @staticmethod
    def _normalized_column_name(name: object) -> str:
        return "".join(ch for ch in str(name).lower() if ch.isalnum())

    def _blackbox_heading_value(self, data: BlackBoxData, row: int, role: str) -> float | None:
        role = str(role).lower()
        candidates = {
            "vessel": ("vesselhdg", "vesselheading", "shipheading", "heading", "hdg"),
            "rov1": ("rov1hdg", "rov1heading", "rov1insheading", "rov1hdgdeg"),
            "rov2": ("rov2hdg", "rov2heading", "rov2insheading", "rov2hdgdeg"),
        }.get(role, ())
        columns = {self._normalized_column_name(name): values for name, values in data.columns.items()}
        for candidate in candidates:
            values = columns.get(candidate)
            if values is None or row < 0 or row >= len(values):
                continue
            try:
                value = float(values[row])
            except (TypeError, ValueError):
                continue
            if np.isfinite(value):
                return value % 360.0
        return None

    def _update_heading_from_cursor(self, x: float, y: float) -> None:
        if not self.bbox_track_layer_names:
            return
        x_range, _ = self.plot_item.vb.viewRange()
        tolerance = abs(x_range[1] - x_range[0]) * 0.012
        best = None
        bbox_names = set(self.bbox_track_layer_names.values())
        for name in bbox_names:
            layer = self.layers.get(name)
            if not isinstance(layer, FastPointLayer) or not layer.visible or layer.data is None:
                continue
            nearest = layer.nearest(x, y, tolerance)
            if nearest and (best is None or nearest[1] < best[2]):
                best = (name, layer, nearest[0], nearest[1])
        if best is None:
            return
        name, layer, index, _distance = best
        record = layer.data.record(index)
        try:
            file_id = int(record.get("file_id"))
            original_index = int(record.get("original_index", index))
        except (TypeError, ValueError):
            return
        data = self.bbox_data_by_file.get(file_id)
        if data is None:
            return
        vessel = self._blackbox_heading_value(data, original_index, "vessel")
        rov1 = self._blackbox_heading_value(data, original_index, "rov1")
        rov2 = self._blackbox_heading_value(data, original_index, "rov2")
        if vessel is None and rov1 is None and rov2 is None:
            return
        self.heading_panel.set_names(
            vessel_name=data.file_info.vessel_name,
            rov1_name=data.file_info.rov1_name,
            rov2_name=data.file_info.rov2_name,
        )
        self.heading_panel.set_headings(vessel=vessel, rov1=rov1, rov2=rov2)
        source = record.get("source", "")
        timestamp = data.time_labels[original_index] if 0 <= original_index < data.time_labels.size else ""
        self.heading_panel.set_context(
            f"{data.file_info.name} — {source}\nSample: {original_index + 1:,}   Time: {timestamp}"
        )

    def _mouse_moved(self, scene_pos: QtCore.QPointF) -> None:
        if not self.plot_item.sceneBoundingRect().contains(scene_pos):
            return
        point = self.plot_item.vb.mapSceneToView(scene_pos)
        self.coord_label.setText(f"X: {point.x():,.3f}   Y: {point.y():,.3f}")
        self._update_heading_from_cursor(float(point.x()), float(point.y()))

    def _map_clicked(self, event: object) -> None:
        if self._node_selection_mode is not None:
            button = event.button()
            if self._node_selection_mode == "polygon" and button == QtCore.Qt.MouseButton.RightButton:
                self._finish_drawn_polygon_selection()
                return
            if button != QtCore.Qt.MouseButton.LeftButton:
                return
            scene_pos = event.scenePos()
            if not self.plot_item.sceneBoundingRect().contains(scene_pos):
                return
            point = self.plot_item.vb.mapSceneToView(scene_pos)
            x, y = float(point.x()), float(point.y())
            target = self._selection_point_layer()
            if target is None:
                self._node_selection_mode = None
                self.ribbon.clear_node_selection_mode()
                return
            if self._node_selection_mode == "radius":
                self._select_nodes_by_radius(target[0], target[1], x, y)
                return
            self._selection_polygon_points.append((x, y))
            polygon = np.asarray(self._selection_polygon_points, dtype=np.float64)
            self.selection_guide.setData(x=polygon[:, 0], y=polygon[:, 1])
            is_double = getattr(event, "double", None)
            if callable(is_double) and is_double():
                self._finish_drawn_polygon_selection()
            else:
                self.statusBar().showMessage(
                    f"Polygon vertex {len(self._selection_polygon_points)} added; "
                    "right-click or double-click to finish."
                )
            return
        if not self.measurement.enabled or event.button() != QtCore.Qt.MouseButton.LeftButton:
            return
        scene_pos = event.scenePos()
        if not self.plot_item.sceneBoundingRect().contains(scene_pos):
            return
        point = self.plot_item.vb.mapSceneToView(scene_pos)
        x, y = float(point.x()), float(point.y())
        x_range, _ = self.plot_item.vb.viewRange()
        tolerance = abs(x_range[1] - x_range[0]) * 0.008
        best = None
        for layer in self.layers.values():
            # Raster/surface layers do not necessarily provide point snapping.
            # Only query layers that explicitly implement a callable nearest().
            if not getattr(layer, "visible", True):
                continue
            nearest_fn = getattr(layer, "nearest", None)
            if not callable(nearest_fn):
                continue
            try:
                nearest = nearest_fn(x, y, tolerance)
            except (AttributeError, TypeError, ValueError):
                # A non-snappable or partially loaded layer must never break
                # the manual measurement tool.
                continue
            if nearest and (best is None or nearest[1] < best[2]):
                best = (layer, nearest[0], nearest[1])
        if best:
            layer, index, _ = best
            if isinstance(layer, FastPointLayer) and layer.data is not None:
                record = layer.data.record(index)
                label = f"{layer.name}: " + ", ".join(f"{k}={v}" for k, v in record.items() if k not in {"x", "y"} and v not in {None, ""})
                self.measurement.add(record["x"], record["y"], label)
            else:
                sx, sy = layer.vertex(index)
                self.measurement.add(sx, sy, f"{layer.name}: vertex {index + 1}")
        else:
            self.measurement.add(x, y)

    @staticmethod
    def _clean_feature_value(value) -> str:
        if value is None:
            return "—"
        text = str(value).strip()
        return "—" if text.lower() in {"", "none", "nan"} else text

    def _format_feature_record(self, layer_name: str, record: dict) -> str:
        lower = {str(key).lower(): value for key, value in record.items()}
        def value(*names: str):
            for name in names:
                if name.lower() in lower:
                    return self._clean_feature_value(lower[name.lower()])
            return "—"
        if layer_name.startswith("DSR"):
            fields = [
                ("Node", value("node")),
                ("Line", value("line")),
                ("Station", value("station", "linepoint")),
                ("Primary Easting", value("primaryeasting")),
                ("Primary Northing", value("primarynorthing")),
                ("Primary Elevation", value("primaryelevation")),
                ("Secondary Easting", value("secondaryeasting")),
                ("Secondary Northing", value("secondarynorthing")),
                ("Secondary Elevation", value("secondaryelevation")),
                ("Deployed by ROV", value("rov")),
                ("Recovered by ROV", value("rov1")),
                ("Deployment Timestamp", value("timestamp")),
                ("Recovery Timestamp", value("timestamp1")),
                ("Comments", value("comments")),
            ]
            return layer_name + "\n\n" + "\n".join(f"{label}: {val}" for label, val in fields)
        if layer_name == "REC_DB":
            fields = [
                ("Line", value("line")),
                ("Point", value("point", "station", "linepoint")),
                ("REC Easting", value("x")),
                ("REC Northing", value("y")),
                ("REC Elevation", value("rec_z")),
                ("REC ID", value("rec_id")),
            ]
            return layer_name + "\n\n" + "\n".join(f"{label}: {val}" for label, val in fields)
        return layer_name + "\n\n" + "\n".join(
            f"{key}: {self._clean_feature_value(val)}" for key, val in record.items()
        )

    def _show_external_record(self, title: str, data: PointLayerData, index: int) -> None:
        if index < 0 or index >= data.count:
            return
        record = data.record(index)
        self.details.setPlainText(self._format_feature_record(title, record))
        self.details_dock.show(); self.details_dock.raise_()
        if title.startswith("DSR"):
            self._load_ocr_images_for_dsr_record(record)

    def _show_record(self, layer_name: str, index: int) -> None:
        layer = self.layers.get(layer_name)
        if not isinstance(layer, FastPointLayer) or layer.data is None:
            return
        record = layer.data.record(index)
        self.details.setPlainText(self._format_feature_record(layer_name, record))
        self.details_dock.show()
        self.details_dock.raise_()
        if layer_name.startswith("DSR"):
            self._load_ocr_images_for_dsr_record(record)

    def _prepare_radial_circles(self, data: PointLayerData) -> None:
        if self.radial_circle_item is None:
            self.radial_circle_item = RadialCircleItem()
            self.plot_item.addItem(self.radial_circle_item, ignoreBounds=False)
        self.radial_circle_item.set_data(data.x, data.y)
        self.radial_circle_item.set_style(**self._radial_circle_style)
        self.radial_circle_item.setVisible(False)

    def _toggle_radial_circles(self, visible: bool) -> None:
        if self.radial_circle_item is not None:
            self.radial_circle_item.setVisible(bool(visible))
        self.statusBar().showMessage(
            f"Preplot QC circles {'shown' if visible else 'hidden'} — radius {self._radial_circle_style['radius']:.2f} m",
            3000,
        )

    def _set_radial_circle_style(self, radius: float, color: str, width: float, line_style: str) -> None:
        self._radial_circle_style = {
            "radius": max(0.01, float(radius)),
            "color": str(color),
            "width": max(0.2, float(width)),
            "line_style": str(line_style),
        }
        if self.radial_circle_item is not None:
            self.radial_circle_item.set_style(**self._radial_circle_style)


    @staticmethod
    def _numeric_values(values: np.ndarray | None) -> np.ndarray:
        if values is None:
            return np.array([], dtype=np.float64)
        result = np.empty(len(values), dtype=np.float64)
        result.fill(np.nan)
        for index, value in enumerate(values):
            try:
                result[index] = float(value)
            except (TypeError, ValueError):
                pass
        return result

    def _dsr_primary_data(self) -> PointLayerData | None:
        layer = self.layers.get("DSR Primary")
        if isinstance(layer, FastPointLayer):
            return layer.data
        return None

    def _populate_dsr_ribbon(self, data: PointLayerData) -> None:
        lines = self._numeric_values(data.metadata.get("line"))
        valid = lines[np.isfinite(lines)]
        line_list = sorted({int(round(value)) for value in valid})
        self.ribbon.set_dsr_lines(line_list)
        if line_list:
            self._dsr_line_changed(line_list[0])

    def _dsr_line_changed(self, line: int) -> None:
        data = self._dsr_primary_data()
        if data is None:
            return
        lines = self._numeric_values(data.metadata.get("line"))
        stations = self._numeric_values(data.metadata.get("station"))
        rounded_lines = np.zeros(lines.size, dtype=np.int64)
        finite_lines = np.isfinite(lines)
        rounded_lines[finite_lines] = np.rint(lines[finite_lines]).astype(np.int64)
        mask = finite_lines & np.isfinite(stations) & (rounded_lines == int(line))
        station_list = sorted({int(round(value)) for value in stations[mask]})
        self._dsr_station_values = station_list
        self.ribbon.set_dsr_stations(station_list)

        station = self.ribbon.current_dsr_station()
        if station is not None:
            self._dsr_qc_requested_station = (int(line), int(station))

        if self.dsr_qc_window is not None and self.dsr_qc_window.isVisible():
            # select_line requests/reloads the QC data if necessary.  The station
            # is retained by DsrQcWindow and applied once the new line arrives.
            self.dsr_qc_window.select_line(int(line))
            if station is not None:
                self.dsr_qc_window.select_station(int(line), int(station))

    def _ensure_dsr_qc_window(self) -> DsrQcWindow:
        if self.dsr_qc_window is None:
            window = DsrQcWindow(self)
            window.line_requested.connect(self._load_dsr_qc_line)
            window.station_selected.connect(self._select_dsr_station)
            window.zoom_station_requested.connect(self._zoom_dsr_station)
            self.dsr_qc_window = window
        return self.dsr_qc_window

    def _open_surface_2d_window(self) -> None:
        """Create a heatmap/contour layer directly on the main project map."""
        try:
            dialog = SurfaceCreateDialog(self.project_path, self)
            if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
                return
            result = dialog.result_data
            if result is None:
                return
            layer = SurfaceMapLayer(
                self.plot_item,
                result.name,
                result.gx,
                result.gy,
                result.grid_z,
                display=result.display,
                cmap=result.cmap,
                opacity=result.opacity,
                contour_levels=result.contour_levels,
                contour_style=result.contour_style,
                value_field=result.value_field,
            )
            registered = self._register_layer(
                "Surfaces", result.name, layer.count, layer,
                tooltip=f"Main-map surface: {result.value_field}",
            )
            self._save_surface_definition(registered, result.definition)
            self._zoom_to_layer(registered)
            self.statusBar().showMessage(f"Added surface to main map: {registered}", 5000)
        except Exception as exc:
            QtWidgets.QMessageBox.warning(
                self,
                "2D surfaces",
                f"Could not create the surface on the main map:\n{exc}",
            )

    def _add_sps_production_overlay(self) -> None:
        """Add production SPSolution points as one layer per FireCode."""
        try:
            repo = SurfaceDataRepository(self.project_path)
            points = repo.load_sps_production_overlay(max_points=400000)
            fire_codes = points.metadata.get("FireCode")
            if fire_codes is None:
                fire_codes = np.full(points.x.size, "Production", dtype=object)
            palette = ["#00e5ff", "#ffd740", "#ff6e40", "#69f0ae", "#ea80fc", "#7c4dff"]
            added = 0
            for idx, code in enumerate(dict.fromkeys(str(v).strip() or "Production" for v in fire_codes)):
                mask = np.asarray([str(v).strip() == code for v in fire_codes], dtype=bool)
                if not mask.any():
                    continue
                metadata = {k: np.asarray(v)[mask] for k, v in points.metadata.items()}
                data = PointLayerData(
                    name=f"SPS Production — FireCode {code}",
                    x=np.asarray(points.x)[mask], y=np.asarray(points.y)[mask],
                    source_index=np.flatnonzero(mask).astype(np.int64),
                    metadata=metadata,
                )
                layer = FastPointLayer(self.plot_item, data.name, palette[idx % len(palette)], palette[idx % len(palette)], "Line" if "Line" in metadata else None)
                layer.point_size = 4.0
                layer.scatter.setSize(4.0)
                layer.set_data(data)
                layer.selection_changed.connect(self._show_record)
                self._register_layer("Source", data.name, data.count, layer,
                                     tooltip=f"Production SPSolution points; FireCode={code}")
                added += 1
            if added:
                self.label_manager.refresh(self.layers)
                self.statusBar().showMessage(f"Added {added} SPS production point layer(s)", 5000)
            else:
                raise RuntimeError("No production SPS points were available.")
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "SPS overlay", str(exc))

    def _add_slsolution_overlay(self) -> None:
        """Add SLSolution StartX/StartY to EndX/EndY lines, grouped by vessel."""
        try:
            repo = SurfaceDataRepository(self.project_path)
            segments = repo.load_slsolution_segments()
            if not segments:
                raise RuntimeError("No valid SLSolution start/end segments were found.")
            palette = ["#40c4ff", "#ffd740", "#ff5252", "#69f0ae", "#e040fb", "#ffab40", "#7c4dff", "#18ffff"]
            vessels = list(dict.fromkeys(str(row.get("vessel") or "Unknown") for row in segments))
            for vi, vessel in enumerate(vessels):
                rows = [row for row in segments if str(row.get("vessel") or "Unknown") == vessel]
                xs=[]; ys=[]; groups=[]; lines=[]; sail=[]; seq=[]; prod=[]
                for gi, row in enumerate(rows):
                    xs.extend([row["x0"], row["x1"], np.nan])
                    ys.extend([row["y0"], row["y1"], np.nan])
                    groups.extend([gi, gi, gi])
                    lines.extend([row.get("line"), row.get("line"), row.get("line")])
                    sail.extend([row.get("sailline"), row.get("sailline"), row.get("sailline")])
                    seq.extend([row.get("seq"), row.get("seq"), row.get("seq")])
                    prod.extend([row.get("production_count"), row.get("production_count"), row.get("production_count")])
                data = PointLayerData(
                    name=f"SLSolution — {vessel}",
                    x=np.asarray(xs, dtype=float), y=np.asarray(ys, dtype=float),
                    source_index=np.arange(len(xs), dtype=np.int64),
                    metadata={
                        "track_group": np.asarray(groups), "line": np.asarray(lines, dtype=object),
                        "SailLine": np.asarray(sail, dtype=object), "Seq": np.asarray(seq, dtype=object),
                        "ProductionCount": np.asarray(prod, dtype=object),
                        "Vessel": np.full(len(xs), vessel, dtype=object),
                    },
                )
                color = palette[vi % len(palette)]
                layer = FastPointLayer(self.plot_item, data.name, color, color, "track_group")
                layer.point_size = 3.0
                layer.line_width = 2.0
                layer.curve.setPen(pg.mkPen(color, width=2.0))
                layer.scatter.setSize(3.0)
                layer.set_data(data)
                layer.selection_changed.connect(self._show_record)
                self._register_layer("Source", data.name, len(rows), layer,
                                     tooltip=f"SLSolution StartXY–EndXY; vessel={vessel}")
            self.statusBar().showMessage(f"Added SLSolution lines for {len(vessels)} vessel(s)", 5000)
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "SLSolution overlay", str(exc))

    def _surface_config_path(self) -> Path:
        return self.project_path / "config" / "dataviewer2_surfaces_mainmap.json"

    def _save_surface_definition(self, registered_name: str, definition: dict) -> None:
        import json
        path = self._surface_config_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {"surfaces": []}
            if path.exists():
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    payload = loaded
            surfaces = payload.setdefault("surfaces", [])
            definition = dict(definition)
            definition["registered_name"] = registered_name
            surfaces[:] = [item for item in surfaces if item.get("registered_name") != registered_name]
            surfaces.append(definition)
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception as exc:
            self.statusBar().showMessage(f"Surface created but configuration was not saved: {exc}", 7000)

    def _open_bathymetry_3d_window(self) -> None:
        """Open the optional multi-surface PyVista 3D workbench."""
        try:
            if self.bathymetry_3d_window is None:
                self.bathymetry_3d_window = Surface3DWindow(self.project_path, self)
                self.bathymetry_3d_window.setAttribute(
                    QtCore.Qt.WidgetAttribute.WA_DeleteOnClose, True
                )
                self.bathymetry_3d_window.destroyed.connect(
                    lambda *_: setattr(self, "bathymetry_3d_window", None)
                )
            self.bathymetry_3d_window.show()
            self.bathymetry_3d_window.raise_()
            self.bathymetry_3d_window.activateWindow()
        except Exception as exc:
            QtWidgets.QMessageBox.warning(
                self,
                "3D bathymetry",
                f"Could not open the 3D viewer:\n{exc}\n\n"
                "Install optional dependencies with:\n"
                "python -m pip install pyvista pyvistaqt vtk",
            )

    def _open_dsr_qc_window(self) -> None:
        window = self._ensure_dsr_qc_window()
        data = self._dsr_primary_data()
        lines: list[int] = []
        if data is not None:
            line_values = self._numeric_values(data.metadata.get("line"))
            lines = sorted({int(round(value)) for value in line_values[np.isfinite(line_values)]})
        selected = self.ribbon.current_dsr_line()
        selected_station = self.ribbon.current_dsr_station()
        window.set_lines(lines, selected)
        window.show()
        window.raise_()
        window.activateWindow()
        if selected is not None:
            if selected_station is not None:
                self._dsr_qc_requested_station = (int(selected), int(selected_station))
                window.select_station(int(selected), int(selected_station))
            self._load_dsr_qc_line(selected)

    def _load_dsr_qc_line(self, line: int) -> None:
        line = int(line)
        window = self._ensure_dsr_qc_window()
        cached = self.dsr_qc_cache.get(line)
        if cached is not None:
            window.set_data(cached)
            return
        self._dsr_qc_request_line = line
        window.set_loading(line)
        worker = FunctionWorker(lambda selected_line=line: self.repository.load_dsr_qc(selected_line))
        worker.signals.completed.connect(self._dsr_qc_loaded)
        worker.signals.failed.connect(window.set_error)
        self._start_worker(worker)

    def _dsr_qc_loaded(self, data: object) -> None:
        line = int(getattr(data, "line"))
        self.dsr_qc_cache[line] = data
        window = self._ensure_dsr_qc_window()
        if self._dsr_qc_request_line in {None, line}:
            window.set_data(data)
            requested = self._dsr_qc_requested_station
            if requested is not None and requested[0] == line:
                window.select_station(requested[0], requested[1])
            else:
                station = self.ribbon.current_dsr_station()
                if station is not None and self.ribbon.current_dsr_line() == line:
                    window.select_station(line, int(station))

    def _dsr_indices(self, line: int, station: int | None = None) -> np.ndarray:
        data = self._dsr_primary_data()
        if data is None:
            return np.array([], dtype=np.int64)
        lines = self._numeric_values(data.metadata.get("line"))
        finite_lines = np.isfinite(lines)
        rounded_lines = np.zeros(lines.size, dtype=np.int64)
        rounded_lines[finite_lines] = np.rint(lines[finite_lines]).astype(np.int64)
        mask = finite_lines & (rounded_lines == int(line))
        if station is not None:
            stations = self._numeric_values(data.metadata.get("station"))
            finite_stations = np.isfinite(stations)
            rounded_stations = np.zeros(stations.size, dtype=np.int64)
            rounded_stations[finite_stations] = np.rint(stations[finite_stations]).astype(np.int64)
            mask &= finite_stations & (rounded_stations == int(station))
        mask &= np.isfinite(data.x) & np.isfinite(data.y)
        return np.flatnonzero(mask)

    def _zoom_dsr_line(self, line: int) -> None:
        data = self._dsr_primary_data()
        indices = self._dsr_indices(line)
        if data is None or indices.size == 0:
            self.statusBar().showMessage(f"No DSR Primary positions found for line {line}", 4000)
            return
        x = data.x[indices]
        y = data.y[indices]
        self._set_view_extent((float(x.min()), float(x.max()), float(y.min()), float(y.max())))
        self.statusBar().showMessage(f"Zoomed to DSR line {line}: {indices.size:,} station record(s)", 3000)

    def _set_station_marker(self, x: float, y: float) -> None:
        if self.dsr_station_marker is None:
            self.dsr_station_marker = pg.ScatterPlotItem(
                size=18,
                symbol="o",
                pen=pg.mkPen("#ffffff", width=2.5),
                brush=pg.mkBrush("#ff1744"),
                pxMode=True,
            )
            self.dsr_station_marker.setZValue(100000.0)
            self.plot_item.addItem(self.dsr_station_marker)
        self.dsr_station_marker.setData([float(x)], [float(y)])

    def _select_dsr_station(self, line: int, station: int) -> None:
        line = int(line)
        station = int(station)
        self._dsr_qc_requested_station = (line, station)

        # Keep the DSR QC station combo and every white InfiniteLine synchronized
        # with selections made by the ribbon, Previous/Next buttons, layer tree,
        # or map.  select_station blocks its own combo signal, preventing loops.
        if self.dsr_qc_window is not None:
            self.dsr_qc_window.select_station(line, station)

        data = self._dsr_primary_data()
        indices = self._dsr_indices(line, station)
        if data is None or indices.size == 0:
            return
        index = int(indices[0])
        record = data.record(index)
        self._selected_dsr_timestamp = record.get("timestamp")
        if self.bbox_window is not None:
            self.bbox_window.highlight_timestamp(self._selected_dsr_timestamp)
        x = float(data.x[index])
        y = float(data.y[index])
        self._set_station_marker(x, y)
        self._show_record("DSR Primary", index)
        self.statusBar().showMessage(f"Selected DSR line {line}, station {station}", 3000)

    def _zoom_dsr_station(self, line: int, station: int) -> None:
        data = self._dsr_primary_data()
        indices = self._dsr_indices(line, station)
        if data is None or indices.size == 0:
            self.statusBar().showMessage(f"Station {station} was not found on line {line}", 4000)
            return
        index = int(indices[0])
        x = float(data.x[index])
        y = float(data.y[index])
        self._select_dsr_station(line, station)
        radius = 25.0
        self._set_view_extent((x - radius, x + radius, y - radius, y + radius))

    def _add_bbox_tracks_for_selected_dsr_line(self, phase: str = "deployment") -> None:
        phase = str(phase or "deployment").strip().lower()
        if phase not in {"deployment", "recovery", "both"}:
            phase = "deployment"

        line = self.ribbon.current_dsr_line()
        if line is None:
            QtWidgets.QMessageBox.information(
                self,
                "BlackBox line tracks",
                "Select a receiver line first.",
            )
            return

        phase_label = {
            "deployment": "deployment (DSR.TimeStamp)",
            "recovery": "recovery (DSR.TimeStamp1)",
            "both": "deployment and recovery",
        }[phase]
        self.statusBar().showMessage(
            f"Loading {phase_label} BlackBox XY tracks for DSR line {line}…"
        )

        def load_tracks(selected_line: int = int(line), selected_phase: str = phase):
            if selected_phase == "both":
                result: list[PointLayerData] = []
                errors: list[str] = []
                for item_phase in ("deployment", "recovery"):
                    try:
                        result.extend(
                            self.repository.load_blackbox_tracks_for_dsr_line(
                                selected_line,
                                phase=item_phase,
                            )
                        )
                    except Exception as exc:
                        errors.append(str(exc))
                if not result and errors:
                    raise ProjectRepositoryError("\n".join(errors))
                return result
            return self.repository.load_blackbox_tracks_for_dsr_line(
                selected_line,
                phase=selected_phase,
            )

        worker = FunctionWorker(load_tracks)
        worker.signals.completed.connect(
            lambda layers, selected_line=int(line), selected_phase=phase:
                self._dsr_bbox_tracks_loaded(selected_line, selected_phase, layers)
        )
        worker.signals.failed.connect(
            lambda message: QtWidgets.QMessageBox.warning(self, "BlackBox line tracks", message)
        )
        self._start_worker(worker)

    def _dsr_bbox_tracks_loaded(
        self,
        line: int,
        requested_phase: str,
        datasets: list[PointLayerData],
    ) -> None:
        if not datasets:
            timestamp_name = {
                "deployment": "DSR.TimeStamp",
                "recovery": "DSR.TimeStamp1",
                "both": "DSR.TimeStamp or DSR.TimeStamp1",
            }.get(requested_phase, "DSR timestamps")
            QtWidgets.QMessageBox.information(
                self,
                "BlackBox line tracks",
                f"No BlackBox XY records were found for line {line} using {timestamp_name}.",
            )
            return
        added = 0
        phase_counts: dict[str, int] = {}
        for data in datasets:
            source_values = data.metadata.get("source")
            file_values = data.metadata.get("file")
            file_id_values = data.metadata.get("file_id")
            phase_values = data.metadata.get("phase")
            start_values = data.metadata.get("window_start")
            end_values = data.metadata.get("window_end")
            source = str(source_values[0]) if source_values is not None and source_values.size else "XY"
            file_name = str(file_values[0]) if file_values is not None and file_values.size else "BlackBox"
            file_id = int(file_id_values[0]) if file_id_values is not None and file_id_values.size else 0
            phase_name = str(phase_values[0]) if phase_values is not None and phase_values.size else requested_phase.title()
            start_time = str(start_values[0]) if start_values is not None and start_values.size else "—"
            end_time = str(end_values[0]) if end_values is not None and end_values.size else "—"
            phase_salt = 100000 if phase_name.lower().startswith("recovery") else 0
            color = self._bbox_layer_color(file_id + int(line) + phase_salt, f"{phase_name}:{source}")
            layer = FastPointLayer(self.plot_item, data.name, color, color, "track_group")
            layer.update_style(
                point_color=color,
                line_color=color,
                line_width=2.5,
                point_size=6.0,
            )
            layer.max_visible_points = 50000
            layer.show_points_below = 25000
            layer.set_data(data)
            layer.selection_changed.connect(self._show_record)
            registered = self._register_layer(
                "BlackBox by receiver line",
                data.name,
                data.count,
                layer,
                (
                    f"Receiver line: {line}\n"
                    f"Phase: {phase_name}\n"
                    f"Time window: {start_time} — {end_time}\n"
                    f"File: {file_name}\n"
                    f"Coordinate source: {source}"
                ),
            )
            self.bbox_track_layer_names[
                (file_id, f"{phase_name} Line {line}:{source}")
            ] = registered
            phase_counts[phase_name] = phase_counts.get(phase_name, 0) + 1
            added += 1
        summary = ", ".join(
            f"{name}: {count}" for name, count in sorted(phase_counts.items())
        )
        self.statusBar().showMessage(
            f"Added {added} BlackBox XY layer(s) for receiver line {line} ({summary})",
            5000,
        )
        self._zoom_bbox_track()

    def _ensure_bbox_window(self) -> BlackBoxWindow:
        if self.bbox_window is None:
            window = BlackBoxWindow(self)
            window.file_requested.connect(self._load_bbox_file)
            window.add_track_requested.connect(self._add_bbox_track)
            window.add_all_tracks_requested.connect(self._add_all_bbox_tracks)
            window.add_all_files_requested.connect(self._add_all_bbox_files)
            window.zoom_track_requested.connect(self._zoom_bbox_track)
            window.reload_files_requested.connect(self._load_bbox_files)
            self.bbox_window = window
            if self._selected_dsr_timestamp is not None:
                window.highlight_timestamp(self._selected_dsr_timestamp)
        return self.bbox_window

    def _open_bbox_window(self) -> None:
        window = self._ensure_bbox_window()
        window.show()
        window.raise_()
        window.activateWindow()
        self._load_bbox_files()

    def _load_bbox_files(self) -> None:
        window = self._ensure_bbox_window()
        worker = FunctionWorker(self.repository.list_blackbox_files)
        worker.signals.completed.connect(window.set_files)
        worker.signals.failed.connect(window.set_loading_error)
        self._start_worker(worker)

    def _load_bbox_file(self, file_id: int) -> None:
        window = self._ensure_bbox_window()
        worker = FunctionWorker(lambda: self.repository.load_blackbox_file(file_id))
        worker.signals.completed.connect(self._bbox_data_loaded)
        worker.signals.failed.connect(window.set_loading_error)
        self._start_worker(worker)

    def _bbox_data_loaded(self, data: BlackBoxData) -> None:
        self.bbox_data = data
        self.bbox_data_by_file[data.file_info.file_id] = data
        window = self._ensure_bbox_window()
        window.set_data(data)
        if self._selected_dsr_timestamp is not None:
            window.highlight_timestamp(self._selected_dsr_timestamp)
        if self.ribbon.bbox_track_button.isChecked():
            track = data.track()
            if track:
                self._add_bbox_track(data, track[0])

    def _add_all_bbox_tracks(self, data: BlackBoxData) -> None:
        added = 0
        for source in data.tracks:
            before = len(self.bbox_track_layer_names)
            self._add_bbox_track(data, source)
            if len(self.bbox_track_layer_names) > before:
                added += 1
        self.statusBar().showMessage(
            f"Added {added} coordinate layers from {data.file_info.name}", 4500
        )

    def _add_all_bbox_files(self, file_ids: list[int]) -> None:
        for file_id in file_ids:
            cached = self.bbox_data_by_file.get(int(file_id))
            if cached is not None:
                self._add_all_bbox_tracks(cached)
                continue
            worker = FunctionWorker(lambda fid=int(file_id): self.repository.load_blackbox_file(fid))
            worker.signals.completed.connect(self._bbox_all_file_loaded)
            worker.signals.failed.connect(lambda message: self.statusBar().showMessage(message, 6000))
            self._start_worker(worker)

    def _bbox_all_file_loaded(self, data: BlackBoxData) -> None:
        self.bbox_data_by_file[data.file_info.file_id] = data
        self._add_all_bbox_tracks(data)

    def _add_bbox_track(self, data: BlackBoxData, source: str) -> None:
        track = data.tracks.get(source)
        if track is None:
            return
        x, y = track
        finite = np.isfinite(x) & np.isfinite(y)
        x = x[finite]
        y = y[finite]
        if not x.size:
            self.statusBar().showMessage(f"BlackBox track {source} has no valid coordinates", 4000)
            return

        key = (int(data.file_info.file_id), str(source))
        existing_name = self.bbox_track_layer_names.get(key)
        if existing_name and existing_name in self.layers:
            self.layer_items[existing_name].setCheckState(0, QtCore.Qt.CheckState.Checked)
            self.bbox_last_track_layer_name = existing_name
            self.statusBar().showMessage(f"BlackBox layer already exists: {existing_name}", 3000)
            return

        source_index = np.arange(x.size, dtype=np.int64)
        metadata = {
            "source": np.asarray([source] * x.size, dtype=object),
            "file": np.asarray([data.file_info.name] * x.size, dtype=object),
            "file_id": np.full(x.size, data.file_info.file_id, dtype=np.int64),
            "track_group": np.zeros(x.size, dtype=np.int8),
            "original_index": np.asarray(
                data.track_indices.get(source, np.arange(x.size, dtype=np.int64)),
                dtype=np.int64,
            )[:x.size],
        }
        point_data = PointLayerData(f"BlackBox {source}", x, y, source_index, metadata)
        color = self._bbox_layer_color(data.file_info.file_id, source)
        layer = FastPointLayer(
            self.plot_item,
            point_data.name,
            color,
            color,
            "track_group",
        )
        layer.update_style(
            point_color=color,
            line_color=color,
            line_width=2.5,
            point_size=7.0,
        )
        layer.curve.setZValue(100)
        layer.scatter.setZValue(100.1)
        layer.scatter.setSize(7)
        layer.max_visible_points = 50000
        layer.show_points_below = 25000
        layer.set_data(point_data)
        layer.selection_changed.connect(self._show_record)

        safe_file = data.file_info.name or f"File {data.file_info.file_id}"
        name = f"BBox {safe_file} — {source}"
        # Guarantee uniqueness even when file names repeat.
        if name in self.layers:
            name = f"{name} [{data.file_info.file_id}]"
        self._register_layer(
            "BlackBox", name, point_data.count, layer,
            f"{data.file_info.name}\nFile ID: {data.file_info.file_id}\nCoordinate source: {source}",
        )
        self.bbox_track_layer_names[key] = name
        self.bbox_last_track_layer_name = name
        self.ribbon.bbox_track_button.setChecked(True)
        self.statusBar().showMessage(
            f"Added {source} from {data.file_info.name} ({point_data.count:,} points)", 3500
        )

    def _toggle_bbox_track(self, enabled: bool) -> None:
        bbox_names = [name for name in self.bbox_track_layer_names.values() if name in self.layer_items]
        if enabled:
            if not bbox_names:
                self._open_bbox_window()
                return
            for name in bbox_names:
                self.layer_items[name].setCheckState(0, QtCore.Qt.CheckState.Checked)
        else:
            for name in bbox_names:
                self.layer_items[name].setCheckState(0, QtCore.Qt.CheckState.Unchecked)

    def _zoom_bbox_track(self) -> None:
        names = [name for name in self.bbox_track_layer_names.values() if name in self.layers]
        if not names:
            self._open_bbox_window()
            return
        bounds = [self.layers[name].bounds for name in names if self.layers[name].bounds]
        if not bounds:
            return
        xmin = min(b[0] for b in bounds)
        xmax = max(b[1] for b in bounds)
        ymin = min(b[2] for b in bounds)
        ymax = max(b[3] for b in bounds)
        width = max(xmax - xmin, 1.0)
        height = max(ymax - ymin, 1.0)
        self.plot_item.setXRange(xmin - width * 0.05, xmax + width * 0.05, padding=0)
        self.plot_item.setYRange(ymin - height * 0.05, ymax + height * 0.05, padding=0)

    def _reload_layers(self) -> None:
        if self._pending:
            return
        self._loading_initial_layers = True
        for layer in self.layers.values():
            layer.remove()
        for overlay in self._dsr_line_overlays.values():
            overlay.remove()
        self._dsr_line_overlays.clear()
        self.layers.clear()
        self.layer_items.clear()
        self._reloading_layers.clear()
        self._custom_definition_by_layer.clear()
        self.bbox_track_layer_names.clear()
        self.bbox_last_track_layer_name = None
        self.dsr_qc_cache.clear()
        if self.dsr_station_marker is not None:
            self.plot_item.removeItem(self.dsr_station_marker)
            self.dsr_station_marker = None
        self.layer_tree.clear()
        self.label_manager.clear()
        self.measurement.clear()
        self.details.clear()
        self.ocr_images_panel.clear_records()
        self.ocr_images_dock.hide()
        self._start_loading()

    def keyPressEvent(self, event: QtGui.QKeyEvent) -> None:
        if event.key() == QtCore.Qt.Key.Key_Left:
            self._step_dsr_station(-1)
            return
        if event.key() == QtCore.Qt.Key.Key_Right:
            self._step_dsr_station(1)
            return
        if event.key() == QtCore.Qt.Key.Key_Backspace:
            self.measurement.remove_last()
            return
        if event.key() == QtCore.Qt.Key.Key_Escape:
            self.measurement.clear()
            return
        super().keyPressEvent(event)

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        settings = QtCore.QSettings()
        settings.setValue("main_geometry", self.saveGeometry())
        settings.setValue("main_state", self.saveState())
        super().closeEvent(event)

    def _restore_state(self) -> None:
        settings = QtCore.QSettings()
        geometry = settings.value("main_geometry")
        state = settings.value("main_state")
        if geometry:
            self.restoreGeometry(geometry)
        if state:
            self.restoreState(state)
