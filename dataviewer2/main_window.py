from __future__ import annotations

from collections import OrderedDict
from pathlib import Path

import pyqtgraph as pg
import numpy as np
from PySide6 import QtCore, QtGui, QtWidgets

from .layers import FastPointLayer, FastShapeLayer
from .measurement import MeasurementTool
from .models import PointLayerData, ShapeLayerData, BlackBoxData
from .repository import ProjectRepository
from .ribbon import RibbonBar
from .shapes import load_shapefile
from .workers import FunctionWorker
from .bbox import BlackBoxWindow


class MainWindow(QtWidgets.QMainWindow):
    """Main DataViewer 2.0 shell with ribbon, docks and fast map canvas."""

    def __init__(self, project_path: str | Path) -> None:
        super().__init__()
        self.project_path = Path(project_path)
        self.repository = ProjectRepository(project_path)
        self.thread_pool = QtCore.QThreadPool.globalInstance()
        self.thread_pool.setMaxThreadCount(2)
        self._workers: set[FunctionWorker] = set()
        self.layers: OrderedDict[str, FastPointLayer | FastShapeLayer] = OrderedDict()
        self.layer_items: dict[str, QtWidgets.QTreeWidgetItem] = {}
        self._pending = 0
        self.bbox_window: BlackBoxWindow | None = None
        self.bbox_data: BlackBoxData | None = None
        self.bbox_data_by_file: dict[int, BlackBoxData] = {}
        self.bbox_track_layer_names: dict[tuple[int, str], str] = {}
        self.bbox_last_track_layer_name: str | None = None
        self._build_ui()
        self._restore_state()
        self._start_loading()

    def _build_ui(self) -> None:
        self.setWindowTitle(f"SeisWebLog DataViewer 2.0 — {self.project_path.name}")
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
        self.setCentralWidget(self.plot_widget)

        self._build_layers_dock()
        self._build_details_dock()
        self._build_measurement_dock()

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
        self.ribbon.clear_measurement_requested.connect(self.measurement.clear)
        self.ribbon.remove_last_measurement_requested.connect(self.measurement.remove_last)
        self.ribbon.grid_toggled.connect(self._set_grid_visible)
        self.ribbon.side_panel_toggled.connect(self.layers_dock.setVisible)
        self.ribbon.bbox_open_requested.connect(self._open_bbox_window)
        self.ribbon.bbox_reload_requested.connect(self._load_bbox_files)
        self.ribbon.bbox_track_toggle_requested.connect(self._toggle_bbox_track)
        self.ribbon.bbox_zoom_requested.connect(self._zoom_bbox_track)

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
        self.layer_tree = QtWidgets.QTreeWidget()
        self.layer_tree.setHeaderLabels(["Layer", "Features"])
        self.layer_tree.setRootIsDecorated(True)
        self.layer_tree.itemChanged.connect(self._tree_item_changed)
        self.layer_tree.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.layer_tree.customContextMenuRequested.connect(self._show_layer_context_menu)
        self.layer_tree.itemDoubleClicked.connect(self._layer_double_clicked)
        layout.addWidget(self.layer_tree)
        self.layers_dock.setWidget(container)
        self.addDockWidget(QtCore.Qt.DockWidgetArea.LeftDockWidgetArea, self.layers_dock)

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
        self.measure_text.setPlaceholderText("Use Measure > Distance, then click map points.")
        layout.addWidget(self.measure_text)
        self.measure_dock.setWidget(body)
        self.addDockWidget(QtCore.Qt.DockWidgetArea.RightDockWidgetArea, self.measure_dock)
        self.tabifyDockWidget(self.details_dock, self.measure_dock)
        self.details_dock.raise_()

    def _start_worker(self, worker: FunctionWorker) -> None:
        """Keep Python worker wrappers alive until their signals finish."""
        self._workers.add(worker)

        def cleanup(*_args) -> None:
            self._workers.discard(worker)

        worker.signals.completed.connect(cleanup)
        worker.signals.failed.connect(cleanup)
        self.thread_pool.start(worker)

    def _start_loading(self) -> None:
        jobs = [
            ("Preplot", "RPPreplot", self.repository.load_rp_preplot, "#27c2ff", "#27c2ff", "line"),
            ("Receiver QC", "DSR Preplot", lambda: self.repository.load_dsr_layer("preplot"), "#a7a7a7", None, None),
            ("Receiver QC", "DSR Primary", lambda: self.repository.load_dsr_layer("primary"), "#41d26f", None, None),
            ("Receiver QC", "DSR Secondary", lambda: self.repository.load_dsr_layer("secondary"), "#44a7ff", None, None),
            ("Receiver QC", "DSR Recovery Primary", lambda: self.repository.load_dsr_layer("recovery_primary"), "#ff9d3d", None, None),
            ("Receiver QC", "REC_DB", self.repository.load_rec_db, "#f05cff", None, None),
        ]
        try:
            shape_definitions = self.repository.load_shape_definitions()
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
                    data = load_shapefile(definition, project_epsg)
                    self._register_shape_data(data)
                except Exception:
                    import traceback
                    self.details.appendPlainText(traceback.format_exc() + "\n")
                QtWidgets.QApplication.processEvents()

        self._pending = len(jobs)
        self.statusBar().showMessage(f"Loading {self._pending} database layer(s)…")
        for group, name, function, point_color, line_color, connect_by in jobs:
            worker = FunctionWorker(function)
            worker.signals.completed.connect(
                lambda data, g=group, n=name, pc=point_color, lc=line_color, cb=connect_by:
                self._point_layer_loaded(g, n, data, pc, lc, cb)
            )
            worker.signals.failed.connect(self._load_failed)
            self._start_worker(worker)

        if self._pending == 0:
            self._zoom_all()
            self.statusBar().showMessage(f"Ready — {len(self.layers)} layer(s)")

    def _group_item(self, name: str) -> QtWidgets.QTreeWidgetItem:
        for index in range(self.layer_tree.topLevelItemCount()):
            item = self.layer_tree.topLevelItem(index)
            if item.text(0) == name:
                return item
        item = QtWidgets.QTreeWidgetItem([name, ""])
        item.setFlags(item.flags() & ~QtCore.Qt.ItemFlag.ItemIsUserCheckable)
        item.setExpanded(True)
        self.layer_tree.addTopLevelItem(item)
        return item

    def _register_layer(self, group: str, name: str, count: int, layer: FastPointLayer | FastShapeLayer, tooltip: str = "") -> None:
        unique = name
        suffix = 2
        while unique in self.layers:
            unique = f"{name} ({suffix})"
            suffix += 1
        layer.name = unique
        self.layers[unique] = layer
        parent = self._group_item(group)
        item = QtWidgets.QTreeWidgetItem([unique, f"{count:,}"])
        item.setFlags(item.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
        item.setCheckState(0, QtCore.Qt.CheckState.Checked)
        item.setToolTip(0, tooltip)
        item.setData(0, QtCore.Qt.ItemDataRole.UserRole, unique)
        parent.addChild(item)
        self.layer_items[unique] = item
        self._update_layer_z_values()

    def _point_layer_loaded(self, group: str, name: str, data: PointLayerData, point_color: str, line_color: str | None, connect_by: str | None) -> None:
        layer = FastPointLayer(self.plot_item, name, point_color, line_color, connect_by)
        layer.set_data(data)
        layer.selection_changed.connect(self._show_record)
        self._register_layer(group, name, data.count, layer)
        self._finish_load()

    def _register_shape_data(self, data: ShapeLayerData) -> None:
        layer = FastShapeLayer(self.plot_item, data)
        tooltip = (f"{data.definition.full_name}\nSource CRS: {data.source_crs}\n"
                   f"Project CRS: {data.target_crs}\nStatus: {data.crs_status}")
        self._register_layer("Project shapes", data.name, data.count, layer, tooltip)

    def _shape_layer_loaded(self, data: ShapeLayerData) -> None:
        # Retained for compatibility with any caller, but normal startup now uses
        # _register_shape_data synchronously to keep pyproj out of worker threads.
        self._register_shape_data(data)
        self._finish_load()

    def _finish_load(self) -> None:
        self._pending = max(0, self._pending - 1)
        if self._pending == 0:
            self._zoom_all()
            self.statusBar().showMessage(f"Ready — {len(self.layers)} layer(s)")

    def _load_failed(self, error: str) -> None:
        self.details.appendPlainText(error + "\n")
        self._finish_load()

    def _tree_item_changed(self, item: QtWidgets.QTreeWidgetItem, column: int) -> None:
        if column != 0:
            return
        name = item.data(0, QtCore.Qt.ItemDataRole.UserRole)
        if name in self.layers:
            self.layers[name].set_visible(item.checkState(0) == QtCore.Qt.CheckState.Checked)

    def _layer_double_clicked(self, item: QtWidgets.QTreeWidgetItem, _column: int) -> None:
        name = item.data(0, QtCore.Qt.ItemDataRole.UserRole)
        if name in self.layers:
            self._zoom_to_layer(name)

    def _show_layer_context_menu(self, position: QtCore.QPoint) -> None:
        item = self.layer_tree.itemAt(position)
        if item is None:
            return
        name = item.data(0, QtCore.Qt.ItemDataRole.UserRole)
        if name not in self.layers:
            return
        layer = self.layers[name]
        menu = QtWidgets.QMenu(self.layer_tree)
        zoom_action = menu.addAction("Zoom to layer")
        zoom_action.triggered.connect(lambda: self._zoom_to_layer(name))
        visibility_action = menu.addAction("Hide layer" if layer.visible else "Show layer")
        visibility_action.triggered.connect(lambda: item.setCheckState(0, QtCore.Qt.CheckState.Unchecked if layer.visible else QtCore.Qt.CheckState.Checked))
        only_action = menu.addAction("Show only this layer")
        only_action.triggered.connect(lambda: self._show_only_layer(name))
        menu.addSeparator()
        top_action = menu.addAction("Move to top")
        up_action = menu.addAction("Move up")
        down_action = menu.addAction("Move down")
        bottom_action = menu.addAction("Move to bottom")
        top_action.triggered.connect(lambda: self._move_layer(name, "top"))
        up_action.triggered.connect(lambda: self._move_layer(name, "up"))
        down_action.triggered.connect(lambda: self._move_layer(name, "down"))
        bottom_action.triggered.connect(lambda: self._move_layer(name, "bottom"))
        menu.addSeparator()
        properties_action = menu.addAction("Properties")
        properties_action.triggered.connect(lambda: self._show_layer_properties(name))
        reload_action = menu.addAction("Reload all layers")
        reload_action.triggered.connect(self._reload_layers)
        copy_action = menu.addAction("Copy extent")
        copy_action.triggered.connect(lambda: self._copy_layer_extent(name))
        menu.addSeparator()
        delete_action = menu.addAction("Delete layer")
        delete_action.triggered.connect(lambda: self._delete_layer(name))
        if isinstance(layer, FastShapeLayer):
            menu.addSeparator()
            folder_action = menu.addAction("Open source folder")
            folder_action.triggered.connect(lambda: QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(layer.data.definition.full_name.parent))))
        menu.exec(self.layer_tree.viewport().mapToGlobal(position))

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

        try:
            layer.remove()
        except Exception:
            pass
        self.layers.pop(name, None)
        item = self.layer_items.pop(name, None)
        if item is not None:
            parent = item.parent()
            if parent is not None:
                parent.takeChild(parent.indexOfChild(item))
                if parent.childCount() == 0:
                    index = self.layer_tree.indexOfTopLevelItem(parent)
                    if index >= 0:
                        self.layer_tree.takeTopLevelItem(index)

        # Remove any BlackBox registry entries pointing to this layer.
        for key, registered_name in list(self.bbox_track_layer_names.items()):
            if registered_name == name:
                self.bbox_track_layer_names.pop(key, None)
        if self.bbox_last_track_layer_name == name:
            self.bbox_last_track_layer_name = next(reversed(self.bbox_track_layer_names.values()), None) if self.bbox_track_layer_names else None
        if not self.bbox_track_layer_names:
            self.ribbon.bbox_track_button.setChecked(False)

        self._update_layer_z_values()
        self.statusBar().showMessage(f"Removed layer: {name}", 3000)

    def _show_only_layer(self, name: str) -> None:
        for layer_name, item in self.layer_items.items():
            item.setCheckState(0, QtCore.Qt.CheckState.Checked if layer_name == name else QtCore.Qt.CheckState.Unchecked)

    def _move_layer(self, name: str, direction: str) -> None:
        keys = list(self.layers.keys())
        if name not in keys:
            return
        index = keys.index(name)
        if direction == "top": new_index = 0
        elif direction == "bottom": new_index = len(keys) - 1
        elif direction == "up": new_index = max(0, index - 1)
        else: new_index = min(len(keys) - 1, index + 1)
        if new_index == index:
            return
        keys.pop(index); keys.insert(new_index, name)
        self.layers = OrderedDict((key, self.layers[key]) for key in keys)
        item = self.layer_items[name]
        parent = item.parent()
        if parent is not None:
            old = parent.indexOfChild(item); parent.takeChild(old)
            group_names = [key for key in keys if self.layer_items[key].parent() is parent]
            parent.insertChild(group_names.index(name), item)
        self._update_layer_z_values()

    def _update_layer_z_values(self) -> None:
        total = len(self.layers)
        for index, layer in enumerate(self.layers.values()):
            layer.set_z_value(float(total - index))

    def _copy_layer_extent(self, name: str) -> None:
        layer = self.layers.get(name)
        if not layer or not layer.bounds:
            return
        xmin, xmax, ymin, ymax = layer.bounds
        QtWidgets.QApplication.clipboard().setText(f"{xmin:.3f}, {xmax:.3f}, {ymin:.3f}, {ymax:.3f}")
        self.statusBar().showMessage("Layer extent copied", 2500)

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
        for item in self.layer_items.values():
            item.setCheckState(0, state)

    def _refresh_layers(self) -> None:
        for layer in self.layers.values():
            layer.refresh_view()

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

    def _toggle_measurement(self, enabled: bool) -> None:
        self.measurement.enabled = enabled
        self.ribbon.set_measurement_checked(enabled)
        self.measure_dock.setVisible(enabled or self.measure_dock.isVisible())
        if enabled:
            self.measure_dock.raise_()
            self.statusBar().showMessage("Distance measurement active: click points; Backspace removes last; Esc clears")

    def _mouse_moved(self, scene_pos: QtCore.QPointF) -> None:
        if not self.plot_item.sceneBoundingRect().contains(scene_pos):
            return
        point = self.plot_item.vb.mapSceneToView(scene_pos)
        self.coord_label.setText(f"X: {point.x():,.3f}   Y: {point.y():,.3f}")

    def _map_clicked(self, event: object) -> None:
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
            nearest = layer.nearest(x, y, tolerance)
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

    def _show_record(self, layer_name: str, index: int) -> None:
        layer = self.layers.get(layer_name)
        if not isinstance(layer, FastPointLayer) or layer.data is None:
            return
        record = layer.data.record(index)
        self.details.setPlainText(layer_name + "\n\n" + "\n".join(f"{key}: {value}" for key, value in record.items()))
        self.details_dock.show()
        self.details_dock.raise_()

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
        }
        point_data = PointLayerData(f"BlackBox {source}", x, y, source_index, metadata)
        palette = {
            "GNSS1": "#00ffff", "GNSS2": "#ffff00", "Vessel": "#ff8c00",
            "INS": "#ff4dff", "USBL": "#00ff66", "ROV1": "#66a3ff",
            "ROV2": "#ff6666",
        }
        color = palette.get(source, "#ffffff")
        layer = FastPointLayer(self.plot_item, point_data.name, color, color, "track_group")
        layer.curve.setPen(pg.mkPen(color, width=2.5))
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
        for layer in self.layers.values():
            layer.remove()
        self.layers.clear()
        self.layer_items.clear()
        self.bbox_track_layer_names.clear()
        self.bbox_last_track_layer_name = None
        self.layer_tree.clear()
        self.measurement.clear()
        self.details.clear()
        self._start_loading()

    def keyPressEvent(self, event: QtGui.QKeyEvent) -> None:
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
