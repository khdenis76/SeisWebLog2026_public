from __future__ import annotations

from collections import OrderedDict
from pathlib import Path

import pyqtgraph as pg
from PySide6 import QtCore, QtGui, QtWidgets

from .layers import FastPointLayer, FastShapeLayer
from .measurement import MeasurementTool
from .models import PointLayerData, ShapeLayerData
from .repository import ProjectRepository
from .ribbon import RibbonBar
from .workers import FunctionWorker
from .shapes import load_shapefile


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, project_path: str | Path) -> None:
        super().__init__()
        self.setWindowTitle("SeisWebLog Fast DataViewer")
        self.resize(1500, 900)
        self.repository = ProjectRepository(project_path)
        self.thread_pool = QtCore.QThreadPool.globalInstance()
        self.layers: OrderedDict[str, FastPointLayer | FastShapeLayer] = OrderedDict()
        self.layer_checkboxes: OrderedDict[str, QtWidgets.QCheckBox] = OrderedDict()
        self._build_ui()
        self._start_loading()

    def _build_ui(self) -> None:
        self.ribbon = RibbonBar(self)
        self.setMenuWidget(self.ribbon)

        central = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout(central)
        splitter = QtWidgets.QSplitter()
        layout.addWidget(splitter)
        self.setCentralWidget(central)

        self.left_panel = QtWidgets.QWidget()
        left = self.left_panel
        left_layout = QtWidgets.QVBoxLayout(left)
        self.layer_box = QtWidgets.QGroupBox("Layers")
        self.layer_layout = QtWidgets.QVBoxLayout(self.layer_box)
        left_layout.addWidget(self.layer_box)

        measure_group = QtWidgets.QGroupBox("Measurement")
        measure_layout = QtWidgets.QVBoxLayout(measure_group)
        self.measure_button = QtWidgets.QPushButton("Start measurement")
        self.measure_button.setCheckable(True)
        self.clear_button = QtWidgets.QPushButton("Clear")
        self.measure_text = QtWidgets.QPlainTextEdit()
        self.measure_text.setReadOnly(True)
        self.measure_text.setMaximumHeight(180)
        measure_layout.addWidget(self.measure_button)
        measure_layout.addWidget(self.clear_button)
        measure_layout.addWidget(self.measure_text)
        left_layout.addWidget(measure_group)

        self.details = QtWidgets.QPlainTextEdit()
        self.details.setReadOnly(True)
        left_layout.addWidget(QtWidgets.QLabel("Selected point"))
        left_layout.addWidget(self.details, 1)
        splitter.addWidget(left)

        self.plot_widget = pg.PlotWidget()
        self.plot_item = self.plot_widget.getPlotItem()
        self.plot_item.setAspectLocked(True)
        self.plot_item.showGrid(x=True, y=True, alpha=0.2)
        self.plot_item.setLabel("bottom", "Easting", units="m")
        self.plot_item.setLabel("left", "Northing", units="m")
        splitter.addWidget(self.plot_widget)
        splitter.setSizes([300, 1200])

        self.measurement = MeasurementTool(self.plot_item)
        self.measurement.changed.connect(self.measure_text.setPlainText)

        self.ribbon.zoom_all_requested.connect(self._zoom_all)
        self.ribbon.refresh_requested.connect(self._reload_layers)
        self.ribbon.select_all_layers_requested.connect(lambda: self._set_all_layers_visible(True))
        self.ribbon.clear_all_layers_requested.connect(lambda: self._set_all_layers_visible(False))
        self.ribbon.measurement_toggled.connect(self.measure_button.setChecked)
        self.ribbon.clear_measurement_requested.connect(self.measurement.clear)
        self.ribbon.remove_last_measurement_requested.connect(self.measurement.remove_last)
        self.ribbon.grid_toggled.connect(self._set_grid_visible)
        self.ribbon.side_panel_toggled.connect(self.left_panel.setVisible)
        self.measure_button.toggled.connect(self._toggle_measurement)
        self.clear_button.clicked.connect(self.measurement.clear)

        self.viewport_timer = QtCore.QTimer(self)
        self.viewport_timer.setSingleShot(True)
        self.viewport_timer.setInterval(120)
        self.viewport_timer.timeout.connect(self._refresh_layers)
        self.plot_item.vb.sigRangeChanged.connect(lambda *_: self.viewport_timer.start())
        self.plot_widget.scene().sigMouseClicked.connect(self._map_clicked)
        self.statusBar().showMessage(f"EPSG: {self.repository.project_epsg() or 'unknown'}")

    def _start_loading(self) -> None:
        jobs = [
            ("RPPreplot", self.repository.load_rp_preplot, "cyan", "cyan", "line"),
            ("DSR Primary", lambda: self.repository.load_dsr_layer("primary"), "green", None, None),
            ("DSR Recovery", lambda: self.repository.load_dsr_layer("recovery_primary"), "orange", None, None),
            ("REC_DB", self.repository.load_rec_db, "magenta", None, None),
        ]

        try:
            shape_definitions = self.repository.load_shape_definitions()
        except Exception as exc:
            shape_definitions = []
            self.details.appendPlainText(f"Shape database read failed: {exc}")

        self._pending = len(jobs) + len(shape_definitions)
        self.statusBar().showMessage(
            f"Loading project layers and {len(shape_definitions)} database shape(s)…"
        )

        for name, function, point_color, line_color, connect_by in jobs:
            worker = FunctionWorker(function)
            worker.signals.completed.connect(
                lambda data, n=name, pc=point_color, lc=line_color, cb=connect_by:
                self._layer_loaded(n, data, pc, lc, cb)
            )
            worker.signals.failed.connect(self._load_failed)
            self.thread_pool.start(worker)

        for definition in shape_definitions:
            worker = FunctionWorker(lambda d=definition: load_shapefile(d))
            worker.signals.completed.connect(self._shape_loaded)
            worker.signals.failed.connect(self._load_failed)
            self.thread_pool.start(worker)

        if self._pending == 0:
            self.statusBar().showMessage("Ready — no map layers found")

    def _layer_loaded(self, name: str, data: PointLayerData, point_color: str, line_color: str | None, connect_by: str | None) -> None:
        layer = FastPointLayer(self.plot_item, name, point_color, line_color, connect_by)
        layer.set_data(data)
        layer.selection_changed.connect(self._show_record)
        self.layers[name] = layer
        checkbox = QtWidgets.QCheckBox(f"{name} ({data.count:,})")
        checkbox.setChecked(True)
        checkbox.toggled.connect(layer.set_visible)
        self.layer_checkboxes[name] = checkbox
        self.layer_layout.addWidget(checkbox)
        self._finish_one_load()

    def _shape_loaded(self, data: ShapeLayerData) -> None:
        base_name = f"Shape: {data.name}"
        name = base_name
        suffix = 2
        while name in self.layers:
            name = f"{base_name} ({suffix})"
            suffix += 1

        layer = FastShapeLayer(self.plot_item, data)
        layer.name = name
        self.layers[name] = layer
        checkbox = QtWidgets.QCheckBox(
            f"{name} [{data.geometry_type}] ({data.count:,} vertices)"
        )
        checkbox.setChecked(True)
        checkbox.setToolTip(str(data.definition.full_name))
        checkbox.toggled.connect(layer.set_visible)
        self.layer_checkboxes[name] = checkbox
        self.layer_layout.addWidget(checkbox)
        self._finish_one_load()

    def _finish_one_load(self) -> None:
        self._pending = max(0, self._pending - 1)
        if self._pending == 0:
            self.plot_item.enableAutoRange()
            self.plot_item.autoRange()
            self.statusBar().showMessage("Ready")

    def _load_failed(self, error: str) -> None:
        self.details.appendPlainText(error)
        self._finish_one_load()
        if self._pending:
            self.statusBar().showMessage("One or more layers failed; continuing…")

    def _refresh_layers(self) -> None:
        for layer in self.layers.values():
            layer.refresh_view()

    def _toggle_measurement(self, enabled: bool) -> None:
        self.measurement.enabled = enabled
        self.measure_button.setText("Stop measurement" if enabled else "Start measurement")
        self.ribbon.set_measurement_checked(enabled)

    def _zoom_all(self) -> None:
        self.plot_item.enableAutoRange()
        self.plot_item.autoRange()

    def _set_all_layers_visible(self, visible: bool) -> None:
        for checkbox in self.layer_checkboxes.values():
            checkbox.setChecked(visible)

    def _set_grid_visible(self, visible: bool) -> None:
        self.plot_item.showGrid(x=visible, y=visible, alpha=0.2)

    def _reload_layers(self) -> None:
        if self._pending:
            return
        for layer in self.layers.values():
            layer.remove()
        self.layers.clear()
        self.layer_checkboxes.clear()
        while self.layer_layout.count():
            item = self.layer_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self.details.clear()
        self.measurement.clear()
        self._start_loading()

    def _map_clicked(self, event: object) -> None:
        if not self.measurement.enabled or event.button() != QtCore.Qt.MouseButton.LeftButton:
            return
        scene_pos = event.scenePos()
        if not self.plot_item.sceneBoundingRect().contains(scene_pos):
            return
        point = self.plot_item.vb.mapSceneToView(scene_pos)
        x, y = float(point.x()), float(point.y())
        x_range, _ = self.plot_item.vb.viewRange()
        tolerance = abs(x_range[1] - x_range[0]) * 0.01
        best: tuple[FastPointLayer | FastShapeLayer, int, float] | None = None
        for layer in self.layers.values():
            nearest = layer.nearest(x, y, tolerance)
            if nearest and (best is None or nearest[1] < best[2]):
                best = (layer, nearest[0], nearest[1])
        if best:
            layer, index, _distance = best
            if isinstance(layer, FastPointLayer) and layer.data is not None:
                record = layer.data.record(index)
                label = f"{layer.name}: " + ", ".join(
                    f"{key}={value}" for key, value in record.items()
                    if key not in {"x", "y"} and value not in {None, ""}
                )
                self.measurement.add(record["x"], record["y"], label)
            elif isinstance(layer, FastShapeLayer):
                sx, sy = layer.vertex(index)
                self.measurement.add(sx, sy, f"{layer.name}: vertex {index + 1}")
        else:
            self.measurement.add(x, y)

    def _show_record(self, layer_name: str, index: int) -> None:
        layer = self.layers[layer_name]
        if not layer.data:
            return
        record = layer.data.record(index)
        self.details.setPlainText(layer_name + "\n\n" + "\n".join(f"{key}: {value}" for key, value in record.items()))

    def keyPressEvent(self, event: QtGui.QKeyEvent) -> None:
        if event.key() == QtCore.Qt.Key.Key_Backspace:
            self.measurement.remove_last()
            return
        if event.key() == QtCore.Qt.Key.Key_Escape:
            self.measurement.clear()
            return
        super().keyPressEvent(event)
