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
from .repository import ProjectRepository
from .ribbon import RibbonBar
from .shapes import load_shapefile
from .workers import FunctionWorker
from .bbox import BlackBoxWindow
from .dsr_qc import DsrQcWindow
from .config import ProjectViewerConfig, CustomDsrLayerDefinition
from .custom_dsr_dialog import CustomDsrLayerDialog
from .icons_manager import icon
from .projects import ProjectsDatabase, ProjectsDatabaseError
from .svp3d import WaterColumnWindow
from .track3d import DsrBBox3DWindow


class MainWindow(QtWidgets.QMainWindow):
    """Main DataViewer 2.0 shell with ribbon, docks and fast map canvas."""

    def __init__(self, project_path: str | Path) -> None:
        super().__init__()
        self.project_path = Path(project_path)
        self.repository = ProjectRepository(project_path)
        self.viewer_config = ProjectViewerConfig(project_path)
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
        self.dsr_qc_window: DsrQcWindow | None = None
        self.svp3d_window: WaterColumnWindow | None = None
        self.track3d_window: DsrBBox3DWindow | None = None
        self.dsr_qc_cache: dict[int, object] = {}
        self._dsr_qc_request_line: int | None = None
        self.dsr_station_marker: pg.ScatterPlotItem | None = None
        self._dsr_tree_building = False
        self._dsr_station_values: list[int] = []
        self._dsr_auto_zoom = True
        self._custom_definition_by_layer: dict[str, CustomDsrLayerDefinition] = {}
        self._dsr_line_overlays: dict[tuple[str, int], FastPointLayer] = {}
        self._replacement_window: MainWindow | None = None
        self._build_ui()
        self._restore_state()
        self._start_loading()

    def _build_ui(self) -> None:
        self.setWindowTitle(f"SeisWebLog DataViewer 2.4 — {self.project_path.name}")
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
        self.ribbon.svp3d_open_requested.connect(self._open_svp3d_window)
        self.ribbon.track3d_open_requested.connect(self._open_track3d_window)

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
        self.layer_tree.itemExpanded.connect(self._tree_item_expanded)
        self.layer_tree.itemClicked.connect(self._tree_item_clicked)
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


    def _open_svp3d_window(self) -> None:
        try:
            if self.svp3d_window is None:
                self.svp3d_window = WaterColumnWindow(self.project_path, self)
                self.svp3d_window.destroyed.connect(lambda: setattr(self, "svp3d_window", None))
            self.svp3d_window.show()
            self.svp3d_window.raise_()
            self.svp3d_window.activateWindow()
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "3D Water Column Viewer", str(exc))

    def _open_track3d_window(self) -> None:
        try:
            if self.track3d_window is None:
                self.track3d_window = DsrBBox3DWindow(self.project_path, self)
                self.track3d_window.destroyed.connect(lambda: setattr(self, "track3d_window", None))
            self.track3d_window.show()
            self.track3d_window.raise_()
            self.track3d_window.activateWindow()
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "3D DSR & BlackBox Viewer", str(exc))

    def _root_projects_database(self) -> Path | None:
        candidates = [
            Path.cwd() / "db.sqlite3",
            Path(__file__).resolve().parents[1] / "db.sqlite3",
        ]
        for candidate in candidates:
            if candidate.is_file():
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
            self._zoom_all()
            self.statusBar().showMessage(f"Ready — {len(self.layers)} layer(s)")

    def _group_item(self, name: str) -> QtWidgets.QTreeWidgetItem:
        for index in range(self.layer_tree.topLevelItemCount()):
            item = self.layer_tree.topLevelItem(index)
            if item.text(0) == name:
                return item
        item = QtWidgets.QTreeWidgetItem([name, ""])
        item.setIcon(0, icon("group", size=20))
        item.setFlags(item.flags() & ~QtCore.Qt.ItemFlag.ItemIsUserCheckable)
        item.setExpanded(True)
        self.layer_tree.addTopLevelItem(item)
        return item

    def _register_layer(self, group: str, name: str, count: int, layer: FastPointLayer | FastShapeLayer, tooltip: str = "") -> str:
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
        item.setIcon(0, icon(icon_key, size=20))
        item.setFlags(item.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
        item.setCheckState(0, QtCore.Qt.CheckState.Checked)
        item.setToolTip(0, tooltip)
        item.setData(0, QtCore.Qt.ItemDataRole.UserRole, unique)
        parent.addChild(item)
        self.layer_items[unique] = item
        self._update_layer_z_values()
        return unique

    def _point_layer_loaded(self, group: str, name: str, data: PointLayerData, point_color: str, line_color: str | None, connect_by: str | None) -> None:
        layer = FastPointLayer(self.plot_item, name, point_color, line_color, connect_by)
        layer.set_data(data)
        layer.selection_changed.connect(self._show_record)
        registered_name = self._register_layer(group, name, data.count, layer)
        if name.startswith("DSR "):
            self._attach_dsr_hierarchy(registered_name)
        if name == "DSR Primary":
            self._populate_dsr_ribbon(data)
        self._finish_load()

    def _register_shape_data(self, data: ShapeLayerData) -> None:
        style_override = self.viewer_config.shape_styles.get(data.name, {})
        layer = FastShapeLayer(self.plot_item, data, style_override=style_override)
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
                color = definition.categories.get(category, {}).get("color", self._category_color(category))
                layer = FastPointLayer(self.plot_item, category_data.name, color, color, "line" if "line" in category_data.metadata else None)
                layer.update_style(point_color=color, line_color=color, point_size=definition.point_size, line_width=1.5)
                layer.set_data(category_data)
                layer.set_visible(definition.visible)
                layer.selection_changed.connect(self._show_record)
                registered = self._register_layer("Custom DSR Layers", category_data.name, category_data.count, layer)
                self._custom_definition_by_layer[registered] = definition
                if definition.split_by_line:
                    self._attach_dsr_hierarchy(registered)
        else:
            color = definition.color
            layer = FastPointLayer(self.plot_item, definition.name, color, color, "line" if "line" in data.metadata else None)
            layer.update_style(point_color=color, line_color=color, point_size=definition.point_size, line_width=1.5)
            layer.set_data(data)
            layer.set_visible(definition.visible)
            layer.selection_changed.connect(self._show_record)
            registered = self._register_layer("Custom DSR Layers", definition.name, data.count, layer)
            self._custom_definition_by_layer[registered] = definition
            if definition.split_by_line:
                self._attach_dsr_hierarchy(registered)
        self._finish_load()

    def _create_custom_dsr_layer(self) -> None:
        try:
            columns = self.repository.dsr_columns()
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Custom DSR layer", str(exc))
            return
        dialog = CustomDsrLayerDialog(columns, self)
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
            self.layers[name].set_visible(item.checkState(0) == QtCore.Qt.CheckState.Checked)

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
        zoom_action = menu.addAction(icon("zoom_layer", size=18), "Zoom to layer")
        zoom_action.triggered.connect(lambda: self._zoom_to_layer(name))
        visibility_action = menu.addAction(icon("hide" if layer.visible else "show", size=18), "Hide layer" if layer.visible else "Show layer")
        visibility_action.triggered.connect(lambda: item.setCheckState(0, QtCore.Qt.CheckState.Unchecked if layer.visible else QtCore.Qt.CheckState.Checked))
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
        point_size = QtWidgets.QDoubleSpinBox()
        point_size.setRange(1.0, 30.0)
        point_size.setDecimals(1)
        point_size.setSingleStep(1.0)
        point_size.setValue(layer.point_size)

        selected = {
            "line": QtGui.QColor(layer.line_color),
            "point": QtGui.QColor(layer.point_color),
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

        form.addRow("Line color:", line_button)
        form.addRow("Point color:", point_button)
        form.addRow("Line thickness:", line_width)
        form.addRow("Point size:", point_size)

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
            point_size=point_size.value(),
        )
        self.statusBar().showMessage(f"Updated style: {name}", 2500)

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

    def _show_external_record(self, title: str, data: PointLayerData, index: int) -> None:
        if index < 0 or index >= data.count:
            return
        record = data.record(index)
        self.details.setPlainText(title + "\n\n" + "\n".join(f"{key}: {value}" for key, value in record.items()))
        self.details_dock.show(); self.details_dock.raise_()

    def _show_record(self, layer_name: str, index: int) -> None:
        layer = self.layers.get(layer_name)
        if not isinstance(layer, FastPointLayer) or layer.data is None:
            return
        record = layer.data.record(index)
        self.details.setPlainText(layer_name + "\n\n" + "\n".join(f"{key}: {value}" for key, value in record.items()))
        self.details_dock.show()
        self.details_dock.raise_()

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
        if self.dsr_qc_window is not None and self.dsr_qc_window.isVisible():
            self.dsr_qc_window.select_line(int(line))

    def _ensure_dsr_qc_window(self) -> DsrQcWindow:
        if self.dsr_qc_window is None:
            window = DsrQcWindow(self)
            window.line_requested.connect(self._load_dsr_qc_line)
            window.station_selected.connect(self._select_dsr_station)
            window.zoom_station_requested.connect(self._zoom_dsr_station)
            self.dsr_qc_window = window
        return self.dsr_qc_window

    def _open_dsr_qc_window(self) -> None:
        window = self._ensure_dsr_qc_window()
        data = self._dsr_primary_data()
        lines: list[int] = []
        if data is not None:
            line_values = self._numeric_values(data.metadata.get("line"))
            lines = sorted({int(round(value)) for value in line_values[np.isfinite(line_values)]})
        selected = self.ribbon.current_dsr_line()
        window.set_lines(lines, selected)
        window.show()
        window.raise_()
        window.activateWindow()
        if selected is not None:
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
        data = self._dsr_primary_data()
        indices = self._dsr_indices(line, station)
        if data is None or indices.size == 0:
            return
        index = int(indices[0])
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
        for layer in self.layers.values():
            layer.remove()
        for overlay in self._dsr_line_overlays.values():
            overlay.remove()
        self._dsr_line_overlays.clear()
        self.layers.clear()
        self.layer_items.clear()
        self.bbox_track_layer_names.clear()
        self.bbox_last_track_layer_name = None
        self.dsr_qc_cache.clear()
        if self.dsr_station_marker is not None:
            self.plot_item.removeItem(self.dsr_station_marker)
            self.dsr_station_marker = None
        self.layer_tree.clear()
        self.measurement.clear()
        self.details.clear()
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
