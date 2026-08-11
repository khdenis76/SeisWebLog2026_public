from __future__ import annotations

from pathlib import Path
import traceback

import numpy as np
from PySide6 import QtCore, QtWidgets

from .data import (
    Bathymetry3DDataError,
    Bathymetry3DRepository,
    BathymetryPoints,
    ReceiverPoints3D,
)

try:
    import pyvista as pv
    from pyvistaqt import QtInteractor
    PYVISTA_AVAILABLE = True
except Exception:
    pv = None
    QtInteractor = None
    PYVISTA_AVAILABLE = False


class Bathymetry3DWindow(QtWidgets.QMainWindow):
    """3D bathymetry surface with DSR receiver overlay."""

    def __init__(self, project_path: str | Path, parent=None) -> None:
        super().__init__(parent)
        self.project_path = Path(project_path)
        self.repository = Bathymetry3DRepository(project_path)
        self.bathymetry: BathymetryPoints | None = None
        self.receivers: ReceiverPoints3D | None = None
        self.origin = np.zeros(3, dtype=float)
        self.setWindowTitle("3D Bathymetry & Receivers — DataViewer 2")
        self.resize(1550, 930)
        self._build_ui()
        self._load_options()

    def _build_ui(self) -> None:
        root = QtWidgets.QWidget()
        outer = QtWidgets.QHBoxLayout(root)
        outer.setContentsMargins(5, 5, 5, 5)

        scroll = QtWidgets.QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setMaximumWidth(370)
        controls = QtWidgets.QWidget()
        controls_layout = QtWidgets.QVBoxLayout(controls)

        title = QtWidgets.QLabel("3D Bathymetry + DSR Receivers")
        title.setStyleSheet("font-size:18px;font-weight:600")
        controls_layout.addWidget(title)
        self.info_label = QtWidgets.QLabel()
        self.info_label.setWordWrap(True)
        controls_layout.addWidget(self.info_label)

        source_box = QtWidgets.QGroupBox("Bathymetry source")
        source_form = QtWidgets.QFormLayout(source_box)
        self.source_combo = QtWidgets.QComboBox()
        self.bathy_position_combo = QtWidgets.QComboBox()
        self.bathy_position_combo.addItems([item[0] for item in self.repository.dsr_position_options()])
        self.line_combo = QtWidgets.QComboBox()
        self.line_combo.setEditable(True)
        self.max_surface_points = QtWidgets.QSpinBox()
        self.max_surface_points.setRange(1000, 500000)
        self.max_surface_points.setSingleStep(10000)
        self.max_surface_points.setValue(120000)
        self.load_surface_button = QtWidgets.QPushButton("Load bathymetry")
        source_form.addRow("Source:", self.source_combo)
        source_form.addRow("DSR position:", self.bathy_position_combo)
        source_form.addRow("Receiver line:", self.line_combo)
        source_form.addRow("Max points:", self.max_surface_points)
        source_form.addRow(self.load_surface_button)
        controls_layout.addWidget(source_box)

        receiver_box = QtWidgets.QGroupBox("DSR receiver overlay")
        receiver_form = QtWidgets.QFormLayout(receiver_box)
        self.show_receivers = QtWidgets.QCheckBox("Show receivers")
        self.show_receivers.setChecked(True)
        self.receiver_position_combo = QtWidgets.QComboBox()
        self.receiver_position_combo.addItems([item[0] for item in self.repository.dsr_position_options()])
        self.receiver_z_combo = QtWidgets.QComboBox()
        self.receiver_z_combo.addItems(("Use DSR Z", "Drape on bathymetry"))
        self.receiver_size = QtWidgets.QDoubleSpinBox()
        self.receiver_size.setRange(2.0, 30.0)
        self.receiver_size.setValue(8.0)
        self.receiver_size.setSingleStep(1.0)
        self.color_receivers_by = QtWidgets.QComboBox()
        self.color_receivers_by.addItems(("Single color", "Receiver line", "ROV"))
        self.load_receivers_button = QtWidgets.QPushButton("Load receivers")
        receiver_form.addRow(self.show_receivers)
        receiver_form.addRow("Position:", self.receiver_position_combo)
        receiver_form.addRow("Receiver Z:", self.receiver_z_combo)
        receiver_form.addRow("Point size:", self.receiver_size)
        receiver_form.addRow("Color by:", self.color_receivers_by)
        receiver_form.addRow(self.load_receivers_button)
        controls_layout.addWidget(receiver_box)

        display_box = QtWidgets.QGroupBox("3D display")
        display_form = QtWidgets.QFormLayout(display_box)
        self.z_mode_combo = QtWidgets.QComboBox()
        self.z_mode_combo.addItems(("Depth below sea level (-|Z|)", "Use source Z", "Invert source Z (-Z)"))
        self.vertical_exaggeration = QtWidgets.QDoubleSpinBox()
        self.vertical_exaggeration.setRange(0.1, 100.0)
        self.vertical_exaggeration.setValue(1.0)
        self.vertical_exaggeration.setSingleStep(0.5)
        self.surface_style_combo = QtWidgets.QComboBox()
        self.surface_style_combo.addItems(("Surface", "Surface + edges", "Points"))
        self.colormap_combo = QtWidgets.QComboBox()
        self.colormap_combo.addItems(("terrain", "viridis", "turbo", "gist_earth", "plasma"))
        self.opacity_spin = QtWidgets.QDoubleSpinBox()
        self.opacity_spin.setRange(0.05, 1.0)
        self.opacity_spin.setSingleStep(0.05)
        self.opacity_spin.setValue(1.0)
        self.show_grid = QtWidgets.QCheckBox("Show axes/grid")
        self.show_grid.setChecked(True)
        display_form.addRow("Z convention:", self.z_mode_combo)
        display_form.addRow("Vertical exaggeration:", self.vertical_exaggeration)
        display_form.addRow("Surface style:", self.surface_style_combo)
        display_form.addRow("Color map:", self.colormap_combo)
        display_form.addRow("Opacity:", self.opacity_spin)
        display_form.addRow(self.show_grid)
        controls_layout.addWidget(display_box)

        action_row = QtWidgets.QHBoxLayout()
        self.render_button = QtWidgets.QPushButton("Plot / Refresh")
        self.zoom_button = QtWidgets.QPushButton("Zoom all")
        action_row.addWidget(self.render_button)
        action_row.addWidget(self.zoom_button)
        controls_layout.addLayout(action_row)
        self.screenshot_button = QtWidgets.QPushButton("Export screenshot")
        controls_layout.addWidget(self.screenshot_button)
        controls_layout.addStretch(1)

        scroll.setWidget(controls)
        outer.addWidget(scroll)

        if PYVISTA_AVAILABLE:
            self.plotter = QtInteractor(root)
            self.plotter.set_background("#111317")
            outer.addWidget(self.plotter.interactor, 1)
        else:
            self.plotter = None
            message = QtWidgets.QTextBrowser()
            message.setHtml(
                "<h2>3D dependencies are not installed</h2>"
                "<p>Install them in the SeisWebLog environment:</p>"
                "<pre>python -m pip install pyvista pyvistaqt vtk</pre>"
            )
            outer.addWidget(message, 1)
        self.setCentralWidget(root)

        self.source_combo.currentTextChanged.connect(self._source_changed)
        self.load_surface_button.clicked.connect(self.load_bathymetry)
        self.load_receivers_button.clicked.connect(self.load_receivers)
        self.render_button.clicked.connect(self.render_scene)
        self.zoom_button.clicked.connect(lambda: self.plotter.reset_camera() if self.plotter else None)
        self.screenshot_button.clicked.connect(self.export_screenshot)
        for widget_signal in (
            self.show_receivers.toggled,
            self.receiver_z_combo.currentTextChanged,
            self.receiver_size.valueChanged,
            self.color_receivers_by.currentTextChanged,
            self.z_mode_combo.currentTextChanged,
            self.vertical_exaggeration.valueChanged,
            self.surface_style_combo.currentTextChanged,
            self.colormap_combo.currentTextChanged,
            self.opacity_spin.valueChanged,
            self.show_grid.toggled,
        ):
            widget_signal.connect(self.render_scene)

    def _load_options(self) -> None:
        try:
            sources = self.repository.available_sources()
            self.source_combo.addItems(sources)
            self.line_combo.addItem("All lines", None)
            for line in self.repository.dsr_lines():
                self.line_combo.addItem(str(line), int(line))
            self.info_label.setText(
                f"Database: {self.repository.db_path}\n"
                f"Bathymetry sources: {', '.join(sources) if sources else 'none'}"
            )
            self._source_changed(self.source_combo.currentText())
            if sources:
                self.load_bathymetry()
                self.load_receivers()
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "3D bathymetry", str(exc))

    def _source_changed(self, source: str) -> None:
        is_dsr = source == "DSR"
        self.bathy_position_combo.setEnabled(is_dsr)
        self.line_combo.setEnabled(is_dsr or True)

    def _selected_line(self) -> int | None:
        value = self.line_combo.currentData()
        return None if value is None else int(value)

    def load_bathymetry(self) -> None:
        try:
            source = self.source_combo.currentText()
            max_points = int(self.max_surface_points.value())
            if source == "SPSolution":
                self.bathymetry = self.repository.load_sps_bathymetry(max_points=max_points)
            elif source == "DSR":
                self.bathymetry = self.repository.load_dsr_bathymetry(
                    self.bathy_position_combo.currentText(),
                    line=self._selected_line(),
                    max_points=max_points,
                )
            else:
                raise Bathymetry3DDataError("No bathymetry source is available.")
            self.statusBar().showMessage(
                f"Loaded {self.bathymetry.source}: {self.bathymetry.x.size:,} points",
                5000,
            )
            self.render_scene()
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Load bathymetry", f"{exc}\n\n{traceback.format_exc()[-1600:]}")

    def load_receivers(self) -> None:
        try:
            self.receivers = self.repository.load_receivers(
                self.receiver_position_combo.currentText(),
                line=self._selected_line(),
            )
            self.statusBar().showMessage(
                f"Loaded {self.receivers.x.size:,} DSR receiver positions",
                5000,
            )
            self.render_scene()
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Load receivers", str(exc))

    def _display_z(self, values: np.ndarray) -> np.ndarray:
        z = np.asarray(values, dtype=float).copy()
        mode = self.z_mode_combo.currentText()
        if mode == "Depth below sea level (-|Z|)":
            z = -np.abs(z)
        elif mode == "Invert source Z (-Z)":
            z = -z
        return z * float(self.vertical_exaggeration.value())

    @staticmethod
    def _nearest_surface_z(
        query_x: np.ndarray,
        query_y: np.ndarray,
        surface: BathymetryPoints,
    ) -> np.ndarray:
        result = np.empty(query_x.size, dtype=float)
        sx, sy, sz = surface.x, surface.y, surface.z
        block = 1000
        for start in range(0, query_x.size, block):
            stop = min(query_x.size, start + block)
            qx = query_x[start:stop, None]
            qy = query_y[start:stop, None]
            # Keep memory bounded for large source datasets.
            best_distance = np.full(stop - start, np.inf, dtype=float)
            best_z = np.full(stop - start, np.nan, dtype=float)
            source_block = 10000
            for source_start in range(0, sx.size, source_block):
                source_stop = min(sx.size, source_start + source_block)
                distance = (qx - sx[source_start:source_stop]) ** 2 + (qy - sy[source_start:source_stop]) ** 2
                local_index = np.argmin(distance, axis=1)
                local_distance = distance[np.arange(stop - start), local_index]
                improve = local_distance < best_distance
                best_distance[improve] = local_distance[improve]
                best_z[improve] = sz[source_start:source_stop][local_index[improve]]
            result[start:stop] = best_z
        return result

    def render_scene(self) -> None:
        if not self.plotter or self.bathymetry is None:
            return
        self.plotter.clear()
        bathy = self.bathymetry
        z_display = self._display_z(bathy.z)
        self.origin = np.asarray([np.nanmean(bathy.x), np.nanmean(bathy.y), 0.0], dtype=float)
        points = np.c_[bathy.x - self.origin[0], bathy.y - self.origin[1], z_display]
        cloud = pv.PolyData(points)
        cloud["Depth"] = np.asarray(bathy.z, dtype=float)

        style = self.surface_style_combo.currentText()
        cmap = self.colormap_combo.currentText()
        opacity = float(self.opacity_spin.value())
        try:
            if style == "Points":
                self.plotter.add_points(
                    cloud,
                    scalars="Depth",
                    cmap=cmap,
                    point_size=4,
                    render_points_as_spheres=False,
                    opacity=opacity,
                    scalar_bar_args={"title": bathy.z_field},
                )
            else:
                surface = cloud.delaunay_2d()
                self.plotter.add_mesh(
                    surface,
                    scalars="Depth",
                    cmap=cmap,
                    show_edges=style == "Surface + edges",
                    edge_color="#444b55",
                    opacity=opacity,
                    scalar_bar_args={"title": bathy.z_field},
                    smooth_shading=False,
                )
        except Exception:
            self.plotter.add_points(
                cloud,
                scalars="Depth",
                cmap=cmap,
                point_size=4,
                opacity=opacity,
                scalar_bar_args={"title": bathy.z_field},
            )

        if self.show_receivers.isChecked() and self.receivers is not None:
            receivers = self.receivers
            rz = np.asarray(receivers.z, dtype=float).copy()
            if self.receiver_z_combo.currentText() == "Drape on bathymetry" or not np.isfinite(rz).any():
                rz = self._nearest_surface_z(receivers.x, receivers.y, bathy)
            else:
                missing = ~np.isfinite(rz)
                if missing.any():
                    rz[missing] = self._nearest_surface_z(receivers.x[missing], receivers.y[missing], bathy)
            receiver_points = np.c_[
                receivers.x - self.origin[0],
                receivers.y - self.origin[1],
                self._display_z(rz),
            ]
            receiver_cloud = pv.PolyData(receiver_points)
            color_by = self.color_receivers_by.currentText()
            kwargs: dict[str, object] = {
                "point_size": float(self.receiver_size.value()),
                "render_points_as_spheres": True,
            }
            if color_by == "Receiver line":
                values = np.nan_to_num(receivers.line, nan=-1.0)
                receiver_cloud["Receiver line"] = values
                kwargs.update({"scalars": "Receiver line", "cmap": "turbo"})
            elif color_by == "ROV":
                categories, encoded = np.unique(receivers.rov.astype(str), return_inverse=True)
                receiver_cloud["ROV"] = encoded.astype(float)
                kwargs.update({"scalars": "ROV", "cmap": "tab20"})
            else:
                kwargs["color"] = "#ff3b30"
            self.plotter.add_points(receiver_cloud, **kwargs)

        if self.show_grid.isChecked():
            self.plotter.show_grid(
                xtitle=f"Local Easting from {self.origin[0]:,.2f} m",
                ytitle=f"Local Northing from {self.origin[1]:,.2f} m",
                ztitle="Elevation / depth",
                color="#d0d5db",
            )
        self.plotter.add_text(
            f"{bathy.source} | {bathy.x.size:,} bathymetry points"
            + (f" | {self.receivers.x.size:,} receivers" if self.receivers is not None else ""),
            position="upper_left",
            font_size=10,
            color="white",
        )
        self.plotter.add_axes()
        self.plotter.reset_camera()
        self.plotter.render()

    def export_screenshot(self) -> None:
        if not self.plotter:
            return
        filename, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Export 3D screenshot",
            str(self.project_path / "bathymetry_3d.png"),
            "PNG image (*.png);;JPEG image (*.jpg *.jpeg)",
        )
        if filename:
            self.plotter.screenshot(filename)
            self.statusBar().showMessage(f"Saved screenshot: {filename}", 5000)

    def closeEvent(self, event) -> None:
        if self.plotter is not None:
            try:
                self.plotter.close()
            except Exception:
                pass
        super().closeEvent(event)
