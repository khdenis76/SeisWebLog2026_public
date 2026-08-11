from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import math
import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore, QtGui, QtWidgets

from .surface_data import SurfaceDataRepository, SurfaceDataError, SurfacePoints

try:
    from scipy.interpolate import griddata
    SCIPY_AVAILABLE = True
except Exception:
    griddata = None
    SCIPY_AVAILABLE = False


@dataclass
class SurfaceLayer2D:
    name: str
    image: pg.ImageItem | None
    contours: list[pg.IsocurveItem]
    colorbar: object | None
    extent: tuple[float, float, float, float]
    gx: np.ndarray
    gy: np.ndarray
    grid_z: np.ndarray
    z_field: str


@dataclass
class OverlayLayer2D:
    name: str
    items: list[object]
    labels: list[pg.TextItem]
    extent: tuple[float, float, float, float]
    points: SurfacePoints
    labels_enabled: bool
    label_mode: str
    label_zoom_span: float
    max_labels: int
    visible: bool = True


class Surface2DWindow(QtWidgets.QMainWindow):
    """Fast 2D heatmap/contour workbench with multiple point/vector overlays."""

    COLORS = ["#00e5ff", "#ffd740", "#ff6e40", "#ea80fc", "#69f0ae", "#82b1ff", "#ff80ab", "#b2ff59"]

    def __init__(self, project_path: str | Path, parent=None) -> None:
        super().__init__(parent)
        self.project_path = Path(project_path)
        self.repo = SurfaceDataRepository(project_path)
        self.surface_layers: dict[str, SurfaceLayer2D] = {}
        self.overlay_layers: dict[str, OverlayLayer2D] = {}
        self.surface_definitions: list[dict] = []
        self.overlay_definitions: list[dict] = []
        self._restoring = False
        self._hover_text: pg.TextItem | None = None
        self.config_path = self.repo.project_path / "config" / "dataviewer2_surfaces.json"
        self.setWindowTitle("2D Surface Maps — DataViewer 2")
        self.resize(1500, 920)
        self._build_ui()
        self._load_sources()
        self._load_config()

    def _build_ui(self) -> None:
        root = QtWidgets.QWidget()
        outer = QtWidgets.QHBoxLayout(root)
        outer.setContentsMargins(4, 4, 4, 4)

        controls = QtWidgets.QTabWidget()
        controls.setMaximumWidth(390)
        controls.setMinimumWidth(360)
        outer.addWidget(controls)

        surface_page = QtWidgets.QWidget(); sf = QtWidgets.QVBoxLayout(surface_page)
        source_box = QtWidgets.QGroupBox("Surface definition"); form = QtWidgets.QFormLayout(source_box)
        self.surface_source = QtWidgets.QComboBox()
        self.surface_x = QtWidgets.QComboBox(); self.surface_y = QtWidgets.QComboBox(); self.surface_z = QtWidgets.QComboBox()
        self.surface_line = QtWidgets.QComboBox(); self.surface_line.addItem("All lines", None)
        self.surface_display = QtWidgets.QComboBox(); self.surface_display.addItems(["Heatmap + contours", "Heatmap", "Contours"])
        self.surface_method = QtWidgets.QComboBox(); self.surface_method.addItems(["Linear", "Nearest", "IDW"])
        self.surface_resolution = QtWidgets.QSpinBox(); self.surface_resolution.setRange(50, 1000); self.surface_resolution.setValue(350)
        self.surface_contours = QtWidgets.QSpinBox(); self.surface_contours.setRange(2, 50); self.surface_contours.setValue(12)
        self.surface_opacity = QtWidgets.QDoubleSpinBox(); self.surface_opacity.setRange(0.05, 1.0); self.surface_opacity.setValue(0.8); self.surface_opacity.setSingleStep(0.05)
        self.surface_cmap = QtWidgets.QComboBox(); self.surface_cmap.addItems(["turbo", "viridis", "plasma", "inferno"])
        self.surface_name = QtWidgets.QLineEdit(); self.surface_name.setPlaceholderText("Automatic name")
        for label, widget in (("Source", self.surface_source), ("X field", self.surface_x), ("Y field", self.surface_y), ("Z parameter", self.surface_z), ("Line filter", self.surface_line), ("Display", self.surface_display), ("Interpolation", self.surface_method), ("Grid resolution", self.surface_resolution), ("Contour levels", self.surface_contours), ("Opacity", self.surface_opacity), ("Color map", self.surface_cmap), ("Layer name", self.surface_name)):
            form.addRow(label + ":", widget)
        sf.addWidget(source_box)
        row = QtWidgets.QHBoxLayout(); self.add_surface_btn = QtWidgets.QPushButton("Add surface"); self.clear_surfaces_btn = QtWidgets.QPushButton("Clear surfaces"); row.addWidget(self.add_surface_btn); row.addWidget(self.clear_surfaces_btn); sf.addLayout(row)
        self.surface_list = QtWidgets.QListWidget(); self.surface_list.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu); sf.addWidget(self.surface_list, 1)
        controls.addTab(surface_page, "Surfaces")

        overlay_page = QtWidgets.QWidget(); of = QtWidgets.QVBoxLayout(overlay_page)
        overlay_box = QtWidgets.QGroupBox("Overlay definition"); o = QtWidgets.QFormLayout(overlay_box)
        self.overlay_type = QtWidgets.QComboBox(); self.overlay_type.addItems(["Point", "Vector"])
        self.overlay_source = QtWidgets.QComboBox()
        self.overlay_x = QtWidgets.QComboBox(); self.overlay_y = QtWidgets.QComboBox(); self.overlay_z = QtWidgets.QComboBox()
        self.vector_end_x = QtWidgets.QComboBox(); self.vector_end_y = QtWidgets.QComboBox()
        self.overlay_line = QtWidgets.QComboBox(); self.overlay_line.addItem("All lines", None)
        self.overlay_color = QtWidgets.QPushButton("#00e5ff"); self.overlay_color_value = "#00e5ff"
        self.overlay_style_mode = QtWidgets.QComboBox(); self.overlay_style_mode.addItems(["Single symbol", "Categorized"])
        self.overlay_category_field = QtWidgets.QComboBox(); self.overlay_category_field.addItem("(none)", "")
        self.overlay_symbol = QtWidgets.QComboBox()
        for text, value in (("Circle","o"),("Square","s"),("Triangle","t"),("Diamond","d"),("Cross","x"),("Plus","+"),("Pentagon","p"),("Hexagon","h"),("Star","star")):
            self.overlay_symbol.addItem(text, value)
        self.overlay_size = QtWidgets.QDoubleSpinBox(); self.overlay_size.setRange(1.0, 30.0); self.overlay_size.setValue(7.0)
        self.overlay_width = QtWidgets.QDoubleSpinBox(); self.overlay_width.setRange(0.2, 10.0); self.overlay_width.setValue(1.5)
        self.overlay_labels = QtWidgets.QCheckBox("Show labels when zoomed in")
        self.overlay_labels.setChecked(True)
        self.overlay_label_field = QtWidgets.QComboBox()
        self.overlay_label_field2 = QtWidgets.QComboBox()
        self.overlay_label_field2.addItem("(none)", "")
        self.overlay_label_separator = QtWidgets.QLineEdit(" / ")
        self.overlay_label_separator.setMaximumWidth(80)
        self.overlay_label_mode = QtWidgets.QComboBox()
        self.overlay_label_mode.addItems(["When zoomed in", "Always"])
        self.overlay_label_zoom = QtWidgets.QDoubleSpinBox()
        self.overlay_label_zoom.setRange(1.0, 10000000.0)
        self.overlay_label_zoom.setDecimals(1)
        self.overlay_label_zoom.setValue(2000.0)
        self.overlay_label_zoom.setSuffix(" m view width")
        self.overlay_max_labels = QtWidgets.QSpinBox(); self.overlay_max_labels.setRange(1, 10000); self.overlay_max_labels.setValue(2000)
        self.overlay_name = QtWidgets.QLineEdit(); self.overlay_name.setPlaceholderText("Automatic name")
        for label, widget in (("Type", self.overlay_type), ("Source", self.overlay_source), ("Start/point X", self.overlay_x), ("Start/point Y", self.overlay_y), ("Z/parameter", self.overlay_z), ("Vector end X", self.vector_end_x), ("Vector end Y", self.vector_end_y), ("Line filter", self.overlay_line), ("Color", self.overlay_color), ("Style mode", self.overlay_style_mode), ("Category field", self.overlay_category_field), ("Point symbol", self.overlay_symbol), ("Point size", self.overlay_size), ("Line width", self.overlay_width), ("Label field 1", self.overlay_label_field), ("Label field 2", self.overlay_label_field2), ("Label separator", self.overlay_label_separator), ("Label visibility", self.overlay_label_mode), ("Label zoom threshold", self.overlay_label_zoom), ("Maximum labels", self.overlay_max_labels), ("Layer name", self.overlay_name)):
            o.addRow(label + ":", widget)
        o.addRow(self.overlay_labels)
        of.addWidget(overlay_box)
        row = QtWidgets.QHBoxLayout(); self.add_overlay_btn = QtWidgets.QPushButton("Add overlay"); self.clear_overlays_btn = QtWidgets.QPushButton("Clear overlays"); row.addWidget(self.add_overlay_btn); row.addWidget(self.clear_overlays_btn); of.addLayout(row)
        self.overlay_list = QtWidgets.QListWidget(); of.addWidget(self.overlay_list, 1)
        controls.addTab(overlay_page, "Overlays")

        self.plot = pg.PlotWidget()
        self.plot.setBackground("#111317")
        self.plot.getPlotItem().setAspectLocked(True)
        self.plot.getPlotItem().showGrid(x=True, y=True, alpha=0.18)
        self.plot.getPlotItem().setLabel("bottom", "Easting", units="m")
        self.plot.getPlotItem().setLabel("left", "Northing", units="m")
        for axis_name in ("left", "bottom"):
            axis = self.plot.getPlotItem().getAxis(axis_name)
            axis.setPen(pg.mkPen("#b9c4cf"))
            axis.setTextPen(pg.mkPen("#f2f5f7"))
        self._hover_text = pg.TextItem(
            "", color="#ffffff", anchor=(0, 1),
            fill=pg.mkBrush(0, 0, 0, 190), border=pg.mkPen(255, 255, 255, 90)
        )
        self._hover_text.setZValue(10000)
        self._hover_text.hide()
        self.plot.addItem(self._hover_text)
        outer.addWidget(self.plot, 1)
        self.setCentralWidget(root)

        self.surface_source.currentTextChanged.connect(self._surface_source_changed)
        self.overlay_source.currentTextChanged.connect(self._overlay_source_changed)
        self.overlay_type.currentTextChanged.connect(self._overlay_type_changed)
        self.overlay_color.clicked.connect(self._choose_overlay_color)
        self.add_surface_btn.clicked.connect(self.add_surface)
        self.clear_surfaces_btn.clicked.connect(self.clear_surfaces)
        self.add_overlay_btn.clicked.connect(self.add_overlay)
        self.clear_overlays_btn.clicked.connect(self.clear_overlays)
        self.surface_list.itemChanged.connect(self._surface_visibility_changed)
        self.overlay_list.itemChanged.connect(self._overlay_visibility_changed)
        self.surface_list.customContextMenuRequested.connect(self._surface_menu)
        self.plot.getViewBox().sigRangeChanged.connect(self._update_label_visibility)
        self._mouse_proxy = pg.SignalProxy(
            self.plot.scene().sigMouseMoved, rateLimit=30, slot=self._mouse_moved
        )

    def _load_sources(self) -> None:
        sources = self.repo.available_sources()
        self.surface_source.addItems(sources); self.overlay_source.addItems(sources)
        if sources:
            self._surface_source_changed(sources[0]); self._overlay_source_changed(sources[0])

    @staticmethod
    def _set_combo(combo: QtWidgets.QComboBox, values: list[str], selected: str = "") -> None:
        combo.blockSignals(True); combo.clear(); combo.addItems(values)
        if selected:
            idx = combo.findText(selected)
            if idx >= 0: combo.setCurrentIndex(idx)
        combo.blockSignals(False)

    def _surface_source_changed(self, source: str) -> None:
        fields = self.repo.numeric_columns(source)
        x, y, z = self.repo.default_fields(source)
        self._set_combo(self.surface_x, fields, x); self._set_combo(self.surface_y, fields, y); self._set_combo(self.surface_z, fields, z)
        self.surface_line.clear(); self.surface_line.addItem("All lines", None)
        for value in self.repo.line_values(source): self.surface_line.addItem(str(value), value)
        note = self.repo.source_note(source)
        if note:
            self.statusBar().showMessage(note, 6000)

    def _overlay_source_changed(self, source: str) -> None:
        fields = self.repo.numeric_columns(source)
        x, y, z = self.repo.default_fields(source)
        for combo, selected in ((self.overlay_x, x), (self.overlay_y, y), (self.overlay_z, z), (self.vector_end_x, "PrimaryEasting"), (self.vector_end_y, "PrimaryNorthing")):
            self._set_combo(combo, fields, selected)
        candidates = self.repo.label_candidates(source)
        self._set_combo(self.overlay_label_field, candidates)
        self.overlay_label_field2.blockSignals(True)
        self.overlay_label_field2.clear()
        self.overlay_label_field2.addItem("(none)", "")
        for value in candidates:
            self.overlay_label_field2.addItem(value, value)
        self.overlay_label_field2.blockSignals(False)
        self.overlay_category_field.blockSignals(True)
        self.overlay_category_field.clear(); self.overlay_category_field.addItem("(none)", "")
        for value in candidates: self.overlay_category_field.addItem(value, value)
        self.overlay_category_field.blockSignals(False)
        note = self.repo.source_note(source)
        if note:
            self.statusBar().showMessage(note, 6000)
        self.overlay_line.clear(); self.overlay_line.addItem("All lines", None)
        for value in self.repo.line_values(source): self.overlay_line.addItem(str(value), value)

    def _overlay_type_changed(self, value: str) -> None:
        enabled = value == "Vector"
        self.vector_end_x.setEnabled(enabled); self.vector_end_y.setEnabled(enabled)

    def _choose_overlay_color(self) -> None:
        c = QtWidgets.QColorDialog.getColor(QtGui.QColor(self.overlay_color_value), self)
        if c.isValid():
            self.overlay_color_value = c.name(); self.overlay_color.setText(c.name()); self.overlay_color.setStyleSheet(f"background:{c.name()}")

    def _interpolate(self, points: SurfacePoints) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        n = int(self.surface_resolution.value())
        gx = np.linspace(float(np.nanmin(points.x)), float(np.nanmax(points.x)), n)
        gy = np.linspace(float(np.nanmin(points.y)), float(np.nanmax(points.y)), n)
        xx, yy = np.meshgrid(gx, gy)
        method = self.surface_method.currentText().lower()
        if SCIPY_AVAILABLE and method in {"linear", "nearest"}:
            zz = griddata(np.c_[points.x, points.y], points.z, (xx, yy), method=method)
        else:
            # Chunked IDW/nearest fallback avoids requiring SciPy.
            zz = np.empty(xx.size, float)
            q = np.c_[xx.ravel(), yy.ravel()]
            p = np.c_[points.x, points.y]
            for start in range(0, q.shape[0], 3000):
                chunk = q[start:start+3000]
                d2 = (chunk[:, None, 0] - p[None, :, 0]) ** 2 + (chunk[:, None, 1] - p[None, :, 1]) ** 2
                if method == "nearest":
                    idx = np.argmin(d2, axis=1); zz[start:start+len(chunk)] = points.z[idx]
                else:
                    k = min(12, p.shape[0]); idx = np.argpartition(d2, k - 1, axis=1)[:, :k]
                    local_d2 = np.take_along_axis(d2, idx, axis=1)
                    values = points.z[idx]; w = 1.0 / np.maximum(local_d2, 1e-12)
                    zz[start:start+len(chunk)] = np.sum(w * values, axis=1) / np.sum(w, axis=1)
            zz = zz.reshape(xx.shape)
        return gx, gy, zz

    def add_surface(self) -> None:
        try:
            source = self.surface_source.currentText()
            points = self.repo.load_points(source, self.surface_x.currentText(), self.surface_y.currentText(), self.surface_z.currentText(), line_filter=self.surface_line.currentData(), max_points=150000)
            gx, gy, zz = self._interpolate(points)
            name = self.surface_name.text().strip() or f"{source} — {self.surface_z.currentText()}"
            definition = {
                "name": name, "source": source, "x": self.surface_x.currentText(),
                "y": self.surface_y.currentText(), "z": self.surface_z.currentText(),
                "line": self.surface_line.currentData(), "display": self.surface_display.currentText(),
                "method": self.surface_method.currentText(), "resolution": int(self.surface_resolution.value()),
                "contours": int(self.surface_contours.value()), "opacity": float(self.surface_opacity.value()),
                "cmap": self.surface_cmap.currentText(),
            }
            if name in self.surface_layers: self._remove_surface(name)
            image = None; contours: list[pg.IsocurveItem] = []; colorbar = None
            cmap = pg.colormap.get(self.surface_cmap.currentText())
            display = self.surface_display.currentText()
            if "Heatmap" in display:
                image = pg.ImageItem(zz.T)
                image.setRect(QtCore.QRectF(gx[0], gy[0], gx[-1]-gx[0], gy[-1]-gy[0]))
                finite = zz[np.isfinite(zz)]
                if finite.size: image.setLevels((float(np.nanmin(finite)), float(np.nanmax(finite))))
                image.setColorMap(cmap); image.setOpacity(float(self.surface_opacity.value()))
                self.plot.addItem(image)
                try:
                    vmin, vmax = float(np.nanmin(finite)), float(np.nanmax(finite))
                    colorbar = pg.ColorBarItem(
                        values=(vmin, vmax), colorMap=cmap,
                        label=self.surface_z.currentText(), interactive=False,
                        width=14
                    )
                    colorbar.setImageItem(image, insert_in=self.plot.getPlotItem())
                    colorbar.axis.setPen(pg.mkPen("#dbe4ec"))
                    colorbar.axis.setTextPen(pg.mkPen("#ffffff"))
                    colorbar.axis.label.setHtml(
                        f'<span style="color:#ffffff">{self.surface_z.currentText()}</span>'
                    )
                except Exception:
                    colorbar = None
            if "contours" in display.lower():
                finite = zz[np.isfinite(zz)]
                if finite.size:
                    levels = np.linspace(float(np.nanmin(finite)), float(np.nanmax(finite)), int(self.surface_contours.value()) + 2)[1:-1]
                    transform = QtGui.QTransform(); transform.translate(gx[0], gy[0]); transform.scale((gx[-1]-gx[0])/(len(gx)-1), (gy[-1]-gy[0])/(len(gy)-1))
                    for level in levels:
                        iso = pg.IsocurveItem(data=zz.T, level=float(level), pen=pg.mkPen("#ffffff", width=1))
                        iso.setTransform(transform); self.plot.addItem(iso); contours.append(iso)
            layer = SurfaceLayer2D(
                name, image, contours, colorbar,
                (gx[0], gx[-1], gy[0], gy[-1]),
                gx, gy, zz, self.surface_z.currentText()
            )
            self.surface_layers[name] = layer
            if not self._restoring:
                self.surface_definitions = [d for d in self.surface_definitions if d.get("name") != name]
                self.surface_definitions.append(definition); self._save_config()
            item = QtWidgets.QListWidgetItem(name); item.setFlags(item.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable); item.setCheckState(QtCore.Qt.CheckState.Checked); self.surface_list.addItem(item)
            self.plot.autoRange(); self.statusBar().showMessage(f"Added {name}", 4000)
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Add 2D surface", str(exc))

    def add_overlay(self) -> None:
        try:
            source = self.overlay_source.currentText()
            kind = self.overlay_type.currentText()
            line_filter = self.overlay_line.currentData()
            labels_enabled = self.overlay_labels.isChecked()
            label_fields: list[str] = []
            if labels_enabled and self.overlay_label_field.currentText():
                label_fields.append(self.overlay_label_field.currentText())
            second = self.overlay_label_field2.currentData()
            if labels_enabled and second:
                label_fields.append(str(second))
            category_field = str(self.overlay_category_field.currentData() or "") if self.overlay_style_mode.currentText() == "Categorized" else ""
            points = self.repo.load_points(
                source,
                self.overlay_x.currentText(),
                self.overlay_y.currentText(),
                self.overlay_z.currentText(),
                label_fields=label_fields,
                label_separator=self.overlay_label_separator.text(),
                metadata_fields=[category_field] if category_field else None,
                line_filter=line_filter,
                max_points=100000,
            )
            name = self.overlay_name.text().strip() or f"{source} — {kind} {len(self.overlay_layers)+1}"
            definition = {
                "name": name, "kind": kind, "source": source,
                "x": self.overlay_x.currentText(), "y": self.overlay_y.currentText(),
                "z": self.overlay_z.currentText(), "end_x": self.vector_end_x.currentText(),
                "end_y": self.vector_end_y.currentText(), "line": line_filter,
                "color": self.overlay_color_value, "style_mode": self.overlay_style_mode.currentText(),
                "category_field": category_field, "symbol": str(self.overlay_symbol.currentData() or "o"),
                "size": float(self.overlay_size.value()),
                "width": float(self.overlay_width.value()), "labels": labels_enabled,
                "label_field": self.overlay_label_field.currentText(),
                "label_field2": str(second or ""),
                "label_separator": self.overlay_label_separator.text(),
                "label_mode": self.overlay_label_mode.currentText(),
                "label_zoom_span": float(self.overlay_label_zoom.value()),
                "max_labels": int(self.overlay_max_labels.value()),
            }
            if name in self.overlay_layers:
                self._remove_overlay(name)
            items: list[object] = []
            labels: list[pg.TextItem] = []
            if kind == "Point":
                if category_field and category_field in points.metadata:
                    categories = np.asarray(points.metadata[category_field], dtype=object)
                    unique_values = list(dict.fromkeys(str(v) if v is not None else "NULL" for v in categories))
                    symbols = ["o", "s", "t", "d", "x", "+", "p", "h", "star"]
                    for category_index, category in enumerate(unique_values):
                        mask = np.asarray([(str(v) if v is not None else "NULL") == category for v in categories], dtype=bool)
                        color = self.COLORS[category_index % len(self.COLORS)]
                        symbol = symbols[category_index % len(symbols)]
                        scatter = pg.ScatterPlotItem(
                            points.x[mask], points.y[mask],
                            size=float(self.overlay_size.value()),
                            symbol=symbol,
                            brush=pg.mkBrush(color),
                            pen=pg.mkPen("#111111"), pxMode=True,
                            data=np.nonzero(mask)[0],
                            name=f"{name} — {category}",
                        )
                        self.plot.addItem(scatter); items.append(scatter)
                else:
                    scatter = pg.ScatterPlotItem(
                        points.x, points.y,
                        size=float(self.overlay_size.value()),
                        symbol=str(self.overlay_symbol.currentData() or "o"),
                        brush=pg.mkBrush(self.overlay_color_value),
                        pen=pg.mkPen("#111111"), pxMode=True,
                        data=np.arange(points.x.size, dtype=np.int64),
                    )
                    self.plot.addItem(scatter)
                    items.append(scatter)
            else:
                end = self.repo.load_points(
                    source,
                    self.vector_end_x.currentText(),
                    self.vector_end_y.currentText(),
                    self.overlay_z.currentText(),
                    line_filter=line_filter,
                    max_points=100000,
                )
                n = min(points.x.size, end.x.size, 5000)
                xs = np.empty(n * 3); ys = np.empty(n * 3)
                xs[0::3] = points.x[:n]; xs[1::3] = end.x[:n]; xs[2::3] = np.nan
                ys[0::3] = points.y[:n]; ys[1::3] = end.y[:n]; ys[2::3] = np.nan
                curve = pg.PlotCurveItem(
                    xs, ys, connect="finite",
                    pen=pg.mkPen(self.overlay_color_value, width=float(self.overlay_width.value()))
                )
                self.plot.addItem(curve); items.append(curve)
                for i in np.linspace(0, n - 1, min(n, 400), dtype=int):
                    dx = end.x[i] - points.x[i]; dy = end.y[i] - points.y[i]
                    if dx == 0 and dy == 0:
                        continue
                    angle = math.degrees(math.atan2(dy, dx))
                    arrow = pg.ArrowItem(
                        pos=(end.x[i], end.y[i]), angle=180 - angle,
                        brush=self.overlay_color_value, pen=None, headLen=8
                    )
                    self.plot.addItem(arrow); items.append(arrow)
            # Labels are generated dynamically from points visible in the current view.
            extent = (
                float(np.nanmin(points.x)), float(np.nanmax(points.x)),
                float(np.nanmin(points.y)), float(np.nanmax(points.y))
            )
            self.overlay_layers[name] = OverlayLayer2D(
                name, items, labels, extent, points, labels_enabled,
                self.overlay_label_mode.currentText(),
                float(self.overlay_label_zoom.value()),
                int(self.overlay_max_labels.value()), True
            )
            if not self._restoring:
                self.overlay_definitions = [d for d in self.overlay_definitions if d.get("name") != name]
                self.overlay_definitions.append(definition)
                self._save_config()
            item = QtWidgets.QListWidgetItem(name)
            item.setFlags(item.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(QtCore.Qt.CheckState.Checked)
            self.overlay_list.addItem(item)
            self.plot.autoRange()
            self._update_label_visibility()
            self.statusBar().showMessage(
                f"Added {name}; Z field: {self.overlay_z.currentText()}", 4000
            )
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Add overlay", str(exc))

    def _update_label_visibility(self, *_args) -> None:
        try:
            view = self.plot.getViewBox().viewRange()
            x0, x1 = map(float, view[0]); y0, y1 = map(float, view[1])
            span = abs(x1 - x0)
        except Exception:
            x0 = y0 = -float("inf"); x1 = y1 = float("inf"); span = float("inf")
        for layer in self.overlay_layers.values():
            show = layer.visible and layer.labels_enabled
            if layer.label_mode == "When zoomed in":
                show = show and span <= layer.label_zoom_span
            for label in layer.labels:
                try: self.plot.removeItem(label)
                except Exception: pass
            layer.labels.clear()
            if not show or layer.points.labels.size == 0:
                continue
            mask = (layer.points.x >= x0) & (layer.points.x <= x1) & (layer.points.y >= y0) & (layer.points.y <= y1)
            indices = np.nonzero(mask)[0]
            if indices.size > layer.max_labels:
                step = int(np.ceil(indices.size / layer.max_labels))
                indices = indices[::step]
            for i in indices:
                value = str(layer.points.labels[i] or "").strip()
                if not value:
                    continue
                text = pg.TextItem(
                    value, color="#ffffff", anchor=(0, 1),
                    fill=pg.mkBrush(0, 0, 0, 175),
                    border=pg.mkPen(255, 255, 255, 80),
                )
                text.setPos(float(layer.points.x[i]), float(layer.points.y[i]))
                text.setZValue(9000)
                self.plot.addItem(text); layer.labels.append(text)

    def _mouse_moved(self, event) -> None:
        position = event[0] if isinstance(event, (tuple, list)) else event
        if not self.plot.sceneBoundingRect().contains(position):
            if self._hover_text:
                self._hover_text.hide()
            return
        point = self.plot.getPlotItem().vb.mapSceneToView(position)
        x, y = float(point.x()), float(point.y())
        parts = [f"X {x:,.3f}", f"Y {y:,.3f}"]
        # Report the topmost visible surface value at the cursor.
        for item_index in range(self.surface_list.count() - 1, -1, -1):
            item = self.surface_list.item(item_index)
            if item.checkState() != QtCore.Qt.CheckState.Checked:
                continue
            layer = self.surface_layers.get(item.text())
            if not layer or layer.gx.size < 2 or layer.gy.size < 2:
                continue
            ix = int(np.clip(np.searchsorted(layer.gx, x), 0, layer.gx.size - 1))
            iy = int(np.clip(np.searchsorted(layer.gy, y), 0, layer.gy.size - 1))
            value = layer.grid_z[iy, ix]
            if np.isfinite(value):
                parts.append(f"{layer.z_field} {float(value):,.3f}")
                break
        # Add nearest overlay XYZ when reasonably close to the cursor.
        best = None
        for layer in self.overlay_layers.values():
            if not layer.visible or layer.points.x.size == 0:
                continue
            step = max(1, layer.points.x.size // 10000)
            xs = layer.points.x[::step]; ys = layer.points.y[::step]
            d2 = (xs - x) ** 2 + (ys - y) ** 2
            index = int(np.argmin(d2))
            candidate = (float(d2[index]), layer, index * step)
            if best is None or candidate[0] < best[0]:
                best = candidate
        if best is not None:
            distance2, layer, index = best
            try:
                x_range = self.plot.getViewBox().viewRange()[0]
                hover_limit = abs(float(x_range[1]) - float(x_range[0])) * 0.03
            except Exception:
                hover_limit = float("inf")
            if distance2 <= hover_limit * hover_limit:
                parts.append(
                    f"{layer.name}: Z {float(layer.points.z[index]):,.3f}"
                )
                if layer.points.labels.size and layer.points.labels[index]:
                    parts.append(str(layer.points.labels[index]))
        message = "   |   ".join(parts)
        self.statusBar().showMessage(message)
        if self._hover_text:
            self._hover_text.setText(message)
            self._hover_text.setPos(x, y)
            self._hover_text.show()

    def _save_config(self) -> None:
        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {}
            if self.config_path.exists():
                try: payload = json.loads(self.config_path.read_text(encoding="utf-8"))
                except Exception: payload = {}
            payload["surface_2d"] = self.surface_definitions
            payload["overlay_2d"] = self.overlay_definitions
            self.config_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    def _select_combo_text(self, combo: QtWidgets.QComboBox, value: str) -> None:
        idx = combo.findText(str(value))
        if idx >= 0: combo.setCurrentIndex(idx)

    def _load_config(self) -> None:
        if not self.config_path.exists(): return
        try:
            payload = json.loads(self.config_path.read_text(encoding="utf-8"))
            self.surface_definitions = list(payload.get("surface_2d") or [])
            self.overlay_definitions = list(payload.get("overlay_2d") or [])
            self._restoring = True
            for d in self.surface_definitions:
                self._select_combo_text(self.surface_source, d.get("source", "")); self._surface_source_changed(self.surface_source.currentText())
                for combo,key in ((self.surface_x,"x"),(self.surface_y,"y"),(self.surface_z,"z"),(self.surface_display,"display"),(self.surface_method,"method"),(self.surface_cmap,"cmap")): self._select_combo_text(combo,d.get(key,""))
                idx=self.surface_line.findData(d.get("line")); self.surface_line.setCurrentIndex(idx if idx>=0 else 0)
                self.surface_resolution.setValue(int(d.get("resolution",350))); self.surface_contours.setValue(int(d.get("contours",12))); self.surface_opacity.setValue(float(d.get("opacity",.8))); self.surface_name.setText(d.get("name","")); self.add_surface()
            for d in self.overlay_definitions:
                self._select_combo_text(self.overlay_type, d.get("kind","Point")); self._select_combo_text(self.overlay_source,d.get("source","")); self._overlay_source_changed(self.overlay_source.currentText())
                for combo,key in ((self.overlay_x,"x"),(self.overlay_y,"y"),(self.overlay_z,"z"),(self.vector_end_x,"end_x"),(self.vector_end_y,"end_y"),(self.overlay_label_field,"label_field")): self._select_combo_text(combo,d.get(key,""))
                second_index = self.overlay_label_field2.findData(d.get("label_field2", ""))
                self.overlay_label_field2.setCurrentIndex(second_index if second_index >= 0 else 0)
                self.overlay_label_separator.setText(d.get("label_separator", " / "))
                self._select_combo_text(self.overlay_label_mode, d.get("label_mode", "When zoomed in"))
                self.overlay_label_zoom.setValue(float(d.get("label_zoom_span", 2000.0)))
                idx=self.overlay_line.findData(d.get("line")); self.overlay_line.setCurrentIndex(idx if idx>=0 else 0)
                self.overlay_color_value=d.get("color","#00e5ff"); self.overlay_color.setText(self.overlay_color_value); self._select_combo_text(self.overlay_style_mode,d.get("style_mode","Single symbol"));
                cat_idx=self.overlay_category_field.findData(d.get("category_field", "")); self.overlay_category_field.setCurrentIndex(cat_idx if cat_idx>=0 else 0);
                sym_idx=self.overlay_symbol.findData(d.get("symbol", "o")); self.overlay_symbol.setCurrentIndex(sym_idx if sym_idx>=0 else 0); self.overlay_size.setValue(float(d.get("size",7))); self.overlay_width.setValue(float(d.get("width",1.5))); self.overlay_labels.setChecked(bool(d.get("labels",False))); self.overlay_max_labels.setValue(int(d.get("max_labels",200))); self.overlay_name.setText(d.get("name","")); self.add_overlay()
            self._restoring = False
        except Exception:
            self._restoring = False

    def _surface_visibility_changed(self,item):
        layer=self.surface_layers.get(item.text()); visible=item.checkState()==QtCore.Qt.CheckState.Checked
        if layer:
            for obj in ([layer.image] if layer.image else [])+layer.contours: obj.setVisible(visible)
            if layer.colorbar is not None:
                try: layer.colorbar.setVisible(visible)
                except Exception: pass
    def _overlay_visibility_changed(self,item):
        layer=self.overlay_layers.get(item.text()); visible=item.checkState()==QtCore.Qt.CheckState.Checked
        if layer:
            layer.visible = visible
            for obj in layer.items: obj.setVisible(visible)
            self._update_label_visibility()
    def _remove_surface(self,name):
        layer=self.surface_layers.pop(name,None)
        if layer:
            for obj in ([layer.image] if layer.image else [])+layer.contours:
                try:self.plot.removeItem(obj)
                except Exception:pass
            if layer.colorbar is not None:
                try: layer.colorbar.close()
                except Exception: pass
    def _remove_overlay(self,name):
        layer=self.overlay_layers.pop(name,None)
        if layer:
            for obj in layer.items+layer.labels:
                try:self.plot.removeItem(obj)
                except Exception:pass
    def clear_surfaces(self):
        for name in list(self.surface_layers): self._remove_surface(name)
        self.surface_list.clear()
    def clear_overlays(self):
        for name in list(self.overlay_layers): self._remove_overlay(name)
        self.overlay_list.clear()
    def _surface_menu(self,pos):
        item=self.surface_list.itemAt(pos)
        if not item:return
        menu=QtWidgets.QMenu(self); zoom=menu.addAction("Zoom to surface"); delete=menu.addAction("Delete surface")
        action=menu.exec(self.surface_list.viewport().mapToGlobal(pos))
        if action is zoom:
            e=self.surface_layers[item.text()].extent; self.plot.setXRange(e[0],e[1]); self.plot.setYRange(e[2],e[3])
        elif action is delete:
            self._remove_surface(item.text()); self.surface_list.takeItem(self.surface_list.row(item))
