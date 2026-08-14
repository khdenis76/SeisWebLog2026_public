from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
from PySide6 import QtCore, QtGui, QtWidgets

from .surface_data import SurfaceDataRepository, SurfacePoints
from .surface_layer import ContourStyle

try:
    from scipy.interpolate import griddata
    SCIPY_AVAILABLE = True
except Exception:
    griddata = None
    SCIPY_AVAILABLE = False


@dataclass
class SurfaceBuildResult:
    name: str
    gx: np.ndarray
    gy: np.ndarray
    grid_z: np.ndarray
    display: str
    cmap: str
    opacity: float
    contour_levels: int
    contour_style: ContourStyle
    value_field: str
    definition: dict


def _interpolate_surface(
    points: SurfacePoints, resolution: int, method: str
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    n = max(2, int(resolution))
    gx = np.linspace(float(np.nanmin(points.x)), float(np.nanmax(points.x)), n)
    gy = np.linspace(float(np.nanmin(points.y)), float(np.nanmax(points.y)), n)
    xx, yy = np.meshgrid(gx, gy)
    method = str(method).strip().lower()
    if SCIPY_AVAILABLE and method in {"linear", "nearest"}:
        zz = griddata(np.c_[points.x, points.y], points.z, (xx, yy), method=method)
    else:
        zz = np.empty(xx.size, float)
        queries = np.c_[xx.ravel(), yy.ravel()]
        source = np.c_[points.x, points.y]
        for start in range(0, queries.shape[0], 2500):
            chunk = queries[start:start + 2500]
            distances = (
                (chunk[:, None, 0] - source[None, :, 0]) ** 2
                + (chunk[:, None, 1] - source[None, :, 1]) ** 2
            )
            if method == "nearest":
                indices = np.argmin(distances, axis=1)
                zz[start:start + len(chunk)] = points.z[indices]
            else:
                count = min(12, source.shape[0])
                indices = np.argpartition(distances, count - 1, axis=1)[:, :count]
                local = np.take_along_axis(distances, indices, axis=1)
                values = points.z[indices]
                weights = 1.0 / np.maximum(local, 1e-12)
                zz[start:start + len(chunk)] = (
                    np.sum(weights * values, axis=1) / np.sum(weights, axis=1)
                )
        zz = zz.reshape(xx.shape)
    return gx, gy, zz


def build_surface_from_definition(
    project_path: str | Path, definition: dict
) -> SurfaceBuildResult:
    """Rebuild a persisted main-map surface from its project data source."""
    definition = dict(definition)
    repo = SurfaceDataRepository(project_path)
    map_type = str(definition.get("map_type") or "Parameter surface")
    if map_type == "SPS point density":
        gx, gy, zz, density_info = repo.load_sps_density_grid(
            cell_size=float(definition.get("cell_size", 100.0)),
            density_type=str(definition.get("density_type") or "count").lower(),
            area_units=str(definition.get("density_units") or "cell"),
            search_radius=float(definition.get("search_radius") or 0.0) or None,
            cells_per_radius=3,
        )
        value_field = str(density_info["units"])
    else:
        source = str(definition.get("source") or "")
        points = repo.load_points(
            source,
            str(definition.get("x") or ""),
            str(definition.get("y") or ""),
            str(definition.get("z") or ""),
            line_filter=definition.get("line"),
            max_points=150000,
        )
        gx, gy, zz = _interpolate_surface(
            points,
            int(definition.get("resolution", 350)),
            str(definition.get("method") or "Linear"),
        )
        value_field = str(definition.get("z") or "Value")

    style = ContourStyle(
        color=str(definition.get("contour_color") or "#ffffff"),
        width=float(definition.get("contour_width", 1.0)),
        labels_enabled=bool(definition.get("labels", True)),
        label_suffix=str(definition.get("label_suffix") or ""),
        max_labels=int(definition.get("max_labels", 80)),
    )
    name = str(
        definition.get("registered_name")
        or definition.get("name")
        or "Saved surface"
    )
    return SurfaceBuildResult(
        name=name,
        gx=gx,
        gy=gy,
        grid_z=zz,
        display=str(definition.get("display") or "Heatmap + contours"),
        cmap=str(definition.get("cmap") or "turbo"),
        opacity=float(definition.get("opacity", 0.75)),
        contour_levels=int(definition.get("contour_levels", 12)),
        contour_style=style,
        value_field=value_field,
        definition=definition,
    )


class SurfaceCreateDialog(QtWidgets.QDialog):
    def __init__(self, project_path: str | Path, parent=None) -> None:
        super().__init__(parent)
        self.repo = SurfaceDataRepository(project_path)
        self.result_data: SurfaceBuildResult | None = None
        self.setWindowTitle("Add surface to main map")
        self.resize(470, 650)
        self._build_ui()
        self._load_sources()

    def _build_ui(self) -> None:
        layout = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()
        self.map_type = QtWidgets.QComboBox()
        self.map_type.addItems(["Parameter surface", "SPS point density"])
        self.source = QtWidgets.QComboBox()
        self.x = QtWidgets.QComboBox(); self.y = QtWidgets.QComboBox(); self.z = QtWidgets.QComboBox()
        self.line = QtWidgets.QComboBox(); self.line.addItem("All lines", None)
        self.display = QtWidgets.QComboBox(); self.display.addItems(["Heatmap + contours", "Heatmap", "Contours"])
        self.method = QtWidgets.QComboBox(); self.method.addItems(["Linear", "Nearest", "IDW"])
        self.density_type = QtWidgets.QComboBox(); self.density_type.addItems(["Count", "Gaussian"])
        self.cell_size = QtWidgets.QDoubleSpinBox(); self.cell_size.setRange(1.0, 10000.0); self.cell_size.setValue(100.0); self.cell_size.setSuffix(" m")
        self.search_radius = QtWidgets.QDoubleSpinBox(); self.search_radius.setRange(0.0, 50000.0); self.search_radius.setValue(300.0); self.search_radius.setSuffix(" m")
        self.density_units = QtWidgets.QComboBox(); self.density_units.addItem("Count per cell", "cell"); self.density_units.addItem("Count per km²", "sq_km"); self.density_units.addItem("Count per m²", "sq_m")
        self.resolution = QtWidgets.QSpinBox(); self.resolution.setRange(50, 1000); self.resolution.setValue(350)
        self.contour_levels = QtWidgets.QSpinBox(); self.contour_levels.setRange(2, 100); self.contour_levels.setValue(12)
        self.opacity = QtWidgets.QDoubleSpinBox(); self.opacity.setRange(0.05, 1.0); self.opacity.setValue(0.75); self.opacity.setSingleStep(0.05)
        self.cmap = QtWidgets.QComboBox(); self.cmap.addItems(["turbo", "viridis", "plasma", "inferno", "CET-L17"])
        self.name = QtWidgets.QLineEdit(); self.name.setPlaceholderText("Automatic name")
        self.contour_color = QtWidgets.QPushButton("#ffffff"); self._contour_color = "#ffffff"
        self.contour_width = QtWidgets.QDoubleSpinBox(); self.contour_width.setRange(0.2, 8.0); self.contour_width.setValue(1.0)
        self.labels = QtWidgets.QCheckBox("Show contour labels"); self.labels.setChecked(True)
        self.label_suffix = QtWidgets.QLineEdit(" m")
        self.max_labels = QtWidgets.QSpinBox(); self.max_labels.setRange(0, 500); self.max_labels.setValue(80)
        for label, widget in (
            ("Map type", self.map_type),
            ("Source", self.source), ("X field", self.x), ("Y field", self.y),
            ("Value field", self.z), ("Line filter", self.line), ("Display", self.display),
            ("Interpolation", self.method), ("Density type", self.density_type),
            ("Density cell size", self.cell_size), ("Search radius", self.search_radius),
            ("Density units", self.density_units), ("Grid resolution", self.resolution),
            ("Contour levels", self.contour_levels), ("Opacity", self.opacity),
            ("Color map", self.cmap), ("Contour color", self.contour_color),
            ("Contour width", self.contour_width), ("Contour label suffix", self.label_suffix),
            ("Maximum contour labels", self.max_labels), ("Layer name", self.name),
        ):
            form.addRow(label + ":", widget)
        form.addRow(self.labels)
        layout.addLayout(form)
        self.note = QtWidgets.QLabel()
        self.note.setWordWrap(True)
        layout.addWidget(self.note)
        buttons = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.StandardButton.Ok | QtWidgets.QDialogButtonBox.StandardButton.Cancel
        )
        buttons.button(QtWidgets.QDialogButtonBox.StandardButton.Ok).setText("Create on main map")
        buttons.accepted.connect(self._create)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.source.currentTextChanged.connect(self._source_changed)
        self.map_type.currentTextChanged.connect(self._map_type_changed)
        self.contour_color.clicked.connect(self._choose_contour_color)

    def _load_sources(self) -> None:
        sources = self.repo.available_sources()
        self.source.addItems(sources)
        if sources:
            self._source_changed(sources[0])
        self._map_type_changed(self.map_type.currentText())

    @staticmethod
    def _set_combo(combo: QtWidgets.QComboBox, values: list[str], selected: str = "") -> None:
        combo.clear(); combo.addItems(values)
        if selected:
            idx = combo.findText(selected)
            if idx >= 0:
                combo.setCurrentIndex(idx)

    def _source_changed(self, source: str) -> None:
        fields = self.repo.numeric_columns(source)
        x, y, z = self.repo.default_fields(source)
        self._set_combo(self.x, fields, x)
        self._set_combo(self.y, fields, y)
        self._set_combo(self.z, fields, z)
        self.line.clear(); self.line.addItem("All lines", None)
        for value in self.repo.line_values(source):
            self.line.addItem(str(value), value)
        self.note.setText(self.repo.source_note(source) or "")

    def _map_type_changed(self, value: str) -> None:
        density = value == "SPS point density"
        self.source.setEnabled(not density)
        self.x.setEnabled(not density); self.y.setEnabled(not density); self.z.setEnabled(not density)
        self.line.setEnabled(not density); self.method.setEnabled(not density)
        self.resolution.setEnabled(not density)
        for widget in (self.density_type, self.cell_size, self.search_radius, self.density_units):
            widget.setEnabled(density)
        if density:
            idx = self.source.findText("SPSolution")
            if idx >= 0:
                self.source.setCurrentIndex(idx)
            self.name.setPlaceholderText("SPS production density")
            self.note.setText(self.repo.source_note("SPSolution"))
        else:
            self.name.setPlaceholderText("Automatic name")
            self._source_changed(self.source.currentText())

    def _choose_contour_color(self) -> None:
        color = QtWidgets.QColorDialog.getColor(QtGui.QColor(self._contour_color), self)
        if color.isValid():
            self._contour_color = color.name()
            self.contour_color.setText(color.name())
            self.contour_color.setStyleSheet(f"background:{color.name()}")

    def _interpolate(self, points: SurfacePoints) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        return _interpolate_surface(
            points, int(self.resolution.value()), self.method.currentText()
        )

    def _create(self) -> None:
        try:
            source = self.source.currentText()
            if self.map_type.currentText() == "SPS point density":
                gx, gy, zz, density_info = self.repo.load_sps_density_grid(
                    cell_size=float(self.cell_size.value()),
                    density_type=self.density_type.currentText().lower(),
                    area_units=str(self.density_units.currentData()),
                    search_radius=float(self.search_radius.value()) or None,
                    cells_per_radius=3,
                )
                value_field = density_info["units"]
                name = self.name.text().strip() or "SPS production density"
            else:
                points = self.repo.load_points(
                    source, self.x.currentText(), self.y.currentText(), self.z.currentText(),
                    line_filter=self.line.currentData(), max_points=150000,
                )
                gx, gy, zz = self._interpolate(points)
                value_field = self.z.currentText()
                name = self.name.text().strip() or f"{source} — {self.z.currentText()}"
            style = ContourStyle(
                color=self._contour_color,
                width=float(self.contour_width.value()),
                labels_enabled=self.labels.isChecked(),
                label_suffix=self.label_suffix.text(),
                max_labels=int(self.max_labels.value()),
            )
            definition = {
                "name": name, "map_type": self.map_type.currentText(), "source": source,
                "x": self.x.currentText(), "y": self.y.currentText(),
                "z": self.z.currentText(), "line": self.line.currentData(), "display": self.display.currentText(),
                "method": self.method.currentText(), "resolution": int(self.resolution.value()),
                "contour_levels": int(self.contour_levels.value()), "opacity": float(self.opacity.value()),
                "cmap": self.cmap.currentText(), "contour_color": self._contour_color,
                "contour_width": float(self.contour_width.value()), "labels": self.labels.isChecked(),
                "label_suffix": self.label_suffix.text(), "max_labels": int(self.max_labels.value()),
                "density_type": self.density_type.currentText(), "cell_size": float(self.cell_size.value()),
                "search_radius": float(self.search_radius.value()), "density_units": self.density_units.currentData(),
            }
            self.result_data = SurfaceBuildResult(
                name, gx, gy, zz, self.display.currentText(), self.cmap.currentText(),
                float(self.opacity.value()), int(self.contour_levels.value()), style,
                value_field, definition,
            )
            self.accept()
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Create surface", str(exc))
