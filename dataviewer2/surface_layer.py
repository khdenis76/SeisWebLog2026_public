from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
import tempfile
from pathlib import Path

import numpy as np
import pyqtgraph as pg
from pyqtgraph.functions import isocurve
from PySide6 import QtCore, QtGui


@dataclass
class ContourStyle:
    color: str = "#ffffff"
    width: float = 1.0
    labels_enabled: bool = True
    label_color: str = "#ffffff"
    label_background: str = "#000000"
    label_opacity: int = 180
    label_suffix: str = ""
    max_labels: int = 80


class SurfaceMapLayer(QtCore.QObject):
    """Heatmap/contour layer rendered directly on the main map canvas."""

    def __init__(
        self,
        plot_item: pg.PlotItem,
        name: str,
        gx: np.ndarray,
        gy: np.ndarray,
        grid_z: np.ndarray,
        *,
        display: str = "Heatmap + contours",
        cmap: str = "turbo",
        opacity: float = 0.8,
        contour_levels: int = 12,
        contour_style: ContourStyle | None = None,
        value_field: str = "Value",
    ) -> None:
        super().__init__()
        self.plot_item = plot_item
        self.name = name
        self.gx = np.asarray(gx, dtype=float)
        self.gy = np.asarray(gy, dtype=float)
        self.grid_z = np.asarray(grid_z, dtype=float)
        self.display = display
        self.cmap_name = cmap
        self.opacity = float(opacity)
        self.contour_levels_count = int(contour_levels)
        self.contour_style = contour_style or ContourStyle()
        self.value_field = value_field
        self.visible = True
        self.loaded = True
        self._cache_path: Path | None = None
        self._cached_count = int(np.isfinite(self.grid_z).sum())
        self._cached_bounds = (
            float(np.nanmin(self.gx)), float(np.nanmax(self.gx)),
            float(np.nanmin(self.gy)), float(np.nanmax(self.gy)),
        ) if self.gx.size and self.gy.size else None
        self.image: pg.ImageItem | None = None
        self.contours: list[pg.PlotCurveItem] = []
        self.labels: list[pg.TextItem] = []
        self.colorbar = None
        self._build()

    @property
    def count(self) -> int:
        return int(np.isfinite(self.grid_z).sum()) if self.loaded else self._cached_count

    @property
    def bounds(self):
        if not self.loaded:
            return self._cached_bounds
        if self.gx.size == 0 or self.gy.size == 0:
            return None
        return (
            float(np.nanmin(self.gx)), float(np.nanmax(self.gx)),
            float(np.nanmin(self.gy)), float(np.nanmax(self.gy)),
        )

    def _transform(self) -> QtGui.QTransform:
        transform = QtGui.QTransform()
        transform.translate(float(self.gx[0]), float(self.gy[0]))
        sx = (float(self.gx[-1]) - float(self.gx[0])) / max(1, self.gx.size - 1)
        sy = (float(self.gy[-1]) - float(self.gy[0])) / max(1, self.gy.size - 1)
        transform.scale(sx, sy)
        return transform

    def _build(self) -> None:
        finite = self.grid_z[np.isfinite(self.grid_z)]
        if finite.size == 0:
            return
        zmin, zmax = float(np.nanmin(finite)), float(np.nanmax(finite))
        transform = self._transform()
        show_heatmap = "heatmap" in self.display.lower()
        show_contours = "contour" in self.display.lower()

        if show_heatmap:
            cmap = pg.colormap.get(self.cmap_name)
            lut = cmap.getLookupTable(0.0, 1.0, 256)
            image = pg.ImageItem(self.grid_z.T)
            image.setLookupTable(lut)
            image.setLevels((zmin, zmax))
            image.setOpacity(self.opacity)
            image.setTransform(transform)
            image.setZValue(-1000)
            self.plot_item.addItem(image)
            self.image = image
            try:
                colorbar = pg.ColorBarItem(
                    values=(zmin, zmax), colorMap=cmap,
                    label=self.value_field, interactive=False,
                    width=14,
                )
                colorbar.setImageItem(image, insert_in=self.plot_item)
                axis = colorbar.axis
                axis.setTextPen(pg.mkPen("#ffffff"))
                axis.setPen(pg.mkPen("#ffffff"))
                colorbar.setZValue(10000)
                self.colorbar = colorbar
            except Exception:
                self.colorbar = None

        if show_contours:
            if zmax == zmin:
                levels = np.array([zmin])
            else:
                levels = np.linspace(zmin, zmax, max(2, self.contour_levels_count + 2))[1:-1]
            label_budget = max(0, int(self.contour_style.max_labels))
            for level in levels:
                try:
                    paths = isocurve(self.grid_z.T, float(level), connected=True)
                except Exception:
                    paths = []
                for path in paths:
                    arr = np.asarray(path, dtype=float)
                    if arr.ndim != 2 or arr.shape[0] < 2:
                        continue
                    mapped = np.empty_like(arr)
                    mapped[:, 0] = self.gx[0] + arr[:, 0] * (self.gx[-1] - self.gx[0]) / max(1, self.gx.size - 1)
                    mapped[:, 1] = self.gy[0] + arr[:, 1] * (self.gy[-1] - self.gy[0]) / max(1, self.gy.size - 1)
                    curve = pg.PlotCurveItem(
                        mapped[:, 0], mapped[:, 1],
                        pen=pg.mkPen(self.contour_style.color, width=self.contour_style.width),
                        antialias=True,
                    )
                    curve.setZValue(-900)
                    self.plot_item.addItem(curve)
                    self.contours.append(curve)
                    if self.contour_style.labels_enabled and label_budget > 0 and arr.shape[0] >= 8:
                        idx = arr.shape[0] // 2
                        p = mapped[idx]
                        p0 = mapped[max(0, idx - 2)]
                        p1 = mapped[min(mapped.shape[0] - 1, idx + 2)]
                        angle = float(np.degrees(np.arctan2(p1[1] - p0[1], p1[0] - p0[0])))
                        text = f"{float(level):.1f}{self.contour_style.label_suffix}"
                        bg = QtGui.QColor(self.contour_style.label_background)
                        bg.setAlpha(max(0, min(255, self.contour_style.label_opacity)))
                        label = pg.TextItem(
                            text,
                            color=self.contour_style.label_color,
                            anchor=(0.5, 0.5),
                            fill=pg.mkBrush(bg),
                            border=pg.mkPen(255, 255, 255, 50),
                        )
                        label.setPos(float(p[0]), float(p[1]))
                        label.setAngle(angle)
                        label.setZValue(9000)
                        self.plot_item.addItem(label)
                        self.labels.append(label)
                        label_budget -= 1

    def set_visible(self, visible: bool) -> None:
        if visible and not self.loaded:
            self.reload()
        self.visible = bool(visible)
        if self.image is not None:
            self.image.setVisible(self.visible)
        for item in self.contours:
            item.setVisible(self.visible)
        for item in self.labels:
            item.setVisible(self.visible)
        if self.colorbar is not None:
            try:
                self.colorbar.setVisible(self.visible)
            except Exception:
                pass


    def _clear_graphics(self) -> None:
        if self.image is not None:
            try:
                self.plot_item.removeItem(self.image)
            except Exception:
                pass
            self.image = None
        for item in self.contours:
            try:
                self.plot_item.removeItem(item)
            except Exception:
                pass
        self.contours.clear()
        for item in self.labels:
            try:
                self.plot_item.removeItem(item)
            except Exception:
                pass
        self.labels.clear()
        if self.colorbar is not None:
            try:
                self.colorbar.close()
            except Exception:
                pass
            self.colorbar = None

    def unload(self) -> bool:
        if not self.loaded:
            return False
        if self._cache_path is None:
            handle = tempfile.NamedTemporaryFile(prefix="seis_dv_surface_", suffix=".npz", delete=False)
            handle.close()
            self._cache_path = Path(handle.name)
        np.savez(self._cache_path, gx=self.gx, gy=self.gy, grid_z=self.grid_z)
        self._cached_count = int(np.isfinite(self.grid_z).sum())
        self._cached_bounds = self.bounds
        self._clear_graphics()
        self.gx = np.array([], dtype=float)
        self.gy = np.array([], dtype=float)
        self.grid_z = np.empty((0, 0), dtype=float)
        self.loaded = False
        return True

    def reload(self) -> bool:
        if self.loaded:
            return True
        if self._cache_path is None or not self._cache_path.exists():
            return False
        with np.load(self._cache_path, allow_pickle=False) as archive:
            self.gx = archive["gx"]
            self.gy = archive["gy"]
            self.grid_z = archive["grid_z"]
        self.loaded = True
        self._build()
        self.set_visible(self.visible)
        return True

    def set_z_value(self, z: float) -> None:
        if self.image is not None:
            self.image.setZValue(float(z))
        for item in self.contours:
            item.setZValue(float(z) + 0.1)
        for item in self.labels:
            item.setZValue(float(z) + 100.0)

    def refresh_view(self) -> None:
        """Refresh layer-dependent display after map pan or zoom.

        The surface geometry is already stored in map coordinates, so there is
        nothing to rebuild here. This method exists to satisfy the common layer
        interface used by MainWindow.
        """
        return

    def remove(self) -> None:
        self._clear_graphics()
        if self._cache_path is not None:
            try:
                self._cache_path.unlink(missing_ok=True)
            except Exception:
                pass
