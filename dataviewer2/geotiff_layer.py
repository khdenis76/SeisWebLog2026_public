from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore, QtGui


@dataclass(slots=True)
class GeoTiffDisplayOptions:
    mode: str = "color + contours"
    cmap: str = "terrain"
    opacity: float = 0.85
    contour_interval: float = 5.0
    contour_color: str = "#202020"
    contour_width: float = 1.0
    contour_style: str = "solid"


def _qt_pen_style(name: str) -> QtCore.Qt.PenStyle:
    return {
        "solid": QtCore.Qt.PenStyle.SolidLine,
        "dash": QtCore.Qt.PenStyle.DashLine,
        "dot": QtCore.Qt.PenStyle.DotLine,
        "dash dot": QtCore.Qt.PenStyle.DashDotLine,
        "dash-dot": QtCore.Qt.PenStyle.DashDotLine,
        "dash dot dot": QtCore.Qt.PenStyle.DashDotDotLine,
        "dash-dot-dot": QtCore.Qt.PenStyle.DashDotDotLine,
    }.get(str(name).strip().lower(), QtCore.Qt.PenStyle.SolidLine)


class GeoTiffLayer:
    """Map layer rendering a single-band GeoTIFF as an image and/or contours."""

    def __init__(self, plot_item: pg.PlotItem, path: str | Path, target_epsg: str, options: GeoTiffDisplayOptions) -> None:
        self.plot_item = plot_item
        self.path = Path(path)
        self.name = self.path.stem
        self.options = options
        self.visible = True
        self.loaded = False
        self.image_item: pg.ImageItem | None = None
        self.contour_items: list[pg.PlotCurveItem] = []
        self._bounds: tuple[float, float, float, float] | None = None
        self._load(target_epsg)

    @property
    def bounds(self):
        return self._bounds

    @property
    def count(self) -> int:
        return 1

    def _load(self, target_epsg: str) -> None:
        try:
            import rasterio
            from rasterio.enums import Resampling
            from rasterio.warp import calculate_default_transform, reproject
        except ImportError as exc:
            raise RuntimeError("GeoTIFF support requires rasterio: python -m pip install rasterio") from exc

        with rasterio.open(self.path) as src:
            data = src.read(1, masked=True).astype(np.float32)
            transform = src.transform
            crs = src.crs
            nodata = src.nodata
            target_crs = f"EPSG:{target_epsg}" if target_epsg and not str(target_epsg).upper().startswith("EPSG:") else target_epsg
            if target_crs and crs and str(crs).upper() != str(target_crs).upper():
                dst_transform, width, height = calculate_default_transform(crs, target_crs, src.width, src.height, *src.bounds)
                # Limit very large display rasters while preserving their geographic extent.
                max_side = 3000
                scale = max(width / max_side, height / max_side, 1.0)
                width = max(1, int(width / scale)); height = max(1, int(height / scale))
                dst_transform = dst_transform * dst_transform.scale(scale, scale)
                dst = np.full((height, width), np.nan, dtype=np.float32)
                reproject(
                    source=np.asarray(data.filled(np.nan)), destination=dst,
                    src_transform=transform, src_crs=crs,
                    dst_transform=dst_transform, dst_crs=target_crs,
                    src_nodata=nodata, dst_nodata=np.nan,
                    resampling=Resampling.bilinear,
                )
                data, transform = np.ma.masked_invalid(dst), dst_transform
            else:
                max_side = 3000
                step = max(1, int(np.ceil(max(src.width, src.height) / max_side)))
                if step > 1:
                    data = src.read(1, out_shape=(max(1, src.height // step), max(1, src.width // step)), masked=True, resampling=Resampling.bilinear).astype(np.float32)
                    transform = src.transform * src.transform.scale(src.width / data.shape[1], src.height / data.shape[0])

        array = np.asarray(data.filled(np.nan), dtype=np.float32)
        finite = np.isfinite(array)
        if not finite.any():
            raise RuntimeError(f"GeoTIFF contains no finite data: {self.path}")
        rows, cols = array.shape
        left = float(transform.c); top = float(transform.f)
        right = left + float(transform.a) * cols
        bottom = top + float(transform.e) * rows
        xmin, xmax = sorted((left, right)); ymin, ymax = sorted((bottom, top))
        self._bounds = (xmin, xmax, ymin, ymax)

        mode = self.options.mode.lower()
        if "color" in mode:
            try:
                from matplotlib import colormaps
                cmap = colormaps.get_cmap(self.options.cmap)
                lo, hi = np.nanpercentile(array, [2, 98])
                if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
                    lo, hi = float(np.nanmin(array)), float(np.nanmax(array))
                norm = np.clip((array - lo) / max(hi - lo, 1e-12), 0.0, 1.0)
                rgba = (cmap(norm) * 255).astype(np.uint8)
                rgba[~finite, 3] = 0
            except Exception:
                norm = np.nan_to_num((array - np.nanmin(array)) / max(np.nanmax(array) - np.nanmin(array), 1e-12))
                gray = (norm * 255).astype(np.uint8)
                rgba = np.dstack((gray, gray, gray, np.where(finite, 255, 0).astype(np.uint8)))
            # ImageItem uses bottom-left map transform after vertically flipping raster rows.
            self.image_item = pg.ImageItem(np.flipud(rgba), axisOrder="row-major")
            self.image_item.setOpacity(float(np.clip(self.options.opacity, 0.0, 1.0)))
            tr = QtGui.QTransform()
            tr.translate(xmin, ymin)
            tr.scale((xmax - xmin) / cols, (ymax - ymin) / rows)
            self.image_item.setTransform(tr)
            self.plot_item.addItem(self.image_item)

        if "contour" in mode and self.options.contour_interval > 0:
            try:
                import matplotlib.pyplot as plt
                x = np.linspace(xmin, xmax, cols)
                y = np.linspace(ymax, ymin, rows)
                lo = np.ceil(np.nanmin(array) / self.options.contour_interval) * self.options.contour_interval
                hi = np.floor(np.nanmax(array) / self.options.contour_interval) * self.options.contour_interval
                levels = np.arange(lo, hi + self.options.contour_interval * 0.5, self.options.contour_interval)
                if levels.size > 400:
                    levels = levels[::int(np.ceil(levels.size / 400))]
                fig, ax = plt.subplots()
                cs = ax.contour(x, y, array, levels=levels)
                pen = pg.mkPen(QtGui.QColor(self.options.contour_color), width=self.options.contour_width)
                pen.setStyle(_qt_pen_style(self.options.contour_style))
                for segments in cs.allsegs:
                    for vertices in segments:
                        if len(vertices) < 2:
                            continue
                        item = pg.PlotCurveItem(vertices[:, 0], vertices[:, 1], pen=pen, antialias=False)
                        self.plot_item.addItem(item); self.contour_items.append(item)
                plt.close(fig)
            except Exception as exc:
                self.remove()
                raise RuntimeError(f"Could not create GeoTIFF contours: {exc}") from exc
        self.loaded = True

    def nearest(self, x: float, y: float, tolerance: float):
        """GeoTIFF pixels are not snapping vertices for manual measurement."""
        return None

    def vertex(self, index: int) -> tuple[float, float]:
        """GeoTIFF layers do not expose vector vertices."""
        raise IndexError("GeoTIFF layers do not contain selectable vertices")

    def set_visible(self, visible: bool) -> None:
        self.visible = bool(visible)
        if self.image_item is not None:
            self.image_item.setVisible(self.visible)
        for item in self.contour_items:
            item.setVisible(self.visible)

    def set_z_value(self, value: float) -> None:
        if self.image_item is not None:
            self.image_item.setZValue(value - 1000.0)
        for item in self.contour_items:
            item.setZValue(value)

    def remove(self) -> None:
        if self.image_item is not None:
            self.plot_item.removeItem(self.image_item); self.image_item = None
        for item in self.contour_items:
            self.plot_item.removeItem(item)
        self.contour_items.clear()
