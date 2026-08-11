from __future__ import annotations

import numpy as np
import tempfile
from pathlib import Path
import pyqtgraph as pg
from PySide6 import QtCore, QtGui, QtWidgets

from .models import PointLayerData


def _text_symbol(text: str) -> QtGui.QPainterPath:
    """Create a centered vector symbol containing one or two letters."""
    path = QtGui.QPainterPath()
    font = QtGui.QFont("Arial")
    font.setBold(True)
    font.setPointSizeF(10.0)
    path.addText(0.0, 0.0, font, str(text))
    rect = path.boundingRect()
    transform = QtGui.QTransform()
    scale = 1.0 / max(rect.width(), rect.height(), 1.0)
    transform.scale(scale, -scale)
    transform.translate(-rect.center().x(), -rect.center().y())
    return transform.map(path)


class FastPointLayer(QtCore.QObject):
    selection_changed = QtCore.Signal(str, int)

    def __init__(self, plot_item: pg.PlotItem, name: str, point_color: str, line_color: str | None = None, connect_by: str | None = None) -> None:
        super().__init__()
        self.plot_item = plot_item
        self.name = name
        self.data: PointLayerData | None = None
        self.loaded = True
        self._cache_path: Path | None = None
        self._cached_count = 0
        self._cached_bounds = None
        self.symbol_name = "circle"
        self.marker_text = ""
        self.connect_by = connect_by
        self.visible = True
        self.max_visible_points = 30000
        self.show_points_below = 50000
        self.point_color = str(point_color)
        self.line_color = str(line_color or point_color)
        self.line_width = 1.0
        self.line_style = "solid"
        self.point_size = 5.0
        self.curve = pg.PlotCurveItem(
            pen=pg.mkPen(QtGui.QColor(self.line_color), width=self.line_width),
            connect="finite",
            antialias=False,
        )
        self.scatter = pg.ScatterPlotItem(
            size=self.point_size,
            pxMode=True,
            brush=pg.mkBrush(QtGui.QColor(self.point_color)),
            pen=None,
            symbol="o",
            useCache=True,
        )
        self.selection_scatter = pg.ScatterPlotItem(
            size=max(9.0, self.point_size + 5.0),
            pxMode=True,
            brush=pg.mkBrush(QtGui.QColor("#ffd740")),
            pen=pg.mkPen(QtGui.QColor("#d32f2f"), width=1.5),
            symbol="o",
            useCache=True,
        )
        self.selected_indices = np.empty(0, dtype=np.int64)
        self.scatter.sigClicked.connect(self._clicked)
        self.display_indices = np.array([], dtype=np.int64)
        plot_item.addItem(self.curve)
        plot_item.addItem(self.scatter)
        plot_item.addItem(self.selection_scatter)

    def set_data(self, data: PointLayerData) -> None:
        self.data = data
        self.loaded = True
        self._cached_count = data.count
        self._cached_bounds = data.bounds
        self._set_full_curve()
        self.refresh_view()

    def _set_full_curve(self) -> None:
        if not self.data or self.data.count == 0:
            self.curve.setData([], [])
            return
        if not self.connect_by or self.connect_by not in self.data.metadata:
            self.curve.setData([], [])
            return
        group = self.data.metadata[self.connect_by]
        breaks = np.flatnonzero(group[1:] != group[:-1]) + 1 if group.size > 1 else np.array([], dtype=int)
        x = np.insert(self.data.x, breaks, np.nan) if breaks.size else self.data.x
        y = np.insert(self.data.y, breaks, np.nan) if breaks.size else self.data.y
        self.curve.setData(x=x, y=y, connect="finite", skipFiniteCheck=True)

    def refresh_view(self) -> None:
        loaded = getattr(self, "loaded", True)

        if (
                not self.visible
                or not loaded
                or self.data is None
                or self.data.count == 0
        ):
            self.display_indices = np.empty(0, dtype=np.int64)
            self.scatter.setData([], [])
            self.scatter.setVisible(False)
            return

        # The layer is loaded and visible.
        self.scatter.setVisible(True)

        (x_min, x_max), (y_min, y_max) = self.plot_item.vb.viewRange()

        mask = (
                (self.data.x >= x_min)
                & (self.data.x <= x_max)
                & (self.data.y >= y_min)
                & (self.data.y <= y_max)
        )

        indices = np.flatnonzero(mask)

        if indices.size > self.max_visible_points:
            step = int(np.ceil(indices.size / self.max_visible_points))
            indices = indices[::step]

        self.display_indices = indices.astype(np.int64, copy=False)

        # At a wide zoom level, hide individual point symbols.
        if indices.size > self.show_points_below:
            self.scatter.setData([], [])
            return

        self.scatter.setData(
            x=self.data.x[indices],
            y=self.data.y[indices],
            data=np.arange(indices.size, dtype=np.int64),
        )

    def set_visible(self, visible: bool) -> None:
        if visible and not self.loaded:
            self.reload()
        self.visible = visible
        self.curve.setVisible(visible)
        self.scatter.setVisible(visible)
        self.selection_scatter.setVisible(visible and self.selected_indices.size > 0)
        if visible:
            self.refresh_view()

    def set_selected_indices(self, indices: np.ndarray | list[int]) -> None:
        """Store and highlight a multi-node selection using full data indices."""
        if self.data is None:
            self.selected_indices = np.empty(0, dtype=np.int64)
            self.selection_scatter.setData([], [])
            return
        values = np.asarray(indices, dtype=np.int64).reshape(-1)
        values = values[(values >= 0) & (values < self.data.count)]
        self.selected_indices = np.unique(values)
        if self.selected_indices.size:
            self.selection_scatter.setData(
                x=self.data.x[self.selected_indices],
                y=self.data.y[self.selected_indices],
            )
        else:
            self.selection_scatter.setData([], [])
        self.selection_scatter.setVisible(self.visible and self.selected_indices.size > 0)

    def clear_selection(self) -> None:
        self.set_selected_indices([])

    def nearest(self, x: float, y: float, tolerance: float) -> tuple[int, float] | None:
        if not self.visible or not self.data or self.display_indices.size == 0:
            return None
        idx = self.display_indices
        d2 = (self.data.x[idx] - x) ** 2 + (self.data.y[idx] - y) ** 2
        local = int(np.argmin(d2))
        distance = float(np.sqrt(d2[local]))
        return (int(idx[local]), distance) if distance <= tolerance else None

    def _clicked(self, _scatter: pg.ScatterPlotItem, points, _event: object) -> None:
        if self.data is None:
            return
        if points is None or len(points) == 0:
            return
        local_index = int(points[0].data())
        if 0 <= local_index < self.display_indices.size:
            self.selection_changed.emit(self.name, int(self.display_indices[local_index]))

    def update_style(
        self,
        *,
        point_color: str | None = None,
        line_color: str | None = None,
        line_width: float | None = None,
        point_size: float | None = None,
        symbol: str | None = None,
        marker_text: str | None = None,
        line_style: str | None = None,
    ) -> None:
        """Update point/track styling without rebuilding the layer data."""
        if point_color is not None:
            self.point_color = str(point_color)
        if line_color is not None:
            self.line_color = str(line_color)
        if line_width is not None:
            self.line_width = max(0.2, float(line_width))
        if point_size is not None:
            self.point_size = max(1.0, float(point_size))
        if symbol is not None:
            self.symbol_name = str(symbol)
        if marker_text is not None:
            self.marker_text = str(marker_text).strip()[:2]
        if line_style is not None:
            self.line_style = str(line_style).strip().lower()

        pen = pg.mkPen(QtGui.QColor(self.line_color), width=self.line_width)
        pen.setStyle({
            "solid": QtCore.Qt.PenStyle.SolidLine,
            "dash": QtCore.Qt.PenStyle.DashLine,
            "dot": QtCore.Qt.PenStyle.DotLine,
            "dash dot": QtCore.Qt.PenStyle.DashDotLine,
            "dash-dot": QtCore.Qt.PenStyle.DashDotLine,
            "dash dot dot": QtCore.Qt.PenStyle.DashDotDotLine,
            "dash-dot-dot": QtCore.Qt.PenStyle.DashDotDotLine,
            "none": QtCore.Qt.PenStyle.NoPen,
        }.get(self.line_style, QtCore.Qt.PenStyle.SolidLine))
        self.curve.setPen(pen)
        self.scatter.setBrush(pg.mkBrush(QtGui.QColor(self.point_color)))
        self.scatter.setSize(self.point_size)
        symbol_map = {
            "circle": "o", "square": "s", "triangle": "t",
            "triangle down": "t1", "diamond": "d", "plus": "+",
            "cross": "x", "star": "star", "pentagon": "p", "hexagon": "h",
        }
        if self.marker_text:
            self.scatter.setSymbol(_text_symbol(self.marker_text))
        else:
            self.scatter.setSymbol(symbol_map.get(self.symbol_name.lower(), "o"))

    @property
    def bounds(self):
        return self.data.bounds if self.data is not None else self._cached_bounds

    @property
    def count(self) -> int:
        return self.data.count if self.data is not None else int(self._cached_count)

    def unload(self) -> bool:
        """Release point arrays and graphics while preserving a disk cache."""
        if not self.loaded or self.data is None:
            return False
        if self._cache_path is None:
            handle = tempfile.NamedTemporaryFile(prefix="seis_dv_layer_", suffix=".npz", delete=False)
            handle.close()
            self._cache_path = Path(handle.name)
        payload = {"x": self.data.x, "y": self.data.y, "source_index": self.data.source_index}
        for key, values in self.data.metadata.items():
            payload[f"meta__{key}"] = values
        np.savez(self._cache_path, **payload)
        self._cached_count = self.data.count
        self._cached_bounds = self.data.bounds
        self.data = None
        self.loaded = False
        self.display_indices = np.array([], dtype=np.int64)
        self.curve.setData([], [])
        self.scatter.setData([], [])
        self.selected_indices = np.empty(0, dtype=np.int64)
        self.selection_scatter.setData([], [])
        return True

    def reload(self) -> bool:
        if self.loaded:
            return True
        if self._cache_path is None or not self._cache_path.exists():
            return False
        with np.load(self._cache_path, allow_pickle=True) as archive:
            metadata = {
                key[6:]: archive[key]
                for key in archive.files if key.startswith("meta__")
            }
            data = PointLayerData(
                name=self.name, x=archive["x"], y=archive["y"],
                source_index=archive["source_index"], metadata=metadata,
            )
        self.set_data(data)
        self.set_visible(self.visible)
        return True

    def set_z_value(self, z: float) -> None:
        self.curve.setZValue(z)
        self.scatter.setZValue(z + 0.1)
        self.selection_scatter.setZValue(z + 0.2)

    def remove(self) -> None:
        """Remove this layer's graphics items from the plot."""
        self.plot_item.removeItem(self.curve)
        self.plot_item.removeItem(self.scatter)
        self.plot_item.removeItem(self.selection_scatter)
        if self._cache_path is not None:
            try:
                self._cache_path.unlink(missing_ok=True)
            except Exception:
                pass


class FastShapeLayer(QtCore.QObject):
    """One graphics layer for one database-registered shapefile."""

    def __init__(self, plot_item: pg.PlotItem, data, style_override: dict | None = None) -> None:
        super().__init__()
        self.plot_item = plot_item
        self.data = data
        self.name = data.name
        self.visible = True
        self.loaded = True
        self.fill_item: QtWidgets.QGraphicsPathItem | None = None

        # Database styles are authoritative. Black is retained when the
        # project_shapes table explicitly specifies black.
        default_line = data.definition.line_color or "#00e5ff"
        default_fill = data.definition.fill_color or default_line

        override = style_override or {}
        self.outline_color = str(override.get("outline_color", default_line))
        self.outline_width = float(override.get("outline_width", data.definition.line_width or 1.5))
        self.outline_style = str(override.get("outline_style", data.definition.line_style or "solid"))
        self.fill_enabled = bool(override.get("fill_enabled", data.definition.is_filled or data.geometry_type == "polygon"))
        self.fill_color = str(override.get("fill_color", default_fill))
        self.fill_opacity = int(override.get("fill_opacity", 120 if data.definition.is_filled else 0))
        self.hatch_pattern = str(override.get("hatch_pattern", data.definition.hatch_pattern or ""))
        self.layer_opacity = float(override.get("layer_opacity", 1.0))
        self.point_size = float(override.get("point_size", max(5.0, self.outline_width + 4.0)))

        self.curve = pg.PlotCurveItem(connect="finite", antialias=False)
        self.scatter = pg.ScatterPlotItem(pxMode=True, useCache=True)
        self._build_geometry()
        plot_item.addItem(self.curve)
        plot_item.addItem(self.scatter)
        self.update_style()

    @staticmethod
    def _qt_pen_style(text: str) -> QtCore.Qt.PenStyle:
        text = (text or "").strip().lower().replace("_", "-")
        if "dash" in text and "dot" in text:
            return QtCore.Qt.PenStyle.DashDotLine
        if "dash" in text:
            return QtCore.Qt.PenStyle.DashLine
        if "dot" in text:
            return QtCore.Qt.PenStyle.DotLine
        return QtCore.Qt.PenStyle.SolidLine

    @staticmethod
    def _qt_brush_style(text: str) -> QtCore.Qt.BrushStyle:
        value = (text or "").strip().lower().replace("_", " ")
        if not value or value in {"solid", "solidpattern"}:
            return QtCore.Qt.BrushStyle.SolidPattern
        if "cross" in value and ("diag" in value or "diagonal" in value):
            return QtCore.Qt.BrushStyle.DiagCrossPattern
        if "cross" in value:
            return QtCore.Qt.BrushStyle.CrossPattern
        if "back" in value or "bdiag" in value or "\\" in value:
            return QtCore.Qt.BrushStyle.BDiagPattern
        if "forward" in value or "fdiag" in value or "/" in value:
            return QtCore.Qt.BrushStyle.FDiagPattern
        if "horizontal" in value or value in {"hor", "h"}:
            return QtCore.Qt.BrushStyle.HorPattern
        if "vertical" in value or value in {"ver", "v"}:
            return QtCore.Qt.BrushStyle.VerPattern
        if "dense1" in value:
            return QtCore.Qt.BrushStyle.Dense1Pattern
        if "dense2" in value:
            return QtCore.Qt.BrushStyle.Dense2Pattern
        if "dense3" in value:
            return QtCore.Qt.BrushStyle.Dense3Pattern
        if "dense4" in value:
            return QtCore.Qt.BrushStyle.Dense4Pattern
        return QtCore.Qt.BrushStyle.SolidPattern

    def _build_geometry(self) -> None:
        if self.data.geometry_type == "point":
            if self.data.points.size:
                self.scatter.setData(x=self.data.points[:, 0], y=self.data.points[:, 1])
            self.curve.setData([], [])
            return

        xs: list[float] = []
        ys: list[float] = []
        for part in self.data.parts:
            if not part.size:
                continue
            xs.extend(part[:, 0].tolist())
            ys.extend(part[:, 1].tolist())
            xs.append(float("nan")); ys.append(float("nan"))
        self.curve.setData(
            x=np.asarray(xs, dtype=np.float64),
            y=np.asarray(ys, dtype=np.float64),
            connect="finite",
            skipFiniteCheck=True,
        )
        self.scatter.setData([], [])

        if self.data.geometry_type == "polygon":
            path = QtGui.QPainterPath()
            path.setFillRule(QtCore.Qt.FillRule.OddEvenFill)
            for part in self.data.parts:
                if part.shape[0] < 3:
                    continue
                path.moveTo(float(part[0, 0]), float(part[0, 1]))
                for point in part[1:]:
                    path.lineTo(float(point[0]), float(point[1]))
                path.closeSubpath()
            self.fill_item = QtWidgets.QGraphicsPathItem(path)
            self.plot_item.addItem(self.fill_item)

    def style_dict(self) -> dict:
        return {
            "outline_color": self.outline_color,
            "outline_width": self.outline_width,
            "outline_style": self.outline_style,
            "fill_enabled": self.fill_enabled,
            "fill_color": self.fill_color,
            "fill_opacity": self.fill_opacity,
            "layer_opacity": self.layer_opacity,
            "point_size": self.point_size,
            "hatch_pattern": self.hatch_pattern,
        }

    def update_style(self, **changes) -> None:
        for key, value in changes.items():
            if hasattr(self, key):
                setattr(self, key, value)
        pen = pg.mkPen(
            QtGui.QColor(self.outline_color),
            width=max(0.2, float(self.outline_width)),
            style=self._qt_pen_style(self.outline_style),
        )
        self.curve.setPen(pen)
        self.scatter.setPen(pen)
        self.scatter.setBrush(pg.mkBrush(QtGui.QColor(self.fill_color)))
        self.scatter.setSize(max(1.0, float(self.point_size)))
        self.curve.setOpacity(max(0.0, min(1.0, float(self.layer_opacity))))
        self.scatter.setOpacity(max(0.0, min(1.0, float(self.layer_opacity))))
        if self.fill_item is not None:
            color = QtGui.QColor(self.fill_color)
            color.setAlpha(max(0, min(255, int(self.fill_opacity))))
            if self.fill_enabled:
                brush = QtGui.QBrush(color)
                brush.setStyle(self._qt_brush_style(self.hatch_pattern))
                self.fill_item.setBrush(brush)
            else:
                self.fill_item.setBrush(QtGui.QBrush(QtCore.Qt.BrushStyle.NoBrush))
            self.fill_item.setPen(QtGui.QPen(QtCore.Qt.PenStyle.NoPen))
            self.fill_item.setOpacity(max(0.0, min(1.0, float(self.layer_opacity))))

    def refresh_view(self) -> None:
        return

    def set_visible(self, visible: bool) -> None:
        """Show or hide every graphics item owned by this shape layer."""
        self.visible = bool(visible)

        effective_visible = self.visible and getattr(self, "loaded", True)

        items = [
            getattr(self, "curve", None),
            getattr(self, "scatter", None),
            getattr(self, "fill_item", None),
        ]

        for item in items:
            if item is None:
                continue

            item.setVisible(effective_visible)
            item.setEnabled(effective_visible)

        # Force PyQtGraph/Qt to repaint the canvas.
        try:
            self.plot_item.update()
            self.plot_item.scene().update()
        except Exception:
            pass

    def nearest(self, x: float, y: float, tolerance: float) -> tuple[int, float] | None:
        if not self.visible:
            return None
        arrays = [self.data.points] if self.data.geometry_type == "point" else self.data.parts
        best_distance: float | None = None
        best_index = 0
        running_index = 0
        for array in arrays:
            if not array.size:
                continue
            d2 = (array[:, 0] - x) ** 2 + (array[:, 1] - y) ** 2
            local = int(np.argmin(d2)); distance = float(np.sqrt(d2[local]))
            if best_distance is None or distance < best_distance:
                best_distance = distance; best_index = running_index + local
            running_index += int(array.shape[0])
        if best_distance is None or best_distance > tolerance:
            return None
        return best_index, best_distance

    def vertex(self, index: int) -> tuple[float, float]:
        if self.data.geometry_type == "point":
            point = self.data.points[index]
            return float(point[0]), float(point[1])
        offset = 0
        for part in self.data.parts:
            if index < offset + part.shape[0]:
                point = part[index - offset]
                return float(point[0]), float(point[1])
            offset += int(part.shape[0])
        raise IndexError(index)

    @property
    def bounds(self):
        return self.data.bounds

    @property
    def count(self) -> int:
        return self.data.count

    def set_z_value(self, z: float) -> None:
        self.curve.setZValue(z)
        self.scatter.setZValue(z + 0.1)
        if self.fill_item is not None:
            self.fill_item.setZValue(z - 0.1)

    def remove(self) -> None:
        self.plot_item.removeItem(self.curve)
        self.plot_item.removeItem(self.scatter)
        if self.fill_item is not None:
            self.plot_item.removeItem(self.fill_item)
