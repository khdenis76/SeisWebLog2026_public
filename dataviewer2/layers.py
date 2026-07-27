from __future__ import annotations

import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore, QtGui, QtWidgets

from .models import PointLayerData


class FastPointLayer(QtCore.QObject):
    selection_changed = QtCore.Signal(str, int)

    def __init__(self, plot_item: pg.PlotItem, name: str, point_color: str, line_color: str | None = None, connect_by: str | None = None) -> None:
        super().__init__()
        self.plot_item = plot_item
        self.name = name
        self.data: PointLayerData | None = None
        self.connect_by = connect_by
        self.visible = True
        self.max_visible_points = 30000
        self.show_points_below = 50000
        self.point_color = str(point_color)
        self.line_color = str(line_color or point_color)
        self.line_width = 1.0
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
            useCache=True,
        )
        self.scatter.sigClicked.connect(self._clicked)
        self.display_indices = np.array([], dtype=np.int64)
        plot_item.addItem(self.curve)
        plot_item.addItem(self.scatter)

    def set_data(self, data: PointLayerData) -> None:
        self.data = data
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
        if not self.visible or not self.data or self.data.count == 0:
            self.scatter.setData([], [])
            return
        (x_min, x_max), (y_min, y_max) = self.plot_item.vb.viewRange()
        mask = (self.data.x >= x_min) & (self.data.x <= x_max) & (self.data.y >= y_min) & (self.data.y <= y_max)
        indices = np.flatnonzero(mask)
        if indices.size > self.max_visible_points:
            step = int(np.ceil(indices.size / self.max_visible_points))
            indices = indices[::step]
        self.display_indices = indices.astype(np.int64, copy=False)
        if mask.sum() > self.show_points_below:
            self.scatter.setData([], [])
        else:
            self.scatter.setData(x=self.data.x[indices], y=self.data.y[indices], data=np.arange(indices.size, dtype=np.int64))

    def set_visible(self, visible: bool) -> None:
        self.visible = visible
        self.curve.setVisible(visible)
        self.scatter.setVisible(visible)
        if visible:
            self.refresh_view()

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

        self.curve.setPen(
            pg.mkPen(QtGui.QColor(self.line_color), width=self.line_width)
        )
        self.scatter.setBrush(pg.mkBrush(QtGui.QColor(self.point_color)))
        self.scatter.setSize(self.point_size)

    @property
    def bounds(self):
        return self.data.bounds if self.data is not None else None

    @property
    def count(self) -> int:
        return self.data.count if self.data is not None else 0

    def set_z_value(self, z: float) -> None:
        self.curve.setZValue(z)
        self.scatter.setZValue(z + 0.1)

    def remove(self) -> None:
        """Remove this layer's graphics items from the plot."""
        self.plot_item.removeItem(self.curve)
        self.plot_item.removeItem(self.scatter)


class FastShapeLayer(QtCore.QObject):
    """One graphics layer for one database-registered shapefile."""

    def __init__(self, plot_item: pg.PlotItem, data) -> None:
        super().__init__()
        from PySide6 import QtGui

        self.plot_item = plot_item
        self.data = data
        self.name = data.name
        self.visible = True
        self._QtGui = QtGui

        style_text = (data.definition.line_style or "").lower()
        qt_style = QtCore.Qt.PenStyle.SolidLine
        if "dash" in style_text:
            qt_style = QtCore.Qt.PenStyle.DashLine
        elif "dot" in style_text:
            qt_style = QtCore.Qt.PenStyle.DotLine

        pen = pg.mkPen(
            data.definition.line_color,
            width=float(data.definition.line_width),
            style=qt_style,
        )
        self.curve = pg.PlotCurveItem(pen=pen, connect="finite", antialias=False)
        self.scatter = pg.ScatterPlotItem(
            size=max(4.0, float(data.definition.line_width) + 3.0),
            pxMode=True,
            brush=pg.mkBrush(data.definition.fill_color or data.definition.line_color),
            pen=pen,
            useCache=True,
        )
        self.fill_item = None

        if data.geometry_type == "point":
            if data.points.size:
                self.scatter.setData(x=data.points[:, 0], y=data.points[:, 1])
            self.curve.setData([], [])
        else:
            xs: list[float] = []
            ys: list[float] = []
            for part in data.parts:
                if not part.size:
                    continue
                xs.extend(part[:, 0].tolist())
                ys.extend(part[:, 1].tolist())
                xs.append(float("nan"))
                ys.append(float("nan"))
            self.curve.setData(
                x=np.asarray(xs, dtype=np.float64),
                y=np.asarray(ys, dtype=np.float64),
                connect="finite",
                skipFiniteCheck=True,
            )
            self.scatter.setData([], [])

            if data.geometry_type == "polygon" and data.definition.is_filled:
                path = QtGui.QPainterPath()
                path.setFillRule(QtCore.Qt.FillRule.OddEvenFill)
                for part in data.parts:
                    if part.shape[0] < 3:
                        continue
                    path.moveTo(float(part[0, 0]), float(part[0, 1]))
                    for point in part[1:]:
                        path.lineTo(float(point[0]), float(point[1]))
                    path.closeSubpath()
                item = QtWidgets.QGraphicsPathItem(path)
                fill = QtGui.QColor(data.definition.fill_color)
                fill.setAlpha(70)
                item.setBrush(QtGui.QBrush(fill))
                item.setPen(QtGui.QPen(QtCore.Qt.PenStyle.NoPen))
                item.setZValue(-10)
                self.fill_item = item
                plot_item.addItem(item)

        plot_item.addItem(self.curve)
        plot_item.addItem(self.scatter)

    def refresh_view(self) -> None:
        return

    def set_visible(self, visible: bool) -> None:
        self.visible = visible
        self.curve.setVisible(visible)
        self.scatter.setVisible(visible)
        if self.fill_item is not None:
            self.fill_item.setVisible(visible)

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
            local = int(np.argmin(d2))
            distance = float(np.sqrt(d2[local]))
            if best_distance is None or distance < best_distance:
                best_distance = distance
                best_index = running_index + local
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
