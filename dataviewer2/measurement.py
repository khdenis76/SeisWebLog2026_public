from __future__ import annotations

import math
from dataclasses import dataclass

import pyqtgraph as pg
from PySide6 import QtCore, QtGui, QtWidgets


@dataclass(slots=True)
class Waypoint:
    x: float
    y: float
    label: str = ""


class MeasurementTool(QtCore.QObject):
    """Shared measurement engine for distance, area, bearing and angle."""

    changed = QtCore.Signal(str)
    mode_changed = QtCore.Signal(str)

    MODES = {"distance", "area", "bearing", "angle"}

    def __init__(self, plot_item: pg.PlotItem) -> None:
        super().__init__()
        self.plot_item = plot_item
        self.enabled = False
        self.mode = "distance"
        self.points: list[Waypoint] = []
        self.curve = pg.PlotCurveItem(
            pen=pg.mkPen("#ffd740", width=2),
            symbol="o",
            symbolSize=8,
            symbolBrush=pg.mkBrush("#ffd740"),
            symbolPen=pg.mkPen("#1b1f23", width=1),
        )
        plot_item.addItem(self.curve)
        self.labels: list[pg.TextItem] = []
        self.fill_item: QtWidgets.QGraphicsPathItem | None = None

    def set_mode(self, mode: str) -> None:
        mode = str(mode).lower().strip()
        if mode not in self.MODES:
            raise ValueError(f"Unsupported measurement mode: {mode}")
        if self.mode != mode:
            self.mode = mode
            self.clear()
        self.enabled = True
        self.mode_changed.emit(mode)
        self._refresh()

    def disable(self) -> None:
        self.enabled = False
        self.mode_changed.emit("")

    def clear(self) -> None:
        self.points.clear()
        self._refresh()

    def remove_last(self) -> None:
        if self.points:
            self.points.pop()
            self._refresh()

    def add(self, x: float, y: float, label: str = "") -> None:
        if not self.enabled:
            return
        # Bearing is defined by two points; a third click starts a new bearing.
        if self.mode == "bearing" and len(self.points) >= 2:
            self.points.clear()
        # Angle is defined by three points; a fourth click starts a new angle.
        if self.mode == "angle" and len(self.points) >= 3:
            self.points.clear()
        self.points.append(Waypoint(float(x), float(y), label))
        self._refresh()

    def _clear_graphics(self) -> None:
        for item in self.labels:
            self.plot_item.removeItem(item)
        self.labels.clear()
        if self.fill_item is not None:
            self.plot_item.removeItem(self.fill_item)
            self.fill_item = None

    def _add_text(self, text: str, x: float, y: float, anchor=(0.5, 1.0)) -> None:
        item = pg.TextItem(
            text,
            color="#ffffff",
            anchor=anchor,
            fill=pg.mkBrush(0, 0, 0, 175),
            border=pg.mkPen("#ffd740", width=1),
        )
        item.setPos(x, y)
        item.setZValue(10_000)
        self.plot_item.addItem(item)
        self.labels.append(item)

    @staticmethod
    def _bearing(a: Waypoint, b: Waypoint) -> float:
        return math.degrees(math.atan2(b.x - a.x, b.y - a.y)) % 360.0

    @staticmethod
    def _distance(a: Waypoint, b: Waypoint) -> float:
        return math.hypot(b.x - a.x, b.y - a.y)

    @staticmethod
    def _polygon_area(points: list[Waypoint]) -> float:
        if len(points) < 3:
            return 0.0
        return abs(sum(
            a.x * b.y - b.x * a.y
            for a, b in zip(points, points[1:] + points[:1])
        )) * 0.5

    @staticmethod
    def _angle(a: Waypoint, vertex: Waypoint, c: Waypoint) -> float:
        v1 = (a.x - vertex.x, a.y - vertex.y)
        v2 = (c.x - vertex.x, c.y - vertex.y)
        n1 = math.hypot(*v1)
        n2 = math.hypot(*v2)
        if n1 == 0 or n2 == 0:
            return float("nan")
        dot = max(-1.0, min(1.0, (v1[0] * v2[0] + v1[1] * v2[1]) / (n1 * n2)))
        return math.degrees(math.acos(dot))

    def _refresh(self) -> None:
        self._clear_graphics()
        points = self.points
        xs = [p.x for p in points]
        ys = [p.y for p in points]

        if self.mode == "area" and len(points) >= 3:
            xs = xs + [points[0].x]
            ys = ys + [points[0].y]
            path = QtGui.QPainterPath()
            path.moveTo(points[0].x, points[0].y)
            for point in points[1:]:
                path.lineTo(point.x, point.y)
            path.closeSubpath()
            self.fill_item = QtWidgets.QGraphicsPathItem(path)
            self.fill_item.setBrush(QtGui.QBrush(QtGui.QColor(255, 215, 64, 45)))
            pen = QtGui.QPen(QtGui.QColor("#ffd740"))
            pen.setWidthF(1.5)
            pen.setCosmetic(True)
            self.fill_item.setPen(pen)
            self.fill_item.setZValue(9_998)
            self.plot_item.addItem(self.fill_item)

        self.curve.setData(xs, ys)

        if not points:
            self.changed.emit(f"{self.mode.title()} measurement: click the map to add points.")
            return

        if self.mode == "distance":
            total = 0.0
            lines = []
            for index, (a, b) in enumerate(zip(points[:-1], points[1:]), start=1):
                distance = self._distance(a, b)
                bearing = self._bearing(a, b)
                total += distance
                self._add_text(f"{distance:.2f} m\n{bearing:.1f}°", (a.x+b.x)/2, (a.y+b.y)/2)
                lines.append(f"{index}→{index+1}: {distance:.2f} m, bearing {bearing:.1f}°")
            lines.append(f"Total distance: {total:.2f} m")
            self.changed.emit("\n".join(lines))
            return

        if self.mode == "bearing":
            if len(points) < 2:
                self.changed.emit("Bearing: click the second point.")
                return
            a, b = points[:2]
            distance = self._distance(a, b)
            bearing = self._bearing(a, b)
            self._add_text(f"Bearing {bearing:.2f}°\nDistance {distance:.2f} m", (a.x+b.x)/2, (a.y+b.y)/2)
            self.changed.emit(
                f"Bearing: {bearing:.2f}°\nDistance: {distance:.2f} m\n"
                f"From: X {a.x:.3f}, Y {a.y:.3f}\nTo: X {b.x:.3f}, Y {b.y:.3f}"
            )
            return

        if self.mode == "area":
            perimeter = sum(self._distance(a, b) for a, b in zip(points, points[1:]))
            if len(points) >= 3:
                perimeter += self._distance(points[-1], points[0])
            area = self._polygon_area(points)
            if len(points) >= 3:
                cx = sum(p.x for p in points) / len(points)
                cy = sum(p.y for p in points) / len(points)
                self._add_text(f"Area {area:,.2f} m²\nPerimeter {perimeter:,.2f} m", cx, cy, anchor=(0.5, 0.5))
            self.changed.emit(
                f"Area: {area:,.2f} m²\nPerimeter: {perimeter:,.2f} m\n"
                f"Vertices: {len(points)}"
            )
            return

        if self.mode == "angle":
            if len(points) < 3:
                self.changed.emit(f"Angle: click {3-len(points)} more point(s). The second point is the vertex.")
                return
            a, vertex, c = points[:3]
            angle = self._angle(a, vertex, c)
            self._add_text(f"{angle:.2f}°", vertex.x, vertex.y, anchor=(0.5, 1.2))
            self.changed.emit(
                f"Angle: {angle:.2f}°\nVertex: X {vertex.x:.3f}, Y {vertex.y:.3f}\n"
                "Point order: first arm → vertex → second arm"
            )
