from __future__ import annotations

import math
from dataclasses import dataclass

import pyqtgraph as pg
from PySide6 import QtCore


@dataclass(slots=True)
class Waypoint:
    x: float
    y: float
    label: str = ""


class MeasurementTool(QtCore.QObject):
    changed = QtCore.Signal(str)

    def __init__(self, plot_item: pg.PlotItem) -> None:
        super().__init__()
        self.plot_item = plot_item
        self.enabled = False
        self.points: list[Waypoint] = []
        self.curve = pg.PlotCurveItem(pen=pg.mkPen("yellow", width=2), symbol="o", symbolSize=8, symbolBrush=pg.mkBrush("yellow"))
        plot_item.addItem(self.curve)
        self.labels: list[pg.TextItem] = []

    def clear(self) -> None:
        self.points.clear()
        self._refresh()

    def remove_last(self) -> None:
        if self.points:
            self.points.pop()
            self._refresh()

    def add(self, x: float, y: float, label: str = "") -> None:
        if self.enabled:
            self.points.append(Waypoint(x, y, label))
            self._refresh()

    def _refresh(self) -> None:
        self.curve.setData([p.x for p in self.points], [p.y for p in self.points])
        for item in self.labels:
            self.plot_item.removeItem(item)
        self.labels.clear()
        total = 0.0
        lines: list[str] = []
        for i in range(1, len(self.points)):
            a, b = self.points[i - 1], self.points[i]
            dx, dy = b.x - a.x, b.y - a.y
            distance = math.hypot(dx, dy)
            bearing = math.degrees(math.atan2(dx, dy)) % 360.0
            total += distance
            text = pg.TextItem(f"{distance:.2f} m\n{bearing:.1f}°", anchor=(0.5, 1.0))
            text.setPos((a.x + b.x) / 2.0, (a.y + b.y) / 2.0)
            self.plot_item.addItem(text)
            self.labels.append(text)
            lines.append(f"{i}→{i+1}: {distance:.2f} m, {bearing:.1f}°")
        self.changed.emit(("\n".join(lines) + (f"\nTotal: {total:.2f} m" if lines else "Click map to add waypoints.")))
