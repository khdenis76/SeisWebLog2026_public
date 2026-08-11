from __future__ import annotations

import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore, QtGui

from .models import PointLayerData


def _pen_style(name: str) -> QtCore.Qt.PenStyle:
    return {
        "solid": QtCore.Qt.PenStyle.SolidLine,
        "dash": QtCore.Qt.PenStyle.DashLine,
        "dot": QtCore.Qt.PenStyle.DotLine,
        "dash dot": QtCore.Qt.PenStyle.DashDotLine,
        "dash-dot": QtCore.Qt.PenStyle.DashDotLine,
        "dash dot dot": QtCore.Qt.PenStyle.DashDotDotLine,
    }.get(str(name).lower(), QtCore.Qt.PenStyle.SolidLine)


class PointComparisonLayer:
    def __init__(self, plot_item: pg.PlotItem, name: str, source: PointLayerData, target: PointLayerData,
                 source_keys: tuple[str, str], target_keys: tuple[str, str], color: str = "#ffd740",
                 width: float = 1.5, line_style: str = "dash", show_labels: bool = True) -> None:
        self.plot_item = plot_item; self.name = name; self.visible = True
        self.color = color; self.width = width; self.line_style = line_style; self.show_labels = show_labels
        self.curve = pg.PlotCurveItem(antialias=False, connect="finite")
        self.labels: list[pg.TextItem] = []
        self.plot_item.addItem(self.curve)
        self._build(source, target, source_keys, target_keys)
        self.update_style(color=color, width=width, line_style=line_style)

    @property
    def count(self): return int(self._count)
    @property
    def bounds(self): return self._bounds

    @staticmethod
    def _values(data: PointLayerData, key: str):
        for existing, values in data.metadata.items():
            if existing.casefold() == key.casefold(): return values
        raise KeyError(key)

    @staticmethod
    def available_key(data: PointLayerData, candidates: tuple[str, ...]) -> str | None:
        lookup = {k.casefold(): k for k in data.metadata}
        for candidate in candidates:
            if candidate.casefold() in lookup: return lookup[candidate.casefold()]
        return None

    def _build(self, source, target, source_keys, target_keys):
        s1, s2 = self._values(source, source_keys[0]), self._values(source, source_keys[1])
        t1, t2 = self._values(target, target_keys[0]), self._values(target, target_keys[1])
        index = {(str(a), str(b)): i for i, (a, b) in enumerate(zip(t1, t2))}
        pairs = [(i, index.get((str(a), str(b)))) for i, (a, b) in enumerate(zip(s1, s2))]
        pairs = [(i, j) for i, j in pairs if j is not None and np.isfinite(source.x[i]) and np.isfinite(source.y[i]) and np.isfinite(target.x[j]) and np.isfinite(target.y[j])]
        self._count = len(pairs)
        if not pairs:
            self.curve.setData([], []); self._bounds = None; return
        x = np.empty(len(pairs) * 3); y = np.empty(len(pairs) * 3)
        allx=[]; ally=[]
        for n, (i,j) in enumerate(pairs):
            x[n*3:n*3+3] = (source.x[i], target.x[j], np.nan); y[n*3:n*3+3]=(source.y[i], target.y[j], np.nan)
            allx.extend((source.x[i], target.x[j])); ally.extend((source.y[i], target.y[j]))
            if self.show_labels and len(self.labels) < 300:
                dx=float(target.x[j]-source.x[i]); dy=float(target.y[j]-source.y[i]); d=float(np.hypot(dx,dy)); az=(float(np.degrees(np.arctan2(dx,dy)))+360)%360
                label=pg.TextItem(f"{d:.2f} m\n{az:.1f}°", anchor=(0.5,1.0), color=QtGui.QColor(self.color), border=None, fill=None)
                label.setPos((source.x[i]+target.x[j])/2, (source.y[i]+target.y[j])/2); self.plot_item.addItem(label); self.labels.append(label)
        self.curve.setData(x=x,y=y,connect="finite",skipFiniteCheck=True)
        self._bounds=(float(np.min(allx)),float(np.max(allx)),float(np.min(ally)),float(np.max(ally)))

    def update_style(self, color=None, width=None, line_style=None):
        if color is not None:self.color=str(color)
        if width is not None:self.width=float(width)
        if line_style is not None:self.line_style=str(line_style)
        pen=pg.mkPen(QtGui.QColor(self.color),width=self.width); pen.setStyle(_pen_style(self.line_style)); self.curve.setPen(pen)
        for label in self.labels: label.setColor(QtGui.QColor(self.color))

    def set_visible(self, visible):
        self.visible=bool(visible); self.curve.setVisible(self.visible)
        for label in self.labels: label.setVisible(self.visible)

    def set_z_value(self,value):
        self.curve.setZValue(value)
        for label in self.labels:label.setZValue(value+1)

    def remove(self):
        self.plot_item.removeItem(self.curve)
        for label in self.labels:self.plot_item.removeItem(label)
        self.labels.clear()
