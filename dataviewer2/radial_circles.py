from __future__ import annotations

import numpy as np
from PySide6 import QtCore, QtGui, QtWidgets


class RadialCircleItem(QtWidgets.QGraphicsItem):
    """Efficiently draw one equal-radius circle around many map coordinates."""

    def __init__(self) -> None:
        super().__init__()
        self._picture = QtGui.QPicture()
        self._bounds = QtCore.QRectF()
        self._x = np.array([], dtype=float)
        self._y = np.array([], dtype=float)
        self.radius = 5.0
        self.color = QtGui.QColor("#ff5252")
        self.width = 1.5
        self.style = QtCore.Qt.PenStyle.SolidLine
        self.setZValue(25000.0)

    def set_data(self, x: np.ndarray, y: np.ndarray) -> None:
        finite = np.isfinite(x) & np.isfinite(y)
        self._x = np.asarray(x[finite], dtype=float)
        self._y = np.asarray(y[finite], dtype=float)
        self._rebuild()

    def set_style(self, radius: float, color: str, width: float, line_style: str) -> None:
        self.radius = max(0.01, float(radius))
        self.color = QtGui.QColor(color)
        self.width = max(0.2, float(width))
        self.style = {
            "solid": QtCore.Qt.PenStyle.SolidLine,
            "dash": QtCore.Qt.PenStyle.DashLine,
            "dot": QtCore.Qt.PenStyle.DotLine,
            "dashdot": QtCore.Qt.PenStyle.DashDotLine,
        }.get(str(line_style).lower(), QtCore.Qt.PenStyle.SolidLine)
        self._rebuild()

    def _rebuild(self) -> None:
        self.prepareGeometryChange()
        picture = QtGui.QPicture()
        painter = QtGui.QPainter(picture)
        pen = QtGui.QPen(self.color)
        pen.setWidthF(self.width)
        pen.setStyle(self.style)
        pen.setCosmetic(True)
        painter.setPen(pen)
        painter.setBrush(QtCore.Qt.BrushStyle.NoBrush)
        r = self.radius
        for x, y in zip(self._x, self._y):
            painter.drawEllipse(QtCore.QPointF(float(x), float(y)), r, r)
        painter.end()
        self._picture = picture
        if self._x.size:
            self._bounds = QtCore.QRectF(
                float(self._x.min() - r), float(self._y.min() - r),
                float(self._x.max() - self._x.min() + 2*r),
                float(self._y.max() - self._y.min() + 2*r),
            )
        else:
            self._bounds = QtCore.QRectF()
        self.update()

    def boundingRect(self) -> QtCore.QRectF:
        return self._bounds

    def paint(self, painter: QtGui.QPainter, option, widget=None) -> None:
        painter.drawPicture(0, 0, self._picture)
