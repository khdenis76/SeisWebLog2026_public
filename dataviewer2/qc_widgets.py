from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore, QtGui, QtWidgets

PLOT_BG = "#11161c"
PANEL_BG = "#f5f6f8"
TEXT = "#e8edf2"
AXIS = "#aeb8c2"
GRID_ALPHA = 0.24
PALETTE = (
    "#00b8d9", "#ff8f00", "#d81b60", "#7e57c2",
    "#00a65a", "#1976d2", "#ef5350", "#8bc34a",
    "#f57c00", "#5e35b1", "#00838f", "#c0a000",
    "#e53935", "#039be5", "#00897b", "#ab47bc",
)


def stable_color(name: str) -> str:
    value = sum((i + 1) * ord(ch) for i, ch in enumerate(name))
    return PALETTE[value % len(PALETTE)]


class ElapsedAxis(pg.AxisItem):
    """Readable elapsed-time axis; avoids automatic ks/Ms prefixes."""

    def __init__(self, *args, unit_scale: float = 1.0, suffix: str = "s", **kwargs):
        super().__init__(*args, **kwargs)
        self.unit_scale = float(unit_scale)
        self.suffix = suffix
        self.enableAutoSIPrefix(False)

    def tickStrings(self, values, scale, spacing):  # noqa: N802
        result = []
        for value in values:
            converted = value / self.unit_scale
            if abs(converted) >= 100:
                label = f"{converted:.0f}"
            elif abs(converted) >= 10:
                label = f"{converted:.1f}"
            else:
                label = f"{converted:.2f}"
            result.append(label)
        return result


class PlainNumberAxis(pg.AxisItem):
    """Numeric axis without scientific/SI prefixes for ordinary QC values."""

    def __init__(self, *args, decimals: int = 3, **kwargs):
        super().__init__(*args, **kwargs)
        self.decimals = decimals
        self.enableAutoSIPrefix(False)

    def tickStrings(self, values, scale, spacing):  # noqa: N802
        result = []
        for value in values:
            if not math.isfinite(value):
                result.append("")
                continue
            abs_value = abs(value)
            if abs_value >= 10000:
                result.append(f"{value:,.0f}")
            elif abs_value >= 100:
                result.append(f"{value:.1f}")
            elif abs_value >= 1:
                result.append(f"{value:.2f}")
            else:
                result.append(f"{value:.3f}")
        return result


def style_plot(plot: pg.PlotItem, title: str, left_label: str, bottom_label: str) -> None:
    plot.setTitle(title, color=TEXT, size="10pt")
    plot.showGrid(x=True, y=True, alpha=GRID_ALPHA)
    plot.setLabel("left", left_label, color=TEXT)
    plot.setLabel("bottom", bottom_label, color=TEXT)
    plot.setMenuEnabled(True)
    for axis_name in ("left", "bottom"):
        axis = plot.getAxis(axis_name)
        axis.setPen(pg.mkPen(AXIS))
        axis.setTextPen(pg.mkPen(TEXT))
        axis.setStyle(tickFont=QtGui.QFont("Segoe UI", 8))
    plot.getAxis("left").setWidth(92)
    plot.getViewBox().setMouseEnabled(x=True, y=True)


def finite_qc(values: np.ndarray, *, sentinel_limit: float | None = 1e8) -> np.ndarray:
    arr = np.asarray(values, dtype=float).copy()
    if sentinel_limit is not None:
        arr[np.abs(arr) >= sentinel_limit] = np.nan
    return arr


def robust_y_range(values: Iterable[np.ndarray]) -> tuple[float, float] | None:
    finite_parts = []
    for values_part in values:
        arr = np.asarray(values_part, dtype=float)
        arr = arr[np.isfinite(arr)]
        if arr.size:
            finite_parts.append(arr)
    if not finite_parts:
        return None
    joined = np.concatenate(finite_parts)
    if joined.size >= 20:
        low, high = np.nanpercentile(joined, [1, 99])
    else:
        low, high = float(np.nanmin(joined)), float(np.nanmax(joined))
    if not np.isfinite(low) or not np.isfinite(high):
        return None
    if low == high:
        margin = max(abs(low) * 0.1, 1.0)
    else:
        margin = (high - low) * 0.12
    return float(low - margin), float(high + margin)


def make_swatch_item(name: str, color: str, width: float, checked: bool = False) -> QtWidgets.QTreeWidgetItem:
    item = QtWidgets.QTreeWidgetItem([name, "", f"{width:.1f}"])
    item.setFlags(item.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
    item.setCheckState(0, QtCore.Qt.CheckState.Checked if checked else QtCore.Qt.CheckState.Unchecked)
    item.setData(0, QtCore.Qt.ItemDataRole.UserRole, name)
    qcolor = QtGui.QColor(color)
    item.setBackground(1, QtGui.QBrush(qcolor))
    item.setToolTip(1, qcolor.name())
    return item
