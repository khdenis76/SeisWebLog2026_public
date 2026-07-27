from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore, QtGui, QtWidgets

from .models import DsrQcData
from .qc_widgets import (
    PLOT_BG,
    PlainNumberAxis,
    make_swatch_item,
    robust_y_range,
    stable_color,
    style_plot,
)


class DsrQcWindow(QtWidgets.QMainWindow):
    """Receiver-line QC workbench with presets, readable plots and map sync."""

    line_requested = QtCore.Signal(int)
    station_selected = QtCore.Signal(int, int)
    zoom_station_requested = QtCore.Signal(int, int)

    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("DSR QC — DataViewer 2.2")
        self.resize(1500, 900)
        self.setAttribute(QtCore.Qt.WidgetAttribute.WA_DeleteOnClose, False)
        self._data: DsrQcData | None = None
        self._styles: dict[str, dict[str, Any]] = {}
        self._plot_widgets: list[pg.PlotWidget] = []
        self._station_lines: list[pg.InfiniteLine] = []
        self._redraw_timer = QtCore.QTimer(self)
        self._redraw_timer.setSingleShot(True)
        self._redraw_timer.setInterval(70)
        self._redraw_timer.timeout.connect(self._redraw_now)
        self._build_ui()

    def _build_ui(self) -> None:
        central = QtWidgets.QWidget()
        root = QtWidgets.QVBoxLayout(central)
        root.setContentsMargins(6, 6, 6, 6)
        root.setSpacing(6)
        self.setCentralWidget(central)

        top = QtWidgets.QFrame()
        top.setFrameShape(QtWidgets.QFrame.Shape.StyledPanel)
        grid = QtWidgets.QGridLayout(top)
        grid.setContentsMargins(8, 6, 8, 6)

        self.line_combo = QtWidgets.QComboBox()
        self.line_combo.setEditable(True)
        self.line_combo.setInsertPolicy(QtWidgets.QComboBox.InsertPolicy.NoInsert)
        self.line_combo.setMinimumWidth(145)
        self.line_combo.currentIndexChanged.connect(self._line_changed)
        self.station_combo = QtWidgets.QComboBox()
        self.station_combo.setEditable(True)
        self.station_combo.setInsertPolicy(QtWidgets.QComboBox.InsertPolicy.NoInsert)
        self.station_combo.setMinimumWidth(145)
        self.station_combo.currentIndexChanged.connect(self._station_changed)
        self.zoom_station_button = QtWidgets.QPushButton("Zoom station")
        self.zoom_station_button.clicked.connect(self._zoom_station)

        self.plot_mode = QtWidgets.QComboBox()
        self.plot_mode.addItems(["QC groups", "Stacked parameters", "Overlay selected"])
        self.plot_mode.currentIndexChanged.connect(lambda *_: self._queue_redraw())
        self.render_mode = QtWidgets.QComboBox()
        self.render_mode.addItems(["Line", "Bar"])
        self.render_mode.currentIndexChanged.connect(lambda *_: self._queue_redraw())
        self.zero_line_check = QtWidgets.QCheckBox("Zero reference")
        self.zero_line_check.setChecked(True)
        self.zero_line_check.toggled.connect(lambda *_: self._queue_redraw())
        self.max_plots = QtWidgets.QSpinBox()
        self.max_plots.setRange(1, 20)
        self.max_plots.setValue(8)
        self.max_plots.valueChanged.connect(lambda *_: self._queue_redraw())

        grid.addWidget(QtWidgets.QLabel("Receiver line"), 0, 0)
        grid.addWidget(self.line_combo, 0, 1)
        grid.addWidget(QtWidgets.QLabel("Station"), 0, 2)
        grid.addWidget(self.station_combo, 0, 3)
        grid.addWidget(self.zoom_station_button, 0, 4)
        grid.addWidget(QtWidgets.QLabel("Layout"), 1, 0)
        grid.addWidget(self.plot_mode, 1, 1)
        grid.addWidget(QtWidgets.QLabel("Graph"), 1, 2)
        grid.addWidget(self.render_mode, 1, 3)
        grid.addWidget(self.zero_line_check, 1, 4)
        grid.addWidget(QtWidgets.QLabel("Max charts"), 1, 5)
        grid.addWidget(self.max_plots, 1, 6)
        grid.setColumnStretch(7, 1)
        root.addWidget(top)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        root.addWidget(splitter, 1)

        left = QtWidgets.QWidget()
        left_layout = QtWidgets.QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)
        self.channel_filter = QtWidgets.QLineEdit()
        self.channel_filter.setPlaceholderText("Filter DSR parameters…")
        self.channel_filter.textChanged.connect(self._filter_channels)
        left_layout.addWidget(self.channel_filter)

        preset_row = QtWidgets.QGridLayout()
        preset_buttons = (
            ("Offsets", self._select_offsets_preset),
            ("Sigma", self._select_sigma_preset),
            ("Coordinates", self._select_coordinates_preset),
            ("Depth", self._select_depth_preset),
            ("Clear", lambda: self._set_all_checked(False)),
            ("All", lambda: self._set_all_checked(True)),
        )
        for index, (text, callback) in enumerate(preset_buttons):
            button = QtWidgets.QPushButton(text)
            button.clicked.connect(callback)
            preset_row.addWidget(button, index // 3, index % 3)
        left_layout.addLayout(preset_row)

        self.channel_tree = QtWidgets.QTreeWidget()
        self.channel_tree.setHeaderLabels(["Parameter", "Color", "Width"])
        self.channel_tree.setColumnWidth(0, 220)
        self.channel_tree.setColumnWidth(1, 48)
        self.channel_tree.setColumnWidth(2, 45)
        self.channel_tree.itemChanged.connect(lambda *_: self._channel_changed())
        self.channel_tree.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.channel_tree.customContextMenuRequested.connect(self._channel_menu)
        left_layout.addWidget(self.channel_tree, 1)
        self.selection_label = QtWidgets.QLabel("0 selected")
        left_layout.addWidget(self.selection_label)
        splitter.addWidget(left)

        self.scroll = QtWidgets.QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.plot_container = QtWidgets.QWidget()
        self.plot_layout = QtWidgets.QVBoxLayout(self.plot_container)
        self.plot_layout.setContentsMargins(0, 0, 0, 0)
        self.plot_layout.setSpacing(6)
        self.scroll.setWidget(self.plot_container)
        splitter.addWidget(self.scroll)
        splitter.setSizes([340, 1160])

        self.status = QtWidgets.QLabel("Select a receiver line.")
        root.addWidget(self.status)

    def set_lines(self, lines: list[int], selected: int | None = None) -> None:
        blocker = QtCore.QSignalBlocker(self.line_combo)
        self.line_combo.clear()
        for line in lines:
            self.line_combo.addItem(str(line), int(line))
        if selected is not None:
            index = self.line_combo.findData(int(selected))
            if index >= 0:
                self.line_combo.setCurrentIndex(index)
        del blocker
        if self.line_combo.count() and self.line_combo.currentIndex() < 0:
            self.line_combo.setCurrentIndex(0)

    def select_line(self, line: int) -> None:
        index = self.line_combo.findData(int(line))
        if index >= 0:
            self.line_combo.setCurrentIndex(index)
        else:
            self.line_requested.emit(int(line))

    def set_loading(self, line: int) -> None:
        self.status.setText(f"Loading DSR QC for line {line}…")

    def set_error(self, message: str) -> None:
        self.status.setText(message)

    def set_data(self, data: DsrQcData) -> None:
        self._data = data
        self.status.setText(
            f"Line {data.line}: {data.count:,} station records, "
            f"{len(data.columns)} numeric QC parameters"
        )
        self._populate_stations()
        self._populate_channels()
        self._queue_redraw()

    def _line_changed(self, _index: int) -> None:
        value = self.line_combo.currentData()
        if value is not None:
            self.line_requested.emit(int(value))

    def _populate_stations(self) -> None:
        blocker = QtCore.QSignalBlocker(self.station_combo)
        previous = self.station_combo.currentData()
        self.station_combo.clear()
        if self._data is not None:
            stations = np.unique(self._data.station[np.isfinite(self._data.station)])
            for station in stations:
                value = int(round(float(station)))
                self.station_combo.addItem(str(value), value)
        if previous is not None:
            index = self.station_combo.findData(previous)
            if index >= 0:
                self.station_combo.setCurrentIndex(index)
        del blocker

    def _station_changed(self, _index: int) -> None:
        if self._data is None:
            return
        station = self.station_combo.currentData()
        if station is None:
            return
        self._move_station_cursor(float(station))
        self.station_selected.emit(self._data.line, int(station))

    def _zoom_station(self) -> None:
        if self._data is not None and self.station_combo.currentData() is not None:
            self.zoom_station_requested.emit(self._data.line, int(self.station_combo.currentData()))

    def _style(self, name: str) -> dict[str, Any]:
        return self._styles.setdefault(name, {"color": stable_color(name), "width": 1.8})

    @staticmethod
    def _is_administrative(name: str) -> bool:
        low = name.lower()
        exact = {
            "id", "linepoint", "linepointidx", "recidx", "tier", "tierline",
            "isexported", "isrecexported", "downloaded", "expecteddownloaded",
            "deployed", "collected", "actualx", "actualy", "spsx", "spsy",
        }
        return low in exact or low.endswith("_fk") or low.startswith("is")

    @staticmethod
    def _category(name: str) -> str:
        low = name.lower().replace("-", "_")
        if any(t in low for t in ("inline", "in_line", "xline", "x_line", "crossline", "cross_line", "radial", "range", "bearing", "brg", "azimuth")):
            return "Offsets"
        if any(t in low for t in ("sigma", "e95", "n95", "z95", "uncert", "hdop", "pdop", "vdop", "95")):
            return "Uncertainty / Sigma"
        if any(t in low for t in ("depth", "elevation", "waterdepth", "height")):
            return "Depth / elevation"
        if any(t in low for t in ("easting", "northing", "actualx", "actualy", "spsx", "spsy", "preplot")):
            return "Coordinates"
        if any(t in low for t in ("time", "date", "day", "week", "year", "month", "jday")):
            return "Time"
        return "Other numeric"

    def _populate_channels(self) -> None:
        previous = set(self._checked_channels())
        blocker = QtCore.QSignalBlocker(self.channel_tree)
        self.channel_tree.clear()
        if self._data is None:
            del blocker
            return
        groups: dict[str, QtWidgets.QTreeWidgetItem] = {}
        offset_candidates: list[str] = []
        for name in self._data.channel_names:
            if self._is_administrative(name):
                continue
            category = self._category(name)
            group = groups.get(category)
            if group is None:
                group = QtWidgets.QTreeWidgetItem([category, "", ""])
                group.setFlags(group.flags() & ~QtCore.Qt.ItemFlag.ItemIsUserCheckable)
                group.setExpanded(category in {"Offsets", "Uncertainty / Sigma"})
                self.channel_tree.addTopLevelItem(group)
                groups[category] = group
            style = self._style(name)
            checked = name in previous
            item = make_swatch_item(name, str(style["color"]), float(style["width"]), checked)
            group.addChild(item)
            if category == "Offsets":
                offset_candidates.append(name)
        del blocker
        if not previous:
            self._select_names(offset_candidates[:6])
        self._update_selected_count()

    def _iter_items(self):
        for i in range(self.channel_tree.topLevelItemCount()):
            group = self.channel_tree.topLevelItem(i)
            for j in range(group.childCount()):
                yield group.child(j)

    def _checked_channels(self) -> list[str]:
        return [str(item.data(0, QtCore.Qt.ItemDataRole.UserRole)) for item in self._iter_items()
                if item.checkState(0) == QtCore.Qt.CheckState.Checked]

    def _channel_changed(self) -> None:
        self._update_selected_count()
        self._queue_redraw()

    def _update_selected_count(self) -> None:
        self.selection_label.setText(f"{len(self._checked_channels())} selected")

    def _set_all_checked(self, checked: bool) -> None:
        blocker = QtCore.QSignalBlocker(self.channel_tree)
        for item in self._iter_items():
            item.setCheckState(0, QtCore.Qt.CheckState.Checked if checked else QtCore.Qt.CheckState.Unchecked)
        del blocker
        self._update_selected_count()
        self._queue_redraw()

    def _select_names(self, names: list[str]) -> None:
        wanted = set(names)
        blocker = QtCore.QSignalBlocker(self.channel_tree)
        for item in self._iter_items():
            name = str(item.data(0, QtCore.Qt.ItemDataRole.UserRole) or "")
            item.setCheckState(0, QtCore.Qt.CheckState.Checked if name in wanted else QtCore.Qt.CheckState.Unchecked)
        del blocker
        self._update_selected_count()
        self._queue_redraw()

    def _select_matching(self, tokens: tuple[str, ...]) -> None:
        matches = []
        for item in self._iter_items():
            name = str(item.data(0, QtCore.Qt.ItemDataRole.UserRole) or "")
            if any(token in name.lower() for token in tokens):
                matches.append(name)
        self._select_names(matches[: self.max_plots.value()])

    def _select_offsets_preset(self) -> None:
        self._select_matching(("inline", "in_line", "xline", "x_line", "crossline", "cross_line", "radial", "range", "bearing", "brg"))

    def _select_sigma_preset(self) -> None:
        self._select_matching(("sigma", "e95", "n95", "z95", "uncert", "95"))

    def _select_coordinates_preset(self) -> None:
        self._select_matching(("easting", "northing", "actualx", "actualy", "spsx", "spsy", "preplot"))

    def _select_depth_preset(self) -> None:
        self._select_matching(("depth", "elevation", "height"))

    def _filter_channels(self, text: str) -> None:
        text = text.strip().lower()
        for i in range(self.channel_tree.topLevelItemCount()):
            group = self.channel_tree.topLevelItem(i)
            visible_count = 0
            for j in range(group.childCount()):
                child = group.child(j)
                visible = not text or text in child.text(0).lower()
                child.setHidden(not visible)
                visible_count += int(visible)
            group.setHidden(visible_count == 0)

    def _channel_menu(self, position: QtCore.QPoint) -> None:
        item = self.channel_tree.itemAt(position)
        if item is None:
            return
        name = item.data(0, QtCore.Qt.ItemDataRole.UserRole)
        if not name:
            return
        name = str(name)
        style = self._style(name)
        menu = QtWidgets.QMenu(self)
        color_action = menu.addAction("Change color…")
        width_action = menu.addAction("Change line thickness…")
        reset_action = menu.addAction("Reset style")
        selected = menu.exec(self.channel_tree.viewport().mapToGlobal(position))
        if selected is color_action:
            color = QtWidgets.QColorDialog.getColor(QtGui.QColor(str(style["color"])), self)
            if color.isValid():
                style["color"] = color.name()
        elif selected is width_action:
            value, ok = QtWidgets.QInputDialog.getDouble(self, "Line thickness", "Width", float(style["width"]), 0.2, 10.0, 1)
            if ok:
                style["width"] = value
        elif selected is reset_action:
            self._styles.pop(name, None)
            style = self._style(name)
        else:
            return
        color = QtGui.QColor(str(style["color"]))
        item.setBackground(1, QtGui.QBrush(color))
        item.setText(2, f"{float(style['width']):.1f}")
        self._queue_redraw()

    def _queue_redraw(self) -> None:
        self._redraw_timer.start()

    @staticmethod
    def _metric_group(name: str) -> str:
        text = name.lower()
        for token in ("primary", "secondary", "deployment", "recovery", "rec", "dep"):
            text = text.replace(token, "")
        return " ".join(text.replace("_", " ").replace("-", " ").split()) or name

    def _groups_for_plot(self, selected: list[str]) -> list[tuple[str, list[str]]]:
        mode = self.plot_mode.currentText()
        if mode == "Overlay selected":
            return [("Selected DSR parameters", selected[: self.max_plots.value()])]
        if mode == "Stacked parameters":
            return [(name, [name]) for name in selected[: self.max_plots.value()]]
        grouped: dict[str, list[str]] = defaultdict(list)
        for name in selected:
            grouped[self._metric_group(name)].append(name)
        return [(key.title(), values) for key, values in list(grouped.items())[: self.max_plots.value()]]

    def _clear_plots(self) -> None:
        while self.plot_layout.count():
            item = self.plot_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()
        self._plot_widgets.clear()
        self._station_lines.clear()

    def _redraw_now(self) -> None:
        self._clear_plots()
        data = self._data
        selected = self._checked_channels()
        if data is None or not selected:
            label = QtWidgets.QLabel("Choose a QC preset or select parameters from the left panel")
            label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            self.plot_layout.addWidget(label, 1)
            return

        groups = self._groups_for_plot(selected)
        previous_plot = None
        created = 0
        for title, channels in groups:
            valid_channels = []
            for channel in channels:
                values = data.columns.get(channel)
                if values is not None and np.isfinite(data.station).any() and np.isfinite(values).any():
                    valid_channels.append(channel)
            if not valid_channels:
                continue

            axis_items = {
                "bottom": PlainNumberAxis(orientation="bottom", decimals=0),
                "left": PlainNumberAxis(orientation="left"),
            }
            widget = pg.PlotWidget(axisItems=axis_items)
            widget.setBackground(PLOT_BG)
            widget.setMinimumHeight(225 if len(groups) > 1 else 560)
            plot = widget.getPlotItem()
            style_plot(plot, title, "Value", "Station")
            if previous_plot is not None:
                plot.setXLink(previous_plot)
            previous_plot = plot
            if len(valid_channels) > 1:
                plot.addLegend(offset=(-10, 10))
            if self.zero_line_check.isChecked():
                plot.addItem(pg.InfiniteLine(pos=0, angle=0, pen=pg.mkPen("#7f8c8d", width=1, style=QtCore.Qt.PenStyle.DashLine)))
            cursor = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen("#ffffff", width=1.2, style=QtCore.Qt.PenStyle.DashLine))
            plot.addItem(cursor)
            self._station_lines.append(cursor)

            plotted_values = []
            for channel in valid_channels:
                values = np.asarray(data.columns[channel], dtype=float)
                finite = np.isfinite(data.station) & np.isfinite(values)
                x = np.asarray(data.station[finite], dtype=float)
                y = np.asarray(values[finite], dtype=float)
                if not x.size:
                    continue
                order = np.argsort(x)
                x, y = x[order], y[order]
                style = self._style(channel)
                color = QtGui.QColor(str(style["color"]))
                if self.render_mode.currentText() == "Bar":
                    unique_x = np.unique(x)
                    spacing = float(np.nanmedian(np.diff(unique_x))) if unique_x.size > 1 else 1.0
                    item = pg.BarGraphItem(x=x, height=y, width=max(0.5, spacing * 0.72), brush=pg.mkBrush(color), pen=pg.mkPen(color.darker(130)))
                    plot.addItem(item)
                else:
                    curve = pg.PlotDataItem(
                        x, y,
                        pen=pg.mkPen(color, width=float(style["width"])),
                        symbol="o", symbolSize=6,
                        symbolBrush=pg.mkBrush(color), symbolPen=pg.mkPen("#20252a"),
                        name=channel,
                    )
                    plot.addItem(curve)
                    curve.setDownsampling(auto=True, method="peak")
                    curve.setClipToView(True)
                    curve.sigPointsClicked.connect(
                        lambda _item, points, _event, line=data.line: self._points_clicked(points, line)
                    )
                plotted_values.append(y)

            y_range = robust_y_range(plotted_values)
            if y_range:
                plot.setYRange(*y_range, padding=0)
            self.plot_layout.addWidget(widget)
            self._plot_widgets.append(widget)
            created += 1

        if not created:
            label = QtWidgets.QLabel("The selected parameters contain no finite values on this line")
            label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            self.plot_layout.addWidget(label, 1)
        else:
            self.plot_layout.addStretch(1)
        station = self.station_combo.currentData()
        if station is not None:
            self._move_station_cursor(float(station))

    def _points_clicked(self, points, line: int) -> None:
        if points is None or len(points) == 0:
            return
        station = int(round(float(points[0].pos().x())))
        index = self.station_combo.findData(station)
        if index >= 0:
            self.station_combo.setCurrentIndex(index)
        self.station_selected.emit(line, station)

    def _move_station_cursor(self, station: float) -> None:
        for cursor in self._station_lines:
            cursor.setValue(station)
