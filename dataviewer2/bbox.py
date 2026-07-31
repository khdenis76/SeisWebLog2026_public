from __future__ import annotations

from typing import Any

import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore, QtGui, QtWidgets

from .icons_manager import icon

from .models import BlackBoxData, BlackBoxFileInfo
from .qc_widgets import (
    ElapsedAxis,
    PLOT_BG,
    PlainNumberAxis,
    finite_qc,
    make_swatch_item,
    robust_y_range,
    stable_color,
    style_plot,
)


class BlackBoxWindow(QtWidgets.QMainWindow):
    """Detachable BlackBox QC workbench with compact controls and readable plots."""

    file_requested = QtCore.Signal(int)
    add_track_requested = QtCore.Signal(object, str)
    add_all_tracks_requested = QtCore.Signal(object)
    add_all_files_requested = QtCore.Signal(object)
    zoom_track_requested = QtCore.Signal()
    reload_files_requested = QtCore.Signal()

    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("BlackBox QC — DataViewer 2.2")
        self.resize(1500, 900)
        self._data: BlackBoxData | None = None
        self._files: list[BlackBoxFileInfo] = []
        self._channel_styles: dict[str, dict[str, Any]] = {}
        self._plot_widgets: list[pg.PlotWidget] = []
        self._redraw_timer = QtCore.QTimer(self)
        self._redraw_timer.setSingleShot(True)
        self._redraw_timer.setInterval(70)
        self._redraw_timer.timeout.connect(self._redraw_now)
        self._build_ui()

    def _build_ui(self) -> None:
        central = QtWidgets.QWidget()
        outer = QtWidgets.QVBoxLayout(central)
        outer.setContentsMargins(6, 6, 6, 6)
        outer.setSpacing(6)

        top = QtWidgets.QFrame()
        top.setFrameShape(QtWidgets.QFrame.Shape.StyledPanel)
        top_layout = QtWidgets.QGridLayout(top)
        top_layout.setContentsMargins(8, 6, 8, 6)

        self.file_combo = QtWidgets.QComboBox()
        self.file_combo.setMinimumWidth(500)
        self.file_combo.currentIndexChanged.connect(self._file_changed)
        self.reload_button = QtWidgets.QPushButton(icon("reload", size=18), "Reload")
        self.reload_button.clicked.connect(self.reload_files_requested)
        self.track_source = QtWidgets.QComboBox()
        self.track_source.setMinimumWidth(130)
        self.add_track_button = QtWidgets.QPushButton(icon("create", size=18), "Add pair")
        self.add_track_button.clicked.connect(self._add_track)
        self.add_all_tracks_button = QtWidgets.QPushButton(icon("layer", size=18), "Add all pairs")
        self.add_all_tracks_button.clicked.connect(self._add_all_tracks)
        self.add_all_files_button = QtWidgets.QPushButton(icon("database", size=18), "Add all files")
        self.add_all_files_button.clicked.connect(self._add_all_files)
        self.zoom_button = QtWidgets.QPushButton(icon("zoom_line", size=18), "Zoom tracks")
        self.zoom_button.clicked.connect(self.zoom_track_requested)

        self.layout_combo = QtWidgets.QComboBox()
        self.layout_combo.addItems(["Stacked", "Overlay"])
        self.layout_combo.currentIndexChanged.connect(lambda *_: self._redraw())
        self.max_plots = QtWidgets.QSpinBox()
        self.max_plots.setRange(1, 20)
        self.max_plots.setValue(8)
        self.max_plots.setToolTip("Maximum stacked charts displayed at one time")
        self.max_plots.valueChanged.connect(lambda *_: self._redraw())

        top_layout.addWidget(QtWidgets.QLabel("BlackBox file"), 0, 0)
        top_layout.addWidget(self.file_combo, 0, 1, 1, 5)
        top_layout.addWidget(self.reload_button, 0, 6)
        top_layout.addWidget(QtWidgets.QLabel("Coordinate pair"), 1, 0)
        top_layout.addWidget(self.track_source, 1, 1)
        top_layout.addWidget(self.add_track_button, 1, 2)
        top_layout.addWidget(self.add_all_tracks_button, 1, 3)
        top_layout.addWidget(self.add_all_files_button, 1, 4)
        top_layout.addWidget(self.zoom_button, 1, 5)
        top_layout.addWidget(QtWidgets.QLabel("Charts"), 1, 6)
        top_layout.addWidget(self.layout_combo, 1, 7)
        top_layout.addWidget(QtWidgets.QLabel("Max"), 1, 8)
        top_layout.addWidget(self.max_plots, 1, 9)
        outer.addWidget(top)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        outer.addWidget(splitter, 1)

        left = QtWidgets.QWidget()
        left_layout = QtWidgets.QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)
        self.channel_filter = QtWidgets.QLineEdit()
        self.channel_filter.setPlaceholderText("Filter channels…")
        self.channel_filter.textChanged.connect(self._filter_channels)
        left_layout.addWidget(self.channel_filter)

        preset_row = QtWidgets.QHBoxLayout()
        for text, callback in (
            ("GNSS", lambda: self._select_preset(("hdop", "pdop", "vdop", "nos", "diffage", "fixquality"))),
            ("Motion", lambda: self._select_preset(("sog", "speed", "hdg", "heading", "cog", "pitch", "roll", "heave"))),
            ("Depth", lambda: self._select_preset(("depth", "altitude", "elevation"))),
            ("Clear", lambda: self._set_all(False)),
        ):
            preset_icon = {
                "GNSS": icon("satellite", size=16),
                "Motion": icon("activity", size=16),
                "Depth": icon("chart", size=16),
                "Clear": icon("clear", size=16),
            }.get(text, QtGui.QIcon())
            button = QtWidgets.QPushButton(preset_icon, text)
            button.clicked.connect(callback)
            preset_row.addWidget(button)
        left_layout.addLayout(preset_row)

        self.channel_tree = QtWidgets.QTreeWidget()
        self.channel_tree.setHeaderLabels(["Channel", "Color", "Width"])
        self.channel_tree.setColumnWidth(0, 210)
        self.channel_tree.setColumnWidth(1, 48)
        self.channel_tree.setColumnWidth(2, 45)
        self.channel_tree.itemChanged.connect(self._channel_changed)
        self.channel_tree.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.channel_tree.customContextMenuRequested.connect(self._show_channel_context_menu)
        left_layout.addWidget(self.channel_tree, 1)
        self.selection_label = QtWidgets.QLabel("0 selected")
        left_layout.addWidget(self.selection_label)
        splitter.addWidget(left)

        self.plot_scroll = QtWidgets.QScrollArea()
        self.plot_scroll.setWidgetResizable(True)
        self.plot_container = QtWidgets.QWidget()
        self.plot_layout = QtWidgets.QVBoxLayout(self.plot_container)
        self.plot_layout.setContentsMargins(0, 0, 0, 0)
        self.plot_layout.setSpacing(6)
        self.plot_scroll.setWidget(self.plot_container)
        splitter.addWidget(self.plot_scroll)
        splitter.setSizes([330, 1170])

        self.status = QtWidgets.QLabel("Select a BlackBox file.")
        outer.addWidget(self.status)
        self.setCentralWidget(central)

    def _style(self, name: str) -> dict[str, Any]:
        return self._channel_styles.setdefault(name, {"color": stable_color(name), "width": 1.6})

    def set_files(self, files: list[BlackBoxFileInfo]) -> None:
        self._files = list(files)
        blocker = QtCore.QSignalBlocker(self.file_combo)
        previous = self.file_combo.currentData()
        self.file_combo.clear()
        for info in files:
            self.file_combo.addItem(info.label, info.file_id)
        if previous is not None:
            index = self.file_combo.findData(previous)
            if index >= 0:
                self.file_combo.setCurrentIndex(index)
        del blocker
        if self.file_combo.count():
            if self.file_combo.currentIndex() < 0:
                self.file_combo.setCurrentIndex(0)
            self._file_changed(self.file_combo.currentIndex())
        else:
            self.status.setText("No BlackBox files were found in this project.")

    def _file_changed(self, index: int) -> None:
        if index < 0:
            return
        file_id = self.file_combo.itemData(index)
        if file_id is not None:
            self.status.setText("Loading BlackBox data…")
            self.file_requested.emit(int(file_id))

    def set_loading_error(self, message: str) -> None:
        self.status.setText(message)

    def set_data(self, data: BlackBoxData) -> None:
        self._data = data
        self.track_source.clear()
        self.track_source.addItems(list(data.tracks.keys()))
        self._build_channel_tree(data)
        self._redraw()
        self.status.setText(
            f"{data.file_info.name}: {data.count:,} samples, "
            f"{len(data.columns)} QC channels, {len(data.tracks)} coordinate pairs"
        )

    def _group_for(self, name: str) -> str:
        low = name.lower()
        if any(x in low for x in ("hdop", "pdop", "vdop", "nos", "diffage", "fixquality", "refstation")):
            return "GNSS quality"
        if any(x in low for x in ("sog", "speed", "hdg", "heading", "cog", "pitch", "roll", "heave")):
            return "Motion"
        if any(x in low for x in ("depth", "altitude", "elevation", "barometer")):
            return "Depth / elevation"
        return "Other"

    def _build_channel_tree(self, data: BlackBoxData) -> None:
        previous = set(self._selected_channels())
        blocker = QtCore.QSignalBlocker(self.channel_tree)
        self.channel_tree.clear()
        groups: dict[str, QtWidgets.QTreeWidgetItem] = {}
        for name in data.columns:
            group_name = self._group_for(name)
            group = groups.get(group_name)
            if group is None:
                group = QtWidgets.QTreeWidgetItem([group_name, "", ""])
                group.setFlags(group.flags() & ~QtCore.Qt.ItemFlag.ItemIsUserCheckable)
                group.setExpanded(group_name != "Other")
                self.channel_tree.addTopLevelItem(group)
                groups[group_name] = group
            style = self._style(name)
            item = make_swatch_item(name, str(style["color"]), float(style["width"]), name in previous)
            group.addChild(item)
        del blocker
        self._update_selected_count()

    def _iter_items(self):
        for i in range(self.channel_tree.topLevelItemCount()):
            group = self.channel_tree.topLevelItem(i)
            for j in range(group.childCount()):
                yield group.child(j)

    def _selected_channels(self) -> list[str]:
        return [str(item.data(0, QtCore.Qt.ItemDataRole.UserRole)) for item in self._iter_items()
                if item.checkState(0) == QtCore.Qt.CheckState.Checked]

    def _channel_changed(self, _item, _column) -> None:
        self._update_selected_count()
        self._redraw_timer.start()

    def _update_selected_count(self) -> None:
        count = len(self._selected_channels())
        self.selection_label.setText(f"{count} selected")

    def _set_all(self, checked: bool) -> None:
        blocker = QtCore.QSignalBlocker(self.channel_tree)
        for item in self._iter_items():
            item.setCheckState(0, QtCore.Qt.CheckState.Checked if checked else QtCore.Qt.CheckState.Unchecked)
        del blocker
        self._update_selected_count()
        self._redraw()

    def _select_preset(self, tokens: tuple[str, ...]) -> None:
        blocker = QtCore.QSignalBlocker(self.channel_tree)
        selected = 0
        for item in self._iter_items():
            name = str(item.data(0, QtCore.Qt.ItemDataRole.UserRole) or "")
            checked = any(token in name.lower() for token in tokens) and selected < self.max_plots.value()
            item.setCheckState(0, QtCore.Qt.CheckState.Checked if checked else QtCore.Qt.CheckState.Unchecked)
            selected += int(checked)
        del blocker
        self._update_selected_count()
        self._redraw()

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

    def _show_channel_context_menu(self, position: QtCore.QPoint) -> None:
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
            self._channel_styles.pop(name, None)
            style = self._style(name)
        else:
            return
        color = QtGui.QColor(str(style["color"]))
        item.setBackground(1, QtGui.QBrush(color))
        item.setText(2, f"{float(style['width']):.1f}")
        self._redraw()

    def _redraw(self) -> None:
        self._redraw_timer.start()

    def _clear_plots(self) -> None:
        while self.plot_layout.count():
            item = self.plot_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()
        self._plot_widgets.clear()

    def _time_axis(self, data: BlackBoxData) -> tuple[float, str]:
        duration = float(np.nanmax(data.time_seconds)) if data.time_seconds.size else 0.0
        if duration >= 7200:
            return 3600.0, "Elapsed time (h)"
        if duration >= 300:
            return 60.0, "Elapsed time (min)"
        return 1.0, "Elapsed time (s)"

    def _redraw_now(self) -> None:
        self._clear_plots()
        data = self._data
        selected = self._selected_channels()
        if data is None or not selected:
            label = QtWidgets.QLabel("Select QC channels from the left panel")
            label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            self.plot_layout.addWidget(label, 1)
            return

        max_count = self.max_plots.value()
        if self.layout_combo.currentText() == "Stacked":
            selected = selected[:max_count]
            groups = [(name, [name]) for name in selected]
        else:
            groups = [("Selected BlackBox channels", selected[:max_count])]

        scale, bottom_label = self._time_axis(data)
        previous_plot = None
        for row, (title, channels) in enumerate(groups):
            axis_items = {
                "bottom": ElapsedAxis(orientation="bottom", unit_scale=scale),
                "left": PlainNumberAxis(orientation="left"),
            }
            widget = pg.PlotWidget(axisItems=axis_items)
            widget.setBackground(PLOT_BG)
            widget.setMinimumHeight(205 if len(groups) > 1 else 500)
            plot = widget.getPlotItem()
            style_plot(plot, title, "Value", bottom_label)
            if previous_plot is not None:
                plot.setXLink(previous_plot)
            previous_plot = plot
            if len(channels) > 1:
                plot.addLegend(offset=(-10, 10))

            plotted_values = []
            times = np.asarray(data.time_seconds, dtype=float)
            for name in channels:
                values = finite_qc(data.columns[name])
                finite = np.isfinite(times) & np.isfinite(values)
                if not finite.any():
                    continue
                style = self._style(name)
                curve = pg.PlotDataItem(
                    times[finite], values[finite],
                    pen=pg.mkPen(QtGui.QColor(str(style["color"])), width=float(style["width"])),
                    name=name,
                )
                plot.addItem(curve)
                curve.setDownsampling(auto=True, method="peak")
                curve.setClipToView(True)
                plotted_values.append(values[finite])
            y_range = robust_y_range(plotted_values)
            if y_range:
                plot.setYRange(*y_range, padding=0)
            if row != len(groups) - 1:
                plot.hideAxis("bottom")
            self.plot_layout.addWidget(widget)
            self._plot_widgets.append(widget)
        self.plot_layout.addStretch(1)

    def _add_track(self) -> None:
        if self._data is not None and self.track_source.currentText():
            self.add_track_requested.emit(self._data, self.track_source.currentText())

    def _add_all_tracks(self) -> None:
        if self._data is not None:
            self.add_all_tracks_requested.emit(self._data)

    def _add_all_files(self) -> None:
        file_ids = [info.file_id for info in self._files]
        if file_ids:
            self.add_all_files_requested.emit(file_ids)
