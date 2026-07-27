from __future__ import annotations

import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore, QtWidgets


from .models import BlackBoxData, BlackBoxFileInfo


class BlackBoxWindow(QtWidgets.QMainWindow):
    """Detachable BlackBox QC workbench with linked PyQtGraph plots."""

    file_requested = QtCore.Signal(int)
    add_track_requested = QtCore.Signal(object, str)
    add_all_tracks_requested = QtCore.Signal(object)
    add_all_files_requested = QtCore.Signal(object)
    zoom_track_requested = QtCore.Signal()
    reload_files_requested = QtCore.Signal()

    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("BlackBox QC — DataViewer 2.0")
        self.resize(1350, 820)
        self._data: BlackBoxData | None = None
        self._files: list[BlackBoxFileInfo] = []
        self._build_ui()

    def _build_ui(self) -> None:
        central = QtWidgets.QWidget()
        outer = QtWidgets.QVBoxLayout(central)
        outer.setContentsMargins(5, 5, 5, 5)

        controls = QtWidgets.QHBoxLayout()
        controls.addWidget(QtWidgets.QLabel("BlackBox file:"))
        self.file_combo = QtWidgets.QComboBox()
        self.file_combo.setMinimumWidth(420)
        self.file_combo.currentIndexChanged.connect(self._file_changed)
        controls.addWidget(self.file_combo, 1)

        self.reload_button = QtWidgets.QPushButton("Reload list")
        self.reload_button.clicked.connect(self.reload_files_requested)
        controls.addWidget(self.reload_button)

        self.track_source = QtWidgets.QComboBox()
        self.track_source.setMinimumWidth(130)
        controls.addWidget(QtWidgets.QLabel("Coordinate pair:"))
        controls.addWidget(self.track_source)

        self.add_track_button = QtWidgets.QPushButton("Add selected pair")
        self.add_track_button.clicked.connect(self._add_track)
        controls.addWidget(self.add_track_button)

        self.add_all_tracks_button = QtWidgets.QPushButton("Add all pairs")
        self.add_all_tracks_button.clicked.connect(self._add_all_tracks)
        controls.addWidget(self.add_all_tracks_button)

        self.add_all_files_button = QtWidgets.QPushButton("Add all files + pairs")
        self.add_all_files_button.clicked.connect(self._add_all_files)
        controls.addWidget(self.add_all_files_button)

        self.zoom_button = QtWidgets.QPushButton("Zoom tracks")
        self.zoom_button.clicked.connect(self.zoom_track_requested)
        controls.addWidget(self.zoom_button)
        outer.addLayout(controls)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        self.channel_tree = QtWidgets.QTreeWidget()
        self.channel_tree.setHeaderLabels(["Channel", "Display"])
        self.channel_tree.setMinimumWidth(230)
        self.channel_tree.itemChanged.connect(self._channel_changed)
        splitter.addWidget(self.channel_tree)

        self.plot_scroll = QtWidgets.QScrollArea()
        self.plot_scroll.setWidgetResizable(True)
        self.plot_container = QtWidgets.QWidget()
        self.plot_layout = QtWidgets.QVBoxLayout(self.plot_container)
        self.plot_layout.setContentsMargins(0, 0, 0, 0)
        self.plot_layout.setSpacing(4)
        self.plot_scroll.setWidget(self.plot_container)
        splitter.addWidget(self.plot_scroll)

        self._plot_widgets: list[pg.PlotWidget] = []
        self._redraw_timer = QtCore.QTimer(self)
        self._redraw_timer.setSingleShot(True)
        self._redraw_timer.setInterval(40)
        self._redraw_timer.timeout.connect(self._redraw_now)
        splitter.setStretchFactor(1, 1)
        outer.addWidget(splitter, 1)

        self.status = QtWidgets.QLabel("Select a BlackBox file.")
        outer.addWidget(self.status)
        self.setCentralWidget(central)

    def set_files(self, files: list[BlackBoxFileInfo]) -> None:
        self._files = list(files)
        blocker = QtCore.QSignalBlocker(self.file_combo)
        previous = self.file_combo.currentData()
        self.file_combo.clear()
        for info in files:
            self.file_combo.addItem(info.label, info.file_id)
        if previous is not None:
            idx = self.file_combo.findData(previous)
            if idx >= 0:
                self.file_combo.setCurrentIndex(idx)
        del blocker
        if self.file_combo.count() and self.file_combo.currentIndex() < 0:
            self.file_combo.setCurrentIndex(0)
        if self.file_combo.count():
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
        self.status.setText(f"{data.file_info.name}: {data.count:,} samples, {len(data.columns)} QC channels, {len(data.tracks)} tracks")

    def _build_channel_tree(self, data: BlackBoxData) -> None:
        blocker = QtCore.QSignalBlocker(self.channel_tree)
        self.channel_tree.clear()
        groups = {
            "GNSS quality": ["HDOP", "PDOP", "VDOP", "NOS", "DiffAge", "FixQuality"],
            "Motion": ["SOG", "Speed", "HDG", "Heading", "COG", "Pitch", "Roll", "Heave"],
            "Depth": ["Depth1", "Depth2", "Depth", "Altitude", "WaterDepth"],
            "Other": [],
        }
        assigned: set[str] = set()
        for group_name, candidates in groups.items():
            group = QtWidgets.QTreeWidgetItem([group_name, ""])
            group.setFlags(group.flags() & ~QtCore.Qt.ItemFlag.ItemIsUserCheckable)
            for name in candidates:
                actual = next((key for key in data.columns if key.lower() == name.lower()), None)
                if actual is None or actual in assigned:
                    continue
                child = QtWidgets.QTreeWidgetItem([actual, ""])
                child.setFlags(child.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
                child.setCheckState(0, QtCore.Qt.CheckState.Checked if len(assigned) < 6 else QtCore.Qt.CheckState.Unchecked)
                child.setData(0, QtCore.Qt.ItemDataRole.UserRole, actual)
                group.addChild(child)
                assigned.add(actual)
            if group_name == "Other":
                for actual in data.columns:
                    if actual in assigned:
                        continue
                    child = QtWidgets.QTreeWidgetItem([actual, ""])
                    child.setFlags(child.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
                    child.setCheckState(0, QtCore.Qt.CheckState.Unchecked)
                    child.setData(0, QtCore.Qt.ItemDataRole.UserRole, actual)
                    group.addChild(child)
                    assigned.add(actual)
            if group.childCount():
                group.setExpanded(group_name != "Other")
                self.channel_tree.addTopLevelItem(group)
        del blocker

    def _channel_changed(self, _item: QtWidgets.QTreeWidgetItem, _column: int) -> None:
        # QTreeWidget can emit several itemChanged signals in one event cycle.
        # Debouncing prevents pyqtgraph items from being destroyed and recreated
        # re-entrantly while Qt is still processing an item-change callback.
        self._redraw_timer.start()

    def _selected_channels(self) -> list[str]:
        result: list[str] = []
        root = self.channel_tree.invisibleRootItem()
        for i in range(root.childCount()):
            group = root.child(i)
            for j in range(group.childCount()):
                child = group.child(j)
                if child.checkState(0) == QtCore.Qt.CheckState.Checked:
                    value = child.data(0, QtCore.Qt.ItemDataRole.UserRole)
                    if value:
                        result.append(str(value))
        return result

    def _redraw(self) -> None:
        # Public redraw entry point. Queue the actual rebuild so it never runs
        # recursively from a Qt itemChanged callback.
        self._redraw_timer.start()

    def _clear_plots(self) -> None:
        while self.plot_layout.count():
            item = self.plot_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()
        self._plot_widgets.clear()

    def _redraw_now(self) -> None:
        self._clear_plots()
        data = self._data
        if data is None or not data.count:
            return

        selected = self._selected_channels()
        if not selected:
            label = QtWidgets.QLabel("Select channels in the left panel")
            label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            self.plot_layout.addWidget(label, 1)
            return

        previous_plot_item = None
        for row, name in enumerate(selected):
            widget = pg.PlotWidget()
            widget.setBackground(None)
            plot = widget.getPlotItem()
            plot.setTitle(name)
            plot.showGrid(x=True, y=True, alpha=0.2)
            plot.setLabel("left", name)

            values = np.asarray(data.columns[name], dtype=float)
            times = np.asarray(data.time_seconds, dtype=float)
            finite = np.isfinite(values) & np.isfinite(times)
            if finite.any():
                curve = pg.PlotDataItem(
                    times[finite],
                    values[finite],
                    pen=pg.mkPen(width=1),
                )
                # Configure optimization only after the item belongs to a real
                # PlotItem/ViewBox. This avoids the GraphicsLayoutWidget parent
                # confusion seen in pyqtgraph's auto-downsample path.
                plot.addItem(curve)
                curve.setDownsampling(auto=True, method="peak")
                curve.setClipToView(True)

            if previous_plot_item is not None:
                plot.setXLink(previous_plot_item)
            previous_plot_item = plot

            if row != len(selected) - 1:
                plot.hideAxis("bottom")
            else:
                plot.setLabel("bottom", "Elapsed time", units="s")

            widget.setMinimumHeight(145)
            self.plot_layout.addWidget(widget)
            self._plot_widgets.append(widget)

        self.plot_layout.addStretch(1)

    def _add_track(self) -> None:
        if self._data is None:
            return
        source = self.track_source.currentText()
        if source:
            self.add_track_requested.emit(self._data, source)

    def _add_all_tracks(self) -> None:
        if self._data is not None:
            self.add_all_tracks_requested.emit(self._data)

    def _add_all_files(self) -> None:
        file_ids = [info.file_id for info in self._files]
        if file_ids:
            self.status.setText(f"Loading all coordinate pairs from {len(file_ids)} BlackBox files…")
            self.add_all_files_requested.emit(file_ids)
