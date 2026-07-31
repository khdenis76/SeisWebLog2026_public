from __future__ import annotations

from PySide6 import QtCore, QtGui, QtWidgets

from .icons_manager import icon


class RibbonButton(QtWidgets.QToolButton):
    """Large Word-style ribbon command button."""

    def __init__(
        self,
        text: str,
        icon: QtGui.QIcon,
        *,
        checkable: bool = False,
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setText(text)
        self.setIcon(icon)
        self.setCheckable(checkable)
        self.setToolButtonStyle(QtCore.Qt.ToolButtonStyle.ToolButtonTextUnderIcon)
        self.setIconSize(QtCore.QSize(32, 32))
        self.setMinimumSize(74, 60)
        self.setAutoRaise(True)


class RibbonGroup(QtWidgets.QFrame):
    """A named command group displayed inside a ribbon tab."""

    def __init__(self, title: str, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.setFrameShape(QtWidgets.QFrame.Shape.StyledPanel)
        self.setObjectName("RibbonGroup")

        outer = QtWidgets.QVBoxLayout(self)
        outer.setContentsMargins(6, 4, 6, 2)
        outer.setSpacing(1)

        self.commands = QtWidgets.QHBoxLayout()
        self.commands.setContentsMargins(0, 0, 0, 0)
        self.commands.setSpacing(3)
        outer.addLayout(self.commands, 1)

        label = QtWidgets.QLabel(title)
        label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        label.setObjectName("RibbonGroupTitle")
        outer.addWidget(label)

    def add_button(self, button: QtWidgets.QToolButton) -> None:
        self.commands.addWidget(button)

    def add_widget(self, widget: QtWidgets.QWidget) -> None:
        self.commands.addWidget(widget)


class RibbonBar(QtWidgets.QTabWidget):
    """Compact Microsoft Office-style ribbon built from standard Qt widgets."""

    zoom_all_requested = QtCore.Signal()
    refresh_requested = QtCore.Signal()
    select_all_layers_requested = QtCore.Signal()
    clear_all_layers_requested = QtCore.Signal()
    measurement_toggled = QtCore.Signal(bool)
    clear_measurement_requested = QtCore.Signal()
    remove_last_measurement_requested = QtCore.Signal()
    grid_toggled = QtCore.Signal(bool)
    side_panel_toggled = QtCore.Signal(bool)
    bbox_open_requested = QtCore.Signal()
    bbox_reload_requested = QtCore.Signal()
    bbox_track_toggle_requested = QtCore.Signal(bool)
    bbox_zoom_requested = QtCore.Signal()
    dsr_open_qc_requested = QtCore.Signal()
    dsr_line_changed = QtCore.Signal(int)
    dsr_station_changed = QtCore.Signal(int, int)
    dsr_zoom_line_requested = QtCore.Signal(int)
    dsr_zoom_station_requested = QtCore.Signal(int, int)
    dsr_previous_station_requested = QtCore.Signal()
    dsr_next_station_requested = QtCore.Signal()
    dsr_auto_zoom_toggled = QtCore.Signal(bool)
    dsr_create_layer_requested = QtCore.Signal()
    dsr_manage_layers_requested = QtCore.Signal()
    project_change_requested = QtCore.Signal()
    project_folder_requested = QtCore.Signal()
    exit_requested = QtCore.Signal()
    export_map_requested = QtCore.Signal()
    export_selected_layer_requested = QtCore.Signal()
    export_visible_layers_requested = QtCore.Signal()
    report_project_requested = QtCore.Signal()
    report_dsr_line_requested = QtCore.Signal()
    reports_folder_requested = QtCore.Signal()
    svp3d_open_requested = QtCore.Signal()
    track3d_open_requested = QtCore.Signal()

    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("RibbonBar")
        self.setDocumentMode(True)
        self.setMovable(False)
        self.setTabPosition(QtWidgets.QTabWidget.TabPosition.North)
        self.setMaximumHeight(126)
        self._build_tabs()
        self._apply_style()

    def _standard_icon(self, standard: QtWidgets.QStyle.StandardPixmap) -> QtGui.QIcon:
        return self.style().standardIcon(standard)

    def _new_tab(self, name: str) -> tuple[QtWidgets.QWidget, QtWidgets.QHBoxLayout]:
        page = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout(page)
        layout.setContentsMargins(5, 4, 5, 4)
        layout.setSpacing(5)
        layout.addStretch(1)
        self.addTab(page, name)
        return page, layout

    @staticmethod
    def _insert_group(layout: QtWidgets.QHBoxLayout, group: RibbonGroup) -> None:
        layout.insertWidget(layout.count() - 1, group)

    def _build_tabs(self) -> None:
        # Project
        _, project = self._new_tab("Project")
        project_group = RibbonGroup("Project")
        self.change_project_button = RibbonButton("Change project", icon("change_project"))
        self.reload_project_button = RibbonButton("Reload", icon("reload"))
        self.project_folder_button = RibbonButton("Open folder", icon("project_folder"))
        self.exit_button = RibbonButton("Exit", icon("exit"))
        self.change_project_button.clicked.connect(self.project_change_requested)
        self.reload_project_button.clicked.connect(self.refresh_requested)
        self.project_folder_button.clicked.connect(self.project_folder_requested)
        self.exit_button.clicked.connect(self.exit_requested)
        for button in (self.change_project_button, self.reload_project_button, self.project_folder_button, self.exit_button):
            project_group.add_button(button)
        self._insert_group(project, project_group)

        # Home
        _, home = self._new_tab("Home")
        navigation_group = RibbonGroup("Map navigation")
        self.zoom_all_button = RibbonButton("Zoom all", icon("zoom_all"))
        self.zoom_all_button.clicked.connect(self.zoom_all_requested)
        navigation_group.add_button(self.zoom_all_button)
        self._insert_group(home, navigation_group)

        visibility_group = RibbonGroup("Layer visibility")
        self.show_all_button = RibbonButton("Show all", icon("show"))
        self.hide_all_button = RibbonButton("Hide all", icon("hide"))
        self.show_all_button.clicked.connect(self.select_all_layers_requested)
        self.hide_all_button.clicked.connect(self.clear_all_layers_requested)
        visibility_group.add_button(self.show_all_button)
        visibility_group.add_button(self.hide_all_button)
        self._insert_group(home, visibility_group)

        # Layers
        _, layers = self._new_tab("Layers")
        layer_group = RibbonGroup("Visibility")
        layer_group.add_button(RibbonButton("Show all", icon("show")))
        layer_group.add_button(RibbonButton("Hide all", icon("hide")))
        layer_group.commands.itemAt(0).widget().clicked.connect(self.select_all_layers_requested)
        layer_group.commands.itemAt(1).widget().clicked.connect(self.clear_all_layers_requested)
        self._insert_group(layers, layer_group)

        # Receiver QC / DSR
        _, dsr = self._new_tab("Receiver QC")
        dsr_select_group = RibbonGroup("Receiver selection")
        selector_widget = QtWidgets.QWidget()
        selector_layout = QtWidgets.QFormLayout(selector_widget)
        selector_layout.setContentsMargins(4, 0, 4, 0)
        selector_layout.setHorizontalSpacing(5)
        selector_layout.setVerticalSpacing(3)
        self.dsr_line_combo = QtWidgets.QComboBox()
        self.dsr_line_combo.setEditable(True)
        self.dsr_line_combo.setInsertPolicy(QtWidgets.QComboBox.InsertPolicy.NoInsert)
        self.dsr_line_combo.setMinimumWidth(130)
        self.dsr_station_combo = QtWidgets.QComboBox()
        self.dsr_station_combo.setEditable(True)
        self.dsr_station_combo.setInsertPolicy(QtWidgets.QComboBox.InsertPolicy.NoInsert)
        self.dsr_station_combo.setMinimumWidth(130)
        selector_layout.addRow("Line:", self.dsr_line_combo)
        selector_layout.addRow("Station:", self.dsr_station_combo)
        dsr_select_group.add_widget(selector_widget)
        self._insert_group(dsr, dsr_select_group)

        dsr_navigation_group = RibbonGroup("Station navigation")
        self.dsr_previous_button = RibbonButton("Previous", icon("previous"))
        self.dsr_next_button = RibbonButton("Next", icon("next"))
        self.dsr_zoom_line_button = RibbonButton("Zoom line", icon("zoom_line"))
        self.dsr_zoom_station_button = RibbonButton("Zoom station", icon("zoom_station"))
        self.dsr_open_qc_button = RibbonButton("Open QC", icon("qc"))
        self.dsr_auto_zoom = QtWidgets.QCheckBox("Auto zoom")
        self.dsr_auto_zoom.setChecked(True)
        auto_widget = QtWidgets.QWidget()
        auto_layout = QtWidgets.QVBoxLayout(auto_widget)
        auto_layout.setContentsMargins(6, 6, 6, 6)
        auto_layout.addWidget(self.dsr_auto_zoom)
        auto_layout.addStretch(1)
        self.dsr_zoom_line_button.clicked.connect(self._emit_dsr_zoom_line)
        self.dsr_zoom_station_button.clicked.connect(self._emit_dsr_zoom_station)
        self.dsr_open_qc_button.clicked.connect(self.dsr_open_qc_requested)
        self.dsr_previous_button.clicked.connect(self.dsr_previous_station_requested)
        self.dsr_next_button.clicked.connect(self.dsr_next_station_requested)
        self.dsr_auto_zoom.toggled.connect(self.dsr_auto_zoom_toggled)
        self.dsr_line_combo.currentIndexChanged.connect(self._emit_dsr_line_changed)
        self.dsr_station_combo.currentIndexChanged.connect(self._emit_dsr_station_changed)
        for button in (self.dsr_previous_button, self.dsr_next_button, self.dsr_zoom_line_button, self.dsr_zoom_station_button):
            dsr_navigation_group.add_button(button)
        dsr_navigation_group.add_widget(auto_widget)
        dsr_navigation_group.add_button(self.dsr_open_qc_button)
        self._insert_group(dsr, dsr_navigation_group)

        dsr_layers_group = RibbonGroup("Custom layers")
        self.dsr_create_layer_button = RibbonButton("Create layer", icon("create"))
        self.dsr_manage_layers_button = RibbonButton("Manage", icon("manage"))
        self.dsr_create_layer_button.clicked.connect(self.dsr_create_layer_requested)
        self.dsr_manage_layers_button.clicked.connect(self.dsr_manage_layers_requested)
        dsr_layers_group.add_button(self.dsr_create_layer_button)
        dsr_layers_group.add_button(self.dsr_manage_layers_button)
        self._insert_group(dsr, dsr_layers_group)

        # BlackBox
        _, bbox = self._new_tab("BlackBox")
        bbox_group = RibbonGroup("Viewer")
        self.bbox_open_button = RibbonButton("Open QC", icon("qc"))
        self.bbox_reload_button = RibbonButton("Reload files", icon("reload"))
        self.bbox_track_button = RibbonButton("Track layers", icon("bbox_track"), checkable=True)
        self.bbox_zoom_button = RibbonButton("Zoom tracks", icon("zoom_line"))
        self.bbox_open_button.clicked.connect(self.bbox_open_requested)
        self.bbox_reload_button.clicked.connect(self.bbox_reload_requested)
        self.bbox_track_button.toggled.connect(self.bbox_track_toggle_requested)
        self.bbox_zoom_button.clicked.connect(self.bbox_zoom_requested)
        for button in (self.bbox_open_button, self.bbox_reload_button, self.bbox_track_button, self.bbox_zoom_button):
            bbox_group.add_button(button)
        self._insert_group(bbox, bbox_group)

        # SVP 3D
        _, svp3d = self._new_tab("SVP 3D")
        svp3d_group = RibbonGroup("Water column")
        self.svp3d_open_button = RibbonButton("Open 3D viewer", icon("svp"))
        self.svp3d_open_button.clicked.connect(self.svp3d_open_requested)
        svp3d_group.add_button(self.svp3d_open_button)
        self._insert_group(svp3d, svp3d_group)

        # DSR + BlackBox 3D
        _, track3d = self._new_tab("3D Tracks")
        track3d_group = RibbonGroup("Receiver and vehicle tracks")
        self.track3d_open_button = RibbonButton("Open 3D tracks", icon("bbox_track"))
        self.track3d_open_button.clicked.connect(self.track3d_open_requested)
        track3d_group.add_button(self.track3d_open_button)
        self._insert_group(track3d, track3d_group)

        # Export
        _, export = self._new_tab("Export")
        map_export_group = RibbonGroup("Map")
        self.export_map_button = RibbonButton("Map image", icon("export_map"))
        self.export_map_button.clicked.connect(self.export_map_requested)
        map_export_group.add_button(self.export_map_button)
        self._insert_group(export, map_export_group)
        data_export_group = RibbonGroup("Layer data")
        self.export_selected_button = RibbonButton("Selected layer", icon("export_csv"))
        self.export_visible_button = RibbonButton("Visible layers", icon("export_visible"))
        self.export_selected_button.clicked.connect(self.export_selected_layer_requested)
        self.export_visible_button.clicked.connect(self.export_visible_layers_requested)
        data_export_group.add_button(self.export_selected_button)
        data_export_group.add_button(self.export_visible_button)
        self._insert_group(export, data_export_group)

        # Reports
        _, reports = self._new_tab("Reports")
        report_group = RibbonGroup("Generate")
        self.project_report_button = RibbonButton("Project summary", icon("report_project"))
        self.dsr_report_button = RibbonButton("Receiver line", icon("report_dsr"))
        self.project_report_button.clicked.connect(self.report_project_requested)
        self.dsr_report_button.clicked.connect(self.report_dsr_line_requested)
        report_group.add_button(self.project_report_button)
        report_group.add_button(self.dsr_report_button)
        self._insert_group(reports, report_group)
        report_folder_group = RibbonGroup("Output")
        self.reports_folder_button = RibbonButton("Open reports", icon("reports_folder"))
        self.reports_folder_button.clicked.connect(self.reports_folder_requested)
        report_folder_group.add_button(self.reports_folder_button)
        self._insert_group(reports, report_folder_group)

        # View
        _, view = self._new_tab("View")
        display_group = RibbonGroup("Display")
        self.grid_button = RibbonButton("Grid", icon("grid"), checkable=True)
        self.grid_button.setChecked(True)
        self.panel_button = RibbonButton("Side panel", icon("side_panel"), checkable=True)
        self.panel_button.setChecked(True)
        self.grid_button.toggled.connect(self.grid_toggled)
        self.panel_button.toggled.connect(self.side_panel_toggled)
        display_group.add_button(self.grid_button)
        display_group.add_button(self.panel_button)
        self._insert_group(view, display_group)

        # Tools
        _, tools = self._new_tab("Tools")
        distance_group = RibbonGroup("Measurement")
        self.measure_button = RibbonButton("Distance", icon("measure"), checkable=True)
        self.remove_last_button = RibbonButton("Undo point", icon("undo"))
        self.clear_measure_button = RibbonButton("Clear", icon("clear"))
        self.measure_button.toggled.connect(self.measurement_toggled)
        self.clear_measure_button.clicked.connect(self.clear_measurement_requested)
        self.remove_last_button.clicked.connect(self.remove_last_measurement_requested)
        distance_group.add_button(self.measure_button)
        distance_group.add_button(self.remove_last_button)
        distance_group.add_button(self.clear_measure_button)
        self._insert_group(tools, distance_group)

    def set_dsr_lines(self, lines: list[int], selected: int | None = None) -> None:
        blocker = QtCore.QSignalBlocker(self.dsr_line_combo)
        self.dsr_line_combo.clear()
        for line in lines:
            self.dsr_line_combo.addItem(str(line), int(line))
        if selected is not None:
            index = self.dsr_line_combo.findData(int(selected))
            if index >= 0:
                self.dsr_line_combo.setCurrentIndex(index)
        del blocker
        if self.dsr_line_combo.count() and self.dsr_line_combo.currentIndex() < 0:
            self.dsr_line_combo.setCurrentIndex(0)

    def set_dsr_stations(self, stations: list[int], selected: int | None = None) -> None:
        blocker = QtCore.QSignalBlocker(self.dsr_station_combo)
        self.dsr_station_combo.clear()
        for station in stations:
            self.dsr_station_combo.addItem(str(station), int(station))
        if selected is not None:
            index = self.dsr_station_combo.findData(int(selected))
            if index >= 0:
                self.dsr_station_combo.setCurrentIndex(index)
        del blocker

    def select_dsr_line(self, line: int) -> None:
        index = self.dsr_line_combo.findData(int(line))
        if index >= 0:
            self.dsr_line_combo.setCurrentIndex(index)

    def select_dsr_station(self, station: int) -> None:
        index = self.dsr_station_combo.findData(int(station))
        if index >= 0:
            self.dsr_station_combo.setCurrentIndex(index)

    def current_dsr_line(self) -> int | None:
        value = self.dsr_line_combo.currentData()
        return None if value is None else int(value)

    def current_dsr_station(self) -> int | None:
        value = self.dsr_station_combo.currentData()
        return None if value is None else int(value)

    def _emit_dsr_line_changed(self, _index: int) -> None:
        line = self.current_dsr_line()
        if line is not None:
            self.dsr_line_changed.emit(line)

    def _emit_dsr_station_changed(self, _index: int) -> None:
        line = self.current_dsr_line()
        station = self.current_dsr_station()
        if line is not None and station is not None:
            self.dsr_station_changed.emit(line, station)

    def _emit_dsr_zoom_line(self) -> None:
        line = self.current_dsr_line()
        if line is not None:
            self.dsr_zoom_line_requested.emit(line)

    def _emit_dsr_zoom_station(self) -> None:
        line = self.current_dsr_line()
        station = self.current_dsr_station()
        if line is not None and station is not None:
            self.dsr_zoom_station_requested.emit(line, station)

    def set_measurement_checked(self, enabled: bool) -> None:
        blocker = QtCore.QSignalBlocker(self.measure_button)
        self.measure_button.setChecked(enabled)
        del blocker

    def _apply_style(self) -> None:
        self.setStyleSheet(
            """
            QTabWidget#RibbonBar::pane {
                border: 1px solid palette(mid);
                background: palette(window);
            }
            QTabWidget#RibbonBar QTabBar::tab {
                min-width: 76px;
                padding: 6px 13px;
            }
            QFrame#RibbonGroup {
                border: 0;
                border-right: 1px solid palette(mid);
                border-radius: 0;
            }
            QLabel#RibbonGroupTitle {
                color: palette(mid);
                font-size: 10px;
                padding-top: 1px;
            }
            QToolButton {
                padding: 3px;
                border: 1px solid transparent;
                border-radius: 3px;
            }
            QToolButton:hover {
                border-color: palette(highlight);
                background: palette(alternate-base);
            }
            QToolButton:checked {
                border-color: palette(highlight);
                background: palette(highlight);
                color: palette(highlighted-text);
            }
            """
        )
