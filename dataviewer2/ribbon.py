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
    measurement_toggled = QtCore.Signal(bool)  # backward compatibility
    measurement_mode_requested = QtCore.Signal(str)
    clear_measurement_requested = QtCore.Signal()
    remove_last_measurement_requested = QtCore.Signal()
    grid_toggled = QtCore.Signal(bool)
    side_panel_toggled = QtCore.Signal(bool)  # backward-compatible alias for Layers panel
    layers_panel_toggled = QtCore.Signal(bool)
    feature_panel_toggled = QtCore.Signal(bool)
    measurement_panel_toggled = QtCore.Signal(bool)
    ocr_panel_toggled = QtCore.Signal(bool)
    heading_panel_toggled = QtCore.Signal(bool)
    status_bar_toggled = QtCore.Signal(bool)
    reset_layout_requested = QtCore.Signal()
    theme_toggled = QtCore.Signal(bool)
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
    labels_toggled = QtCore.Signal(bool)
    dsr_add_bbox_requested = QtCore.Signal(str)
    radial_circles_toggled = QtCore.Signal(bool)
    radial_circle_style_changed = QtCore.Signal(float, str, float, str)
    view3d_open_requested = QtCore.Signal()
    surface2d_open_requested = QtCore.Signal()
    sps_overlay_requested = QtCore.Signal()
    slsolution_overlay_requested = QtCore.Signal()
    geotiff_open_requested = QtCore.Signal()
    point_compare_requested = QtCore.Signal()
    node_selection_requested = QtCore.Signal(str)
    clear_node_selection_requested = QtCore.Signal()
    daily_dsr_production_requested = QtCore.Signal(str)
    source_preplot_load_requested = QtCore.Signal()
    source_points_load_requested = QtCore.Signal()
    tabular_point_import_requested = QtCore.Signal()

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

        optional_source_group = RibbonGroup("Optional source data")
        self.load_source_preplot_button = RibbonButton("Source preplot", icon("preplot"))
        self.load_source_preplot_button.setToolTip(
            "Load Source Preplot on demand. It is not loaded during startup."
        )
        self.load_source_points_button = RibbonButton("Source points", icon("station"))
        self.load_source_points_button.setToolTip(
            "Load SPSolution source-point layers on demand. They are not loaded during startup."
        )
        self.load_source_preplot_button.clicked.connect(self.source_preplot_load_requested)
        self.load_source_points_button.clicked.connect(self.source_points_load_requested)
        optional_source_group.add_button(self.load_source_preplot_button)
        optional_source_group.add_button(self.load_source_points_button)
        self._insert_group(layers, optional_source_group)

        import_group = RibbonGroup("Import")
        self.import_points_button = RibbonButton("Excel / CSV", icon("station"))
        self.import_points_button.setToolTip(
            "Import a spreadsheet as a point layer and choose coordinates, labels, and symbol style."
        )
        self.import_points_button.clicked.connect(self.tabular_point_import_requested)
        import_group.add_button(self.import_points_button)
        self._insert_group(layers, import_group)

        production_group = RibbonGroup("DSR production")
        self.daily_dsr_button = RibbonButton("Daily production", icon("calendar"))
        self.daily_dsr_button.setToolTip(
            "Create DSR production layers for a selected calendar day, split by vehicle and line."
        )
        self.daily_dsr_button.setPopupMode(
            QtWidgets.QToolButton.ToolButtonPopupMode.MenuButtonPopup
        )
        production_menu = QtWidgets.QMenu(self.daily_dsr_button)
        deployment_action = production_menu.addAction("Deployment…")
        recovery_action = production_menu.addAction("Recovery…")
        deployment_action.triggered.connect(
            lambda: self.daily_dsr_production_requested.emit("deployment")
        )
        recovery_action.triggered.connect(
            lambda: self.daily_dsr_production_requested.emit("recovery")
        )
        self.daily_dsr_button.setMenu(production_menu)
        self.daily_dsr_button.clicked.connect(
            lambda: self.daily_dsr_production_requested.emit("deployment")
        )
        production_group.add_button(self.daily_dsr_button)
        self._insert_group(layers, production_group)

        surface_group = RibbonGroup("Surfaces")
        self.surface2d_open_button = RibbonButton("Add surface", icon("map"))
        self.surface2d_open_button.setToolTip("Create heatmaps and contours for the main-map surface workflow.")
        self.surface2d_open_button.clicked.connect(self.surface2d_open_requested)
        self.sps_overlay_button = RibbonButton("SPS points", icon("station"))
        self.sps_overlay_button.setToolTip("Add production SPSolution points to the main map.")
        self.sps_overlay_button.clicked.connect(self.sps_overlay_requested)
        self.slsolution_overlay_button = RibbonButton("Source lines", icon("preplot"))
        self.slsolution_overlay_button.setToolTip("Add SLSolution StartXY–EndXY lines to the main map.")
        self.slsolution_overlay_button.clicked.connect(self.slsolution_overlay_requested)
        surface_group.add_button(self.surface2d_open_button)
        surface_group.add_button(self.sps_overlay_button)
        surface_group.add_button(self.slsolution_overlay_button)
        self.geotiff_button = RibbonButton("GeoTIFF", icon("map"))
        self.geotiff_button.setToolTip("Load a GeoTIFF as a color map, contours, or both.")
        self.geotiff_button.clicked.connect(self.geotiff_open_requested)
        surface_group.add_button(self.geotiff_button)
        self._insert_group(layers, surface_group)

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

        dsr_bbox_group = RibbonGroup("BlackBox by receiver line")
        self.dsr_add_bbox_button = RibbonButton("Add BBOX tracks", icon("bbox_track"))
        self.dsr_add_bbox_button.setToolTip(
            "Add BlackBox XY tracks for deployment, recovery, or both time windows on the selected receiver line."
        )
        self.dsr_add_bbox_button.setPopupMode(QtWidgets.QToolButton.ToolButtonPopupMode.MenuButtonPopup)
        bbox_menu = QtWidgets.QMenu(self.dsr_add_bbox_button)
        deployment_action = bbox_menu.addAction(icon("bbox_track"), "Deployment BBOX")
        recovery_action = bbox_menu.addAction(icon("bbox_track"), "Recovery BBOX")
        both_action = bbox_menu.addAction(icon("bbox_track"), "Deployment + Recovery")
        deployment_action.setToolTip("Use DSR.TimeStamp from the selected line.")
        recovery_action.setToolTip("Use DSR.TimeStamp1 from the selected line.")
        both_action.setToolTip("Load both deployment and recovery BlackBox tracks.")
        deployment_action.triggered.connect(lambda: self.dsr_add_bbox_requested.emit("deployment"))
        recovery_action.triggered.connect(lambda: self.dsr_add_bbox_requested.emit("recovery"))
        both_action.triggered.connect(lambda: self.dsr_add_bbox_requested.emit("both"))
        self.dsr_add_bbox_button.setMenu(bbox_menu)
        # Clicking the main part of the split button keeps the most common action.
        self.dsr_add_bbox_button.clicked.connect(
            lambda: self.dsr_add_bbox_requested.emit("deployment")
        )
        dsr_bbox_group.add_button(self.dsr_add_bbox_button)
        self._insert_group(dsr, dsr_bbox_group)

        radial_group = RibbonGroup("Preplot QC radius")
        radial_widget = QtWidgets.QWidget()
        radial_form = QtWidgets.QFormLayout(radial_widget)
        radial_form.setContentsMargins(4, 0, 4, 0)
        radial_form.setHorizontalSpacing(5)
        radial_form.setVerticalSpacing(2)
        self.radial_radius_spin = QtWidgets.QDoubleSpinBox()
        self.radial_radius_spin.setRange(0.01, 10000.0)
        self.radial_radius_spin.setDecimals(2)
        self.radial_radius_spin.setSuffix(" m")
        self.radial_radius_spin.setValue(5.0)
        self.radial_width_spin = QtWidgets.QDoubleSpinBox()
        self.radial_width_spin.setRange(0.2, 10.0)
        self.radial_width_spin.setSingleStep(0.5)
        self.radial_width_spin.setValue(1.5)
        self.radial_style_combo = QtWidgets.QComboBox()
        self.radial_style_combo.addItem("Solid", "solid")
        self.radial_style_combo.addItem("Dashed", "dash")
        self.radial_style_combo.addItem("Dotted", "dot")
        self.radial_style_combo.addItem("Dash-dot", "dashdot")
        self.radial_color = "#ff5252"
        self.radial_color_button = QtWidgets.QPushButton(self.radial_color)
        self.radial_color_button.setMinimumWidth(82)
        self.radial_color_button.clicked.connect(self._choose_radial_color)
        radial_form.addRow("Radius:", self.radial_radius_spin)
        radial_form.addRow("Color:", self.radial_color_button)
        radial_form.addRow("Width:", self.radial_width_spin)
        radial_form.addRow("Line:", self.radial_style_combo)
        radial_group.add_widget(radial_widget)
        self.radial_show_button = RibbonButton("Show circles", icon("show"), checkable=True)
        self.radial_show_button.toggled.connect(self.radial_circles_toggled)
        self.radial_apply_button = RibbonButton("Apply style", icon("style"))
        self.radial_apply_button.clicked.connect(self._emit_radial_style)
        radial_group.add_button(self.radial_show_button)
        radial_group.add_button(self.radial_apply_button)
        self._insert_group(dsr, radial_group)

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

        # 3D View
        _, view3d = self._new_tab("3D View")
        bathy_group = RibbonGroup("Bathymetry")
        self.view3d_open_button = RibbonButton("Open 3D view", icon("view3d"))
        self.view3d_open_button.setToolTip(
            "Create multiple 3D surfaces with independent Z/color fields and add point/vector overlays with optional labels."
        )
        self.view3d_open_button.clicked.connect(self.view3d_open_requested)
        bathy_group.add_button(self.view3d_open_button)
        self._insert_group(view3d, bathy_group)

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

        panels_group = RibbonGroup("Panels")
        self.layers_panel_button = RibbonButton("Layers panel", icon("side_panel"), checkable=True)
        self.layers_panel_button.setChecked(True)
        self.feature_panel_button = RibbonButton("Feature panel", icon("properties"), checkable=True)
        self.feature_panel_button.setChecked(True)
        self.measurement_panel_button = RibbonButton("Measurement", icon("measure"), checkable=True)
        self.measurement_panel_button.setChecked(True)
        self.ocr_panel_button = RibbonButton("OCR Images", icon("image"), checkable=True)
        self.ocr_panel_button.setChecked(False)
        self.heading_panel_button = RibbonButton("Heading", icon("measure_bearing"), checkable=True)
        self.heading_panel_button.setChecked(True)
        self.status_bar_button = RibbonButton("Status bar", icon("activity"), checkable=True)
        self.status_bar_button.setChecked(True)
        self.layers_panel_button.toggled.connect(self.layers_panel_toggled)
        self.layers_panel_button.toggled.connect(self.side_panel_toggled)
        self.feature_panel_button.toggled.connect(self.feature_panel_toggled)
        self.measurement_panel_button.toggled.connect(self.measurement_panel_toggled)
        self.ocr_panel_button.toggled.connect(self.ocr_panel_toggled)
        self.heading_panel_button.toggled.connect(self.heading_panel_toggled)
        self.status_bar_button.toggled.connect(self.status_bar_toggled)
        for button in (
            self.layers_panel_button,
            self.feature_panel_button,
            self.measurement_panel_button,
            self.ocr_panel_button,
            self.heading_panel_button,
            self.status_bar_button,
        ):
            panels_group.add_button(button)
        self._insert_group(view, panels_group)

        display_group = RibbonGroup("Map display")
        self.grid_button = RibbonButton("Grid", icon("grid"), checkable=True)
        self.grid_button.setChecked(True)
        self.labels_button = RibbonButton("Line / point labels", icon("station"), checkable=True)
        self.labels_button.setChecked(False)
        self.grid_button.toggled.connect(self.grid_toggled)
        self.labels_button.toggled.connect(self.labels_toggled)
        display_group.add_button(self.grid_button)
        display_group.add_button(self.labels_button)
        self._insert_group(view, display_group)

        appearance_group = RibbonGroup("Appearance")
        self.night_mode_button = RibbonButton("Night mode", icon("theme"), checkable=True)
        self.night_mode_button.setToolTip("Switch between Day and Night interface colors")
        self.night_mode_button.toggled.connect(self.theme_toggled)
        appearance_group.add_button(self.night_mode_button)
        self._insert_group(view, appearance_group)

        workspace_group = RibbonGroup("Workspace")
        self.reset_layout_button = RibbonButton("Reset layout", icon("reload"))
        self.reset_layout_button.clicked.connect(self.reset_layout_requested)
        workspace_group.add_button(self.reset_layout_button)
        self._insert_group(view, workspace_group)

        # Backward-compatible attribute used by older MainWindow builds.
        self.panel_button = self.layers_panel_button

        # Measurement
        _, measurement = self._new_tab("Measurement")
        mode_group = RibbonGroup("Measurement mode")
        self.measure_button_group = QtWidgets.QButtonGroup(self)
        # Exclusivity is managed in _select_measurement_mode so the active
        # button can be clicked a second time to turn measurement off.
        self.measure_button_group.setExclusive(False)
        self.measure_buttons: dict[str, RibbonButton] = {}
        for mode, title, icon_key in (
            ("distance", "Distance", "measure_distance"),
            ("area", "Area", "measure_area"),
            ("bearing", "Bearing", "measure_bearing"),
            ("angle", "Angle", "measure_angle"),
        ):
            button = RibbonButton(title, icon(icon_key), checkable=True)
            button.clicked.connect(lambda checked=False, value=mode: self._select_measurement_mode(value, checked))
            self.measure_button_group.addButton(button)
            self.measure_buttons[mode] = button
            mode_group.add_button(button)
        self.measure_button = self.measure_buttons["distance"]  # compatibility
        self._insert_group(measurement, mode_group)

        edit_group = RibbonGroup("Measurement edit")
        self.remove_last_button = RibbonButton("Undo point", icon("undo"))
        self.clear_measure_button = RibbonButton("Clear", icon("clear"))
        self.remove_last_button.clicked.connect(self.remove_last_measurement_requested)
        self.clear_measure_button.clicked.connect(self.clear_measurement_requested)
        edit_group.add_button(self.remove_last_button)
        edit_group.add_button(self.clear_measure_button)
        self._insert_group(measurement, edit_group)

        compare_group = RibbonGroup("Point comparison")
        self.point_compare_button = RibbonButton("Compare points", icon("measure_bearing"))
        self.point_compare_button.setToolTip("Draw distance and bearing lines between matching points in two layers.")
        self.point_compare_button.clicked.connect(self.point_compare_requested)
        compare_group.add_button(self.point_compare_button)
        self._insert_group(measurement, compare_group)

        # Node selection
        _, selection = self._new_tab("Select")
        selection_group = RibbonGroup("Select nodes")
        self.node_select_button_group = QtWidgets.QButtonGroup(self)
        self.node_select_button_group.setExclusive(True)
        self.radius_select_button = RibbonButton(
            "By radius", icon("select_radius"), checkable=True
        )
        self.polygon_select_button = RibbonButton(
            "Draw polygon", icon("select_polygon"), checkable=True
        )
        self.shape_select_button = RibbonButton(
            "Shape polygon", icon("select_shape")
        )
        self.node_select_button_group.addButton(self.radius_select_button)
        self.node_select_button_group.addButton(self.polygon_select_button)
        self.radius_select_button.clicked.connect(
            lambda: self.node_selection_requested.emit("radius")
        )
        self.polygon_select_button.clicked.connect(
            lambda: self.node_selection_requested.emit("polygon")
        )
        self.shape_select_button.clicked.connect(
            lambda: self.node_selection_requested.emit("shape")
        )
        selection_group.add_button(self.radius_select_button)
        selection_group.add_button(self.polygon_select_button)
        selection_group.add_button(self.shape_select_button)
        self._insert_group(selection, selection_group)

        options_group = RibbonGroup("Selection options")
        options_widget = QtWidgets.QWidget()
        options_form = QtWidgets.QFormLayout(options_widget)
        options_form.setContentsMargins(4, 0, 4, 0)
        self.selection_location_combo = QtWidgets.QComboBox()
        self.selection_location_combo.addItems(["Inside", "Outside"])
        self.selection_radius_spin = QtWidgets.QDoubleSpinBox()
        self.selection_radius_spin.setRange(0.01, 1000000.0)
        self.selection_radius_spin.setDecimals(2)
        self.selection_radius_spin.setValue(25.0)
        self.selection_radius_spin.setSuffix(" m")
        options_form.addRow("Nodes:", self.selection_location_combo)
        options_form.addRow("Radius:", self.selection_radius_spin)
        options_group.add_widget(options_widget)
        self._insert_group(selection, options_group)

        edit_selection_group = RibbonGroup("Selection edit")
        self.clear_node_selection_button = RibbonButton("Clear", icon("clear"))
        self.clear_node_selection_button.clicked.connect(
            self.clear_node_selection_requested
        )
        edit_selection_group.add_button(self.clear_node_selection_button)
        self._insert_group(selection, edit_selection_group)

        # Tools remains separate for non-measurement utilities.
        _, tools = self._new_tab("Tools")
        tools_group = RibbonGroup("Utilities")
        tools_note = QtWidgets.QLabel("Project and map utilities")
        tools_note.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        tools_note.setMinimumWidth(150)
        tools_group.add_widget(tools_note)
        self._insert_group(tools, tools_group)


    def set_theme_checked(self, theme: str) -> None:
        """Synchronize the View-ribbon Day/Night switch without emitting a signal."""
        blocker = QtCore.QSignalBlocker(self.night_mode_button)
        self.night_mode_button.setChecked(str(theme).lower() == "night")
        self.night_mode_button.setText("Night mode" if self.night_mode_button.isChecked() else "Day mode")
        del blocker

    def selection_inside(self) -> bool:
        return self.selection_location_combo.currentText() == "Inside"

    def selection_radius(self) -> float:
        return float(self.selection_radius_spin.value())

    def clear_node_selection_mode(self) -> None:
        self.node_select_button_group.setExclusive(False)
        self.radius_select_button.setChecked(False)
        self.polygon_select_button.setChecked(False)
        self.node_select_button_group.setExclusive(True)

    def _select_measurement_mode(self, mode: str, checked: bool = True) -> None:
        button = self.measure_buttons.get(mode)
        if not checked:
            self.measurement_toggled.emit(False)
            return
        for key, candidate in self.measure_buttons.items():
            blocker = QtCore.QSignalBlocker(candidate)
            candidate.setChecked(key == mode)
            del blocker
        if button is not None:
            self.measurement_mode_requested.emit(mode)
            self.measurement_toggled.emit(True)

    def set_radial_default(self, radius: float) -> None:
        self.radial_radius_spin.setValue(max(0.01, float(radius)))

    def _choose_radial_color(self) -> None:
        color = QtWidgets.QColorDialog.getColor(QtGui.QColor(self.radial_color), self, "QC circle color")
        if not color.isValid():
            return
        self.radial_color = color.name()
        self.radial_color_button.setText(self.radial_color)
        self.radial_color_button.setStyleSheet(f"background:{self.radial_color};")
        self._emit_radial_style()

    def _emit_radial_style(self) -> None:
        self.radial_circle_style_changed.emit(
            float(self.radial_radius_spin.value()),
            str(self.radial_color),
            float(self.radial_width_spin.value()),
            str(self.radial_style_combo.currentData()),
        )

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

    def set_panel_button_checked(self, panel: str, visible: bool) -> None:
        button = {
            "layers": getattr(self, "layers_panel_button", None),
            "feature": getattr(self, "feature_panel_button", None),
            "measurement": getattr(self, "measurement_panel_button", None),
            "ocr": getattr(self, "ocr_panel_button", None),
            "heading": getattr(self, "heading_panel_button", None),
            "status": getattr(self, "status_bar_button", None),
        }.get(panel)
        if button is None:
            return
        blocker = QtCore.QSignalBlocker(button)
        button.setChecked(bool(visible))
        del blocker

    def set_measurement_checked(self, enabled: bool, mode: str | None = None) -> None:
        target = mode or "distance"
        for key, button in getattr(self, "measure_buttons", {}).items():
            blocker = QtCore.QSignalBlocker(button)
            button.setChecked(bool(enabled and key == target))
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
