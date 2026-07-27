from __future__ import annotations

from PySide6 import QtCore, QtGui, QtWidgets


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
        self.setIconSize(QtCore.QSize(28, 28))
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
        _, home = self._new_tab("Home")
        project_group = RibbonGroup("Project")
        self.refresh_button = RibbonButton(
            "Reload",
            self._standard_icon(QtWidgets.QStyle.StandardPixmap.SP_BrowserReload),
        )
        self.refresh_button.clicked.connect(self.refresh_requested)
        project_group.add_button(self.refresh_button)
        self._insert_group(home, project_group)

        navigation_group = RibbonGroup("Navigation")
        self.zoom_all_button = RibbonButton(
            "Zoom all",
            self._standard_icon(QtWidgets.QStyle.StandardPixmap.SP_DesktopIcon),
        )
        self.zoom_all_button.clicked.connect(self.zoom_all_requested)
        navigation_group.add_button(self.zoom_all_button)
        self._insert_group(home, navigation_group)

        _, layers = self._new_tab("Layers")
        visibility_group = RibbonGroup("Visibility")
        self.show_all_button = RibbonButton(
            "Show all",
            self._standard_icon(QtWidgets.QStyle.StandardPixmap.SP_DialogApplyButton),
        )
        self.hide_all_button = RibbonButton(
            "Hide all",
            self._standard_icon(QtWidgets.QStyle.StandardPixmap.SP_DialogCancelButton),
        )
        self.show_all_button.clicked.connect(self.select_all_layers_requested)
        self.hide_all_button.clicked.connect(self.clear_all_layers_requested)
        visibility_group.add_button(self.show_all_button)
        visibility_group.add_button(self.hide_all_button)
        self._insert_group(layers, visibility_group)

        _, measure = self._new_tab("Measure")
        distance_group = RibbonGroup("Distance")
        self.measure_button = RibbonButton(
            "Distance",
            self._standard_icon(QtWidgets.QStyle.StandardPixmap.SP_ArrowRight),
            checkable=True,
        )
        self.clear_measure_button = RibbonButton(
            "Clear",
            self._standard_icon(QtWidgets.QStyle.StandardPixmap.SP_TrashIcon),
        )
        self.remove_last_button = RibbonButton(
            "Undo point",
            self._standard_icon(QtWidgets.QStyle.StandardPixmap.SP_ArrowBack),
        )
        self.measure_button.toggled.connect(self.measurement_toggled)
        self.clear_measure_button.clicked.connect(self.clear_measurement_requested)
        self.remove_last_button.clicked.connect(self.remove_last_measurement_requested)
        distance_group.add_button(self.measure_button)
        distance_group.add_button(self.remove_last_button)
        distance_group.add_button(self.clear_measure_button)
        self._insert_group(measure, distance_group)

        _, view = self._new_tab("View")
        display_group = RibbonGroup("Display")
        self.grid_button = RibbonButton(
            "Grid",
            self._standard_icon(QtWidgets.QStyle.StandardPixmap.SP_FileDialogDetailedView),
            checkable=True,
        )
        self.grid_button.setChecked(True)
        self.panel_button = RibbonButton(
            "Side panel",
            self._standard_icon(QtWidgets.QStyle.StandardPixmap.SP_FileDialogListView),
            checkable=True,
        )
        self.panel_button.setChecked(True)
        self.grid_button.toggled.connect(self.grid_toggled)
        self.panel_button.toggled.connect(self.side_panel_toggled)
        display_group.add_button(self.grid_button)
        display_group.add_button(self.panel_button)
        self._insert_group(view, display_group)

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
