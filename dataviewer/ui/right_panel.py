from PySide6 import QtCore, QtWidgets
from ..widgets.rotating_svg import RotatingSvgView


class InfoCard(QtWidgets.QGroupBox):
    def __init__(self, title: str, parent=None):
        super().__init__(title, parent)
        lay = QtWidgets.QFormLayout(self)
        lay.setContentsMargins(10, 10, 10, 10)
        lay.setHorizontalSpacing(12)
        lay.setVerticalSpacing(6)
        self._rows = {}

    def set_value(self, key: str, value):
        if key not in self._rows:
            lbl = QtWidgets.QLabel("-")
            lbl.setTextInteractionFlags(QtCore.Qt.TextInteractionFlag.TextSelectableByMouse)
            self.layout().addRow(f"{key}:", lbl)
            self._rows[key] = lbl
        self._rows[key].setText("-" if value is None else str(value))


class RightPanel(QtWidgets.QFrame):
    closeRequested = QtCore.Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameShape(QtWidgets.QFrame.Shape.StyledPanel)
        self.setMinimumWidth(320)

        main_layout = QtWidgets.QVBoxLayout(self)
        main_layout.setContentsMargins(8, 8, 8, 8)
        main_layout.setSpacing(8)

        header = QtWidgets.QHBoxLayout()
        self.lbl_title = QtWidgets.QLabel("Inspector")
        self.lbl_title.setStyleSheet("font-weight:700; font-size:14px;")
        header.addWidget(self.lbl_title)
        header.addStretch(1)

        self.btn_close = QtWidgets.QToolButton()
        self.btn_close.setText("✕")
        self.btn_close.setToolTip("Close panel")
        self.btn_close.setAutoRaise(True)
        header.addWidget(self.btn_close)
        main_layout.addLayout(header)

        self.tabs = QtWidgets.QTabWidget()
        self.tabs.setTabPosition(QtWidgets.QTabWidget.TabPosition.North)
        main_layout.addWidget(self.tabs, 1)

        self._build_info_tab()
        self._build_heading_tab()
        self._build_notes_tab()

        self.btn_close.clicked.connect(self.closeRequested.emit)

    def _build_info_tab(self):
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        self.selection_card = InfoCard("Selection")
        self.selection_card.set_value("Layer", "-")
        self.selection_card.set_value("Line", "-")
        self.selection_card.set_value("Station", "-")
        self.selection_card.set_value("Node", "-")

        self.coords_card = InfoCard("Coordinates")
        self.coords_card.set_value("X", "-")
        self.coords_card.set_value("Y", "-")

        self.metrics_card = InfoCard("Metrics")
        self.metrics_card.set_value("Project", "-")
        self.metrics_card.set_value("Cursor", "-")
        self.metrics_card.set_value("Visible", "-")

        layout.addWidget(self.selection_card)
        layout.addWidget(self.coords_card)
        layout.addWidget(self.metrics_card)
        layout.addStretch(1)
        self.tabs.addTab(tab, "Info")

    def _build_heading_tab(self):
        from pathlib import Path

        tab = QtWidgets.QWidget()
        v = QtWidgets.QVBoxLayout(tab)
        v.setContentsMargins(0, 0, 0, 0)
        v.setSpacing(8)

        base_svg = Path(__file__).resolve().parents[2] / "baseproject" / "static" / "baseproject" / "svg"
        windrose_bg = base_svg / "windrose_inv.svg"
        vessel_svg_path = base_svg / "vessel.svg"
        rov_svg_path = base_svg / "rov_red.svg"

        self.vessel_svg = RotatingSvgView(str(vessel_svg_path), parent=None, bg_svg_path=None, fg_scale=1, bg_scale=0.0)
        self.rov1_svg = RotatingSvgView(str(rov_svg_path), parent=None, bg_svg_path=str(windrose_bg), fg_scale=1, bg_scale=1.0)
        self.rov2_svg = RotatingSvgView(str(rov_svg_path), parent=None, bg_svg_path=str(windrose_bg), fg_scale=1, bg_scale=1.0)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        splitter.addWidget(self.vessel_svg)
        splitter.addWidget(self.rov1_svg)
        splitter.addWidget(self.rov2_svg)
        splitter.setSizes([220, 180, 180])
        v.addWidget(splitter, 1)
        self.tabs.addTab(tab, "Heading")

    def _build_notes_tab(self):
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        self.txt_notes = QtWidgets.QPlainTextEdit()
        self.txt_notes.setReadOnly(True)
        self.txt_notes.setPlaceholderText("Selection summary and debug notes.")
        layout.addWidget(self.txt_notes)
        self.tabs.addTab(tab, "Notes")

    def set_text(self, text: str):
        self.txt_notes.setPlainText(text or "")

    def append_text(self, text: str):
        if not text:
            return
        self.txt_notes.appendPlainText(str(text))

    def set_metric(self, key: str, value):
        self.metrics_card.set_value(key, value)

    def set_selection(self, **kwargs):
        for key, value in kwargs.items():
            self.selection_card.set_value(key, value)

    def set_coordinates(self, x=None, y=None):
        self.coords_card.set_value("X", x)
        self.coords_card.set_value("Y", y)

    def set_heading_series(self, vessel=None, rov1=None, rov2=None, x=None, autorange=False):
        return

    def set_vessel_heading(self, hdg: float):
        if hasattr(self, "vessel_svg") and self.vessel_svg:
            self.vessel_svg.set_heading_deg(hdg)

    def set_rov1_heading(self, hdg: float):
        if hasattr(self, "rov1_svg") and self.rov1_svg:
            self.rov1_svg.set_heading_deg(hdg)

    def set_rov2_heading(self, hdg: float):
        if hasattr(self, "rov2_svg") and self.rov2_svg:
            self.rov2_svg.set_heading_deg(hdg)
