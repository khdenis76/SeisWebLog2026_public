from PySide6 import QtCore, QtWidgets
import pyqtgraph as pg
from ..widgets.rotating_svg import RotatingSvgView

class RightPanel(QtWidgets.QFrame):
    closeRequested = QtCore.Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameShape(QtWidgets.QFrame.Shape.StyledPanel)
        self.setMinimumWidth(280)

        main_layout = QtWidgets.QVBoxLayout(self)
        main_layout.setContentsMargins(6, 6, 6, 6)
        main_layout.setSpacing(6)

        # Header
        header = QtWidgets.QHBoxLayout()
        self.lbl_title = QtWidgets.QLabel("Inspector")
        self.lbl_title.setStyleSheet("font-weight:600;")
        header.addWidget(self.lbl_title)
        header.addStretch(1)

        self.btn_close = QtWidgets.QToolButton()
        self.btn_close.setText("✕")
        self.btn_close.setToolTip("Close panel")
        self.btn_close.setAutoRaise(True)
        header.addWidget(self.btn_close)

        main_layout.addLayout(header)

        # Tabs
        self.tabs = QtWidgets.QTabWidget()
        self.tabs.setTabPosition(QtWidgets.QTabWidget.TabPosition.North)
        main_layout.addWidget(self.tabs, 1)

        self._build_info_tab()
        self._build_heading_tab()
        self._build_diagram_tab()

        self.btn_close.clicked.connect(self.closeRequested.emit)

    # ----------------------------------
    # TAB 1 — Info (empty for now)
    # ----------------------------------
    def _build_info_tab(self):
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addStretch(1)
        self.tabs.addTab(tab, "Info")

    # ----------------------------------
    # TAB 2 — Heading (3 pyqtgraph plots)
    # ----------------------------------
    def _build_heading_tab(self):
        from pathlib import Path

        tab = QtWidgets.QWidget()
        v = QtWidgets.QVBoxLayout(tab)
        v.setContentsMargins(0, 0, 0, 0)
        v.setSpacing(6)

        base_svg = (
                Path(__file__).resolve().parents[2]
                / "baseproject" / "static" / "baseproject" / "svg"
        )

        windrose_bg = base_svg / "windrose_inv.svg"

        vessel_svg_path = base_svg / "vessel.svg"
        rov_svg_path = base_svg / "rov_red.svg"  # or vessel.svg if you reuse it

        # Foreground icon slightly smaller so wind rose stays visible
        self.vessel_svg = RotatingSvgView(
            fg_svg_path=str(vessel_svg_path),
            bg_svg_path=None,
            fg_scale=1,
            bg_scale=0.00,
        )
        self.rov1_svg = RotatingSvgView(
            fg_svg_path=str(rov_svg_path),
            bg_svg_path=str(windrose_bg),
            fg_scale=1,
            bg_scale=1.00,
        )
        self.rov2_svg = RotatingSvgView(
            fg_svg_path=str(rov_svg_path),
            bg_svg_path=str(windrose_bg),
            fg_scale=1,
            bg_scale=1.00,
        )

        splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        splitter.addWidget(self.vessel_svg)
        splitter.addWidget(self.rov1_svg)
        splitter.addWidget(self.rov2_svg)
        splitter.setSizes([220, 180, 180])

        v.addWidget(splitter, 1)
        self.tabs.addTab(tab, "Heading")

    def _make_heading_plot(self, title: str) -> pg.PlotWidget:
        w = pg.PlotWidget()
        p = w.getPlotItem()
        p.setTitle(title)
        p.showGrid(x=True, y=True)
        p.setLabel("left", "Heading", units="deg")
        p.setLabel("bottom", "Index")  # you can change to Time later (see notes below)
        p.setMenuEnabled(True)
        p.setClipToView(True)

        # Heading is cyclic (0..360). Keeping visible range consistent helps.
        p.setYRange(0, 360, padding=0.02)

        # Optional: legend (nice when you add multiple lines)
        p.addLegend(offset=(10, 10))

        return w

    # ----------------------------------
    # TAB 3 — Diagram (empty for now)
    # ----------------------------------
    def _build_diagram_tab(self):
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addStretch(1)
        self.tabs.addTab(tab, "Diagram")

    # ----------------------------------
    # Public API: update data
    # ----------------------------------
    def set_heading_series(self, vessel=None, rov1=None, rov2=None, x=None, autorange=False):
        # vessel line removed because vessel is now SVG
        if x is None:
            if rov1 is not None:
                self.curve_heading_rov1.setData(list(range(len(rov1))), rov1)
            if rov2 is not None:
                self.curve_heading_rov2.setData(list(range(len(rov2))), rov2)
        else:
            if rov1 is not None:
                self.curve_heading_rov1.setData(x, rov1)
            if rov2 is not None:
                self.curve_heading_rov2.setData(x, rov2)

        if autorange:
            self.plt_heading_rov1.enableAutoRange()
            self.plt_heading_rov2.enableAutoRange()
            self.plt_heading_rov1.setYRange(0, 360, padding=0.02)
            self.plt_heading_rov2.setYRange(0, 360, padding=0.02)

    def set_vessel_heading(self, hdg: float):
        if hasattr(self, "vessel_svg") and self.vessel_svg:
            self.vessel_svg.set_heading_deg(hdg)

    def set_rov1_heading(self, hdg: float):
        if hasattr(self, "rov1_svg") and self.rov1_svg:
            self.rov1_svg.set_heading_deg(hdg)

    def set_rov2_heading(self, hdg: float):
        if hasattr(self, "rov2_svg") and self.rov2_svg:
            self.rov2_svg.set_heading_deg(hdg)