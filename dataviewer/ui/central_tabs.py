from PySide6 import QtWidgets
import pyqtgraph as pg


class CentralTabs(QtWidgets.QTabWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        # Map tab
        tab_map = QtWidgets.QWidget()
        map_layout = QtWidgets.QVBoxLayout(tab_map)
        map_layout.setContentsMargins(8, 8, 8, 8)

        self.map_plot = pg.PlotWidget(title="Map (Easting / Northing)")
        self.map_plot.showGrid(x=True, y=True)
        self.map_plot.setLabel("bottom", "Easting")
        self.map_plot.setLabel("left", "Northing")
        self.map_scatter = pg.ScatterPlotItem(size=6, pxMode=True)
        self.map_plot.addItem(self.map_scatter)
        map_layout.addWidget(self.map_plot)

        # Time series tab
        tab_ts = QtWidgets.QWidget()
        ts_layout = QtWidgets.QVBoxLayout(tab_ts)
        ts_layout.setContentsMargins(8, 8, 8, 8)

        self.ts_plot = pg.PlotWidget(title="Time Series")
        self.ts_plot.showGrid(x=True, y=True)
        self.ts_plot.setLabel("bottom", "Time")
        self.ts_plot.setLabel("left", "Value")
        self.ts_curve1 = self.ts_plot.plot([], [])
        self.ts_curve2 = self.ts_plot.plot([], [])
        ts_layout.addWidget(self.ts_plot)

        # Table tab
        tab_tbl = QtWidgets.QWidget()
        tbl_layout = QtWidgets.QVBoxLayout(tab_tbl)
        tbl_layout.setContentsMargins(8, 8, 8, 8)

        self.table = QtWidgets.QTableWidget()
        tbl_layout.addWidget(self.table)

        self.addTab(tab_map, "Map")
        self.addTab(tab_ts, "Time Series")
        self.addTab(tab_tbl, "Table")
