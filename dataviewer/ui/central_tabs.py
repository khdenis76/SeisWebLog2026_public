from PySide6 import QtWidgets
import pyqtgraph as pg


class CentralTabs(QtWidgets.QTabWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setDocumentMode(True)
        self.setMovable(False)

        self._build_map_tab()
        self._build_time_series_tab()
        self._build_depth_tab()
        self._build_table_tab()
        self._build_log_tab()

    def _prepare_plot(self, title: str, x_label: str, y_label: str):
        w = pg.PlotWidget(title=title)
        p = w.getPlotItem()
        p.showGrid(x=True, y=True, alpha=0.2)
        p.setClipToView(True)
        p.setDownsampling(auto=True, mode="peak")
        p.setLabel("bottom", x_label)
        p.setLabel("left", y_label)
        return w

    def _build_map_tab(self):
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self.map_plot = self._prepare_plot("Map Workspace", "Easting", "Northing")
        self.legend = self.map_plot.addLegend(offset=(10, 10))
        self.legend.setBrush(pg.mkBrush(25, 25, 25, 210))
        self.legend.setPen(pg.mkPen("w"))
        self.map_scatter = pg.ScatterPlotItem(size=6, pxMode=True)
        self.map_plot.addItem(self.map_scatter)

        layout.addWidget(self.map_plot)
        self.addTab(tab, "Map")

    def _build_time_series_tab(self):
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self.ts_plot = self._prepare_plot("Time Series", "Time", "Value")
        self.ts_curve1 = self.ts_plot.plot([], [], pen=pg.mkPen(width=2))
        self.ts_curve2 = self.ts_plot.plot([], [], pen=pg.mkPen(width=2, style=pg.QtCore.Qt.PenStyle.DashLine))
        layout.addWidget(self.ts_plot)
        self.addTab(tab, "Time Series")

    def _build_depth_tab(self):
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self.depth_plot = self._prepare_plot("Depth / QC Overview", "Station", "Depth")
        self.depth_curve1 = self.depth_plot.plot([], [], pen=pg.mkPen(width=2))
        self.depth_curve2 = self.depth_plot.plot([], [], pen=pg.mkPen(width=2, style=pg.QtCore.Qt.PenStyle.DashLine))
        layout.addWidget(self.depth_plot)
        self.addTab(tab, "Depth")

    def _build_table_tab(self):
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self.table = QtWidgets.QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(False)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(False)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table)
        self.addTab(tab, "Table")

    def _build_log_tab(self):
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self.log_text = QtWidgets.QPlainTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setPlaceholderText("Application messages, diagnostics, and lightweight event log.")
        layout.addWidget(self.log_text)
        self.addTab(tab, "Logs")

    def log(self, message: str):
        self.log_text.appendPlainText(str(message))
