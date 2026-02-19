from PySide6 import QtWidgets
import pyqtgraph as pg


class PlotWindow(QtWidgets.QMainWindow):
    """
    Generic window that contains a single pyqtgraph PlotWidget.
    You can reuse this for every plot type.
    """
    def __init__(self, title="Plot", window_id=None, parent=None,seq=1):
        super().__init__(parent)
        self.window_id = window_id or title
        self.setWindowTitle(title)
        screen = self.screen() or QtWidgets.QApplication.primaryScreen()
        geo = screen.availableGeometry()

        w = int(geo.width() * 0.30)  # 30% of screen width
        h = int(geo.height() * 0.25)  # 25% of screen height
        self.resize(w, h)

        # optional: position near top-right
        x = geo.x() + geo.width() - w - 40
        y = geo.y() + 40+h*seq
        self.move(x, y)

        self.plot = pg.PlotWidget()
        self.plot.showGrid(x=True, y=True, alpha=0.2)
        self.setCentralWidget(self.plot)

        # storage for plot items and state
        self.items = {}   # e.g. {"primary_curve": PlotDataItem, "sel_line": InfiniteLine}
        self.state = {}   # free dict for anything (selected_station, selected_line, etc.)

    def set_labels(self, x_label="X", y_label="Y", y_units=None):
        self.plot.setLabel("bottom", x_label)
        self.plot.setLabel("left", y_label, units=y_units)

    def clear_plot(self):
        self.plot.clear()
        self.items.clear()
