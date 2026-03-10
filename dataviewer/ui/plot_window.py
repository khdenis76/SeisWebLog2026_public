from PySide6 import QtCore, QtWidgets
import pyqtgraph as pg


class ClickableLegendItem(QtWidgets.QFrame):
    clicked = QtCore.Signal()

    def __init__(self, name: str, color: str, parent=None):
        super().__init__(parent)

        self._active = True
        self._color = str(color)

        self.setCursor(QtCore.Qt.PointingHandCursor)
        self.setFrameShape(QtWidgets.QFrame.NoFrame)

        layout = QtWidgets.QHBoxLayout(self)
        layout.setContentsMargins(4, 2, 4, 2)
        layout.setSpacing(6)

        self.swatch = QtWidgets.QLabel()
        self.swatch.setFixedSize(18, 4)

        self.label = QtWidgets.QLabel(str(name))

        layout.addWidget(self.swatch)
        layout.addWidget(self.label)

        self.set_active(True)

    def mousePressEvent(self, event):
        if event.button() == QtCore.Qt.LeftButton:
            self.clicked.emit()
        super().mousePressEvent(event)

    def set_active(self, active: bool):
        self._active = bool(active)

        if self._active:
            swatch_style = (
                f"background:{self._color};"
                "border:1px solid rgba(255,255,255,0.25);"
            )
            label_style = "color: palette(text);"
            self.setWindowOpacity(1.0)
        else:
            swatch_style = (
                f"background:{self._color};"
                "border:1px solid rgba(255,255,255,0.20);"
            )
            label_style = "color: gray;"
            self.setWindowOpacity(0.55)

        self.swatch.setStyleSheet(swatch_style)
        self.label.setStyleSheet(label_style)

    @property
    def active(self) -> bool:
        return self._active


class PlotWindow(QtWidgets.QMainWindow):
    def __init__(self, title="Plot", parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(900, 500)

        self.items = {}
        self.state = {}

        central = QtWidgets.QWidget()
        self.setCentralWidget(central)

        self.main_layout = QtWidgets.QVBoxLayout(central)
        self.main_layout.setContentsMargins(6, 6, 6, 6)
        self.main_layout.setSpacing(4)

        self.graphics_layout_widget = pg.GraphicsLayoutWidget()
        self.main_layout.addWidget(self.graphics_layout_widget, 1)

        self.plot = self.graphics_layout_widget.addPlot()
        self.plot.showGrid(x=True, y=True, alpha=0.25)

        self.legend_widget = QtWidgets.QWidget()
        self.legend_layout = QtWidgets.QHBoxLayout(self.legend_widget)
        self.legend_layout.setContentsMargins(8, 2, 8, 6)
        self.legend_layout.setSpacing(14)
        self.main_layout.addWidget(self.legend_widget, 0)

    def clear_legend(self):
        while self.legend_layout.count():
            item = self.legend_layout.takeAt(0)
            w = item.widget()
            if w is not None:
                w.deleteLater()