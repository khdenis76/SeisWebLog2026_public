from PySide6 import QtCore, QtWidgets
from PySide6.QtGui import QAction


class DsrMdiWindow(QtWidgets.QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle("DSR Plots")
        self.resize(1200, 800)

        self.mdi = QtWidgets.QMdiArea()
        self.setCentralWidget(self.mdi)

        self._subs = {}

        self._build_menu()

    def _build_menu(self):
        bar = self.menuBar()
        menu_window = bar.addMenu("Window")

        self.act_tile = QAction("Tile", self)
        self.act_cascade = QAction("Cascade", self)
        self.act_tile_vertical = QAction("Tile Vertical", self)
        self.act_tile_horizontal = QAction("Tile Horizontal", self)
        self.act_close_all = QAction("Close All", self)

        self.act_tile.triggered.connect(self.mdi.tileSubWindows)
        self.act_cascade.triggered.connect(self.mdi.cascadeSubWindows)
        self.act_tile_vertical.triggered.connect(self.tile_vertical)
        self.act_tile_horizontal.triggered.connect(self.tile_horizontal)
        self.act_close_all.triggered.connect(self.close_all_subwindows)

        menu_window.addAction(self.act_tile)
        menu_window.addAction(self.act_cascade)
        menu_window.addSeparator()
        menu_window.addAction(self.act_tile_vertical)
        menu_window.addAction(self.act_tile_horizontal)
        menu_window.addSeparator()
        menu_window.addAction(self.act_close_all)

        tb = self.addToolBar("Window")
        tb.setMovable(False)
        tb.addAction(self.act_tile)
        tb.addAction(self.act_cascade)
        tb.addAction(self.act_tile_vertical)
        tb.addAction(self.act_tile_horizontal)
        tb.addAction(self.act_close_all)

    def set_line_title(self, line: int):
        self.setWindowTitle(f"Line {line} - DSR Plots")

    def add_plot_window(self, key: str, widget: QtWidgets.QWidget, title: str):
        old = self._subs.get(key)
        if old is not None:
            try:
                old.close()
            except Exception:
                pass

        sub = QtWidgets.QMdiSubWindow()
        sub.setWidget(widget)
        sub.setAttribute(QtCore.Qt.WidgetAttribute.WA_DeleteOnClose, False)
        sub.setWindowTitle(title)

        self.mdi.addSubWindow(sub)
        sub.show()

        self._subs[key] = sub
        return sub

    def get_subwindow(self, key: str):
        sub = self._subs.get(key)
        if sub is None:
            return None
        if sub.isHidden():
            return None
        return sub

    def has_subwindow(self, key: str) -> bool:
        return self.get_subwindow(key) is not None

    def close_all_subwindows(self):
        for sub in list(self._subs.values()):
            try:
                sub.close()
            except Exception:
                pass
        self._subs.clear()

    def _visible_subwindows(self):
        out = []
        for key, sub in self._subs.items():
            try:
                if sub is not None and not sub.isHidden():
                    out.append(sub)
            except Exception:
                pass
        return out

    def tile_vertical(self):
        subs = self._visible_subwindows()
        n = len(subs)
        if n == 0:
            return

        rect = self.mdi.viewport().rect()
        w = rect.width() // n
        h = rect.height()

        for i, sub in enumerate(subs):
            x = i * w
            width = w if i < n - 1 else rect.width() - x
            sub.setGeometry(x, 0, width, h)
            sub.show()

    def tile_horizontal(self):
        subs = self._visible_subwindows()
        n = len(subs)
        if n == 0:
            return

        rect = self.mdi.viewport().rect()
        w = rect.width()
        h = rect.height() // n

        for i, sub in enumerate(subs):
            y = i * h
            height = h if i < n - 1 else rect.height() - y
            sub.setGeometry(0, y, w, height)
            sub.show()