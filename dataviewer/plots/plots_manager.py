from PySide6 import QtCore

from dataviewer.ui.plot_window import PlotWindow


class PlotManager(QtCore.QObject):
    """
    Creates plot windows on demand and keeps references.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.windows = {}  # key -> PlotWindow

    def get_or_create(self, key: str, title: str,seq=1) -> PlotWindow:
        w = self.windows.get(key)
        if w is not None:
            return w

        w = PlotWindow(title=title, window_id=key, parent=None,seq=seq)
        self.windows[key] = w

        # remove from dict when closed
        w.destroyed.connect(lambda *_: self.windows.pop(key, None))
        return w

    def show(self, key: str):
        w = self.windows.get(key)
        if not w:
            return
        w.show()
        w.raise_()
        w.activateWindow()

    def close_all(self):
        for key, w in list(self.windows.items()):
            try:
                w.close()
            except Exception:
                pass
        self.windows.clear()

