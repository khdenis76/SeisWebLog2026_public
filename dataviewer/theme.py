from PySide6 import QtWidgets
from PySide6.QtGui import QPalette, QColor
from PySide6.QtCore import Qt
import pyqtgraph as pg


def apply_dark_theme(app: QtWidgets.QApplication):
    app.setStyle("Fusion")
    pal = QPalette()
    pal.setColor(QPalette.ColorRole.Window, QColor(38, 38, 42))
    pal.setColor(QPalette.ColorRole.WindowText, Qt.GlobalColor.white)
    pal.setColor(QPalette.ColorRole.Base, QColor(24, 24, 27))
    pal.setColor(QPalette.ColorRole.AlternateBase, QColor(32, 32, 36))
    pal.setColor(QPalette.ColorRole.Text, Qt.GlobalColor.white)
    pal.setColor(QPalette.ColorRole.Button, QColor(45, 45, 50))
    pal.setColor(QPalette.ColorRole.ButtonText, Qt.GlobalColor.white)
    pal.setColor(QPalette.ColorRole.Highlight, QColor(59, 130, 246))
    pal.setColor(QPalette.ColorRole.HighlightedText, Qt.GlobalColor.white)
    pal.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text, QColor(127, 127, 127))
    pal.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, QColor(127, 127, 127))
    app.setPalette(pal)

    pg.setConfigOption("background", "#18181b")
    pg.setConfigOption("foreground", "w")
    pg.setConfigOptions(antialias=False)
