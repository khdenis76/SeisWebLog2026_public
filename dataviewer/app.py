import sys
from PySide6 import QtWidgets
import pyqtgraph as pg

from .main_window import MainWindow
from PySide6.QtGui import QPalette, QColor
from PySide6.QtCore import Qt


def apply_fusion_dark(app: QtWidgets.QApplication):
    app.setStyle("Fusion")
    pal = QPalette()
    pal.setColor(QPalette.ColorRole.Window, QColor(45, 45, 45))
    pal.setColor(QPalette.ColorRole.WindowText, Qt.GlobalColor.white)
    pal.setColor(QPalette.ColorRole.Base, QColor(30, 30, 30))
    pal.setColor(QPalette.ColorRole.AlternateBase, QColor(45, 45, 45))
    pal.setColor(QPalette.ColorRole.Text, Qt.GlobalColor.white)
    pal.setColor(QPalette.ColorRole.Button, QColor(45, 45, 45))
    pal.setColor(QPalette.ColorRole.ButtonText, Qt.GlobalColor.white)
    pal.setColor(QPalette.ColorRole.Highlight, QColor(38, 79, 120))
    pal.setColor(QPalette.ColorRole.HighlightedText, Qt.GlobalColor.white)
    pal.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text, QColor(127, 127, 127))
    pal.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, QColor(127, 127, 127))
    app.setPalette(pal)


def main():
    app = QtWidgets.QApplication(sys.argv)
    apply_fusion_dark(app)

    pg.setConfigOption("background", "#1e1e1e")
    pg.setConfigOption("foreground", "w")
    pg.setConfigOptions(antialias=False)

    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
