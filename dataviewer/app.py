import sys
from PySide6 import QtWidgets

from .main_window import MainWindow
from .theme import apply_dark_theme


def main():
    app = QtWidgets.QApplication(sys.argv)
    apply_dark_theme(app)

    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
