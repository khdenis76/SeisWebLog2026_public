from __future__ import annotations

import pyqtgraph as pg
from PySide6 import QtGui, QtWidgets


THEMES = {"night", "day"}


def normalize_theme(value: str | None) -> str:
    return "day" if str(value or "").lower() == "day" else "night"


def _palette(theme: str) -> QtGui.QPalette:
    theme = normalize_theme(theme)
    palette = QtGui.QPalette()
    if theme == "day":
        window = QtGui.QColor("#f4f6f8")
        base = QtGui.QColor("#ffffff")
        alternate = QtGui.QColor("#eef2f5")
        text = QtGui.QColor("#1f2328")
        disabled = QtGui.QColor("#8a939d")
        button = QtGui.QColor("#f7f8fa")
        highlight = QtGui.QColor("#2f7ed8")
        highlighted = QtGui.QColor("#ffffff")
        mid = QtGui.QColor("#c7cdd4")
    else:
        window = QtGui.QColor("#1d2127")
        base = QtGui.QColor("#111317")
        alternate = QtGui.QColor("#252a31")
        text = QtGui.QColor("#e8eaed")
        disabled = QtGui.QColor("#7f8790")
        button = QtGui.QColor("#292e35")
        highlight = QtGui.QColor("#3d8bfd")
        highlighted = QtGui.QColor("#ffffff")
        mid = QtGui.QColor("#454b54")

    for role in (QtGui.QPalette.ColorRole.WindowText, QtGui.QPalette.ColorRole.Text,
                 QtGui.QPalette.ColorRole.ButtonText, QtGui.QPalette.ColorRole.ToolTipText):
        palette.setColor(role, text)
    palette.setColor(QtGui.QPalette.ColorRole.Window, window)
    palette.setColor(QtGui.QPalette.ColorRole.Base, base)
    palette.setColor(QtGui.QPalette.ColorRole.AlternateBase, alternate)
    palette.setColor(QtGui.QPalette.ColorRole.Button, button)
    palette.setColor(QtGui.QPalette.ColorRole.Highlight, highlight)
    palette.setColor(QtGui.QPalette.ColorRole.HighlightedText, highlighted)
    palette.setColor(QtGui.QPalette.ColorRole.Mid, mid)
    palette.setColor(QtGui.QPalette.ColorGroup.Disabled, QtGui.QPalette.ColorRole.Text, disabled)
    palette.setColor(QtGui.QPalette.ColorGroup.Disabled, QtGui.QPalette.ColorRole.ButtonText, disabled)
    return palette


def apply_application_theme(app: QtWidgets.QApplication, theme: str) -> str:
    theme = normalize_theme(theme)
    app.setPalette(_palette(theme))
    if theme == "day":
        app.setStyleSheet("""
            QToolTip { color:#1f2328; background:#fffbe6; border:1px solid #aab2bb; }
            QTreeWidget, QTableWidget, QTableView { gridline-color:#d7dce1; }
            QDockWidget::title { padding:5px; background:#e8edf2; }
        """)
        pg.setConfigOptions(background="#ffffff", foreground="#20252b")
    else:
        app.setStyleSheet("""
            QToolTip { color:#f2f3f5; background:#30353c; border:1px solid #626a74; }
            QTreeWidget, QTableWidget, QTableView { gridline-color:#3c424a; }
            QDockWidget::title { padding:5px; background:#272c33; }
        """)
        pg.setConfigOptions(background="#111317", foreground="#e8e8e8")
    return theme
