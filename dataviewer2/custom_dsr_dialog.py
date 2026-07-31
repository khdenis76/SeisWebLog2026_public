from __future__ import annotations

import re
import uuid
from typing import Iterable

from PySide6 import QtGui, QtWidgets

from .config import CustomDsrLayerDefinition


class CustomDsrLayerDialog(QtWidgets.QDialog):
    """Compact first-version custom DSR layer builder."""

    def __init__(self, columns: Iterable[str], parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Create custom DSR layer")
        self.resize(520, 430)
        columns = sorted(set(columns), key=str.lower)

        form = QtWidgets.QFormLayout(self)
        self.name_edit = QtWidgets.QLineEdit("Custom DSR layer")
        self.x_combo = QtWidgets.QComboBox(); self.x_combo.addItems(columns)
        self.y_combo = QtWidgets.QComboBox(); self.y_combo.addItems(columns)
        self.filter_field = QtWidgets.QComboBox(); self.filter_field.addItem("(none)", ""); self.filter_field.addItems(columns)
        self.filter_operator = QtWidgets.QComboBox(); self.filter_operator.addItems(["=", "!=", ">", ">=", "<", "<=", "IS NULL", "IS NOT NULL"])
        self.filter_value = QtWidgets.QLineEdit()
        self.category_field = QtWidgets.QComboBox(); self.category_field.addItem("Single symbol", "");
        for column in columns:
            self.category_field.addItem(column, column)
        self.color_button = QtWidgets.QPushButton("#00e5ff")
        self.color_button.setStyleSheet("background:#00e5ff;color:#000")
        self.color_button.clicked.connect(self._choose_color)
        self.point_size = QtWidgets.QDoubleSpinBox(); self.point_size.setRange(2, 30); self.point_size.setValue(7)
        self.visible = QtWidgets.QCheckBox(); self.visible.setChecked(True)
        self.split_lines = QtWidgets.QCheckBox(); self.split_lines.setChecked(True)
        self.show_stations = QtWidgets.QCheckBox(); self.show_stations.setChecked(True)

        for combo, preferred in ((self.x_combo, "PrimaryEasting"), (self.y_combo, "PrimaryNorthing")):
            index = combo.findText(preferred)
            if index >= 0:
                combo.setCurrentIndex(index)

        form.addRow("Layer name:", self.name_edit)
        form.addRow("X field:", self.x_combo)
        form.addRow("Y field:", self.y_combo)
        form.addRow("Filter field:", self.filter_field)
        form.addRow("Operator:", self.filter_operator)
        form.addRow("Filter value:", self.filter_value)
        form.addRow("Categorize by:", self.category_field)
        form.addRow("Default color:", self.color_button)
        form.addRow("Point size:", self.point_size)
        form.addRow("Visible at startup:", self.visible)
        form.addRow("Split into receiver lines:", self.split_lines)
        form.addRow("Show stations in tree:", self.show_stations)

        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.StandardButton.Ok | QtWidgets.QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject)
        form.addRow(buttons)

    def _choose_color(self) -> None:
        color = QtWidgets.QColorDialog.getColor(QtGui.QColor(self.color_button.text()), self)
        if color.isValid():
            self.color_button.setText(color.name())
            self.color_button.setStyleSheet(f"background:{color.name()};color:{'#000' if color.lightness() > 150 else '#fff'}")

    def definition(self) -> CustomDsrLayerDefinition:
        name = self.name_edit.text().strip() or "Custom DSR layer"
        slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "custom"
        raw = self.filter_value.text().strip()
        value: object = raw
        if raw:
            try:
                value = float(raw)
            except ValueError:
                value = raw
        return CustomDsrLayerDefinition(
            id=f"{slug}_{uuid.uuid4().hex[:8]}",
            name=name,
            x_field=self.x_combo.currentText(),
            y_field=self.y_combo.currentText(),
            filter_field=str(self.filter_field.currentData() or self.filter_field.currentText()),
            filter_operator=self.filter_operator.currentText(),
            filter_value=value,
            category_field=str(self.category_field.currentData() or ""),
            color=self.color_button.text(),
            point_size=float(self.point_size.value()),
            visible=self.visible.isChecked(),
            split_by_line=self.split_lines.isChecked(),
            show_stations=self.show_stations.isChecked(),
        )
