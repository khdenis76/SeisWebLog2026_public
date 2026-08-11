from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from PySide6 import QtCore, QtGui, QtWidgets


class OcrImagesPanel(QtWidgets.QWidget):
    """Table-only browser for OCR records linked to one DSR station."""

    COLUMNS: tuple[tuple[str, str], ...] = (
        ("file_role", "Role"),
        ("image_name", "Image"),
        ("resolution", "Resolution"),
        ("rov", "ROV"),
        ("dive", "Dive"),
        ("date", "Date"),
        ("time", "Time"),
        ("delta_m", "Delta (m)"),
        ("ocr_vs_file", "OCR vs file"),
        ("file_vs_dsr", "File vs DSR"),
        ("status", "Status"),
        ("station_image_count", "Images"),
        ("expected_images", "Expected"),
        ("station_status", "Station status"),
        ("message", "Message"),
        ("checked", "Checked"),
    )

    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self._records: list[dict[str, Any]] = []

        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(4, 4, 4, 4)
        root.setSpacing(4)

        header = QtWidgets.QHBoxLayout()
        self.station_label = QtWidgets.QLabel("No DSR station selected")
        self.station_label.setStyleSheet("font-weight: 600;")
        self.count_label = QtWidgets.QLabel("0 image(s)")
        header.addWidget(self.station_label)
        header.addStretch(1)
        header.addWidget(self.count_label)
        root.addLayout(header)

        self.table = QtWidgets.QTableWidget(0, len(self.COLUMNS))
        self.table.setHorizontalHeaderLabels([label for _, label in self.COLUMNS])
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setStretchLastSection(False)
        self.table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeMode.Stretch)
        self.table.setToolTip("Double-click a row to open the image with the Windows default image viewer.")
        root.addWidget(self.table, 1)

        self.path_label = QtWidgets.QLineEdit()
        self.path_label.setReadOnly(True)
        self.path_label.setPlaceholderText("Select a row to see the image path. Double-click to open it.")
        root.addWidget(self.path_label)

        self.table.currentCellChanged.connect(self._row_changed)
        self.table.cellDoubleClicked.connect(lambda *_: self.open_current_image())

    def set_records(self, line: Any, station: Any, records: list[dict[str, Any]]) -> None:
        self.table.setSortingEnabled(False)
        self._records = list(records)
        self.station_label.setText(f"DSR Line {line} / Station {station}")
        self.count_label.setText(f"{len(records):,} image(s)")
        self.table.setRowCount(len(records))

        for row_index, record in enumerate(records):
            for column_index, (key, _label) in enumerate(self.COLUMNS):
                value = record.get(key)
                if key == "checked":
                    text = "Yes" if str(value).strip().lower() in {"1", "true", "yes", "y"} else "No"
                elif key == "delta_m" and value not in (None, ""):
                    try:
                        text = f"{float(value):.2f}"
                    except (TypeError, ValueError):
                        text = str(value)
                else:
                    text = "" if value is None else str(value)
                item = QtWidgets.QTableWidgetItem(text)
                item.setData(QtCore.Qt.ItemDataRole.UserRole, row_index)
                if key == "delta_m":
                    item.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
                self.table.setItem(row_index, column_index, item)

        self.table.setSortingEnabled(True)
        if records:
            self.table.selectRow(0)
            self._show_record(0)
        else:
            self.path_label.setText("No OCR records found for this DSR station")

    def clear_records(self, message: str = "No DSR station selected") -> None:
        self._records = []
        self.table.setRowCount(0)
        self.station_label.setText(message)
        self.count_label.setText("0 image(s)")
        self.path_label.clear()

    def _record_index_for_row(self, row: int) -> int:
        if row < 0:
            return -1
        item = self.table.item(row, 0)
        if item is None:
            return row
        value = item.data(QtCore.Qt.ItemDataRole.UserRole)
        try:
            return int(value)
        except (TypeError, ValueError):
            return row

    def _row_changed(self, current_row: int, _current_column: int, _previous_row: int, _previous_column: int) -> None:
        self._show_record(current_row)

    def _show_record(self, row: int) -> None:
        index = self._record_index_for_row(row)
        if index < 0 or index >= len(self._records):
            self.path_label.clear()
            return
        path = self._records[index].get("image_path")
        self.path_label.setText("" if path is None else str(path))

    def open_current_image(self) -> None:
        row = self.table.currentRow()
        index = self._record_index_for_row(row)
        if index < 0 or index >= len(self._records):
            return
        path_value = self._records[index].get("image_path")
        if not path_value:
            return
        path = Path(str(path_value))
        if not path.is_file():
            QtWidgets.QMessageBox.warning(
                self,
                "OCR image",
                f"The image file was not found:\n{path}",
            )
            return
        try:
            if os.name == "nt":
                os.startfile(str(path))  # type: ignore[attr-defined]
            else:
                QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(path)))
        except OSError as exc:
            QtWidgets.QMessageBox.warning(self, "OCR image", f"Could not open the image:\n{exc}")
