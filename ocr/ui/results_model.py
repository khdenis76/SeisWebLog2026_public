from __future__ import annotations

from typing import Any

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor, QBrush

STATUS_BG = {
    "OK": QColor(220, 255, 220),
    "WARNING": QColor(255, 245, 200),
    "ERROR": QColor(255, 220, 220),
    "NO_DSR": QColor(240, 230, 230),
    "BAD_FILENAME": QColor(235, 220, 255),
    "BAD_RESOLUTION": QColor(255, 225, 225),
    "NO_COORDS": QColor(250, 240, 215),
    "DEPLOYMENT_OK": QColor(220, 255, 220),
    "COMPLETE_OK": QColor(205, 245, 205),
    "DEPLOYMENT_INCOMPLETE": QColor(255, 245, 200),
    "RECOVERY_INCOMPLETE": QColor(255, 235, 180),
    "TOO_MANY": QColor(255, 220, 220),
}

COLUMN_TITLES = {
    "checked": "Checked",
    "file_line": "Line",
    "file_station": "Station",
    "images": "Images",
    "expected": "Expected",
    "station_status": "Station status",
    "status": "Status",
    "dsr_line": "DSR line",
    "dsr_station": "DSR station",
    "dsr_timestamp": "Deployment date",
    "dsr_timestamp1": "Recovery date",
    "dsr_rov": "ROV",
    "dsr_rov1": "ROV1",
    "delta_m": "Delta (m)",
    "message": "Message",
    "image": "Image",
    "image_path": "Image path",
    "resolution": "Resolution",
    "config_used": "Config",
    "file_role": "Role",
    "file_index": "Index",
    "rov": "OCR ROV",
    "dive": "Dive",
    "date": "OCR date",
    "time": "OCR time",
    "line": "OCR line",
    "station": "OCR station",
    "east": "East",
    "north": "North",
    "dsr_x": "DSR East",
    "dsr_y": "DSR North",
    "ocr_vs_file": "OCR vs file",
    "file_vs_dsr": "File vs DSR",
    "station_image_count": "Station images",
    "expected_images": "Expected images",
    "processed_at": "Processed at",
}

DARK_TEXT = QBrush(QColor(24, 24, 24))


class DictTableModel(QAbstractTableModel):
    def __init__(self, columns: list[str], rows: list[dict[str, Any]] | None = None, parent=None):
        super().__init__(parent)
        self.columns = columns
        self.rows = rows or []

    def set_rows(self, rows):
        self.beginResetModel()
        self.rows = rows or []
        self.endResetModel()

    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self.rows)

    def columnCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self.columns)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal and 0 <= section < len(self.columns):
            return COLUMN_TITLES.get(self.columns[section], self.columns[section])
        return section + 1

    def flags(self, index):
        if not index.isValid():
            return Qt.NoItemFlags
        flags = Qt.ItemIsEnabled | Qt.ItemIsSelectable
        if self.columns[index.column()] == "checked":
            flags |= Qt.ItemIsUserCheckable
        return flags

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        row = self.rows[index.row()]
        col = self.columns[index.column()]
        value = row.get(col, "")
        status = str(row.get("status") or row.get("station_status") or "").strip().upper()
        if role == Qt.CheckStateRole and col == "checked":
            return Qt.Checked if bool(value) else Qt.Unchecked
        if role == Qt.DisplayRole:
            if col == "checked":
                return None
            return "" if value is None else str(value)
        if role == Qt.BackgroundRole and status in STATUS_BG:
            return QBrush(STATUS_BG[status])
        if role == Qt.ForegroundRole and status in STATUS_BG:
            return DARK_TEXT
        return None

    def setData(self, index, value, role=Qt.EditRole):
        if not index.isValid():
            return False
        col = self.columns[index.column()]
        if col == "checked" and role == Qt.CheckStateRole:
            self.rows[index.row()][col] = 1 if value == Qt.Checked else 0
            self.dataChanged.emit(index, index, [Qt.CheckStateRole])
            return True
        return False

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder):
        if not (0 <= column < len(self.columns)):
            return
        key = self.columns[column]
        self.layoutAboutToBeChanged.emit()
        reverse = order == Qt.DescendingOrder

        def sk(row):
            v = row.get(key, "")
            try:
                return (0, float(v))
            except Exception:
                return (1, str(v))

        self.rows.sort(key=sk, reverse=reverse)
        self.layoutChanged.emit()
