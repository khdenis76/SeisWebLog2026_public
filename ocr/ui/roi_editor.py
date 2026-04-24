from __future__ import annotations
from ocr.core.models import ROIField
from ocr.core.ocr_engine import OCREngine
import numpy as np
import os
import pytesseract

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from PIL import Image

from PySide6.QtCore import QPointF, QRectF, Qt, Signal
from PySide6.QtGui import QColor, QPainter, QPen, QPixmap
from PySide6.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QGraphicsRectItem,
    QGraphicsScene,
    QGraphicsSimpleTextItem,
    QGraphicsView,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)


@dataclass
class RoiRow:
    name: str = "roi"
    field_type: str = "text"
    x: int = 0
    y: int = 0
    w: int = 100
    h: int = 40


class RoiRectItem(QGraphicsRectItem):
    def __init__(self, roi_index: int, rect: QRectF):
        super().__init__(rect)
        self.roi_index = roi_index
        self.label_item: Optional[QGraphicsSimpleTextItem] = None


class RoiGraphicsView(QGraphicsView):
    roi_drawn = Signal(int, int, int, int)

    def __init__(self, parent_dialog):
        super().__init__()
        self.parent_dialog = parent_dialog
        self.scene_obj = QGraphicsScene(self)
        self.setScene(self.scene_obj)

        self.pixmap_item = None
        self.roi_items: list[RoiRectItem] = []

        self.drawing = False
        self.start_pos = QPointF()
        self.temp_rect_item = None

        self.setRenderHint(QPainter.Antialiasing)
        self.setTransformationAnchor(QGraphicsView.AnchorUnderMouse)
        self.setResizeAnchor(QGraphicsView.AnchorUnderMouse)
        self.setDragMode(QGraphicsView.NoDrag)

    def set_image(self, pixmap: QPixmap):
        self.scene_obj.clear()
        self.roi_items.clear()

        self.pixmap_item = self.scene_obj.addPixmap(pixmap)
        self.scene_obj.setSceneRect(QRectF(pixmap.rect()))

        self.resetTransform()
        self.fitInView(self.scene_obj.sceneRect(), Qt.KeepAspectRatio)

    def wheelEvent(self, event):
        zoom = 1.2
        if event.angleDelta().y() > 0:
            self.scale(zoom, zoom)
        else:
            self.scale(1 / zoom, 1 / zoom)

    def draw_rois(self, rois: list[RoiRow], selected: int = -1):
        for item in self.roi_items:
            if item.label_item is not None:
                self.scene_obj.removeItem(item.label_item)
            self.scene_obj.removeItem(item)
        self.roi_items.clear()

        for i, r in enumerate(rois):
            rect = QRectF(r.x, r.y, r.w, r.h)
            item = RoiRectItem(i, rect)

            # 3 colors:
            # cyan = selected row
            # red = normal saved ROI
            # temp/new drawing uses green elsewhere
            if i == selected:
                pen = QPen(QColor(0, 255, 255), 3)   # third color for selected ROI
            else:
                pen = QPen(QColor(255, 80, 80), 2)

            item.setPen(pen)
            self.scene_obj.addItem(item)

            label = QGraphicsSimpleTextItem(r.name or f"roi_{i+1}")
            label.setBrush(QColor(255, 255, 0))
            label.setPos(r.x, max(0, r.y - 18))
            item.label_item = label
            self.scene_obj.addItem(label)

            self.roi_items.append(item)

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            pos = self.mapToScene(event.position().toPoint())
            self.start_pos = pos
            self.drawing = True

            self.temp_rect_item = QGraphicsRectItem()
            self.temp_rect_item.setPen(QPen(QColor(0, 255, 0), 2))
            self.scene_obj.addItem(self.temp_rect_item)
            self.temp_rect_item.setRect(QRectF(pos, pos))
            return

        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self.drawing and self.temp_rect_item:
            pos = self.mapToScene(event.position().toPoint())
            rect = QRectF(self.start_pos, pos).normalized()
            self.temp_rect_item.setRect(rect)
            return

        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        if self.drawing and self.temp_rect_item:
            rect = self.temp_rect_item.rect()
            self.scene_obj.removeItem(self.temp_rect_item)
            self.temp_rect_item = None
            self.drawing = False

            self.roi_drawn.emit(
                int(rect.x()),
                int(rect.y()),
                int(rect.width()),
                int(rect.height()),
            )
            return

        super().mouseReleaseEvent(event)


class RoiEditorDialog(QDialog):
    def __init__(self, config=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("ROI Editor")
        self.resize(1450, 920)

        self.config = config
        self.image_path: Optional[str] = None
        self.rois: list[RoiRow] = []
        self._updating_table = False

        self._build_ui()
        self._load_config_rois()

    def _build_ui(self):
        root = QVBoxLayout(self)

        top = QHBoxLayout()
        self.btn_open = QPushButton("Open image")
        self.btn_add = QPushButton("Add ROI")
        self.btn_delete = QPushButton("Delete ROI")
        top.addWidget(self.btn_open)
        top.addWidget(self.btn_add)
        top.addWidget(self.btn_delete)
        root.addLayout(top)

        splitter = QSplitter()

        # left: image
        self.view = RoiGraphicsView(self)
        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.addWidget(self.view)
        splitter.addWidget(left)

        # right: info + table + test
        right = QWidget()
        right_layout = QVBoxLayout(right)

        info_box = QWidget()
        info_form = QFormLayout(info_box)

        self.lbl_name = QLabel("-")
        self.lbl_path = QLabel("-")
        self.lbl_path.setWordWrap(True)
        self.lbl_format = QLabel("-")
        self.lbl_mode = QLabel("-")
        self.lbl_size = QLabel("-")
        self.lbl_dpi = QLabel("-")
        self.lbl_filesize = QLabel("-")
        self.lbl_modified = QLabel("-")

        info_form.addRow("File:", self.lbl_name)
        info_form.addRow("Path:", self.lbl_path)
        info_form.addRow("Format:", self.lbl_format)
        info_form.addRow("Mode:", self.lbl_mode)
        info_form.addRow("Width × Height:", self.lbl_size)
        info_form.addRow("DPI:", self.lbl_dpi)
        info_form.addRow("File size:", self.lbl_filesize)
        info_form.addRow("Modified:", self.lbl_modified)

        right_layout.addWidget(info_box)

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(["name", "type", "x", "y", "w", "h"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        right_layout.addWidget(self.table)

        btn_bar = QHBoxLayout()
        self.btn_test = QPushButton("Test selected ROI")
        self.btn_test_all = QPushButton("Test all ROIs")
        btn_bar.addWidget(self.btn_test)
        btn_bar.addWidget(self.btn_test_all)
        right_layout.addLayout(btn_bar)

        self.preview = QLabel()
        self.preview.setMinimumHeight(140)
        self.preview.setAlignment(Qt.AlignCenter)

        self.ocr_result = QLabel("OCR result")
        self.ocr_result.setWordWrap(True)
        self.ocr_result.setTextInteractionFlags(Qt.TextSelectableByMouse)

        right_layout.addWidget(self.preview)
        right_layout.addWidget(self.ocr_result)

        splitter.addWidget(right)
        splitter.setSizes([900, 550])

        root.addWidget(splitter)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        root.addWidget(buttons)

        self.btn_open.clicked.connect(self.open_image)
        self.btn_add.clicked.connect(self.add_roi)
        self.btn_delete.clicked.connect(self.delete_roi)
        self.btn_test.clicked.connect(self.test_roi)
        self.btn_test_all.clicked.connect(self.test_all)

        self.view.roi_drawn.connect(self.add_or_update_roi)

        self.table.itemChanged.connect(self.on_table_item_changed)
        self.table.itemSelectionChanged.connect(self.on_selection_changed)

        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

    def _load_config_rois(self):
        fields = []
        if isinstance(self.config, dict):
            fields = self.config.get("roi_fields", [])
        else:
            fields = getattr(self.config, "roi_fields", [])

        self.rois = []

        for r in fields:
            if isinstance(r, dict):
                name = r.get("name", "roi")
                field_type = r.get("field_type", r.get("type", "text"))
                x = r.get("x", 0)
                y = r.get("y", 0)
                w = r.get("w", 100)
                h = r.get("h", 40)
            else:
                name = getattr(r, "name", "roi")
                field_type = getattr(r, "field_type", getattr(r, "type", "text"))
                x = getattr(r, "x", 0)
                y = getattr(r, "y", 0)
                w = getattr(r, "w", 100)
                h = getattr(r, "h", 40)

            self.rois.append(
                RoiRow(
                    name=str(name),
                    field_type=str(field_type),
                    x=int(x),
                    y=int(y),
                    w=int(w),
                    h=int(h),
                )
            )

        self.refresh_table()

    def _set_image_info(self, path: str):
        p = Path(path)

        try:
            img = Image.open(path)
            width, height = img.size
            fmt = img.format or "-"
            mode = img.mode or "-"
            dpi = img.info.get("dpi", None)

            if dpi:
                dpi_text = f"{dpi}"
            else:
                dpi_text = "-"

            file_size = os.path.getsize(path)
            file_size_mb = file_size / (1024 * 1024)
            mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M:%S")

            self.lbl_name.setText(p.name)
            self.lbl_path.setText(str(p))
            self.lbl_format.setText(fmt)
            self.lbl_mode.setText(mode)
            self.lbl_size.setText(f"{width} × {height}")
            self.lbl_dpi.setText(dpi_text)
            self.lbl_filesize.setText(f"{file_size_mb:.2f} MB")
            self.lbl_modified.setText(mtime)

        except Exception as e:
            self.lbl_name.setText(p.name)
            self.lbl_path.setText(str(p))
            self.lbl_format.setText("-")
            self.lbl_mode.setText("-")
            self.lbl_size.setText("-")
            self.lbl_dpi.setText("-")
            self.lbl_filesize.setText("-")
            self.lbl_modified.setText(f"error: {e}")

    def open_image(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Open image",
            "",
            "Images (*.png *.jpg *.jpeg *.bmp *.tif *.tiff)"
        )
        if not path:
            return

        self.image_path = path
        pix = QPixmap(path)
        if pix.isNull():
            QMessageBox.warning(self, "Open image", "Could not load image.")
            return

        self._set_image_info(path)
        self.view.set_image(pix)
        self.view.draw_rois(self.rois, self.table.currentRow())

    def add_roi(self):
        self.rois.append(RoiRow(name=f"roi_{len(self.rois)+1}"))
        self.refresh_table()
        if self.table.rowCount() > 0:
            self.table.selectRow(self.table.rowCount() - 1)

    def delete_roi(self):
        row = self.table.currentRow()
        if row < 0:
            return
        del self.rois[row]
        self.refresh_table()

    def add_or_update_roi(self, x, y, w, h):
        row = self.table.currentRow()
        if row < 0:
            self.rois.append(RoiRow(x=x, y=y, w=w, h=h))
        else:
            self.rois[row].x = x
            self.rois[row].y = y
            self.rois[row].w = w
            self.rois[row].h = h
        self.refresh_table()

    def _roi_from_table_row(self, row: int) -> RoiRow | None:
        if row < 0 or row >= self.table.rowCount():
            return None
        try:
            return RoiRow(
                name=(self.table.item(row, 0).text().strip() if self.table.item(row, 0) else "roi"),
                field_type=(self.table.item(row, 1).text().strip() if self.table.item(row, 1) else "text"),
                x=int(float(self.table.item(row, 2).text().strip() if self.table.item(row, 2) else 0)),
                y=int(float(self.table.item(row, 3).text().strip() if self.table.item(row, 3) else 0)),
                w=max(1, int(float(self.table.item(row, 4).text().strip() if self.table.item(row, 4) else 1))),
                h=max(1, int(float(self.table.item(row, 5).text().strip() if self.table.item(row, 5) else 1))),
            )
        except Exception:
            return None

    def _sync_rois_from_table(self):
        new_rois = []
        for row in range(self.table.rowCount()):
            roi = self._roi_from_table_row(row)
            if roi is not None:
                new_rois.append(roi)
        self.rois = new_rois

    def refresh_table(self):
        self._updating_table = True
        self.table.setRowCount(len(self.rois))

        for i, r in enumerate(self.rois):
            vals = [r.name, r.field_type, r.x, r.y, r.w, r.h]
            for c, v in enumerate(vals):
                self.table.setItem(i, c, QTableWidgetItem(str(v)))

        self._updating_table = False
        self.view.draw_rois(self.rois, self.table.currentRow())

    def on_table_item_changed(self, item):
        if self._updating_table:
            return
        self._sync_rois_from_table()
        self.view.draw_rois(self.rois, self.table.currentRow())

    def on_selection_changed(self):
        self.view.draw_rois(self.rois, self.table.currentRow())

    def test_roi(self):
        if not self.image_path:
            QMessageBox.warning(self, "Test ROI", "Open an image first.")
            return

        row = self.table.currentRow()
        if row < 0:
            QMessageBox.warning(self, "Test ROI", "Select an ROI row first.")
            return

        roi = self._roi_from_table_row(row)
        if roi is None:
            QMessageBox.warning(self, "Test ROI", "ROI row is incomplete or invalid.")
            return

        img = Image.open(self.image_path)
        img_w, img_h = img.size

        x = max(0, min(roi.x, img_w - 1))
        y = max(0, min(roi.y, img_h - 1))
        w = max(1, min(roi.w, img_w - x))
        h = max(1, min(roi.h, img_h - y))

        crop = img.crop((x, y, x + w, y + h))

        preview = crop.resize((320, 120))
        preview_path = "_roi_preview.png"
        preview.save(preview_path)
        self.preview.setPixmap(QPixmap(preview_path))

        ocr = OCREngine(use_easyocr_first=False)
        value = ocr.read_text(np.array(crop), roi.field_type)

        self.ocr_result.setText(
            f"ROI name: {roi.name}\n"
            f"Type: {roi.field_type}\n"
            f"x={x}, y={y}, w={w}, h={h}\n"
            f"Decoded: {value or '[empty]'}"
        )

    def test_all(self):
        if not self.image_path:
            QMessageBox.warning(self, "Test all ROIs", "Open an image first.")
            return

        img = Image.open(self.image_path)
        img_w, img_h = img.size
        ocr = OCREngine(use_easyocr_first=False)

        results = []

        for row in range(self.table.rowCount()):
            roi = self._roi_from_table_row(row)
            if roi is None:
                results.append(f"row {row + 1}: invalid ROI")
                continue

            x = max(0, min(roi.x, img_w - 1))
            y = max(0, min(roi.y, img_h - 1))
            w = max(1, min(roi.w, img_w - x))
            h = max(1, min(roi.h, img_h - y))

            crop = img.crop((x, y, x + w, y + h))
            value = ocr.read_text(np.array(crop), roi.field_type)

            results.append(f"{roi.name}: {value or '[empty]'}")

        self.ocr_result.setText("\n".join(results))

    def get_config(self):
        self._sync_rois_from_table()

        roi_fields = [
            ROIField(
                name=r.name,
                field_type=r.field_type,
                x=r.x,
                y=r.y,
                w=r.w,
                h=r.h,
            )
            for r in self.rois
        ]

        if isinstance(self.config, dict):
            self.config["roi_fields"] = roi_fields
            return self.config

        self.config.roi_fields = roi_fields
        return self.config