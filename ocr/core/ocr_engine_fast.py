from __future__ import annotations

import re

import cv2
import numpy as np

from .cleaners import clean_value, format_value

try:
    import pytesseract
except Exception:
    pytesseract = None


class OCREngine:
    """Fast Tesseract OCR for small, fixed overlay ROIs.

    The old implementation always ran three Tesseract processes per ROI.
    This version runs one inexpensive pass first and only tries fallback
    images when the first result is empty or invalid for the field type.
    """

    def __init__(self, use_easyocr_first: bool = False) -> None:
        # Kept for config compatibility. EasyOCR is intentionally not loaded:
        # its model startup cost is high and the fixed numeric overlay is a
        # better fit for Tesseract with field-specific whitelists.
        self.use_easyocr_first = bool(use_easyocr_first)

    @staticmethod
    def _normalize_field_type(field_type: str) -> str:
        ft = (field_type or "").strip().lower()
        if ft in ("line", "station", "dive"):
            return "int"
        if ft in ("east", "north", "easting", "northing"):
            return "float"
        return ft

    @classmethod
    def _tesseract_config(cls, field_type: str) -> str:
        ft = cls._normalize_field_type(field_type)
        base = "--oem 1 --psm 8"
        if ft == "int":
            return base + " -c tessedit_char_whitelist=0123456789"
        if ft == "float":
            return base + " -c tessedit_char_whitelist=0123456789."
        if ft == "date":
            return base + " -c tessedit_char_whitelist=0123456789-"
        if ft == "time":
            return base + " -c tessedit_char_whitelist=0123456789:"
        return base

    @staticmethod
    def _to_gray(crop: np.ndarray) -> np.ndarray | None:
        if crop is None or crop.size == 0:
            return None
        if crop.ndim == 3:
            code = cv2.COLOR_BGRA2GRAY if crop.shape[2] == 4 else cv2.COLOR_BGR2GRAY
            return cv2.cvtColor(crop, code)
        return crop.copy()

    @classmethod
    def _fast_image(cls, crop: np.ndarray) -> np.ndarray | None:
        gray = cls._to_gray(crop)
        if gray is None:
            return None
        # 2x is enough for the supplied 35 px-high overlay and is cheaper than 4x.
        big = cv2.resize(gray, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
        _, binary = cv2.threshold(big, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        return binary

    @classmethod
    def _fallback_images(cls, crop: np.ndarray) -> list[np.ndarray]:
        gray = cls._to_gray(crop)
        if gray is None:
            return []
        big = cv2.resize(gray, None, fx=4, fy=4, interpolation=cv2.INTER_CUBIC)
        _, binary = cv2.threshold(big, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        return [big, 255 - binary]

    @classmethod
    def _is_valid(cls, value: str, field_type: str) -> bool:
        value = (value or "").strip()
        if not value:
            return False
        ft = cls._normalize_field_type(field_type)
        if ft == "int":
            return value.isdigit()
        if ft == "float":
            return re.fullmatch(r"\d+(?:\.\d+)?", value) is not None
        if ft == "date":
            return re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", value) is not None
        if ft == "time":
            return re.fullmatch(r"\d{1,2}:\d{2}(?::\d{2})?", value) is not None
        return True

    def _recognize(self, image: np.ndarray, field_type: str) -> str:
        try:
            text = pytesseract.image_to_string(
                image,
                config=self._tesseract_config(field_type),
            ).strip()
        except Exception:
            return ""
        return clean_value(text, self._normalize_field_type(field_type))

    def read_text(self, crop: np.ndarray, field_type: str) -> str:
        if pytesseract is None:
            return ""

        first = self._fast_image(crop)
        if first is None:
            return ""

        value = self._recognize(first, field_type)
        if self._is_valid(value, field_type):
            return value

        best = value
        for image in self._fallback_images(crop):
            candidate = self._recognize(image, field_type)
            if self._is_valid(candidate, field_type):
                return candidate
            if len(candidate) > len(best):
                best = candidate
        return best

    def read_roi(self, crop: np.ndarray, roi) -> str:
        raw = self.read_text(crop, getattr(roi, "field_type", "text"))
        return format_value(raw, roi)
