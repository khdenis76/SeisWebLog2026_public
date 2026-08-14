from __future__ import annotations

import re
from typing import Any

import cv2
import numpy as np

from .cleaners import clean_value, format_value

try:
    import pytesseract
except Exception:
    pytesseract = None


class OCREngine:
    """Fast fixed-ROI OCR engine.

    Normal operation uses one inexpensive OpenCV threshold and one Tesseract
    call per ROI. A second OCR call is made only when the first result fails a
    basic field-type validation check.
    """

    def __init__(self, use_easyocr_first: bool = False) -> None:
        # Kept for config compatibility. EasyOCR is deliberately not loaded:
        # model startup and inference are slower for these small fixed overlays.
        self.use_easyocr_first = bool(use_easyocr_first)

    @staticmethod
    def _normalize_field_type(field_type: str) -> str:
        ft = (field_type or "").strip().lower()
        if ft in ("line", "station", "dive"):
            return "int"
        if ft in ("east", "north", "easting", "northing"):
            return "float"
        if ft in ("string", "str"):
            return "text"
        return ft

    @classmethod
    def _tesseract_config(cls, field_type: str) -> str:
        ft = cls._normalize_field_type(field_type)
        base = "--oem 1 --psm 7"

        if ft == "int":
            return f"{base} -c tessedit_char_whitelist=0123456789"
        if ft == "float":
            return f"{base} -c tessedit_char_whitelist=0123456789."
        if ft == "date":
            return f"{base} -c tessedit_char_whitelist=0123456789-/."
        if ft == "time":
            return f"{base} -c tessedit_char_whitelist=0123456789:"
        return base

    @staticmethod
    def _to_gray(crop: np.ndarray) -> np.ndarray | None:
        if crop is None or crop.size == 0:
            return None
        if crop.ndim == 2:
            return crop.copy()
        if crop.shape[2] == 4:
            return cv2.cvtColor(crop, cv2.COLOR_BGRA2GRAY)
        return cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)

    @classmethod
    def _fast_preprocess(cls, crop: np.ndarray, threshold: int = 140) -> np.ndarray | None:
        """Competitor-style fixed threshold; intentionally no resize/filter."""
        gray = cls._to_gray(crop)
        if gray is None:
            return None
        _, binary = cv2.threshold(gray, threshold, 255, cv2.THRESH_BINARY)
        return binary

    @classmethod
    def _fallback_preprocess(cls, crop: np.ndarray) -> np.ndarray | None:
        """One fallback only, used when the fast result is invalid."""
        gray = cls._to_gray(crop)
        if gray is None:
            return None
        # 2x linear resize is much cheaper than the previous 4x cubic resize.
        gray = cv2.resize(gray, None, fx=2, fy=2, interpolation=cv2.INTER_LINEAR)
        _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        return binary

    @classmethod
    def _looks_valid(cls, value: str, field_type: str, roi: Any = None) -> bool:
        if not value:
            return False

        ft = cls._normalize_field_type(field_type)
        digits = re.sub(r"\D", "", value)

        if ft == "int":
            expected = int(getattr(roi, "digits_before", 0) or 0) if roi is not None else 0
            return len(digits) == expected if expected else bool(digits)

        if ft == "float":
            before = int(getattr(roi, "digits_before", 0) or 0) if roi is not None else 0
            after = int(getattr(roi, "digits_after", 0) or 0) if roi is not None else 0
            expected = before + after
            return len(digits) >= expected if expected else len(digits) >= 3

        if ft == "date":
            return len(digits) == 8

        if ft == "time":
            return len(digits) in (4, 6)

        return bool(value.strip())

    @staticmethod
    def _ocr(image: np.ndarray | None, config: str) -> str:
        if pytesseract is None or image is None:
            return ""
        try:
            return pytesseract.image_to_string(image, config=config).strip()
        except Exception:
            return ""

    def read_text(self, crop: np.ndarray, field_type: str, roi: Any = None) -> str:
        if pytesseract is None:
            return ""

        normalized_type = self._normalize_field_type(field_type)
        config = self._tesseract_config(normalized_type)

        # Fast path: one OpenCV threshold + one Tesseract call.
        fast_image = self._fast_preprocess(crop)
        fast_raw = self._ocr(fast_image, config)
        fast_value = clean_value(fast_raw, normalized_type)
        if self._looks_valid(fast_value, normalized_type, roi):
            return fast_value

        # Fallback is paid only for missing or structurally invalid output.
        fallback_image = self._fallback_preprocess(crop)
        fallback_raw = self._ocr(fallback_image, config)
        fallback_value = clean_value(fallback_raw, normalized_type)

        if self._looks_valid(fallback_value, normalized_type, roi):
            return fallback_value

        # Preserve the most informative result for QC instead of discarding it.
        return fallback_value if len(fallback_value) > len(fast_value) else fast_value

    def read_roi(self, crop: np.ndarray, roi) -> str:
        """Recognize and apply the output format stored in an ROIField."""
        field_type = getattr(roi, "field_type", "text")
        raw = self.read_text(crop, field_type, roi=roi)
        return format_value(raw, roi)
