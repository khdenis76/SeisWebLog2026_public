from __future__ import annotations

import cv2
import numpy as np

from .cleaners import clean_value

try:
    import pytesseract
except Exception:
    pytesseract = None


class OCREngine:
    def __init__(self, use_easyocr_first: bool = False) -> None:
        self.use_easyocr_first = False

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

        if ft == "int":
            return "--psm 8 -c tessedit_char_whitelist=0123456789"

        if ft == "float":
            return "--psm 8 -c tessedit_char_whitelist=0123456789."

        if ft == "date":
            return "--psm 8 -c tessedit_char_whitelist=0123456789-"

        if ft == "time":
            return "--psm 8 -c tessedit_char_whitelist=0123456789:"

        return "--psm 7"

    @staticmethod
    def _prep_variants(crop: np.ndarray) -> list[np.ndarray]:
        if crop is None or crop.size == 0:
            return []

        if len(crop.shape) == 3:
            if crop.shape[2] == 4:
                gray = cv2.cvtColor(crop, cv2.COLOR_BGRA2GRAY)
            else:
                gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
        else:
            gray = crop.copy()

        # enlarge strongly for tiny overlay text
        gray_big = cv2.resize(gray, None, fx=4, fy=4, interpolation=cv2.INTER_CUBIC)

        # variant 1: plain enlarged grayscale
        v1 = gray_big

        # variant 2: thresholded
        _, v2 = cv2.threshold(gray_big, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

        # variant 3: inverted thresholded
        v3 = 255 - v2

        return [v1, v2, v3]

    def read_text(self, crop: np.ndarray, field_type: str) -> str:
        if pytesseract is None:
            return ""

        cfg = self._tesseract_config(field_type)
        variants = self._prep_variants(crop)

        best = ""

        for img in variants:
            try:
                txt = pytesseract.image_to_string(img, config=cfg).strip()
            except Exception:
                txt = ""

            txt = clean_value(txt, self._normalize_field_type(field_type))

            if txt and len(txt) > len(best):
                best = txt

        return best