from __future__ import annotations

import re
from typing import Any


def normalize_field_type(field_type: str) -> str:
    ft = (field_type or "").strip().lower()
    if ft in ("line", "station", "dive"):
        return "int"
    if ft in ("east", "north", "easting", "northing"):
        return "float"
    if ft in ("string", "str"):
        return "text"
    return ft


def _ocr_character_fixes(text: str) -> str:
    return (
        (text or "").strip()
        .replace("O", "0").replace("o", "0")
        .replace("I", "1").replace("l", "1")
        .replace("|", "1")
        .replace(",", ".")
    )


def clean_value(text: str, field_type: str) -> str:
    """Basic OCR cleanup without forcing a fixed output width."""
    text = _ocr_character_fixes(text)
    ft = normalize_field_type(field_type)

    if ft == "int":
        # Numeric identifier ROIs (line, station and dive) are non-negative.
        return re.sub(r"\D", "", text)

    if ft == "float":
        # Coordinate ROIs (east and north) are configured as positive values.
        body = re.sub(r"[^0-9.]", "", text)
        if body.count(".") > 1:
            first, *rest = body.split(".")
            body = first + "." + "".join(rest)
        return body

    if ft == "date":
        text = re.sub(r"[^0-9-]", "", text)
        if re.fullmatch(r"\d{3}-\d{2}-\d{2}", text):
            text = "2" + text
        return text

    if ft == "time":
        return re.sub(r"[^0-9:]", "", text)

    return (text or "").strip()


def format_value(text: str, roi: Any) -> str:
    """Clean and format OCR output using ROIField settings."""
    ft = normalize_field_type(getattr(roi, "field_type", "text"))
    before = max(0, int(getattr(roi, "digits_before", 0) or 0))
    after = max(0, int(getattr(roi, "digits_after", 0) or 0))
    cleaned = clean_value(text, ft)

    if not cleaned:
        return ""

    if ft == "int":
        digits = re.sub(r"\D", "", cleaned)
        if before:
            # Fixed-width integer output; keep the rightmost digits if OCR returned too many.
            digits = digits[-before:].zfill(before)
        return digits

    if ft == "float":
        unsigned = cleaned

        if "." in unsigned:
            integer_part, decimal_part = unsigned.split(".", 1)
        else:
            digits = re.sub(r"\D", "", unsigned)
            if after:
                digits = digits.zfill(after + 1)
                integer_part = digits[:-after]
                decimal_part = digits[-after:]
            else:
                integer_part = digits
                decimal_part = ""

        integer_part = re.sub(r"\D", "", integer_part) or "0"
        decimal_part = re.sub(r"\D", "", decimal_part)

        if before:
            integer_part = integer_part[-before:].zfill(before)

        if after:
            decimal_part = (decimal_part + ("0" * after))[:after]
            result = f"{integer_part}.{decimal_part}"
        else:
            result = integer_part if not decimal_part else f"{integer_part}.{decimal_part}"

        return result

    if ft == "date":
        digits = re.sub(r"\D", "", cleaned)
        if len(digits) == 8:
            return f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}"
        return cleaned

    if ft == "time":
        digits = re.sub(r"\D", "", cleaned)
        if len(digits) == 6:
            return f"{digits[0:2]}:{digits[2:4]}:{digits[4:6]}"
        if len(digits) == 4:
            return f"{digits[0:2]}:{digits[2:4]}"
        return cleaned

    return cleaned
