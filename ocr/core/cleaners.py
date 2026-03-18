import re


def clean_value(text: str, field_type: str) -> str:
    text = (text or "").strip()

    ft = (field_type or "").strip().lower()
    if ft in ("line", "station", "dive"):
        ft = "int"
    elif ft in ("east", "north", "easting", "northing"):
        ft = "float"

    text = text.replace("O", "0").replace("o", "0")
    text = text.replace("I", "1").replace("l", "1")
    text = text.replace("|", "1")

    if ft == "int":
        return re.sub(r"\D", "", text)

    if ft == "float":
        return re.sub(r"[^0-9.]", "", text)

    if ft == "date":
        text = re.sub(r"[^0-9-]", "", text)
        if re.fullmatch(r"\d{3}-\d{2}-\d{2}", text):
            text = "2" + text
        return text

    if ft == "time":
        return re.sub(r"[^0-9:]", "", text)

    return text