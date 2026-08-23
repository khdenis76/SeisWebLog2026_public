from __future__ import annotations

import re
from typing import Any

import numpy as np
import pyqtgraph as pg
from PySide6 import QtGui

from .layers import FastPointLayer


DEFAULT_LABEL_STYLE: dict[str, Any] = {
    "enabled": True,
    "format": "",
    "font_size": 9.0,
    "color": "#f3f6f8",
    "offset_x": 5.0,
    "offset_y": -5.0,
    "max_labels": 300,
}


class MapLabelManager:
    """Viewport-aware labels for every point layer.

    Styling is stored per layer in ``config/dataviewer2.json``.  Labels are
    plain text (no rectangle/background) and may contain any metadata fields,
    e.g. ``{Line}/{Station}\n{Node}\n{ROV}``.
    """

    _field_re = re.compile(r"\{([^{}]+)\}")

    def __init__(self, plot_item: pg.PlotItem, max_labels: int = 300, config=None) -> None:
        self.plot_item = plot_item
        self.max_labels = max(25, int(max_labels))
        self.enabled = False
        self.config = config
        self.items: list[pg.TextItem] = []

    def clear(self) -> None:
        for item in self.items:
            try:
                self.plot_item.removeItem(item)
            except Exception:
                pass
        self.items.clear()

    def style_for(self, layer_name: str) -> dict[str, Any]:
        style = dict(DEFAULT_LABEL_STYLE)
        if self.config is not None:
            try:
                style.update(self.config.get_label_style(layer_name))
            except Exception:
                style.update(dict(getattr(self.config, "label_styles", {}).get(layer_name, {})))
        return style

    def set_style(self, layer_name: str, style: dict[str, Any]) -> None:
        merged = self.style_for(layer_name)
        merged.update(style)
        if self.config is not None:
            setter = getattr(self.config, "set_label_style", None)
            if callable(setter):
                setter(layer_name, merged)
            else:
                self.config.label_styles[layer_name] = merged
                self.config.save()

    @staticmethod
    def _metadata(layer: FastPointLayer, *names: str):
        if layer.data is None:
            return None
        by_lower = {key.lower(): value for key, value in layer.data.metadata.items()}
        for name in names:
            value = by_lower.get(name.lower())
            if value is not None:
                return value
        return None

    @staticmethod
    def _format_value(value) -> str:
        if value is None:
            return ""
        try:
            numeric = float(value)
            if np.isfinite(numeric):
                if numeric.is_integer():
                    return str(int(numeric))
                return f"{numeric:g}"
        except (TypeError, ValueError):
            pass
        text = str(value).strip()
        return "" if text.lower() in {"", "none", "nan"} else text

    def default_format(self, layer: FastPointLayer) -> str:
        if layer.data is None:
            return ""
        keys = list(layer.data.metadata.keys())
        by_lower = {key.lower(): key for key in keys}

        line = next((by_lower[k] for k in ("line", "rline", "receiverline", "dsrline") if k in by_lower), None)
        station = next((by_lower[k] for k in ("station", "point", "linepoint") if k in by_lower), None)
        node = next((by_lower[k] for k in ("node", "node_hex_id", "dsrnode") if k in by_lower), None)
        images = by_lower.get("images")

        if layer.name.casefold() == "ocr image counts" and images:
            return f"{{{images}}}"

        if line and station:
            return f"{{{line}}}/{{{station}}}"
        if station:
            return f"{{{station}}}"
        if line:
            return f"{{{line}}}"
        # Keep the default conservative; users can insert any other fields in
        # Layer Style.  One first metadata field is better than an unreadable
        # concatenation of dozens of DSR fields.
        return f"{{{keys[0]}}}" if keys else ""

    def render_text(self, layer: FastPointLayer, index: int, template: str) -> str:
        if layer.data is None:
            return ""
        metadata = layer.data.metadata
        by_lower = {key.lower(): key for key in metadata}

        def replace(match: re.Match[str]) -> str:
            requested = match.group(1).strip()
            virtual = requested.casefold().replace("_", "")
            try:
                if virtual in {"mapeasting", "eastingx"}:
                    return self._format_value(layer.data.x[index])
                if virtual in {"mapnorthing", "northingy"}:
                    return self._format_value(layer.data.y[index])
                if virtual in {"sourceindex", "recordindex"}:
                    return self._format_value(layer.data.source_index[index])
            except Exception:
                return ""
            actual = requested if requested in metadata else by_lower.get(requested.lower())
            if actual is None:
                return ""
            values = metadata.get(actual)
            try:
                return self._format_value(values[index])
            except Exception:
                return ""

        return self._field_re.sub(replace, template.replace("\\n", "\n")).strip()

    def refresh(self, layers: dict[str, object]) -> None:
        self.clear()

        (xmin, xmax), (ymin, ymax) = self.plot_item.vb.viewRange()
        candidates: list[tuple[FastPointLayer, np.ndarray, dict[str, Any], str]] = []
        global_remaining = self.max_labels

        # Every point layer can be labelled. Raster, polygon and comparison
        # layers are ignored because they do not expose FastPointLayer metadata.
        for layer in layers.values():
            if not isinstance(layer, FastPointLayer) or not layer.visible or layer.data is None:
                continue
            if not self.enabled and layer.name.casefold() != "ocr image counts":
                continue
            style = self.style_for(layer.name)
            if not bool(style.get("enabled", True)):
                continue
            data = layer.data
            mask = (
                np.isfinite(data.x) & np.isfinite(data.y)
                & (data.x >= xmin) & (data.x <= xmax)
                & (data.y >= ymin) & (data.y <= ymax)
            )
            indices = np.flatnonzero(mask)
            if not indices.size:
                continue
            template = str(style.get("format") or "").strip() or self.default_format(layer)
            if not template:
                continue
            candidates.append((layer, indices, style, template))

        for layer, indices, style, template in candidates:
            if global_remaining <= 0:
                break
            per_layer_max = max(1, int(style.get("max_labels", self.max_labels)))
            allowed = min(global_remaining, per_layer_max)
            if indices.size > allowed:
                step = int(np.ceil(indices.size / allowed))
                indices = indices[::step][:allowed]

            color = QtGui.QColor(str(style.get("color", "#f3f6f8")))
            font = QtGui.QFont()
            font.setPointSizeF(max(4.0, float(style.get("font_size", 9.0))))
            dx = float(style.get("offset_x", 5.0))
            dy = float(style.get("offset_y", -5.0))

            for index in indices:
                text = self.render_text(layer, int(index), template)
                if not text:
                    continue
                item = pg.TextItem(
                    text=text,
                    color=color,
                    anchor=(0.0, 1.0),
                    border=None,
                    fill=None,
                )
                item.setFont(font)
                item.setPos(float(layer.data.x[index]), float(layer.data.y[index]))
                # TextItem's child text object ignores map scaling; this gives a
                # stable screen-pixel offset from the point where supported by
                # the installed pyqtgraph version.
                try:
                    item.textItem.setPos(dx, dy)
                except Exception:
                    pass
                item.setZValue(50000.0)
                self.plot_item.addItem(item)
                self.items.append(item)
                global_remaining -= 1
                if global_remaining <= 0:
                    break
