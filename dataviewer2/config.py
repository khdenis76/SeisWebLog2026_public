from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class CustomDsrLayerDefinition:
    id: str
    name: str
    x_field: str
    y_field: str
    filter_field: str = ""
    filter_operator: str = ""
    filter_value: Any = None
    category_field: str = ""
    color: str = "#00e5ff"
    point_size: float = 7.0
    visible: bool = True
    split_by_line: bool = True
    show_stations: bool = True
    group_name: str = "Custom DSR Layers"
    categories: dict[str, dict[str, Any]] = field(default_factory=dict)


class ProjectViewerConfig:
    """Project-local persistent DataViewer settings.

    Settings are intentionally stored outside SQLite so operational project
    schemas are not modified by the desktop viewer.
    """

    VERSION = 2

    def __init__(self, project_path: str | Path) -> None:
        supplied = Path(project_path)
        if supplied.suffix.lower() in {".sqlite", ".sqlite3", ".db"}:
            supplied = supplied.parent.parent if supplied.parent.name.lower() == "data" else supplied.parent
        self.project_path = supplied
        self.config_dir = self.project_path / "config"
        self.path = self.config_dir / "dataviewer2.json"
        self.shape_styles: dict[str, dict[str, Any]] = {}
        self.custom_dsr_layers: list[CustomDsrLayerDefinition] = []
        self.group_order: list[str] = []
        self.layer_order: dict[str, list[str]] = {}
        self.theme: str = "night"
        self.label_styles: dict[str, dict[str, Any]] = {}
        self.layer_visibility: dict[str, bool] = {}
        self.group_visibility: dict[str, bool] = {}
        self.removed_layers: list[str] = []
        self.labels_enabled: bool = False
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return
        self.shape_styles = dict(payload.get("shape_styles") or {})
        self.group_order = [str(value) for value in (payload.get("group_order") or [])]
        self.theme = "day" if str(payload.get("theme") or "night").lower() == "day" else "night"
        self.label_styles = {
            str(name): dict(style)
            for name, style in dict(payload.get("label_styles") or {}).items()
            if isinstance(style, dict)
        }
        self.layer_visibility = {
            str(name): bool(visible)
            for name, visible in dict(payload.get("layer_visibility") or {}).items()
        }
        self.group_visibility = {
            str(name): bool(visible)
            for name, visible in dict(payload.get("group_visibility") or {}).items()
        }
        self.removed_layers = [str(name) for name in payload.get("removed_layers") or []]
        self.labels_enabled = bool(payload.get("labels_enabled", False))
        self.layer_order = {
            str(group): [str(value) for value in values]
            for group, values in dict(payload.get("layer_order") or {}).items()
            if isinstance(values, list)
        }
        self.custom_dsr_layers = []
        for raw in payload.get("custom_dsr_layers") or []:
            try:
                self.custom_dsr_layers.append(CustomDsrLayerDefinition(**raw))
            except TypeError:
                continue

    def save(self) -> None:
        self.config_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": self.VERSION,
            "shape_styles": self.shape_styles,
            "custom_dsr_layers": [asdict(item) for item in self.custom_dsr_layers],
            "group_order": self.group_order,
            "layer_order": self.layer_order,
            "theme": self.theme,
            "label_styles": self.label_styles,
            "layer_visibility": self.layer_visibility,
            "group_visibility": self.group_visibility,
            "removed_layers": self.removed_layers,
            "labels_enabled": self.labels_enabled,
        }
        self.path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def set_shape_style(self, layer_name: str, style: dict[str, Any]) -> None:
        self.shape_styles[layer_name] = dict(style)
        self.save()

    def add_custom_layer(self, definition: CustomDsrLayerDefinition) -> None:
        self.custom_dsr_layers = [item for item in self.custom_dsr_layers if item.id != definition.id]
        self.custom_dsr_layers.append(definition)
        self.save()

    def remove_custom_layer(self, definition_id: str) -> None:
        self.custom_dsr_layers = [item for item in self.custom_dsr_layers if item.id != definition_id]
        self.save()

    def set_layer_tree_order(
        self, group_order: list[str], layer_order: dict[str, list[str]]
    ) -> None:
        self.group_order = [str(value) for value in group_order]
        self.layer_order = {
            str(group): [str(value) for value in values]
            for group, values in layer_order.items()
        }
        self.save()

    def set_layer_tree_state(
        self,
        group_order: list[str],
        layer_order: dict[str, list[str]],
        layer_visibility: dict[str, bool],
        group_visibility: dict[str, bool],
    ) -> None:
        self.group_order = [str(value) for value in group_order]
        self.layer_order = {
            str(group): [str(value) for value in values]
            for group, values in layer_order.items()
        }
        self.layer_visibility = {
            str(name): bool(visible) for name, visible in layer_visibility.items()
        }
        self.group_visibility = {
            str(name): bool(visible) for name, visible in group_visibility.items()
        }
        self.save()

    def mark_layer_removed(self, layer_name: str) -> None:
        name = str(layer_name)
        if name not in self.removed_layers:
            self.removed_layers.append(name)
        self.layer_visibility.pop(name, None)
        self.save()

    def mark_layer_present(self, layer_name: str) -> None:
        name = str(layer_name)
        if name in self.removed_layers:
            self.removed_layers.remove(name)
            self.save()

    def is_layer_removed(self, layer_name: str) -> bool:
        return str(layer_name) in self.removed_layers

    def set_labels_enabled(self, enabled: bool) -> None:
        self.labels_enabled = bool(enabled)
        self.save()


    def set_label_style(self, layer_name: str, style: dict[str, Any]) -> None:
        self.label_styles[str(layer_name)] = dict(style)
        self.save()

    def get_label_style(self, layer_name: str) -> dict[str, Any]:
        return dict(self.label_styles.get(str(layer_name), {}))

    def set_theme(self, theme: str) -> None:
        self.theme = "day" if str(theme).lower() == "day" else "night"
        self.save()
