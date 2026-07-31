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
    categories: dict[str, dict[str, Any]] = field(default_factory=dict)


class ProjectViewerConfig:
    """Project-local persistent DataViewer settings.

    Settings are intentionally stored outside SQLite so operational project
    schemas are not modified by the desktop viewer.
    """

    VERSION = 1

    def __init__(self, project_path: str | Path) -> None:
        supplied = Path(project_path)
        if supplied.suffix.lower() in {".sqlite", ".sqlite3", ".db"}:
            supplied = supplied.parent.parent if supplied.parent.name.lower() == "data" else supplied.parent
        self.project_path = supplied
        self.config_dir = self.project_path / "config"
        self.path = self.config_dir / "dataviewer2.json"
        self.shape_styles: dict[str, dict[str, Any]] = {}
        self.custom_dsr_layers: list[CustomDsrLayerDefinition] = []
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return
        self.shape_styles = dict(payload.get("shape_styles") or {})
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
