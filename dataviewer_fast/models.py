from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np


@dataclass(slots=True)
class PointLayerData:
    name: str
    x: np.ndarray
    y: np.ndarray
    source_index: np.ndarray
    metadata: dict[str, np.ndarray] = field(default_factory=dict)

    @property
    def count(self) -> int:
        return int(self.x.size)

    @property
    def bounds(self) -> tuple[float, float, float, float] | None:
        if self.count == 0:
            return None
        return (
            float(np.nanmin(self.x)),
            float(np.nanmax(self.x)),
            float(np.nanmin(self.y)),
            float(np.nanmax(self.y)),
        )

    def record(self, index: int) -> dict[str, Any]:
        result: dict[str, Any] = {"x": float(self.x[index]), "y": float(self.y[index])}
        for key, values in self.metadata.items():
            value = values[index]
            result[key] = value.item() if hasattr(value, "item") else value
        return result


@dataclass(slots=True)
class ProjectShapeDefinition:
    name: str
    full_name: Path
    is_filled: bool = False
    fill_color: str = "#000000"
    line_color: str = "#000000"
    line_width: float = 1.0
    line_style: str = "solid"
    hatch_pattern: str = ""


@dataclass(slots=True)
class ShapeLayerData:
    """Geometry for one database-registered shapefile."""

    definition: ProjectShapeDefinition
    geometry_type: str
    parts: list[np.ndarray] = field(default_factory=list)
    points: np.ndarray = field(default_factory=lambda: np.empty((0, 2), dtype=np.float64))

    @property
    def name(self) -> str:
        return self.definition.name

    @property
    def count(self) -> int:
        if self.geometry_type == "point":
            return int(self.points.shape[0])
        return int(sum(part.shape[0] for part in self.parts))

    @property
    def bounds(self) -> tuple[float, float, float, float] | None:
        arrays = [self.points] if self.geometry_type == "point" else self.parts
        arrays = [array for array in arrays if array.size]
        if not arrays:
            return None
        xy = np.vstack(arrays)
        return (
            float(np.nanmin(xy[:, 0])),
            float(np.nanmax(xy[:, 0])),
            float(np.nanmin(xy[:, 1])),
            float(np.nanmax(xy[:, 1])),
        )
