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
        finite = np.isfinite(self.x) & np.isfinite(self.y)
        if not finite.any():
            return None
        return (float(self.x[finite].min()), float(self.x[finite].max()),
                float(self.y[finite].min()), float(self.y[finite].max()))

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
    source_epsg: str = ""


@dataclass(slots=True)
class ShapeLayerData:
    definition: ProjectShapeDefinition
    geometry_type: str
    parts: list[np.ndarray] = field(default_factory=list)
    points: np.ndarray = field(default_factory=lambda: np.empty((0, 2), dtype=np.float64))
    source_crs: str = "unknown"
    target_crs: str = "unknown"
    crs_status: str = "Not transformed"

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
        finite = np.isfinite(xy[:, 0]) & np.isfinite(xy[:, 1])
        if not finite.any():
            return None
        xy = xy[finite]
        return (float(xy[:, 0].min()), float(xy[:, 0].max()),
                float(xy[:, 1].min()), float(xy[:, 1].max()))


@dataclass(slots=True)
class BlackBoxFileInfo:
    file_id: int
    name: str
    start_time: str = ""
    end_time: str = ""
    row_count: int = 0

    @property
    def label(self) -> str:
        pieces = [self.name]
        if self.start_time or self.end_time:
            pieces.append(f"{self.start_time or '—'} → {self.end_time or '—'}")
        if self.row_count:
            pieces.append(f"{self.row_count:,} rows")
        return " | ".join(pieces)


@dataclass(slots=True)
class BlackBoxData:
    file_info: BlackBoxFileInfo
    time_seconds: np.ndarray
    time_labels: np.ndarray
    columns: dict[str, np.ndarray] = field(default_factory=dict)
    tracks: dict[str, tuple[np.ndarray, np.ndarray]] = field(default_factory=dict)

    @property
    def count(self) -> int:
        return int(self.time_seconds.size)

    def track(self, preferred: list[str] | None = None) -> tuple[str, np.ndarray, np.ndarray] | None:
        order = preferred or ["GNSS1", "GNSS2", "Vessel", "INS", "USBL", "ROV1", "ROV2"]
        for name in order:
            value = self.tracks.get(name)
            if value is not None and value[0].size:
                return name, value[0], value[1]
        for name, value in self.tracks.items():
            if value[0].size:
                return name, value[0], value[1]
        return None
