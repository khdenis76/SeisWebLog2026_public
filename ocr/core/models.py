
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List


@dataclass
class ROIField:
    name: str
    field_type: str = "text"
    x: int = 0
    y: int = 0
    w: int = 0
    h: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ROIField":
        return cls(
            name=str(data.get("name", "field")),
            field_type=str(data.get("field_type", "text")),
            x=int(data.get("x", 0)),
            y=int(data.get("y", 0)),
            w=int(data.get("w", 0)),
            h=int(data.get("h", 0)),
        )


@dataclass
class OCRConfig:
    config_name: str = "default"
    file_masks: List[str] = field(default_factory=lambda: ["*.png"])
    include_subfolders: bool = False
    expected_width: int = 0
    expected_height: int = 0
    deploy_images: int = 2
    recovery_images: int = 2
    delta_warning: float = 5.0
    delta_error: float = 10.0
    use_easyocr_first: bool = False
    roi_fields: List[ROIField] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "config_name": self.config_name,
            "file_masks": list(self.file_masks),
            "include_subfolders": self.include_subfolders,
            "expected_width": self.expected_width,
            "expected_height": self.expected_height,
            "deploy_images": self.deploy_images,
            "recovery_images": self.recovery_images,
            "delta_warning": self.delta_warning,
            "delta_error": self.delta_error,
            "use_easyocr_first": self.use_easyocr_first,
            "roi_fields": [f.to_dict() for f in self.roi_fields],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OCRConfig":
        return cls(
            config_name=str(data.get("config_name", "default")),
            file_masks=list(data.get("file_masks", ["*.png"])),
            include_subfolders=bool(data.get("include_subfolders", False)),
            expected_width=int(data.get("expected_width", 0)),
            expected_height=int(data.get("expected_height", 0)),
            deploy_images=int(data.get("deploy_images", 2)),
            recovery_images=int(data.get("recovery_images", 2)),
            delta_warning=float(data.get("delta_warning", 5.0)),
            delta_error=float(data.get("delta_error", 10.0)),
            use_easyocr_first=bool(data.get("use_easyocr_first", False)),
            roi_fields=[ROIField.from_dict(x) for x in data.get("roi_fields", [])],
        )
