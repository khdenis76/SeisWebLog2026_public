from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SVPFormatSetup:
    # file type
    format_name: str = "generic"
    file_ext: str | None = None

    # header detection
    header_line_count: int | None = None
    data_header_line_index: int | None = None
    data_start_line_index: int | None = None
    delimiter: str | None = None

    # metadata fields from file header
    meta_name_key: str | None = None
    meta_location_key: str | None = None
    meta_timestamp_key: str | None = None
    meta_rov_key: str | None = None
    meta_serial_key: str | None = None
    meta_model_key: str | None = None
    meta_make_key: str | None = None
    meta_lat_key: str | None = None
    meta_lon_key: str | None = None
    meta_coordinates_key: str | None = None

    # data columns
    col_timestamp: str | None = None
    col_depth: str | None = None
    col_velocity: str | None = None
    col_temperature: str | None = None
    col_salinity: str | None = None
    col_density: str | None = None

    # optional transforms
    use_calc_velocity: bool = True
    pressure_is_depth: bool = True
    sort_by_depth: bool = True
    clamp_negative_depth_to_zero: bool = True

    # raw config container
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()