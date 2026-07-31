from __future__ import annotations

from dataclasses import dataclass
import numpy as np

from .data import SvpCast


PARAMETERS = {
    "Velocity": {
        "attribute": "velocity",
        "scalar_name": "Sound velocity (m/s)",
        "unit": "m/s",
        "file_stem": "velocity_cube",
    },
    "Salinity": {
        "attribute": "salinity",
        "scalar_name": "Salinity (PSU)",
        "unit": "PSU",
        "file_stem": "salinity_cube",
    },
    "Temperature": {
        "attribute": "temperature",
        "scalar_name": "Temperature (°C)",
        "unit": "°C",
        "file_stem": "temperature_cube",
    },
}


@dataclass(slots=True)
class ScalarCube:
    x: np.ndarray
    y: np.ndarray
    depth: np.ndarray
    values: np.ndarray  # shape z,y,x
    parameter: str
    scalar_name: str
    unit: str
    file_stem: str


# Backwards-compatible alias used by older integrations.
VelocityCube = ScalarCube


def _profiles_on_depth(
    casts: list[SvpCast], depths: np.ndarray, attribute: str
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    xy: list[tuple[float, float]] = []
    profiles: list[np.ndarray] = []

    for cast in casts:
        values = np.asarray(getattr(cast, attribute), dtype=float)
        depth = np.asarray(cast.depth, dtype=float)
        valid = np.isfinite(depth) & np.isfinite(values)
        if valid.sum() < 2:
            continue

        d = depth[valid]
        v = values[valid]
        order = np.argsort(d)
        d, v = d[order], v[order]
        unique_depth, unique_idx = np.unique(d, return_index=True)
        if unique_depth.size < 2:
            continue

        interp = np.interp(
            depths,
            unique_depth,
            v[unique_idx],
            left=np.nan,
            right=np.nan,
        )
        xy.append((cast.x, cast.y))
        profiles.append(interp)

    if not profiles:
        raise ValueError(
            f"No usable {attribute} values are available in the selected SVP casts."
        )

    xy_arr = np.asarray(xy, dtype=float)
    return xy_arr[:, 0], xy_arr[:, 1], np.asarray(profiles, dtype=float)


def build_scalar_cube(
    casts: list[SvpCast],
    parameter: str = "Velocity",
    nx: int = 40,
    ny: int = 40,
    nz: int = 80,
    method: str = "IDW",
    power: float = 2.0,
) -> ScalarCube:
    if not casts:
        raise ValueError("No SVP casts found.")

    spec = PARAMETERS.get(parameter)
    if spec is None:
        raise ValueError(f"Unsupported interpolation parameter: {parameter}")

    usable_casts = []
    for cast in casts:
        values = np.asarray(getattr(cast, spec["attribute"]), dtype=float)
        if np.count_nonzero(np.isfinite(cast.depth) & np.isfinite(values)) >= 2:
            usable_casts.append(cast)

    if not usable_casts:
        raise ValueError(f"No SVP casts contain usable {parameter.lower()} data.")

    xs = np.asarray([c.x for c in usable_casts], dtype=float)
    ys = np.asarray([c.y for c in usable_casts], dtype=float)
    max_depth = float(
        np.nanmax([
            np.nanmax(np.asarray(c.depth)[np.isfinite(c.depth)])
            for c in usable_casts
        ])
    )

    pad_x = max(float(np.ptp(xs)) * 0.05, 1.0)
    pad_y = max(float(np.ptp(ys)) * 0.05, 1.0)
    gx = np.linspace(float(np.min(xs) - pad_x), float(np.max(xs) + pad_x), max(2, nx))
    gy = np.linspace(float(np.min(ys) - pad_y), float(np.max(ys) + pad_y), max(2, ny))
    gz = np.linspace(0.0, max_depth, max(2, nz))

    px, py, profiles = _profiles_on_depth(usable_casts, gz, spec["attribute"])
    xx, yy = np.meshgrid(gx, gy, indexing="xy")
    dx = xx[..., None] - px
    dy = yy[..., None] - py
    dist2 = dx * dx + dy * dy

    if method.lower().startswith("nearest"):
        nearest = np.argmin(dist2, axis=2)
        cube = np.stack([profiles[nearest, k] for k in range(gz.size)], axis=0)
    else:
        weights = 1.0 / np.maximum(dist2, 1e-12) ** (max(power, 0.1) / 2.0)
        cube = np.empty((gz.size, gy.size, gx.size), dtype=float)
        for k in range(gz.size):
            vals = profiles[:, k]
            valid = np.isfinite(vals)
            if not valid.any():
                cube[k] = np.nan
                continue
            w = weights[..., valid]
            cube[k] = np.sum(w * vals[valid], axis=2) / np.sum(w, axis=2)

    return ScalarCube(
        gx,
        gy,
        gz,
        cube,
        parameter,
        spec["scalar_name"],
        spec["unit"],
        spec["file_stem"],
    )


def build_velocity_cube(
    casts: list[SvpCast],
    nx: int = 40,
    ny: int = 40,
    nz: int = 80,
    method: str = "IDW",
    power: float = 2.0,
) -> ScalarCube:
    """Compatibility wrapper for older callers."""
    return build_scalar_cube(casts, "Velocity", nx, ny, nz, method, power)
