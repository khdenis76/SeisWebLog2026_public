from __future__ import annotations

from pathlib import Path

import numpy as np

from .models import ProjectShapeDefinition, ShapeLayerData


class ShapeLoadError(RuntimeError):
    pass


def load_shapefile(definition: ProjectShapeDefinition) -> ShapeLayerData:
    """Load one SHP using pyshp and return lightweight NumPy geometry."""
    try:
        import shapefile  # pyshp
    except ImportError as exc:
        raise ShapeLoadError("Shape support requires the 'pyshp' package.") from exc

    path = Path(definition.full_name)
    if not path.exists():
        raise ShapeLoadError(f"Shape file not found: {path}")

    try:
        reader = shapefile.Reader(str(path))
    except Exception as exc:
        raise ShapeLoadError(f"Could not open shape file {path}: {exc}") from exc

    point_types = {
        shapefile.POINT, shapefile.POINTM, shapefile.POINTZ,
        shapefile.MULTIPOINT, shapefile.MULTIPOINTM, shapefile.MULTIPOINTZ,
    }
    line_types = {shapefile.POLYLINE, shapefile.POLYLINEM, shapefile.POLYLINEZ}
    polygon_types = {shapefile.POLYGON, shapefile.POLYGONM, shapefile.POLYGONZ}

    all_points: list[tuple[float, float]] = []
    parts: list[np.ndarray] = []
    geometry_type = "line"

    for shape in reader.iterShapes():
        shape_type = int(shape.shapeType)
        if shape_type in point_types:
            geometry_type = "point"
            all_points.extend((float(p[0]), float(p[1])) for p in shape.points)
            continue

        if shape_type in polygon_types:
            geometry_type = "polygon"
        elif shape_type in line_types and geometry_type != "polygon":
            geometry_type = "line"
        elif not shape.points:
            continue

        starts = list(shape.parts) + [len(shape.points)]
        for start, end in zip(starts[:-1], starts[1:]):
            if end <= start:
                continue
            array = np.asarray(shape.points[start:end], dtype=np.float64)
            if array.ndim == 2 and array.shape[1] >= 2:
                parts.append(np.ascontiguousarray(array[:, :2]))

    point_array = (
        np.asarray(all_points, dtype=np.float64).reshape((-1, 2))
        if all_points
        else np.empty((0, 2), dtype=np.float64)
    )
    return ShapeLayerData(definition, geometry_type, parts, point_array)
