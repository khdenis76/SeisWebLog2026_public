from __future__ import annotations

import struct
import importlib.util
from pathlib import Path
import numpy as np

from .models import ProjectShapeDefinition, ShapeLayerData


class ShapeLoadError(RuntimeError):
    pass


def _pyarrow_available() -> bool:
    """Return whether pyogrio's optional Arrow acceleration can be used."""
    return importlib.util.find_spec("pyarrow") is not None


def _read_native_shp(path: Path) -> tuple[str, list[np.ndarray], np.ndarray]:
    point_records: list[tuple[float, float]] = []
    parts: list[np.ndarray] = []
    geometry_type = "line"
    with path.open("rb") as handle:
        header = handle.read(100)
        if len(header) != 100 or struct.unpack(">i", header[:4])[0] != 9994:
            raise ShapeLoadError(f"Invalid shapefile header: {path}")
        while True:
            record_header = handle.read(8)
            if not record_header:
                break
            if len(record_header) != 8:
                raise ShapeLoadError(f"Truncated shapefile record header: {path}")
            _record_number, words = struct.unpack(">2i", record_header)
            content = handle.read(words * 2)
            if len(content) != words * 2:
                raise ShapeLoadError(f"Truncated shapefile record: {path}")
            if len(content) < 4:
                continue
            shape_type = struct.unpack("<i", content[:4])[0]
            if shape_type == 0:
                continue
            if shape_type in {1, 11, 21}:
                if len(content) >= 20:
                    point_records.append(struct.unpack("<2d", content[4:20]))
                    geometry_type = "point"
                continue
            if shape_type in {8, 18, 28}:
                if len(content) < 40:
                    continue
                count = struct.unpack("<i", content[36:40])[0]
                raw = np.frombuffer(content, dtype="<f8", count=count * 2, offset=40)
                if raw.size == count * 2:
                    point_records.extend(map(tuple, raw.reshape((-1, 2))))
                    geometry_type = "point"
                continue
            if shape_type not in {3, 5, 13, 15, 23, 25} or len(content) < 44:
                continue
            number_parts, number_points = struct.unpack("<2i", content[36:44])
            parts_offset = 44
            points_offset = parts_offset + number_parts * 4
            if points_offset + number_points * 16 > len(content):
                continue
            starts = list(struct.unpack(f"<{number_parts}i", content[parts_offset:points_offset]))
            starts.append(number_points)
            raw = np.frombuffer(content, dtype="<f8", count=number_points * 2, offset=points_offset)
            xy = raw.reshape((-1, 2)).copy()
            for start, end in zip(starts[:-1], starts[1:]):
                if end > start:
                    parts.append(np.ascontiguousarray(xy[start:end]))
            geometry_type = "polygon" if shape_type in {5, 15, 25} else "line"
    points = (np.asarray(point_records, dtype=np.float64).reshape((-1, 2))
              if point_records else np.empty((0, 2), dtype=np.float64))
    return geometry_type, parts, points


def _read_source_crs(definition: ProjectShapeDefinition):
    try:
        from pyproj import CRS
    except ImportError as exc:
        raise ShapeLoadError("Shape reprojection requires 'pyproj'. Run: python -m pip install pyproj") from exc
    if definition.source_epsg:
        try:
            return CRS.from_user_input(definition.source_epsg)
        except Exception:
            pass
    prj_path = definition.full_name.with_suffix(".prj")
    if not prj_path.exists():
        raise ShapeLoadError(
            f"CRS is unknown for '{definition.name}'. No valid database EPSG and no .prj file: {prj_path}"
        )
    wkt = prj_path.read_text(encoding="utf-8", errors="ignore").strip()
    if not wkt:
        raise ShapeLoadError(f"Empty .prj file for '{definition.name}': {prj_path}")
    try:
        return CRS.from_wkt(wkt)
    except Exception as exc:
        raise ShapeLoadError(f"Cannot read CRS for '{definition.name}': {exc}") from exc


def _transform_array(array: np.ndarray, transformer) -> np.ndarray:
    if not array.size:
        return array
    x, y = transformer.transform(array[:, 0], array[:, 1])
    return np.column_stack((np.asarray(x, dtype=np.float64), np.asarray(y, dtype=np.float64)))


def _geometry_arrays_from_geodataframe(frame) -> tuple[str, list[np.ndarray], np.ndarray]:
    """Convert GeoPandas/Shapely geometries into the existing fast NumPy format."""
    parts: list[np.ndarray] = []
    points: list[tuple[float, float]] = []
    kinds: set[str] = set()

    def visit(geometry) -> None:
        if geometry is None or geometry.is_empty:
            return
        geom_type = geometry.geom_type
        if geom_type == "Point":
            points.append((float(geometry.x), float(geometry.y))); kinds.add("point")
        elif geom_type == "MultiPoint":
            for child in geometry.geoms: visit(child)
        elif geom_type in {"LineString", "LinearRing"}:
            array = np.asarray(geometry.coords, dtype=np.float64)
            if array.shape[0]: parts.append(np.ascontiguousarray(array[:, :2])); kinds.add("line")
        elif geom_type == "MultiLineString":
            for child in geometry.geoms: visit(child)
        elif geom_type == "Polygon":
            exterior = np.asarray(geometry.exterior.coords, dtype=np.float64)
            if exterior.shape[0]: parts.append(np.ascontiguousarray(exterior[:, :2]))
            for ring in geometry.interiors:
                array = np.asarray(ring.coords, dtype=np.float64)
                if array.shape[0]: parts.append(np.ascontiguousarray(array[:, :2]))
            kinds.add("polygon")
        elif geom_type == "MultiPolygon":
            for child in geometry.geoms: visit(child)
        elif geom_type == "GeometryCollection":
            for child in geometry.geoms: visit(child)

    for geometry in frame.geometry:
        visit(geometry)
    geometry_type = "polygon" if "polygon" in kinds else ("line" if "line" in kinds else "point")
    point_array = np.asarray(points, dtype=np.float64).reshape((-1, 2)) if points else np.empty((0, 2), dtype=np.float64)
    return geometry_type, parts, point_array


def load_vector_layer(definition: ProjectShapeDefinition, project_epsg: str) -> ShapeLayerData:
    path = Path(definition.full_name)
    if definition.source_type == "gpkg":
        if not path.exists():
            raise ShapeLoadError(f"GeoPackage not found: {path}")
        try:
            import pyogrio
        except ImportError as exc:
            raise ShapeLoadError("GeoPackage support requires pyogrio. Run: python -m pip install pyogrio") from exc
        try:
            frame = pyogrio.read_dataframe(
                path,
                layer=definition.source_layer,
                use_arrow=_pyarrow_available(),
            )
        except Exception as exc:
            raise ShapeLoadError(f"Cannot read GeoPackage layer '{definition.source_layer}': {exc}") from exc
        if frame.crs is None and definition.source_epsg:
            frame = frame.set_crs(definition.source_epsg, allow_override=True)
        if frame.crs is None:
            raise ShapeLoadError(f"CRS is unknown for GeoPackage layer '{definition.source_layer}'")
        if not project_epsg:
            raise ShapeLoadError("Project EPSG is missing from project_main.epsg")
        source_text = frame.crs.to_string()
        try:
            if str(frame.crs) != str(project_epsg):
                frame = frame.to_crs(project_epsg)
                status = "Reprojected to project CRS"
            else:
                status = "Already in project CRS"
        except Exception as exc:
            raise ShapeLoadError(f"Failed to reproject '{definition.name}': {exc}") from exc
        geometry_type, parts, points = _geometry_arrays_from_geodataframe(frame)
        return ShapeLayerData(definition, geometry_type, parts, points,
                              source_crs=source_text, target_crs=str(project_epsg), crs_status=status)
    return load_shapefile(definition, project_epsg)


def load_shapefile(definition: ProjectShapeDefinition, project_epsg: str) -> ShapeLayerData:
    path = Path(definition.full_name)
    if not path.exists():
        raise ShapeLoadError(f"Shape file not found: {path}")
    if path.suffix.lower() != ".shp":
        raise ShapeLoadError(f"DataViewer 2.0 currently supports .shp files: {path}")
    geometry_type, parts, points = _read_native_shp(path)
    if not project_epsg:
        raise ShapeLoadError("Project EPSG is missing from project_main.epsg")
    try:
        from pyproj import CRS, Transformer
        source = _read_source_crs(definition)
        target = CRS.from_user_input(project_epsg)
        if source == target:
            status = "Already in project CRS"
        else:
            transformer = Transformer.from_crs(source, target, always_xy=True)
            points = _transform_array(points, transformer)
            parts = [_transform_array(part, transformer) for part in parts]
            status = "Reprojected to project CRS"
        source_text = source.to_string()
        target_text = target.to_string()
    except ShapeLoadError:
        raise
    except Exception as exc:
        raise ShapeLoadError(f"Failed to reproject '{definition.name}': {exc}") from exc
    return ShapeLayerData(definition, geometry_type, parts, points,
                          source_crs=source_text, target_crs=target_text, crs_status=status)
