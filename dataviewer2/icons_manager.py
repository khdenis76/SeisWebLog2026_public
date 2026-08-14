from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from PySide6 import QtCore, QtGui

try:
    from PySide6 import QtSvg
except ImportError:  # pragma: no cover - optional on unusual PySide builds
    QtSvg = None


class IconManager:
    """Central access to the SVG icon collection shipped with DataViewer 2.

    Icons are addressed by a stable semantic key rather than by file path. SVGs
    using ``currentColor`` are rendered in a functional accent color so they stay
    readable in both light and dark Qt themes.
    """

    ROOT = Path(__file__).resolve().parent / "icons"

    FILES: dict[str, str] = {
        # General application and navigation
        "app": "navigation/map.svg",
        "map": "navigation/map.svg",
        "reload": "actions/refresh-cw.svg",
        "zoom_all": "layout/maximize-2.svg",
        "zoom_in": "navigation/zoom-in.svg",
        "zoom_out": "navigation/zoom-out.svg",
        "zoom_layer": "navigation/scan-search.svg",
        "zoom_line": "navigation/route.svg",
        "zoom_station": "navigation/crosshair.svg",
        "previous": "actions/chevron-left.svg",
        "next": "actions/chevron-right.svg",
        "show": "actions/eye.svg",
        "hide": "actions/eye-off.svg",
        "show_only": "navigation/target.svg",
        "grid": "layout/grid-3x3.svg",
        "side_panel": "navigation/panel-left.svg",
        "measure": "miscellaneous/ruler.svg",
        "measure_distance": "miscellaneous/ruler.svg",
        "measure_area": "layers/shapes.svg",
        "measure_bearing": "navigation/compass.svg",
        "measure_angle": "navigation/drafting-compass.svg",
        "draw_radius_circle": "layers/circle-dot-dashed.svg",
        "draw_line": "miscellaneous/baseline.svg",
        "draw_polygon": "layers/pentagon.svg",
        "draw_circle": "layers/circle.svg",
        "draw_free": "miscellaneous/paintbrush.svg",
        "draw_text": "miscellaneous/type.svg",
        "tools": "actions/wrench.svg",
        "undo": "actions/undo-2.svg",
        "clear": "actions/trash-2.svg",
        "delete": "actions/trash-2.svg",
        "copy": "actions/copy.svg",
        "properties": "actions/settings-2.svg",
        "style": "miscellaneous/palette.svg",
        "select_radius": "layers/circle-dot-dashed.svg",
        "select_polygon": "layers/pentagon.svg",
        "select_shape": "layers/shapes.svg",
        "labels_show": "layers/message-square-text.svg",
        "labels_hide": "layers/message-square-off.svg",
        "open_folder": "files/folder-open.svg",
        "create": "layers/layers-plus.svg",
        "manage": "miscellaneous/sliders-horizontal.svg",
        "save": "actions/save.svg",
        "change_project": "media/switch-camera.svg",
        "project_folder": "files/folder-cog.svg",
        "exit": "actions/log-out.svg",
        "export_map": "media/image-down.svg",
        "export_csv": "files/file-down.svg",
        "export_visible": "database/table.svg",
        "report_project": "files/file-text.svg",
        "report_dsr": "files/clipboard-list.svg",
        "reports_folder": "files/folder-output.svg",
        # Layer tree
        "group": "layout/list-tree.svg",
        "layer": "layers/layers.svg",
        "preplot": "navigation/route.svg",
        "receiver": "devices/radio-tower.svg",
        "rec_db": "navigation/target.svg",
        "shape": "layers/shapes.svg",
        "custom_layer": "layers/layers-plus.svg",
        "line": "navigation/route.svg",
        "station": "navigation/map-pin.svg",
        "loading": "layers/loader-circle.svg",
        # QC and BlackBox
        "qc": "charts/chart-no-axes-combined.svg",
        "chart": "charts/chart-line.svg",
        "activity": "charts/activity.svg",
        "bbox": "navigation/satellite.svg",
        "bbox_track": "navigation/satellite-dish.svg",
        "database": "database/database.svg",
        "view3d": "miscellaneous/axis-3d.svg",
        "bathymetry": "miscellaneous/mountain.svg",
        "receivers3d": "devices/radio-tower.svg",
        # Ordering
        "move_top": "actions/chevrons-up.svg",
        "move_up": "actions/chevron-up.svg",
        "move_down": "actions/chevron-down.svg",
        "move_bottom": "actions/chevrons-down.svg",
    }

    COLORS: dict[str, str] = {
        "app": "#1976D2",
        "map": "#1976D2",
        "reload": "#1976D2",
        "zoom_all": "#1565C0",
        "zoom_in": "#1565C0",
        "zoom_out": "#1565C0",
        "zoom_layer": "#1565C0",
        "zoom_line": "#1976D2",
        "zoom_station": "#D32F2F",
        "previous": "#455A64",
        "next": "#455A64",
        "show": "#2E7D32",
        "hide": "#C62828",
        "show_only": "#6A1B9A",
        "grid": "#546E7A",
        "side_panel": "#546E7A",
        "measure": "#F57C00",
        "measure_distance": "#F57C00",
        "measure_area": "#2E7D32",
        "measure_bearing": "#1565C0",
        "measure_angle": "#8E24AA",
        "draw_radius_circle": "#D84315",
        "draw_line": "#00897B",
        "draw_polygon": "#7B1FA2",
        "draw_circle": "#1976D2",
        "draw_free": "#C2185B",
        "draw_text": "#5D4037",
        "tools": "#546E7A",
        "undo": "#6D4C41",
        "clear": "#C62828",
        "delete": "#C62828",
        "copy": "#455A64",
        "properties": "#546E7A",
        "style": "#8E24AA",
        "select_radius": "#1565C0",
        "select_polygon": "#00897B",
        "select_shape": "#6A1B9A",
        "labels_show": "#2E7D32",
        "labels_hide": "#C62828",
        "open_folder": "#F9A825",
        "create": "#2E7D32",
        "manage": "#546E7A",
        "save": "#2E7D32",
        "change_project": "#1565C0",
        "project_folder": "#F9A825",
        "exit": "#C62828",
        "export_map": "#00897B",
        "export_csv": "#2E7D32",
        "export_visible": "#5E35B1",
        "report_project": "#1565C0",
        "report_dsr": "#00897B",
        "reports_folder": "#F9A825",
        "group": "#607D8B",
        "layer": "#1976D2",
        "preplot": "#0288D1",
        "receiver": "#43A047",
        "rec_db": "#AB47BC",
        "shape": "#00ACC1",
        "custom_layer": "#7B1FA2",
        "line": "#1976D2",
        "station": "#E53935",
        "loading": "#78909C",
        "qc": "#00897B",
        "chart": "#00897B",
        "activity": "#00897B",
        "bbox": "#F9A825",
        "bbox_track": "#00ACC1",
        "database": "#5E35B1",
        "view3d": "#7B1FA2",
        "bathymetry": "#00838F",
        "receivers3d": "#E53935",
        "move_top": "#546E7A",
        "move_up": "#546E7A",
        "move_down": "#546E7A",
        "move_bottom": "#546E7A",
    }

    @classmethod
    def path(cls, key: str) -> Path | None:
        relative = cls.FILES.get(key)
        if not relative:
            return None
        path = cls.ROOT / relative
        return path if path.exists() else None

    @classmethod
    @lru_cache(maxsize=512)
    def icon(cls, key: str, color: str | None = None, size: int = 32) -> QtGui.QIcon:
        path = cls.path(key)
        if path is None:
            return QtGui.QIcon()
        tint = color or cls.COLORS.get(key, "#455A64")
        if QtSvg is None:
            return QtGui.QIcon(str(path))
        try:
            svg = path.read_text(encoding="utf-8")
            svg = svg.replace("currentColor", tint)
            renderer = QtSvg.QSvgRenderer(QtCore.QByteArray(svg.encode("utf-8")))
            if not renderer.isValid():
                return QtGui.QIcon(str(path))
            image_size = max(16, int(size))
            pixmap = QtGui.QPixmap(image_size, image_size)
            pixmap.fill(QtCore.Qt.GlobalColor.transparent)
            painter = QtGui.QPainter(pixmap)
            painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing)
            renderer.render(painter)
            painter.end()
            return QtGui.QIcon(pixmap)
        except Exception:
            return QtGui.QIcon(str(path))


def icon(key: str, color: str | None = None, size: int = 32) -> QtGui.QIcon:
    return IconManager.icon(key, color, size)
