from PySide6.QtCore import Signal
from PySide6.QtWidgets import QMainWindow
import pyqtgraph as pg


class StationMapWindow(QMainWindow):
    stationClicked = Signal(str, str)

    def __init__(self, preplot_rows, dsr_rows, parent=None):
        super().__init__(parent)

        self.setWindowTitle("Station Map")
        self.resize(1200, 850)

        self.plot = pg.PlotWidget()
        self.setCentralWidget(self.plot)

        self.plot.setBackground((30, 30, 30))
        self.plot.showGrid(x=True, y=True, alpha=0.25)

        self._last_hover_msg = ""
        self._last_mouse_xy = ""
        self.statusBar().showMessage("Ready")

        self._draw_preplot(preplot_rows)
        self._draw_dsr(dsr_rows)
        self.plot.scene().sigMouseMoved.connect(self._mouse_moved)



    def _draw_preplot(self, rows):
        x = []
        y = []

        for r in rows:
            xv = self._to_float(r.get("x"))
            yv = self._to_float(r.get("y"))
            if xv is None or yv is None:
                continue
            x.append(xv)
            y.append(yv)

        if not x:
            return

        scatter = pg.ScatterPlotItem(
            x=x,
            y=y,
            size=5,
            brush=pg.mkBrush(100, 140, 255, 120),
            pen=pg.mkPen(120, 120, 120),
            name="Preplot",
            pxMode=True,
        )
        self.plot.addItem(scatter)

    def _draw_dsr(self, rows):
        spots = []

        for r in rows:
            x = self._to_float(r.get("dsr_x"))
            y = self._to_float(r.get("dsr_y"))
            if x is None or y is None:
                continue

            images = self._to_int(r.get("images", 0))
            status = str(r.get("status", "") or "")
            error = str(r.get("error", "") or "")
            brush, size = self._color_for_station(images, status)

            spots.append({
                "pos": (x, y),
                "size": size,
                "brush": brush,
                "pen": pg.mkPen("white"),
                "symbol": "o",
                "data": {
                    "line": str(r.get("dsr_line", "")),
                    "station": str(r.get("dsr_station", "")),

                    "dsr_rov": str(r.get("dsr_rov", "")),
                    "dsr_rov1": str(r.get("dsr_rov1", "")),

                    "images": int(r.get("station_image_count", 0)),

                    "status": str(r.get("status", "")),
                    "error": str(r.get("error", "")),

                    "deploy": str(r.get("dsr_timestamp", "")),
                    "recover": str(r.get("dsr_timestamp1", "")),
                }
            })

        self.scatter = pg.ScatterPlotItem(pxMode=True)
        self.scatter.addPoints(spots)
        self.scatter.sigClicked.connect(self._click_station)
        #self.scatter.sigHovered.connect(self._hover_station)
        self.plot.scene().sigMouseMoved.connect(self._mouse_moved)
        self.plot.addItem(self.scatter)

    def _color_for_station(self, images, status):
        status = str(status or "").upper()

        if "ERROR" in status or "NO_DSR" in status or "BAD_" in status:
            return pg.mkBrush(220, 50, 50), 14
        if "INCOMPLETE" in status or "WARNING" in status:
            return pg.mkBrush(255, 170, 50), 13
        if images == 0:
            return pg.mkBrush(180, 180, 180), 10
        if images == 1:
            return pg.mkBrush(70, 140, 255), 11
        if images == 2:
            return pg.mkBrush(70, 200, 120), 12
        return pg.mkBrush(20, 210, 160), 13

    def _hover_station(self, plot, points, ev):
        if points is None or len(points) == 0:
            self._last_hover_msg = ""
            self._update_status_bar()
            return

        p = points[0]
        d = p.data() or {}

        msg = (
            f"Line: {d.get('line', '')}   "
            f"Station: {d.get('station', '')}   "
            f"ROV: {d.get('rov', '')}   "
            f"ROV1: {d.get('rov1', '')}   "
            f"Files: {d.get('station_image_count', 0)}   "
            f"Status: {d.get('status', '')}"
        )

        if d.get("error"):
            msg += f"   Error: {d.get('error', '')}"

        if d.get("deployment_date"):
            msg += f"   Deploy: {d.get('deployment_date', '')}"

        if d.get("recovery_date"):
            msg += f"   Recover: {d.get('recovery_date', '')}"

        self._last_hover_msg = msg
        self._update_status_bar()

    def _click_station(self, plot, points, ev):
        if points is None or len(points) == 0:
            return

        d = points[0].data() or {}
        line = str(d.get("line", "")).strip()
        station = str(d.get("station", "")).strip()

        if line and station:
            self.stationClicked.emit(line, station)

    def _mouse_moved(self, pos):

        mouse_point = self.plot.plotItem.vb.mapSceneToView(pos)

        x = mouse_point.x()
        y = mouse_point.y()

        nearest_point = None
        nearest_dist = 1e20

        for p in self.scatter.points():

            pt = p.pos()

            dx = pt.x() - x
            dy = pt.y() - y
            dist = dx * dx + dy * dy

            if dist < nearest_dist:
                nearest_dist = dist
                nearest_point = p

        msg = ""

        # increase threshold for UTM coordinates
        if nearest_point and nearest_dist < 200:

            d = nearest_point.data() or {}

            msg = (
                f"Line {d.get('line', '')}   "
                f"Station {d.get('station', '')}   "
                f"ROV {d.get('dsr_rov', '')}   "
                f"ROV1 {d.get('dsr_rov1', '')}   "
                f"Images {d.get('station_image_count', '')}   "
                f"Deploy {d.get('deploy', '')}   "
                f"Recover {d.get('recover', '')}   "
                f"Status {d.get('status', '')} "
                f"Station Status {d.get('station_status', '')} "
            )

            if d.get("error"):
                msg += f"   ERROR: {d.get('error')}"

            if d.get("error"):
                msg += f"   ERROR: {d.get('error')}"

        self.statusBar().showMessage(
            f"{msg}    |    X {x:.2f}   Y {y:.2f}"
        )

    def _update_status_bar(self):
        if self._last_hover_msg and self._last_mouse_xy:
            self.statusBar().showMessage(f"{self._last_hover_msg}   |   {self._last_mouse_xy}")
        elif self._last_hover_msg:
            self.statusBar().showMessage(self._last_hover_msg)
        elif self._last_mouse_xy:
            self.statusBar().showMessage(self._last_mouse_xy)
        else:
            self.statusBar().showMessage("Ready")

    @staticmethod
    def _to_float(v):
        try:
            if v in ("", None):
                return None
            return float(v)
        except Exception:
            return None

    @staticmethod
    def _to_int(v):
        try:
            if v in ("", None):
                return 0
            return int(v)
        except Exception:
            try:
                return int(float(v))
            except Exception:
                return 0