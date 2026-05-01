from __future__ import annotations

from PySide6.QtCore import Signal, Qt
from PySide6.QtWidgets import QCheckBox, QMainWindow, QToolBar
import pyqtgraph as pg


class StationMapWindow(QMainWindow):
    stationClicked = Signal(str, str)

    def __init__(self, preplot_rows, dsr_rows, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Station Map - all database")
        self.resize(1200, 850)
        self.preplot_rows = preplot_rows or []
        self.dsr_rows = dsr_rows or []
        self.labels = []
        self.scatter = None
        self.highlight_item = None
        self._point_by_key = {}

        self.plot = pg.PlotWidget()
        self.setCentralWidget(self.plot)
        self.plot.setBackground((30, 30, 30))
        self.plot.showGrid(x=True, y=True, alpha=0.25)
        self.plot.setAspectLocked(False)

        tb = QToolBar("Map")
        self.addToolBar(Qt.TopToolBarArea, tb)
        self.show_preplot_chk = QCheckBox("Preplot")
        self.show_preplot_chk.setChecked(True)
        self.show_preplot_chk.stateChanged.connect(self.redraw)
        self.show_labels_chk = QCheckBox("Image count labels")
        self.show_labels_chk.setChecked(True)
        self.show_labels_chk.stateChanged.connect(self._set_label_visible)
        tb.addWidget(self.show_preplot_chk)
        tb.addSeparator()
        tb.addWidget(self.show_labels_chk)

        self.statusBar().showMessage("Ready")
        self.redraw()
        self.plot.scene().sigMouseMoved.connect(self._mouse_moved)

    def redraw(self):
        self.plot.clear()
        self.labels.clear()
        self.highlight_item = None
        self._point_by_key.clear()
        if self.show_preplot_chk.isChecked():
            self._draw_preplot(self.preplot_rows)
        self._draw_dsr(self.dsr_rows)

    def _draw_preplot(self, rows):
        x, y = [], []
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
            brush=pg.mkBrush(100, 140, 255, 90),
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
            images = self._to_int(r.get("images") or r.get("station_image_count") or 0)
            status = str(r.get("station_status") or r.get("status") or "")
            brush, size = self._color_for_station(images, status)
            data = {
                "line": str(r.get("dsr_line", "")),
                "station": str(r.get("dsr_station", "")),
                "dsr_rov": str(r.get("dsr_rov", "")),
                "dsr_rov1": str(r.get("dsr_rov1", "")),
                "images": images,
                "station_image_count": images,
                "status": str(r.get("status", "")),
                "station_status": str(r.get("station_status", "")),
                "message": str(r.get("message", "")),
                "deploy": str(r.get("dsr_timestamp", "")),
                "recover": str(r.get("dsr_timestamp1", "")),
            }
            spots.append({"pos": (x, y), "size": size, "brush": brush, "pen": pg.mkPen("white"), "symbol": "o", "data": data})
            key = (str(data.get("line", "")).strip(), str(data.get("station", "")).strip())
            if key != ("", ""):
                self._point_by_key[key] = (x, y, data)

            label = pg.TextItem(
                str(images),
                color=(255, 255, 255),
                anchor=(0.5, 1.4),
                border=pg.mkPen(30, 30, 30, 180),
                fill=pg.mkBrush(30, 30, 30, 160),
            )
            label.setPos(x, y)
            label.setVisible(self.show_labels_chk.isChecked())
            self.plot.addItem(label)
            self.labels.append(label)

        self.scatter = pg.ScatterPlotItem(pxMode=True)
        if spots:
            self.scatter.addPoints(spots)
        self.scatter.sigClicked.connect(self._click_station)
        self.plot.addItem(self.scatter)

    def highlight_station(self, line: str, station: str, center: bool = True):
        key = (str(line or "").strip(), str(station or "").strip())
        point = self._point_by_key.get(key)

        if not point:
            def norm(v):
                try:
                    return str(int(float(str(v).strip())))
                except Exception:
                    return str(v).strip()
            nkey = (norm(key[0]), norm(key[1]))
            for k, v in self._point_by_key.items():
                if (norm(k[0]), norm(k[1])) == nkey:
                    point = v
                    break

        if not point:
            self.statusBar().showMessage(f"Station not found on map: line {line}, station {station}")
            return

        x, y, data = point
        if self.highlight_item is not None:
            try:
                self.plot.removeItem(self.highlight_item)
            except Exception:
                pass
            self.highlight_item = None

        self.highlight_item = pg.ScatterPlotItem(
            x=[x],
            y=[y],
            size=30,
            pxMode=True,
            symbol="o",
            brush=pg.mkBrush(255, 255, 0, 70),
            pen=pg.mkPen(255, 255, 0, width=3),
        )
        self.plot.addItem(self.highlight_item)

        if center:
            vb = self.plot.plotItem.vb
            xr, yr = vb.viewRange()
            half_x = max((xr[1] - xr[0]) / 2.0, 10.0)
            half_y = max((yr[1] - yr[0]) / 2.0, 10.0)
            vb.setRange(xRange=(x - half_x, x + half_x), yRange=(y - half_y, y + half_y), padding=0)

        self.statusBar().showMessage(
            f"Highlighted line {data.get('line', '')}, station {data.get('station', '')}, "
            f"images {data.get('images', '')}, ROV {data.get('dsr_rov', '')}, ROV1 {data.get('dsr_rov1', '')}"
        )

    def _set_label_visible(self):
        visible = self.show_labels_chk.isChecked()
        for label in self.labels:
            label.setVisible(visible)

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

    def _click_station(self, plot, points, ev):
        if not points:
            return
        d = points[0].data() or {}
        line = str(d.get("line", "")).strip()
        station = str(d.get("station", "")).strip()
        if line and station:
            self.highlight_station(line, station, center=False)
            self.stationClicked.emit(line, station)

    def _mouse_moved(self, pos):
        mouse_point = self.plot.plotItem.vb.mapSceneToView(pos)
        x = mouse_point.x()
        y = mouse_point.y()
        nearest_point = None
        nearest_dist = 1e20
        if self.scatter is not None:
            for p in self.scatter.points():
                pt = p.pos()
                dx = pt.x() - x
                dy = pt.y() - y
                dist = dx * dx + dy * dy
                if dist < nearest_dist:
                    nearest_dist = dist
                    nearest_point = p
        msg = ""
        if nearest_point and nearest_dist < 200:
            d = nearest_point.data() or {}
            msg = (
                f"Line {d.get('line', '')}   Station {d.get('station', '')}   "
                f"ROV {d.get('dsr_rov', '')}   ROV1 {d.get('dsr_rov1', '')}   "
                f"Images {d.get('images', '')}   Deploy {d.get('deploy', '')}   "
                f"Recover {d.get('recover', '')}   Status {d.get('status', '')}   "
                f"Station Status {d.get('station_status', '')}"
            )
            if d.get("message"):
                msg += f"   {d.get('message')}"
        self.statusBar().showMessage(f"{msg}    |    X {x:.2f}   Y {y:.2f}")

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
            return int(float(v))
        except Exception:
            return 0
