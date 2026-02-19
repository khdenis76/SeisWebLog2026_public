import numpy as np
import pandas as pd
import pyqtgraph as pg
from PySide6 import QtCore


class PlotFactory:
    @staticmethod
    def _xy_from_df(
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        max_points: int = 300_000,
    ) -> tuple[np.ndarray, np.ndarray]:
        if df is None or df.empty:
            return np.array([], dtype=float), np.array([], dtype=float)

        if x_col not in df.columns or y_col not in df.columns:
            raise ValueError(f"Missing columns: {x_col}, {y_col}")

        d = df[[x_col, y_col]].dropna(subset=[x_col, y_col]).copy()
        if d.empty:
            return np.array([], dtype=float), np.array([], dtype=float)

        x = d[x_col].astype(float).to_numpy()
        y = d[y_col].astype(float).to_numpy()

        if max_points and len(x) > max_points:
            idx = np.linspace(0, len(x) - 1, max_points).astype(int)
            x = x[idx]
            y = y[idx]

        return x, y

    @staticmethod
    def create_preplot_scatter(
        df: pd.DataFrame,
        x_col: str = "X",
        y_col: str = "Y",
        size: int = 5,
        color: str = "cyan",
        max_points: int = 300_000,
    ) -> pg.ScatterPlotItem:
        scatter = pg.ScatterPlotItem(
            size=size,
            pxMode=True,
            brush=pg.mkBrush(color),
            pen=None,
        )
        x, y = PlotFactory._xy_from_df(df, x_col, y_col, max_points=max_points)
        scatter.setData(x, y)
        return scatter
    @staticmethod
    def create_scatter_layer(
            df,
            x_col="X",
            y_col="Y",
            group_col=None,  # if provided → connect only inside this group
            order_col=None,  # sorting inside group

            plot_name="Layer",
            plot_id=None,

            point_size=5,
            point_shape="o",
            point_color="cyan",

            line_color="cyan",
            line_width=1.0,
            line_style=None,

            connect_points=True,  # enable/disable line drawing
            max_points=1_000_000,
    ):
        if df is None or df.empty:
            return {
                "scatter": None,
                "curve": None,
                "plot_name": plot_name,
                "plot_id": plot_id,
            }

        # Validate required columns
        for c in (x_col, y_col):
            if c not in df.columns:
                raise ValueError(f"Missing column: {c}")

        d = df.dropna(subset=[x_col, y_col]).copy()

        if group_col and group_col not in d.columns:
            raise ValueError(f"Missing group column: {group_col}")

        # Sorting
        if group_col and order_col and order_col in d.columns:
            d[order_col] = d[order_col].astype(float)
            d = d.sort_values([group_col, order_col])
        elif order_col and order_col in d.columns:
            d = d.sort_values(order_col)

        xs_line = []
        ys_line = []

        xs_points = []
        ys_points = []

        last_group = None

        for r in d.itertuples(index=False):
            x = float(getattr(r, x_col))
            y = float(getattr(r, y_col))

            if group_col:
                group_val = getattr(r, group_col)

                if last_group is not None and group_val != last_group:
                    xs_line.append(float("nan"))
                    ys_line.append(float("nan"))

                last_group = group_val

            xs_line.append(x)
            ys_line.append(y)

            xs_points.append(x)
            ys_points.append(y)

            if max_points and len(xs_points) >= max_points:
                break

        # ----- Line -----
        curve = None
        if connect_points:
            pen_kwargs = dict(color=line_color, width=line_width)
            if line_style is not None:
                pen_kwargs["style"] = line_style

            pen = pg.mkPen(**pen_kwargs)
            curve = pg.PlotCurveItem(xs_line, ys_line, pen=pen)

            curve.plot_name = plot_name
            curve.plot_id = plot_id

        # ----- Scatter -----
        scatter = pg.ScatterPlotItem(
            x=xs_points,
            y=ys_points,
            size=point_size,
            symbol=point_shape,
            brush=pg.mkBrush(point_color),
            pen=None,
            pxMode=True,
        )

        scatter.plot_name = plot_name
        scatter.plot_id = plot_id

        return {
            "scatter": scatter,
            "curve": curve,
            "plot_name": plot_name,
            "plot_id": plot_id,
        }


    def create_circle_layer_fast(
            df,
            x_col="X",
            y_col="Y",
            radius_col=None,  # optional: per-row radius in data units
            radius=10.0,  # default radius in data units
            line_color="yellow",
            fill_color=None,  # e.g. (255,0,0,40) or "rgba(255,0,0,40)" if you already use that
            line_width=1.0,
            max_circles=None,  # e.g. 200000 to cap
    ):
        """
        Very fast circle drawing in DATA units (meters/UTM/etc.)
        Returns a SINGLE GraphicsObject you add once:
            item = create_circle_layer_fast(...)
            plot.addItem(item)
        """

        # local Qt imports via pyqtgraph to avoid you managing imports elsewhere
        QtCore = pg.QtCore
        QtGui = pg.QtGui

        if df is None or df.empty:
            # return an empty item (safe)
            class _Empty(pg.GraphicsObject):
                def paint(self, p, *args): ...

                def boundingRect(self): return QtCore.QRectF()

            return _Empty()

        for c in (x_col, y_col):
            if c not in df.columns:
                raise ValueError(f"Missing column: {c}")

        d = df[[x_col, y_col] + ([radius_col] if radius_col and radius_col in df.columns else [])].dropna().copy()
        if d.empty:
            class _Empty(pg.GraphicsObject):
                def paint(self, p, *args): ...

                def boundingRect(self): return QtCore.QRectF()

            return _Empty()

        if max_circles and len(d) > max_circles:
            d = d.iloc[:max_circles].copy()

        # convert once
        xs = d[x_col].astype(float).to_numpy()
        ys = d[y_col].astype(float).to_numpy()
        if radius_col and radius_col in d.columns:
            rs = d[radius_col].astype(float).to_numpy()
        else:
            rs = None

        # pen/brush once
        pen = pg.mkPen(line_color, width=line_width)
        brush = pg.mkBrush(fill_color) if fill_color is not None else QtCore.Qt.BrushStyle.NoBrush

        # Build picture (cached drawing)
        picture = QtGui.QPicture()
        painter = QtGui.QPainter(picture)
        painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, False)
        painter.setPen(pen)
        if brush is not QtCore.Qt.BrushStyle.NoBrush:
            painter.setBrush(brush)

        # draw all circles
        if rs is None:
            r0 = float(radius)
            for x, y in zip(xs, ys):
                painter.drawEllipse(QtCore.QPointF(x, y), r0, r0)
        else:
            for x, y, r in zip(xs, ys, rs):
                rr = float(r)
                painter.drawEllipse(QtCore.QPointF(x, y), rr, rr)

        painter.end()

        # One GraphicsObject that replays the picture quickly
        class CirclePictureItem(pg.GraphicsObject):
            def __init__(self, pic, bounds):
                super().__init__()
                self._pic = pic
                self._bounds = bounds

            def paint(self, p, *args):
                p.drawPicture(0, 0, self._pic)

            def boundingRect(self):
                return self._bounds

        # Bounding rect (important for correct view auto-range)
        if rs is None:
            rmax = float(radius)
        else:
            rmax = float(np.nanmax(rs)) if len(rs) else float(radius)

        xmin = float(np.nanmin(xs) - rmax)
        xmax = float(np.nanmax(xs) + rmax)
        ymin = float(np.nanmin(ys) - rmax)
        ymax = float(np.nanmax(ys) + rmax)
        bounds = QtCore.QRectF(xmin, ymin, xmax - xmin, ymax - ymin)

        item = CirclePictureItem(picture, bounds)
        # optional metadata
        item.plot_name = "CircleLayer"
        return item
    @staticmethod
    def build_two_series_vs_station(
            w,
            df,
            station_col,
            series1_col,
            series2_col,
            y_label="Value",
            title=None,
            series1_name=None,
            series2_name=None,
    ):
        if title:
            w.setWindowTitle(title)

        w.plot.setLabel("bottom", "Station")
        w.plot.setLabel("left", y_label)

        if series1_name is None:
            series1_name = series1_col
        if series2_name is None:
            series2_name = series2_col

        # Create plot items only once
        if "curve1" not in w.items:
            w.items["curve1"] = pg.PlotDataItem(pen=pg.mkPen(width=2))
            w.items["curve2"] = pg.PlotDataItem(
                pen=pg.mkPen(width=2, style=QtCore.Qt.PenStyle.DashLine)
            )

            w.items["scatter1"] = pg.ScatterPlotItem(size=7, pxMode=True, pen=None)
            w.items["scatter2"] = pg.ScatterPlotItem(size=7, pxMode=True, pen=None, symbol="t")

            w.items["sel_line"] = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen("red", width=2))
            w.items["sel_line"].setVisible(False)

            w.plot.addItem(w.items["curve1"])
            w.plot.addItem(w.items["curve2"])
            w.plot.addItem(w.items["scatter1"])
            w.plot.addItem(w.items["scatter2"])
            w.plot.addItem(w.items["sel_line"])

            def on_clicked(scatter, points):
                if not points:
                    return
                st = points[0].data()
                if st is None:
                    return
                try:
                    st = int(st)
                except Exception:
                    return

                w.items["sel_line"].setPos(float(st))
                w.items["sel_line"].setVisible(True)
                w.state["selected_station"] = st

                cb = w.state.get("on_station_selected")
                if callable(cb):
                    cb(st)

            w.items["scatter1"].sigClicked.connect(on_clicked)
            w.items["scatter2"].sigClicked.connect(on_clicked)

        # Update data
        if df is None or df.empty:
            w.items["curve1"].setData([], [])
            w.items["curve2"].setData([], [])
            w.items["scatter1"].setData([])
            w.items["scatter2"].setData([])
            w.items["sel_line"].setVisible(False)
            return

        for c in (station_col, series1_col, series2_col):
            if c not in df.columns:
                raise ValueError(f"Missing column '{c}'")

        d = df[[station_col, series1_col, series2_col]].copy()
        d[station_col] = pd.to_numeric(d[station_col], errors="coerce")
        d[series1_col] = pd.to_numeric(d[series1_col], errors="coerce")
        d[series2_col] = pd.to_numeric(d[series2_col], errors="coerce")
        d = d.dropna(subset=[station_col]).sort_values(station_col)

        x = d[station_col].to_numpy(dtype=float)
        y1 = d[series1_col].to_numpy(dtype=float)
        y2 = d[series2_col].to_numpy(dtype=float)

        w.items["curve1"].setData(x, y1)
        w.items["curve2"].setData(x, y2)

        spots1 = [{"pos": (float(xx), float(yy)), "data": int(xx)} for xx, yy in zip(x, y1) if pd.notna(yy)]
        spots2 = [{"pos": (float(xx), float(yy)), "data": int(xx)} for xx, yy in zip(x, y2) if pd.notna(yy)]

        w.items["scatter1"].setData(spots1)
        w.items["scatter2"].setData(spots2)

        w.plot.enableAutoRange()

    @staticmethod
    def set_depth_window_selected_station(w, station: int):
        if not w or not hasattr(w, "items"):
            return
        if "sel_line" not in w.items:
            return
        try:
            st = float(station)
        except Exception:
            return
        w.items["sel_line"].setPos(st)
        w.items["sel_line"].setVisible(True)
        w.state["selected_station"] = int(station)

