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

            meta_cols=None,
            meta_mode="tuple",  # "tuple" | "dict" | "index"
    ):
        meta_cols = meta_cols or []
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

        # Keep only meta columns that exist
        meta_cols = [c for c in meta_cols if c in d.columns]

        # Sorting
        if group_col and order_col and order_col in d.columns:
            d[order_col] = d[order_col].astype(float)
            d = d.sort_values([group_col, order_col])
        elif order_col and order_col in d.columns:
            d = d.sort_values(order_col)

        xs_line, ys_line = [], []
        spots = []  # scatter points with metadata

        last_group = None
        n = 0

        # Use itertuples for speed
        for r in d.itertuples(index=True, name="Row"):
            x = float(getattr(r, x_col))
            y = float(getattr(r, y_col))

            # group breaks for line
            if group_col:
                group_val = getattr(r, group_col)
                if last_group is not None and group_val != last_group:
                    xs_line.append(float("nan"))
                    ys_line.append(float("nan"))
                last_group = group_val

            xs_line.append(x)
            ys_line.append(y)

            # Build metadata payload
            if meta_mode == "index":
                payload = int(r.Index)  # original dataframe index
            elif meta_mode == "dict":
                payload = {c: getattr(r, c) for c in meta_cols}
            else:
                # tuple mode: (colnames stored once on scatter)
                payload = tuple(getattr(r, c) for c in meta_cols)

            spots.append({
                "pos": (x, y),
                "data": payload,
            })

            n += 1
            if max_points and n >= max_points:
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
            size=point_size,
            symbol=point_shape,
            brush=pg.mkBrush(point_color),
            pen=None,
            pxMode=True,
            name=plot_name,
        )
        scatter.addPoints(spots)

        # store meta schema once (useful for tuple mode)
        scatter.meta_cols = meta_cols
        scatter.meta_mode = meta_mode

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
            radius_col=None,
            radius=10.0,
            line_color="yellow",
            fill_color=None,
            line_width=1.0,
            max_circles=None,
            name=None,
    ):
        QtCore = pg.QtCore
        QtGui = pg.QtGui

        class _Empty(pg.GraphicsObject):
            def paint(self, p, *args):
                return

            def boundingRect(self):
                return QtCore.QRectF()

        if df is None or df.empty:
            return _Empty()

        for c in (x_col, y_col):
            if c not in df.columns:
                raise ValueError(f"Missing column: {c}")

        cols = [x_col, y_col]
        if radius_col and radius_col in df.columns:
            cols.append(radius_col)

        d = df[cols].dropna()
        if d.empty:
            return _Empty()

        if max_circles and len(d) > max_circles:
            d = d.iloc[:max_circles]

        xs = d[x_col].astype(float).to_numpy()
        ys = d[y_col].astype(float).to_numpy()

        rs = None
        if radius_col and radius_col in d.columns:
            rs = d[radius_col].astype(float).to_numpy()

        # filter finite values ONCE (and keep arrays aligned)
        mask = np.isfinite(xs) & np.isfinite(ys)
        if rs is not None:
            mask &= np.isfinite(rs)

        xs = xs[mask]
        ys = ys[mask]
        if rs is not None:
            rs = rs[mask]

        if xs.size == 0:
            return _Empty()

        pen = pg.mkPen(line_color, width=line_width)

        # ALWAYS provide a real QBrush (avoid native crashes)
        if fill_color is None:
            brush = QtGui.QBrush(QtCore.Qt.NoBrush)
        else:
            brush = pg.mkBrush(fill_color)

        picture = QtGui.QPicture()
        painter = QtGui.QPainter(picture)
        painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, False)
        painter.setPen(pen)
        painter.setBrush(brush)

        if rs is None:
            r0 = float(radius)
            for x, y in zip(xs, ys):
                painter.drawEllipse(QtCore.QPointF(x, y), r0, r0)
            rmax = r0
        else:
            # draw per-row radius
            for x, y, r in zip(xs, ys, rs):
                rr = float(r)
                painter.drawEllipse(QtCore.QPointF(x, y), rr, rr)
            rmax = float(np.nanmax(rs)) if rs.size else float(radius)

        painter.end()

        # bounds (must be finite)
        if not np.isfinite(rmax) or rmax < 0:
            rmax = float(radius)

        xmin = float(np.nanmin(xs) - rmax)
        xmax = float(np.nanmax(xs) + rmax)
        ymin = float(np.nanmin(ys) - rmax)
        ymax = float(np.nanmax(ys) + rmax)

        w = xmax - xmin
        h = ymax - ymin
        if not (np.isfinite(w) and np.isfinite(h)) or w <= 0 or h <= 0:
            bounds = QtCore.QRectF()
        else:
            bounds = QtCore.QRectF(xmin, ymin, w, h)

        class CirclePictureItem(pg.GraphicsObject):
            def __init__(self, pic, bounds, nm):
                super().__init__()
                self._pic = pic
                self._bounds = bounds
                self._name = nm

            def paint(self, p, *args):
                p.drawPicture(0, 0, self._pic)

            def boundingRect(self):
                return self._bounds

            def name(self):
                return self._name

        return CirclePictureItem(picture, bounds, name or "CircleLayer")

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

            # --- colors ---
            red_pen = pg.mkPen((220, 50, 50), width=2)
            blue_pen = pg.mkPen((50, 120, 220), width=2, style=QtCore.Qt.PenStyle.DashLine)

            red_brush = pg.mkBrush(220, 50, 50)
            blue_brush = pg.mkBrush(50, 120, 220)

            # Real plotted curves (no symbols here)
            w.items["curve1"] = pg.PlotDataItem(pen=red_pen)
            w.items["curve2"] = pg.PlotDataItem(pen=blue_pen)

            # Real clickable scatters
            w.items["scatter1"] = pg.ScatterPlotItem(size=7, pxMode=True, pen=None, brush=red_brush, symbol="o")
            w.items["scatter2"] = pg.ScatterPlotItem(size=7, pxMode=True, pen=None, brush=blue_brush, symbol="t")

            w.items["sel_line"] = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen("red", width=2))
            w.items["sel_line"].setVisible(False)
            w.items["sel_line"].setZValue(10_000)

            # --- legend (once) ---
            if w.plot.plotItem.legend is None:
                w.plot.addLegend(offset=(10, 10))

            # Legend-only dummy items to show "line + marker" combined
            # (do NOT add these to the plot)
            w.items["legend1"] = pg.PlotDataItem(
                pen=red_pen,
                symbol="o",
                symbolSize=9,
                symbolBrush=red_brush,
                symbolPen=None,
            )
            w.items["legend2"] = pg.PlotDataItem(
                pen=blue_pen,
                symbol="t",
                symbolSize=9,
                symbolBrush=blue_brush,
                symbolPen=None,
            )

            # Add real items to plot
            w.plot.addItem(w.items["curve1"])
            w.plot.addItem(w.items["curve2"])
            w.plot.addItem(w.items["scatter1"])
            w.plot.addItem(w.items["scatter2"])
            w.plot.addItem(w.items["sel_line"])

            # Fill legend with combined samples
            try:
                leg = w.plot.plotItem.legend
                leg.clear()
                leg.addItem(w.items["legend1"], series1_name)
                leg.addItem(w.items["legend2"], series2_name)
            except Exception:
                pass

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

        else:
            # Update legend labels if names change between calls
            try:
                leg = w.plot.plotItem.legend
                if leg is not None and "legend1" in w.items and "legend2" in w.items:
                    leg.clear()
                    leg.addItem(w.items["legend1"], series1_name)
                    leg.addItem(w.items["legend2"], series2_name)
            except Exception:
                pass

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

        # --- restore selected station line if we have it ---
        st = w.state.get("selected_station")
        if st is not None:
            try:
                stf = float(st)
                if len(x) and (x.min() <= stf <= x.max()):
                    w.items["sel_line"].setPos(stf)
                    w.items["sel_line"].setVisible(True)
                else:
                    w.items["sel_line"].setVisible(False)
            except Exception:
                w.items["sel_line"].setVisible(False)

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

