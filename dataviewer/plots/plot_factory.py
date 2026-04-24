import numpy as np
import pandas as pd
import pyqtgraph as pg
from PySide6 import QtCore
from ..ui.plot_window import ClickableLegendItem


class PlotFactory:
    @staticmethod
    def _empty_layer(plot_name="Layer", plot_id=None):
        return {
            "scatter": None,
            "curve": None,
            "plot_name": plot_name,
            "plot_id": plot_id,
            "stats": {
                "total_rows": 0,
                "line_points": 0,
                "scatter_points": 0,
                "interactive_points": 0,
            },
        }

    @staticmethod
    def _downsample_indices(n: int, max_points: int | None) -> np.ndarray:
        if n <= 0:
            return np.array([], dtype=np.int64)
        if not max_points or n <= max_points:
            return np.arange(n, dtype=np.int64)
        return np.unique(np.rint(np.linspace(0, n - 1, max_points)).astype(np.int64))

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

        d = df[[x_col, y_col]].copy()
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
        d = d.dropna(subset=[x_col, y_col])
        if d.empty:
            return np.array([], dtype=float), np.array([], dtype=float)

        idx = PlotFactory._downsample_indices(len(d), max_points)
        d = d.iloc[idx]
        return (
            d[x_col].to_numpy(dtype=float, copy=False),
            d[y_col].to_numpy(dtype=float, copy=False),
        )

    @staticmethod
    def _prepare_xy_df(
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        group_col: str | None = None,
        order_col: str | None = None,
        meta_cols: list[str] | None = None,
    ) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        if x_col not in df.columns or y_col not in df.columns:
            raise ValueError(f"Missing columns: {x_col}, {y_col}")

        meta_cols = meta_cols or []
        cols = [x_col, y_col]
        if group_col and group_col in df.columns:
            cols.append(group_col)
        if order_col and order_col in df.columns and order_col not in cols:
            cols.append(order_col)
        cols.extend([c for c in meta_cols if c in df.columns and c not in cols])

        d = df.loc[:, cols].copy()
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        d[y_col] = pd.to_numeric(d[y_col], errors="coerce")

        if order_col and order_col in d.columns:
            d[order_col] = pd.to_numeric(d[order_col], errors="coerce")

        d = d.dropna(subset=[x_col, y_col])
        if d.empty:
            return d

        if group_col and group_col in d.columns and order_col and order_col in d.columns:
            d = d.sort_values([group_col, order_col], kind="mergesort")
        elif order_col and order_col in d.columns:
            d = d.sort_values(order_col, kind="mergesort")

        return d.reset_index(drop=False).rename(columns={"index": "__src_index__"})

    @staticmethod
    def _build_line_arrays(
        d: pd.DataFrame,
        x_col: str,
        y_col: str,
        group_col: str | None = None,
        max_points: int | None = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        if d.empty:
            return np.array([], dtype=float), np.array([], dtype=float)

        idx = PlotFactory._downsample_indices(len(d), max_points)
        dd = d.iloc[idx]

        x = dd[x_col].to_numpy(dtype=float, copy=False)
        y = dd[y_col].to_numpy(dtype=float, copy=False)

        if group_col and group_col in dd.columns:
            g = dd[group_col].to_numpy()
            if len(g) > 1:
                breaks = np.flatnonzero(g[1:] != g[:-1]) + 1
                if len(breaks):
                    x = np.insert(x, breaks, np.nan)
                    y = np.insert(y, breaks, np.nan)

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
        scatter.setData(x=x, y=y)
        return scatter

    @staticmethod
    def create_scatter_layer(
        df,
        x_col="X",
        y_col="Y",
        group_col=None,
        order_col=None,
        plot_name="Layer",
        plot_id=None,
        point_size=5,
        point_shape="o",
        point_color="cyan",
        line_color="cyan",
        line_width=1.0,
        line_style=None,
        connect_points=True,
        line_max_points=1_000_000,
        scatter_max_points=120_000,
        interactive_max_points=40_000,
        max_points=None,
        meta_cols=None,
        meta_mode="tuple",
        interactive=True,
    ):
        meta_cols = meta_cols or []
        if max_points is not None:
            line_max_points = max_points
            scatter_max_points = min(scatter_max_points, max_points)
            interactive_max_points = min(interactive_max_points, scatter_max_points)

        d = PlotFactory._prepare_xy_df(
            df=df,
            x_col=x_col,
            y_col=y_col,
            group_col=group_col,
            order_col=order_col,
            meta_cols=meta_cols,
        )
        if d.empty:
            return PlotFactory._empty_layer(plot_name=plot_name, plot_id=plot_id)

        curve = None
        line_points = 0
        if connect_points:
            x_line, y_line = PlotFactory._build_line_arrays(
                d=d,
                x_col=x_col,
                y_col=y_col,
                group_col=group_col,
                max_points=line_max_points,
            )
            line_points = len(x_line)

            pen_kwargs = dict(color=line_color, width=line_width)
            if line_style is not None:
                pen_kwargs["style"] = line_style

            curve = pg.PlotCurveItem(
                x=x_line,
                y=y_line,
                pen=pg.mkPen(**pen_kwargs),
                connect="finite",
            )
            curve.plot_name = plot_name
            curve.plot_id = plot_id
            try:
                curve.setSkipFiniteCheck(True)
            except Exception:
                pass

        s_idx = PlotFactory._downsample_indices(len(d), scatter_max_points)
        ds = d.iloc[s_idx]

        sx = ds[x_col].to_numpy(dtype=float, copy=False)
        sy = ds[y_col].to_numpy(dtype=float, copy=False)

        scatter = pg.ScatterPlotItem(
            size=point_size,
            symbol=point_shape,
            brush=pg.mkBrush(point_color),
            pen=None,
            pxMode=True,
            name=plot_name,
        )

        use_interactive_data = (
            interactive
            and len(ds) <= interactive_max_points
            and (
                meta_mode == "index"
                or (meta_mode in {"dict", "tuple"} and len(meta_cols) > 0)
            )
        )
        interactive_points = 0

        if use_interactive_data:
            payload = np.empty(len(ds), dtype=object)
            if meta_mode == "index":
                payload[:] = ds["__src_index__"].to_numpy(dtype=np.int64, copy=False)
            elif meta_mode == "dict":
                cols = [c for c in meta_cols if c in ds.columns]
                for i, row in enumerate(ds[cols].itertuples(index=False, name=None)):
                    payload[i] = dict(zip(cols, row))
            else:
                cols = [c for c in meta_cols if c in ds.columns]
                for i, row in enumerate(ds[cols].itertuples(index=False, name=None)):
                    payload[i] = row

            scatter.setData(x=sx, y=sy, data=payload)
            interactive_points = len(ds)
        else:
            scatter.setData(x=sx, y=sy)

        scatter.meta_cols = [c for c in meta_cols if c in d.columns]
        scatter.meta_mode = meta_mode
        scatter.plot_name = plot_name
        scatter.plot_id = plot_id

        return {
            "scatter": scatter,
            "curve": curve,
            "plot_name": plot_name,
            "plot_id": plot_id,
            "stats": {
                "total_rows": int(len(d)),
                "line_points": int(line_points),
                "scatter_points": int(len(ds)),
                "interactive_points": int(interactive_points),
            },
        }

    @staticmethod
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

        d = df.loc[:, cols].copy()
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
        if radius_col and radius_col in d.columns:
            d[radius_col] = pd.to_numeric(d[radius_col], errors="coerce")
        d = d.dropna(subset=[x_col, y_col])
        if d.empty:
            return _Empty()

        if max_circles and len(d) > max_circles:
            d = d.iloc[PlotFactory._downsample_indices(len(d), max_circles)]

        xs = d[x_col].to_numpy(dtype=float, copy=False)
        ys = d[y_col].to_numpy(dtype=float, copy=False)
        rs = None
        if radius_col and radius_col in d.columns:
            rs = d[radius_col].to_numpy(dtype=float, copy=False)

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
        brush = QtGui.QBrush(QtCore.Qt.NoBrush) if fill_color is None else pg.mkBrush(fill_color)

        picture = QtGui.QPicture()
        painter = QtGui.QPainter(picture)
        painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, False)
        painter.setPen(pen)
        painter.setBrush(brush)

        if rs is None:
            rr = float(radius)
            for x, y in zip(xs, ys):
                painter.drawEllipse(QtCore.QPointF(x, y), rr, rr)
            rmax = rr
        else:
            for x, y, r in zip(xs, ys, rs):
                rr = float(r)
                painter.drawEllipse(QtCore.QPointF(x, y), rr, rr)
            rmax = float(np.nanmax(rs)) if rs.size else float(radius)

        painter.end()

        if not np.isfinite(rmax) or rmax < 0:
            rmax = float(radius)

        xmin = float(np.nanmin(xs) - rmax)
        xmax = float(np.nanmax(xs) + rmax)
        ymin = float(np.nanmin(ys) - rmax)
        ymax = float(np.nanmax(ys) + rmax)

        w = xmax - xmin
        h = ymax - ymin
        bounds = QtCore.QRectF() if (not np.isfinite(w) or not np.isfinite(h) or w <= 0 or h <= 0) else QtCore.QRectF(xmin, ymin, w, h)

        class CirclePictureItem(pg.GraphicsObject):
            def __init__(self, pic, rect, nm):
                super().__init__()
                self._pic = pic
                self._rect = rect
                self._name = nm

            def paint(self, p, *args):
                p.drawPicture(0, 0, self._pic)

            def boundingRect(self):
                return self._rect

            def name(self):
                return self._name

        return CirclePictureItem(picture, bounds, name or "CircleLayer")

    @staticmethod
    def build_two_series_vs_station(
            window,
            df,
            station_col="Station",
            series1_col="PrimaryElevation",
            series2_col="SecondaryElevation",
            y_label="Value",
            title="Plot",
            time_col=None,
    ):
        import numpy as np
        import pandas as pd
        import pyqtgraph as pg
        from PySide6 import QtCore

        if df is None or df.empty:
            return

        d = df.copy()

        for c in (station_col, series1_col, series2_col):
            if c in d.columns:
                d[c] = pd.to_numeric(d[c], errors="coerce")

        if time_col and time_col in d.columns:
            d[time_col] = pd.to_datetime(d[time_col], errors="coerce")

        d = d.dropna(subset=[station_col]).sort_values(station_col)
        if d.empty:
            return

        x = d[station_col].to_numpy(dtype=float)

        y1 = d[series1_col].to_numpy(dtype=float) if series1_col in d.columns else None
        y2 = d[series2_col].to_numpy(dtype=float) if series2_col in d.columns else None

        plot = window.plot
        plot.clear()
        plot.showGrid(x=True, y=True, alpha=0.25)
        plot.setTitle(title)
        plot.setLabel("bottom", station_col)
        plot.setLabel("left", y_label)

        if not hasattr(window, "items") or window.items is None:
            window.items = {}
        if not hasattr(window, "state") or window.state is None:
            window.state = {}

        if hasattr(window, "clear_legend"):
            try:
                window.clear_legend()
            except Exception:
                pass

        c1 = None
        c2 = None

        if y1 is not None:
            c1 = pg.PlotCurveItem(
                x=x,
                y=y1,
                pen=pg.mkPen("yellow", width=2),
                name=series1_col,
            )
            plot.addItem(c1)

        if y2 is not None:
            c2 = pg.PlotCurveItem(
                x=x,
                y=y2,
                pen=pg.mkPen("cyan", width=2),
                name=series2_col,
            )
            plot.addItem(c2)

        # simple legend below plot if PlotWindow supports it
        if hasattr(window, "legend_layout"):
            try:
                window.legend_layout.addStretch(1)

                if c1 is not None:
                    w1 = QtCore.QObject()  # placeholder to keep closure simple
                    entry1 = pg.QtWidgets.QWidget()
                    row1 = pg.QtWidgets.QHBoxLayout(entry1)
                    row1.setContentsMargins(0, 0, 0, 0)
                    row1.setSpacing(6)

                    sw1 = pg.QtWidgets.QLabel()
                    sw1.setFixedSize(18, 4)
                    sw1.setStyleSheet("background:yellow; border:1px solid rgba(255,255,255,0.25);")
                    lb1 = pg.QtWidgets.QLabel(str(series1_col))

                    row1.addWidget(sw1)
                    row1.addWidget(lb1)
                    window.legend_layout.addWidget(entry1)

                if c2 is not None:
                    entry2 = pg.QtWidgets.QWidget()
                    row2 = pg.QtWidgets.QHBoxLayout(entry2)
                    row2.setContentsMargins(0, 0, 0, 0)
                    row2.setSpacing(6)

                    sw2 = pg.QtWidgets.QLabel()
                    sw2.setFixedSize(18, 4)
                    sw2.setStyleSheet("background:cyan; border:1px solid rgba(255,255,255,0.25);")
                    lb2 = pg.QtWidgets.QLabel(str(series2_col))

                    row2.addWidget(sw2)
                    row2.addWidget(lb2)
                    window.legend_layout.addWidget(entry2)

                window.legend_layout.addStretch(1)
            except Exception:
                pass

        first_station = float(x[0])
        sel_line = pg.InfiniteLine(
            pos=first_station,
            angle=90,
            movable=False,
            pen=pg.mkPen("red", width=2),
        )
        sel_line.setZValue(10000)
        sel_line.setVisible(True)
        plot.addItem(sel_line)

        window.items["curve1"] = c1
        window.items["curve2"] = c2
        window.items["sel_line"] = sel_line
        window.items["station_values"] = x
        window.items["plot_df"] = d.copy()
        window.items["station_col"] = station_col
        window.items["time_col"] = time_col
        window.state["selected_station"] = int(round(first_station))

        def _nearest_station_from_x(clicked_x: float):
            vals = window.items.get("station_values")
            if vals is None or len(vals) == 0:
                return None
            vals = np.asarray(vals, dtype=float)
            idx = int(np.argmin(np.abs(vals - clicked_x)))
            return int(round(float(vals[idx])))

        def _on_scene_click(mouse_event):
            try:
                if mouse_event is None:
                    return

                if mouse_event.button() != QtCore.Qt.MouseButton.LeftButton:
                    return

                vb = plot.getViewBox()
                mouse_point = vb.mapSceneToView(mouse_event.scenePos())
                clicked_x = float(mouse_point.x())

                st = _nearest_station_from_x(clicked_x)
                if st is None:
                    return

                sel_line.setPos(float(st))
                sel_line.setVisible(True)
                sel_line.setZValue(10000)
                window.state["selected_station"] = st

                cb = window.state.get("on_station_selected")
                if cb:
                    cb(st)

            except Exception as e:
                print("[build_two_series_vs_station] click error:", e)

        old_proxy = window.items.get("click_proxy")
        if old_proxy is not None:
            try:
                old_proxy.disconnect()
            except Exception:
                pass

        proxy = pg.SignalProxy(
            plot.scene().sigMouseClicked,
            rateLimit=30,
            slot=lambda args: _on_scene_click(args[0] if isinstance(args, tuple) else args),
        )
        window.items["click_proxy"] = proxy

    @staticmethod
    @staticmethod
    def build_multi_series_vs_time(
            window,
            df,
            time_col="TimeStampNum",
            series=None,
            title="Time Series",
            y_label="Value",
    ):
        import numpy as np
        import pandas as pd
        import pyqtgraph as pg

        series = series or []

        if df is None or df.empty or time_col not in df.columns:
            return

        x = pd.to_numeric(df[time_col], errors="coerce").to_numpy(dtype=np.float64, copy=False)
        good_x = np.isfinite(x)
        if not good_x.any():
            return

        plot = window.plot
        plot.clear()
        plot.showGrid(x=True, y=True, alpha=0.20)
        plot.setTitle(title)
        plot.setLabel("bottom", "Time")
        plot.setLabel("left", y_label)

        try:
            plot.setClipToView(True)
            plot.setDownsampling(auto=True, mode="peak")
        except Exception:
            pass

        if not hasattr(window, "items") or window.items is None:
            window.items = {}
        if not hasattr(window, "state") or window.state is None:
            window.state = {}

        if hasattr(window, "clear_legend"):
            try:
                window.clear_legend()
            except Exception:
                pass

        curves = []
        legend_defs = []

        for s in series:
            col = s.get("col")
            name = s.get("name", col)
            color = s.get("color", "white")

            if col not in df.columns:
                continue

            y = pd.to_numeric(df[col], errors="coerce").to_numpy(dtype=np.float64, copy=False)
            mask = good_x & np.isfinite(y)
            if not mask.any():
                continue

            curve = pg.PlotCurveItem(
                x=x[mask],
                y=y[mask],
                pen=pg.mkPen(color, width=2),
                name=name,
                antialias=False,
            )
            try:
                curve.setSkipFiniteCheck(True)
            except Exception:
                pass

            plot.addItem(curve)
            curves.append(curve)
            legend_defs.append((curve, str(name), str(color)))

        first_x = float(x[good_x][0])
        sel_line = pg.InfiniteLine(
            pos=first_x,
            angle=90,
            movable=False,
            pen=pg.mkPen("red", width=2),
        )
        sel_line.setZValue(10000)
        plot.addItem(sel_line)

        window.items["curves"] = curves
        window.items["sel_line"] = sel_line
        window.state["selected_time"] = first_x

        if hasattr(window, "legend_layout"):
            window.legend_layout.addStretch(1)
            for curve, name, color in legend_defs:
                entry = ClickableLegendItem(name=name, color=color)

                def _toggle_curve(c=curve, e=entry):
                    vis = c.isVisible()
                    c.setVisible(not vis)
                    e.set_active(not vis)

                entry.clicked.connect(_toggle_curve)
                window.legend_layout.addWidget(entry)

            window.legend_layout.addStretch(1)