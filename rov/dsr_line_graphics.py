import math
import sqlite3
from pathlib import Path

import pandas as pd
from bokeh.embed import json_item
from bokeh.layouts import column, gridplot, row
from bokeh.models import ColumnDataSource, HoverTool, Range1d
from bokeh.palettes import Category10, Turbo256
from bokeh.plotting import figure, show
import numpy as np
from bokeh.models import ColumnDataSource, ColorBar
from bokeh.transform import linear_cmap
from bokeh.palettes import Viridis256




class DSRLineGraphics(object):
    def __init__(self,db_path):
        self.db_path = db_path
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def get_sigmas_deltas(self,line):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT Station, Sigma, Sigma1, Sigma2, Sigma3,ROV,TimeStamp,Node,
                DeltaEprimarytosecondary,DeltaNprimarytosecondary,Rangeprimarytosecondary,RangetoPrePlot,BrgtoPrePlot 
                FROM DSR
                WHERE Line = ?
                  AND TimeStamp IS NOT NULL
                  AND TRIM(TimeStamp) <> ''
                ORDER BY Station
                """,
                (line,),
            ).fetchall()
        return rows
    def plot_dep_sigmas(self, line, rows, isShow=False):
        """
        Bokeh plot from DSR table fields Sigma, Sigma1, Sigma2, Sigma3
        X axis is Station
        4 plots located vertically with common X axis
        return json_item or if isShow is true show(column(p1,p2,p3,p4))
        """

        try:
            line = int(line)
        except Exception:
            raise ValueError("line must be integer")

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT Station, Sigma, Sigma1, Sigma2, Sigma3,ROV,TimeStamp,Node  
                FROM DSR
                WHERE Line = ?
                  AND TimeStamp IS NOT NULL
                  AND TRIM(TimeStamp) <> ''
                ORDER BY Station
                """,
                (line,),
            ).fetchall()

        if not rows:
            p = figure(height=220, width=950, title=f"Line {line} — no deployed sigma data")
            layout = column(p, sizing_mode="stretch_width")
            if isShow:
                show(layout)
                return None
            return layout

        stations = [r["Station"] for r in rows]
        src = ColumnDataSource(
            data=dict(
                station=stations,
                Sigma=[r["Sigma"] for r in rows],
                Sigma1=[r["Sigma1"] for r in rows],
                Sigma2=[r["Sigma2"] for r in rows],
                Sigma3=[r["Sigma3"] for r in rows],
                Rov =[r["ROV"] for r in rows],
                Date=[r['TimeStamp'] for r in rows],
            )
        )

        x_min = min(stations)
        x_max = max(stations)
        shared_x = Range1d(x_min, x_max)
        if len(stations) > 1:
            spacings = [b - a for a, b in zip(stations, stations[1:]) if b > a]
            step = min(spacings) if spacings else 1
            bar_width = step * 0.8
        else:
            bar_width = 1
        colors = Category10[10]  # safe to use 0..9
        c0 = colors[0]
        c1 = colors[1]
        c2 = colors[2]
        c3 = colors[3]

        def stats(values):
            vals = [v for v in values if v is not None and not math.isnan(v)]
            if not vals:
                return None, None, None
            return min(vals), max(vals), sum(vals) / len(vals)
        def _make_plot(field, title, x_range,bar_width,color):

            p = figure(
                width_policy='max',
                height_policy='max',
                title=title,
                x_axis_label="Station",
                y_axis_label='σ,m',
                x_range=x_range,  # always a Range1d now
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )

            p.vbar(
                x="station",
                top=field,
                source=src,
                width=bar_width,
                fill_color=color
            )

            #p.line("station", field, source=src, line_width=2)
            #p.circle("station", field, source=src, size=4)

            p.add_tools(
                HoverTool(
                    tooltips=[("Station", "@station"),
                              (field, f"@{field}"),
                              ("ROV", f"@Rov"),
                              ("Date:", f"@Date"),
                              ],
                    mode="vline",
                )
            )
            p.xgrid.visible = True
            p.ygrid.visible = True
            return p

        sig0 = [r["Sigma"] for r in rows]
        sig1 = [r["Sigma1"] for r in rows]
        sig2 = [r["Sigma2"] for r in rows]
        sig3 = [r["Sigma3"] for r in rows]

        min0, max0, avg0 = stats(sig0)
        min1, max1, avg1 = stats(sig1)
        min2, max2, avg2 = stats(sig2)
        min3, max3, avg3 = stats(sig3)
        p1 = _make_plot("Sigma", f"Line {line} — 95%CE Primary Easting σ (deployed) min:{min0:.1f}; max:{max0:.1f};avg:{avg0:.1f}", shared_x,bar_width,c0)
        p2 = _make_plot("Sigma1", f"Line {line} — 95%CE Primary Northing σ (deployed) min:{min1:.1f}; max:{max1:.1f};avg:{avg1:.1f}", shared_x,bar_width,c1)
        p3 = _make_plot("Sigma2", f"Line {line} — 95%CE Secondary Easting σ (deployed) min:{min2:.1f}; max:{max2:.1f};avg:{avg2:.1f}", shared_x,bar_width,c2)
        p4 = _make_plot("Sigma3", f"Line {line} — 95%CE Secondary Northing σ (deployed) min:{min3:.1f}; max:{max3:.1f};avg:{avg3:.1f}", shared_x,bar_width,c3)

        layout = gridplot(
            [[p1], [p2], [p3], [p4]],
            merge_tools=True,
            toolbar_location="above",  # or "above"
            sizing_mode="stretch_both",
        )

        if isShow:
            show(layout)
            return None

        return layout
    def plot_dep_deltas(self, line, rows, isShow=False):
        """
        Bokeh plot from DSR table fields Sigma, Sigma1, Sigma2, Sigma3
        X axis is Station
        4 plots located vertically with common X axis
        return json_item or if isShow is true show(column(p1,p2,p3,p4))
        """

        try:
            line = int(line)
        except Exception:
            raise ValueError("line must be integer")


        if not rows:
            p = figure(height=220, width=950, title=f"Line {line} — no deployed sigma data")
            layout = column(p, sizing_mode="stretch_width")
            if isShow:
                show(layout)
                return None
            return layout

        stations = [r["Station"] for r in rows]
        src = ColumnDataSource(
            data=dict(
                station=stations,
                Delta1=[r["DeltaEprimarytosecondary"] for r in rows],
                Delta2=[r["DeltaNprimarytosecondary"] for r in rows],
                Delta3=[r["Rangeprimarytosecondary"] for r in rows],
                Delta4=[r["RangetoPrePlot"] for r in rows],
                Rov =[r["ROV"] for r in rows],
                Date=[r['TimeStamp'] for r in rows],
            )
        )

        x_min = min(stations)
        x_max = max(stations)
        shared_x = Range1d(x_min, x_max)
        if len(stations) > 1:
            spacings = [b - a for a, b in zip(stations, stations[1:]) if b > a]
            step = min(spacings) if spacings else 1
            bar_width = step * 0.8
        else:
            bar_width = 1
        colors = Category10[10]  # safe to use 0..9
        c0 = colors[0]
        c1 = colors[1]
        c2 = colors[2]
        c3 = colors[3]

        def stats(values):
            vals = [v for v in values if v is not None and not math.isnan(v)]
            if not vals:
                return None, None, None
            return min(vals), max(vals), sum(vals) / len(vals)
        def _make_plot(field, title, x_range,bar_width,color):

            p = figure(
                width_policy='max',
                height_policy='max',
                title=title,
                x_axis_label="Station",
                y_axis_label='σ,m',
                x_range=x_range,  # always a Range1d now
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )

            p.vbar(
                x="station",
                top=field,
                source=src,
                width=bar_width,
                fill_color=color
            )

            #p.line("station", field, source=src, line_width=2)
            #p.circle("station", field, source=src, size=4)

            p.add_tools(
                HoverTool(
                    tooltips=[("Station", "@station"),
                              (field, f"@{field}"),
                              ("ROV", f"@Rov"),
                              ("Date:", f"@Date"),
                              ],
                    mode="vline",
                )
            )
            p.xgrid.visible = True
            p.ygrid.visible = True
            return p

        dt0 = [r["Sigma"] for r in rows]
        dt1 = [r["Sigma1"] for r in rows]
        dt2 = [r["Sigma2"] for r in rows]
        dt3 = [r["Sigma3"] for r in rows]

        min0, max0, avg0 = stats(dt0)
        min1, max1, avg1 = stats(dt1)
        min2, max2, avg2 = stats(dt2)
        min3, max3, avg3 = stats(dt3)
        p1 = _make_plot("Delta1", f"Line {line} — Primary vs Secondary Easting Δ (deployed) min:{min0:.1f}; max:{max0:.1f};avg:{avg0:.1f}", shared_x,bar_width,c0)
        p2 = _make_plot("Delta2", f"Line {line} — Primary vs Secondary Northing Δ (deployed) min:{min1:.1f}; max:{max1:.1f};avg:{avg1:.1f}", shared_x,bar_width,c1)
        p3 = _make_plot("Delta3", f"Line {line} — Range Primary vs Secondary Δ (deployed) min:{min2:.1f}; max:{max2:.1f};avg:{avg2:.1f}", shared_x,bar_width,c2)
        p4 = _make_plot("Delta4",f"Line {line} — Range to Preplot Δ (deployed) min:{min2:.1f}; max:{max2:.1f};avg:{avg2:.1f}",
                        shared_x, bar_width, c3)


        layout = gridplot(
            [[p1], [p2], [p3],[p4]],
            merge_tools=True,
            toolbar_location="above",  # or "above"
            sizing_mode="stretch_both",
        )

        if isShow:
            show(layout)
            return None

        return layout

    def bokeh_hist2d_rov1_depth_vs_diff(
            self,
            bins_x: int = 40,
            bins_y: int = 40,
            title: str = "ROV1 Depth vs ROV1 DepthDiff — Allocation Histogram",
            x_label: str = "ROV1 Depth (m)",
            y_label: str = "ROV1 DepthDiff (m)",
            clip_percentile: float | None = None,
            is_show: bool = False,
    ):
        sql="""select ROV1_Depth,ROV1_Depth1, ROV1_Depth2, (ROV1_Depth1-ROV1_Depth2) as ROV1_DepthDiff 
                FROM BlackBox WHERE ROV1_Depth BETWEEN -2000 AND 0 AND ROV1_Depth2 BETWEEN -2000 AND 0 ORDER BY ROV1_Depth"""
        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn)
        df = df[["ROV1_Depth", "ROV1_DepthDiff"]].dropna()
        if df.empty:
            p = figure( title=f"{title} (no data)")
            p.xaxis.axis_label = x_label
            p.yaxis.axis_label = y_label
            return p

        x = pd.to_numeric(df["ROV1_Depth"], errors="coerce").to_numpy()
        y = pd.to_numeric(df["ROV1_DepthDiff"], errors="coerce").to_numpy()
        m = np.isfinite(x) & np.isfinite(y)
        x, y = x[m], y[m]

        if x.size == 0:
            p = figure(title=f"{title} (no finite values)")
            p.xaxis.axis_label = x_label
            p.yaxis.axis_label = y_label
            return p

        if clip_percentile is not None and 0 < clip_percentile < 100:
            x_lo, x_hi = np.percentile(x, [100 - clip_percentile, clip_percentile])
            y_lo, y_hi = np.percentile(y, [100 - clip_percentile, clip_percentile])
            mm = (x >= x_lo) & (x <= x_hi) & (y >= y_lo) & (y <= y_hi)
            x, y = x[mm], y[mm]

        xedges = np.linspace(x.min(), x.max(), bins_x + 1)
        yedges = np.linspace(y.min(), y.max(), bins_y + 1)

        hist, xedges, yedges = np.histogram2d(x, y, bins=[xedges, yedges])

        xcenters = (xedges[:-1] + xedges[1:]) / 2
        ycenters = (yedges[:-1] + yedges[1:]) / 2
        xs, ys = np.meshgrid(xcenters, ycenters)
        counts = hist.T.flatten()

        source = ColumnDataSource(dict(
            x=xs.flatten(),
            y=ys.flatten(),
            count=counts,
        ))

        p = figure(
            sizing_mode='stretch_both',
            title=title,
            x_axis_label=x_label,
            y_axis_label=y_label,
            tools="pan,wheel_zoom,box_zoom,reset,hover,save",
            active_scroll="wheel_zoom",
        )

        mapper = linear_cmap(
            field_name="count",
            palette=Viridis256,
            low=float(np.nanmin(counts)),
            high=float(np.nanmax(counts)) if np.nanmax(counts) > 0 else 1.0,
        )

        p.rect(
            x="x",
            y="y",
            width=(xedges[1] - xedges[0]),
            height=(yedges[1] - yedges[0]),
            source=source,
            fill_color=mapper,
            line_color=None,
        )

        color_bar = ColorBar(color_mapper=mapper["transform"], width=10)
        p.add_layout(color_bar, "right")

        p.hover.tooltips = [
            ("Depth", "@x{0.00}"),
            ("DepthDiff", "@y{0.00}"),
            ("Count", "@count{0}"),
        ]
        if is_show:
            show(row(p,sizing_mode="stretch_both"))
        else:
            return p

    def bokeh_scatter_rov_depth1_vs_depth2_qc(
            self,
            depth_min: float = -2000,
            depth_max: float = -100,
            title: str = "ROV Depth1 vs Depth2 — QC (all points, DepthDiff colored)",
            point_size: int = 5,
            alpha: float = 0.6,
            is_show: bool = False,
    ):
        # expected imports at top of file:
        # import numpy as np
        # import pandas as pd
        # from bokeh.plotting import figure, show
        # from bokeh.models import ColumnDataSource, ColorBar
        # from bokeh.transform import linear_cmap
        # from bokeh.palettes import Turbo256

        QC_MIN = -10.0
        QC_MAX = 10.0

        # ---------- SQL per ROV ----------
        sql_rov1 = """
            SELECT
                ROV1_Depth1 AS Depth1,
                ROV1_Depth2 AS Depth2
            FROM BlackBox
            WHERE ROV1_Depth1 BETWEEN ? AND ?
              AND ROV1_Depth2 BETWEEN ? AND ?
            ORDER BY ROV1_Depth1 DESC
        """

        sql_rov2 = """
            SELECT
                ROV2_Depth1 AS Depth1,
                ROV2_Depth2 AS Depth2
            FROM BlackBox
            WHERE ROV2_Depth1 BETWEEN ? AND ?
              AND ROV2_Depth2 BETWEEN ? AND ?
            ORDER BY ROV2_Depth1 DESC
        """

        params = (depth_min, depth_max, depth_min, depth_max)

        with self._connect() as conn:
            df1 = pd.read_sql_query(sql_rov1, conn, params=params)
            df2 = pd.read_sql_query(sql_rov2, conn, params=params)

        if df1.empty and df2.empty:
            p = figure(title=f"{title} (no data)", sizing_mode="stretch_both")
            if is_show:
                show(p)
            return p

        # ---------- prepare dataframe ----------
        def _prepare(df, rov_label):
            if df.empty:
                return pd.DataFrame(columns=["Depth1", "Depth2", "Diff", "ROV"])

            tmp = df.copy()
            tmp["Depth1"] = pd.to_numeric(tmp["Depth1"], errors="coerce")
            tmp["Depth2"] = pd.to_numeric(tmp["Depth2"], errors="coerce")
            tmp = tmp.dropna(subset=["Depth1", "Depth2"])

            tmp["Diff"] = tmp["Depth1"] - tmp["Depth2"]
            tmp["ROV"] = rov_label
            return tmp

        g1 = _prepare(df1, "ROV1")
        g2 = _prepare(df2, "ROV2")
        g = pd.concat([g1, g2], ignore_index=True)

        if g.empty:
            p = figure(title=f"{title} (no valid data)", sizing_mode="stretch_both")
            if is_show:
                show(p)
            return p

        # ---------- sort deep → shallow ----------
        g = g.sort_values("Depth1", ascending=False)

        Depth1 = g["Depth1"].to_numpy()
        Depth2 = g["Depth2"].to_numpy()

        # ---------- reversed & locked axes ----------
        lo = float(min(depth_min, Depth1.min(), Depth2.min()))
        hi = float(max(depth_max, Depth1.max(), Depth2.max()))

        src1_all = ColumnDataSource(g[g["ROV"] == "ROV1"])
        src2_all = ColumnDataSource(g[g["ROV"] == "ROV2"])

        p = figure(
            title=title,
            x_axis_label="Depth1 (m)",
            y_axis_label="Depth2 (m)",
            tools="pan,wheel_zoom,box_zoom,lasso_select,reset,hover,save",
            active_scroll="wheel_zoom",
            x_range=(hi, lo),
            y_range=(hi, lo),
            match_aspect=True,
            sizing_mode="stretch_both",
        )

        # ---------- fixed color scale ----------
        mapper = linear_cmap("Diff", Turbo256, QC_MIN, QC_MAX)

        # ---------- split OK / ERROR ----------
        def _split_ok_err(src):
            diffs = np.asarray(src.data["Diff"], dtype=float)
            mask_ok = np.isfinite(diffs) & (diffs >= QC_MIN) & (diffs <= QC_MAX)
            mask_err = np.isfinite(diffs) & ~mask_ok

            ok = {k: np.asarray(v)[mask_ok] for k, v in src.data.items()}
            err = {k: np.asarray(v)[mask_err] for k, v in src.data.items()}

            return ColumnDataSource(ok), ColumnDataSource(err)

        src1_ok, src1_err = _split_ok_err(src1_all)
        src2_ok, src2_err = _split_ok_err(src2_all)

        # ---------- plot ----------
        r1_ok = p.circle(
            x="Depth1", y="Depth2",
            source=src1_ok,
            size=point_size,
            alpha=alpha,
            line_alpha=0,
            fill_color=mapper,
            legend_label="ROV1 OK (±10 m)",
        )
        r1_err = p.circle(
            x="Depth1", y="Depth2",
            source=src1_err,
            size=point_size + 2,
            alpha=1.0,
            fill_color="red",
            line_color="red",
            legend_label="ROV1 ERROR (>10 m)",
        )

        r2_ok = p.triangle(
            x="Depth1", y="Depth2",
            source=src2_ok,
            size=point_size,
            alpha=alpha,
            line_alpha=0,
            fill_color=mapper,
            legend_label="ROV2 OK (±10 m)",
        )
        r2_err = p.triangle(
            x="Depth1", y="Depth2",
            source=src2_err,
            size=point_size + 2,
            alpha=1.0,
            fill_color="red",
            line_color="red",
            legend_label="ROV2 ERROR (>10 m)",
        )

        # ---------- 1:1 diagonal ----------
        p.line([hi, lo], [hi, lo], line_width=2, legend_label="1:1")

        p.legend.location = "top_left"
        p.legend.click_policy = "hide"

        # ---------- colorbar ----------
        color_bar = ColorBar(
            color_mapper=mapper["transform"],
            width=12,
            title="DepthDiff (m)  [-10 … +10]",
        )
        p.add_layout(color_bar, "right")

        # ---------- hover ----------
        p.hover.tooltips = [
            ("ROV", "@ROV"),
            ("Depth1", "@Depth1{0.0}"),
            ("Depth2", "@Depth2{0.0}"),
            ("DepthDiff", "@Diff{0.0}"),
        ]
        p.hover.renderers = [r1_ok, r2_ok, r1_err, r2_err]

        if is_show:
            show(p)
        else:
            return p

    def bokeh_compare_sensors_rov1(
            self,
            depth_min: float = -2000,
            depth_max: float = -100,
            rov_num:int =1,
            # filters (use any combination)
            file_name: str | None = None,  # BlackBox_Files.FileName
            day: str | None = None,  # "2026-02-03"
            start_ts: str | None = None,  # "2026-02-03 06:00:00"
            end_ts: str | None = None,  # "2026-02-03 18:00:00"
            # plot options
            plot: str = "scatter",  # "scatter" | "hist" | "bland"
            qc_limit: float = 10.0,  # +/- tolerance
            bins: int = 80,  # histogram bins
            point_size: int = 4,
            alpha: float = 0.5,
            title: str | None = None,
            is_show: bool = False,
    ):
        # expects these already imported at top of file:
        # import numpy as np
        # import pandas as pd
        # from bokeh.plotting import figure, show
        # from bokeh.models import ColumnDataSource, ColorBar
        # from bokeh.transform import linear_cmap
        # from bokeh.palettes import Turbo256

        QC_MIN, QC_MAX = -abs(qc_limit), abs(qc_limit)

        where = [
            f"bb.ROV{rov_num}_Depth1 BETWEEN ? AND ?",
            f"bb.ROV{rov_num}_Depth2 BETWEEN ? AND ?",
        ]
        params = [depth_min, depth_max, depth_min, depth_max]

        if file_name:
            where.append("bf.FileName = ?")
            params.append(file_name)

        if day:
            where.append("bb.TimeStamp >= ? AND bb.TimeStamp < ?")
            params.append(f"{day} 00:00:00")
            params.append(f"{day} 23:59:59")

        if start_ts:
            where.append("bb.TimeStamp >= ?")
            params.append(start_ts)
        if end_ts:
            where.append("bb.TimeStamp <= ?")
            params.append(end_ts)

        sql = f"""
            SELECT
                bb.TimeStamp AS T,
                bb.ROV{rov_num}_Depth1 AS Depth1,
                bb.ROV{rov_num}_Depth2 AS Depth2
            FROM BlackBox bb
            JOIN BlackBox_Files bf ON bf.ID = bb.File_FK
            WHERE {" AND ".join(where)}
            ORDER BY bb.TimeStamp
        """

        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=tuple(params))

        df["Depth1"] = pd.to_numeric(df["Depth1"], errors="coerce")
        df["Depth2"] = pd.to_numeric(df["Depth2"], errors="coerce")
        df = df.dropna(subset=["Depth1", "Depth2"])

        if df.empty:
            p = figure(title=(title or f"ROV{rov_num} compare (no data)"), sizing_mode="stretch_both")
            if is_show:
                show(p)
            return p

        df["Diff"] = df["Depth1"] - df["Depth2"]
        df["Mean"] = (df["Depth1"] + df["Depth2"]) / 2.0

        base_title = title or f"ROV{rov_num} compare — {plot}"
        if file_name:
            base_title += f" — file: {file_name}"
        if day:
            base_title += f" — day: {day}"
        if start_ts or end_ts:
            base_title += f" — slice: {start_ts or ''} .. {end_ts or ''}"

        # ---------------- scatter (Depth1 vs Depth2) ----------------
        if plot == "scatter":
            df = df.sort_values("Depth1", ascending=False)

            lo = float(min(depth_min, df["Depth1"].min(), df["Depth2"].min()))
            hi = float(max(depth_max, df["Depth1"].max(), df["Depth2"].max()))

            src_all = ColumnDataSource(df)

            diffs = df["Diff"].to_numpy()
            mask_ok = np.isfinite(diffs) & (diffs >= QC_MIN) & (diffs <= QC_MAX)
            mask_err = np.isfinite(diffs) & ~mask_ok

            src_ok = ColumnDataSource({k: np.asarray(v)[mask_ok] for k, v in src_all.data.items()})
            src_err = ColumnDataSource({k: np.asarray(v)[mask_err] for k, v in src_all.data.items()})

            p = figure(
                title=base_title,
                x_axis_label=f"ROV{rov_num} Depth1 (m)",
                y_axis_label=f"ROV{rov_num} Depth2 (m)",
                tools="pan,wheel_zoom,box_zoom,lasso_select,reset,hover,save",
                active_scroll="wheel_zoom",
                x_range=(hi, lo),
                y_range=(hi, lo),
                match_aspect=True,
                sizing_mode="stretch_both",
            )

            mapper = linear_cmap("Diff", Turbo256, QC_MIN, QC_MAX)

            r_ok = p.circle(
                x="Depth1", y="Depth2",
                source=src_ok,
                size=point_size,
                alpha=alpha,
                line_alpha=0,
                fill_color=mapper,
                legend_label=f"OK (±{qc_limit:g} m)",
            )
            r_err = p.circle(
                x="Depth1", y="Depth2",
                source=src_err,
                size=point_size + 2,
                alpha=1.0,
                fill_color="red",
                line_color="red",
                legend_label=f"ERROR (>{qc_limit:g} m)",
            )

            p.line([hi, lo], [hi, lo], line_width=2, legend_label="1:1")

            color_bar = ColorBar(
                color_mapper=mapper["transform"],
                width=12,
                title=f"DepthDiff (m)  [{QC_MIN:g} … {QC_MAX:g}]",
            )
            p.add_layout(color_bar, "right")

            p.legend.location = "top_left"
            p.legend.click_policy = "hide"

            p.hover.tooltips = [
                ("TimeStamp", "@T"),
                ("Depth1", "@Depth1{0.0}"),
                ("Depth2", "@Depth2{0.0}"),
                ("Diff", "@Diff{0.00}"),
            ]
            p.hover.renderers = [r_ok, r_err]

            if is_show:
                show(p)
            return p

        # ---------------- histogram (Diff distribution) ----------------
        if plot == "hist":
            diff = df["Diff"].to_numpy()
            diff = diff[np.isfinite(diff)]
            hist, edges = np.histogram(diff, bins=bins)

            p = figure(
                title=base_title + " — histogram",
                x_axis_label="Depth1 - Depth2 (m)",
                y_axis_label="Count",
                tools="pan,wheel_zoom,box_zoom,reset,hover,save",
                active_scroll="wheel_zoom",
                sizing_mode="stretch_both",
            )

            src = ColumnDataSource(dict(left=edges[:-1], right=edges[1:], top=hist))
            p.quad(left="left", right="right", bottom=0, top="top", source=src, alpha=0.8)

            p.ray(x=qc_limit, y=0, length=0, angle=1.5708, line_width=2, line_color="red")
            p.ray(x=-qc_limit, y=0, length=0, angle=1.5708, line_width=2, line_color="red")
            p.ray(x=0, y=0, length=0, angle=1.5708, line_width=2)

            p.hover.tooltips = [
                ("Range", "@left{0.00} .. @right{0.00}"),
                ("Count", "@top"),
            ]

            if is_show:
                show(p)
            return p

        # ---------------- bland-altman (Diff vs Mean) ----------------
        if plot == "bland":
            src_all = ColumnDataSource(df)

            diffs = df["Diff"].to_numpy()
            mask_ok = np.isfinite(diffs) & (diffs >= QC_MIN) & (diffs <= QC_MAX)
            mask_err = np.isfinite(diffs) & ~mask_ok
            src_ok = ColumnDataSource({k: np.asarray(v)[mask_ok] for k, v in src_all.data.items()})
            src_err = ColumnDataSource({k: np.asarray(v)[mask_err] for k, v in src_all.data.items()})

            p = figure(
                title=base_title + " — Bland–Altman",
                x_axis_label="Mean depth (Depth1+Depth2)/2 (m)",
                y_axis_label="Diff (Depth1-Depth2) (m)",
                tools="pan,wheel_zoom,box_zoom,lasso_select,reset,hover,save",
                active_scroll="wheel_zoom",
                sizing_mode="stretch_both",
            )

            mapper = linear_cmap("Diff", Turbo256, QC_MIN, QC_MAX)

            r_ok = p.circle(
                x="Mean", y="Diff",
                source=src_ok,
                size=point_size,
                alpha=alpha,
                line_alpha=0,
                fill_color=mapper,
                legend_label=f"OK (±{qc_limit:g} m)",
            )
            r_err = p.circle(
                x="Mean", y="Diff",
                source=src_err,
                size=point_size + 2,
                alpha=1.0,
                fill_color="red",
                line_color="red",
                legend_label=f"ERROR (>{qc_limit:g} m)",
            )

            xmin = float(df["Mean"].min())
            xmax = float(df["Mean"].max())
            p.line([xmin, xmax], [0, 0], line_width=2, legend_label="0")
            p.line([xmin, xmax], [qc_limit, qc_limit], line_width=2, line_color="red", legend_label=f"+{qc_limit:g}")
            p.line([xmin, xmax], [-qc_limit, -qc_limit], line_width=2, line_color="red", legend_label=f"-{qc_limit:g}")

            color_bar = ColorBar(
                color_mapper=mapper["transform"],
                width=12,
                title=f"DepthDiff (m)  [{QC_MIN:g} … {QC_MAX:g}]",
            )
            p.add_layout(color_bar, "right")

            p.legend.location = "top_left"
            p.legend.click_policy = "hide"

            p.hover.tooltips = [
                ("TimeStamp", "@T"),
                ("Mean", "@Mean{0.0}"),
                ("Diff", "@Diff{0.00}"),
                ("Depth1", "@Depth1{0.0}"),
                ("Depth2", "@Depth2{0.0}"),
            ]
            p.hover.renderers = [r_ok, r_err]

            if is_show:
                show(p)
            return p

        raise ValueError('plot must be one of: "scatter", "hist", "bland"')










