import math
import sqlite3
from pathlib import Path

import pandas as pd
from bokeh.embed import json_item
from bokeh.layouts import column, gridplot, row
from bokeh.models import ColumnDataSource, HoverTool, Range1d, Span
from bokeh.palettes import Category10, Turbo256
from bokeh.plotting import figure, show
import numpy as np
from bokeh.models import ColumnDataSource, ColorBar
from bokeh.transform import linear_cmap, dodge
from bokeh.palettes import Viridis256
class BlackBoxGraphics(object):
    """Bokeh Graphical functions for BlackBox Logs"""
    def __init__(self,db_path):
        self.db_path = db_path
        self.db_path = Path(db_path)
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def get_bbox_config_names_by_filename(self, file_name: str) -> dict:
        """
        From FileName:
          BlackBox_Files -> Config_FK
          BBox_Configs_List -> rov/gnss names

        Returns:
            {
                "Config_FK": int,
                "rov1_name": str | None,
                "gnss1_name": str | None,
                "rov2_name": str | None,
                "gnss2_name": str | None,
            }
        """

        sql = """
            SELECT
                bf.Config_FK,
                cfg.rov1_name,
                cfg.gnss1_name,
                cfg.rov2_name,
                cfg.gnss2_name,
                cfg.Depth1_name,
                cfg.Depth2_name 
            FROM BlackBox_Files bf
            JOIN BBox_Configs_List cfg ON cfg.ID = bf.Config_FK
            WHERE bf.FileName = ?
        """

        with self._connect() as conn:
            row = conn.execute(sql, (file_name,)).fetchone()

        if row is None:
            raise ValueError(f"FileName not found or no config linked: {file_name}")

        return {
            "file_name": file_name,
            "Config_FK": row["Config_FK"],
            "rov1_name": row["rov1_name"],
            "gnss1_name": row["gnss1_name"],
            "rov2_name": row["rov2_name"],
            "gnss2_name": row["gnss2_name"],
        }

    def bokeh_gnss_qc_timeseries(
            self,
            # selection
            file_name: str | None = None,
            file_names: list[str] | None = None,
            file_ids: list[int] | None = None,
            config_fk: int | None = None,
            day: str | None = None,
            start_ts: str | None = None,
            end_ts: str | None = None,

            # labels
            gnss1_label: str | None = None,
            gnss2_label: str | None = None,

            # plot options
            title: str = "GNSS QC",
            height: int = 220,
            is_show: bool = False,

            # DiffAge thresholds (seconds)
            diff_good_max: float = 19.9,
            diff_warn_max: float = 29.9,
    ):
        # expects imports at top of file

        # ---------- resolve legend names ----------
        _gnss1 = "GNSS1"
        _gnss2 = "GNSS2"

        single_file = None
        if file_name:
            single_file = file_name
        elif file_names and len(file_names) == 1:
            single_file = file_names[0]

        if single_file:
            try:
                cfg = self.get_bbox_config_names_by_filename(single_file)
                if cfg.get("gnss1_name"):
                    _gnss1 = cfg["gnss1_name"]
                if cfg.get("gnss2_name"):
                    _gnss2 = cfg["gnss2_name"]
            except Exception:
                pass

        if gnss1_label:
            _gnss1 = gnss1_label
        if gnss2_label:
            _gnss2 = gnss2_label

        # ---------- normalize file inputs ----------
        if file_names is None:
            file_names = []
        if file_name:
            file_names = list(file_names) + [file_name]
        if file_ids is None:
            file_ids = []

        # ---------- file_names -> file_ids ----------
        if file_names:
            placeholders = ",".join("?" for _ in file_names)
            q = f"SELECT ID, FileName FROM BlackBox_Files WHERE FileName IN ({placeholders})"
            with self._connect() as conn:
                rows = conn.execute(q, tuple(file_names)).fetchall()

            found = {r["FileName"]: int(r["ID"]) for r in rows} if rows else {}
            missing = [fn for fn in file_names if fn not in found]
            if missing:
                raise ValueError(f"BlackBox_Files: FileName not found: {missing}")

            file_ids = list(set(file_ids + list(found.values())))

        # ---------- build WHERE ----------
        where = ["bb.TimeStamp IS NOT NULL"]
        params: list = []

        if file_ids:
            placeholders = ",".join("?" for _ in file_ids)
            where.append(f"bb.File_FK IN ({placeholders})")
            params.extend(file_ids)

        if config_fk is not None:
            where.append("bf.Config_FK = ?")
            params.append(int(config_fk))

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
                bb.GNSS1_NOS, bb.GNSS1_DiffAge, bb.GNSS1_FixQuality,
                bb.GNSS2_NOS, bb.GNSS2_DiffAge, bb.GNSS2_FixQuality
            FROM BlackBox bb
            JOIN BlackBox_Files bf ON bf.ID = bb.File_FK
            WHERE {" AND ".join(where)}
            ORDER BY bb.TimeStamp
        """

        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=tuple(params))

        if df.empty:
            p = figure(title=f"{title} (no data)", sizing_mode="stretch_both")
            if is_show:
                show(p)
            return p

        # ---------- normalize types ----------
        df["T"] = pd.to_datetime(df["T"], errors="coerce")
        df = df.dropna(subset=["T"])

        # numeric
        for c in ["GNSS1_NOS", "GNSS1_DiffAge", "GNSS1_FixQuality",
                  "GNSS2_NOS", "GNSS2_DiffAge", "GNSS2_FixQuality"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # NOS >= 0
        df.loc[df["GNSS1_NOS"] < 0, "GNSS1_NOS"] = np.nan
        df.loc[df["GNSS2_NOS"] < 0, "GNSS2_NOS"] = np.nan

        # FixQuality in 0..9
        for c in ("GNSS1_FixQuality", "GNSS2_FixQuality"):
            df.loc[(df[c].notna()) & ((df[c] < 0) | (df[c] > 9)), c] = np.nan

        # DiffAge sane
        for c in ("GNSS1_DiffAge", "GNSS2_DiffAge"):
            df.loc[(df[c].notna()) & (df[c] < 0), c] = np.nan
            df.loc[(df[c].notna()) & (df[c] > 9999), c] = np.nan

        # ---------- DiffAge QC colors ----------
        def _qc_diffage(v):
            if pd.isna(v):
                return "no"
            if v <= diff_good_max:
                return "good"
            if v <= diff_warn_max:
                return "warn"
            return "bad"

        def _qc_color(st):
            return {
                "good": "#2ca02c",  # green
                "warn": "#ff7f0e",  # orange
                "bad": "#d62728",  # red
                "no": "#9e9e9e",  # gray
            }.get(st, "#9e9e9e")

        df["GNSS1_DA_Color"] = df["GNSS1_DiffAge"].apply(_qc_diffage).apply(_qc_color)
        df["GNSS2_DA_Color"] = df["GNSS2_DiffAge"].apply(_qc_diffage).apply(_qc_color)

        # plot-friendly (avoid extreme spikes flattening the plot)
        df["GNSS1_DiffAgePlot"] = df["GNSS1_DiffAge"].clip(lower=0, upper=diff_warn_max * 3)
        df["GNSS2_DiffAgePlot"] = df["GNSS2_DiffAge"].clip(lower=0, upper=diff_warn_max * 3)

        src = ColumnDataSource(df)

        tools = "pan,wheel_zoom,box_zoom,reset,hover,save"

        # ---------- plot 1: NOS ----------
        p1 = figure(
            title=f"{title} — NOS",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            active_scroll="wheel_zoom",
            sizing_mode="stretch_both",
        )
        p1.line("T", "GNSS1_NOS", source=src, line_width=2, color="red", legend_label=_gnss1)
        p1.line("T", "GNSS2_NOS", source=src, line_width=2, color="blue", legend_label=_gnss2)
        p1.legend.location = "top_left"
        p1.legend.click_policy = "hide"

        # bar width in ms (adjust if needed)
        bar_width_ms = 30_000

        # ---------- plot 2a: DiffAge GNSS1 (vbars) ----------
        p2a = figure(
            title=f"{title} — DiffAge {_gnss1} (s)  good≤{diff_good_max}  warn≤{diff_warn_max}  bad>{diff_warn_max}",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            active_scroll="wheel_zoom",
            sizing_mode="stretch_both",
            x_range=p1.x_range,
        )
        p2a.vbar(
            x="T",
            top="GNSS1_DiffAgePlot",
            width=bar_width_ms,
            source=src,
            fill_color="GNSS1_DA_Color",
            line_color=None,
            alpha=0.9,
            legend_label=_gnss1,
        )
        p2a.add_layout(Span(location=diff_good_max, dimension="width", line_dash="dashed", line_width=2))
        p2a.add_layout(Span(location=diff_warn_max, dimension="width", line_dash="dotted", line_width=2))
        p2a.yaxis.axis_label = "DiffAge (sec)"
        p2a.legend.location = "top_left"
        p2a.legend.click_policy = "hide"

        # ---------- plot 2b: DiffAge GNSS2 (vbars) ----------
        p2b = figure(
            title=f"{title} — DiffAge {_gnss2} (s)  good≤{diff_good_max}  warn≤{diff_warn_max}  bad>{diff_warn_max}",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            active_scroll="wheel_zoom",
            sizing_mode="stretch_both",
            x_range=p1.x_range,
        )
        p2b.vbar(
            x="T",
            top="GNSS2_DiffAgePlot",
            width=bar_width_ms,
            source=src,
            fill_color="GNSS2_DA_Color",
            line_color=None,
            alpha=0.9,
            legend_label=_gnss2,
        )
        p2b.add_layout(Span(location=diff_good_max, dimension="width", line_dash="dashed", line_width=2))
        p2b.add_layout(Span(location=diff_warn_max, dimension="width", line_dash="dotted", line_width=2))
        p2b.yaxis.axis_label = "DiffAge (sec)"
        p2b.legend.location = "top_left"
        p2b.legend.click_policy = "hide"

        # ---------- plot 3: FixQuality ----------
        fq_hint = "FQ:0 inv 1 GPS 2 DGPS 4 RTKfix 5 RTKflt 6 est 7 man 8 sim 9 SBAS"
        gnss1_leg = f"{_gnss1} ({fq_hint})"
        gnss2_leg = f"{_gnss2} ({fq_hint})"

        p3 = figure(
            title=f"{title} — FixQuality",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            active_scroll="wheel_zoom",
            sizing_mode="stretch_both",
            x_range=p1.x_range,
        )
        p3.step("T", "GNSS1_FixQuality", source=src, mode="after", line_width=2, legend_label=gnss1_leg)
        p3.step("T", "GNSS2_FixQuality", source=src, mode="after", line_width=2, legend_label=gnss2_leg)
        p3.legend.location = "top_left"
        p3.legend.click_policy = "hide"

        layout = column(p1, p2a, p2b, p3, sizing_mode="stretch_both")

        if is_show:
            show(layout)
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
            rov_num: int = 1,
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

        # ✅ JOIN to config table to fetch depth sensor names
        sql = f"""
            SELECT
                bb.TimeStamp AS T,
                bb.ROV{rov_num}_Depth1 AS Depth1,
                bb.ROV{rov_num}_Depth2 AS Depth2,
                bc.ID AS ConfigID,
                bc.Depth1_name AS Depth1Name,
                bc.Depth2_name AS Depth2Name,
                bc.rov1_name AS Rov1Name,
                bc.rov2_name AS Rov2Name,
                bf.FileName
            FROM BlackBox bb
            JOIN BlackBox_Files bf 
                ON bf.ID = bb.File_FK
            LEFT JOIN BBox_Configs_List bc 
                ON bc.ID = bf.Config_FK
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

        # ✅ pick labels:
        # if slice contains multiple configs, show generic labels but keep names in hover
        u1 = [x for x in df["Depth1Name"].dropna().unique().tolist() if str(x).strip()]
        u2 = [x for x in df["Depth2Name"].dropna().unique().tolist() if str(x).strip()]

        depth1_label = u1[0] if len(u1) == 1 else f"ROV{rov_num} Depth1"
        depth2_label = u2[0] if len(u2) == 1 else f"ROV{rov_num} Depth2"

        base_title = title or f"ROV{rov_num} compare — {plot}"
        if file_name:
            base_title += f" — file: {file_name}"
        if day:
            base_title += f" — day: {day}"
        if start_ts or end_ts:
            base_title += f" — slice: {start_ts or ''} .. {end_ts or ''}"
        if len(u1) != 1 or len(u2) != 1:
            base_title += " — (multiple configs)"

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
                x_axis_label=f"{depth1_label} (m)",
                y_axis_label=f"{depth2_label} (m)",
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

            # ✅ show sensor names in hover (always)
            p.hover.tooltips = [
                ("TimeStamp", "@T"),
                ("File", "@FileName"),
                ("ConfigID", "@ConfigID"),
                ("Depth1 name", "@Depth1Name"),
                ("Depth2 name", "@Depth2Name"),
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
                x_axis_label=f"{depth1_label} - {depth2_label} (m)",
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
                x_axis_label=f"Mean depth ({depth1_label}+{depth2_label})/2 (m)",
                y_axis_label=f"Diff ({depth1_label}-{depth2_label}) (m)",
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
            """
            r_err = p.circle(
                x="Mean", y="Diff",
                source=src_err,
                size=point_size + 2,
                alpha=1.0,
                fill_color="red",
                line_color="red",
                legend_label=f"ERROR (>{qc_limit:g} m)",
            )
            """
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
                ("File", "@FileName"),
                ("Depth1 name", "@Depth1Name"),
                ("Depth2 name", "@Depth2Name"),
                ("Mean", "@Mean{0.0}"),
                ("Diff", "@Diff{0.00}"),
                ("Depth1", "@Depth1{0.0}"),
                ("Depth2", "@Depth2{0.0}"),
            ]
            #p.hover.renderers = [r_ok, r_err]
            p.hover.renderers = [r_ok]
            if is_show:
                show(p)
            return p

        raise ValueError('plot must be one of: "scatter", "hist", "bland"')






