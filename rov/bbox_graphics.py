import math
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from bokeh.embed import json_item
from bokeh.layouts import column, gridplot, row
from bokeh.models import (
    ColumnDataSource,
    HoverTool,
    Range1d,
    Span,
    ColorBar, LinearColorMapper, BasicTicker, PrintfTickFormatter, Legend, LegendItem, BoxAnnotation, Button, CustomJS,
)
from bokeh.palettes import Category10, Turbo256, Viridis256, Blues256, Turbo256
from bokeh.plotting import figure, show
from bokeh.transform import linear_cmap, dodge


class BlackBoxGraphics(object):
    """Bokeh Graphical functions for BlackBox Logs (SeisWebLog)"""

    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self._bbox_cache: dict[tuple, pd.DataFrame] = {}
        self.dsr_df=None

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    # ---------------------------------------------------------------------
    # Shared loading + caching
    # ---------------------------------------------------------------------
    def _cache_key(self, **kwargs) -> tuple:
        items = []
        for k in sorted(kwargs.keys()):
            v = kwargs[k]
            if isinstance(v, list):
                v = tuple(v)
            items.append((k, v))
        return tuple(items)

    def clear_bbox_cache(self):
        self._bbox_cache.clear()

    def load_bbox_data(
        self,
        # selection
        file_name: str | None = None,
        file_names: list[str] | None = None,
        file_ids: list[int] | None = None,
        config_fk: int | None = None,
        day: str | None = None,
        start_ts: str | None = None,
        end_ts: str | None = None,
        # what to load
        columns: list[str] | None = None,
        use_cache: bool = True,
        force_reload: bool = False,
    ) -> pd.DataFrame:
        """
        Load BlackBox rows once and reuse across many plots.

        Notes:
          - You can pass file_name OR file_names OR file_ids.
          - Returned dataframe includes 'T' datetime if requested as `bb.TimeStamp AS T`.
          - If `columns` is None, a "common package" (GNSS + ROV + config names) is loaded.
        """

        if file_names is None:
            file_names = []
        if file_ids is None:
            file_ids = []
        if file_name:
            file_names = list(file_names) + [file_name]

        if columns is None:
            # common package used by most plots in this file
            columns = [
                "bb.TimeStamp AS T",
                "bb.File_FK AS File_FK",
                "bf.FileName AS FileName",
                "bf.Config_FK AS Config_FK",

                # GNSS
                "bb.GNSS1_NOS", "bb.GNSS1_DiffAge", "bb.GNSS1_FixQuality","bb.GNSS1_HDOP",
                "bb.GNSS2_NOS", "bb.GNSS2_DiffAge", "bb.GNSS2_FixQuality","bb.GNSS2_HDOP",
                "bb.GNSS1_Elevation", "bb.GNSS2_Elevation",

                # ROV depths (both rovs; keep as-is even if null in some configs)
                "bb.ROV1_Depth1", "bb.ROV1_Depth2",
                "bb.ROV2_Depth1", "bb.ROV2_Depth2",
                "bb.ROV1_Depth","bb.ROV1_HDG","bb.ROV1_SOG","bb.ROV1_COG","bb.ROV1_PITCH","bb.ROV1_ROLL",
                "bb.ROV2_Depth", "bb.ROV2_HDG", "bb.ROV2_SOG", "bb.ROV2_COG","bb.ROV2_PITCH","bb.ROV2_ROLL",


                # Vessel parameters
                "bb.VesselHDG","bb.VesselSOG","bb.VesselCOG",


                # names from config
                "cfg.ID AS ConfigID",
                "cfg.rov1_name AS Rov1Name",
                "cfg.rov2_name AS Rov2Name",
                "cfg.gnss1_name AS Gnss1Name",
                "cfg.gnss2_name AS Gnss2Name",
                "cfg.Depth1_name AS Depth1Name",
                "cfg.Depth2_name AS Depth2Name",
                "cfg.Vessel_name AS VesselName",
            ]

        cache_key = self._cache_key(
            file_names=file_names,
            file_ids=file_ids,
            config_fk=config_fk,
            day=day,
            start_ts=start_ts,
            end_ts=end_ts,
            columns=tuple(columns),
        )

        if use_cache and not force_reload and cache_key in self._bbox_cache:
            return self._bbox_cache[cache_key].copy()

        # file_names -> file_ids
        if file_names:
            placeholders = ",".join("?" for _ in file_names)
            q = f"SELECT ID, Config_FK, FileName FROM BlackBox_Files WHERE FileName IN ({placeholders})"
            with self._connect() as conn:
                rows = conn.execute(q, tuple(file_names)).fetchall()

            found = {r["FileName"]: int(r["ID"]) for r in rows} if rows else {}
            missing = [fn for fn in file_names if fn not in found]
            if missing:
                raise ValueError(f"BlackBox_Files: FileName not found: {missing}")

            file_ids = list(set(list(file_ids) + list(found.values())))

        # WHERE
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
                {", ".join(columns)}
            FROM BlackBox bb
            JOIN BlackBox_Files bf ON bf.ID = bb.File_FK
            LEFT JOIN BBox_Configs_List cfg ON cfg.ID = bf.Config_FK
            WHERE {" AND ".join(where)}
            ORDER BY bb.TimeStamp
        """

        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=tuple(params))

        # normalize timestamps if present
        if "T" in df.columns:
            df["T"] = pd.to_datetime(df["T"], errors="coerce")
            df = df.dropna(subset=["T"])

        if use_cache:
            self._bbox_cache[cache_key] = df.copy()

        return df

    # ---------------------------------------------------------------------
    # Small helpers
    # ---------------------------------------------------------------------
    def get_bbox_config_names_by_filename(self, file_name: str) -> dict:
        """
        From FileName:
          BlackBox_Files -> Config_FK
          BBox_Configs_List -> rov/gnss names
        """
        sql = """
            SELECT
                bf.Config_FK AS Config_FK,
                bc.rov1_name,
                bc.rov2_name,
                bc.gnss1_name,
                bc.gnss2_name,
                bc.Vessel_name,
                bc.Depth1_name,
                bc.Depth2_name
            FROM BlackBox_Files bf
            LEFT JOIN BBox_Configs_List bc ON bc.ID = bf.Config_FK
            WHERE bf.FileName = ?
            LIMIT 1
        """
        with self._connect() as conn:
            row = conn.execute(sql, (file_name,)).fetchone()
        if not row:
            raise ValueError(f"BlackBox_Files: FileName not found: {file_name}")
        return dict(row)

    def _qc_diffage(self, v, diff_good_max: float, diff_warn_max: float) -> str:
        if pd.isna(v):
            return "no"
        if v <= diff_good_max:
            return "good"
        if v <= diff_warn_max:
            return "warn"
        return "bad"

    def _qc_color(self, st: str) -> str:
        return {
            "good": "#2ca02c",  # green
            "warn": "#ff7f0e",  # orange
            "bad": "#d62728",   # red
            "no": "#9e9e9e",    # gray
        }.get(st, "#9e9e9e")

    # ---------------------------------------------------------------------
    # Plots
    # ---------------------------------------------------------------------
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

        # shared preloaded data
        data: pd.DataFrame | None = None,
    ):
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

        # ---------- load once / reuse ----------
        if data is None:
            df = self.load_bbox_data(
                file_name=file_name,
                file_names=file_names,
                file_ids=file_ids,
                config_fk=config_fk,
                day=day,
                start_ts=start_ts,
                end_ts=end_ts,
                columns=[
                    "bb.TimeStamp AS T",
                    "bb.GNSS1_NOS", "bb.GNSS1_DiffAge", "bb.GNSS1_FixQuality",
                    "bb.GNSS2_NOS", "bb.GNSS2_DiffAge", "bb.GNSS2_FixQuality",
                ],
            )
        else:
            df = data.copy()

        if df.empty:
            p = figure(title=f"{title} (no data)", sizing_mode="stretch_both")
            if is_show:
                show(p)
            return p

        # ---------- normalize numeric ----------
        for c in ["GNSS1_NOS", "GNSS1_DiffAge", "GNSS1_FixQuality",
                  "GNSS2_NOS", "GNSS2_DiffAge", "GNSS2_FixQuality"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # NOS >= 0
        if "GNSS1_NOS" in df.columns:
            df.loc[df["GNSS1_NOS"] < 0, "GNSS1_NOS"] = np.nan
        if "GNSS2_NOS" in df.columns:
            df.loc[df["GNSS2_NOS"] < 0, "GNSS2_NOS"] = np.nan

        # FixQuality in 0..9
        for c in ("GNSS1_FixQuality", "GNSS2_FixQuality"):
            if c in df.columns:
                df.loc[(df[c].notna()) & ((df[c] < 0) | (df[c] > 9)), c] = np.nan

        # DiffAge sane
        for c in ("GNSS1_DiffAge", "GNSS2_DiffAge"):
            if c in df.columns:
                df.loc[(df[c].notna()) & (df[c] < 0), c] = np.nan
                df.loc[(df[c].notna()) & (df[c] > 9999), c] = np.nan

        # ---------- DiffAge QC colors ----------
        if "GNSS1_DiffAge" in df.columns:
            df["GNSS1_DA_Color"] = (
                df["GNSS1_DiffAge"]
                .apply(lambda v: self._qc_diffage(v, diff_good_max, diff_warn_max))
                .apply(self._qc_color)
            )
            df["GNSS1_DiffAgePlot"] = df["GNSS1_DiffAge"].clip(lower=0, upper=diff_warn_max * 3)

        if "GNSS2_DiffAge" in df.columns:
            df["GNSS2_DA_Color"] = (
                df["GNSS2_DiffAge"]
                .apply(lambda v: self._qc_diffage(v, diff_good_max, diff_warn_max))
                .apply(self._qc_color)
            )
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

        # bar width/offset in milliseconds (adjust if your sample rate differs)
        bar_width_ms = 30_000
        offset_ms = 12_000

        # ---------- plot 2: DiffAge ----------
        p2 = figure(
            title=f"{title} — DiffAge (s)  good≤{diff_good_max}  warn≤{diff_warn_max}  bad>{diff_warn_max}",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            active_scroll="wheel_zoom",
            sizing_mode="stretch_both",
            x_range=p1.x_range,
        )
        p2.vbar(
            x=dodge("T", -offset_ms, range=p2.x_range),
            top="GNSS1_DiffAgePlot",
            width=bar_width_ms,
            source=src,
            fill_color="GNSS1_DA_Color",
            line_color=None,
            legend_label=_gnss1,
            alpha=0.9,
        )
        p2.vbar(
            x=dodge("T", +offset_ms, range=p2.x_range),
            top="GNSS2_DiffAgePlot",
            width=bar_width_ms,
            source=src,
            fill_color="GNSS2_DA_Color",
            line_color=None,
            legend_label=_gnss2,
            alpha=0.9,
        )
        p2.legend.location = "top_left"
        p2.legend.click_policy = "hide"

        # ---------- plot 3: FixQuality ----------
        p3 = figure(
            title=f"{title} — FixQuality (0..9)",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            active_scroll="wheel_zoom",
            sizing_mode="stretch_both",
            x_range=p1.x_range,
        )
        p3.line("T", "GNSS1_FixQuality", source=src, line_width=2, color="red", legend_label=_gnss1)
        p3.line("T", "GNSS2_FixQuality", source=src, line_width=2, color="blue", legend_label=_gnss2)
        p3.y_range = Range1d(-0.5, 9.5)
        p3.legend.location = "top_left"
        p3.legend.click_policy = "hide"

        # hover
        hover = HoverTool(
            tooltips=[
                ("T", "@T{%F %T}"),
                (f"{_gnss1} NOS", "@GNSS1_NOS{0.0}"),
                (f"{_gnss2} NOS", "@GNSS2_NOS{0.0}"),
                (f"{_gnss1} DiffAge", "@GNSS1_DiffAge{0.0}"),
                (f"{_gnss2} DiffAge", "@GNSS2_DiffAge{0.0}"),
                (f"{_gnss1} FixQ", "@GNSS1_FixQuality{0.0}"),
                (f"{_gnss2} FixQ", "@GNSS2_FixQuality{0.0}"),
            ],
            formatters={"@T": "datetime"},
            mode="vline",
        )
        for p in (p1, p2, p3):
            p.add_tools(hover)

        layout = column(p1, p2, p3, sizing_mode="stretch_both")

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
        depth_min: float = -2000,
        depth_max: float = -100,
        diff_abs_max: float = 30,
        is_show: bool = False,
        # selection
        file_name: str | None = None,
        file_names: list[str] | None = None,
        file_ids: list[int] | None = None,
        config_fk: int | None = None,
        day: str | None = None,
        start_ts: str | None = None,
        end_ts: str | None = None,
        # shared preloaded data
        data: pd.DataFrame | None = None,
    ):
        if data is None:
            df = self.load_bbox_data(
                file_name=file_name,
                file_names=file_names,
                file_ids=file_ids,
                config_fk=config_fk,
                day=day,
                start_ts=start_ts,
                end_ts=end_ts,
                columns=[
                    "bb.ROV1_Depth1 AS Depth1",
                    "bb.ROV1_Depth2 AS Depth2",
                ],
            )
        else:
            df = data.copy()

        if df.empty or "Depth1" not in df.columns or "Depth2" not in df.columns:
            p = figure(title=f"{title} (no data)", sizing_mode="stretch_both")
            if is_show:
                show(p)
            return p

        df["Depth1"] = pd.to_numeric(df["Depth1"], errors="coerce")
        df["Depth2"] = pd.to_numeric(df["Depth2"], errors="coerce")
        df = df.dropna(subset=["Depth1", "Depth2"])

        df = df[(df["Depth1"].between(depth_min, depth_max)) & (df["Depth2"].between(depth_min, depth_max))]
        if df.empty:
            p = figure(title=f"{title} (no data in range)", sizing_mode="stretch_both")
            if is_show:
                show(p)
            return p

        df["DepthDiff"] = df["Depth2"] - df["Depth1"]
        df = df[df["DepthDiff"].abs() <= diff_abs_max]
        if df.empty:
            p = figure(title=f"{title} (no data after diff filter)", sizing_mode="stretch_both")
            if is_show:
                show(p)
            return p

        x = df["Depth1"].to_numpy()
        y = df["DepthDiff"].to_numpy()

        # 2D histogram
        H, xedges, yedges = np.histogram2d(x, y, bins=[bins_x, bins_y])
        H = H.T

        # Flatten into rects
        xs = []
        ys = []
        counts = []
        for i in range(len(xedges) - 1):
            for j in range(len(yedges) - 1):
                c = H[j, i]
                if c <= 0:
                    continue
                xs.append((xedges[i] + xedges[i + 1]) / 2)
                ys.append((yedges[j] + yedges[j + 1]) / 2)
                counts.append(c)

        if not counts:
            p = figure(title=f"{title} (no bins with counts)", sizing_mode="stretch_both")
            if is_show:
                show(p)
            return p

        src = ColumnDataSource(dict(x=xs, y=ys, count=counts))
        mapper = linear_cmap(field_name="count", palette=Viridis256, low=min(counts), high=max(counts))

        p = figure(
            title=title,
            x_axis_label=x_label,
            y_axis_label=y_label,
            tools="pan,wheel_zoom,box_zoom,reset,hover,save",
            sizing_mode="stretch_both",
        )

        # rect size from bin edges
        dx = abs(xedges[1] - xedges[0]) if len(xedges) > 1 else 1
        dy = abs(yedges[1] - yedges[0]) if len(yedges) > 1 else 1

        p.rect(
            x="x",
            y="y",
            width=dx,
            height=dy,
            source=src,
            fill_color=mapper,
            line_color=None,
        )

        color_bar = ColorBar(color_mapper=mapper["transform"], width=8, location=(0, 0))
        p.add_layout(color_bar, "right")

        p.add_tools(HoverTool(tooltips=[("Depth", "@x{0.0}"), ("Diff", "@y{0.0}"), ("Count", "@count{0}")]))
        if is_show:
            show(p)
        return p

    def bokeh_scatter_rov_depth1_vs_depth2_qc(
        self,
        depth_min: float = -2000,
        depth_max: float = -100,
        title: str = "ROV Depth1 vs Depth2 — QC (all points, DepthDiff colored)",
        point_size: int = 5,
        alpha: float = 0.6,
        is_show: bool = False,
        # selection
        file_name: str | None = None,
        file_names: list[str] | None = None,
        file_ids: list[int] | None = None,
        config_fk: int | None = None,
        day: str | None = None,
        start_ts: str | None = None,
        end_ts: str | None = None,
        # shared preloaded data
        data: pd.DataFrame | None = None,
    ):
        # load both ROVs in one go to keep old behavior
        if data is None:
            df = self.load_bbox_data(
                file_name=file_name,
                file_names=file_names,
                file_ids=file_ids,
                config_fk=config_fk,
                day=day,
                start_ts=start_ts,
                end_ts=end_ts,
                columns=[
                    "bb.ROV1_Depth1 AS ROV1_Depth1",
                    "bb.ROV1_Depth2 AS ROV1_Depth2",
                    "bb.ROV2_Depth1 AS ROV2_Depth1",
                    "bb.ROV2_Depth2 AS ROV2_Depth2",
                ],
            )
        else:
            df = data.copy()

        def _build(df_in: pd.DataFrame, a: str, b: str, label: str):
            if a not in df_in.columns or b not in df_in.columns:
                return pd.DataFrame(columns=["Depth1", "Depth2", "DepthDiff", "ROV"])
            tmp = df_in[[a, b]].copy()
            tmp.columns = ["Depth1", "Depth2"]
            tmp["Depth1"] = pd.to_numeric(tmp["Depth1"], errors="coerce")
            tmp["Depth2"] = pd.to_numeric(tmp["Depth2"], errors="coerce")
            tmp = tmp.dropna(subset=["Depth1", "Depth2"])
            tmp = tmp[(tmp["Depth1"].between(depth_min, depth_max)) & (tmp["Depth2"].between(depth_min, depth_max))]
            tmp["DepthDiff"] = tmp["Depth2"] - tmp["Depth1"]
            tmp["ROV"] = label
            return tmp

        df1 = _build(df, "ROV1_Depth1", "ROV1_Depth2", "ROV1")
        df2 = _build(df, "ROV2_Depth1", "ROV2_Depth2", "ROV2")
        all_df = pd.concat([df1, df2], ignore_index=True)

        if all_df.empty:
            p = figure(title=f"{title} (no data)", sizing_mode="stretch_both")
            if is_show:
                show(p)
            return p

        # color by diff
        max_abs = float(np.nanmax(np.abs(all_df["DepthDiff"].to_numpy()))) if len(all_df) else 1.0
        max_abs = max(max_abs, 0.001)

        src = ColumnDataSource(all_df)
        mapper = linear_cmap("DepthDiff", Turbo256, low=-max_abs, high=max_abs)

        p = figure(
            title=title,
            x_axis_label="Depth1 (m)",
            y_axis_label="Depth2 (m)",
            tools="pan,wheel_zoom,box_zoom,reset,hover,save",
            sizing_mode="stretch_both",
        )

        p.scatter(
            x="Depth1",
            y="Depth2",
            size=point_size,
            alpha=alpha,
            source=src,
            fill_color=mapper,
            line_color=None,
            legend_field="ROV",
        )

        # 1:1 line
        p.line([depth_min, depth_max], [depth_min, depth_max], line_dash="dashed", line_width=2)

        color_bar = ColorBar(color_mapper=mapper["transform"], width=8, location=(0, 0))
        p.add_layout(color_bar, "right")

        p.legend.location = "top_left"
        p.legend.click_policy = "hide"

        p.add_tools(
            HoverTool(
                tooltips=[
                    ("ROV", "@ROV"),
                    ("Depth1", "@Depth1{0.0}"),
                    ("Depth2", "@Depth2{0.0}"),
                    ("Diff", "@DepthDiff{0.0}"),
                ]
            )
        )

        if is_show:
            show(p)
        return p

    def _prepare(self, df: pd.DataFrame, depth_min: float, depth_max: float) -> pd.DataFrame:
        df = df.copy()
        df["Depth1"] = pd.to_numeric(df["Depth1"], errors="coerce")
        df["Depth2"] = pd.to_numeric(df["Depth2"], errors="coerce")
        if "T" in df.columns:
            df["T"] = pd.to_datetime(df["T"], errors="coerce")
        df = df.dropna(subset=["Depth1", "Depth2"])
        df = df[(df["Depth1"].between(depth_min, depth_max)) & (df["Depth2"].between(depth_min, depth_max))]
        df["Diff"] = df["Depth2"] - df["Depth1"]
        df["AbsDiff"] = df["Diff"].abs()
        return df

    def _split_ok_err(self, df: pd.DataFrame, diff_thr: float) -> tuple[pd.DataFrame, pd.DataFrame]:
        ok = df[df["AbsDiff"] <= diff_thr].copy()
        err = df[df["AbsDiff"] > diff_thr].copy()
        return ok, err

    def bokeh_compare_sensors_rov(
            self,
            rov_num: int = 1,
            title: str | None = None,
            height: int = 320,
            tools: str = "pan,wheel_zoom,box_zoom,reset,save,hover",
            is_show: bool = False,
            data: "pd.DataFrame | None" = None,
            mode: str = "bland",
            plot: str | None = None,
    ):
        """
        Compare two depth sensors inside ONE ROV.

        Uses:
            ROV{n}_Depth1 / ROV{n}_Depth2
            Depth1_name / Depth2_name
            rov1_name / rov2_name
        """

        # ----------------------------
        # backward compatibility
        # ----------------------------
        if plot is not None:
            mode = str(plot).lower().strip()
        else:
            mode = str(mode).lower().strip()

        # ----------------------------
        # Load data if needed
        # ----------------------------
        if data is None:
            data = self.load_bbox_data()

        df = data.copy()

        col1 = f"ROV{rov_num}_Depth1"
        col2 = f"ROV{rov_num}_Depth2"

        if col1 not in df.columns or col2 not in df.columns:
            raise ValueError(
                f"Missing columns {col1}/{col2}. "
                f"Available: {list(df.columns)}"
            )

        df["Depth1"] = df[col1]
        df["Depth2"] = df[col2]

        df = df.dropna(subset=["Depth1", "Depth2"])
        if df.empty:
            raise ValueError(f"No valid depth data for ROV{rov_num}")

        # =====================================================
        # ✅ Dynamic labels from config
        # =====================================================

        # ROV name
        rov_col = "Rov1Name" if rov_num == 1 else "Rov2Name"
        if rov_col in df.columns and df[rov_col].notna().any():
            rov_label = str(df[rov_col].dropna().iloc[0])
        else:
            rov_label = f"ROV{rov_num}"

        # Sensor names
        if "Depth1Name" in df.columns and df["Depth1Name"].notna().any():
            d1_label = str(df["Depth1Name"].dropna().iloc[0])
        else:
            d1_label = "Depth1"

        if "Depth2Name" in df.columns and df["Depth2Name"].notna().any():
            d2_label = str(df["Depth2Name"].dropna().iloc[0])
        else:
            d2_label = "Depth2"

        # =====================================================
        # Calculations
        # =====================================================
        df["Diff"] = df["Depth2"] - df["Depth1"]
        df["Mean"] = (df["Depth1"] + df["Depth2"]) / 2.0

        src = ColumnDataSource(df)

        if title is None:
            title = f"{rov_label} — {d1_label} vs {d2_label}"

        # =====================================================
        # MODE: SCATTER
        # =====================================================
        if mode in ("scatter", "xy"):

            p = figure(
                title=title,
                height=height,
                tools=tools,
                x_axis_label=d1_label,
                y_axis_label=d2_label,
                active_scroll="wheel_zoom",
                sizing_mode="stretch_width",
            )

            p.circle("Depth1", "Depth2", source=src, size=5, alpha=0.6)

            mn = min(df["Depth1"].min(), df["Depth2"].min())
            mx = max(df["Depth1"].max(), df["Depth2"].max())

            p.line([mn, mx], [mn, mx], line_dash="dashed", line_width=2)

            if is_show:
                show(p)

            return p

        # =====================================================
        # MODE: HIST2D
        # =====================================================
        if mode in ("hist2d", "density"):

            x = df["Mean"].to_numpy()
            y = df["Diff"].to_numpy()

            H, xedges, yedges = np.histogram2d(x, y, bins=40)

            xs = (xedges[:-1] + xedges[1:]) / 2
            ys = (yedges[:-1] + yedges[1:]) / 2

            xs_grid = np.repeat(xs, len(ys))
            ys_grid = np.tile(ys, len(xs))
            vals = H.T.flatten()

            hsrc = ColumnDataSource(dict(x=xs_grid, y=ys_grid, v=vals))

            mapper = LinearColorMapper(
                palette=Turbo256,
                low=float(vals.min()),
                high=float(vals.max()),
            )

            p = figure(
                title=title,
                height=height,
                tools=tools,
                x_axis_label=f"Mean({d1_label}, {d2_label})",
                y_axis_label=f"Diff({d2_label} - {d1_label})",
                active_scroll="wheel_zoom",
                sizing_mode="stretch_width",
            )

            p.rect(
                x="x",
                y="y",
                width=float(xedges[1] - xedges[0]),
                height=float(yedges[1] - yedges[0]),
                source=hsrc,
                fill_color={"field": "v", "transform": mapper},
                line_color=None,
            )

            p.add_layout(ColorBar(color_mapper=mapper), "right")

            if is_show:
                show(p)

            return p

        # =====================================================
        # DEFAULT: BLAND–ALTMAN
        # =====================================================
        p = figure(
            title=title,
            height=height,
            tools=tools,
            x_axis_label=f"Mean({d1_label}, {d2_label})",
            y_axis_label=f"{d2_label} - {d1_label}",
            active_scroll="wheel_zoom",
            sizing_mode="stretch_width",
        )

        p.circle("Mean", "Diff", source=src, size=5, alpha=0.6)

        mu = float(df["Diff"].mean())
        sd = float(df["Diff"].std(ddof=1)) if len(df) > 1 else 0.0

        loa_hi = mu + 1.96 * sd
        loa_lo = mu - 1.96 * sd

        xmin = float(df["Mean"].min())
        xmax = float(df["Mean"].max())

        p.line([xmin, xmax], [mu, mu], line_width=2)
        p.line([xmin, xmax], [loa_hi, loa_hi], line_dash="dashed", line_width=2)
        p.line([xmin, xmax], [loa_lo, loa_lo], line_dash="dashed", line_width=2)

        p.title.text = (
            f"{rov_label} — {d1_label} vs {d2_label} "
            f"(μ={mu:.3f}, σ={sd:.3f})"
        )

        if is_show:
            show(p)

        return p

    def bokeh_bbox_depth12_diff_timeseries(
            self,
            df,  # pd.DataFrame prepared BEFORE call (from load_bbox_data)
            *,
            diff_threshold: float | None = None,
            # e.g. 0.5 (meters). If None -> no threshold split and no fixed y-range
            bins_downsample: int | None = None,  # e.g. 5 -> take each 5th row
            plot_kind: str = "scatter",  # "scatter" or "vbar"
            vbar_width_ms: int = 30_000,  # only for plot_kind="vbar" (datetime width in ms)
            depth_min: float = -3500.0,  # fixed marine depth scale
            depth_max: float = 0.0,
            return_json: bool = False,
            is_show: bool = False,
    ):
        """
        QC-safe depth difference plot (Depth1 - Depth2) for ROV1 and ROV2:
          - 2 stacked figures, common x-axis, ONE toolbar
          - OK series colored by depth (blue-ish), ERR series red
          - Missing values never become 0 (prevents fake diffs)
          - ONE common colorbar (fixed depth range)
          - ONE common legend (controls BOTH plots)
          - ERR series hidden initially
          - If diff_threshold is set: y-range is [-1.5*thr, +1.5*thr] for both plots

        Required imports at module top:
          import numpy as np
          import pandas as pd
          from bokeh.plotting import figure
          from bokeh.models import (
              ColumnDataSource, HoverTool, ColorBar, BasicTicker, PrintfTickFormatter, Span,
              Range1d, Legend, LegendItem
          )
          from bokeh.transform import linear_cmap
          from bokeh.layouts import gridplot
          from bokeh.palettes import Blues256
          from bokeh.io import show
          from bokeh.embed import json_item
        """

        # ---- empty / validation ----
        if df is None or len(df) == 0 or "T" not in df.columns:
            p_empty1 = figure(
                title="ROV1 — no data",
                x_axis_type="datetime",
                sizing_mode="stretch_both",
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )
            p_empty2 = figure(
                title="ROV2 — no data",
                x_axis_type="datetime",
                x_range=p_empty1.x_range,
                sizing_mode="stretch_both",
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )
            layout = gridplot([[p_empty1], [p_empty2]], merge_tools=True, toolbar_location="above",
                              sizing_mode="stretch_both")
            if is_show:
                show(layout)
                return layout
            if return_json:
                return json_item(layout)
            return layout

        # ---- copy + dedupe columns ----
        df = df.copy()
        df = df.loc[:, ~df.columns.duplicated()]

        # ---- timestamps ----
        if not np.issubdtype(df["T"].dtype, np.datetime64):
            df["T"] = pd.to_datetime(df["T"], errors="coerce")
        df = df.dropna(subset=["T"]).copy()

        # ---- optional downsample ----
        if bins_downsample and int(bins_downsample) > 1:
            df = df.iloc[::int(bins_downsample), :].copy()

        # ---- QC-safe numeric coercion (NO fillna(0)) ----
        num_cols = [
            "ROV1_Depth", "ROV1_Depth1", "ROV1_Depth2",
            "ROV2_Depth", "ROV2_Depth1", "ROV2_Depth2",
        ]
        for c in num_cols:
            if c in df.columns:
                col = df[c]
                if hasattr(col, "ndim") and col.ndim == 2:
                    col = col.iloc[:, 0]
                df[c] = pd.to_numeric(col, errors="coerce")

        # ---- compute diffs ONLY where both sensors valid ----
        m1_valid = df.get("ROV1_Depth1", pd.Series(index=df.index, dtype=float)).notna() & \
                   df.get("ROV1_Depth2", pd.Series(index=df.index, dtype=float)).notna()
        m2_valid = df.get("ROV2_Depth1", pd.Series(index=df.index, dtype=float)).notna() & \
                   df.get("ROV2_Depth2", pd.Series(index=df.index, dtype=float)).notna()

        df["ROV1_D12_Diff"] = np.nan
        df.loc[m1_valid, "ROV1_D12_Diff"] = df.loc[m1_valid, "ROV1_Depth1"] - df.loc[m1_valid, "ROV1_Depth2"]

        df["ROV2_D12_Diff"] = np.nan
        df.loc[m2_valid, "ROV2_D12_Diff"] = df.loc[m2_valid, "ROV2_Depth1"] - df.loc[m2_valid, "ROV2_Depth2"]

        # ---- labels from config (fallbacks) ----
        def _first_nonnull(col_name: str, fallback: str) -> str:
            if col_name in df.columns and df[col_name].notna().any():
                v = df[col_name].dropna().iloc[0]
                s = str(v).strip()
                return s if s else fallback
            return fallback

        rov1_label = _first_nonnull("Rov1Name", "ROV1")
        rov2_label = _first_nonnull("Rov2Name", "ROV2")
        depth1_label = _first_nonnull("Depth1Name", "Depth1")
        depth2_label = _first_nonnull("Depth2Name", "Depth2")

        # ---- threshold split (OK vs ERR) ----
        thr = None if diff_threshold is None else float(diff_threshold)

        df["ROV1_D12_OK"] = np.nan
        df["ROV1_D12_ERR"] = np.nan
        df["ROV2_D12_OK"] = np.nan
        df["ROV2_D12_ERR"] = np.nan

        if thr is None:
            df.loc[m1_valid, "ROV1_D12_OK"] = df.loc[m1_valid, "ROV1_D12_Diff"]
            df.loc[m2_valid, "ROV2_D12_OK"] = df.loc[m2_valid, "ROV2_D12_Diff"]
        else:
            m1_err = m1_valid & (df["ROV1_D12_Diff"].abs() > thr)
            m2_err = m2_valid & (df["ROV2_D12_Diff"].abs() > thr)

            m1_ok = m1_valid & ~m1_err
            m2_ok = m2_valid & ~m2_err

            df.loc[m1_ok, "ROV1_D12_OK"] = df.loc[m1_ok, "ROV1_D12_Diff"]
            df.loc[m1_err, "ROV1_D12_ERR"] = df.loc[m1_err, "ROV1_D12_Diff"]

            df.loc[m2_ok, "ROV2_D12_OK"] = df.loc[m2_ok, "ROV2_D12_Diff"]
            df.loc[m2_err, "ROV2_D12_ERR"] = df.loc[m2_err, "ROV2_D12_Diff"]

        # ---- bokeh sources ----
        src1 = ColumnDataSource(df)
        src2 = ColumnDataSource(df)

        tools = "pan,wheel_zoom,box_zoom,reset,save,hover"

        # ---- fixed y-range based on threshold ----
        y_rng = None
        thr_txt = ""
        if thr is not None and thr > 0:
            lim = 1.5 * thr
            y_rng = Range1d(-lim, +lim)
            thr_txt = f"  (thr ±{thr:g})"

        fig_kwargs_1 = dict(
            x_axis_type="datetime",
            sizing_mode="stretch_both",
            tools=tools,
            active_scroll="wheel_zoom",
        )
        fig_kwargs_2 = dict(
            x_axis_type="datetime",
            sizing_mode="stretch_both",
            tools=tools,
            active_scroll="wheel_zoom",
        )
        if y_rng is not None:
            fig_kwargs_1["y_range"] = y_rng
            fig_kwargs_2["y_range"] = y_rng

        p1 = figure(title=f"{rov1_label} — {depth1_label} - {depth2_label}{thr_txt}", **fig_kwargs_1)
        p2 = figure(title=f"{rov2_label} — {depth1_label} - {depth2_label}{thr_txt}", x_range=p1.x_range,
                    **fig_kwargs_2)

        p1.yaxis.axis_label = f"{depth1_label} - {depth2_label}"
        p2.yaxis.axis_label = f"{depth1_label} - {depth2_label}"
        p2.xaxis.axis_label = "Time"

        # zero + threshold guide lines
        p1.add_layout(Span(location=0.0, dimension="width"))
        p2.add_layout(Span(location=0.0, dimension="width"))
        if thr is not None and thr > 0:
            for p in (p1, p2):
                p.add_layout(Span(location=+thr, dimension="width", line_dash="dashed"))
                p.add_layout(Span(location=-thr, dimension="width", line_dash="dashed"))

        # ---- fixed marine depth colormap (deep=dark blue, shallow=light blue) ----
        #blue_palette = Blues256[::-1]
        blue_palette = Turbo256[::-1]
        cmap_rov1 = linear_cmap("ROV1_Depth", blue_palette, low=float(depth_min), high=float(depth_max))
        cmap_rov2 = linear_cmap("ROV2_Depth", blue_palette, low=float(depth_min), high=float(depth_max))

        plot_kind = (plot_kind or "scatter").lower().strip()
        if plot_kind not in ("scatter", "vbar"):
            plot_kind = "scatter"

        # ---- render OK + ERR (NO legend_label here; we'll build ONE combined legend manually) ----
        if plot_kind == "vbar":
            r1_ok = p1.vbar(x="T", top="ROV1_D12_OK", width=vbar_width_ms, source=src1, fill_color=cmap_rov1,
                            line_color=None, alpha=0.9)
            r1_er = p1.vbar(x="T", top="ROV1_D12_ERR", width=vbar_width_ms, source=src1, fill_color="red",
                            line_color=None, alpha=0.9)

            r2_ok = p2.vbar(x="T", top="ROV2_D12_OK", width=vbar_width_ms, source=src2, fill_color=cmap_rov2,
                            line_color=None, alpha=0.9)
            r2_er = p2.vbar(x="T", top="ROV2_D12_ERR", width=vbar_width_ms, source=src2, fill_color="red",
                            line_color=None, alpha=0.9)
        else:
            r1_ok = p1.scatter(x="T", y="ROV1_D12_OK", source=src1, size=6, fill_color=cmap_rov1, line_color=None,
                               alpha=0.85)
            r1_er = p1.scatter(x="T", y="ROV1_D12_ERR", source=src1, size=8, fill_color="red", line_color=None,
                               alpha=0.95)

            r2_ok = p2.scatter(x="T", y="ROV2_D12_OK", source=src2, size=6, fill_color=cmap_rov2, line_color=None,
                               alpha=0.85)
            r2_er = p2.scatter(x="T", y="ROV2_D12_ERR", source=src2, size=8, fill_color="red", line_color=None,
                               alpha=0.95)

        # hide ERR at start (both plots)
        r1_er.visible = False
        r2_er.visible = False

        # ---- ONE common legend for BOTH plots ----
        leg = Legend(
            items=[
                LegendItem(label="OK", renderers=[r1_ok, r2_ok]),
                LegendItem(label="ERROR", renderers=[r1_er, r2_er]),
            ],
            click_policy="hide",
            location="top_left",
        )
        leg.orientation='horizontal'
        p1.add_layout(leg, "above")  # put legend once on p1
        p2.legend.visible = False  # ensure no auto legend shows up

        # ---- hover (attach to OK+ERR so red points show tooltips too) ----
        h1 = p1.select_one(HoverTool)
        if h1:
            h1.tooltips = [
                ("Time", "@T{%F %T}"),
                ("File", "@FileName"),
                (f"{rov1_label} Depth", "@ROV1_Depth{0.00}"),
                (f"{depth1_label}", "@ROV1_Depth1{0.00}"),
                (f"{depth2_label}", "@ROV1_Depth2{0.00}"),
                ("Diff", "@ROV1_D12_Diff{0.00}"),
            ]
            h1.formatters = {"@T": "datetime"}
            h1.renderers = [r1_ok, r1_er]

        h2 = p2.select_one(HoverTool)
        if h2:
            h2.tooltips = [
                ("Time", "@T{%F %T}"),
                ("File", "@FileName"),
                (f"{rov2_label} Depth", "@ROV2_Depth{0.00}"),
                (f"{depth1_label}", "@ROV2_Depth1{0.00}"),
                (f"{depth2_label}", "@ROV2_Depth2{0.00}"),
                ("Diff", "@ROV2_D12_Diff{0.00}"),
            ]
            h2.formatters = {"@T": "datetime"}
            h2.renderers = [r2_ok, r2_er]

        # ---- ONE common colorbar (attach to p1 only) ----
        common_colorbar = ColorBar(
            color_mapper=cmap_rov1["transform"],
            ticker=BasicTicker(desired_num_ticks=8),
            formatter=PrintfTickFormatter(format="%.0f"),
            label_standoff=8,
            title="Primary Depth (m)",
        )
        p2.add_layout(common_colorbar, "below")

        # ---- layout (ONE toolbar + stretch) ----
        layout = gridplot(
            [[p1], [p2]],
            merge_tools=True,
            toolbar_location="above",
            sizing_mode="stretch_both",
        )

        # ---- return logic ----
        if is_show:
            show(layout)
            return layout
        if return_json:
            return json_item(layout)
        return layout

    def bokeh_bbox_sog_timeseries(
            self,
            df,
            *,
            bins_downsample: int | None = None,
            plot_kind: str = "line",
            vbar_width_ms: int = 30_000,
            return_json: bool = False,
            is_show: bool = False,
    ):
        """
        Plot Vessel SOG, ROV1 SOG, ROV2 SOG vs Time (knots)
        Legend includes min / max / avg values.
        """

        if df is None or len(df) == 0 or "T" not in df.columns:
            p = figure(
                title="SOG — no data",
                x_axis_type="datetime",
                sizing_mode="stretch_both",
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )
            if is_show:
                show(p)
                return p
            return json_item(p) if return_json else p

        df = df.copy()
        df = df.loc[:, ~df.columns.duplicated()]

        if not np.issubdtype(df["T"].dtype, np.datetime64):
            df["T"] = pd.to_datetime(df["T"], errors="coerce")
        df = df.dropna(subset=["T"]).copy()

        if bins_downsample and int(bins_downsample) > 1:
            df = df.iloc[::int(bins_downsample), :].copy()

        sog_cols = ["VesselSOG", "ROV1_SOG", "ROV2_SOG"]

        for c in sog_cols:
            if c in df.columns:
                col = df[c]
                if hasattr(col, "ndim") and col.ndim == 2:
                    col = col.iloc[:, 0]
                df[c] = pd.to_numeric(col, errors="coerce")

        # ---- statistics function ----
        def _stats(series):
            s = pd.to_numeric(series, errors="coerce")
            s = s.dropna()
            if len(s) == 0:
                return 0.0, 0.0, 0.0
            return float(s.min()), float(s.max()), float(s.mean())

        vessel_min, vessel_max, vessel_avg = _stats(df.get("VesselSOG"))
        rov1_min, rov1_max, rov1_avg = _stats(df.get("ROV1_SOG"))
        rov2_min, rov2_max, rov2_avg = _stats(df.get("ROV2_SOG"))

        # ---- labels ----
        def _first_nonnull(col_name: str, fallback: str) -> str:
            if col_name in df.columns and df[col_name].notna().any():
                v = df[col_name].dropna().iloc[0]
                s = str(v).strip()
                return s if s else fallback
            return fallback

        vessel_label = _first_nonnull("VesselName", "Vessel")
        rov1_label = _first_nonnull("Rov1Name", "ROV1")
        rov2_label = _first_nonnull("Rov2Name", "ROV2")

        # ---- legend labels with stats ----
        vessel_legend = f"{vessel_label}  min:{vessel_min:.2f}  max:{vessel_max:.2f}  avg:{vessel_avg:.2f}"
        rov1_legend = f"{rov1_label} SOG  min:{rov1_min:.2f}  max:{rov1_max:.2f}  avg:{rov1_avg:.2f}"
        rov2_legend = f"{rov2_label} SOG  min:{rov2_min:.2f}  max:{rov2_max:.2f}  avg:{rov2_avg:.2f}"

        src = ColumnDataSource(df)

        p = figure(
            title="SOG vs Time (knots)",
            x_axis_type="datetime",
            sizing_mode="stretch_both",
            tools="pan,wheel_zoom,box_zoom,reset,save,hover",
            y_range=Range1d(0, 15),
            active_scroll="wheel_zoom",
        )
        p.yaxis.axis_label = "SOG (knots)"
        p.xaxis.axis_label = "Time"

        plot_kind = (plot_kind or "line").lower().strip()
        if plot_kind not in ("line", "scatter", "vbar"):
            plot_kind = "line"

        c0, c1, c2 = Category10[3][0], Category10[3][1], Category10[3][2]

        renderers = []

        if plot_kind == "vbar":
            if "VesselSOG" in df.columns:
                renderers.append(
                    p.vbar(x="T", top="VesselSOG", width=vbar_width_ms, source=src, fill_color=c0, line_color=None,
                           alpha=0.8, legend_label=vessel_legend))
            if "ROV1_SOG" in df.columns:
                renderers.append(
                    p.vbar(x="T", top="ROV1_SOG", width=vbar_width_ms, source=src, fill_color=c1, line_color=None,
                           alpha=0.8, legend_label=rov1_legend))
            if "ROV2_SOG" in df.columns:
                renderers.append(
                    p.vbar(x="T", top="ROV2_SOG", width=vbar_width_ms, source=src, fill_color=c2, line_color=None,
                           alpha=0.8, legend_label=rov2_legend))

        elif plot_kind == "scatter":
            if "VesselSOG" in df.columns:
                renderers.append(p.scatter(x="T", y="VesselSOG", source=src, size=5, color=c0, alpha=0.85,
                                           legend_label=vessel_legend))
            if "ROV1_SOG" in df.columns:
                renderers.append(
                    p.scatter(x="T", y="ROV1_SOG", source=src, size=5, color=c1, alpha=0.85, legend_label=rov1_legend))
            if "ROV2_SOG" in df.columns:
                renderers.append(
                    p.scatter(x="T", y="ROV2_SOG", source=src, size=5, color=c2, alpha=0.85, legend_label=rov2_legend))

        else:
            if "VesselSOG" in df.columns:
                renderers.append(p.line(x="T", y="VesselSOG", source=src, line_width=2, color=c0, alpha=0.95,
                                        legend_label=vessel_legend))
            if "ROV1_SOG" in df.columns:
                renderers.append(p.line(x="T", y="ROV1_SOG", source=src, line_width=2, color=c1, alpha=0.95,
                                        legend_label=rov1_legend))
            if "ROV2_SOG" in df.columns:
                renderers.append(p.line(x="T", y="ROV2_SOG", source=src, line_width=2, color=c2, alpha=0.95,
                                        legend_label=rov2_legend))

        p.legend.click_policy = "hide"
        p.legend.location = "top_right"

        h = p.select_one(HoverTool)
        if h:
            h.tooltips = [
                ("Time", "@T{%F %T}"),
                (vessel_label, "@VesselSOG{0.00} kn"),
                (f"{rov1_label} SOG", "@ROV1_SOG{0.00} kn"),
                (f"{rov2_label} SOG", "@ROV2_SOG{0.00} kn"),
            ]
            h.formatters = {"@T": "datetime"}
            if renderers:
                h.renderers = renderers

        if is_show:
            show(p)
            return p

        if return_json:
            return json_item(p)

        return p

    def bokeh_bbox_gnss_hdop_timeseries(
            self,
            df,
            *,
            bins_downsample: int | None = None,
            hdop_good_max: float = 2.0,
            hdop_warn_max: float = 4.0,
            return_json: bool = False,
            is_show: bool = False,
    ):
        """
        Plot GNSS1_HDOP and GNSS2_HDOP vs Time.

        - X axis: Time
        - Y axis: HDOP
        - QC threshold lines
        - Legend includes min/max/avg
        """

        if df is None or len(df) == 0 or "T" not in df.columns:
            p = figure(
                title="GNSS HDOP — no data",
                x_axis_type="datetime",
                sizing_mode="stretch_both",
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )
            if is_show:
                show(p)
                return p
            return json_item(p) if return_json else p

        df = df.copy()
        df = df.loc[:, ~df.columns.duplicated()]

        if not np.issubdtype(df["T"].dtype, np.datetime64):
            df["T"] = pd.to_datetime(df["T"], errors="coerce")

        for c in ["GNSS1_HDOP", "GNSS2_HDOP"]:
            if c in df.columns:
                col = df[c]
                if hasattr(col, "ndim") and col.ndim == 2:
                    col = col.iloc[:, 0]
                df[c] = pd.to_numeric(col, errors="coerce")

        df = df.dropna(subset=["T"]).copy()

        if bins_downsample and int(bins_downsample) > 1:
            df = df.iloc[::int(bins_downsample), :].copy()

        def _first_nonnull(col_name: str, fallback: str) -> str:
            if col_name in df.columns and df[col_name].notna().any():
                v = df[col_name].dropna().iloc[0]
                s = str(v).strip()
                return s if s else fallback
            return fallback

        g1 = _first_nonnull("Gnss1Name", "GNSS1")
        g2 = _first_nonnull("Gnss2Name", "GNSS2")

        def _stats(series):
            s = pd.to_numeric(series, errors="coerce").dropna()
            if len(s) == 0:
                return 0.0, 0.0, 0.0
            return float(s.min()), float(s.max()), float(s.mean())

        g1_min, g1_max, g1_avg = _stats(df.get("GNSS1_HDOP"))
        g2_min, g2_max, g2_avg = _stats(df.get("GNSS2_HDOP"))

        g1_label = f"{g1}  min:{g1_min:.2f}  max:{g1_max:.2f}  avg:{g1_avg:.2f}"
        g2_label = f"{g2}  min:{g2_min:.2f}  max:{g2_max:.2f}  avg:{g2_avg:.2f}"

        src = ColumnDataSource(df)

        p = figure(
            title="GNSS HDOP vs Time",
            x_axis_type="datetime",
            sizing_mode="stretch_both",
            tools="pan,wheel_zoom,box_zoom,reset,save,hover",
            active_scroll="wheel_zoom",
            y_axis_label="HDOP",
            x_axis_label="Time",
        )

        r1 = p.line(
            x="T",
            y="GNSS1_HDOP",
            source=src,
            line_width=2,
            color=Category10[3][0],
            alpha=0.95,
            legend_label=g1_label,
        )

        r2 = p.line(
            x="T",
            y="GNSS2_HDOP",
            source=src,
            line_width=2,
            color=Category10[3][1],
            alpha=0.95,
            legend_label=g2_label,
        )

        # QC threshold lines
        p.add_layout(Span(location=hdop_good_max, dimension="width", line_dash="dashed"))
        p.add_layout(Span(location=hdop_warn_max, dimension="width", line_dash="dashed"))

        p.legend.location = "top_left"
        p.legend.click_policy = "hide"

        h = p.select_one(HoverTool)
        if h:
            h.tooltips = [
                ("Time", "@T{%F %T}"),
                (g1, "@GNSS1_HDOP{0.00}"),
                (g2, "@GNSS2_HDOP{0.00}"),
            ]
            h.formatters = {"@T": "datetime"}
            h.renderers = [r1, r2]

        if is_show:
            show(p)
            return p

        if return_json:
            return json_item(p)

        return p

    def bokeh_polar_qc_cog(
            self,
            df,
            *,
            cog_col: str = "VesselCOG",  # or "ROV1_COG", "ROV2_COG"
            sog_col: str = "VesselSOG",  # for optional coloring/size
            label: str | None = None,
            bins_downsample: int | None = None,
            plot_kind: str = "scatter",  # "scatter" or "vbar"
            vbar_bin_deg: int = 5,  # only for plot_kind="vbar" (histogram bins)
            r_max: float | None = None,  # radial max (knots). None -> auto
            return_json: bool = True,
            is_show: bool = False,
    ):
        """
        Polar QC plot for direction of motion:
          - scatter: points at angle=COG, radius=SOG (knots)
          - vbar: circular histogram of COG counts (radius=count)

        Notes:
          - Bokeh is cartesian; we convert polar->x,y.
          - Requires df columns: cog_col, and (for scatter) sog_col.

        Required imports at module top:
          import numpy as np
          import pandas as pd
          from bokeh.plotting import figure
          from bokeh.models import ColumnDataSource, HoverTool
          from bokeh.embed import json_item
          from bokeh.io import show
        """

        # ---- validate ----
        if df is None or len(df) == 0 or cog_col not in df.columns:
            p = figure(title="Polar COG — no data", sizing_mode="stretch_both", tools="pan,wheel_zoom,reset,save")
            if is_show:
                show(p)
                return p
            return json_item(p) if return_json else p

        df = df.copy()
        df = df.loc[:, ~df.columns.duplicated()]

        if bins_downsample and int(bins_downsample) > 1:
            df = df.iloc[::int(bins_downsample), :].copy()

        # numeric coercion
        def _to_num(colname):
            if colname not in df.columns:
                return
            col = df[colname]
            if hasattr(col, "ndim") and col.ndim == 2:
                col = col.iloc[:, 0]
            df[colname] = pd.to_numeric(col, errors="coerce")

        _to_num(cog_col)
        _to_num(sog_col)

        # clean
        if plot_kind == "scatter":
            df = df.dropna(subset=[cog_col, sog_col]).copy()
        else:
            df = df.dropna(subset=[cog_col]).copy()

        if df.empty:
            p = figure(title="Polar COG — no valid rows", sizing_mode="stretch_both", tools="pan,wheel_zoom,reset,save")
            if is_show:
                show(p)
                return p
            return json_item(p) if return_json else p

        # label
        if label is None:
            label = cog_col

        # normalize angles to [0,360)
        ang = (df[cog_col] % 360.0).astype(float)

        plot_kind = (plot_kind or "scatter").lower().strip()
        if plot_kind not in ("scatter", "vbar"):
            plot_kind = "scatter"

        if plot_kind == "vbar":
            # --- circular histogram in polar: radius = count per angle bin ---
            bin_deg = max(1, int(vbar_bin_deg))
            edges = np.arange(0, 360 + bin_deg, bin_deg)
            cats = pd.cut(ang, bins=edges, include_lowest=True, right=False)
            counts = cats.value_counts().sort_index()

            # bin centers in degrees
            centers = np.array([i.left + bin_deg / 2 for i in counts.index], dtype=float)
            r = counts.values.astype(float)

            # polar->xy for bar tips
            theta = np.deg2rad(90.0 - centers)  # 0° at North, clockwise
            x_tip = r * np.cos(theta)
            y_tip = r * np.sin(theta)

            src = ColumnDataSource(dict(
                center_deg=centers,
                count=r,
                x_tip=x_tip,
                y_tip=y_tip,
            ))

            # set plot limits
            rr_max = float(r.max()) if len(r) else 1.0
            lim = rr_max * 1.15
            p = figure(
                title=f"Polar QC — {label} (COG histogram, bin {bin_deg}°)",
                sizing_mode="stretch_both",
                x_range=(-lim, lim),
                y_range=(-lim, lim),
                tools="pan,wheel_zoom,reset,save,hover",
                active_scroll="wheel_zoom",
            )
            p.axis.visible = False
            p.grid.visible = False

            # radial rings
            for k in range(1, 5):
                rr = rr_max * k / 4
                p.circle(x=0, y=0, radius=rr, fill_color=None, line_alpha=0.2)

            # spokes every 30°
            for deg in range(0, 360, 30):
                t = np.deg2rad(90.0 - deg)
                p.line([0, lim * np.cos(t)], [0, lim * np.sin(t)], line_alpha=0.15)

            # bars as rays (segments)
            seg = p.segment(x0=0, y0=0, x1="x_tip", y1="y_tip", source=src, line_width=3, line_alpha=0.9)

            h = p.select_one(HoverTool)
            if h:
                h.tooltips = [("COG bin (deg)", "@center_deg{0}"), ("Count", "@count{0}")]
                h.renderers = [seg]

        else:
            # --- scatter in polar: radius=SOG, angle=COG ---
            sog = df[sog_col].astype(float)

            theta = np.deg2rad(90.0 - ang.to_numpy())  # 0° at North, clockwise
            r = sog.to_numpy()

            x = r * np.cos(theta)
            y = r * np.sin(theta)

            df["_x"] = x
            df["_y"] = y
            df["_ang"] = ang
            df["_r"] = r

            rr_max = float(np.nanmax(r)) if r_max is None else float(r_max)
            if not np.isfinite(rr_max) or rr_max <= 0:
                rr_max = 1.0
            lim = rr_max * 1.15

            src = ColumnDataSource(df)

            p = figure(
                title=f"Polar QC — {label} (radius={sog_col} kn, angle={cog_col}°)",
                sizing_mode="stretch_both",
                x_range=(-lim, lim),
                y_range=(-lim, lim),
                tools="pan,wheel_zoom,reset,save,hover",
                active_scroll="wheel_zoom",
            )
            p.axis.visible = False
            p.grid.visible = False

            # radial rings
            for k in range(1, 6):
                rr = rr_max * k / 5
                p.circle(x=0, y=0, radius=rr, fill_color=None, line_alpha=0.2)

            # spokes every 30°
            for deg in range(0, 360, 30):
                t = np.deg2rad(90.0 - deg)
                p.line([0, lim * np.cos(t)], [0, lim * np.sin(t)], line_alpha=0.15)

            pts = p.scatter(x="_x", y="_y", source=src, size=5, alpha=0.6)

            h = p.select_one(HoverTool)
            if h:
                h.tooltips = [
                    ("COG (deg)", "@_ang{0.0}"),
                    (f"{sog_col} (kn)", "@_r{0.00}"),
                    ("Time", "@T{%F %T}"),
                ]
                h.formatters = {"@T": "datetime"}
                h.renderers = [pts]

        if is_show:
            show(p)
            return p

        if return_json:
            return json_item(p)

        return p

    def plotly_polar_qc_cog(
            self,
            df,
            *,
            cog_col: str = "VesselCOG",  # or "ROV1_COG", "ROV2_COG"
            sog_col: str = "VesselSOG",  # radius (knots)
            label: str | None = None,
            bins_downsample: int | None = None,
            plot_kind: str = "scatter",  # "scatter" or "hist"
            hist_bin_deg: int = 5,  # for plot_kind="hist"
            r_max: float | None = None,  # radial max (knots) for scatter. None -> auto
            show_fig: bool = False,
    ):
        """
        Plotly polar QC plot for direction of motion.

        - scatter: theta=COG (deg), r=SOG (knots)
        - hist: polar histogram of COG counts (r=count)

        Expected df columns:
          - cog_col
          - (for scatter) sog_col
          - optional: T for hover

        Required imports at module top:
          import numpy as np
          import pandas as pd
          import plotly.graph_objects as go
        """

        if df is None or len(df) == 0 or cog_col not in df.columns:
            fig = go.Figure()
            fig.update_layout(title="Polar COG — no data")
            if show_fig:
                fig.show()
            return fig

        df = df.copy()
        df = df.loc[:, ~df.columns.duplicated()]

        if bins_downsample and int(bins_downsample) > 1:
            df = df.iloc[::int(bins_downsample), :].copy()

        # numeric coercion
        def _to_num(colname):
            if colname not in df.columns:
                return
            col = df[colname]
            if hasattr(col, "ndim") and col.ndim == 2:
                col = col.iloc[:, 0]
            df[colname] = pd.to_numeric(col, errors="coerce")

        _to_num(cog_col)
        _to_num(sog_col)

        plot_kind = (plot_kind or "scatter").lower().strip()
        if plot_kind not in ("scatter", "hist"):
            plot_kind = "scatter"

        if label is None:
            label = cog_col

        # normalize angles to [0,360)
        df[cog_col] = (df[cog_col] % 360.0)

        if plot_kind == "hist":
            # polar histogram (counts vs angle bins)
            dfh = df.dropna(subset=[cog_col]).copy()
            if dfh.empty:
                fig = go.Figure()
                fig.update_layout(title="Polar COG — no valid angles")
                if show_fig:
                    fig.show()
                return fig

            bin_deg = max(1, int(hist_bin_deg))
            edges = np.arange(0, 360 + bin_deg, bin_deg)

            cats = pd.cut(dfh[cog_col], bins=edges, include_lowest=True, right=False)
            counts = cats.value_counts().sort_index()

            centers = np.array([i.left + bin_deg / 2 for i in counts.index], dtype=float)
            r = counts.values.astype(int)

            fig = go.Figure()
            fig.add_trace(
                go.Barpolar(
                    r=r,
                    theta=centers,
                    width=[bin_deg] * len(centers),
                    name="COG count",
                    opacity=0.85,
                    hovertemplate="COG bin: %{theta:.0f}°<br>Count: %{r}<extra></extra>",
                )
            )

            fig.update_layout(
                title=f"Polar QC — {label} (COG histogram, bin {bin_deg}°)",
                polar=dict(
                    angularaxis=dict(direction="clockwise", rotation=90),  # 0° at North
                ),
                showlegend=False,
            )

        else:
            # scatter polar (r=SOG, theta=COG)
            dfs = df.dropna(subset=[cog_col, sog_col]).copy()
            if dfs.empty:
                fig = go.Figure()
                fig.update_layout(title="Polar COG — no valid COG/SOG")
                if show_fig:
                    fig.show()
                return fig

            theta = dfs[cog_col].astype(float).to_numpy()
            r = dfs[sog_col].astype(float).to_numpy()

            # hover text
            hover = []
            has_t = "T" in dfs.columns
            if has_t:
                # ensure stringable
                tt = dfs["T"].astype(str).to_numpy()
                for a, sp, t in zip(theta, r, tt):
                    hover.append(f"COG: {a:.1f}°<br>SOG: {sp:.2f} kn<br>Time: {t}")
            else:
                for a, sp in zip(theta, r):
                    hover.append(f"COG: {a:.1f}°<br>SOG: {sp:.2f} kn")

            # r range
            if r_max is None:
                rr_max = float(np.nanmax(r)) if len(r) else 1.0
            else:
                rr_max = float(r_max)
            if not np.isfinite(rr_max) or rr_max <= 0:
                rr_max = 1.0

            fig = go.Figure()
            fig.add_trace(
                go.Scatterpolar(
                    r=r,
                    theta=theta,
                    mode="markers",
                    name=label,
                    marker=dict(size=6, opacity=0.65),
                    text=hover,
                    hoverinfo="text",
                )
            )

            fig.update_layout(
                title=f"Polar QC — {label} (radius={sog_col} kn, angle={cog_col}°)",
                polar=dict(
                    radialaxis=dict(range=[0, rr_max]),
                    angularaxis=dict(direction="clockwise", rotation=90),  # 0° at North
                ),
                showlegend=False,
            )

        if show_fig:
            fig.show()
        return fig

    def plotly_polar_hdg_cog_time_spiral(
            self,
            df,
            *,
            time_col: str = "T",  # "T" from your load_bbox_data (bb.TimeStamp AS T)
            hdg_col: str = "VesselHDG",  # or "ROV1_HDG", "ROV2_HDG"
            cog_col: str = "VesselCOG",  # or "ROV1_COG", "ROV2_COG"
            label_hdg: str = "HDG",
            label_cog: str = "COG",
            bins_downsample: int | None = None,
            r_unit: str = "minutes",  # "seconds" | "minutes" | "hours"
            show_markers: bool = True,
            show_fig: bool = False,
    ):
        """
        Plotly polar "time spiral":
          - Radius (r): time since first sample (in seconds/minutes/hours)
          - Angle (theta): HDG and COG (degrees 0..360)
          - Two series: HDG and COG

        Expected df columns:
          - time_col (datetime-like)
          - hdg_col, cog_col (degrees)

        Required imports at module top:
          import numpy as np
          import pandas as pd
          import plotly.graph_objects as go
        """

        fig = go.Figure()

        if df is None or len(df) == 0 or time_col not in df.columns:
            fig.update_layout(title="Polar Time Spiral — no data")
            if show_fig:
                fig.show()
            return fig

        df = df.copy()
        df = df.loc[:, ~df.columns.duplicated()]

        # time -> datetime
        if not np.issubdtype(df[time_col].dtype, np.datetime64):
            df[time_col] = pd.to_datetime(df[time_col], errors="coerce")

        # numeric coercion for angles
        for c in (hdg_col, cog_col):
            if c in df.columns:
                col = df[c]
                if hasattr(col, "ndim") and col.ndim == 2:
                    col = col.iloc[:, 0]
                df[c] = pd.to_numeric(col, errors="coerce")

        # drop invalid time
        df = df.dropna(subset=[time_col]).copy()
        if df.empty:
            fig.update_layout(title="Polar Time Spiral — no valid timestamps")
            if show_fig:
                fig.show()
            return fig

        # optional downsample
        if bins_downsample and int(bins_downsample) > 1:
            df = df.iloc[::int(bins_downsample), :].copy()

        # radius: elapsed time
        t0 = df[time_col].iloc[0]
        dt_s = (df[time_col] - t0).dt.total_seconds()

        r_unit = (r_unit or "minutes").lower().strip()
        if r_unit == "seconds":
            r = dt_s.to_numpy()
            r_label = "Time (s)"
        elif r_unit == "hours":
            r = (dt_s / 3600.0).to_numpy()
            r_label = "Time (h)"
        else:
            r = (dt_s / 60.0).to_numpy()
            r_label = "Time (min)"

        # angles normalized to [0,360)
        def _ang(series):
            a = pd.to_numeric(series, errors="coerce")
            return (a % 360.0).to_numpy()

        mode = "lines+markers" if show_markers else "lines"

        # HDG trace
        if hdg_col in df.columns:
            m = df[hdg_col].notna()
            theta_hdg = _ang(df.loc[m, hdg_col])
            r_hdg = r[m.to_numpy()]

            hover_hdg = (
                "Time: %{customdata}<br>"
                f"{label_hdg}: %{{theta:.1f}}°<br>"
                f"{r_label}: %{{r:.2f}}<extra></extra>"
            )
            fig.add_trace(
                go.Scatterpolar(
                    r=r_hdg,
                    theta=theta_hdg,
                    mode=mode,
                    name=label_hdg,
                    customdata=df.loc[m, time_col].astype(str).to_numpy(),
                    hovertemplate=hover_hdg,
                )
            )

        # COG trace
        if cog_col in df.columns:
            m = df[cog_col].notna()
            theta_cog = _ang(df.loc[m, cog_col])
            r_cog = r[m.to_numpy()]

            hover_cog = (
                "Time: %{customdata}<br>"
                f"{label_cog}: %{{theta:.1f}}°<br>"
                f"{r_label}: %{{r:.2f}}<extra></extra>"
            )
            fig.add_trace(
                go.Scatterpolar(
                    r=r_cog,
                    theta=theta_cog,
                    mode=mode,
                    name=label_cog,
                    customdata=df.loc[m, time_col].astype(str).to_numpy(),
                    hovertemplate=hover_cog,
                )
            )

        # layout: 0° at North, clockwise like marine headings
        fig.update_layout(
            title=f"Polar Time Spiral — {label_hdg} & {label_cog} (radius = {r_label})",
            polar=dict(
                angularaxis=dict(direction="clockwise", rotation=90),
                radialaxis=dict(title=r_label),
            ),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        )

        if show_fig:
            fig.show()

        return fig

    def bokeh_cog_hdg_drift_rose_qc(
            self,
            df,
            *,
            time_col: str = "T",
            cog_col: str = "VesselCOG",
            hdg_col: str = "VesselHDG",
            sog_col: str = "VesselSOG",
            label_prefix: str = "Vessel",
            sog_min_for_rose: float = 1.0,
            drift_warn_deg: float = 5.0,
            drift_bad_deg: float = 10.0,
            rose_bin_deg: int = 10,
            bins_downsample: int | None = None,
            return_json: bool = True,
            is_show: bool = False,
    ):
        """
        3-panel QC dashboard:
          1) COG & HDG vs time (wrapped properly by unwrapping)
          2) Drift angle = wrap180(COG - HDG) vs time with thresholds (±warn, ±bad)
          3) Polar rose (histogram) of COG weighted by SOG, filtered by SOG >= sog_min_for_rose

        Required imports at module top:
          import numpy as np
          import pandas as pd
          from bokeh.plotting import figure
          from bokeh.models import (
              ColumnDataSource, HoverTool, Span, BoxAnnotation, Range1d
          )
          from bokeh.layouts import gridplot
          from bokeh.palettes import Category10
          from bokeh.io import show
          from bokeh.embed import json_item
        """

        # ---- validate ----
        if df is None or len(df) == 0 or time_col not in df.columns:
            p = figure(title="COG/HDG QC — no data", x_axis_type="datetime", sizing_mode="stretch_both")
            layout = gridplot([[p]], sizing_mode="stretch_both")
            if is_show:
                show(layout)
                return layout
            return json_item(layout) if return_json else layout

        df = df.copy()
        df = df.loc[:, ~df.columns.duplicated()]

        # ---- time ----
        if not np.issubdtype(df[time_col].dtype, np.datetime64):
            df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        df = df.dropna(subset=[time_col]).copy()

        if bins_downsample and int(bins_downsample) > 1:
            df = df.iloc[::int(bins_downsample), :].copy()

        # ---- numeric coercion ----
        for c in (cog_col, hdg_col, sog_col):
            if c in df.columns:
                col = df[c]
                if hasattr(col, "ndim") and col.ndim == 2:
                    col = col.iloc[:, 0]
                df[c] = pd.to_numeric(col, errors="coerce")

        # Require at least something
        if cog_col not in df.columns and hdg_col not in df.columns:
            p = figure(title="COG/HDG QC — missing columns", x_axis_type="datetime", sizing_mode="stretch_both")
            layout = gridplot([[p]], sizing_mode="stretch_both")
            if is_show:
                show(layout)
                return layout
            return json_item(layout) if return_json else layout

        # ---- helpers ----
        def wrap180(a_deg):
            # returns in [-180, +180)
            a = (a_deg + 180.0) % 360.0 - 180.0
            return a

        def wrap360(series_deg):
            s = pd.to_numeric(series_deg, errors="coerce")
            return (s % 360.0)
        def unwrap_deg(series_deg):
            s = pd.to_numeric(series_deg, errors="coerce")

            # mask valid values
            mask = s.notna()

            if not mask.any():
                return np.full(len(s), np.nan)

            rad = np.deg2rad(s[mask].to_numpy(dtype=float))

            # unwrap only valid slice
            unwrapped_rad = np.unwrap(rad)

            unwrapped_deg = np.rad2deg(unwrapped_rad)

            # rebuild full array
            result = np.full(len(s), np.nan)
            result[mask.to_numpy()] = unwrapped_deg

            return result

        # ---- build series ----
        # unwrap COG/HDG for time series display (prevents vertical jumps at 0/360)
        if cog_col in df.columns:
            df["_COG_unwrap"] = unwrap_deg(df[cog_col])
        else:
            df["_COG_unwrap"] = np.nan

        if hdg_col in df.columns:
            df["_HDG_unwrap"] = unwrap_deg(df[hdg_col])
        else:
            df["_HDG_unwrap"] = np.nan
        df["_COG_plot"] = wrap360(df[cog_col])
        df["_HDG_plot"] = wrap360(df[hdg_col])
        # Drift angle should stay wrapped (meaningful physical offset)
        if cog_col in df.columns and hdg_col in df.columns:
            df["_DRIFT"] = wrap180(df[cog_col] - df[hdg_col])
        else:
            df["_DRIFT"] = np.nan

        # ---- SOURCES ----
        src = ColumnDataSource(df)

        tools = "pan,wheel_zoom,box_zoom,reset,save,hover"

        # =========================
        # 1) COG & HDG vs time
        # =========================
        p1 = figure(
            title=f"{label_prefix} — COG & HDG vs Time",
            x_axis_type="datetime",
            sizing_mode="stretch_both",
            tools=tools,
            active_scroll="wheel_zoom",
        )
        p1.yaxis.axis_label = "Degrees (unwrapped)"
        p1.xaxis.axis_label = "Time"

        r_cog = None
        r_hdg = None

        if cog_col in df.columns:
            r_cog = p1.line(
                x=time_col, y="_COG_plot",
                source=src,
                line_width=2,
                color=Category10[3][0],
                alpha=0.95,
                legend_label=f"{label_prefix} COG",
            )
        if hdg_col in df.columns:
            r_hdg = p1.line(
                x=time_col, y="_HDG_plot",
                source=src,
                line_width=2,
                color=Category10[3][1],
                alpha=0.95,
                legend_label=f"{label_prefix} HDG",
            )

        p1.legend.click_policy = "hide"
        p1.legend.location = "top_left"

        h1 = p1.select_one(HoverTool)
        if h1:
            h1.tooltips = [
                ("Time", f"@{time_col}{{%F %T}}"),
                ("COG (raw)", f"@{cog_col}{{0.0}}°" if cog_col in df.columns else "n/a"),
                ("HDG (raw)", f"@{hdg_col}{{0.0}}°" if hdg_col in df.columns else "n/a"),
                ("COG (unwrap)", "@_COG_unwrap{0.0}°"),
                ("HDG (unwrap)", "@_HDG_unwrap{0.0}°"),
                ("SOG (kn)", f"@{sog_col}{{0.00}}" if sog_col in df.columns else "n/a"),
            ]
            h1.formatters = {f"@{time_col}": "datetime"}
            rr = []
            if r_cog is not None:
                rr.append(r_cog)
            if r_hdg is not None:
                rr.append(r_hdg)
            if rr:
                h1.renderers = rr

        # =========================
        # 2) Drift angle vs time
        # =========================
        p2 = figure(
            title=f"{label_prefix} — Drift angle (COG − HDG) vs Time",
            x_axis_type="datetime",
            x_range=p1.x_range,
            sizing_mode="stretch_both",
            tools=tools,
            active_scroll="wheel_zoom",
        )
        p2.yaxis.axis_label = "Drift (deg, wrapped -180..180)"
        p2.xaxis.axis_label = "Time"

        # background bands
        # green zone: [-warn, +warn]
        p2.add_layout(BoxAnnotation(bottom=-drift_warn_deg, top=drift_warn_deg, fill_color="green", fill_alpha=0.08))
        # orange zones: warn..bad
        p2.add_layout(BoxAnnotation(bottom=drift_warn_deg, top=drift_bad_deg, fill_color="orange", fill_alpha=0.08))
        p2.add_layout(BoxAnnotation(bottom=-drift_bad_deg, top=-drift_warn_deg, fill_color="orange", fill_alpha=0.08))
        # red zones: beyond bad
        p2.add_layout(BoxAnnotation(bottom=drift_bad_deg, top=180, fill_color="red", fill_alpha=0.04))
        p2.add_layout(BoxAnnotation(bottom=-180, top=-drift_bad_deg, fill_color="red", fill_alpha=0.04))

        # threshold lines
        for y in (0.0, drift_warn_deg, -drift_warn_deg, drift_bad_deg, -drift_bad_deg):
            p2.add_layout(Span(location=y, dimension="width", line_dash="dashed"))

        r_drift = p2.line(
            x=time_col, y="_DRIFT",
            source=src,
            line_width=2,
            color=Category10[3][2],
            alpha=0.95,
            legend_label="Drift",
        )
        p2.legend.click_policy = "hide"
        p2.legend.location = "top_left"

        h2 = p2.select_one(HoverTool)
        if h2:
            h2.tooltips = [
                ("Time", f"@{time_col}{{%F %T}}"),
                ("Drift", "@_DRIFT{0.0}°"),
                ("COG", f"@{cog_col}{{0.0}}°" if cog_col in df.columns else "n/a"),
                ("HDG", f"@{hdg_col}{{0.0}}°" if hdg_col in df.columns else "n/a"),
                ("SOG (kn)", f"@{sog_col}{{0.00}}" if sog_col in df.columns else "n/a"),
            ]
            h2.formatters = {f"@{time_col}": "datetime"}
            h2.renderers = [r_drift]

        # set y-range focused around thresholds (nice QC view)
        lim = max(15.0, float(drift_bad_deg) * 1.5)
        p2.y_range = Range1d(-lim, +lim)

        # =========================
        # 3) Polar rose of COG weighted by SOG
        # =========================
        p3 = figure(
            title=f"{label_prefix} — COG rose (weighted by SOG, SOG≥{sog_min_for_rose:g} kn, bin={rose_bin_deg}°)",
            sizing_mode="stretch_both",
            tools="pan,wheel_zoom,reset,save,hover",
            active_scroll="wheel_zoom",
            x_range=(-1.05, 1.05),
            y_range=(-1.05, 1.05),
        )
        p3.axis.visible = False
        p3.grid.visible = False

        # Prepare rose data
        df_rose = df.copy()
        if sog_col in df_rose.columns:
            df_rose = df_rose[df_rose[sog_col].notna()].copy()
        df_rose = df_rose[df_rose[cog_col].notna()].copy() if cog_col in df_rose.columns else df_rose.iloc[0:0].copy()

        if sog_col in df_rose.columns:
            df_rose = df_rose[df_rose[sog_col] >= float(sog_min_for_rose)].copy()

        if not df_rose.empty and cog_col in df_rose.columns:
            # bins
            bin_deg = max(1, int(rose_bin_deg))
            edges = np.arange(0, 360 + bin_deg, bin_deg)

            # normalize angles to [0,360)
            ang = (df_rose[cog_col] % 360.0).astype(float)

            cats = pd.cut(ang, bins=edges, include_lowest=True, right=False)
            # weights = SOG (or 1 if missing)
            if sog_col in df_rose.columns:
                w = pd.to_numeric(df_rose[sog_col], errors="coerce").fillna(0.0)
            else:
                w = pd.Series(1.0, index=df_rose.index)

            # weighted sum per bin
            rose = pd.DataFrame({"bin": cats, "w": w}).groupby("bin", dropna=True)["w"].sum()
            rose = rose.reindex(rose.index.sort_values())

            centers = np.array([i.left + bin_deg / 2 for i in rose.index], dtype=float)
            vals = rose.values.astype(float)

            # normalize to unit circle
            vmax = float(np.max(vals)) if len(vals) else 1.0
            if vmax <= 0:
                vmax = 1.0
            rnorm = vals / vmax

            # polar->xy for ray tips (0° at North, clockwise)
            theta = np.deg2rad(90.0 - centers)
            x_tip = rnorm * np.cos(theta)
            y_tip = rnorm * np.sin(theta)

            src_rose = ColumnDataSource(dict(
                center_deg=centers,
                weight=vals,
                rnorm=rnorm,
                x_tip=x_tip,
                y_tip=y_tip,
            ))

            # rings
            for rr in (0.25, 0.5, 0.75, 1.0):
                p3.circle(x=0, y=0, radius=rr, fill_color=None, line_alpha=0.2)

            # spokes every 30°
            for deg in range(0, 360, 30):
                t = np.deg2rad(90.0 - deg)
                p3.line([0, np.cos(t)], [0, np.sin(t)], line_alpha=0.15)

            seg = p3.segment(x0=0, y0=0, x1="x_tip", y1="y_tip", source=src_rose, line_width=4, line_alpha=0.9)

            h3 = p3.select_one(HoverTool)
            if h3:
                h3.tooltips = [
                    ("COG bin (deg)", "@center_deg{0}"),
                    ("Weighted SOG sum (kn)", "@weight{0.00}"),
                    ("Norm", "@rnorm{0.00}"),
                ]
                h3.renderers = [seg]
        else:
            # still draw compass rings/spokes so it's not empty looking
            for rr in (0.25, 0.5, 0.75, 1.0):
                p3.circle(x=0, y=0, radius=rr, fill_color=None, line_alpha=0.2)
            for deg in range(0, 360, 30):
                t = np.deg2rad(90.0 - deg)
                p3.line([0, np.cos(t)], [0, np.sin(t)], line_alpha=0.15)

        # ---- layout with one toolbar ----
        layout = gridplot(
            [[p1], [p2], [p3]],
            merge_tools=True,
            toolbar_location="above",
            sizing_mode="stretch_both",
        )

        if is_show:
            show(layout)
            return layout

        if return_json:
            return json_item(layout)

        return layout

    def bokeh_cog_hdg_timeseries(
            self,
            df,
            *,
            time_col: str = "T",
            cog_col: str = "VesselCOG",  # or "ROV1_COG", "ROV2_COG"
            hdg_col: str = "VesselHDG",  # or "ROV1_HDG", "ROV2_HDG"
            sog_col: str | None = "VesselSOG",  # optional for hover
            label_prefix: str = "Vessel",
            vessel_column: str = "VesselName",
            bins_downsample: int | None = None,
            break_jump_deg: float = 180.0,  # break line if wrap jump > this
            return_json: bool = True,
            is_show: bool = False,
    ):
        """
        Marine QC-safe plot: COG & HDG vs Time (0..360, with line breaks at wrap jumps)

        - X axis: Time
        - Y axis: Degrees (0..360)
        - Wrap values to [0, 360)
        - Break the line at wrap-around to avoid vertical spikes (NaN insertion)

        Required imports at module top:
          import numpy as np
          import pandas as pd
          from bokeh.plotting import figure
          from bokeh.models import ColumnDataSource, HoverTool, Range1d
          from bokeh.palettes import Category10
          from bokeh.io import show
          from bokeh.embed import json_item
        """

        if df is None or len(df) == 0 or time_col not in df.columns:
            p = figure(
                title=f"{label_prefix} — COG & HDG vs Time (no data)",
                x_axis_type="datetime",
                sizing_mode="stretch_both",
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )
            if is_show:
                show(p)
                return p
            return json_item(p) if return_json else p

        df = df.copy()
        df = df.loc[:, ~df.columns.duplicated()]

        # time
        if not np.issubdtype(df[time_col].dtype, np.datetime64):
            df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        df = df.dropna(subset=[time_col]).copy()

        if bins_downsample and int(bins_downsample) > 1:
            df = df.iloc[::int(bins_downsample), :].copy()

        # numeric angles
        for c in (cog_col, hdg_col):
            if c in df.columns:
                col = df[c]
                if hasattr(col, "ndim") and col.ndim == 2:
                    col = col.iloc[:, 0]
                df[c] = pd.to_numeric(col, errors="coerce")

        if sog_col and sog_col in df.columns:
            col = df[sog_col]
            if hasattr(col, "ndim") and col.ndim == 2:
                col = col.iloc[:, 0]
            df[sog_col] = pd.to_numeric(col, errors="coerce")

        def wrap360(series_deg):
            s = pd.to_numeric(series_deg, errors="coerce")
            return (s % 360.0)

        if cog_col in df.columns:
            df["_COG_plot"] = wrap360(df[cog_col])
        else:
            df["_COG_plot"] = np.nan

        if hdg_col in df.columns:
            df["_HDG_plot"] = wrap360(df[hdg_col])
        else:
            df["_HDG_plot"] = np.nan
        vessel_label = df[vessel_column][0]
        src = ColumnDataSource(df)

        p = figure(
            title=f"{vessel_label} — COG & HDG vs Time",
            x_axis_type="datetime",
            sizing_mode="stretch_both",
            tools="pan,wheel_zoom,box_zoom,reset,save,hover",
            active_scroll="wheel_zoom",
            y_range=Range1d(0, 360),
        )
        p.yaxis.axis_label = "Degrees (0–360)"
        p.xaxis.axis_label = "Time"

        r_cog = p.line(
            x=time_col, y="_COG_plot",
            source=src,
            line_width=2,
            color=Category10[3][0],
            alpha=0.95,
            legend_label=f"{label_prefix} COG",
        )
        r_hdg = p.line(
            x=time_col, y="_HDG_plot",
            source=src,
            line_width=2,
            color=Category10[3][1],
            alpha=0.95,
            legend_label=f"{label_prefix} HDG",
        )

        p.legend.location = "top_left"
        p.legend.orientation = "horizontal"
        p.legend.click_policy = "hide"

        h = p.select_one(HoverTool)
        if h:
            tips = [("Time", f"@{time_col}{{%F %T}}")]
            if cog_col in df.columns:
                tips.append(("COG", f"@{cog_col}{{0.0}}°"))
            tips.append(("COG (plot)", "@_COG_plot{0.0}°"))
            if hdg_col in df.columns:
                tips.append(("HDG", f"@{hdg_col}{{0.0}}°"))
            tips.append(("HDG (plot)", "@_HDG_plot{0.0}°"))
            if sog_col and sog_col in df.columns:
                tips.append(("SOG (kn)", f"@{sog_col}{{0.00}}"))
            tips.append(("File", "@FileName"))

            h.tooltips = tips
            h.formatters = {f"@{time_col}": "datetime"}
            h.renderers = [r_cog, r_hdg]

        if is_show:
            show(p)
            return p

        if return_json:
            return json_item(p)

        return p
    def boke_cog_hdg_timeseries_all(self,df: pd.DataFrame,
                                    is_show:bool=False,
                                    return_json:bool=False,
                                    cog_columns=["VesselCOG","ROV1_COG","ROV2_COG"],
                                    hdg_columns=["VesselHDG","ROV1_HDG","ROV2_HDG"],
                                    vessel_columns =["VesselName",'Rov1Name','Rov2Name']):

        plots=[]
        for cog,hdg,name in zip(cog_columns,hdg_columns,vessel_columns):
            p = self.bokeh_cog_hdg_timeseries(df=df,
                                          cog_col=cog,
                                          hdg_col=hdg,
                                          vessel_column=name,
                                          is_show=is_show,
                                          return_json=return_json,
                                          )
            plots.append(p)
        toggle_legend_btn = Button(
            label="Hide legend",
            button_type="primary",
            width=120,
        )
        toggle_legend_btn.js_on_click(
            CustomJS(
                args=dict(plots=plots, btn=toggle_legend_btn),
                code="""
                let anyVisible = false;

                // check if any legend is visible
                for (let i = 0; i < plots.length; i++) {
                    if (plots[i].legend.length > 0 && plots[i].legend[0].visible) {
                        anyVisible = true;
                        break;
                    }
                }

                // toggle all legends
                for (let i = 0; i < plots.length; i++) {
                    if (plots[i].legend.length > 0) {
                        plots[i].legend[0].visible = !anyVisible;
                    }
                }

                btn.label = anyVisible ? "Show legends" : "Hide legends";
                """
            )
        )

        layout = column(
            toggle_legend_btn,
            gridplot([[p] for p in plots], merge_tools=True, sizing_mode="stretch_both"),
            sizing_mode="stretch_both"
        )
        if is_show:
           show(layout)
           return

        if return_json:
           return json_item(layout)

        else:
            return layout

    def dsr_points_in_bbox_timeframe(
            self,
            bbox_df,  # pd.DataFrame from load_bbox_data(...)  (must contain datetime column)
            *,
            bbox_time_col: str = "T",  # your BlackBox time column in df
            dsr_table: str = "DSR",
            dsr_time_col: str = "TimeStamp",  # DSR timestamp column name
            rov_col: str = "ROV",
            linepoint_col: str = "LinePoint",
            rovs: list[str] | None = None,  # e.g. ["ROV1","ROV2"] or ["1","2"] depending your DSR content
            buffer_seconds: int = 0,  # extend bbox timeframe by +/- seconds
    ):
        """
        Return a dataframe with [LinePoint, ROV, TimeStamp] from DSR
        where DSR.TimeStamp falls inside the BlackBox timeframe (min/max of bbox_df[bbox_time_col]).

        Notes:
          - bbox_df[bbox_time_col] should be datetime-like; we coerce safely.
          - Uses SQLite BETWEEN with ISO strings.
          - buffer_seconds can be used for clock offsets.
        """

        if bbox_df is None or len(bbox_df) == 0 or bbox_time_col not in bbox_df.columns:
            # empty
            return pd.DataFrame(columns=[linepoint_col, rov_col, dsr_time_col])

        # --- normalize bbox time ---
        bb = bbox_df.copy()
        if not pd.api.types.is_datetime64_any_dtype(bb[bbox_time_col]):
            bb[bbox_time_col] = pd.to_datetime(bb[bbox_time_col], errors="coerce")
        bb = bb.dropna(subset=[bbox_time_col])
        if bb.empty:
            return pd.DataFrame(columns=[linepoint_col, rov_col, dsr_time_col])

        tmin = bb[bbox_time_col].min()
        tmax = bb[bbox_time_col].max()

        if buffer_seconds and int(buffer_seconds) != 0:
            tmin = tmin - pd.Timedelta(seconds=int(buffer_seconds))
            tmax = tmax + pd.Timedelta(seconds=int(buffer_seconds))

        # SQLite-friendly strings
        start_ts = tmin.strftime("%Y-%m-%d %H:%M:%S")
        end_ts = tmax.strftime("%Y-%m-%d %H:%M:%S")

        # --- build SQL ---
        where = [f"{dsr_time_col} IS NOT NULL", f"{dsr_time_col} >= ? AND {dsr_time_col} <= ?"]
        params = [start_ts, end_ts]

        if rovs:
            placeholders = ",".join("?" for _ in rovs)
            where.append(f"{rov_col} IN ({placeholders})")
            params.extend(list(rovs))

        sql = f"""
            SELECT
                {linepoint_col} AS LinePoint,
                {rov_col} AS ROV,
                {dsr_time_col} AS TimeStamp,
                Line,Station 
            FROM {dsr_table}
            WHERE {" AND ".join(where)}
            ORDER BY {dsr_time_col}
        """

        with self._connect() as conn:
            dsr_df = pd.read_sql_query(sql, conn, params=tuple(params))

        # normalize DSR timestamp to datetime
        if "TimeStamp" in dsr_df.columns:
            dsr_df["TimeStamp"] = pd.to_datetime(dsr_df["TimeStamp"], errors="coerce")
            dsr_df = dsr_df.dropna(subset=["TimeStamp"]).copy()
        self.dsr_df =dsr_df
        return dsr_df

    def bokeh_drift_timeseries(
            self,
            df,
            *,
            time_col: str = "T",
            cog_col: str = "VesselCOG",  # or "ROV1_COG", "ROV2_COG"
            hdg_col: str = "VesselHDG",  # or "ROV1_HDG", "ROV2_HDG"
            vn_col: str ="VesselName", # or "Rov1Name","Rov2Name"
            label: str = "Drift (COG − HDG)",
            drift_warn_deg: float = 5.0,
            drift_bad_deg: float = 10.0,
            bins_downsample: int | None = None,
            return_json: bool = True,
            is_show: bool = False,
    ):
        """
        Drift angle QC plot:
          - X axis: Time
          - Y axis: Drift = wrap180(COG - HDG) in degrees
          - No HoverTool
          - Threshold bands + lines

        Required imports at module top:
          import numpy as np
          import pandas as pd
          from bokeh.plotting import figure
          from bokeh.models import ColumnDataSource, Span, BoxAnnotation, Range1d
          from bokeh.io import show
          from bokeh.embed import json_item
        """

        if df is None or len(df) == 0 or time_col not in df.columns:

            p = figure(
                title="Drift — no data",
                x_axis_type="datetime",
                sizing_mode="stretch_both",
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )
            if is_show:
                show(p)
                return p
            return json_item(p) if return_json else p

        df = df.copy()
        df = df.loc[:, ~df.columns.duplicated()]

        # time
        if not np.issubdtype(df[time_col].dtype, np.datetime64):
            df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        df = df.dropna(subset=[time_col]).copy()

        if bins_downsample and int(bins_downsample) > 1:
            df = df.iloc[::int(bins_downsample), :].copy()

        # numeric angles
        for c in (cog_col, hdg_col):
            if c in df.columns:
                col = df[c]
                if hasattr(col, "ndim") and col.ndim == 2:
                    col = col.iloc[:, 0]
                df[c] = pd.to_numeric(col, errors="coerce")

        if cog_col not in df.columns or hdg_col not in df.columns:
            p = figure(
                title="Drift — missing COG/HDG columns",
                x_axis_type="datetime",
                sizing_mode="stretch_both",
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )
            if is_show:
                show(p)
                return p
            return json_item(p) if return_json else p

        # wrap to [-180, 180)
        drift = (df[cog_col] - df[hdg_col] + 180.0) % 360.0 - 180.0
        df["_DRIFT"] = drift

        src = ColumnDataSource(df)
        vessel_label = df[vn_col][0]
        # tools WITHOUT hover
        p = figure(
            title=f"{vessel_label}{label}",
            x_axis_type="datetime",
            sizing_mode="stretch_both",
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
        )
        p.yaxis.axis_label = "Drift (deg)"
        p.xaxis.axis_label = "Time"

        # background QC bands
        warn = float(drift_warn_deg)
        bad = float(drift_bad_deg)

        p.add_layout(BoxAnnotation(bottom=-warn, top=warn, fill_color="green", fill_alpha=0.08))
        p.add_layout(BoxAnnotation(bottom=warn, top=bad, fill_color="orange", fill_alpha=0.08))
        p.add_layout(BoxAnnotation(bottom=-bad, top=-warn, fill_color="orange", fill_alpha=0.08))
        p.add_layout(BoxAnnotation(bottom=bad, top=180, fill_color="red", fill_alpha=0.04))
        p.add_layout(BoxAnnotation(bottom=-180, top=-bad, fill_color="red", fill_alpha=0.04))
        lim = max(15.0, bad * 1.5)
        p.y_range = Range1d(-lim, +lim)
        # threshold lines
        for y in (0.0, warn, -warn, bad, -bad):
            p.add_layout(Span(location=y, dimension="width", line_dash="dashed"))

        p.line(x=time_col, y="_DRIFT", source=src, line_width=2, alpha=0.95)
        self.add_dsr_vertical_lines(p=p,
                                    dsr_df=self.dsr_df, rov_name=vessel_label)
        # nice y-range focused around thresholds


        if is_show:
            show(p)
            return p

        if return_json:
            return json_item(p)

        return p

    def add_dsr_vertical_lines(
            self,
            p,
            dsr_df,
            *,
            time_col: str = "TimeStamp",
            linepoint_col: str = "LinePoint",
            rov_col: str = "ROV",
            rov_name:str="",
            color: str = "red",
            line_width: int = 2,
            line_dash: str = "dashed",
            alpha: float = 0.8,
    ):
        """
        Add vertical DSR lines and attach them to ONE HoverTool (no duplicates).

        Fixes:
          - HoverTool.renderers can be "auto" (string). We normalize it to a list.
          - Avoids adding a 2nd HoverTool if one already exists.
        """

        if dsr_df is None or len(dsr_df) == 0:
            return p

        df = dsr_df.copy()
        df = df[df["ROV"] == rov_name]
        if time_col not in df.columns:
            return p

        # ensure datetime
        if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
            df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        df = df.dropna(subset=[time_col])
        if df.empty:
            return p

        # pick reasonable y extents (works for fixed y_range; ok for auto-range too)
        y0 = p.y_range.start
        y1 = p.y_range.end
        if y0 is None:
            y0 = 0
        if y1 is None:
            y1 = 1

        src = ColumnDataSource(dict(
            x=df[time_col],
            y0=[y0] * len(df),
            y1=[y1] * len(df),
            TimeStamp=df[time_col],
            LinePoint=df[linepoint_col] if linepoint_col in df.columns else [""] * len(df),
            ROV=df[rov_col] if rov_col in df.columns else [""] * len(df),
        ))

        r = p.segment(
            x0="x", y0="y0",
            x1="x", y1="y1",
            source=src,
            line_color=color,
            line_width=line_width,
            line_dash=line_dash,
            alpha=alpha,legend_label="NODE"
        )

        # reuse existing hover if present, else create one
        hover = p.select_one(HoverTool)
        if hover is None:
            hover = HoverTool(
                tooltips=[
                    ("DSR Time", "@TimeStamp{%F %T}"),
                    ("LinePoint", "@LinePoint"),
                    ("ROV", "@ROV"),
                ],
                formatters={"@TimeStamp": "datetime"},
            )
            p.add_tools(hover)
        else:
            # ensure our DSR tooltips exist (append, avoid duplicates)
            dsr_tooltips = [
                ("DSR Time", "@TimeStamp{%F %T}"),
                ("LinePoint", "@LinePoint"),
                ("ROV", "@ROV"),
            ]
            existing = hover.tooltips or []
            existing_list = list(existing) if isinstance(existing, (list, tuple)) else [existing]
            for t in dsr_tooltips:
                if t not in existing_list:
                    existing_list.append(t)
            hover.tooltips = existing_list
            hover.formatters = {**(hover.formatters or {}), "@TimeStamp": "datetime"}

        # ---- CRITICAL FIX: normalize hover.renderers ----
        hr = hover.renderers

        # Bokeh default is "auto" (string) -> treat as empty list and set explicit list
        if hr is None or hr == "auto":
            hover.renderers = [r]
        else:
            # sometimes it's a tuple; normalize to list
            if not isinstance(hr, (list, tuple)):
                hover.renderers = [r]
            else:
                lst = list(hr)
                if r not in lst:
                    lst.append(r)
                hover.renderers = lst

        return p















