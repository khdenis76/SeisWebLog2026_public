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
                cfg.gnss2_name
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

            # labels (NEW)
            gnss1_label: str | None = None,
            gnss2_label: str | None = None,

            # plot options
            title: str = "GNSS QC",
            height: int = 220,
            is_show: bool = False,
    ):
        # expects imports at top of file

        # ---------- resolve legend names ----------
        # defaults
        _gnss1 = "GNSS1"
        _gnss2 = "GNSS2"

        # override from config if exactly one file
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

        # FINAL override from function call
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

        df["T"] = pd.to_datetime(df["T"], errors="coerce")
        df = df.dropna(subset=["T"])
        # NOS must be >= 0
        df["GNSS1_NOS"] = pd.to_numeric(df["GNSS1_NOS"], errors="coerce")
        df["GNSS2_NOS"] = pd.to_numeric(df["GNSS2_NOS"], errors="coerce")
        df.loc[df["GNSS1_NOS"] < 0, "GNSS1_NOS"] = np.nan
        df.loc[df["GNSS2_NOS"] < 0, "GNSS2_NOS"] = np.nan

        for c in [
            "GNSS1_NOS", "GNSS1_DiffAge", "GNSS1_FixQuality",
            "GNSS2_NOS", "GNSS2_DiffAge", "GNSS2_FixQuality",
        ]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

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
        r11 = p1.line("T", "GNSS1_NOS", source=src, line_width=2, color= 'red',legend_label=_gnss1)
        r12 = p1.line("T", "GNSS2_NOS", source=src, line_width=2, color='blue', legend_label=_gnss2)
        p1.legend.location = "top_left"
        p1.legend.click_policy = "hide"

        # ---------- plot 2: DiffAge ----------
        p2 = figure(
            title=f"{title} — DiffAge",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            active_scroll="wheel_zoom",
            sizing_mode="stretch_both",
            x_range=p1.x_range,
        )
        r21 = p2.line("T", "GNSS1_DiffAge", source=src, line_width=2, legend_label=_gnss1)
        r22 = p2.line("T", "GNSS2_DiffAge", source=src, line_width=2, legend_label=_gnss2)
        p2.legend.location = "top_left"
        p2.legend.click_policy = "hide"

        # ---------- plot 3: FixQuality ----------
        p3 = figure(
            title=f"{title} — FixQuality",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            active_scroll="wheel_zoom",
            sizing_mode="stretch_both",
            x_range=p1.x_range,
        )
        r31 = p3.step("T", "GNSS1_FixQuality", source=src, mode="after", line_width=2, legend_label=_gnss1)
        r32 = p3.step("T", "GNSS2_FixQuality", source=src, mode="after", line_width=2, legend_label=_gnss2)
        p3.legend.location = "top_left"
        p3.legend.click_policy = "hide"

        layout = column(p1, p2, p3, sizing_mode="stretch_both")

        if is_show:
            show(layout)
        return layout



