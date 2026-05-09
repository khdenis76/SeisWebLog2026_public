import math
import sqlite3
from pathlib import Path

import pandas as pd

from bokeh.embed import file_html, json_item
from bokeh.io import output_file, save, show
from bokeh.models import (
    ColumnDataSource,
    HoverTool,
    WheelZoomTool,
    ResetTool,
    PanTool,
    BoxZoomTool,
    SaveTool,
)
from bokeh.plotting import figure
from bokeh.resources import CDN


class ProjectTemplatePlots:
    def __init__(self, db_path):
        self.db_path = Path(db_path)

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 60000")
        return conn

    def _read_preplot_lines(self, table_name):
        conn = self._connect()
        try:
            sql = f"""
                SELECT
                    ID,
                    Line,
                    TierLine,
                    FirstPoint,
                    LastPoint,
                    RealStartX,
                    RealStartY,
                    RealEndX,
                    RealEndY,
                    StartX,
                    StartY,
                    EndX,
                    EndY,
                    LineLength,
                    Tier
                FROM {table_name}
                WHERE Line IS NOT NULL
            """
            return pd.read_sql_query(sql, conn)
        finally:
            conn.close()

    @staticmethod
    def _as_float(value):
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _segment_intersects(a1, a2, b1, b2):
        """
        Fast 2D segment intersection test.
        a1/a2 = source line start/end
        b1/b2 = receiver line start/end
        """

        def orient(p, q, r):
            return (q[1] - p[1]) * (r[0] - q[0]) - (q[0] - p[0]) * (r[1] - q[1])

        def on_segment(p, q, r):
            return (
                min(p[0], r[0]) <= q[0] <= max(p[0], r[0])
                and min(p[1], r[1]) <= q[1] <= max(p[1], r[1])
            )

        o1 = orient(a1, a2, b1)
        o2 = orient(a1, a2, b2)
        o3 = orient(b1, b2, a1)
        o4 = orient(b1, b2, a2)

        if o1 * o2 < 0 and o3 * o4 < 0:
            return True

        eps = 1e-9

        if abs(o1) < eps and on_segment(a1, b1, a2):
            return True
        if abs(o2) < eps and on_segment(a1, b2, a2):
            return True
        if abs(o3) < eps and on_segment(b1, a1, b2):
            return True
        if abs(o4) < eps and on_segment(b1, a2, b2):
            return True

        return False

    @staticmethod
    def _line_points(row):
        """
        Prefer RealStart/RealEnd coordinates.
        Fall back to Start/End if Real coordinates are missing.
        """
        sx = ProjectTemplatePlots._as_float(row.get("RealStartX"))
        sy = ProjectTemplatePlots._as_float(row.get("RealStartY"))
        ex = ProjectTemplatePlots._as_float(row.get("RealEndX"))
        ey = ProjectTemplatePlots._as_float(row.get("RealEndY"))

        if sx is None or sy is None or ex is None or ey is None:
            sx = ProjectTemplatePlots._as_float(row.get("StartX"))
            sy = ProjectTemplatePlots._as_float(row.get("StartY"))
            ex = ProjectTemplatePlots._as_float(row.get("EndX"))
            ey = ProjectTemplatePlots._as_float(row.get("EndY"))

        if sx is None or sy is None or ex is None or ey is None:
            return None

        return (sx, sy), (ex, ey)

    def make_visual_offset_identifier_plot(
            self,
            *,
            title="Visual Offset Identifier",
            plot_width=1800,
            plot_height=900,
            is_show=False,
            save_html_path=None,
            return_json=False,
    ):
        """
        Visual Offset Identifier matrix.

        Columns:
            SLPreplot.Line

        Rows:
            project_template.RLine

        Blue cell:
            SLPreplot.Line is inside project_template.FirstSL / LastSL range
            for that RLine.
        """

        conn = self._connect()
        try:
            sl_df = pd.read_sql_query("""
                SELECT DISTINCT Line
                FROM SLPreplot
                WHERE Line IS NOT NULL
                ORDER BY Line
            """, conn)

            template_df = pd.read_sql_query("""
                SELECT
                    ID,
                    FirstSL,
                    LastSL,
                    LNum,
                    RLine,
                    Tier
                FROM project_template
                WHERE RLine IS NOT NULL
                  AND FirstSL IS NOT NULL
                  AND LastSL IS NOT NULL
                ORDER BY RLine
            """, conn)
        finally:
            conn.close()

        if sl_df.empty or template_df.empty:
            p = figure(
                title="No project template / SLPreplot data found",
                width=plot_width,
                height=plot_height,
            )
            return json_item(p) if return_json else p

        sl_lines = [int(v) for v in sl_df["Line"].tolist()]
        rlines = sorted({int(v) for v in template_df["RLine"].tolist()})

        x_labels = [str(v) for v in sl_lines]
        y_labels = [str(v) for v in rlines]

        data = {
            "x": [],
            "y": [],
            "sl_line": [],
            "rline": [],
            "first_sl": [],
            "last_sl": [],
            "lnum": [],
            "tier": [],
            "label": [],
        }

        for row in template_df.to_dict("records"):
            first_sl = int(row["FirstSL"])
            last_sl = int(row["LastSL"])
            rline = int(row["RLine"])

            lo = min(first_sl, last_sl)
            hi = max(first_sl, last_sl)

            for sl in sl_lines:
                if lo <= sl <= hi:
                    data["x"].append(str(sl))
                    data["y"].append(str(rline))
                    data["sl_line"].append(sl)
                    data["rline"].append(rline)
                    data["first_sl"].append(first_sl)
                    data["last_sl"].append(last_sl)
                    data["lnum"].append(row["LNum"])
                    data["tier"].append(row["Tier"])
                    data["label"].append("---")

        source = ColumnDataSource(data)

        p = figure(
            title=title,
            x_range=x_labels,
            y_range=list(reversed(y_labels)),
            width=plot_width,
            height=plot_height,
            toolbar_location="above",
            tools="pan,wheel_zoom,box_zoom,reset,save",
            output_backend="webgl",
        )

        p.rect(
            x="x",
            y="y",
            width=0.95,
            height=0.95,
            source=source,
            fill_color="#04b8ee",
            fill_alpha=0.95,
            line_color="black",
            line_alpha=0.35,
            line_width=0.4,
        )

        p.text(
            x="x",
            y="y",
            text="label",
            source=source,
            text_align="center",
            text_baseline="middle",
            text_font_size="7pt",
            text_color="black",
        )

        hover = HoverTool(
            tooltips=[
                ("RLine", "@rline"),
                ("SL Line", "@sl_line"),
                ("Range", "@first_sl - @last_sl"),
                ("# Lines", "@lnum"),
                ("Tier", "@tier"),
            ]
        )
        p.add_tools(hover)

        p.xaxis.major_label_orientation = math.radians(90)
        p.xaxis.major_label_text_font_size = "7pt"
        p.yaxis.major_label_text_font_size = "7pt"

        p.xaxis.axis_label = "SLPreplot.Line"
        p.yaxis.axis_label = "Project Template RLine"

        p.grid.grid_line_color = "black"
        p.grid.grid_line_alpha = 0.25

        p.border_fill_color = "white"
        p.background_fill_color = "white"

        p.title.text_font_size = "14pt"
        p.title.text_font_style = "bold"

        if save_html_path:
            output_file(str(save_html_path), title=title)
            save(p)

        if is_show:
            show(p)

        if return_json:
            return json_item(p)

        return p

