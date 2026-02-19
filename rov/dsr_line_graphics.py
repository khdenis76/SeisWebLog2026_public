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












