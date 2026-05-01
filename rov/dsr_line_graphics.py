import math
import sqlite3
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd
import os
from bokeh.embed import json_item
from bokeh.io import output_file, save, show
from bokeh.embed import file_html
from bokeh.resources import CDN, INLINE
from django.shortcuts import render

from bokeh.layouts import column, gridplot, row, Spacer
from bokeh.models import (
    ColumnDataSource, HoverTool, Range1d, Button, CustomJS, FactorRange,
    LegendItem, Legend, Div, Span, TextInput, LinearColorMapper,
    NumeralTickFormatter, PrintfTickFormatter, Rect, MultiSelect, Ray,
    ColorBar, LabelSet, Label, Arrow, OpenHead
)
from bokeh.models import Range1d
from bokeh.plotting import figure
from bokeh.transform import linear_cmap, factor_cmap, dodge, transform
from bokeh.palettes import Viridis256, Category10, Category20, Turbo256, Paired




class DSRLineGraphics(object):
    def __init__(self,db_path):
        self.db_path = db_path
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn
    def read_dsr_for_line(self, line: int) -> pd.DataFrame:
        with self._connect() as conn:
            ok = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='DSR' LIMIT 1"
            ).fetchone()
            if not ok:
                raise ProjectDbError("Table 'DSR' not found in project DB.")

            df = pd.read_sql_query(
                "SELECT * FROM DSR WHERE Line = ? ORDER BY LinePoint, TimeStamp",
                conn,
                params=(int(line),),
            )
        return df
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

    def _error_layout(
            self,
            title: str = "Plot Error",
            message: str ="",
            *,
            details: str = "",
            level: str = "error",  # "error" | "warning" | "info"
            is_show: bool = False,
            json_return: bool = False,
            retry_js: str = "window.location.reload();",
    ):
        # Timestamp (no imports)
        ts = str(pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S"))

        # Simple HTML escaping (avoid breaking layout if message has < > &)
        def _esc(s):
            if s is None:
                return ""
            s = str(s)
            return (
                s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )

        icon_map = {
            "error": "❌",
            "warning": "⚠️",
            "info": "ℹ️",
        }
        border_map = {
            "error": "#ef4444",
            "warning": "#f59e0b",
            "info": "#3b82f6",
        }
        bg_map = {
            "error": "#fff5f5",
            "warning": "#fffbeb",
            "info": "#eff6ff",
        }

        icon = icon_map.get(level, "❌")
        border = border_map.get(level, "#ef4444")
        bg = bg_map.get(level, "#fff5f5")

        title_html = _esc(title)
        msg_html = _esc(message)
        details_html = _esc(details).replace("\n", "<br>")

        panel = Div(
            text=f"""
            <div style="
                border:1px solid {border};
                border-left:6px solid {border};
                background:{bg};
                padding:12px 14px;
                border-radius:10px;
                font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
            ">
              <div style="display:flex; gap:10px; align-items:flex-start;">
                <div style="font-size:20px; line-height:1;">{icon}</div>
                <div style="flex:1;">
                  <div style="font-weight:700; font-size:14px; margin-bottom:2px;">
                    {title_html}
                  </div>
                  <div style="font-size:13px; margin-bottom:6px;">
                    {msg_html}
                  </div>

                  <div style="display:flex; gap:12px; flex-wrap:wrap; align-items:center;">
                    <div style="font-size:12px; color:#6b7280;">
                      <b>Time:</b> {ts}
                    </div>
                    <div style="font-size:12px; color:#6b7280;">
                      <b>Level:</b> {_esc(level)}
                    </div>
                  </div>

                  {"<div style='margin-top:8px; font-size:12px; color:#374151;'><b>Details:</b><div style='margin-top:4px;'>" + details_html + "</div></div>" if details_html else ""}
                </div>
              </div>
            </div>
            """,
            sizing_mode="stretch_width",
        )

        retry_btn = Button(label="Retry", button_type="primary", width=90)
        retry_btn.js_on_click(CustomJS(code=retry_js))

        # Empty plot placeholder (keeps plot area consistent)
        p = figure(
            height=220,
            toolbar_location=None,
            x_axis_type="datetime",
            title="",
            width_policy="max",
        )
        p.xaxis.visible = False
        p.yaxis.visible = False
        p.xgrid.visible = False
        p.ygrid.visible = False
        p.outline_line_alpha = 0.25

        layout = column(
            panel,
            row(retry_btn, sizing_mode="stretch_width"),
            p,
            sizing_mode="stretch_both",
        )

        if is_show:
            show(layout)
            return None

        if json_return:
            return json_item(layout)

        return layout



    def _plotly_error_html(
            self,
            title="Plot Error",
            message="Something went wrong.",
            details=None,
            level="error",  # "error" | "warning" | "info"
            retry_js=None,  # optional JS function name to call (no parentheses)
            is_show=False,
            json_return=False,
    ):
        """
        Plotly-friendly error output.

        - Default: returns HTML string for {{ plotly_plot|safe }}
        - json_return=True: returns dict suitable for JsonResponse
        - is_show=True: prints the HTML to console (useful in tests) and returns None
        """

        icon_map = {"error": "❌", "warning": "⚠", "info": "ℹ"}
        color_map = {"error": "#f8d7da", "warning": "#fff3cd", "info": "#e7f1ff"}
        border_map = {"error": "#dc3545", "warning": "#ffc107", "info": "#0d6efd"}

        lvl = str(level or "error").strip().lower()
        icon = icon_map.get(lvl, "❌")
        bg = color_map.get(lvl, "#f8d7da")
        border = border_map.get(lvl, "#dc3545")

        # Robust timestamp (works even if datetime wasn't imported elsewhere)
        try:
            ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            ts = ""

        # Avoid None and keep HTML safe-ish
        title_txt = "" if title is None else str(title)
        msg_txt = "" if message is None else str(message)

        retry_button = ""
        if retry_js:
            fn = str(retry_js).strip()
            # allow passing "reloadChart" or "reloadChart()"
            onclick = fn if fn.endswith(")") else f"{fn}()"
            retry_button = f"""
            <button class="btn btn-sm btn-outline-dark mt-3" onclick="{onclick}">
                Retry
            </button>
            """

        details_block = ""
        if details:
            details_block = f"""
            <div class="mt-2 small text-muted" style="white-space:pre-wrap;">
                <b>Details:</b><br>{details}
            </div>
            """

        html = f"""
        <div style="
            border: 1px solid {border};
            background: {bg};
            padding: 20px;
            border-radius: 10px;
            width: 100%;
        ">
            <div style="font-size:18px; font-weight:600;">
                {icon} {title_txt}
            </div>

            <div class="mt-2">
                {msg_txt}
            </div>

            {details_block}

            <div class="mt-3 small text-muted">
                Generated: {ts}
            </div>

            {retry_button}
        </div>
        """

        if is_show:
            # Plotly errors are HTML; showing in console is the safest "show"
            print(html)
            return None

        if json_return:
            # Good for Django JsonResponse({"ok": False, **result})
            return {
                "ok": False,
                "level": lvl,
                "title": title_txt,
                "message": msg_txt,
                "details": details,
                "timestamp": ts,
                "html": html,
            }

        return html

    @staticmethod
    def add_inline_xline_offsets(
            dsr_df: pd.DataFrame,
            rp_preplot_df: pd.DataFrame,
            *,
            from_xy=("PreplotEasting", "PreplotNorthing"),
            to_xy=("PrimaryEasting", "PrimaryNorthing"),
            bearing_col="LineBearing",
            out_prefix="Pri",
    ) -> pd.DataFrame:
        """
        Add Inline/Xline offset columns into dsr_df using a common line bearing
        taken from first row of rp_preplot_df[bearing_col].

        Offsets are computed as:
          dx = to_x - from_x
          dy = to_y - from_y

        Bearing is assumed AZIMUTH clockwise from North:
          0 = North, 90 = East

        Inline axis points along the bearing direction.
        Xline axis is +90° to the right of inline.

        Output columns:
          {out_prefix}OffE, {out_prefix}OffN, {out_prefix}OffInline, {out_prefix}OffXline
        """

        if dsr_df is None or dsr_df.empty:
            return dsr_df

        if rp_preplot_df is None or rp_preplot_df.empty:
            raise ValueError("rp_preplot_df is empty; cannot read LineBearing.")

        if bearing_col not in rp_preplot_df.columns:
            raise ValueError(f"'{bearing_col}' not found in rp_preplot_df columns.")

        # Take common bearing from first non-null value (safer than iloc[0])
        bearing_series = pd.to_numeric(rp_preplot_df[bearing_col], errors="coerce").dropna()
        if bearing_series.empty:
            raise ValueError(f"'{bearing_col}' has no numeric values in rp_preplot_df.")
        bearing_deg = float(bearing_series.iloc[0])

        # Ensure required columns exist in dsr_df
        fx, fy = from_xy
        tx, ty = to_xy
        for c in (fx, fy, tx, ty):
            if c not in dsr_df.columns:
                raise ValueError(f"'{c}' missing in dsr_df; cannot compute offsets.")

        # numeric arrays (NaN-safe)
        from_x = pd.to_numeric(dsr_df[fx], errors="coerce").to_numpy(dtype="float64")
        from_y = pd.to_numeric(dsr_df[fy], errors="coerce").to_numpy(dtype="float64")
        to_x = pd.to_numeric(dsr_df[tx], errors="coerce").to_numpy(dtype="float64")
        to_y = pd.to_numeric(dsr_df[ty], errors="coerce").to_numpy(dtype="float64")

        dx = to_x - from_x
        dy = to_y - from_y

        # Convert bearing (azimuth from North) to unit vectors
        # inline unit vector (east, north) = (sinθ, cosθ)
        th = np.deg2rad(bearing_deg)
        uix, uiy = np.sin(th), np.cos(th)

        # xline unit vector = rotate inline +90° (to the right)
        # (east, north) = (cosθ, -sinθ)
        ux, uy = np.cos(th), -np.sin(th)

        inline_off = dx * uix + dy * uiy
        xline_off = dx * ux + dy * uy

        # Write outputs
        dsr_df[f"{out_prefix}OffE"] = dx
        dsr_df[f"{out_prefix}OffN"] = dy
        dsr_df[f"{out_prefix}OffInline"] = inline_off
        dsr_df[f"{out_prefix}OffXline"] = xline_off

        # Optional: also total offset distance
        dsr_df[f"{out_prefix}OffDist"] = np.sqrt(dx * dx + dy * dy)

        return dsr_df
    def bokeh_two_series_vs_station(
            self,
            df,
            *,
            title="Two Series vs Station",
            x_col="Station",
            series1_col="PrimaryElevation",
            series2_col="SecondaryElevation",
            series1_label=None,
            series2_label=None,
            line_col="Line",
            point_col="Point",
            rov_col="ROV",
            ts_col="TimeStamp",
            require_rov=True,
            y_label="Water depth",
            reverse_y_if_negative=True,
                json_return=False,
            is_show=False,
    ):
        """
        Universal: plot 2 numeric series vs Station (x_col).
        - Optional filter: require_rov=True keeps only rows where ROV is not empty
        - Hover: Line, Point, ROV, TimeStamp + both series values
        - If reverse_y_if_negative and any values < 0 -> reverse Y axis (depth down)
        - sizing_mode="stretch_both" (no width/height)
        - json_item flag returns Bokeh json_item(fig) dict
        - is_show flag calls bokeh.plotting.show(fig)

        Returns:
          - fig (default) OR json_item(fig) if json_item=True
        """
        # --- safe empty
        if df is None or len(df) == 0:
            p = figure(title=title, sizing_mode="stretch_both", tools="pan,wheel_zoom,box_zoom,reset,save")
            out = json_item(p) if json_return else p
            if is_show and not json_return:
                show(p)
            return out

        d = df.copy()

        # --- optional filter by ROV not empty
        if require_rov and rov_col in d.columns:
            d = d[d[rov_col].astype(str).str.strip().ne("")]
        elif require_rov and rov_col not in d.columns:
            d = d.iloc[0:0]

        if len(d) == 0:
            p = figure(title=f"{title} (no rows)", sizing_mode="stretch_both",
                       tools="pan,wheel_zoom,box_zoom,reset,save")
            out = json_item(p) if json_return else p
            if is_show and not json_return:
                show(p)
            return out

        # --- numeric conversion
        for c in (x_col, series1_col, series2_col):
            if c in d.columns:
                d[c] = pd.to_numeric(d[c], errors="coerce")

        d = d.dropna(subset=[x_col])
        d = d.sort_values(by=[x_col])

        # --- hover text fields
        def _safe_str(col_name):
            if col_name in d.columns:
                return d[col_name].astype(str).fillna("")
            return pd.Series([""] * len(d), index=d.index)

        # --- build source
        source = ColumnDataSource(
            data=dict(
                x=d[x_col].to_numpy(),
                s1=d[series1_col].to_numpy() if series1_col in d.columns else np.full(len(d), np.nan),
                s2=d[series2_col].to_numpy() if series2_col in d.columns else np.full(len(d), np.nan),
                line=_safe_str(line_col).to_numpy(),
                point=_safe_str(point_col).to_numpy(),
                rov=_safe_str(rov_col).to_numpy(),
                ts=_safe_str(ts_col).to_numpy(),
            )
        )

        # --- y-range logic (with optional reverse for negative depths)
        y_all = np.concatenate(
            [
                source.data["s1"][np.isfinite(source.data["s1"])],
                source.data["s2"][np.isfinite(source.data["s2"])],
            ]
        )
        if y_all.size == 0:
            y_min, y_max = -1.0, 1.0
        else:
            y_min, y_max = float(np.min(y_all)), float(np.max(y_all))

        pad = (y_max - y_min) * 0.05 if (y_max - y_min) > 0 else (abs(y_min) * 0.05 + 1.0)

        if reverse_y_if_negative and y_min < 0:
            # reverse: start > end
            y_start = max(0.0, y_max + pad)
            y_end = y_min - pad
            y_range = Range1d(start=y_start, end=y_end)
        else:
            y_range = Range1d(start=y_min - pad, end=y_max + pad)

        # --- plot
        p = figure(
            title=title,
            x_axis_label="Station",
            y_axis_label=y_label,
            y_range=y_range,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
            sizing_mode="stretch_both",
        )

        # distinct colors
        c1, c2 = Category10[10][0], Category10[10][1]

        l1 = series1_label or series1_col
        l2 = series2_label or series2_col

        r1 = p.line("x", "s1", source=source, line_width=2, color=c1, legend_label=l1)
        s1 = p.scatter("x", "s1", source=source, size=6, color=c1, legend_label=l1)

        r2 = p.line("x", "s2", source=source, line_width=2, line_dash="dashed", color=c2, legend_label=l2)
        s2 = p.scatter("x", "s2", source=source, size=6, color=c2, legend_label=l2)

        # hover
        p.add_tools(
            HoverTool(
                renderers=[s1, s2],
                mode="mouse",
                tooltips=[
                    ("Line", "@line"),
                    ("Point", "@point"),
                    ("ROV", "@rov"),
                    ("TimeStamp", "@ts"),
                    (l1, "@s1{0.00}"),
                    (l2, "@s2{0.00}"),
                ],
            )
        )

        # legend styling
        p.legend.click_policy = "hide"
        p.legend.location = "top_right"
        p.legend.label_text_font_size = "9pt"
        p.legend.spacing = 2
        p.legend.padding = 4
        p.legend.margin = 4

        # JS controls: toggle legend + move corners
        btn_toggle = Button(label="Legend", button_type="default", width=80)
        btn_move = Button(label="Move", button_type="default", width=70)

        btn_toggle.js_on_click(CustomJS(args=dict(leg=p.legend[0]), code="leg.visible = !leg.visible;"))

        btn_move.js_on_click(
            CustomJS(
                args=dict(leg=p.legend[0]),
                code="""
                    const locs = ["top_right","top_left","bottom_left","bottom_right"];
                    const cur = leg.location || "top_right";
                    const i = locs.indexOf(cur);
                    leg.location = locs[(i + 1) % locs.length];
                """,
            )
        )

        layout = column(row(btn_toggle, btn_move), p, sizing_mode="stretch_both")

        if is_show and not json_return:
            show(layout)

        return json_item(layout) if json_return else layout

    def bokeh_two_series_vbar_vs_station_colorby(
            self,
            df,
            *,
            title="Two Series (vbar) vs Station",
            x_col="Station",
            series1_col="PrimaryElevation",
            series2_col="SecondaryElevation",
            series1_label=None,
            series2_label=None,
            color_column="ROV",
            line_col="Line",
            point_col="Point",
            ts_col="TimeStamp",
            y_label="Water depth",
            reverse_y_if_negative=True,
            json_return=False,
            is_show=False,
    ):
        """
        Universal grouped vbar plot (2 series vs Station).
        Bar colors depend on `color_column` (string categories).

        json_return:
            True  -> return json_item(layout)
            False -> return layout object
        """



        # ---------------- Empty safety ----------------
        if df is None or len(df) == 0:
            p = figure(title=title, sizing_mode="stretch_both")
            layout = column(p, sizing_mode="stretch_both")
            return json_item(layout) if json_return else layout

        d = df.copy()

        # ---------------- Numeric conversion ----------------
        for c in (x_col, series1_col, series2_col):
            if c in d.columns:
                d[c] = pd.to_numeric(d[c], errors="coerce")

        d = d.dropna(subset=[x_col])
        if len(d) == 0:
            p = figure(title=f"{title} (no stations)", sizing_mode="stretch_both")
            layout = column(p, sizing_mode="stretch_both")
            return json_item(layout) if json_return else layout

        # ---------------- Category column ----------------
        if color_column in d.columns:
            d[color_column] = d[color_column].astype(str).fillna("")
        else:
            d[color_column] = ""

        # ---------------- Sorting ----------------
        d = d.sort_values(by=[x_col])

        # ---------------- Station factors ----------------
        stations = d[x_col].astype(str)
        d["_station_factor"] = stations

        categories = sorted(d[color_column].unique().tolist())
        if not categories:
            categories = [""]

        # ---------------- Palette selection ----------------
        def pick_palette(n):
            if n <= 10:
                return list(Category10[10])[:n]
            if n <= 20:
                return list(Category20[20])[:n]
            idx = np.linspace(0, 255, n).astype(int)
            return [Turbo256[i] for i in idx]

        palette = pick_palette(len(categories))

        # ---------------- Data source ----------------
        source = ColumnDataSource(
            data=dict(
                station=d["_station_factor"],
                s1=d[series1_col] if series1_col in d.columns else np.nan,
                s2=d[series2_col] if series2_col in d.columns else np.nan,
                cat=d[color_column],
                line=d[line_col].astype(str) if line_col in d.columns else "",
                point=d[point_col].astype(str) if point_col in d.columns else "",
                ts=d[ts_col].astype(str) if ts_col in d.columns else "",
            )
        )

        # ---------------- Y range ----------------
        y_vals = np.concatenate([
            np.array(source.data["s1"], dtype=float),
            np.array(source.data["s2"], dtype=float),
        ])
        y_vals = y_vals[np.isfinite(y_vals)]

        if len(y_vals) == 0:
            y_min, y_max = -1, 1
        else:
            y_min, y_max = float(np.min(y_vals)), float(np.max(y_vals))

        pad = (y_max - y_min) * 0.05 if (y_max - y_min) > 0 else 1

        if reverse_y_if_negative and y_min < 0:
            y_range = Range1d(start=y_max + pad, end=y_min - pad)
        else:
            y_range = Range1d(start=y_min - pad, end=y_max + pad)

        # ---------------- Figure ----------------
        p = figure(
            title=title,
            x_range=FactorRange(*stations.unique()),
            y_range=y_range,
            x_axis_label="Station",
            y_axis_label=y_label,
            sizing_mode="stretch_both",
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
        )

        cmap = factor_cmap("cat", palette=palette, factors=categories)

        bar_width = 0.35
        offset = 0.2

        l1 = series1_label or series1_col
        l2 = series2_label or series2_col

        v1 = p.vbar(
            x=dodge("station", -offset, range=p.x_range),
            top="s1",
            width=bar_width,
            source=source,
            fill_color=cmap,
            line_color=cmap,
            fill_alpha=0.9,
            legend_label=l1,
        )

        v2 = p.vbar(
            x=dodge("station", +offset, range=p.x_range),
            top="s2",
            width=bar_width,
            source=source,
            fill_color=cmap,
            line_color=cmap,
            fill_alpha=0.45,
            legend_label=l2,
        )

        # ---------------- Hover ----------------
        p.add_tools(
            HoverTool(
                renderers=[v1, v2],
                tooltips=[
                    ("Line", "@line"),
                    ("Point", "@point"),
                    (color_column, "@cat"),
                    ("TimeStamp", "@ts"),
                    ("Station", "@station"),
                    (l1, "@s1{0.00}"),
                    (l2, "@s2{0.00}"),
                ],
            )
        )

        p.legend.click_policy = "hide"
        p.legend.location = "top_right"
        p.legend.label_text_font_size = "9pt"

        # ---------------- JS Buttons ----------------
        btn_toggle = Button(label="Legend", width=80)
        btn_move = Button(label="Move", width=70)

        btn_toggle.js_on_click(CustomJS(args=dict(leg=p.legend[0]), code="""
            leg.visible = !leg.visible;
        """))

        btn_move.js_on_click(CustomJS(args=dict(leg=p.legend[0]), code="""
            const locs = ["top_right","top_left","bottom_left","bottom_right"];
            const i = locs.indexOf(leg.location || "top_right");
            leg.location = locs[(i + 1) % locs.length];
        """))

        layout = column(row(btn_toggle, btn_move), p, sizing_mode="stretch_both")

        if is_show and not json_return:
            show(layout)

        return json_item(layout) if json_return else layout

    def bokeh_one_series_vbar_vs_station_by_category(
            self,
            df,
            *,
            title="Series (vbar) vs Station",
            x_col="Station",
            y_col="PrimaryElevation",
            y_label="Water depth",
            y_name=None,
            category_col="ROV",  # create separate series + legend entry per category (e.g. ROV)
            line_col="Line",
            point_col="Point",
            rov_col="ROV",
            ts_col="TimeStamp",
            require_category=True,  # if True -> keep only rows where category is not empty
            reverse_y_if_negative=True,
            json_return=False,
            is_show=False,
    ):
        """
        One-series vbar vs Station with separate renderers per category (ROV).
        - Each category gets its own legend item (click to hide)
        - Bars are colored per category
        - Hover shows Line, Point, ROV, TimeStamp, Station, Value
        - If negative values exist -> reverse Y axis (depth down)
        - sizing_mode="stretch_both"
        - JS buttons: toggle legend + move legend corners

        Returns:
          layout (default) OR json_item(layout) if json_return=True
        """

        # -------- empty safety
        if df is None or len(df) == 0:
            p = figure(title=title, sizing_mode="stretch_both", tools="pan,wheel_zoom,box_zoom,reset,save")
            layout = column(p, sizing_mode="stretch_both")
            if is_show and not json_return:
                show(layout)
            return json_item(layout) if json_return else layout

        d = df.copy()

        # -------- numeric
        if x_col in d.columns:
            d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        if y_col in d.columns:
            d[y_col] = pd.to_numeric(d[y_col], errors="coerce")

        d = d.dropna(subset=[x_col])
        if len(d) == 0:
            p = figure(title=f"{title} (no stations)", sizing_mode="stretch_both",
                       tools="pan,wheel_zoom,box_zoom,reset,save")
            layout = column(p, sizing_mode="stretch_both")
            if is_show and not json_return:
                show(layout)
            return json_item(layout) if json_return else layout

        # -------- categories
        if category_col not in d.columns:
            # fallback to single category
            d[category_col] = ""
        else:
            d[category_col] = d[category_col].astype(str).fillna("")

        if require_category:
            d = d[d[category_col].astype(str).str.strip().ne("")]

        if len(d) == 0:
            p = figure(title=f"{title} (no {category_col})", sizing_mode="stretch_both",
                       tools="pan,wheel_zoom,box_zoom,reset,save")
            layout = column(p, sizing_mode="stretch_both")
            if is_show and not json_return:
                show(layout)
            return json_item(layout) if json_return else layout

        # -------- sort
        d = d.sort_values(by=[x_col])

        # station factors (categorical to align bars perfectly)
        stations = d[x_col].to_numpy()
        station_factors = [str(int(s)) if float(s).is_integer() else str(s) for s in stations]
        d["_station_factor"] = station_factors

        # y-range
        y_arr = pd.to_numeric(d[y_col], errors="coerce").to_numpy(dtype=float) if y_col in d.columns else np.full(
            len(d), np.nan)
        y_vals = y_arr[np.isfinite(y_arr)]
        if y_vals.size == 0:
            y_min, y_max = -1.0, 1.0
        else:
            y_min, y_max = float(np.min(y_vals)), float(np.max(y_vals))
        pad = (y_max - y_min) * 0.05 if (y_max - y_min) > 0 else (abs(y_min) * 0.05 + 1.0)

        if reverse_y_if_negative and y_min < 0:
            y_range = Range1d(start=y_max + pad, end=y_min - pad)  # reversed
        else:
            y_range = Range1d(start=y_min - pad, end=y_max + pad)

        # figure
        p = figure(
            title=title,
            x_range=FactorRange(*d["_station_factor"].unique().tolist()),
            y_range=y_range,
            x_axis_label="Station",
            y_axis_label=y_label,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
            sizing_mode="stretch_both",
        )
        p.xgrid.grid_line_alpha = 0.15
        p.ygrid.grid_line_alpha = 0.15

        # palette (simple, repeats if more categories)
        base_palette = [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
            "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
        ]

        # hover (we will attach to all renderers)
        hover = HoverTool(
            mode="mouse",
            tooltips=[
                ("Line", "@line"),
                ("Point", "@point"),
                ("ROV", "@rov"),
                ("TimeStamp", "@ts"),
                ("Station", "@station"),
                ((y_name or y_col), "@y{0.00}"),
            ],
        )
        p.add_tools(hover)

        # build one vbar series per category
        categories = sorted(d[category_col].unique().tolist())

        renderers = []
        legend_label_prefix = y_name or y_col

        for i, cat in enumerate(categories):
            sub = d[d[category_col] == cat]
            if len(sub) == 0:
                continue

            src = ColumnDataSource(
                data=dict(
                    station=sub["_station_factor"].to_numpy(),
                    y=pd.to_numeric(sub[y_col], errors="coerce").to_numpy(),
                    rov=sub[rov_col].astype(str).fillna("").to_numpy() if rov_col in sub.columns else np.array(
                        [""] * len(sub)),
                    ts=sub[ts_col].astype(str).fillna("").to_numpy() if ts_col in sub.columns else np.array(
                        [""] * len(sub)),
                    line=sub[line_col].astype(str).fillna("").to_numpy() if line_col in sub.columns else np.array(
                        [""] * len(sub)),
                    point=sub[point_col].astype(str).fillna("").to_numpy() if point_col in sub.columns else np.array(
                        [""] * len(sub)),
                )
            )

            color = base_palette[i % len(base_palette)]
            label = f"{legend_label_prefix} ({cat})" if cat else legend_label_prefix

            r = p.vbar(
                x="station",
                top="y",
                width=0.8,
                source=src,
                fill_color=color,
                line_color=color,
                fill_alpha=0.85,
                legend_label=label,
            )
            renderers.append(r)

        # apply hover to all bars
        hover.renderers = renderers

        # legend behaviour
        p.legend.click_policy = "hide"
        p.legend.location = "top_right"
        p.legend.label_text_font_size = "9pt"
        p.legend.spacing = 2
        p.legend.padding = 4
        p.legend.margin = 4

        # JS buttons
        btn_toggle = Button(label="Legend", button_type="default", width=80)
        btn_move = Button(label="Move", button_type="default", width=70)

        btn_toggle.js_on_click(CustomJS(args=dict(leg=p.legend[0]), code="leg.visible = !leg.visible;"))
        btn_move.js_on_click(CustomJS(args=dict(leg=p.legend[0]), code="""
            const locs = ["top_right","top_left","bottom_left","bottom_right"];
            const cur = leg.location || "top_right";
            const i = locs.indexOf(cur);
            leg.location = locs[(i + 1) % locs.length];
        """))

        layout = column(row(btn_toggle, btn_move), p, sizing_mode="stretch_both")

        if is_show and not json_return:
            show(layout)

        return json_item(layout) if json_return else layout

    def bokeh_three_vbar_by_category_shared_x(
            self,
            df,
            *,
            title1="Series 1",
            title2="Series 2",
            title3="Series 3",
            x_col="Station",
            y1_col="PrimaryElevation",
            y2_col="SecondaryElevation",
            y3_col="WaterDepth",
            y1_label=None,
            y2_label=None,
            y3_label=None,
            y_axis_label="Value",
            category_col="ROV",
            line_col="Line",
            point_col=None,
            rov_col="ROV",
            ts_col="TimeStamp",
            require_category=True,
            reverse_y_if_negative=True,
            legend_title="Legend",
            x_tick_step=5,
            x_tick_font_size="8pt",

            p1_line1_col=None,
            p1_line2_col=None,
            p2_line1_col=None,
            p2_line2_col=None,
            p3_line1_col=None,
            p3_line2_col=None,

            p1_line1_label=None,
            p1_line2_label=None,
            p2_line1_label=None,
            p2_line2_label=None,
            p3_line1_label=None,
            p3_line2_label=None,

            p1_line1_color="#d62728",
            p1_line2_color="#1f77b4",
            p2_line1_color="#2ca02c",
            p2_line2_color="#ff7f0e",
            p3_line1_color="#9467bd",
            p3_line2_color="#8c564b",

            line_line_width=2,
            line_marker_size=6,

            json_return=False,
            is_show=False,
    ):
        """
        Three stacked Bokeh plots with shared categorical X axis.

        Each plot contains:
          - one vbar series colored by category_col
          - two optional line+circle series specific to that plot

        If no line columns are provided, only bars are drawn.
        """

        def _err(msg: str, details: str = ""):
            try:
                return self._error_layout(
                    title="Plot build error",
                    message=msg,
                    details=details,
                    level="error",
                    json_return=json_return,
                    is_show=is_show,
                )
            except TypeError:
                try:
                    return self._error_layout(
                        "Plot build error",
                        msg,
                        details=details,
                        json_return=json_return,
                        is_show=is_show,
                    )
                except Exception:
                    return self._error_layout(msg)

        def _to_num(series):
            return pd.to_numeric(series, errors="coerce")

        def _finite_count(frame, col):
            if not col or col not in frame.columns:
                return 0
            arr = _to_num(frame[col]).to_numpy(dtype=float)
            return int(np.isfinite(arr).sum())

        def _label_or_col(label, col, fallback):
            return label or col or fallback

        def _series_to_str(frame, col, default=""):
            if col and col in frame.columns:
                return frame[col].fillna(default).astype(str)
            return pd.Series([default] * len(frame), index=frame.index)

        def _make_y_range(frame, main_y_col, extra_cols=None):
            cols = []
            if main_y_col and main_y_col in frame.columns:
                cols.append(main_y_col)
            if extra_cols:
                cols.extend([c for c in extra_cols if c and c in frame.columns])

            vals = []
            for c in cols:
                arr = _to_num(frame[c]).to_numpy(dtype=float)
                arr = arr[np.isfinite(arr)]
                if arr.size:
                    vals.append(arr)

            if not vals:
                return Range1d(start=-1, end=1)

            arr = np.concatenate(vals)
            y_min = float(np.min(arr))
            y_max = float(np.max(arr))

            if y_min == y_max:
                pad = abs(y_min) * 0.05 + 1.0
            else:
                pad = (y_max - y_min) * 0.05

            if reverse_y_if_negative and y_min < 0:
                return Range1d(start=y_max + pad, end=y_min - pad)

            return Range1d(start=y_min - pad, end=y_max + pad)

        def _build_line_source(frame, l1_col, l2_col):
            return ColumnDataSource(data=dict(
                station=frame["_station_factor"].tolist(),
                line1=_to_num(frame[l1_col]).tolist() if l1_col and l1_col in frame.columns else [np.nan] * len(frame),
                line2=_to_num(frame[l2_col]).tolist() if l2_col and l2_col in frame.columns else [np.nan] * len(frame),
                rov=_series_to_str(frame, rov_col).tolist(),
                ts=_series_to_str(frame, ts_col).tolist(),
                line=_series_to_str(frame, line_col).tolist(),
                point=_series_to_str(frame, point_col).tolist() if point_col else [""] * len(frame),
            ))

        def _build_bar_source(frame, y_col):
            return ColumnDataSource(data=dict(
                station=frame["_station_factor"].tolist(),
                y=_to_num(frame[y_col]).tolist(),
                rov=_series_to_str(frame, rov_col).tolist(),
                ts=_series_to_str(frame, ts_col).tolist(),
                line=_series_to_str(frame, line_col).tolist(),
                point=_series_to_str(frame, point_col).tolist() if point_col else [""] * len(frame),
                cat=_series_to_str(frame, category_col).tolist(),
            ))

        def _build_one_plot(
                *,
                plot_title,
                y_col,
                y_label_text,
                line1_col,
                line2_col,
                line1_label,
                line2_label,
                line1_color,
                line2_color,
        ):
            if y_col not in d.columns:
                p = figure(
                    title=f"{plot_title} (missing '{y_col}')",
                    x_range=shared_x,
                    sizing_mode="stretch_both",
                    tools="pan,wheel_zoom,box_zoom,reset,save",
                    active_scroll="wheel_zoom",
                    min_border_right=35,
                )
                return p, []

            if _finite_count(d, y_col) == 0:
                p = figure(
                    title=f"{plot_title} (no numeric values in '{y_col}')",
                    x_range=shared_x,
                    sizing_mode="stretch_both",
                    tools="pan,wheel_zoom,box_zoom,reset,save",
                    active_scroll="wheel_zoom",
                    min_border_right=35,
                )
                return p, []

            p = figure(
                title=plot_title,
                x_range=shared_x,
                y_range=_make_y_range(d, y_col, [line1_col, line2_col]),
                x_axis_label="Station",
                y_axis_label=y_axis_label or y_label_text,
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
                sizing_mode="stretch_both",
                min_border_right=35,
            )

            p.xgrid.grid_line_alpha = 0.15
            p.ygrid.grid_line_alpha = 0.15
            p.xaxis.major_label_orientation = 1.5708
            p.xaxis.major_label_text_font_size = x_tick_font_size
            p.xaxis.major_label_standoff = 6
            p.xaxis.major_label_overrides = x_label_overrides

            renderers = []
            legend_items = []

            for cat in categories:
                sub = d[d[category_col] == cat].copy()
                if sub.empty:
                    continue

                src_bar = _build_bar_source(sub, y_col)
                color = cat_color.get(cat, "#94a3b8")

                r_bar = p.vbar(
                    x="station",
                    top="y",
                    width=0.82,
                    source=src_bar,
                    fill_color=color,
                    line_color=color,
                    fill_alpha=0.85,
                )
                renderers.append(r_bar)
                legend_items.append(
                    LegendItem(label=f"{y_label_text} · {cat or 'N/A'}", renderers=[r_bar])
                )

            src_line = _build_line_source(d, line1_col, line2_col)

            if line1_col and line1_col in d.columns and _finite_count(d, line1_col) > 0:
                r_l1 = p.line(
                    x="station",
                    y="line1",
                    source=src_line,
                    line_width=line_line_width,
                    color=line1_color,
                    alpha=0.95,
                )
                r_l1c = p.circle(
                    x="station",
                    y="line1",
                    source=src_line,
                    size=line_marker_size,
                    color=line1_color,
                    alpha=0.95,
                )
                renderers.extend([r_l1, r_l1c])
                legend_items.append(
                    LegendItem(
                        label=_label_or_col(line1_label, line1_col, "Line 1"),
                        renderers=[r_l1, r_l1c]
                    )
                )

            if line2_col and line2_col in d.columns and _finite_count(d, line2_col) > 0:
                r_l2 = p.line(
                    x="station",
                    y="line2",
                    source=src_line,
                    line_width=line_line_width,
                    color=line2_color,
                    alpha=0.95,
                )
                r_l2c = p.circle(
                    x="station",
                    y="line2",
                    source=src_line,
                    size=line_marker_size,
                    color=line2_color,
                    alpha=0.95,
                )
                renderers.extend([r_l2, r_l2c])
                legend_items.append(
                    LegendItem(
                        label=_label_or_col(line2_label, line2_col, "Line 2"),
                        renderers=[r_l2, r_l2c]
                    )
                )

            tooltips = [
                ("Line", "@line"),
                ("Point", "@point"),
                ("ROV", "@rov"),
                ("TimeStamp", "@ts"),
                ("Station", "@station"),
                (y_label_text, "@y{0.00}"),
            ]

            if line1_col and line1_col in d.columns:
                tooltips.append((_label_or_col(line1_label, line1_col, "Line 1"), "@line1{0.00}"))
            if line2_col and line2_col in d.columns:
                tooltips.append((_label_or_col(line2_label, line2_col, "Line 2"), "@line2{0.00}"))

            hover = HoverTool(
                mode="mouse",
                renderers=renderers,
                tooltips=tooltips,
            )
            p.add_tools(hover)

            return p, legend_items

        if df is None:
            return _err("No dataframe provided.")
        if not hasattr(df, "columns"):
            return _err("Input must be a pandas DataFrame.")
        if len(df) == 0:
            return _err("No data to plot (dataframe is empty).")

        d = df.copy()

        if x_col not in d.columns:
            return _err(f"Missing required x column: '{x_col}'.")

        y_cols_existing = [c for c in [y1_col, y2_col, y3_col] if c in d.columns]
        if not y_cols_existing:
            return _err(f"None of Y columns exist: '{y1_col}', '{y2_col}', '{y3_col}'.")

        if category_col not in d.columns:
            d[category_col] = ""
        d[category_col] = d[category_col].fillna("").astype(str).str.strip()

        d[x_col] = _to_num(d[x_col])
        d = d.dropna(subset=[x_col]).copy()
        if d.empty:
            return _err(f"No valid numeric values in '{x_col}'.")

        numeric_cols = [
            y1_col, y2_col, y3_col,
            p1_line1_col, p1_line2_col,
            p2_line1_col, p2_line2_col,
            p3_line1_col, p3_line2_col,
        ]
        for c in numeric_cols:
            if c and c in d.columns:
                d[c] = _to_num(d[c])

        if require_category:
            d = d[d[category_col].ne("")].copy()
            if d.empty:
                return _err(f"No rows with non-empty '{category_col}' after filtering.")

        if (
                _finite_count(d, y1_col) +
                _finite_count(d, y2_col) +
                _finite_count(d, y3_col)
        ) == 0:
            return _err("No numeric Y values available to plot (all Y columns are empty/NaN).")

        d = d.sort_values(by=[x_col]).reset_index(drop=True)

        stations = d[x_col].to_numpy(dtype=float)
        d["_station_factor"] = [
            str(int(v)) if float(v).is_integer() else str(v)
            for v in stations
        ]

        x_factors = d["_station_factor"].drop_duplicates().tolist()
        if not x_factors:
            return _err("No stations available after processing.")

        shared_x = FactorRange(*x_factors)

        try:
            step = max(1, int(x_tick_step))
        except Exception:
            step = 1

        x_label_overrides = {
            fac: (fac if (i % step == 0) else "")
            for i, fac in enumerate(x_factors)
        }

        base_palette = [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
            "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
            "#393b79", "#637939", "#8c6d31", "#843c39", "#7b4173",
            "#3182bd", "#31a354", "#756bb1", "#636363", "#e6550d",
        ]

        categories = sorted([
            c for c in d[category_col].dropna().astype(str).unique().tolist()
            if c != ""
        ])

        if not categories:
            categories = [""]

        cat_color = {
            cat: base_palette[i % len(base_palette)]
            for i, cat in enumerate(categories)
        }
        cat_color[""] = "#94a3b8"

        s1_label = y1_label or y1_col
        s2_label = y2_label or y2_col
        s3_label = y3_label or y3_col

        p1, leg1 = _build_one_plot(
            plot_title=title1,
            y_col=y1_col,
            y_label_text=s1_label,
            line1_col=p1_line1_col,
            line2_col=p1_line2_col,
            line1_label=p1_line1_label,
            line2_label=p1_line2_label,
            line1_color=p1_line1_color,
            line2_color=p1_line2_color,
        )

        p2, leg2 = _build_one_plot(
            plot_title=title2,
            y_col=y2_col,
            y_label_text=s2_label,
            line1_col=p2_line1_col,
            line2_col=p2_line2_col,
            line1_label=p2_line1_label,
            line2_label=p2_line2_label,
            line1_color=p2_line1_color,
            line2_color=p2_line2_color,
        )

        p3, leg3 = _build_one_plot(
            plot_title=title3,
            y_col=y3_col,
            y_label_text=s3_label,
            line1_col=p3_line1_col,
            line2_col=p3_line2_col,
            line1_label=p3_line1_label,
            line2_label=p3_line2_label,
            line1_color=p3_line1_color,
            line2_color=p3_line2_color,
        )

        p1.xaxis.visible = False
        p2.xaxis.visible = False

        all_leg_items = leg1 + leg2 + leg3
        if all_leg_items:
            legend = Legend(
                items=all_leg_items,
                title=legend_title,
                orientation="horizontal",
                click_policy="hide",
                spacing=6,
                padding=6,
                margin=4,
                label_text_font_size="9pt",
                title_text_font_size="9pt",
                ncols=5,
            )
            p1.add_layout(legend, "above")
        else:
            legend = None

        stack = gridplot(
            [[p1], [p2], [p3]],
            merge_tools=True,
            toolbar_location="above",
            sizing_mode="stretch_both",
        )

        btn_toggle = Button(label="Legend", button_type="default", width=80)
        if legend is not None:
            btn_toggle.js_on_click(CustomJS(args=dict(leg=legend), code="leg.visible = !leg.visible;"))
        else:
            btn_toggle.disabled = True

        layout = column(
            row(btn_toggle),
            stack,
            sizing_mode="stretch_both",
        )

        if is_show and not json_return:
            show(layout)

        return json_item(layout) if json_return else layout

    def make_dxdy_primary_secondary_with_hists(
            self,
            df,
            dx_p_col="dX_primary",
            dy_p_col="dY_primary",
            dx_s_col="dX_secondary",
            dy_s_col="dY_secondary",
            # hover meta
            line_col="Line",
            station_col="Station",
            rov_col="ROV",
            deploy_time_col="TimeStamp",
            range_to_preplot_col="RangeToPreplot",
            sma_col="SMA95",
            p_sma_e95_col="Primary_e95",
            p_sma_n95_col="Primary_n95",
            s_sma_e95_col="Secondary_e95",
            s_sma_n95_col="Secondary_n95",
            title="DSR dX/dY (Primary & Secondary)",
            p_name="Primary",
            s_name="Secondary",
            # circles
            red_radius=None,
            red_is_show=True,
            show_percentile_circles=True,
            extra_circle_radii=None,
            extra_circle_colors=None,
            extra_circle_labels=None,
            # display scaling
            red_radius_mode="max",  # "max" | "fixed"
            display_radius_mode="p95",  # "max" | "p95" | "p99" | "fixed"
            display_radius=None,
            display_radius_pad_ratio=0.20,
            max_to_display_ratio_for_clip=1.5,
            show_outside_max_arrow=True,
            # hist
            bins=30,
            hist_show_std=True,
            hist_show_kde=True,
            kde_points=200,
            kde_bandwidth=None,
            top_bottom_hist_height=210,
            side_hist_width=140,
            # visual
            show_station_labels=True,
            station_font_size="7pt",
            max_station_labels=None,
            connect_pairs=True,
            pair_line_width=2,
            pair_line_alpha=0.75,
            show_pair_heatmap=True,
            show_worst_station=True,
            show_colorbar=True,
            colorbar_label="Pair offset, m",
            point_size=6,
            p50_circle_color="#2ca02c",
            p95_circle_color="#9467bd",
            red_circle_color="#d62728",
            primary_color="#1f77b4",
            secondary_color="#ff7f0e",
            # controls
            show_controls=True,
            # output
            is_show=False,
            json_return=False,
            target_id="dxdy_plot",
    ):
        try:
            if df is None:
                return self._error_layout(
                    title="DX/DY Plot Error",
                    message="DataFrame is None.",
                    level="error",
                    is_show=is_show,
                    json_return=json_return,
                )

            if df.empty:
                return self._error_layout(
                    title="DX/DY Plot Error",
                    message="DataFrame is empty.",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            required_cols = [dx_p_col, dy_p_col, dx_s_col, dy_s_col]
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                return self._error_layout(
                    title="DX/DY Plot Error",
                    message="Missing required columns.",
                    details=", ".join(missing),
                    level="error",
                    is_show=is_show,
                    json_return=json_return,
                )

            hover_cols = [
                line_col, station_col, rov_col, deploy_time_col, range_to_preplot_col,
                sma_col, p_sma_e95_col, p_sma_n95_col, s_sma_e95_col, s_sma_n95_col,
            ]
            hover_present = {c: (c in df.columns) for c in hover_cols}

            def _safe_str(v):
                if v is None:
                    return ""
                try:
                    s = str(v)
                    return "" if s == "nan" else s
                except Exception:
                    return ""

            def _safe_float(v):
                try:
                    if v is None:
                        return None
                    vv = float(v)
                    if vv != vv:
                        return None
                    return vv
                except Exception:
                    return None

            def _safe_radius(value):
                try:
                    if value is None:
                        return None
                    v = float(value)
                    if v != v or v <= 0:
                        return None
                    return v
                except Exception:
                    return None

            def _mean_std(vals):
                vals2 = [float(v) for v in vals if _safe_float(v) is not None]
                n = len(vals2)
                if n == 0:
                    return 0.0, 0.0
                m = sum(vals2) / float(n)
                if n < 2:
                    return m, 0.0
                s2 = 0.0
                for v in vals2:
                    d = v - m
                    s2 += d * d
                s2 /= float(n - 1)
                return m, s2 ** 0.5

            def _percentile(vals, q):
                vals2 = sorted([float(v) for v in vals if _safe_float(v) is not None])
                n = len(vals2)
                if n == 0:
                    return None
                if n == 1:
                    return vals2[0]
                pos = (n - 1) * q
                lo = int(pos)
                hi = min(lo + 1, n - 1)
                frac = pos - lo
                return vals2[lo] * (1.0 - frac) + vals2[hi] * frac

            def _hist(vals, bins_count, forced_min=None, forced_max=None):
                vals2 = [float(v) for v in vals if _safe_float(v) is not None]
                if not vals2:
                    return {"left": [], "right": [], "top": [], "bin_width": 1.0, "n": 0}

                vmin = min(vals2) if forced_min is None else float(forced_min)
                vmax = max(vals2) if forced_max is None else float(forced_max)

                if vmin == vmax:
                    vmin -= 0.5
                    vmax += 0.5

                step = (vmax - vmin) / float(bins_count)
                counts = [0] * bins_count

                for v in vals2:
                    idx = int((v - vmin) / step)
                    if idx < 0:
                        idx = 0
                    if idx >= bins_count:
                        idx = bins_count - 1
                    counts[idx] += 1

                left = [vmin + i * step for i in range(bins_count)]
                right = [vmin + (i + 1) * step for i in range(bins_count)]
                return {"left": left, "right": right, "top": counts, "bin_width": step, "n": len(vals2)}

            def _kde_gaussian(vals, x_grid, bw):
                n = len(vals)
                if n == 0:
                    return [0.0 for _ in x_grid]
                if bw <= 0:
                    bw = 1.0
                inv = 1.0 / (n * bw)
                c = 0.3989422804014327
                e = 2.718281828459045
                out = []
                for x in x_grid:
                    s = 0.0
                    for v in vals:
                        z = (x - v) / bw
                        s += c * (e ** (-0.5 * z * z))
                    out.append(inv * s)
                return out

            def _kde_scaled_to_counts(vals, bins_count, grid_n, bw_override=None, forced_min=None, forced_max=None):
                vals2 = [float(v) for v in vals if _safe_float(v) is not None]
                if not vals2:
                    return [], []

                vmin = min(vals2) if forced_min is None else float(forced_min)
                vmax = max(vals2) if forced_max is None else float(forced_max)

                if vmin == vmax:
                    vmin -= 0.5
                    vmax += 0.5

                if grid_n < 50:
                    grid_n = 50

                step = (vmax - vmin) / float(grid_n - 1)
                x_grid = [vmin + i * step for i in range(grid_n)]

                bw = None
                if bw_override is not None:
                    try:
                        bw = float(bw_override)
                    except Exception:
                        bw = None

                _, sd = _mean_std(vals2)
                n = len(vals2)
                if bw is None:
                    if sd <= 0 or n < 2:
                        bw = (vmax - vmin) / 20.0 if (vmax - vmin) > 0 else 1.0
                    else:
                        bw = 1.06 * sd * (n ** (-0.2))

                dens = _kde_gaussian(vals2, x_grid, bw)
                bin_width = (vmax - vmin) / float(bins_count) if bins_count > 0 else 1.0
                y_scaled = [d * n * bin_width for d in dens]
                return x_grid, y_scaled

            def _build_source(dff, xcol, ycol, kind):
                x = []
                y = []
                Line = []
                Station = []
                ROV = []
                DeployTime = []
                RangeToPreplot = []
                SMA95 = []
                Primary_e95 = []
                Primary_n95 = []
                Secondary_e95 = []
                Secondary_n95 = []

                for _, r in dff.iterrows():
                    fx = _safe_float(r.get(xcol))
                    fy = _safe_float(r.get(ycol))
                    if fx is None or fy is None:
                        continue

                    x.append(fx)
                    y.append(fy)
                    Line.append(_safe_str(r.get(line_col)) if hover_present.get(line_col) else "")
                    Station.append(_safe_str(r.get(station_col)) if hover_present.get(station_col) else "")
                    ROV.append(_safe_str(r.get(rov_col)) if hover_present.get(rov_col) else "")
                    DeployTime.append(_safe_str(r.get(deploy_time_col)) if hover_present.get(deploy_time_col) else "")
                    RangeToPreplot.append(
                        _safe_str(r.get(range_to_preplot_col)) if hover_present.get(range_to_preplot_col) else "")
                    SMA95.append(_safe_str(r.get(sma_col)) if hover_present.get(sma_col) else "")
                    Primary_e95.append(_safe_str(r.get(p_sma_e95_col)) if hover_present.get(p_sma_e95_col) else "")
                    Primary_n95.append(_safe_str(r.get(p_sma_n95_col)) if hover_present.get(p_sma_n95_col) else "")
                    Secondary_e95.append(_safe_str(r.get(s_sma_e95_col)) if hover_present.get(s_sma_e95_col) else "")
                    Secondary_n95.append(_safe_str(r.get(s_sma_n95_col)) if hover_present.get(s_sma_n95_col) else "")

                if not x:
                    return None

                return ColumnDataSource(data=dict(
                    x=x, y=y,
                    kind=[kind] * len(x),
                    Line=Line,
                    Station=Station,
                    ROV=ROV,
                    DeployTime=DeployTime,
                    RangeToPreplot=RangeToPreplot,
                    SMA95=SMA95,
                    Primary_e95=Primary_e95,
                    Primary_n95=Primary_n95,
                    Secondary_e95=Secondary_e95,
                    Secondary_n95=Secondary_n95,
                ))

            keep_hover = [c for c in hover_cols if hover_present.get(c)]
            dpf = df[[dx_p_col, dy_p_col] + keep_hover].copy().dropna(subset=[dx_p_col, dy_p_col])
            dsf = df[[dx_s_col, dy_s_col] + keep_hover].copy().dropna(subset=[dx_s_col, dy_s_col])

            if dpf.empty and dsf.empty:
                return self._error_layout(
                    title="DX/DY Plot Error",
                    message="No valid points after dropping NaNs.",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            src_p = _build_source(dpf, dx_p_col, dy_p_col, p_name)
            src_s = _build_source(dsf, dx_s_col, dy_s_col, s_name)

            pair_x0 = []
            pair_y0 = []
            pair_x1 = []
            pair_y1 = []
            pair_len = []
            pair_station = []
            pair_label_x = []
            pair_label_y = []
            pair_label = []

            rp_all = []
            rs_all = []
            pair_max_all = []

            for _, r in df.iterrows():
                xpv = _safe_float(r.get(dx_p_col))
                ypv = _safe_float(r.get(dy_p_col))
                xsv = _safe_float(r.get(dx_s_col))
                ysv = _safe_float(r.get(dy_s_col))

                rp = None
                rs = None
                if xpv is not None and ypv is not None:
                    rp = (xpv * xpv + ypv * ypv) ** 0.5
                    rp_all.append(rp)
                if xsv is not None and ysv is not None:
                    rs = (xsv * xsv + ysv * ysv) ** 0.5
                    rs_all.append(rs)

                pm = None
                if rp is not None and rs is not None:
                    pm = max(rp, rs)
                elif rp is not None:
                    pm = rp
                elif rs is not None:
                    pm = rs
                if pm is not None:
                    pair_max_all.append(pm)

                if xpv is not None and ypv is not None and xsv is not None and ysv is not None:
                    dxp = xsv - xpv
                    dyp = ysv - ypv
                    plen = (dxp * dxp + dyp * dyp) ** 0.5

                    xm = (xpv + xsv) / 2.0
                    ym = (ypv + ysv) / 2.0

                    if plen > 0:
                        ox = -dyp / plen
                        oy = dxp / plen
                    else:
                        ox = 0.0
                        oy = 0.0

                    pair_x0.append(xpv)
                    pair_y0.append(ypv)
                    pair_x1.append(xsv)
                    pair_y1.append(ysv)
                    pair_len.append(plen)
                    pair_station.append(_safe_str(r.get(station_col)) if hover_present.get(station_col) else "")
                    pair_label_x.append(xm + 0.06 * ox)
                    pair_label_y.append(ym + 0.06 * oy)
                    pair_label.append(_safe_str(r.get(station_col)) if hover_present.get(station_col) else "")

            if src_p is None and src_s is None:
                return self._error_layout(
                    title="DX/DY Plot Error",
                    message="No numeric points could be parsed.",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            if red_radius_mode == "fixed":
                circle_radius = _safe_radius(red_radius)
            else:
                circle_radius = max(pair_max_all) if pair_max_all else None

            p50_radius = _percentile(pair_max_all, 0.50) if show_percentile_circles else None
            p95_radius = _percentile(pair_max_all, 0.95) if show_percentile_circles else None
            p99_radius = _percentile(pair_max_all, 0.99) if pair_max_all else None

            if display_radius_mode == "max":
                plot_radius = _safe_radius(circle_radius)
            elif display_radius_mode == "p95":
                plot_radius = _safe_radius(p95_radius)
            elif display_radius_mode == "p99":
                plot_radius = _safe_radius(p99_radius)
            elif display_radius_mode == "fixed":
                plot_radius = _safe_radius(display_radius)
            else:
                plot_radius = _safe_radius(p95_radius) or _safe_radius(circle_radius)

            if plot_radius is None:
                allr = rp_all + rs_all
                plot_radius = _percentile(allr, 0.95) if allr else 1.0

            plot_radius = max(float(plot_radius), 1.0)
            plot_lim = plot_radius * (1.0 + float(display_radius_pad_ratio))

            worst_p_x = []
            worst_p_y = []
            worst_s_x = []
            worst_s_y = []

            if pair_max_all:
                worst_idx = None
                worst_val = -1.0

                for i, (_, r) in enumerate(df.iterrows()):
                    xpv = _safe_float(r.get(dx_p_col))
                    ypv = _safe_float(r.get(dy_p_col))
                    xsv = _safe_float(r.get(dx_s_col))
                    ysv = _safe_float(r.get(dy_s_col))

                    rp = (xpv * xpv + ypv * ypv) ** 0.5 if xpv is not None and ypv is not None else None
                    rs = (xsv * xsv + ysv * ysv) ** 0.5 if xsv is not None and ysv is not None else None

                    pm = None
                    if rp is not None and rs is not None:
                        pm = max(rp, rs)
                    elif rp is not None:
                        pm = rp
                    elif rs is not None:
                        pm = rs

                    if pm is not None and pm > worst_val:
                        worst_val = pm
                        worst_idx = i

                if worst_idx is not None:
                    wr = df.iloc[worst_idx]
                    xp_w = _safe_float(wr.get(dx_p_col))
                    yp_w = _safe_float(wr.get(dy_p_col))
                    xs_w = _safe_float(wr.get(dx_s_col))
                    ys_w = _safe_float(wr.get(dy_s_col))

                    if xp_w is not None and yp_w is not None:
                        worst_p_x = [xp_w]
                        worst_p_y = [yp_w]
                    if xs_w is not None and ys_w is not None:
                        worst_s_x = [xs_w]
                        worst_s_y = [ys_w]

            hover = HoverTool(tooltips=[
                ("Series", "@kind"),
                ("Line", "@Line"),
                ("Station", "@Station"),
                ("ROV", "@ROV"),
                ("Deploy Time", "@DeployTime"),
                ("RangeToPreplot", "@RangeToPreplot"),
                ("SMA95", "@SMA95"),
                ("Primary SMA95 (e95,n95)", "(@Primary_e95, @Primary_n95)"),
                ("Secondary SMA95 (e95,n95)", "(@Secondary_e95, @Secondary_n95)"),
                ("dX", "@x{0.00}"),
                ("dY", "@y{0.00}"),
            ])

            p = figure(
                title="",
                x_axis_label="dX, m",
                y_axis_label="dY, m",
                x_range=Range1d(-plot_lim, plot_lim),
                y_range=Range1d(-plot_lim, plot_lim),
                match_aspect=True,
                tools=[hover, "pan", "wheel_zoom", "box_zoom", "reset", "save"],
                active_scroll="wheel_zoom",
                sizing_mode="stretch_both",
                toolbar_location="above",
            )

            p.add_layout(Span(location=0, dimension="width", line_width=1, line_color="#1565c0"))
            p.add_layout(Span(location=0, dimension="height", line_width=1, line_color="#1565c0"))

            title_parts = [title]
            if circle_radius is not None:
                title_parts.append(f"Max={circle_radius:.2f} m")
            if p50_radius is not None:
                title_parts.append(f"P50={p50_radius:.2f} m")
            if p95_radius is not None:
                title_parts.append(f"P95={p95_radius:.2f} m")
            title_parts.append(f"Display={plot_radius:.2f} m")
            p.title.text = " | ".join(title_parts)
            p.title.text_font_size = "14pt"

            pair_src = ColumnDataSource(data=dict(
                x0=pair_x0, y0=pair_y0, x1=pair_x1, y1=pair_y1,
                pair_len=pair_len, Station=pair_station,
            ))

            line_mapper = LinearColorMapper(
                palette=Turbo256,
                low=min(pair_len) if pair_len else 0.0,
                high=max(pair_len) if pair_len else 1.0,
            )

            if connect_pairs and pair_x0:
                if show_pair_heatmap:
                    p.segment(
                        x0="x0", y0="y0", x1="x1", y1="y1",
                        source=pair_src,
                        line_width=pair_line_width,
                        line_alpha=pair_line_alpha,
                        line_color=transform("pair_len", line_mapper),
                    )
                else:
                    p.segment(
                        x0="x0", y0="y0", x1="x1", y1="y1",
                        source=pair_src,
                        line_width=pair_line_width,
                        line_alpha=pair_line_alpha,
                        line_color="#777777",
                    )

            r_primary = None
            r_secondary = None

            if src_p is not None:
                r_primary = p.circle(
                    "x", "y",
                    source=src_p,
                    size=point_size,
                    color=primary_color,
                    alpha=0.95,
                    legend_label=p_name,
                )

            if src_s is not None:
                r_secondary = p.triangle(
                    "x", "y",
                    source=src_s,
                    size=point_size + 1,
                    color=secondary_color,
                    alpha=0.95,
                    legend_label=s_name,
                )

            hover.renderers = [r for r in [r_primary, r_secondary] if r is not None]

            if show_percentile_circles and p50_radius is not None:
                p.circle(
                    x=[0], y=[0], radius=[p50_radius],
                    radius_units="data",
                    fill_alpha=0.0,
                    line_color=p50_circle_color,
                    line_dash="dashed",
                    line_width=2,
                    legend_label=f"P50 = {p50_radius:.2f} m",
                )

            if show_percentile_circles and p95_radius is not None:
                p.circle(
                    x=[0], y=[0], radius=[p95_radius],
                    radius_units="data",
                    fill_alpha=0.0,
                    line_color=p95_circle_color,
                    line_dash="dotted",
                    line_width=2,
                    legend_label=f"P95 = {p95_radius:.2f} m",
                )

            max_circle_drawn = False
            if red_is_show and circle_radius is not None:
                if circle_radius <= plot_lim * max_to_display_ratio_for_clip:
                    p.circle(
                        x=[0], y=[0], radius=[circle_radius],
                        radius_units="data",
                        fill_alpha=0.0,
                        line_color=red_circle_color,
                        line_width=2,
                        legend_label=f"Max = {circle_radius:.2f} m",
                    )
                    max_circle_drawn = True

            extra_circle_radii = extra_circle_radii or []
            extra_circle_colors = extra_circle_colors or []
            extra_circle_labels = extra_circle_labels or []

            for i, rr in enumerate(extra_circle_radii):
                rrv = _safe_radius(rr)
                if rrv is None:
                    continue

                ccol = extra_circle_colors[i] if i < len(extra_circle_colors) and _safe_str(
                    extra_circle_colors[i]) else "#f59e0b"
                p.circle(
                    x=[0.0],
                    y=[0.0],
                    radius=[rrv],
                    radius_units="data",
                    fill_alpha=0.0,
                    line_color=ccol,
                    line_width=2,
                    line_dash="dashed",
                )

            if show_worst_station:
                if worst_p_x and worst_p_y:
                    p.circle(
                        x=worst_p_x, y=worst_p_y,
                        size=14,
                        fill_alpha=0.0,
                        line_color=red_circle_color,
                        line_width=3,
                    )
                    p.circle(
                        x=worst_p_x, y=worst_p_y,
                        size=6,
                        color=red_circle_color,
                        legend_label="Worst station",
                    )

                if worst_s_x and worst_s_y:
                    p.circle(
                        x=worst_s_x, y=worst_s_y,
                        size=14,
                        fill_alpha=0.0,
                        line_color=red_circle_color,
                        line_width=3,
                    )
                    p.circle(
                        x=worst_s_x, y=worst_s_y,
                        size=6,
                        color=red_circle_color,
                    )

            if show_station_labels and pair_label and (
                    max_station_labels is None or len(pair_label) <= max_station_labels):
                label_src = ColumnDataSource(data=dict(
                    x=pair_label_x,
                    y=pair_label_y,
                    txt=pair_label,
                ))
                labels = LabelSet(
                    x="x", y="y", text="txt",
                    source=label_src,
                    text_font_size=station_font_size,
                    text_color="black",
                )
                p.add_layout(labels)

            txt_x = []
            txt_y = []
            txt = []
            txt_color = []

            if p50_radius is not None and p50_radius <= plot_lim * 1.02:
                txt_x.append(p50_radius * 0.97)
                txt_y.append(0)
                txt.append(f"P50 {p50_radius:.2f}")
                txt_color.append(p50_circle_color)

            if p95_radius is not None and p95_radius <= plot_lim * 1.02:
                txt_x.append(p95_radius * 0.97)
                txt_y.append(0)
                txt.append(f"P95 {p95_radius:.2f}")
                txt_color.append(p95_circle_color)

            if max_circle_drawn and circle_radius is not None:
                txt_x.append(circle_radius * 0.97)
                txt_y.append(0)
                txt.append(f"Max {circle_radius:.2f}")
                txt_color.append(red_circle_color)

            if txt:
                txt_src = ColumnDataSource(data=dict(x=txt_x, y=txt_y, txt=txt, col=txt_color))
                txt_labels = LabelSet(
                    x="x", y="y", text="txt", text_color="col",
                    source=txt_src, text_font_size="9pt"
                )
                p.add_layout(txt_labels)

            if show_outside_max_arrow and circle_radius is not None and circle_radius > plot_lim:
                arr = Arrow(
                    end=OpenHead(line_color=red_circle_color, line_width=2, size=10),
                    x_start=plot_lim * 0.35,
                    y_start=plot_lim * 0.90,
                    x_end=plot_lim * 0.98,
                    y_end=0,
                    line_color=red_circle_color,
                    line_width=2,
                )
                p.add_layout(arr)
                p.add_layout(Label(
                    x=plot_lim * 0.35,
                    y=plot_lim * 0.90,
                    text=f"Max {circle_radius:.2f} m outside display range",
                    text_color=red_circle_color,
                    text_font_size="9pt",
                    background_fill_color="white",
                    background_fill_alpha=0.8,
                ))

            if getattr(p, "legend", None):
                try:
                    p.legend.click_policy = "hide"
                    p.legend.location = "bottom_right"
                except Exception:
                    pass

            color_bar = None
            if show_colorbar and connect_pairs and show_pair_heatmap and pair_len:
                color_bar = ColorBar(
                    color_mapper=line_mapper,
                    label_standoff=8,
                    width=10,
                    location=(0, 0),
                    title=colorbar_label,
                )

            px_p = src_p.data["x"] if src_p is not None else []
            py_p = src_p.data["y"] if src_p is not None else []
            px_s = src_s.data["x"] if src_s is not None else []
            py_s = src_s.data["y"] if src_s is not None else []

            forced_min = -plot_lim
            forced_max = plot_lim

            hx_p_h = _hist(px_p, bins, forced_min, forced_max)
            hx_s_h = _hist(px_s, bins, forced_min, forced_max)
            hy_p_h = _hist(py_p, bins, forced_min, forced_max)
            hy_s_h = _hist(py_s, bins, forced_min, forced_max)

            hx_p_src = ColumnDataSource({"left": hx_p_h["left"], "right": hx_p_h["right"], "top": hx_p_h["top"]})
            hx_s_src = ColumnDataSource({"left": hx_s_h["left"], "right": hx_s_h["right"], "top": hx_s_h["top"]})
            hy_p_src = ColumnDataSource({"left": hy_p_h["left"], "right": hy_p_h["right"], "top": hy_p_h["top"]})
            hy_s_src = ColumnDataSource({"left": hy_s_h["left"], "right": hy_s_h["right"], "top": hy_s_h["top"]})

            mxp, sxp = _mean_std(px_p)
            mxs, sxs = _mean_std(px_s)
            myp, syp = _mean_std(py_p)
            mys, sys = _mean_std(py_s)

            kxp_x, kxp_y = _kde_scaled_to_counts(px_p, bins, kde_points, kde_bandwidth, forced_min,
                                                 forced_max) if hist_show_kde else ([], [])
            kxs_x, kxs_y = _kde_scaled_to_counts(px_s, bins, kde_points, kde_bandwidth, forced_min,
                                                 forced_max) if hist_show_kde else ([], [])
            kyp_ygrid, kyp_xscaled = _kde_scaled_to_counts(py_p, bins, kde_points, kde_bandwidth, forced_min,
                                                           forced_max) if hist_show_kde else ([], [])
            kys_ygrid, kys_xscaled = _kde_scaled_to_counts(py_s, bins, kde_points, kde_bandwidth, forced_min,
                                                           forced_max) if hist_show_kde else ([], [])

            hx_top = figure(
                title=f"Primary dX\nmean={mxp:.2f}, std={sxp:.2f}",
                x_range=p.x_range,
                height=top_bottom_hist_height,
                tools="",
                toolbar_location=None,
                sizing_mode="stretch_width",
            )
            hx_top.quad(
                left="left", right="right", bottom=0, top="top", source=hx_p_src,
                fill_color=primary_color, line_color=primary_color, fill_alpha=0.25
            )
            if hist_show_std and px_p:
                hx_top.add_layout(Span(location=mxp, dimension="height", line_color=primary_color, line_width=2))
                hx_top.add_layout(
                    Span(location=mxp - sxp, dimension="height", line_color=primary_color, line_dash="dashed",
                         line_width=1))
                hx_top.add_layout(
                    Span(location=mxp + sxp, dimension="height", line_color=primary_color, line_dash="dashed",
                         line_width=1))
            if hist_show_kde and kxp_x:
                hx_top.line(kxp_x, kxp_y, line_width=2, color=primary_color)
            hx_top.xaxis.visible = False
            hx_top.yaxis.axis_label = "Count"

            hx_bot = figure(
                title=f"Secondary dX\nmean={mxs:.2f}, std={sxs:.2f}",
                x_range=p.x_range,
                height=top_bottom_hist_height,
                tools="",
                toolbar_location=None,
                sizing_mode="stretch_width",
            )
            hx_bot.quad(
                left="left", right="right", bottom=0, top="top", source=hx_s_src,
                fill_color=secondary_color, line_color=secondary_color, fill_alpha=0.25
            )
            if hist_show_std and px_s:
                hx_bot.add_layout(Span(location=mxs, dimension="height", line_color=secondary_color, line_width=2))
                hx_bot.add_layout(
                    Span(location=mxs - sxs, dimension="height", line_color=secondary_color, line_dash="dashed",
                         line_width=1))
                hx_bot.add_layout(
                    Span(location=mxs + sxs, dimension="height", line_color=secondary_color, line_dash="dashed",
                         line_width=1))
            if hist_show_kde and kxs_x:
                hx_bot.line(kxs_x, kxs_y, line_width=2, color=secondary_color)
            hx_bot.xaxis.axis_label = "dX, m"
            hx_bot.yaxis.axis_label = "Count"

            hy_left = figure(
                title=f"Primary dY\nmean={myp:.2f}, std={syp:.2f}",
                y_range=p.y_range,
                width=side_hist_width,
                tools="",
                toolbar_location=None,
                sizing_mode="stretch_height",
            )
            hy_left.quad(
                bottom="left", top="right", left=0, right="top", source=hy_p_src,
                fill_color=primary_color, line_color=primary_color, fill_alpha=0.25
            )
            if hist_show_std and py_p:
                hy_left.add_layout(Span(location=myp, dimension="width", line_color=primary_color, line_width=2))
                hy_left.add_layout(
                    Span(location=myp - syp, dimension="width", line_color=primary_color, line_dash="dashed",
                         line_width=1))
                hy_left.add_layout(
                    Span(location=myp + syp, dimension="width", line_color=primary_color, line_dash="dashed",
                         line_width=1))
            if hist_show_kde and kyp_ygrid:
                hy_left.line(kyp_xscaled, kyp_ygrid, line_width=2, color=primary_color)
            hy_left.xaxis.axis_label = "Count"
            hy_left.yaxis.axis_label = "dY, m"

            hy_right = figure(
                title=f"Secondary dY\nmean={mys:.2f}, std={sys:.2f}",
                y_range=p.y_range,
                width=side_hist_width,
                tools="",
                toolbar_location=None,
                sizing_mode="stretch_height",
            )
            hy_right.quad(
                bottom="left", top="right", left=0, right="top", source=hy_s_src,
                fill_color=secondary_color, line_color=secondary_color, fill_alpha=0.25
            )
            if hist_show_std and py_s:
                hy_right.add_layout(Span(location=mys, dimension="width", line_color=secondary_color, line_width=2))
                hy_right.add_layout(
                    Span(location=mys - sys, dimension="width", line_color=secondary_color, line_dash="dashed",
                         line_width=1))
                hy_right.add_layout(
                    Span(location=mys + sys, dimension="width", line_color=secondary_color, line_dash="dashed",
                         line_width=1))
            if hist_show_kde and kys_ygrid:
                hy_right.line(kys_xscaled, kys_ygrid, line_width=2, color=secondary_color)
            hy_right.xaxis.axis_label = "Count"
            hy_right.yaxis.visible = False

            if color_bar is not None:
                cb_plot = figure(
                    width=55,
                    height=300,
                    toolbar_location=None,
                    tools="",
                    min_border=0,
                    outline_line_color=None,
                    sizing_mode="stretch_height",
                )
                cb_plot.add_layout(color_bar, "right")
                cb_plot.xaxis.visible = False
                cb_plot.yaxis.visible = False
                cb_plot.grid.visible = False
            else:
                cb_plot = Spacer(width=25, height=25)

            center_row = row(
                hy_left,
                p,
                cb_plot,
                hy_right,
                sizing_mode="stretch_both",
            )

            controls = None
            if show_controls:
                txt_extra_radii = TextInput(
                    title="Extra radii (comma separated)",
                    value=", ".join([str(v) for v in (extra_circle_radii or [])]),
                    width=260,
                )
                txt_extra_colors = TextInput(
                    title="Extra colors (comma separated)",
                    value=", ".join([str(v) for v in (extra_circle_colors or [])]),
                    width=260,
                )
                txt_extra_labels = TextInput(
                    title="Extra labels (comma separated)",
                    value=", ".join([str(v) for v in (extra_circle_labels or [])]),
                    width=320,
                )

                status_div = Div(text="", width=500, height=20)

                btn_legend = Button(label="Legend", button_type="default", width=80)
                btn_legend.js_on_click(CustomJS(args=dict(plot=p), code="""
                    const legends = plot.legend;
                    if (!legends || legends.length === 0) {
                        console.log("No legend found");
                        return;
                    }
                    const leg = legends[0];
                    leg.visible = !leg.visible;
                    plot.change.emit();
                """))

                btn_move = Button(label="Move Legend", button_type="default", width=100)
                btn_move.js_on_click(CustomJS(args=dict(plot=p), code="""
                    const legends = plot.legend;
                    if (!legends || legends.length === 0) {
                        console.log("No legend found");
                        return;
                    }
                    const leg = legends[0];
                    const locs = ["bottom_right", "top_right", "top_left", "bottom_left"];
                    const cur = leg.location || "bottom_right";
                    const i = locs.indexOf(cur);
                    leg.location = locs[(i + 1) % locs.length];
                    plot.change.emit();
                """))

                btn_hist = Button(label="Histograms", button_type="default", width=90)
                btn_hist.js_on_click(CustomJS(args=dict(h1=hx_top, h2=hx_bot, h3=hy_left, h4=hy_right), code="""
                    h1.visible = !h1.visible;
                    h2.visible = !h2.visible;
                    h3.visible = !h3.visible;
                    h4.visible = !h4.visible;
                    h1.change.emit();
                    h2.change.emit();
                    h3.change.emit();
                    h4.change.emit();
                """))

                btn_apply_circles = Button(label="Apply circles", button_type="primary", width=110)
                btn_apply_circles.js_on_click(CustomJS(
                    args=dict(
                        plot=p,
                        radii_in=txt_extra_radii,
                        colors_in=txt_extra_colors,
                        labels_in=txt_extra_labels,
                        status=status_div,
                    ),
                    code="""
                        function parseList(text) {
                            if (!text) return [];
                            return text.split(",").map(s => s.trim());
                        }

                        function safeRadius(v) {
                            const x = parseFloat(v);
                            if (isNaN(x) || x <= 0) return null;
                            return x;
                        }

                        const radiiRaw = parseList(radii_in.value);
                        const colorsRaw = parseList(colors_in.value);
                        const labelsRaw = parseList(labels_in.value);

                        const keepers = [];
                        for (const r of plot.renderers) {
                            if (r.tags && r.tags.includes("extra_circle_dynamic")) {
                                continue;
                            }
                            keepers.push(r);
                        }
                        plot.renderers = keepers;

                        let added = 0;

                        for (let i = 0; i < radiiRaw.length; i++) {
                            const rr = safeRadius(radiiRaw[i]);
                            if (rr === null) continue;

                            let cc = "#f59e0b";
                            if (i < colorsRaw.length && colorsRaw[i]) {
                                cc = colorsRaw[i];
                            }

                            let ll = "";
                            if (i < labelsRaw.length && labelsRaw[i]) {
                                ll = labelsRaw[i];
                            }
                            if (!ll) {
                                ll = `Circle ${added + 1} = ${rr.toFixed(2)} m`;
                            }

                            const src = new Bokeh.ColumnDataSource({
                                data: {
                                    x: [0.0],
                                    y: [0.0],
                                    radius: [rr],
                                }
                            });

                            const glyph = new Bokeh.Circle({
                                x: {field: "x"},
                                y: {field: "y"},
                                radius: {field: "radius"},
                                radius_units: "data",
                                fill_alpha: 0.0,
                                line_alpha: 1.0,
                                line_color: cc,
                                line_width: 2,
                                line_dash: "dashed",
                            });

                            const renderer = new Bokeh.GlyphRenderer({
                                data_source: src,
                                glyph: glyph,
                                tags: ["extra_circle_dynamic", ll]
                            });

                            plot.renderers.push(renderer);
                            added += 1;
                        }

                        status.text = `Applied ${added} extra circle(s)`;
                        plot.change.emit();
                    """))

                btn_clear_circles = Button(label="Clear circles", button_type="warning", width=110)
                btn_clear_circles.js_on_click(CustomJS(
                    args=dict(plot=p, status=status_div),
                    code="""
                        const keepers = [];
                        let removed = 0;
                        for (const r of plot.renderers) {
                            if (r.tags && r.tags.includes("extra_circle_dynamic")) {
                                removed += 1;
                                continue;
                            }
                            keepers.push(r);
                        }
                        plot.renderers = keepers;
                        status.text = `Removed ${removed} extra circle(s)`;
                        plot.change.emit();
                    """
                ))

                controls = column(
                    row(btn_legend, btn_move, btn_hist, btn_apply_circles, btn_clear_circles,
                        sizing_mode="stretch_width"),
                    row(txt_extra_radii, txt_extra_colors, txt_extra_labels, sizing_mode="stretch_width"),
                    status_div,
                    sizing_mode="stretch_width",
                )

            if controls is not None:
                layout = column(
                    controls,
                    hx_top,
                    center_row,
                    hx_bot,
                    sizing_mode="stretch_both",
                )
            else:
                layout = column(
                    hx_top,
                    center_row,
                    hx_bot,
                    sizing_mode="stretch_both",
                )

            if is_show:
                show(layout)

            if json_return:
                return {"layout": layout, "json_item": json_item(layout, target_id)}

            return layout

        except Exception as e:
            return self._error_layout(
                title="DX/DY Plot Fatal Error",
                message="Unexpected error while building plot.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

    def deployment_offsets_vs_preplot(
            self,
            df,
            line,
            line_bearing,
            is_show=False,
            json_return=False,
    ):
        """
        3 stacked plots for deployment vs preplot using DSR dataframe.

        Parameters
        ----------
        df : pandas.DataFrame
            DSR dataframe for selected line.
        line : str|int
            Receiver line name/number.
        line_bearing : float
            Receiver line bearing in degrees.
        is_show : bool
            If True -> show plot directly.
        json_return : bool
            If True -> return bokeh json_item(layout).

        Required columns in df
        ----------------------
        Station, Node, ROV, TimeStamp,
        PrimaryElevation, SecondaryElevation,
        PreplotEasting, PreplotNorthing,
        PrimaryEasting, PrimaryNorthing,
        SecondaryEasting, SecondaryNorthing
        """
        try:
            if df is None or df.empty:
                return self._error_layout(
                    title="Deployment vs Preplot",
                    message=f"No DSR rows for line {line}.",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            required_cols = [
                "Station",
                "Node",
                "ROV",
                "TimeStamp",
                "PrimaryElevation",
                "SecondaryElevation",
                "PreplotEasting",
                "PreplotNorthing",
                "PrimaryEasting",
                "PrimaryNorthing",
                "SecondaryEasting",
                "SecondaryNorthing",
            ]
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                return self._error_layout(
                    title="Deployment vs Preplot",
                    message="Missing required columns.",
                    details=", ".join(missing),
                    level="error",
                    is_show=is_show,
                    json_return=json_return,
                )

            if line_bearing is None or str(line_bearing).strip() == "":
                return self._error_layout(
                    title="Deployment vs Preplot",
                    message=f"Line bearing is missing for line {line}.",
                    level="error",
                    is_show=is_show,
                    json_return=json_return,
                )

            d = df.copy()

            numeric_cols = [
                "Station",
                "PrimaryElevation",
                "SecondaryElevation",
                "PreplotEasting",
                "PreplotNorthing",
                "PrimaryEasting",
                "PrimaryNorthing",
                "SecondaryEasting",
                "SecondaryNorthing",
            ]
            for c in numeric_cols:
                d[c] = pd.to_numeric(d[c], errors="coerce")

            d = d.dropna(
                subset=[
                    "Station",
                    "PreplotEasting",
                    "PreplotNorthing",
                    "PrimaryEasting",
                    "PrimaryNorthing",
                    "SecondaryEasting",
                    "SecondaryNorthing",
                ]
            ).sort_values("Station")

            if d.empty:
                return self._error_layout(
                    title="Deployment vs Preplot",
                    message=f"Line {line} has no valid deployment/preplot coordinates.",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            bearing_rad = np.deg2rad(float(line_bearing))

            # along-line unit vector
            ux = np.sin(bearing_rad)
            uy = np.cos(bearing_rad)

            # cross-line unit vector
            vx = np.cos(bearing_rad)
            vy = -np.sin(bearing_rad)

            # primary delta from preplot
            dx1 = (
                    d["PrimaryEasting"].to_numpy(dtype=float)
                    - d["PreplotEasting"].to_numpy(dtype=float)
            )
            dy1 = (
                    d["PrimaryNorthing"].to_numpy(dtype=float)
                    - d["PreplotNorthing"].to_numpy(dtype=float)
            )

            # secondary delta from preplot
            dx2 = (
                    d["SecondaryEasting"].to_numpy(dtype=float)
                    - d["PreplotEasting"].to_numpy(dtype=float)
            )
            dy2 = (
                    d["SecondaryNorthing"].to_numpy(dtype=float)
                    - d["PreplotNorthing"].to_numpy(dtype=float)
            )

            # offsets
            il1 = dx1 * ux + dy1 * uy
            il2 = dx2 * ux + dy2 * uy

            xl1 = dx1 * vx + dy1 * vy
            xl2 = dx2 * vx + dy2 * vy

            radial1 = np.sqrt(dx1 ** 2 + dy1 ** 2)
            radial2 = np.sqrt(dx2 ** 2 + dy2 ** 2)

            # distance between primary and secondary
            p12x = (
                    d["SecondaryEasting"].to_numpy(dtype=float)
                    - d["PrimaryEasting"].to_numpy(dtype=float)
            )
            p12y = (
                    d["SecondaryNorthing"].to_numpy(dtype=float)
                    - d["PrimaryNorthing"].to_numpy(dtype=float)
            )
            range_primary_to_secondary = np.sqrt(p12x ** 2 + p12y ** 2)

            # choose smaller distance to preplot
            range_to_preplot = np.where(radial1 <= radial2, radial1, radial2)

            source = ColumnDataSource(
                data=dict(
                    Station=d["Station"].to_numpy(),
                    Node=d["Node"].fillna("").astype(str).to_numpy(),
                    ROV=d["ROV"].fillna("").astype(str).to_numpy(),
                    Date=d["TimeStamp"].fillna("").astype(str).to_numpy(),
                    PrimaryElevation=d["PrimaryElevation"].to_numpy(dtype=float),
                    SecondaryElevation=d["SecondaryElevation"].to_numpy(dtype=float),
                    il1=il1,
                    il2=il2,
                    xl1=xl1,
                    xl2=xl2,
                    radial1=radial1,
                    radial2=radial2,
                    Rangeprimarytosecondary=range_primary_to_secondary,
                    RangetoPrePlot=range_to_preplot,
                )
            )

            primary_marker = "circle"
            secondary_marker = "square"
            marker_size = 8

            x_min = float(d["Station"].min())
            x_max = float(d["Station"].max())
            if x_min == x_max:
                x_min -= 1
                x_max += 1

            shared_x_range = Range1d(x_min, x_max)

            common_kwargs = dict(
                x_axis_label="STATION",
                toolbar_location="above",
                sizing_mode="stretch_both",
                width_policy="max",
                height_policy="max",
                min_height=230,
                x_range=shared_x_range,
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )

            il_offset = figure(
                title=f"In-Line Offsets (m), REC.LINE: {line}",
                y_axis_label="IN-LINE OFFSET",
                **common_kwargs,
            )
            xl_offset = figure(
                title=f"X-Line Offsets (m), REC.LINE: {line}",
                y_axis_label="CROSS-LINE OFFSET",
                **common_kwargs,
            )
            radial_offset = figure(
                title=f"Radial Offsets (m), REC.LINE: {line}",
                y_axis_label="RADIAL OFFSET",
                **common_kwargs,
            )

            # In-Line
            il_point_primary = il_offset.scatter(
                x="Station",
                y="il1",
                source=source,
                marker=primary_marker,
                size=marker_size,
                color="red",
                fill_color="red",
                line_width=1.5,
                legend_label="Primary",
            )
            il_line_primary = il_offset.line(
                x="Station",
                y="il1",
                source=source,
                line_width=2,
                color="red",
                legend_label="Primary",
            )
            il_point_secondary = il_offset.scatter(
                x="Station",
                y="il2",
                source=source,
                marker=secondary_marker,
                size=marker_size,
                color="blue",
                fill_color="blue",
                line_width=1.5,
                legend_label="Secondary",
            )
            il_line_secondary = il_offset.line(
                x="Station",
                y="il2",
                source=source,
                line_width=2,
                color="blue",
                legend_label="Secondary",
            )

            # X-Line
            xl_point_primary = xl_offset.scatter(
                x="Station",
                y="xl1",
                source=source,
                marker=primary_marker,
                size=marker_size,
                color="red",
                fill_color="red",
                line_width=1.5,
                legend_label="Primary",
            )
            xl_line_primary = xl_offset.line(
                x="Station",
                y="xl1",
                source=source,
                line_width=2,
                color="red",
                legend_label="Primary",
            )
            xl_point_secondary = xl_offset.scatter(
                x="Station",
                y="xl2",
                source=source,
                marker=secondary_marker,
                size=marker_size,
                color="blue",
                fill_color="blue",
                line_width=1.5,
                legend_label="Secondary",
            )
            xl_line_secondary = xl_offset.line(
                x="Station",
                y="xl2",
                source=source,
                line_width=2,
                color="blue",
                legend_label="Secondary",
            )

            # Radial
            rad_point_primary = radial_offset.scatter(
                x="Station",
                y="radial1",
                source=source,
                marker=primary_marker,
                size=marker_size,
                color="red",
                fill_color="red",
                line_width=1.5,
                legend_label="Primary",
            )
            rad_line_primary = radial_offset.line(
                x="Station",
                y="radial1",
                source=source,
                line_width=2,
                color="red",
                legend_label="Primary",
            )
            rad_point_secondary = radial_offset.scatter(
                x="Station",
                y="radial2",
                source=source,
                marker=secondary_marker,
                size=marker_size,
                color="blue",
                fill_color="blue",
                line_width=1.5,
                legend_label="Secondary",
            )
            rad_line_secondary = radial_offset.line(
                x="Station",
                y="radial2",
                source=source,
                line_width=2,
                color="blue",
                legend_label="Secondary",
            )

            tooltips = [
                ("Station", "@Station"),
                ("Node", "@Node"),
                ("WD Prim.", "@PrimaryElevation{0.0}"),
                ("WD Sec.", "@SecondaryElevation{0.0}"),
                ("ROV", "@ROV"),
                ("IL-Offset Prim.", "@il1{0.00}"),
                ("IL-Offset Sec.", "@il2{0.00}"),
                ("XL-Offset Prim.", "@xl1{0.00}"),
                ("XL-Offset Sec.", "@xl2{0.00}"),
                ("Radial Off. Prim.", "@radial1{0.00}"),
                ("Radial Off. Sec.", "@radial2{0.00}"),
                ("Range Prim. to Sec.", "@Rangeprimarytosecondary{0.00}"),
                ("Range to Preplot", "@RangetoPrePlot{0.00}"),
                ("Dep. date", "@Date"),
            ]

            hover = HoverTool(
                tooltips=tooltips,
                renderers=[
                    il_point_primary,
                    il_point_secondary,
                    xl_point_primary,
                    xl_point_secondary,
                    rad_point_primary,
                    rad_point_secondary,
                ],
            )

            il_offset.add_tools(hover)
            xl_offset.add_tools(hover)
            radial_offset.add_tools(hover)

            il_offset.legend.visible = False
            xl_offset.legend.visible = False
            radial_offset.legend.visible = False

            combined_legend = Legend(
                items=[
                    LegendItem(
                        label="Primary",
                        renderers=[
                            il_point_primary,
                            il_line_primary,
                            xl_point_primary,
                            xl_line_primary,
                            rad_point_primary,
                            rad_line_primary,
                        ],
                    ),
                    LegendItem(
                        label="Secondary",
                        renderers=[
                            il_point_secondary,
                            il_line_secondary,
                            xl_point_secondary,
                            xl_line_secondary,
                            rad_point_secondary,
                            rad_line_secondary,
                        ],
                    ),
                ]
            )
            combined_legend.orientation = "horizontal"
            combined_legend.click_policy = "hide"

            il_offset.add_layout(combined_legend, "above")

            layout = gridplot(
                [[il_offset], [xl_offset], [radial_offset]],
                sizing_mode="stretch_both",
                merge_tools=True,
                toolbar_location="above",
            )

            if is_show:
                show(layout)
                return None

            if json_return:
                return json_item(layout)

            return layout

        except Exception as e:
            return self._error_layout(
                title="Deployment vs Preplot",
                message="Failed to build plot.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

    def graph_recover_time(
            self,
            df,
            line,
            is_deploy=False,
            is_show=False,
            json_return=False,
    ):
        """
        Plot deployment/recovery time vs station and time difference.
        """

        try:

            if df is None or df.empty:
                return self._error_layout(
                    title="Deployment / Recovery Time",
                    message=f"No DSR data for line {line}",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            d = df.copy()

            if is_deploy:

                title = "DEPLOYMENT TIME vs NODE POSITION"
                y_title = "DEPLOYMENT TIME"
                line_title = "Deployment Time"

                required_cols = ["Station", "TimeStamp", "Date", "ROV"]

                missing = [c for c in required_cols if c not in d.columns]
                if missing:
                    return self._error_layout(
                        title="Deployment / Recovery Time",
                        message="Missing required columns.",
                        details=", ".join(missing),
                        level="error",
                        is_show=is_show,
                        json_return=json_return,
                    )

                d["PlotTime"] = pd.to_datetime(d["TimeStamp"], errors="coerce")
                rov_col = "ROV"
                date_col = "Date"

            else:

                title = "RETRIEVE TIME vs NODE POSITION"
                y_title = "RETRIEVE TIME"
                line_title = "Pick Up Time"

                required_cols = ["Station", "TimeStamp1", "Date1", "ROV1"]

                missing = [c for c in required_cols if c not in d.columns]
                if missing:
                    return self._error_layout(
                        title="Deployment / Recovery Time",
                        message="Missing required columns.",
                        details=", ".join(missing),
                        level="error",
                        is_show=is_show,
                        json_return=json_return,
                    )

                d = d[(d["ROV1"].notna()) & (d["ROV1"].astype(str).str.strip() != "")]
                d["PlotTime"] = pd.to_datetime(d["TimeStamp1"], errors="coerce")

                rov_col = "ROV1"
                date_col = "Date1"

            d["Station"] = pd.to_numeric(d["Station"], errors="coerce")
            d = d.dropna(subset=["Station", "PlotTime"]).sort_values("Station")

            if d.empty:
                return self._error_layout(
                    title="Deployment / Recovery Time",
                    message=f"No valid timestamps for line {line}",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            d["TimeDiff"] = d["PlotTime"].diff()
            d["Hours"] = d["TimeDiff"].dt.total_seconds() / 3600.0

            stations = np.sort(d["Station"].dropna().unique())

            if len(stations) > 1:
                diffs = np.diff(stations)
                diffs = diffs[diffs > 0]
                width = float(diffs.min()) * 0.8 if len(diffs) else 1.0
            else:
                width = 1.0

            max_hours = pd.to_numeric(d["Hours"], errors="coerce").max()

            if pd.isna(max_hours) or max_hours <= 0:
                max_hours = 1.0

            color_mapper = LinearColorMapper(
                palette="Turbo256",
                low=0,
                high=float(max_hours),
            )

            source = ColumnDataSource(d)

            x_min = float(d["Station"].min())
            x_max = float(d["Station"].max())

            if x_min == x_max:
                x_min -= 1
                x_max += 1

            line_map = figure(
                title=f"{title}: {line}",
                x_axis_label="Station",
                y_axis_label=y_title,
                toolbar_location="above",
                sizing_mode="stretch_both",
                width_policy="max",
                height_policy="max",
                min_height=250,
                y_axis_type="datetime",
                x_range=(x_min, x_max),
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )

            diff_graph = figure(
                title="Time Difference",
                x_axis_label="Station",
                y_axis_label="Hours",
                toolbar_location="above",
                sizing_mode="stretch_both",
                width_policy="max",
                height_policy="max",
                min_height=150,
                max_height=220,
                x_range=line_map.x_range,
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
            )

            scatter = line_map.scatter(
                x="Station",
                y="PlotTime",
                source=source,
                color="red",
                size=6,
                legend_label=line_title,
            )

            line_map.line(
                x="Station",
                y="PlotTime",
                source=source,
                color="red",
                legend_label=line_title,
            )

            diff_bar = diff_graph.vbar(
                x="Station",
                top="Hours",
                source=source,
                width=width,
                fill_color={"field": "Hours", "transform": color_mapper},
                line_color=None,
            )

            TOOLTIPS = [
                ("Station", "@Station"),
                ("TimeStamp", f"@{date_col}"),
                ("ROV", f"@{rov_col}"),
            ]

            TOOLTIPSD = [
                ("Station", "@Station"),
                ("TimeStamp", f"@{date_col}"),
                ("ROV", f"@{rov_col}"),
                ("Time Diff (h)", "@Hours{0.00}"),
            ]

            hover = HoverTool(tooltips=TOOLTIPS, renderers=[scatter])
            hoverd = HoverTool(tooltips=TOOLTIPSD, renderers=[diff_bar])

            line_map.add_tools(hover)
            diff_graph.add_tools(hoverd)

            diff_graph.yaxis.formatter = NumeralTickFormatter(format="0.0")

            layout = column([line_map, diff_graph], sizing_mode="stretch_both")

            if is_show:
                show(layout)
                return None

            if json_return:
                return json_item(layout)

            return layout

        except Exception as e:
            return self._error_layout(
                title="Deployment / Recovery Time",
                message="Failed to build graph.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

    def get_bbox_config_for_line(self, df):
        """
        Detect BBox configuration for a line based on df.ROV values.

        Returns
        -------
        dict | None
            {
                "config_id": int,
                "rov1_name": str,
                "rov2_name": str,
                "vessel_name": str
            }
        """

        if df is None or df.empty or "ROV" not in df.columns:
            return None

        rov_values = (
            df["ROV"]
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )

        if not rov_values:
            return None

        sql = """
        SELECT
            ID,
            rov1_name,
            rov2_name,
            Vessel_name
        FROM BBox_Configs_List
        """

        try:
            with self._connect() as conn:
                cfg = pd.read_sql(sql, conn)
        except Exception as e:
            print("BBox config lookup error:", e)
            return None

        if cfg.empty:
            return None

        for r in cfg.itertuples():

            if (
                    r.rov1_name in rov_values
                    or r.rov2_name in rov_values
            ):
                return {
                    "config_id": r.ID,
                    "rov1_name": r.rov1_name,
                    "rov2_name": r.rov2_name,
                    "vessel_name": r.Vessel_name,
                }

        return None

    def plot_line_map(
            self,
            df,
            bbdata,
            cfg_row,
            line,
            is_shape_show=False,
            isShow=False,
            point_size=5,
            rov_size=5,
            html_path=None,
    ):
        colors = Paired[10]

        if df is None:
            df = pd.DataFrame()
        else:
            df = df.copy()

        if bbdata is None:
            bbdata = pd.DataFrame()
        else:
            bbdata = bbdata.copy()

        vessel_name = "Node Vessel"
        rov1_name = "ROV1"
        rov2_name = "ROV2"

        if cfg_row is not None:
            vessel_name = str(cfg_row.get("Vessel_name") or "Node Vessel")
            rov1_name = str(cfg_row.get("rov1_name") or "ROV1")
            rov2_name = str(cfg_row.get("rov2_name") or "ROV2")

        dsr_point_renderers = []
        radius_renderers = []

        def _to_num(d, cols):
            if d is None or d.empty:
                return d
            for c in cols:
                if c in d.columns:
                    d[c] = pd.to_numeric(d[c], errors="coerce")
            return d

        def _valid_xy(d, x_col, y_col):
            if d is None or d.empty:
                return pd.DataFrame()
            if not {x_col, y_col}.issubset(d.columns):
                return pd.DataFrame()

            out = d.copy()
            out = _to_num(out, [x_col, y_col])
            out = out[
                out[x_col].notna()
                & out[y_col].notna()
                & (out[x_col] != 0)
                & (out[y_col] != 0)
                ].copy()
            return out

        def _ensure_cols(d, cols):
            for c in cols:
                if c not in d.columns:
                    d[c] = ""
            return d

        def _add_distance_line(
                plot,
                data,
                x0,
                y0,
                x1,
                y1,
                label_prefix,
                line_color,
                legend_label,
                station_col="Station",
                node_col="Node",
                rov_col="ROV",
                time_col="TimeStamp",
                line_dash="dashed",
        ):
            needed = {x0, y0, x1, y1}
            if data is None or data.empty or not needed.issubset(data.columns):
                return

            d = data.copy()
            d = _ensure_cols(d, [station_col, node_col, rov_col, time_col])
            d = _to_num(d, [x0, y0, x1, y1])

            d = d[
                d[x0].notna()
                & d[y0].notna()
                & d[x1].notna()
                & d[y1].notna()
                & (d[x0] != 0)
                & (d[y0] != 0)
                & (d[x1] != 0)
                & (d[y1] != 0)
                ].copy()

            if d.empty:
                return

            d["DIST_MID_X"] = (d[x0] + d[x1]) / 2.0
            d["DIST_MID_Y"] = (d[y0] + d[y1]) / 2.0
            d["DIST_VALUE"] = np.sqrt((d[x0] - d[x1]) ** 2 + (d[y0] - d[y1]) ** 2)
            d["DIST_LABEL"] = d["DIST_VALUE"].map(lambda v: f"{v:.1f} m")

            src = ColumnDataSource(d)

            seg = plot.segment(
                x0=x0,
                y0=y0,
                x1=x1,
                y1=y1,
                source=src,
                line_color=line_color,
                line_width=2,
                line_alpha=0.75,
                line_dash=line_dash,
                legend_label=legend_label,
            )

            plot.text(
                x="DIST_MID_X",
                y="DIST_MID_Y",
                text="DIST_LABEL",
                source=src,
                text_font_size="8pt",
                text_color=line_color,
                text_align="center",
                text_baseline="middle",
                x_offset=8,
                y_offset=8,
            )

            plot.add_tools(
                HoverTool(
                    renderers=[seg],
                    tooltips=[
                        ("Type", label_prefix),
                        ("Station", f"@{station_col}"),
                        ("Node", f"@{node_col}"),
                        ("ROV", f"@{rov_col}"),
                        ("From E", f"@{x0}{{0.0}}"),
                        ("From N", f"@{y0}{{0.0}}"),
                        ("To E", f"@{x1}{{0.0}}"),
                        ("To N", f"@{y1}{{0.0}}"),
                        ("Distance", "@DIST_LABEL"),
                        ("Time", f"@{time_col}"),
                    ],
                )
            )

        def _read_recdb_points_for_line(line_value):
            try:
                with self._connect() as conn:
                    table_ok = conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='REC_DB' LIMIT 1"
                    ).fetchone()

                    if not table_ok:
                        return pd.DataFrame()

                    cols_info = conn.execute("PRAGMA table_info(REC_DB)").fetchall()
                    cols = [r["name"] if isinstance(r, sqlite3.Row) else r[1] for r in cols_info]
                    cols_lut = {c.lower(): c for c in cols}

                    if "rec_x" not in cols_lut or "rec_y" not in cols_lut:
                        return pd.DataFrame()

                    line_col = None
                    for cand in ["Line", "line", "LINE"]:
                        if cand.lower() in cols_lut:
                            line_col = cols_lut[cand.lower()]
                            break

                    keep_cols = []
                    for cand in [
                        line_col,
                        cols_lut.get("station"),
                        cols_lut.get("point"),
                        cols_lut.get("node"),
                        cols_lut.get("node_id"),
                        cols_lut.get("rec_id"),
                        cols_lut.get("rec_x"),
                        cols_lut.get("rec_y"),
                        cols_lut.get("prep_x"),
                        cols_lut.get("prep_y"),
                    ]:
                        if cand and cand not in keep_cols:
                            keep_cols.append(cand)

                    select_cols = ", ".join([f'"{c}"' for c in keep_cols])

                    if line_col:
                        sql = f"""
                            SELECT {select_cols}
                            FROM REC_DB
                            WHERE CAST("{line_col}" AS INTEGER) = ?
                        """
                        rec = pd.read_sql_query(sql, conn, params=(int(line_value),))
                    else:
                        rec = pd.read_sql_query(f"SELECT {select_cols} FROM REC_DB", conn)

                    rec = rec.rename(
                        columns={
                            cols_lut.get("rec_x", "REC_X"): "REC_X",
                            cols_lut.get("rec_y", "REC_Y"): "REC_Y",
                            cols_lut.get("prep_x", "PREP_X"): "PREP_X",
                            cols_lut.get("prep_y", "PREP_Y"): "PREP_Y",
                        }
                    )

                    if "Station" not in rec.columns:
                        for cand in ["Station", "station", "Point", "POINT", "point"]:
                            if cand in rec.columns:
                                rec["Station"] = rec[cand]
                                break

                    if "Node" not in rec.columns:
                        for cand in ["NODE_ID", "Node", "node", "REC_ID", "rec_id"]:
                            if cand in rec.columns:
                                rec["Node"] = rec[cand]
                                break

                    rec = _ensure_cols(rec, ["Station", "Node", "PREP_X", "PREP_Y"])
                    rec = _to_num(rec, ["REC_X", "REC_Y", "PREP_X", "PREP_Y"])

                    rec["REC_DB_DistToPreplot"] = np.nan
                    rec["REC_DB_DistLabel"] = ""

                    good_prep = (
                            rec["REC_X"].notna()
                            & rec["REC_Y"].notna()
                            & rec["PREP_X"].notna()
                            & rec["PREP_Y"].notna()
                            & (rec["PREP_X"] != 0)
                            & (rec["PREP_Y"] != 0)
                    )

                    rec.loc[good_prep, "REC_DB_DistToPreplot"] = np.sqrt(
                        (rec.loc[good_prep, "REC_X"] - rec.loc[good_prep, "PREP_X"]) ** 2
                        + (rec.loc[good_prep, "REC_Y"] - rec.loc[good_prep, "PREP_Y"]) ** 2
                    )

                    rec["REC_DB_DistLabel"] = rec["REC_DB_DistToPreplot"].map(
                        lambda v: f"{v:.1f} m" if pd.notna(v) else ""
                    )

                    rec["Source"] = "REC_DB"
                    return _valid_xy(rec, "REC_X", "REC_Y")

            except Exception as e:
                print("REC_DB plot read error:", e)
                return pd.DataFrame()

        line_map = figure(
            title=f"Line Map. Line: {line}",
            x_axis_label="Easting, m",
            y_axis_label="Northing, m",
            toolbar_location="above",
            sizing_mode="stretch_both",
            match_aspect=True,
            width_policy="max",
            height_policy="max",
            min_height=100,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
        )

        ppdata = _valid_xy(df, "PreplotEasting", "PreplotNorthing")

        if len(ppdata) > 0:
            ppdata = _ensure_cols(ppdata, ["Station"])
            ppdata["PointLabel"] = ppdata["Station"].astype(str)
            pp_source = ColumnDataSource(ppdata)

            pp_points = line_map.scatter(
                x="PreplotEasting",
                y="PreplotNorthing",
                source=pp_source,
                color="grey",
                size=point_size,
                legend_label="Preplot Stations",
            )

            radius_pp = line_map.circle(
                x="PreplotEasting",
                y="PreplotNorthing",
                source=pp_source,
                fill_color=None,
                line_color="green",
                radius=5,
                radius_units="data",
                visible=False,
                legend_label="Preplot Radius Circle",
            )
            radius_renderers.append(radius_pp)

            line_map.text(
                x="PreplotEasting",
                y="PreplotNorthing",
                text="PointLabel",
                source=pp_source,
                text_font_size="8pt",
            )

            x_mean = ppdata["PreplotEasting"].mean()
            y_mean = ppdata["PreplotNorthing"].mean()

            if pd.notna(x_mean) and pd.notna(y_mean):
                line_map.x_range = Range1d(x_mean - 10000, x_mean + 10000)
                line_map.y_range = Range1d(y_mean - 10000, y_mean + 10000)

        # BBOX points/tracks: NOT added to dsr_point_renderers
        if len(bbdata) > 0:
            bb_numeric_cols = [
                "VesselEasting", "VesselNorthing", "VesselHDG",
                "ROV1_INS_Easting", "ROV1_INS_Northing",
                "ROV1_USBL_Easting", "ROV1_USBL_Northing",
                "ROV2_INS_Easting", "ROV2_INS_Northing",
                "ROV2_USBL_Easting", "ROV2_USBL_Northing",
            ]

            bbdata = _to_num(bbdata, bb_numeric_cols)

            if {"VesselEasting", "VesselNorthing", "VesselHDG"}.issubset(bbdata.columns):
                bb_track = bbdata.loc[
                    bbdata["VesselEasting"].notna()
                    & bbdata["VesselNorthing"].notna()
                    & (bbdata["VesselEasting"] > 0)
                    & (bbdata["VesselNorthing"] > 0)
                    ].copy()

                if len(bb_track) > 0:
                    bb_source = ColumnDataSource(bb_track)

                    line_map.line(
                        x="VesselEasting",
                        y="VesselNorthing",
                        source=bb_source,
                        line_width=1,
                        color=colors[0],
                        legend_label=f"{vessel_name} Track",
                    )

                    vessel_rect = Rect(
                        x="VesselEasting",
                        y="VesselNorthing",
                        width=24,
                        height=6,
                        width_units="data",
                        angle="VesselHDG",
                        angle_units="deg",
                        line_color="#3288bd",
                        fill_color="white",
                        line_width=3,
                    )

                    vessel_ray = Ray(
                        x="VesselEasting",
                        y="VesselNorthing",
                        length=24,
                        length_units="data",
                        angle="VesselHDG",
                        angle_units="deg",
                        line_color="#3288bd",
                        line_width=3,
                    )

                    line_map.add_glyph(bb_source, vessel_rect)
                    line_map.add_glyph(bb_source, vessel_ray)

            for x_col, y_col, marker, color, label in [
                ("ROV1_INS_Easting", "ROV1_INS_Northing", "triangle", colors[2], f"{rov1_name} INS"),
                ("ROV1_USBL_Easting", "ROV1_USBL_Northing", "circle", colors[4], f"{rov1_name} USBL"),
                ("ROV2_INS_Easting", "ROV2_INS_Northing", "triangle", colors[3], f"{rov2_name} INS"),
                ("ROV2_USBL_Easting", "ROV2_USBL_Northing", "circle", colors[5], f"{rov2_name} USBL"),
            ]:
                if {x_col, y_col}.issubset(bbdata.columns):
                    d = _valid_xy(bbdata, x_col, y_col)
                    if len(d) > 0:
                        src = ColumnDataSource(d)
                        line_map.scatter(
                            x=x_col,
                            y=y_col,
                            marker=marker,
                            size=rov_size,
                            color=color,
                            legend_label=label,
                            source=src,
                        )
                        line_map.line(
                            x=x_col,
                            y=y_col,
                            width=1,
                            color=color,
                            legend_label=label,
                            source=src,
                        )

        if len(df) > 0:
            df = _ensure_cols(
                df,
                [
                    "Station", "Node", "ROV", "ROV1",
                    "TimeStamp", "TimeStamp1", "Comments",
                    "PrimaryElevation", "SecondaryElevation",
                    "Rangeprimarytosecondary", "RangetoPrePlot",
                ],
            )

            dsr_dep = _valid_xy(df, "PrimaryEasting", "PrimaryNorthing")

            if len(dsr_dep) > 0:
                dsr_dep_source = ColumnDataSource(dsr_dep)

                dsr_dep_points = line_map.scatter(
                    x="PrimaryEasting",
                    y="PrimaryNorthing",
                    marker="square",
                    size=point_size + 1,
                    fill_color=colors[-1],
                    color=colors[-1],
                    legend_label="DSR Deployment Primary",
                    source=dsr_dep_source,
                )
                dsr_point_renderers.append(dsr_dep_points)

                line_map.line(
                    x="PrimaryEasting",
                    y="PrimaryNorthing",
                    width=1,
                    color=colors[-1],
                    legend_label="DSR Deployment Primary",
                    source=dsr_dep_source,
                    line_dash="dashed",
                )

                line_map.text(
                    x="PrimaryEasting",
                    y="PrimaryNorthing",
                    text="Station",
                    source=dsr_dep_source,
                    color=colors[-2],
                    text_font_size="10pt",
                    text_align="right",
                )

                line_map.text(
                    x="PrimaryEasting",
                    y="PrimaryNorthing",
                    text="Comments",
                    source=dsr_dep_source,
                    color="red",
                    text_font_size="9pt",
                    text_align="left",
                )

                line_map.add_tools(
                    HoverTool(
                        tooltips=[
                            ("Type", "DSR Deployment Primary"),
                            ("Station", "@Station"),
                            ("Node", "@Node"),
                            ("ROV", "@ROV"),
                            ("Primary E", "@PrimaryEasting{0.0}"),
                            ("Primary N", "@PrimaryNorthing{0.0}"),
                            ("WD Prim.", "@PrimaryElevation{0.0}"),
                            ("WD Sec.", "@SecondaryElevation{0.0}"),
                            ("Range Prim.2Sec.", "@Rangeprimarytosecondary{0.0}"),
                            ("Range 2 Preplot", "@RangetoPrePlot{0.0}"),
                            ("Dep date", "@TimeStamp"),
                            ("Comments", "@Comments"),
                        ],
                        renderers=[dsr_dep_points],
                    )
                )

            dep_sec = _valid_xy(df, "SecondaryEasting", "SecondaryNorthing")
            if len(dep_sec) > 0:
                dep_sec = _ensure_cols(dep_sec, ["Station", "Node", "ROV", "TimeStamp"])
                dep_sec_source = ColumnDataSource(dep_sec)

                dep_sec_points = line_map.scatter(
                    x="SecondaryEasting",
                    y="SecondaryNorthing",
                    source=dep_sec_source,
                    marker="circle",
                    size=point_size + 1,
                    fill_color="#9ec5fe",
                    line_color="#0d6efd",
                    legend_label="DSR Deployment Secondary",
                )
                dsr_point_renderers.append(dep_sec_points)

            _add_distance_line(
                plot=line_map,
                data=df,
                x0="PrimaryEasting",
                y0="PrimaryNorthing",
                x1="SecondaryEasting",
                y1="SecondaryNorthing",
                label_prefix="Deployment Primary to Secondary",
                line_color="#0d6efd",
                legend_label="Deployment Primary ↔ Secondary",
                station_col="Station",
                node_col="Node",
                rov_col="ROV",
                time_col="TimeStamp",
                line_dash="solid",
            )

            _add_distance_line(
                plot=line_map,
                data=df,
                x0="PreplotEasting",
                y0="PreplotNorthing",
                x1="PrimaryEasting",
                y1="PrimaryNorthing",
                label_prefix="Preplot to Deployment Primary",
                line_color="#6f42c1",
                legend_label="Preplot ↔ Deployment Primary",
                station_col="Station",
                node_col="Node",
                rov_col="ROV",
                time_col="TimeStamp",
                line_dash="dashed",
            )

            _add_distance_line(
                plot=line_map,
                data=df,
                x0="PreplotEasting",
                y0="PreplotNorthing",
                x1="SecondaryEasting",
                y1="SecondaryNorthing",
                label_prefix="Preplot to Deployment Secondary",
                line_color="#6610f2",
                legend_label="Preplot ↔ Deployment Secondary",
                station_col="Station",
                node_col="Node",
                rov_col="ROV",
                time_col="TimeStamp",
                line_dash="dotdash",
            )

        if len(df) > 0:
            dsr_rec = _valid_xy(df, "PrimaryEasting1", "PrimaryNorthing1")

            if len(dsr_rec) > 0:
                dsr_rec = _ensure_cols(dsr_rec, ["Station", "Node", "ROV1", "TimeStamp1"])

                dsr_rec = dsr_rec[
                    dsr_rec["ROV1"].notna()
                    & (dsr_rec["ROV1"].astype(str).str.strip() != "")
                    ].copy()

                if len(dsr_rec) > 0:
                    dsr_rec_source = ColumnDataSource(dsr_rec)

                    dsr_rec_points = line_map.scatter(
                        x="PrimaryEasting1",
                        y="PrimaryNorthing1",
                        marker="diamond",
                        size=point_size + 3,
                        fill_color="#198754",
                        line_color="#0f5132",
                        legend_label="DSR Recovery Primary",
                        source=dsr_rec_source,
                    )
                    dsr_point_renderers.append(dsr_rec_points)

                    line_map.line(
                        x="PrimaryEasting1",
                        y="PrimaryNorthing1",
                        width=1,
                        color="#198754",
                        legend_label="DSR Recovery Primary",
                        source=dsr_rec_source,
                        line_dash="dotdash",
                    )

            rec_sec = _valid_xy(df, "SecondaryEasting1", "SecondaryNorthing1")
            if len(rec_sec) > 0:
                rec_sec = _ensure_cols(rec_sec, ["Station", "Node", "ROV1", "TimeStamp1"])
                rec_sec = rec_sec[
                    rec_sec["ROV1"].notna()
                    & (rec_sec["ROV1"].astype(str).str.strip() != "")
                    ].copy()

                if len(rec_sec) > 0:
                    rec_sec_source = ColumnDataSource(rec_sec)

                    rec_sec_points = line_map.scatter(
                        x="SecondaryEasting1",
                        y="SecondaryNorthing1",
                        source=rec_sec_source,
                        marker="circle",
                        size=point_size + 1,
                        fill_color="#a3cfbb",
                        line_color="#198754",
                        legend_label="DSR Recovery Secondary",
                    )
                    dsr_point_renderers.append(rec_sec_points)

            _add_distance_line(
                plot=line_map,
                data=df,
                x0="PrimaryEasting1",
                y0="PrimaryNorthing1",
                x1="SecondaryEasting1",
                y1="SecondaryNorthing1",
                label_prefix="Recovery Primary to Secondary",
                line_color="#198754",
                legend_label="Recovery Primary ↔ Secondary",
                station_col="Station",
                node_col="Node",
                rov_col="ROV1",
                time_col="TimeStamp1",
                line_dash="solid",
            )

        recdb_df = _read_recdb_points_for_line(line)

        if len(recdb_df) > 0:
            recdb_source = ColumnDataSource(recdb_df)

            recdb_points = line_map.scatter(
                x="REC_X",
                y="REC_Y",
                source=recdb_source,
                marker="x",
                size=point_size + 5,
                line_width=2,
                color="#dc3545",
                legend_label="REC_DB REC_X/REC_Y",
            )
            dsr_point_renderers.append(recdb_points)

            line_map.line(
                x="REC_X",
                y="REC_Y",
                source=recdb_source,
                width=1,
                color="#dc3545",
                line_dash="dotted",
                legend_label="REC_DB REC_X/REC_Y",
            )

            recdb_prep = recdb_df[
                recdb_df["PREP_X"].notna()
                & recdb_df["PREP_Y"].notna()
                & (recdb_df["PREP_X"] != 0)
                & (recdb_df["PREP_Y"] != 0)
                ].copy()

            if len(recdb_prep) > 0:
                recdb_prep["MID_X"] = (recdb_prep["REC_X"] + recdb_prep["PREP_X"]) / 2.0
                recdb_prep["MID_Y"] = (recdb_prep["REC_Y"] + recdb_prep["PREP_Y"]) / 2.0
                recdb_prep_source = ColumnDataSource(recdb_prep)

                prep_points = line_map.scatter(
                    x="PREP_X",
                    y="PREP_Y",
                    source=recdb_prep_source,
                    marker="circle_cross",
                    size=point_size + 3,
                    color="#fd7e14",
                    legend_label="REC_DB PREP_X/PREP_Y",
                )
                dsr_point_renderers.append(prep_points)

                line_map.segment(
                    x0="PREP_X",
                    y0="PREP_Y",
                    x1="REC_X",
                    y1="REC_Y",
                    source=recdb_prep_source,
                    line_color="#fd7e14",
                    line_width=2,
                    line_dash="dashed",
                    line_alpha=0.8,
                    legend_label="Preplot ↔ REC_DB",
                )

                line_map.text(
                    x="MID_X",
                    y="MID_Y",
                    text="REC_DB_DistLabel",
                    source=recdb_prep_source,
                    text_font_size="8pt",
                    text_color="#fd7e14",
                    text_align="center",
                    text_baseline="middle",
                    x_offset=8,
                    y_offset=8,
                )

            line_map.add_tools(
                HoverTool(
                    tooltips=[
                        ("Type", "REC_DB"),
                        ("Station", "@Station"),
                        ("Node", "@Node"),
                        ("REC_X", "@REC_X{0.0}"),
                        ("REC_Y", "@REC_Y{0.0}"),
                        ("PREP_X", "@PREP_X{0.0}"),
                        ("PREP_Y", "@PREP_Y{0.0}"),
                        ("Distance to Preplot", "@REC_DB_DistLabel"),
                    ],
                    renderers=[recdb_points],
                )
            )

        options = []
        lock_code = []

        if len(df) > 0 and {"Station", "PrimaryEasting", "PrimaryNorthing"}.issubset(df.columns):
            tmp_lock = _valid_xy(df, "PrimaryEasting", "PrimaryNorthing")

            for p in tmp_lock.itertuples():
                try:
                    station_str = str(p.Station)
                    x0 = float(p.PrimaryEasting) - 20
                    x1 = float(p.PrimaryEasting) + 20
                    y0 = float(p.PrimaryNorthing) - 20
                    y1 = float(p.PrimaryNorthing) + 20

                    options.append(station_str)
                    lock_code.append(
                        f"'{station_str}': {{x_range: [{x0}, {x1}], y_range: [{y0}, {y1}]}}"
                    )
                except Exception:
                    pass

        multiselect = MultiSelect(
            title="Select Station",
            options=options,
            value=[],
            size=20,
            width=120,
            height=200,
            sizing_mode="stretch_height",
        )

        code1 = f"""
            const locations = {{{", ".join(lock_code)}}};
            const selected_locations = multiselect.value;

            if (selected_locations.length > 0) {{
                const selected_location = selected_locations[0];
                const ranges = locations[selected_location];

                if (ranges) {{
                    plot.x_range.start = ranges.x_range[0];
                    plot.x_range.end = ranges.x_range[1];
                    plot.y_range.start = ranges.y_range[0];
                    plot.y_range.end = ranges.y_range[1];
                    plot.change.emit();
                }}
            }}
        """

        js_callback = CustomJS(args=dict(plot=line_map, multiselect=multiselect), code=code1)
        multiselect.js_on_change("value", js_callback)

        btn_legend = Button(label="Hide Legend", button_type="success", width=120)

        point_size_input = TextInput(
            title="DSR point size",
            value=str(point_size),
            width=120,
        )

        btn_apply_point_size = Button(
            label="Apply point size",
            button_type="primary",
            width=140,
        )

        radius_input = TextInput(
            title="Preplot circle radius, m",
            value="5",
            width=160,
        )

        btn_apply_radius = Button(label="Apply radius", button_type="primary", width=120)
        btn_toggle_radius = Button(label="Show circles", button_type="success", width=120)

        if len(line_map.legend) > 0:
            callback = CustomJS(
                args=dict(button=btn_legend, legend=line_map.legend[0]),
                code="""
                    if (legend.visible) {
                        legend.visible = false;
                        button.label = 'Show Legend';
                    } else {
                        legend.visible = true;
                        button.label = 'Hide Legend';
                    }
                """,
            )
            btn_legend.js_on_click(callback)

        btn_apply_point_size.js_on_click(
            CustomJS(
                args=dict(renderers=dsr_point_renderers, point_size_input=point_size_input),
                code="""
                    const size = parseFloat(point_size_input.value);

                    if (isNaN(size) || size <= 0) {
                        alert("Input valid point size");
                        return;
                    }

                    for (const r of renderers) {
                        if (r.glyph && 'size' in r.glyph) {
                            r.glyph.size = size;
                        }
                    }
                """,
            )
        )

        btn_apply_radius.js_on_click(
            CustomJS(
                args=dict(renderers=radius_renderers, radius_input=radius_input),
                code="""
                    const radius = parseFloat(radius_input.value);

                    if (isNaN(radius) || radius <= 0) {
                        alert("Input valid radius in meters");
                        return;
                    }

                    for (const r of renderers) {
                        if (r.glyph && 'radius' in r.glyph) {
                            r.glyph.radius = radius;
                            r.visible = true;
                        }
                    }
                """,
            )
        )

        btn_toggle_radius.js_on_click(
            CustomJS(
                args=dict(renderers=radius_renderers, button=btn_toggle_radius),
                code="""
                    let show = true;

                    if (renderers.length > 0 && renderers[0].visible) {
                        show = false;
                    }

                    for (const r of renderers) {
                        r.visible = show;
                    }

                    button.label = show ? "Hide circles" : "Show circles";
                """,
            )
        )

        line_map.yaxis.formatter = PrintfTickFormatter(format="%d")
        line_map.xaxis.formatter = PrintfTickFormatter(format="%d")

        if len(line_map.legend) > 0:
            line_map.legend.click_policy = "hide"

        top_controls = row(
            btn_legend,
            point_size_input,
            btn_apply_point_size,
            radius_input,
            btn_apply_radius,
            btn_toggle_radius,
            sizing_mode="stretch_width",
        )

        side_controls = column([multiselect], sizing_mode="stretch_height")

        layout = column(
            top_controls,
            row([line_map, side_controls], sizing_mode="stretch_both"),
            sizing_mode="stretch_both",
        )

        if html_path:
            html_path = Path(html_path)
            html_path.parent.mkdir(parents=True, exist_ok=True)

            output_file(
                filename=str(html_path),
                title=f"DSR Line Map {line}",
                mode="inline",
            )
            save(layout)

        if isShow:
            show(layout)

        return layout

    def bokeh_three_vbar_with_2l_by_category_shared_x(
            self,
            df,
            *,
            title1="Series 1",
            title2="Series 2",
            title3="Series 3",
            x_col="Station",
            y1_col="PrimaryElevation",
            y2_col="SecondaryElevation",
            y3_col="WaterDepth",
            y1_label=None,
            y2_label=None,
            y3_label=None,
            y_axis_label="Water depth",
            category_col="ROV",
            line_col="Line",
            point_col=None,
            rov_col="ROV",
            ts_col="TimeStamp",
            require_category=True,
            reverse_y_if_negative=True,
            legend_title="Legend",
            x_tick_step=5,
            x_tick_font_size="8pt",

            # --- new params for 2 line graphs ---
            line1_col=None,
            line2_col=None,
            line1_label=None,
            line2_label=None,
            line1_color="#d62728",
            line2_color="#1f77b4",
            line_line_width=2,
            line_marker_size=6,
            show_lines_on_all_plots=True,

            json_return=False,
            is_show=False,
    ):
        """
        3 stacked vbar plots (shared categorical x):
          - shared X (Stations) via shared FactorRange
          - merged toolbar for all plots
          - one legend above the stack
          - per plot:
              * one numeric vbar series split by category_col (e.g. ROV / ROV1)
              * optional 2 line series over bars
          - X labels vertical and sparse via x_tick_step

        New:
          - line1_col / line2_col are optional numeric columns
          - same two lines are drawn on each of the three plots
        """

        def _err(msg: str, details: str = ""):
            try:
                return self._error_layout(
                    title="Plot build error",
                    message=msg,
                    details=details,
                    level="error",
                    json_return=json_return,
                    is_show=is_show,
                )
            except TypeError:
                return self._error_layout(msg)

        if df is None:
            return _err("No dataframe provided.")
        if not hasattr(df, "columns"):
            return _err("Input must be a pandas DataFrame.")
        if len(df) == 0:
            return _err("No data to plot (dataframe is empty).")

        d = df.copy()

        # ---------- required columns
        if x_col not in d.columns:
            return _err(f"Missing required x column: '{x_col}'.")

        if (y1_col not in d.columns) and (y2_col not in d.columns) and (y3_col not in d.columns):
            return _err(f"None of y-columns exist in df: '{y1_col}', '{y2_col}', '{y3_col}'.")

        # ---------- category
        if category_col not in d.columns:
            d[category_col] = ""
        else:
            d[category_col] = d[category_col].fillna("").astype(str)

        # ---------- numeric conversion
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")

        for yc in (y1_col, y2_col, y3_col, line1_col, line2_col):
            if yc and yc in d.columns:
                d[yc] = pd.to_numeric(d[yc], errors="coerce")

        # ---------- drop invalid x
        d = d.dropna(subset=[x_col]).copy()
        if len(d) == 0:
            return _err(f"No valid '{x_col}' values after numeric conversion.")

        # ---------- require category
        if require_category:
            d = d[d[category_col].astype(str).str.strip().ne("")]
            if len(d) == 0:
                return _err(f"No rows with non-empty '{category_col}' after filtering.")

        def _finite_count(col: str) -> int:
            if not col or col not in d.columns:
                return 0
            arr = pd.to_numeric(d[col], errors="coerce").to_numpy(dtype=float)
            return int(np.isfinite(arr).sum())

        if (_finite_count(y1_col) + _finite_count(y2_col) + _finite_count(y3_col)) == 0:
            return _err("No numeric Y values available to plot (all Y columns are empty/NaN).")

        # ---------- sort
        d = d.sort_values(by=[x_col]).copy()

        # ---------- station factors
        stations = d[x_col].to_numpy(dtype=float)
        station_factors = [str(int(s)) if float(s).is_integer() else str(s) for s in stations]
        d["_station_factor"] = station_factors

        x_factors = d["_station_factor"].drop_duplicates().tolist()
        if not x_factors:
            return _err("No stations available after processing.")

        shared_x = FactorRange(*x_factors)

        # ---------- x label density
        try:
            step = int(x_tick_step)
            if step < 1:
                step = 1
        except Exception:
            step = 1

        x_label_overrides = {
            f: (f if (i % step == 0) else "")
            for i, f in enumerate(x_factors)
        }

        # ---------- y-range helper
        def _make_y_range(dframe, y_col, extra_cols=None):
            cols = [y_col]
            if extra_cols:
                cols.extend([c for c in extra_cols if c and c in dframe.columns])

            vals = []
            for c in cols:
                if c not in dframe.columns:
                    continue
                arr = pd.to_numeric(dframe[c], errors="coerce").to_numpy(dtype=float)
                arr = arr[np.isfinite(arr)]
                if arr.size:
                    vals.append(arr)

            if not vals:
                return Range1d(start=-1, end=1)

            arr = np.concatenate(vals)
            y_min, y_max = float(np.min(arr)), float(np.max(arr))
            pad = (y_max - y_min) * 0.05 if (y_max - y_min) > 0 else (abs(y_min) * 0.05 + 1.0)

            if reverse_y_if_negative and y_min < 0:
                return Range1d(start=y_max + pad, end=y_min - pad)
            return Range1d(start=y_min - pad, end=y_max + pad)

        # ---------- palette for categories
        base_palette = [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
            "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
        ]

        categories = sorted(d[category_col].unique().tolist())
        if not categories:
            categories = [""]

        # ---------- shared line source
        line_src = ColumnDataSource(
            data=dict(
                station=d["_station_factor"].to_numpy(),
                station_num=d[x_col].to_numpy(dtype=float),
                line1=(pd.to_numeric(d[line1_col], errors="coerce").to_numpy(dtype=float)
                       if line1_col and line1_col in d.columns else np.full(len(d), np.nan)),
                line2=(pd.to_numeric(d[line2_col], errors="coerce").to_numpy(dtype=float)
                       if line2_col and line2_col in d.columns else np.full(len(d), np.nan)),
                rov=(d[rov_col].astype(str).fillna("").to_numpy()
                     if rov_col in d.columns else np.array([""] * len(d))),
                ts=(d[ts_col].astype(str).fillna("").to_numpy()
                    if ts_col in d.columns else np.array([""] * len(d))),
                line=(d[line_col].astype(str).fillna("").to_numpy()
                      if line_col in d.columns else np.array([""] * len(d))),
                point=(d[point_col].astype(str).fillna("").to_numpy()
                       if (point_col and point_col in d.columns) else np.array([""] * len(d))),
            )
        )

        # ---------- build one plot
        def _build_one_plot(plot_title, y_col, y_label_text, add_lines=True):
            if y_col not in d.columns:
                p = figure(
                    title=f"{plot_title} (missing '{y_col}')",
                    x_range=shared_x,
                    sizing_mode="stretch_both",
                    tools="pan,wheel_zoom,box_zoom,reset,save",
                    active_scroll="wheel_zoom",
                    min_border_right=35,
                )
                return p, []

            if _finite_count(y_col) == 0:
                p = figure(
                    title=f"{plot_title} (no numeric values in '{y_col}')",
                    x_range=shared_x,
                    sizing_mode="stretch_both",
                    tools="pan,wheel_zoom,box_zoom,reset,save",
                    active_scroll="wheel_zoom",
                    min_border_right=35,
                )
                return p, []

            p = figure(
                title=plot_title,
                x_range=shared_x,
                y_range=_make_y_range(
                    d, y_col,
                    extra_cols=[line1_col if add_lines else None, line2_col if add_lines else None]
                ),
                x_axis_label="Station",
                y_axis_label=y_axis_label,
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
                sizing_mode="stretch_both",
                min_border_right=35,
            )

            p.xgrid.grid_line_alpha = 0.15
            p.ygrid.grid_line_alpha = 0.15
            p.xaxis.major_label_orientation = 1.5708
            p.xaxis.major_label_text_font_size = x_tick_font_size
            p.xaxis.major_label_standoff = 6
            p.xaxis.major_label_overrides = x_label_overrides

            hover = HoverTool(
                mode="mouse",
                tooltips=[
                    ("Line", "@line"),
                    ("ROV", "@rov"),
                    ("TimeStamp", "@ts"),
                    ("Station", "@station"),
                    (y_label_text, "@y{0.00}"),
                    *(([(line1_label or line1_col, "@line1{0.00}")] if line1_col and line1_col in d.columns else [])),
                    *(([(line2_label or line2_col, "@line2{0.00}")] if line2_col and line2_col in d.columns else [])),
                ],
            )
            p.add_tools(hover)

            renderers = []
            legend_items = []

            # ----- category bars
            for i, cat in enumerate(categories):
                sub = d[d[category_col] == cat]
                if len(sub) == 0:
                    continue

                y_arr = pd.to_numeric(sub[y_col], errors="coerce").to_numpy(dtype=float)

                src = ColumnDataSource(
                    data=dict(
                        station=sub["_station_factor"].to_numpy(),
                        y=y_arr,
                        rov=(sub[rov_col].astype(str).fillna("").to_numpy()
                             if rov_col in sub.columns else np.array([""] * len(sub))),
                        ts=(sub[ts_col].astype(str).fillna("").to_numpy()
                            if ts_col in sub.columns else np.array([""] * len(sub))),
                        line=(sub[line_col].astype(str).fillna("").to_numpy()
                              if line_col in sub.columns else np.array([""] * len(sub))),
                        point=(sub[point_col].astype(str).fillna("").to_numpy()
                               if (point_col and point_col in sub.columns) else np.array([""] * len(sub))),
                        line1=(pd.to_numeric(sub[line1_col], errors="coerce").to_numpy(dtype=float)
                               if line1_col and line1_col in sub.columns else np.full(len(sub), np.nan)),
                        line2=(pd.to_numeric(sub[line2_col], errors="coerce").to_numpy(dtype=float)
                               if line2_col and line2_col in sub.columns else np.full(len(sub), np.nan)),
                    )
                )

                color = base_palette[i % len(base_palette)]
                r = p.vbar(
                    x="station",
                    top="y",
                    width=0.82,
                    source=src,
                    fill_color=color,
                    line_color=color,
                    fill_alpha=0.85,
                )
                renderers.append(r)
                legend_items.append(LegendItem(label=f"{y_label_text} · {cat}", renderers=[r]))

            # ----- two overlay lines
            if add_lines:
                if line1_col and line1_col in d.columns and _finite_count(line1_col) > 0:
                    r_l1 = p.line(
                        x="station",
                        y="line1",
                        source=line_src,
                        line_width=line_line_width,
                        color=line1_color,
                        alpha=0.95,
                    )
                    r_l1c = p.circle(
                        x="station",
                        y="line1",
                        source=line_src,
                        size=line_marker_size,
                        color=line1_color,
                        alpha=0.95,
                    )
                    renderers.extend([r_l1, r_l1c])
                    legend_items.append(LegendItem(
                        label=(line1_label or line1_col),
                        renderers=[r_l1, r_l1c]
                    ))

                if line2_col and line2_col in d.columns and _finite_count(line2_col) > 0:
                    r_l2 = p.line(
                        x="station",
                        y="line2",
                        source=line_src,
                        line_width=line_line_width,
                        color=line2_color,
                        alpha=0.95,
                    )
                    r_l2c = p.circle(
                        x="station",
                        y="line2",
                        source=line_src,
                        size=line_marker_size,
                        color=line2_color,
                        alpha=0.95,
                    )
                    renderers.extend([r_l2, r_l2c])
                    legend_items.append(LegendItem(
                        label=(line2_label or line2_col),
                        renderers=[r_l2, r_l2c]
                    ))

            hover.renderers = renderers
            return p, legend_items

        s1_label = y1_label or y1_col
        s2_label = y2_label or y2_col
        s3_label = y3_label or y3_col

        p1, leg_items1 = _build_one_plot(title1, y1_col, s1_label, add_lines=show_lines_on_all_plots)
        p2, leg_items2 = _build_one_plot(title2, y2_col, s2_label, add_lines=show_lines_on_all_plots)
        p3, leg_items3 = _build_one_plot(title3, y3_col, s3_label, add_lines=show_lines_on_all_plots)

        p1.xaxis.visible = False
        p2.xaxis.visible = False

        all_leg_items = leg_items1 + leg_items2 + leg_items3
        if all_leg_items:
            L = Legend(
                items=all_leg_items,
                title=legend_title,
                orientation="horizontal",
                click_policy="hide",
                spacing=6,
                padding=6,
                margin=4,
                label_text_font_size="9pt",
                title_text_font_size="9pt",
            )
            p1.add_layout(L, "above")
        else:
            L = None

        stack = gridplot(
            [[p1], [p2], [p3]],
            merge_tools=True,
            toolbar_location="above",
            sizing_mode="stretch_both",
        )

        btn_toggle = Button(label="Legend", button_type="default", width=80)
        if L is not None:
            btn_toggle.js_on_click(CustomJS(args=dict(leg=L), code="leg.visible = !leg.visible;"))
        else:
            btn_toggle.disabled = True

        layout = column(row(btn_toggle), stack, sizing_mode="stretch_both")

        if is_show and not json_return:
            show(layout)

        return json_item(layout) if json_return else layout

    def bokeh_two_series_vs_station_with_diff_bar(
            self,
            df,
            *,
            title="Two Series vs Station with Diff",
            x_col="Station",
            series1_col="PrimaryElevation",
            series2_col="SecondaryElevation",
            series1_label=None,
            series2_label=None,
            diff_label=None,
            rov_col="ROV",
            line_col="Line",
            point_col="Point",
            ts_col="TimeStamp",
            require_rov=False,
            y_label="Water depth",
            diff_y_label="Difference",
            diff_mode="series1_minus_series2",  # "series1_minus_series2" | "series2_minus_series1" | "abs"
            reverse_y_if_negative=True,
            x_tick_step=2,
            json_return=False,
            is_show=False,
    ):
        """
        Two stacked Bokeh plots:
          - top: difference bars (series1 - series2) colored by ROV
          - bottom: two line plots (Primary / Secondary)
        Shared numeric X axis.

        Legend:
          - top plot -> ROV categories
          - bottom plot -> only series1 / series2

        JS buttons:
          - Legend: show/hide both legends
          - Move: move both legends through plot corners
        """

        # -------------------------------------------------
        # safe empty
        # -------------------------------------------------
        if df is None or len(df) == 0:
            p = figure(
                title=title,
                sizing_mode="stretch_both",
                tools="pan,wheel_zoom,box_zoom,reset,save",
            )
            layout = column(p, sizing_mode="stretch_both")
            if is_show and not json_return:
                show(layout)
            return json_item(layout) if json_return else layout

        d = df.copy()

        # -------------------------------------------------
        # optional filter by ROV not empty
        # -------------------------------------------------
        if require_rov and rov_col in d.columns:
            d = d[d[rov_col].astype(str).str.strip().ne("")]
        elif require_rov and rov_col not in d.columns:
            d = d.iloc[0:0]

        if len(d) == 0:
            p = figure(
                title=f"{title} (no rows)",
                sizing_mode="stretch_both",
                tools="pan,wheel_zoom,box_zoom,reset,save",
            )
            layout = column(p, sizing_mode="stretch_both")
            if is_show and not json_return:
                show(layout)
            return json_item(layout) if json_return else layout

        # -------------------------------------------------
        # numeric conversion
        # -------------------------------------------------
        for c in (x_col, series1_col, series2_col):
            if c in d.columns:
                d[c] = pd.to_numeric(d[c], errors="coerce")

        d = d.dropna(subset=[x_col]).copy()
        d = d.sort_values(by=[x_col]).reset_index(drop=True)

        if len(d) == 0:
            p = figure(
                title=f"{title} (no valid {x_col})",
                sizing_mode="stretch_both",
                tools="pan,wheel_zoom,box_zoom,reset,save",
            )
            layout = column(p, sizing_mode="stretch_both")
            if is_show and not json_return:
                show(layout)
            return json_item(layout) if json_return else layout

        # -------------------------------------------------
        # safe string helper
        # -------------------------------------------------
        def _safe_str(col_name):
            if col_name in d.columns:
                return d[col_name].fillna("").astype(str)
            return pd.Series([""] * len(d), index=d.index)

        # -------------------------------------------------
        # diff
        # -------------------------------------------------
        if diff_mode == "series2_minus_series1":
            d["_diff_"] = d[series2_col] - d[series1_col]
            if diff_label is None:
                diff_label = f"{series2_label or series2_col} - {series1_label or series1_col}"
        elif diff_mode == "abs":
            d["_diff_"] = (d[series1_col] - d[series2_col]).abs()
            if diff_label is None:
                diff_label = f"|{series1_label or series1_col} - {series2_label or series2_col}|"
        else:
            d["_diff_"] = d[series1_col] - d[series2_col]
            if diff_label is None:
                diff_label = f"{series1_label or series1_col} - {series2_label or series2_col}"

        l1 = series1_label or series1_col
        l2 = series2_label or series2_col

        # -------------------------------------------------
        # x range + bar width
        # -------------------------------------------------
        x_vals = pd.to_numeric(d[x_col], errors="coerce").dropna().to_list()

        if not x_vals:
            p = figure(
                title=f"{title} (no stations)",
                sizing_mode="stretch_both",
                tools="pan,wheel_zoom,box_zoom,reset,save",
            )
            layout = column(p, sizing_mode="stretch_both")
            if is_show and not json_return:
                show(layout)
            return json_item(layout) if json_return else layout

        x_min = float(min(x_vals))
        x_max = float(max(x_vals))
        if x_min == x_max:
            x_min -= 1.0
            x_max += 1.0

        shared_x = Range1d(x_min, x_max)

        if len(x_vals) > 1:
            spacings = [b - a for a, b in zip(x_vals, x_vals[1:]) if (b - a) > 0]
            step = min(spacings) if spacings else 1.0
            bar_width = step * 0.8
        else:
            step = 1.0
            bar_width = 1.0

        # -------------------------------------------------
        # y ranges
        # -------------------------------------------------
        y_all_main = np.concatenate([
            pd.to_numeric(d[series1_col], errors="coerce").dropna().to_numpy(dtype=float)
            if series1_col in d.columns else np.array([], dtype=float),
            pd.to_numeric(d[series2_col], errors="coerce").dropna().to_numpy(dtype=float)
            if series2_col in d.columns else np.array([], dtype=float),
        ])

        if y_all_main.size == 0:
            y1_min, y1_max = -1.0, 1.0
        else:
            y1_min, y1_max = float(np.min(y_all_main)), float(np.max(y_all_main))

        pad1 = (y1_max - y1_min) * 0.05 if (y1_max - y1_min) > 0 else (abs(y1_min) * 0.05 + 1.0)

        if reverse_y_if_negative and y1_min < 0:
            main_y_range = Range1d(start=y1_max + pad1, end=y1_min - pad1)
        else:
            main_y_range = Range1d(start=y1_min - pad1, end=y1_max + pad1)

        y_diff = pd.to_numeric(d["_diff_"], errors="coerce").dropna().to_numpy(dtype=float)
        if y_diff.size == 0:
            yd_min, yd_max = -1.0, 1.0
        else:
            yd_min, yd_max = float(np.min(y_diff)), float(np.max(y_diff))

        pad2 = (yd_max - yd_min) * 0.05 if (yd_max - yd_min) > 0 else (abs(yd_min) * 0.05 + 1.0)
        diff_y_range = Range1d(start=yd_min - pad2, end=yd_max + pad2)

        # -------------------------------------------------
        # figures
        # -------------------------------------------------
        p_top = figure(
            title=f"{title} | Diff",
            x_axis_label="Station",
            y_axis_label=diff_y_label,
            x_range=shared_x,
            y_range=diff_y_range,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
            sizing_mode="stretch_both",
            min_height=220,
        )

        p_main = figure(
            title=title,
            x_axis_label="Station",
            y_axis_label=y_label,
            x_range=shared_x,
            y_range=main_y_range,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
            sizing_mode="stretch_both",
            min_height=420,
        )

        # -------------------------------------------------
        # top plot by ROV
        # separate renderers -> separate legend items
        # -------------------------------------------------
        if rov_col in d.columns:
            d["_rov_"] = d[rov_col].fillna("").astype(str).str.strip()
        else:
            d["_rov_"] = ""

        rov_values = sorted([v for v in d["_rov_"].unique().tolist() if v != ""])

        if not rov_values:
            rov_values = [""]

        def _pick_palette(n):
            if n <= 10:
                return list(Category10[10])[:n]
            if n <= 20:
                return list(Category20[20])[:n]
            idx = np.linspace(0, 255, n).astype(int)
            return [Turbo256[i] for i in idx]

        palette = _pick_palette(len(rov_values))
        rov_to_color = {rov: palette[i] for i, rov in enumerate(rov_values)}

        top_renderers = []
        for rov in rov_values:
            sub = d[d["_rov_"] == rov].copy()
            if len(sub) == 0:
                continue

            src_bar = ColumnDataSource(data=dict(
                x=sub[x_col].to_numpy(dtype=float),
                diff=sub["_diff_"].to_numpy(dtype=float),
                line=_safe_str(line_col).loc[sub.index].to_numpy(),
                point=_safe_str(point_col).loc[sub.index].to_numpy(),
                rov=sub["_rov_"].to_numpy(),
                ts=_safe_str(ts_col).loc[sub.index].to_numpy(),
                s1=pd.to_numeric(sub[series1_col], errors="coerce").to_numpy(dtype=float),
                s2=pd.to_numeric(sub[series2_col], errors="coerce").to_numpy(dtype=float),
            ))

            r = p_top.vbar(
                x="x",
                top="diff",
                width=bar_width,
                source=src_bar,
                fill_color=rov_to_color[rov],
                line_color=rov_to_color[rov],
                fill_alpha=0.90,
                legend_label=(rov if rov != "" else "N/A"),
            )
            top_renderers.append(r)

        p_top.add_tools(
            HoverTool(
                renderers=top_renderers,
                mode="mouse",
                tooltips=[
                    ("Line", "@line"),
                    ("Point", "@point"),
                    ("ROV", "@rov"),
                    ("TimeStamp", "@ts"),
                    ("Station", "@x{0.##}"),
                    (l1, "@s1{0.00}"),
                    (l2, "@s2{0.00}"),
                    (diff_label, "@diff{0.00}"),
                ],
            )
        )

        zero_line = Span(location=0, dimension="width", line_width=1, line_color="black", line_alpha=0.8)
        p_top.add_layout(zero_line)

        # -------------------------------------------------
        # main plot source
        # -------------------------------------------------
        source = ColumnDataSource(data=dict(
            x=d[x_col].to_numpy(dtype=float),
            s1=d[series1_col].to_numpy(dtype=float) if series1_col in d.columns else np.full(len(d), np.nan),
            s2=d[series2_col].to_numpy(dtype=float) if series2_col in d.columns else np.full(len(d), np.nan),
            line=_safe_str(line_col).to_numpy(),
            point=_safe_str(point_col).to_numpy(),
            rov=_safe_str(rov_col).to_numpy(),
            ts=_safe_str(ts_col).to_numpy(),
        ))

        c1, c2 = Category10[10][0], Category10[10][1]

        r1 = p_main.line("x", "s1", source=source, line_width=2, color=c1, legend_label=l1)
        s1r = p_main.scatter("x", "s1", source=source, size=6, color=c1, legend_label=l1)

        r2 = p_main.line("x", "s2", source=source, line_width=2, line_dash="dashed", color=c2, legend_label=l2)
        s2r = p_main.scatter("x", "s2", source=source, size=6, color=c2, legend_label=l2)

        p_main.add_tools(
            HoverTool(
                renderers=[s1r, s2r],
                mode="mouse",
                tooltips=[
                    ("Line", "@line"),
                    ("Point", "@point"),
                    ("ROV", "@rov"),
                    ("TimeStamp", "@ts"),
                    ("Station", "@x{0.##}"),
                    (l1, "@s1{0.00}"),
                    (l2, "@s2{0.00}"),
                ],
            )
        )

        # -------------------------------------------------
        # x ticks every 2nd node, vertical labels
        # -------------------------------------------------
        try:
            step_tick = int(x_tick_step)
            if step_tick < 1:
                step_tick = 1
        except Exception:
            step_tick = 2

        x_unique = sorted(pd.Series(x_vals).dropna().unique().tolist())
        major_ticks = x_unique[::step_tick] if x_unique else []

        if x_unique and x_unique[-1] not in major_ticks:
            major_ticks.append(x_unique[-1])

        tick_labels = {}
        for v in major_ticks:
            if float(v).is_integer():
                tick_labels[v] = str(int(v))
            else:
                tick_labels[v] = str(v)

        for pp in (p_top, p_main):
            pp.xaxis.ticker = major_ticks
            pp.xaxis.major_label_overrides = tick_labels
            pp.xaxis.major_label_orientation = 1.5708
            pp.xgrid.grid_line_alpha = 0.15
            pp.ygrid.grid_line_alpha = 0.15

        p_top.xaxis.visible = False

        # -------------------------------------------------
        # legend styling
        # -------------------------------------------------
        if len(p_top.legend) > 0:
            p_top.legend.title = "ROV"
            p_top.legend.click_policy = "hide"
            p_top.legend.location = "top_right"
            p_top.legend.label_text_font_size = "9pt"
            p_top.legend.spacing = 2
            p_top.legend.padding = 4
            p_top.legend.margin = 4

        if len(p_main.legend) > 0:
            p_main.legend.click_policy = "hide"
            p_main.legend.location = "top_right"
            p_main.legend.label_text_font_size = "9pt"
            p_main.legend.spacing = 2
            p_main.legend.padding = 4
            p_main.legend.margin = 4

        # -------------------------------------------------
        # JS buttons: toggle + move both legends
        # -------------------------------------------------
        btn_toggle = Button(label="Legend", button_type="default", width=80)
        btn_move = Button(label="Move", button_type="default", width=70)

        top_leg = p_top.legend[0] if len(p_top.legend) > 0 else None
        main_leg = p_main.legend[0] if len(p_main.legend) > 0 else None

        btn_toggle.js_on_click(CustomJS(
            args=dict(leg1=top_leg, leg2=main_leg),
            code="""
                if (leg1) { leg1.visible = !leg1.visible; }
                if (leg2) { leg2.visible = !leg2.visible; }
            """
        ))

        btn_move.js_on_click(CustomJS(
            args=dict(leg1=top_leg, leg2=main_leg),
            code="""
                const locs = ["top_right","top_left","bottom_left","bottom_right"];

                function moveLegend(leg) {
                    if (!leg) return;
                    const cur = leg.location || "top_right";
                    const i = locs.indexOf(cur);
                    leg.location = locs[(i + 1) % locs.length];
                }

                moveLegend(leg1);
                moveLegend(leg2);
            """
        ))

        stack = gridplot(
            [[p_top], [p_main]],
            merge_tools=True,
            toolbar_location="above",
            sizing_mode="stretch_both",
        )

        layout = column(
            row(btn_toggle, btn_move),
            stack,
            sizing_mode="stretch_both",
        )

        if is_show and not json_return:
            show(layout)

        return json_item(layout) if json_return else layout

    def read_rec_db_preplot_all(self) -> pd.DataFrame:
        """
        Read all REC_DB rows joined with DSR preplot coordinates.

        REC_DB schema supported:
          REC_DB.Line
          REC_DB.Point   -> joins to DSR.Station
          REC_DB.REC_X
          REC_DB.REC_Y

        DSR schema required:
          DSR.Line
          DSR.Station
          DSR.ROV
          DSR.PreplotEasting
          DSR.PreplotNorthing
        """
        with self._connect() as conn:
            rec_cols = {
                r["name"] for r in conn.execute("PRAGMA table_info(REC_DB)").fetchall()
            }
            dsr_cols = {
                r["name"] for r in conn.execute("PRAGMA table_info(DSR)").fetchall()
            }

            if not rec_cols:
                raise ValueError("Table REC_DB not found.")

            if not dsr_cols:
                raise ValueError("Table DSR not found.")

            missing_rec = [c for c in ["REC_X", "REC_Y"] if c not in rec_cols]
            if missing_rec:
                raise ValueError(f"REC_DB missing columns: {missing_rec}")

            missing_dsr = [
                c for c in [
                    "Line",
                    "Station",
                    "ROV",
                    "PreplotEasting",
                    "PreplotNorthing",
                ]
                if c not in dsr_cols
            ]
            if missing_dsr:
                raise ValueError(f"DSR missing columns: {missing_dsr}")

            if "Line" in rec_cols and "Point" in rec_cols:
                join_sql = """
                    CAST(r.Line AS INTEGER) = CAST(d.Line AS INTEGER)
                    AND CAST(r.Point AS REAL) = CAST(d.Station AS REAL)
                """
            elif "Line" in rec_cols and "Station" in rec_cols:
                join_sql = """
                    CAST(r.Line AS INTEGER) = CAST(d.Line AS INTEGER)
                    AND CAST(r.Station AS REAL) = CAST(d.Station AS REAL)
                """
            elif "Point" in rec_cols:
                join_sql = """
                    CAST(r.Point AS REAL) = CAST(d.Station AS REAL)
                """
            elif "Station" in rec_cols:
                join_sql = """
                    CAST(r.Station AS REAL) = CAST(d.Station AS REAL)
                """
            elif "Node" in rec_cols and "Node" in dsr_cols:
                join_sql = """
                    CAST(r.Node AS TEXT) = CAST(d.Node AS TEXT)
                """
            else:
                raise ValueError("REC_DB must contain Point, Station, or Node for join to DSR.")

            dsr_extra = []
            for c in ["Node", "TimeStamp", "TimeStamp1"]:
                if c in dsr_cols:
                    dsr_extra.append(f"d.{c} AS {c}")

            rec_extra = []
            for c in [
                "ID",
                "File_FK",
                "Preplot_FK",
                "REC_ID",
                "NODE_ID",
                "Line",
                "Point",
                "LinePoint",
                "LinePointIdx",
                "TierLine",
                "TierLinePoint",
                "RPRE_X",
                "RPRE_Y",
                "RFIELD_X",
                "RFIELD_Y",
                "RFIELD_Z",
                "REC_Z",
            ]:
                if c in rec_cols:
                    rec_extra.append(f"r.{c} AS rec_{c}")

            extra_sql = ""
            if dsr_extra or rec_extra:
                extra_sql = ",\n" + ",\n".join(dsr_extra + rec_extra)

            sql = f"""
                SELECT
                    d.Line,
                    d.Station,
                    d.ROV,
                    d.PreplotEasting,
                    d.PreplotNorthing,
                    r.REC_X,
                    r.REC_Y
                    {extra_sql}
                FROM DSR d
                INNER JOIN REC_DB r
                    ON {join_sql}
                WHERE d.ROV IS NOT NULL
                  AND TRIM(CAST(d.ROV AS TEXT)) <> ''
                  AND d.PreplotEasting IS NOT NULL
                  AND d.PreplotNorthing IS NOT NULL
                  AND r.REC_X IS NOT NULL
                  AND r.REC_Y IS NOT NULL
                ORDER BY CAST(d.Line AS INTEGER), CAST(d.Station AS REAL)
            """

            return pd.read_sql_query(sql, conn)

    def bokeh_recdb_histograms_all(
            self,
            *,
            bins=40,
            title_prefix="REC_DB vs PREPLOT",
            json_return=False,
            is_show=False,
            export_html_path=None,
            export_resources="inline",
            show_std_lines=True,
            show_kde=True,
            kde_points=300,
            kde_max_samples=5000,
            max_offset=None,
            qc_limits=None,
    ):
        try:
            if qc_limits is None:
                qc_limits = {
                    "Inline": [-2.0, 2.0],
                    "Xline": [-2.0, 2.0],
                    "RangeToPreplot": [2.0, 5.0],
                }

            def _clean_array(vals):
                arr = np.asarray(vals, dtype=float)
                return arr[np.isfinite(arr)]

            df = self.read_rec_db_preplot_all()

            if df is None or df.empty:
                return self._error_layout(
                    title="REC_DB vs PREPLOT Histogram",
                    message="No REC_DB rows joined to DSR / PREPLOT.",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            d = df.copy()

            for c in ["Line", "Station", "REC_X", "REC_Y", "PreplotEasting", "PreplotNorthing"]:
                d[c] = pd.to_numeric(d[c], errors="coerce")

            d = d.dropna(
                subset=["Line", "Station", "REC_X", "REC_Y", "PreplotEasting", "PreplotNorthing"]
            )

            if d.empty:
                return self._error_layout(
                    title="REC_DB vs PREPLOT Histogram",
                    message="No valid REC_DB / PREPLOT coordinates after numeric cleanup.",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            d["ROV"] = d["ROV"].astype(str).fillna("").str.strip()
            d = d[d["ROV"] != ""]

            d["dx"] = d["REC_X"] - d["PreplotEasting"]
            d["dy"] = d["REC_Y"] - d["PreplotNorthing"]
            d["RangeToPreplot"] = np.sqrt(d["dx"] ** 2 + d["dy"] ** 2)

            d["Inline"] = np.nan
            d["Xline"] = np.nan

            for line_value, idx in d.groupby("Line").groups.items():
                sub = d.loc[idx].sort_values("Station")

                if len(sub) < 2:
                    bearing_rad = 0.0
                else:
                    x1 = float(sub["PreplotEasting"].iloc[0])
                    y1 = float(sub["PreplotNorthing"].iloc[0])
                    x2 = float(sub["PreplotEasting"].iloc[-1])
                    y2 = float(sub["PreplotNorthing"].iloc[-1])
                    bearing_rad = np.arctan2(x2 - x1, y2 - y1)

                u_inline_e = np.sin(bearing_rad)
                u_inline_n = np.cos(bearing_rad)

                u_xline_e = np.cos(bearing_rad)
                u_xline_n = -np.sin(bearing_rad)

                d.loc[idx, "Inline"] = (
                        d.loc[idx, "dx"] * u_inline_e
                        + d.loc[idx, "dy"] * u_inline_n
                )
                d.loc[idx, "Xline"] = (
                        d.loc[idx, "dx"] * u_xline_e
                        + d.loc[idx, "dy"] * u_xline_n
                )

            plots = [
                ("Inline", "Inline Offset", "Inline, m"),
                ("Xline", "Xline Offset", "Xline, m"),
                ("RangeToPreplot", "Range To Preplot", "Range, m"),
            ]

            def _metric_limits(field, vals):
                vals = _clean_array(vals)

                if vals.size == 0:
                    return -0.5, 0.5

                vmin = float(np.min(vals))
                vmax = float(np.max(vals))

                if field == "RangeToPreplot":
                    vmin = 0.0

                if max_offset is not None:
                    mo = abs(float(max_offset))
                    if field == "RangeToPreplot":
                        vmin, vmax = 0.0, mo
                    else:
                        vmin, vmax = -mo, mo

                if vmin == vmax:
                    vmin -= 0.5
                    vmax += 0.5

                pad = (vmax - vmin) * 0.05
                if field == "RangeToPreplot":
                    return 0.0, vmax + pad
                return vmin - pad, vmax + pad

            x_ranges = {}
            for field, title, label in plots:
                vmin, vmax = _metric_limits(field, d[field])
                x_ranges[field] = Range1d(start=vmin, end=vmax)

            def _stats_dict(vals):
                s = _clean_array(vals)

                if s.size == 0:
                    return {
                        "n": 0,
                        "min": np.nan,
                        "max": np.nan,
                        "mean": np.nan,
                        "std": np.nan,
                        "p95": np.nan,
                        "p99": np.nan,
                    }

                std = float(np.std(s, ddof=1)) if s.size > 1 else 0.0

                return {
                    "n": int(s.size),
                    "min": float(np.min(s)),
                    "max": float(np.max(s)),
                    "mean": float(np.mean(s)),
                    "std": std,
                    "p95": float(np.quantile(s, 0.95)),
                    "p99": float(np.quantile(s, 0.99)),
                }

            def _stats_text(vals):
                st = _stats_dict(vals)
                if st["n"] == 0:
                    return "no data"
                return (
                    f"avg:{st['mean']:.2f}; std:{st['std']:.2f}; "
                    f"p95:{st['p95']:.2f}; p99:{st['p99']:.2f}"
                )

            def _outlier_count(vals, field):
                s = _clean_array(vals)

                if s.size == 0:
                    return 0

                limits = qc_limits.get(field)
                if not limits:
                    return 0

                if field == "RangeToPreplot":
                    limit = max(abs(float(x)) for x in limits)
                    return int(np.sum(s > limit))

                low = min(float(x) for x in limits)
                high = max(float(x) for x in limits)

                return int(np.sum((s < low) | (s > high)))

            def _kde_xy(vals, xmin, xmax, hist_bin_width):
                vals = _clean_array(vals)

                if vals.size < 2:
                    return None, None

                if vals.size > kde_max_samples:
                    vals = np.random.choice(
                        vals,
                        size=int(kde_max_samples),
                        replace=False,
                    )

                std = float(np.std(vals, ddof=1))
                if not np.isfinite(std) or std <= 0:
                    return None, None

                n = vals.size
                bandwidth = 1.06 * std * (n ** (-1 / 5))

                if not np.isfinite(bandwidth) or bandwidth <= 0:
                    return None, None

                xs = np.linspace(xmin, xmax, int(kde_points))
                density = np.zeros_like(xs, dtype=float)

                chunk = 1000
                for i in range(0, n, chunk):
                    v = vals[i:i + chunk]
                    z = (xs[:, None] - v[None, :]) / bandwidth
                    density += np.exp(-0.5 * z * z).sum(axis=1)

                density /= n * bandwidth * np.sqrt(2 * np.pi)
                ys = density * n * hist_bin_width

                return xs, ys

            def _make_hist(rov_df, rov_name, field, plot_title, x_label):
                vals_all = _clean_array(rov_df[field])

                x_range = x_ranges[field]
                vmin = float(x_range.start)
                vmax = float(x_range.end)

                edges = np.linspace(vmin, vmax, int(bins) + 1)
                hist, e = np.histogram(vals_all, bins=edges)

                bin_width = float(e[1] - e[0]) if len(e) > 1 else 1.0

                src = ColumnDataSource(
                    data=dict(
                        top=hist,
                        left=e[:-1],
                        right=e[1:],
                        bin_center=((e[:-1] + e[1:]) / 2.0),
                        rov=[rov_name] * len(hist),
                        percent=[
                            (float(v) / max(len(vals_all), 1)) * 100.0
                            for v in hist
                        ],
                    )
                )

                st = _stats_dict(vals_all)
                outliers = _outlier_count(vals_all, field)

                p = figure(
                    title=f"{plot_title} | {_stats_text(vals_all)} | outliers:{outliers}",
                    x_axis_label=x_label,
                    y_axis_label="Count",
                    x_range=x_range,
                    sizing_mode="stretch_width",
                    min_height=360,
                    tools="pan,wheel_zoom,box_zoom,reset,save",
                    active_scroll="wheel_zoom",
                )

                r = p.quad(
                    top="top",
                    bottom=0,
                    left="left",
                    right="right",
                    source=src,
                    fill_alpha=0.55,
                    line_alpha=0.9,
                    legend_label=str(rov_name),
                )

                p.add_tools(
                    HoverTool(
                        renderers=[r],
                        tooltips=[
                            ("ROV", "@rov"),
                            ("Bin center", "@bin_center{0.00}"),
                            ("Count", "@top"),
                            ("Percent", "@percent{0.00}%"),
                        ],
                    )
                )

                if field != "RangeToPreplot":
                    p.add_layout(
                        Span(
                            location=0,
                            dimension="height",
                            line_color="black",
                            line_dash="dashed",
                            line_width=1,
                            line_alpha=0.7,
                        )
                    )

                limits = qc_limits.get(field)
                if limits:
                    for lim in limits:
                        lim = float(lim)
                        if field == "RangeToPreplot" and lim < 0:
                            continue
                        p.add_layout(
                            Span(
                                location=lim,
                                dimension="height",
                                line_color="red",
                                line_dash="dotdash",
                                line_width=2,
                                line_alpha=0.75,
                            )
                        )

                if show_std_lines and st["n"] > 1 and np.isfinite(st["std"]):
                    mean = st["mean"]
                    std = st["std"]

                    p.add_layout(
                        Span(
                            location=mean,
                            dimension="height",
                            line_color="blue",
                            line_dash="solid",
                            line_width=2,
                            line_alpha=0.85,
                        )
                    )

                    for mult, dash, alpha in [
                        (1, "dashed", 0.75),
                        (2, "dotted", 0.65),
                    ]:
                        for x in [mean - mult * std, mean + mult * std]:
                            if field == "RangeToPreplot" and x < 0:
                                continue
                            p.add_layout(
                                Span(
                                    location=float(x),
                                    dimension="height",
                                    line_color="blue",
                                    line_dash=dash,
                                    line_width=1,
                                    line_alpha=alpha,
                                )
                            )

                if show_kde and vals_all.size >= 2:
                    xs, ys = _kde_xy(vals_all, vmin, vmax, bin_width)

                    if xs is not None and ys is not None:
                        p.line(
                            xs,
                            ys,
                            line_width=3,
                            line_alpha=0.9,
                            legend_label="KDE",
                        )

                max_hist = int(np.max(hist)) if len(hist) else 0
                label_y = max_hist * 0.92 if max_hist > 0 else 1

                p.add_layout(
                    Label(
                        x=vmin + (vmax - vmin) * 0.02,
                        y=label_y,
                        text=f"P95={st['p95']:.2f} | P99={st['p99']:.2f} | Out={outliers}",
                        text_font_size="9pt",
                        text_alpha=0.8,
                    )
                )

                p.legend.location = "top_right"
                p.legend.label_text_font_size = "8pt"
                p.legend.spacing = 1
                p.legend.padding = 4
                p.legend.click_policy = "hide"

                p.xgrid.visible = True
                p.ygrid.visible = True

                return p

            rovs = sorted(d["ROV"].dropna().unique().tolist())

            def _project_metric_summary(field):
                st = _stats_dict(d[field])
                out = _outlier_count(d[field], field)

                if st["n"] == 0:
                    return f"{field}: no data"

                return (
                    f"{field}: avg {st['mean']:.2f}, std {st['std']:.2f}, "
                    f"P95 {st['p95']:.2f}, P99 {st['p99']:.2f}, outliers {out}"
                )

            header = Div(
                text=f"""
                <div style="padding:8px 10px;border-left:4px solid #0d6efd;background:#f8fafc;">
                    <b>{title_prefix}</b><br>
                    Scope: <b>whole project database</b> |
                    ROVs: <b>{", ".join(rovs)}</b> |
                    Lines: <b>{d["Line"].nunique()}</b> |
                    Rows: <b>{len(d)}</b> |
                    X-axis: <b>synced by metric across ROVs</b><br>
                    <span>{_project_metric_summary("Inline")}</span><br>
                    <span>{_project_metric_summary("Xline")}</span><br>
                    <span>{_project_metric_summary("RangeToPreplot")}</span>
                </div>
                """,
                sizing_mode="stretch_width",
            )

            sections = [header]

            for rov_name in rovs:
                rov_df = d[d["ROV"] == rov_name].copy()

                inline_out = _outlier_count(rov_df["Inline"], "Inline")
                xline_out = _outlier_count(rov_df["Xline"], "Xline")
                range_out = _outlier_count(rov_df["RangeToPreplot"], "RangeToPreplot")

                rov_header = Div(
                    text=f"""
                    <div style="margin-top:14px;padding:7px 10px;
                                border-left:4px solid #198754;background:#f3f8f5;">
                        <b>ROV: {rov_name}</b> |
                        Rows: <b>{len(rov_df)}</b> |
                        Lines: <b>{rov_df["Line"].nunique()}</b> |
                        Stations: <b>{rov_df["Station"].nunique()}</b> |
                        Outliers Inline/Xline/Range:
                        <b>{inline_out}</b> / <b>{xline_out}</b> / <b>{range_out}</b>
                    </div>
                    """,
                    sizing_mode="stretch_width",
                )

                figures = [
                    _make_hist(rov_df, rov_name, field, title, label)
                    for field, title, label in plots
                ]

                sections.append(rov_header)
                sections.append(
                    gridplot(
                        [[figures[0], figures[1], figures[2]]],
                        sizing_mode="stretch_width",
                        merge_tools=True,
                        toolbar_location="above",
                    )
                )

            layout = column(*sections, sizing_mode="stretch_width")

            if export_html_path:
                export_html_path = os.path.abspath(export_html_path)
                os.makedirs(os.path.dirname(export_html_path), exist_ok=True)

                resources = INLINE
                if str(export_resources).lower() == "cdn":
                    resources = CDN

                html = file_html(layout, resources, title=title_prefix)

                with open(export_html_path, "w", encoding="utf-8") as f:
                    f.write(html)

            if is_show:
                show(layout)
                return export_html_path if export_html_path else None

            if json_return:
                return json_item(layout)

            if export_html_path:
                return export_html_path

            return layout

        except Exception as e:
            return self._error_layout(
                title="REC_DB vs PREPLOT Histogram",
                message="Failed to build REC_DB vs PREPLOT histogram plot.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )

    def read_rec_db_primary_all(self) -> pd.DataFrame:
        """
        Read all REC_DB rows joined with DSR primary deployed coordinates.

        REC_DB:
          Line
          Point -> DSR.Station
          REC_X
          REC_Y

        DSR:
          Line
          Station
          ROV
          PrimaryEasting
          PrimaryNorthing
        """
        with self._connect() as conn:
            rec_cols = {
                r["name"] for r in conn.execute("PRAGMA table_info(REC_DB)").fetchall()
            }
            dsr_cols = {
                r["name"] for r in conn.execute("PRAGMA table_info(DSR)").fetchall()
            }

            if not rec_cols:
                raise ValueError("Table REC_DB not found.")

            if not dsr_cols:
                raise ValueError("Table DSR not found.")

            missing_rec = [c for c in ["REC_X", "REC_Y"] if c not in rec_cols]
            if missing_rec:
                raise ValueError(f"REC_DB missing columns: {missing_rec}")

            missing_dsr = [
                c for c in [
                    "Line",
                    "Station",
                    "ROV",
                    "PrimaryEasting",
                    "PrimaryNorthing",
                ]
                if c not in dsr_cols
            ]
            if missing_dsr:
                raise ValueError(f"DSR missing columns: {missing_dsr}")

            if "Line" in rec_cols and "Point" in rec_cols:
                join_sql = """
                    CAST(r.Line AS INTEGER) = CAST(d.Line AS INTEGER)
                    AND CAST(r.Point AS REAL) = CAST(d.Station AS REAL)
                """
            elif "Line" in rec_cols and "Station" in rec_cols:
                join_sql = """
                    CAST(r.Line AS INTEGER) = CAST(d.Line AS INTEGER)
                    AND CAST(r.Station AS REAL) = CAST(d.Station AS REAL)
                """
            elif "Point" in rec_cols:
                join_sql = """
                    CAST(r.Point AS REAL) = CAST(d.Station AS REAL)
                """
            elif "Station" in rec_cols:
                join_sql = """
                    CAST(r.Station AS REAL) = CAST(d.Station AS REAL)
                """
            elif "Node" in rec_cols and "Node" in dsr_cols:
                join_sql = """
                    CAST(r.Node AS TEXT) = CAST(d.Node AS TEXT)
                """
            else:
                raise ValueError("REC_DB must contain Point, Station, or Node for join to DSR.")

            dsr_extra = []
            for c in ["Node", "TimeStamp", "TimeStamp1"]:
                if c in dsr_cols:
                    dsr_extra.append(f"d.{c} AS {c}")

            rec_extra = []
            for c in [
                "ID",
                "File_FK",
                "Preplot_FK",
                "REC_ID",
                "NODE_ID",
                "Line",
                "Point",
                "LinePoint",
                "LinePointIdx",
                "TierLine",
                "TierLinePoint",
                "RPRE_X",
                "RPRE_Y",
                "RFIELD_X",
                "RFIELD_Y",
                "RFIELD_Z",
                "REC_Z",
            ]:
                if c in rec_cols:
                    rec_extra.append(f"r.{c} AS rec_{c}")

            extra_sql = ""
            if dsr_extra or rec_extra:
                extra_sql = ",\n" + ",\n".join(dsr_extra + rec_extra)

            sql = f"""
                SELECT
                    d.Line,
                    d.Station,
                    d.ROV,
                    d.PrimaryEasting,
                    d.PrimaryNorthing,
                    r.REC_X,
                    r.REC_Y
                    {extra_sql}
                FROM DSR d
                INNER JOIN REC_DB r
                    ON {join_sql}
                WHERE d.ROV IS NOT NULL
                  AND TRIM(CAST(d.ROV AS TEXT)) <> ''
                  AND d.PrimaryEasting IS NOT NULL
                  AND d.PrimaryNorthing IS NOT NULL
                  AND r.REC_X IS NOT NULL
                  AND r.REC_Y IS NOT NULL
                ORDER BY CAST(d.Line AS INTEGER), CAST(d.Station AS REAL)
            """

            return pd.read_sql_query(sql, conn)

    def bokeh_recdb_primary_histograms_all(
            self,
            *,
            bins=40,
            title_prefix="REC_DB vs DSR PRIMARY",
            json_return=False,
            is_show=False,
            export_html_path=None,
            export_resources="inline",
            show_std_lines=True,
            show_kde=True,
            kde_points=300,
            kde_max_samples=5000,
            max_offset=None,
            qc_limits=None,
    ):
        """
        Whole project REC_DB vs DSR Primary coordinate histogram QC.

        Per ROV:
          1. dX = REC_X - PrimaryEasting
          2. dY = REC_Y - PrimaryNorthing
          3. RangeToPrimary

        Requires imports:
          from bokeh.models import Range1d, Span, Label
          from bokeh.embed import file_html
          from bokeh.resources import CDN, INLINE
        """
        try:
            if qc_limits is None:
                qc_limits = {
                    "dx": [-2.0, 2.0],
                    "dy": [-2.0, 2.0],
                    "RangeToPrimary": [2.0, 5.0],
                }

            def _clean_array(vals):
                arr = np.asarray(vals, dtype=float)
                return arr[np.isfinite(arr)]

            df = self.read_rec_db_primary_all()

            if df is None or df.empty:
                return self._error_layout(
                    title="REC_DB vs DSR PRIMARY Histogram",
                    message="No REC_DB rows joined to DSR Primary coordinates.",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            d = df.copy()

            for c in ["Line", "Station", "REC_X", "REC_Y", "PrimaryEasting", "PrimaryNorthing"]:
                d[c] = pd.to_numeric(d[c], errors="coerce")

            d = d.dropna(
                subset=[
                    "Line",
                    "Station",
                    "REC_X",
                    "REC_Y",
                    "PrimaryEasting",
                    "PrimaryNorthing",
                ]
            )

            if d.empty:
                return self._error_layout(
                    title="REC_DB vs DSR PRIMARY Histogram",
                    message="No valid REC_DB / DSR Primary coordinates after numeric cleanup.",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            d["ROV"] = d["ROV"].astype(str).fillna("").str.strip()
            d = d[d["ROV"] != ""]

            d["dx"] = d["REC_X"] - d["PrimaryEasting"]
            d["dy"] = d["REC_Y"] - d["PrimaryNorthing"]
            d["RangeToPrimary"] = np.sqrt(d["dx"] ** 2 + d["dy"] ** 2)

            plots = [
                ("dx", "dX = REC_X - PrimaryEasting", "dX, m"),
                ("dy", "dY = REC_Y - PrimaryNorthing", "dY, m"),
                ("RangeToPrimary", "Range To DSR Primary", "Range, m"),
            ]

            def _metric_limits(field, vals):
                vals = _clean_array(vals)

                if vals.size == 0:
                    return -0.5, 0.5

                vmin = float(np.min(vals))
                vmax = float(np.max(vals))

                if field == "RangeToPrimary":
                    vmin = 0.0

                if max_offset is not None:
                    mo = abs(float(max_offset))
                    if field == "RangeToPrimary":
                        vmin, vmax = 0.0, mo
                    else:
                        vmin, vmax = -mo, mo

                if vmin == vmax:
                    vmin -= 0.5
                    vmax += 0.5

                pad = (vmax - vmin) * 0.05

                if field == "RangeToPrimary":
                    return 0.0, vmax + pad

                return vmin - pad, vmax + pad

            x_ranges = {}
            for field, title, label in plots:
                vmin, vmax = _metric_limits(field, d[field])
                x_ranges[field] = Range1d(start=vmin, end=vmax)

            def _stats_dict(vals):
                s = _clean_array(vals)

                if s.size == 0:
                    return {
                        "n": 0,
                        "min": np.nan,
                        "max": np.nan,
                        "mean": np.nan,
                        "std": np.nan,
                        "p95": np.nan,
                        "p99": np.nan,
                    }

                std = float(np.std(s, ddof=1)) if s.size > 1 else 0.0

                return {
                    "n": int(s.size),
                    "min": float(np.min(s)),
                    "max": float(np.max(s)),
                    "mean": float(np.mean(s)),
                    "std": std,
                    "p95": float(np.quantile(s, 0.95)),
                    "p99": float(np.quantile(s, 0.99)),
                }

            def _stats_text(vals):
                st = _stats_dict(vals)
                if st["n"] == 0:
                    return "no data"

                return (
                    f"avg:{st['mean']:.2f}; std:{st['std']:.2f}; "
                    f"p95:{st['p95']:.2f}; p99:{st['p99']:.2f}"
                )

            def _outlier_count(vals, field):
                s = _clean_array(vals)

                if s.size == 0:
                    return 0

                limits = qc_limits.get(field)
                if not limits:
                    return 0

                if field == "RangeToPrimary":
                    limit = max(abs(float(x)) for x in limits)
                    return int(np.sum(s > limit))

                low = min(float(x) for x in limits)
                high = max(float(x) for x in limits)

                return int(np.sum((s < low) | (s > high)))

            def _kde_xy(vals, xmin, xmax, hist_bin_width):
                vals = _clean_array(vals)

                if vals.size < 2:
                    return None, None

                if vals.size > kde_max_samples:
                    vals = np.random.choice(
                        vals,
                        size=int(kde_max_samples),
                        replace=False,
                    )

                std = float(np.std(vals, ddof=1))

                if not np.isfinite(std) or std <= 0:
                    return None, None

                n = vals.size
                bandwidth = 1.06 * std * (n ** (-1 / 5))

                if not np.isfinite(bandwidth) or bandwidth <= 0:
                    return None, None

                xs = np.linspace(xmin, xmax, int(kde_points))
                density = np.zeros_like(xs, dtype=float)

                chunk = 1000
                for i in range(0, n, chunk):
                    v = vals[i:i + chunk]
                    z = (xs[:, None] - v[None, :]) / bandwidth
                    density += np.exp(-0.5 * z * z).sum(axis=1)

                density /= n * bandwidth * np.sqrt(2 * np.pi)
                ys = density * n * hist_bin_width

                return xs, ys

            def _make_hist(rov_df, rov_name, field, plot_title, x_label):
                vals_all = _clean_array(rov_df[field])

                x_range = x_ranges[field]
                vmin = float(x_range.start)
                vmax = float(x_range.end)

                edges = np.linspace(vmin, vmax, int(bins) + 1)
                hist, e = np.histogram(vals_all, bins=edges)

                bin_width = float(e[1] - e[0]) if len(e) > 1 else 1.0

                src = ColumnDataSource(
                    data=dict(
                        top=hist,
                        left=e[:-1],
                        right=e[1:],
                        bin_center=((e[:-1] + e[1:]) / 2.0),
                        rov=[rov_name] * len(hist),
                        percent=[
                            (float(v) / max(len(vals_all), 1)) * 100.0
                            for v in hist
                        ],
                    )
                )

                st = _stats_dict(vals_all)
                outliers = _outlier_count(vals_all, field)

                p = figure(
                    title=f"{plot_title} | {_stats_text(vals_all)} | outliers:{outliers}",
                    x_axis_label=x_label,
                    y_axis_label="Count",
                    x_range=x_range,
                    sizing_mode="stretch_width",
                    min_height=360,
                    tools="pan,wheel_zoom,box_zoom,reset,save",
                    active_scroll="wheel_zoom",
                )

                r = p.quad(
                    top="top",
                    bottom=0,
                    left="left",
                    right="right",
                    source=src,
                    fill_alpha=0.55,
                    line_alpha=0.9,
                    legend_label=str(rov_name),
                )

                p.add_tools(
                    HoverTool(
                        renderers=[r],
                        tooltips=[
                            ("ROV", "@rov"),
                            ("Bin center", "@bin_center{0.00}"),
                            ("Count", "@top"),
                            ("Percent", "@percent{0.00}%"),
                        ],
                    )
                )

                if field != "RangeToPrimary":
                    p.add_layout(
                        Span(
                            location=0,
                            dimension="height",
                            line_color="black",
                            line_dash="dashed",
                            line_width=1,
                            line_alpha=0.7,
                        )
                    )

                limits = qc_limits.get(field)
                if limits:
                    for lim in limits:
                        lim = float(lim)
                        if field == "RangeToPrimary" and lim < 0:
                            continue
                        p.add_layout(
                            Span(
                                location=lim,
                                dimension="height",
                                line_color="red",
                                line_dash="dotdash",
                                line_width=2,
                                line_alpha=0.75,
                            )
                        )

                if show_std_lines and st["n"] > 1 and np.isfinite(st["std"]):
                    mean = st["mean"]
                    std = st["std"]

                    p.add_layout(
                        Span(
                            location=mean,
                            dimension="height",
                            line_color="blue",
                            line_dash="solid",
                            line_width=2,
                            line_alpha=0.85,
                        )
                    )

                    for mult, dash, alpha in [
                        (1, "dashed", 0.75),
                        (2, "dotted", 0.65),
                    ]:
                        for x in [mean - mult * std, mean + mult * std]:
                            if field == "RangeToPrimary" and x < 0:
                                continue
                            p.add_layout(
                                Span(
                                    location=float(x),
                                    dimension="height",
                                    line_color="blue",
                                    line_dash=dash,
                                    line_width=1,
                                    line_alpha=alpha,
                                )
                            )

                if show_kde and vals_all.size >= 2:
                    xs, ys = _kde_xy(vals_all, vmin, vmax, bin_width)

                    if xs is not None and ys is not None:
                        p.line(
                            xs,
                            ys,
                            line_width=3,
                            line_alpha=0.9,
                            legend_label="KDE",
                        )

                max_hist = int(np.max(hist)) if len(hist) else 0
                label_y = max_hist * 0.92 if max_hist > 0 else 1

                p.add_layout(
                    Label(
                        x=vmin + (vmax - vmin) * 0.02,
                        y=label_y,
                        text=f"P95={st['p95']:.2f} | P99={st['p99']:.2f} | Out={outliers}",
                        text_font_size="9pt",
                        text_alpha=0.8,
                    )
                )

                p.legend.location = "top_right"
                p.legend.label_text_font_size = "8pt"
                p.legend.spacing = 1
                p.legend.padding = 4
                p.legend.click_policy = "hide"

                p.xgrid.visible = True
                p.ygrid.visible = True

                return p

            rovs = sorted(d["ROV"].dropna().unique().tolist())

            def _project_metric_summary(field):
                st = _stats_dict(d[field])
                out = _outlier_count(d[field], field)

                if st["n"] == 0:
                    return f"{field}: no data"

                return (
                    f"{field}: avg {st['mean']:.2f}, std {st['std']:.2f}, "
                    f"P95 {st['p95']:.2f}, P99 {st['p99']:.2f}, outliers {out}"
                )

            header = Div(
                text=f"""
                <div style="padding:8px 10px;border-left:4px solid #0d6efd;background:#f8fafc;">
                    <b>{title_prefix}</b><br>
                    Scope: <b>whole project database</b> |
                    ROVs: <b>{", ".join(rovs)}</b> |
                    Lines: <b>{d["Line"].nunique()}</b> |
                    Rows: <b>{len(d)}</b> |
                    X-axis: <b>synced by metric across ROVs</b><br>
                    <span>{_project_metric_summary("dx")}</span><br>
                    <span>{_project_metric_summary("dy")}</span><br>
                    <span>{_project_metric_summary("RangeToPrimary")}</span>
                </div>
                """,
                sizing_mode="stretch_width",
            )

            sections = [header]

            for rov_name in rovs:
                rov_df = d[d["ROV"] == rov_name].copy()

                dx_out = _outlier_count(rov_df["dx"], "dx")
                dy_out = _outlier_count(rov_df["dy"], "dy")
                range_out = _outlier_count(rov_df["RangeToPrimary"], "RangeToPrimary")

                rov_header = Div(
                    text=f"""
                    <div style="margin-top:14px;padding:7px 10px;
                                border-left:4px solid #198754;background:#f3f8f5;">
                        <b>ROV: {rov_name}</b> |
                        Rows: <b>{len(rov_df)}</b> |
                        Lines: <b>{rov_df["Line"].nunique()}</b> |
                        Stations: <b>{rov_df["Station"].nunique()}</b> |
                        Outliers dX/dY/Range:
                        <b>{dx_out}</b> / <b>{dy_out}</b> / <b>{range_out}</b>
                    </div>
                    """,
                    sizing_mode="stretch_width",
                )

                figures = [
                    _make_hist(rov_df, rov_name, field, title, label)
                    for field, title, label in plots
                ]

                sections.append(rov_header)
                sections.append(
                    gridplot(
                        [[figures[0], figures[1], figures[2]]],
                        sizing_mode="stretch_width",
                        merge_tools=True,
                        toolbar_location="above",
                    )
                )

            layout = column(*sections, sizing_mode="stretch_width")

            if export_html_path:
                export_html_path = os.path.abspath(export_html_path)
                os.makedirs(os.path.dirname(export_html_path), exist_ok=True)

                resources = INLINE
                if str(export_resources).lower() == "cdn":
                    resources = CDN

                html = file_html(layout, resources, title=title_prefix)

                with open(export_html_path, "w", encoding="utf-8") as f:
                    f.write(html)

            if is_show:
                show(layout)
                return export_html_path if export_html_path else None

            if json_return:
                return json_item(layout)

            if export_html_path:
                return export_html_path

            return layout

        except Exception as e:
            return self._error_layout(
                title="REC_DB vs DSR PRIMARY Histogram",
                message="Failed to build REC_DB vs DSR Primary histogram plot.",
                details=str(e),
                level="error",
                is_show=is_show,
                json_return=json_return,
            )






