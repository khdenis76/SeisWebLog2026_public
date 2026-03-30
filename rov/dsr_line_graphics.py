import math
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
from bokeh.embed import json_item
from bokeh.layouts import column, gridplot, row
from bokeh.models import ColumnDataSource, HoverTool, Range1d, Button, CustomJS, FactorRange, LegendItem, Legend, Div, \
    Span, TextInput, LinearColorMapper, NumeralTickFormatter, PrintfTickFormatter, Rect, MultiSelect, Ray
from bokeh.palettes import Category10, Turbo256, Category20, Paired
from bokeh.plotting import figure, show
import numpy as np
from bokeh.models import ColumnDataSource, ColorBar
from bokeh.transform import linear_cmap, factor_cmap, dodge
from bokeh.palettes import Viridis256
from django.shortcuts import render


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
                json_item=False,
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
            out = json_item(p) if json_item else p
            if is_show and not json_item:
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
            out = _json_item(p) if json_item else p
            if is_show and not json_item:
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

        if is_show and not json_item:
            show(layout)

        return _json_item(layout) if json_item else layout

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
            deploy_time_col="TimeStamp",  # shown as "Deploy Time"
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
            red_radius=None,  # fixed, legend-controlled
            red_is_show=True,
            orange_radius=None,  # dynamic via JS input
            orange_is_show=True,
            # orange radius input
            orange_input_is_show=True,
            # hist
            bins=40,
            hist_show_std=True,
            hist_show_kde=True,
            kde_points=200,
            kde_bandwidth=None,  # None -> Silverman
            padding_ratio=0.10,
            # output
            is_show=False,
            json_return=False,
            target_id="dxdy_plot",
    ):
        try:
            # -------------------- basic validation --------------------
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

            # optional hover columns (if missing -> empty strings)
            hover_cols = [
                line_col, station_col, rov_col, deploy_time_col, range_to_preplot_col,
                sma_col, p_sma_e95_col, p_sma_n95_col, s_sma_e95_col, s_sma_n95_col,
            ]
            hover_present = {c: (c in df.columns) for c in hover_cols}

            # -------------------- helpers --------------------
            def _safe_str(v):
                if v is None:
                    return ""
                try:
                    return str(v)
                except Exception:
                    return ""

            def _safe_float(v):
                try:
                    if v is None:
                        return None
                    vv = float(v)
                    if vv != vv:  # NaN
                        return None
                    return vv
                except Exception:
                    return None

            def _mean_std(vals):
                n = len(vals)
                if n == 0:
                    return (0.0, 0.0)
                m = sum(vals) / float(n)
                if n < 2:
                    return (m, 0.0)
                s2 = 0.0
                for v in vals:
                    d = v - m
                    s2 += d * d
                s2 /= float(n - 1)
                return (m, s2 ** 0.5)

            def _hist(vals, bins_count):
                vals2 = []
                for v in vals:
                    fv = _safe_float(v)
                    if fv is not None:
                        vals2.append(fv)
                if not vals2:
                    return {"left": [], "right": [], "top": []}

                vmin = min(vals2)
                vmax = max(vals2)
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
                c = 0.3989422804014327  # 1/sqrt(2*pi)
                e = 2.718281828459045
                out = []
                for x in x_grid:
                    s = 0.0
                    for v in vals:
                        z = (x - v) / bw
                        s += c * (e ** (-0.5 * z * z))
                    out.append(inv * s)
                return out

            def _kde_scaled_to_counts(vals, bins_count, grid_n, bw_override=None):
                if not vals:
                    return ([], [])
                vmin = min(vals)
                vmax = max(vals)
                if vmin == vmax:
                    vmin -= 0.5
                    vmax += 0.5

                if grid_n < 50:
                    grid_n = 50
                step = (vmax - vmin) / float(grid_n - 1)
                x_grid = [vmin + i * step for i in range(grid_n)]

                # bandwidth
                bw = None
                if bw_override is not None:
                    try:
                        bw = float(bw_override)
                    except Exception:
                        bw = None

                m, sd = _mean_std(vals)
                n = len(vals)
                if bw is None:
                    if sd <= 0 or n < 2:
                        bw = (vmax - vmin) / 20.0 if (vmax - vmin) > 0 else 1.0
                    else:
                        bw = 1.06 * sd * (n ** (-0.2))

                dens = _kde_gaussian(vals, x_grid, bw)

                bin_width = (vmax - vmin) / float(bins_count) if bins_count > 0 else 1.0
                y_scaled = [d * n * bin_width for d in dens]
                return (x_grid, y_scaled)

            # -------------------- build sources with hover fields --------------------
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

            src_p = _build_source(dpf, dx_p_col, dy_p_col, "Primary")
            src_s = _build_source(dsf, dx_s_col, dy_s_col, "Secondary")

            if src_p is None and src_s is None:
                return self._error_layout(
                    title="DX/DY Plot Error",
                    message="No numeric points could be parsed.",
                    level="warning",
                    is_show=is_show,
                    json_return=json_return,
                )

            # -------------------- symmetric limits (0,0 centered) --------------------
            def _max_abs_from_src(src):
                if src is None:
                    return 0.0
                xs = src.data.get("x", [])
                ys = src.data.get("y", [])
                m = 0.0
                if xs:
                    m = max(m, max(abs(min(xs)), abs(max(xs))))
                if ys:
                    m = max(m, max(abs(min(ys)), abs(max(ys))))
                return m

            max_abs = max(1.0, _max_abs_from_src(src_p), _max_abs_from_src(src_s))

            if red_radius is not None:
                try:
                    max_abs = max(max_abs, abs(float(red_radius)))
                except Exception:
                    pass
            if orange_radius is not None:
                try:
                    max_abs = max(max_abs, abs(float(orange_radius)))
                except Exception:
                    pass

            lim = max_abs * (1.0 + float(padding_ratio))

            # -------------------- hover tool --------------------
            hover = HoverTool(tooltips=[
                ("Series", "@kind"),
                ("Line", "@Line"),
                ("Station", "@Station"),
                ("ROV", "@ROV"),
                ("Deploy Time", "@DeployTime"),
                ("RangeToPreplot", "@RangeToPrePlot"),
                ("Primary SMA95 (e95,n95)", "(@Primary_e95, @Primary_n95)"),
                ("Secondary SMA95 (e95,n95)", "(@Secondary_e95, @Secondary_n95)"),
                ("dX", "@x{0.00}"),
                ("dY", "@y{0.00}"),
            ], )

            # -------------------- main plot --------------------
            p = figure(
                title=title,
                x_axis_label="dX (m)",
                y_axis_label="dY (m)",
                x_range=(-lim, lim),
                y_range=(-lim, lim),
                match_aspect=True,
                tools=[hover, "pan", "wheel_zoom", "box_zoom", "reset", "save"],
                active_scroll="wheel_zoom",
                sizing_mode="stretch_both",
            )
            p.add_layout(Span(location=0, dimension="width", line_width=2))
            p.add_layout(Span(location=0, dimension="height", line_width=2))

            if src_p is not None:
                r_primary =p.scatter("x", "y", source=src_p, size=6, legend_label=p_name, color="red",alpha=0.9)
            if src_s is not None:
                r_secondary =p.scatter("x", "y", source=src_s, size=6, legend_label=s_name, color="green", alpha=0.9, marker="triangle")
            hover.renderers = [r_primary, r_secondary]
            # -------------------- circles --------------------
            # RED: fixed from function call + legend-controlled (click to hide/show)
            red_src = ColumnDataSource(data={
                "x": [0.0],
                "y": [0.0],
                "radius": [float(red_radius) if red_radius is not None else 0.0],
            })
            red_renderer = p.circle(
                x="x", y="y", radius="radius", source=red_src,
                radius_units="data",
                fill_alpha=0.0,
                line_color="red",
                line_width=2,
                legend_label=(f"Tolerance ({float(red_radius):g} m)" if red_radius is not None else "Tolerance"),
                visible=bool(red_is_show and red_radius is not None),
            )

            # ORANGE: dynamic via JS input (not in legend)
            orange_src = ColumnDataSource(data={
                "x": [0.0],
                "y": [0.0],
                "radius": [float(orange_radius) if orange_radius is not None else 0.0],
            })
            orange_renderer = p.circle(
                x="x", y="y", radius="radius", source=orange_src,
                radius_units="data",
                fill_alpha=0.0,
                line_color="orange",
                line_width=2,
                visible=bool(orange_is_show and orange_radius is not None),
            )

            # legend click hides/shows primary/secondary + red circle
            p.legend.click_policy = "hide"

            # -------------------- histograms + stats + KDE --------------------
            px_all = (src_p.data["x"] if src_p is not None else []) + (src_s.data["x"] if src_s is not None else [])
            py_all = (src_p.data["y"] if src_p is not None else []) + (src_s.data["y"] if src_s is not None else [])

            hx_h = _hist(px_all, bins)
            hy_h = _hist(py_all, bins)

            hx_src = ColumnDataSource({"left": hx_h["left"], "right": hx_h["right"], "top": hx_h["top"]})
            hy_src = ColumnDataSource({"left": hy_h["left"], "right": hy_h["right"], "top": hy_h["top"]})

            mx, sxv = _mean_std(px_all)
            my, syv = _mean_std(py_all)

            kx_x, kx_y = ([], [])
            ky_ygrid, ky_xscaled = ([], [])
            if hist_show_kde:
                kx_x, kx_y = _kde_scaled_to_counts(px_all, bins, kde_points, kde_bandwidth)
                ky_ygrid, ky_xscaled = _kde_scaled_to_counts(py_all, bins, kde_points, kde_bandwidth)

            # top hist (dX)
            hx = figure(
                title=f"dX (mean={mx:.2f}, std={sxv:.2f})",
                x_range=p.x_range,
                height=180,
                tools="pan,wheel_zoom,box_zoom,reset,save",
                sizing_mode="stretch_width",
            )
            hx.quad(left="left", right="right", bottom=0, top="top", source=hx_src, alpha=0.9)
            hx.xaxis.visible = False

            if hist_show_std and px_all:
                hx.add_layout(Span(location=mx, dimension="height", line_width=2,line_dash="dotted"))
                hx.add_layout(Span(location=mx - sxv, dimension="height", line_width=1,line_dash="dotted"))
                hx.add_layout(Span(location=mx + sxv, dimension="height", line_width=1,line_dash="dotted"))

            if hist_show_kde and kx_x:
                hx.line(kx_x, kx_y, line_width=2, legend_label="KDE",color="pink")
                hx.legend.click_policy = "hide"

            # right hist (dY)
            hy = figure(
                title=f"dY (mean={my:.2f}, std={syv:.2f})",
                y_range=p.y_range,
                width=220,
                tools="pan,wheel_zoom,box_zoom,reset,save",
                sizing_mode="stretch_height",
            )
            hy.quad(bottom="left", top="right", left=0, right="top", source=hy_src, alpha=0.9)
            hy.yaxis.visible = False

            if hist_show_std and py_all:
                hy.add_layout(Span(location=my, dimension="width", line_width=2,line_dash="dotted"))
                hy.add_layout(Span(location=my - syv, dimension="width", line_width=1,line_dash="dotted"))
                hy.add_layout(Span(location=my + syv, dimension="width", line_width=1,line_dash="dotted"))

            if hist_show_kde and ky_ygrid:
                hy.line(ky_xscaled, ky_ygrid, line_width=2, legend_label="KDE",color="orange")
                hy.legend.click_policy = "hide"

            # -------------------- controls --------------------
            btn_toggle = Button(label="Toggle legend & histograms", button_type="primary")
            btn_toggle.js_on_click(CustomJS(
                args=dict(p=p, hx=hx, hy=hy),
                code="""
                    if (p.legend && p.legend.length > 0) {
                        const L = p.legend[0];
                        L.visible = !L.visible;
                    }
                    hx.visible = !hx.visible;
                    hy.visible = !hy.visible;
                    p.change.emit(); hx.change.emit(); hy.change.emit();
                """
            ))

            controls = [btn_toggle]

            if orange_input_is_show:
                orange_input = TextInput(
                    title="Orange Radius (m)",
                    value=str(orange_radius if orange_radius is not None else ""),
                )
                orange_input.js_on_change("value", CustomJS(
                    args=dict(src=orange_src, rend=orange_renderer),
                    code="""
                        const s = (cb_obj.value || "").trim().replace(",", ".");
                        const v = parseFloat(s);

                        if (!isFinite(v) || v <= 0) {
                            src.data['radius'][0] = 0.0;
                            rend.visible = false;
                        } else {
                            src.data['radius'][0] = v;
                            rend.visible = true;
                        }
                        src.change.emit();
                    """
                ))
                controls.append(orange_input)

            controls_row = row(*controls, sizing_mode="stretch_width")

            # -------------------- layout --------------------
            top = column(controls_row, hx, sizing_mode="stretch_width")
            main_row = row(p, hy, sizing_mode="stretch_both")
            layout = column(top, main_row, sizing_mode="stretch_both")

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
        )



        # -------------------------------------------------
        # PREPLOT
        # -------------------------------------------------
        if len(df) > 0 and {"PreplotEasting", "PreplotNorthing"}.issubset(df.columns):
            ppdata = df.loc[
                pd.to_numeric(df["PreplotEasting"], errors="coerce").notna()
                & pd.to_numeric(df["PreplotNorthing"], errors="coerce").notna()
                ].copy()

            if len(ppdata) > 0:
                ppdata["PreplotEasting"] = pd.to_numeric(ppdata["PreplotEasting"], errors="coerce")
                ppdata["PreplotNorthing"] = pd.to_numeric(ppdata["PreplotNorthing"], errors="coerce")
                ppdata["PointLabel"] = ppdata["Station"].astype(str) if "Station" in ppdata.columns else ""

                pp_source = ColumnDataSource(ppdata)

                line_map.scatter(
                    x="PreplotEasting",
                    y="PreplotNorthing",
                    source=pp_source,
                    color="grey",
                    size=point_size,
                    legend_label="Preplot Stations",
                )

                line_map.circle(
                    x="PreplotEasting",
                    y="PreplotNorthing",
                    source=pp_source,
                    fill_color=None,
                    line_color="green",
                    radius=5,
                    radius_units="data",
                    legend_label="5m Radius",
                )

                line_map.text(
                    x="PreplotEasting",
                    y="PreplotNorthing",
                    text="PointLabel",
                    source=pp_source,
                )

                x_mean = ppdata["PreplotEasting"].mean()
                y_mean = ppdata["PreplotNorthing"].mean()
                if pd.notna(x_mean) and pd.notna(y_mean):
                    line_map.x_range = Range1d(x_mean - 10000, x_mean + 10000)
                    line_map.y_range = Range1d(y_mean - 10000, y_mean + 10000)

        # -------------------------------------------------
        # BLACKBOX
        # -------------------------------------------------
        if len(bbdata) > 0:
            bb_numeric_cols = [
                "VesselEasting", "VesselNorthing", "VesselHDG",
                "ROV1_INS_Easting", "ROV1_INS_Northing",
                "ROV1_USBL_Easting", "ROV1_USBL_Northing",
                "ROV2_INS_Easting", "ROV2_INS_Northing",
                "ROV2_USBL_Easting", "ROV2_USBL_Northing",
            ]
            for c in bb_numeric_cols:
                if c in bbdata.columns:
                    bbdata[c] = pd.to_numeric(bbdata[c], errors="coerce")

            if {"VesselEasting", "VesselNorthing", "VesselHDG"}.issubset(bbdata.columns):
                bb_track = bbdata.loc[
                    bbdata["VesselEasting"].notna() & bbdata["VesselNorthing"].notna()
                    ].copy()

                if len(bb_track) > 0:
                    bb_source = ColumnDataSource(bb_track)

                    vessel_line = line_map.line(
                        x="VesselEasting",
                        y="VesselNorthing",
                        source=bb_source,
                        line_width=1,
                        color=colors[0],
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

                    rect_renderer = line_map.add_glyph(bb_source, vessel_rect)
                    ray_renderer = line_map.add_glyph(bb_source, vessel_ray)

                    if len(line_map.legend) == 0:
                        legend = Legend(items=[
                            LegendItem(label=f"{vessel_name} Track",
                                       renderers=[vessel_line, rect_renderer, ray_renderer])
                        ])
                        line_map.add_layout(legend, "right")
                    else:
                        line_map.legend[0].items.append(
                            LegendItem(label=f"{vessel_name} Track",
                                       renderers=[vessel_line, rect_renderer, ray_renderer])
                        )

            if {"ROV1_INS_Easting", "ROV1_INS_Northing"}.issubset(bbdata.columns):
                bb1_primary = bbdata.loc[
                    (bbdata["ROV1_INS_Easting"] > 0) & (bbdata["ROV1_INS_Northing"] > 0)
                    ].copy()
                if len(bb1_primary) > 0:
                    src = ColumnDataSource(bb1_primary)
                    line_map.scatter(
                        x="ROV1_INS_Easting",
                        y="ROV1_INS_Northing",
                        marker="triangle",
                        size=rov_size,
                        color=colors[2],
                        legend_label=f"{rov1_name} (Primary)",
                        source=src,
                    )
                    line_map.line(
                        x="ROV1_INS_Easting",
                        y="ROV1_INS_Northing",
                        width=1,
                        color=colors[2],
                        legend_label=f"{rov1_name} (Primary)",
                        source=src,
                    )

            if {"ROV1_USBL_Easting", "ROV1_USBL_Northing"}.issubset(bbdata.columns):
                bb1_secondary = bbdata.loc[
                    (bbdata["ROV1_USBL_Easting"] > 0) & (bbdata["ROV1_USBL_Northing"] > 0)
                    ].copy()
                if len(bb1_secondary) > 0:
                    src = ColumnDataSource(bb1_secondary)
                    line_map.scatter(
                        x="ROV1_USBL_Easting",
                        y="ROV1_USBL_Northing",
                        marker="circle",
                        size=rov_size,
                        color=colors[4],
                        legend_label=f"{rov1_name} (Secondary)",
                        source=src,
                    )
                    line_map.line(
                        x="ROV1_USBL_Easting",
                        y="ROV1_USBL_Northing",
                        width=1,
                        color=colors[4],
                        legend_label=f"{rov1_name} (Secondary)",
                        source=src,
                    )

            if {"ROV2_INS_Easting", "ROV2_INS_Northing"}.issubset(bbdata.columns):
                bb2_primary = bbdata.loc[
                    (bbdata["ROV2_INS_Easting"] > 0) & (bbdata["ROV2_INS_Northing"] > 0)
                    ].copy()
                if len(bb2_primary) > 0:
                    src = ColumnDataSource(bb2_primary)
                    line_map.scatter(
                        x="ROV2_INS_Easting",
                        y="ROV2_INS_Northing",
                        marker="circle",
                        size=rov_size,
                        color=colors[3],
                        legend_label=f"{rov2_name} (Primary)",
                        source=src,
                    )
                    line_map.line(
                        x="ROV2_INS_Easting",
                        y="ROV2_INS_Northing",
                        width=1,
                        color=colors[3],
                        legend_label=f"{rov2_name} (Primary)",
                        source=src,
                    )

            if {"ROV2_USBL_Easting", "ROV2_USBL_Northing"}.issubset(bbdata.columns):
                bb2_secondary = bbdata.loc[
                    (bbdata["ROV2_USBL_Easting"] > 0) & (bbdata["ROV2_USBL_Northing"] > 0)
                    ].copy()
                if len(bb2_secondary) > 0:
                    src = ColumnDataSource(bb2_secondary)
                    line_map.scatter(
                        x="ROV2_USBL_Easting",
                        y="ROV2_USBL_Northing",
                        marker="circle",
                        size=rov_size,
                        color=colors[5],
                        legend_label=f"{rov2_name} (Secondary)",
                        source=src,
                    )
                    line_map.line(
                        x="ROV2_USBL_Easting",
                        y="ROV2_USBL_Northing",
                        width=1,
                        color=colors[5],
                        legend_label=f"{rov2_name} (Secondary)",
                        source=src,
                    )

        # -------------------------------------------------
        # DSR
        # -------------------------------------------------
        if len(df) > 0:
            if "Station" not in df.columns:
                df["Station"] = ""
            if "ROV" not in df.columns:
                df["ROV"] = ""
            if "TimeStamp" not in df.columns:
                df["TimeStamp"] = ""
            if "Comments" not in df.columns:
                df["Comments"] = ""

            dsr_source = ColumnDataSource(df)

            if {"PrimaryEasting", "PrimaryNorthing"}.issubset(df.columns):
                dsr_dep_points = line_map.scatter(
                    x="PrimaryEasting",
                    y="PrimaryNorthing",
                    marker="square",
                    size=point_size,
                    fill_color=colors[-1],
                    color=colors[-1],
                    legend_label="DSR Deployment",
                    source=dsr_source,
                )

                line_map.line(
                    x="PrimaryEasting",
                    y="PrimaryNorthing",
                    width=1,
                    color=colors[-1],
                    legend_label="DSR Deployment",
                    source=dsr_source,
                    line_dash="dashed",
                )

                line_map.text(
                    x="PrimaryEasting",
                    y="PrimaryNorthing",
                    text="Station",
                    source=dsr_source,
                    color=colors[-2],
                    text_font_size="18pt",
                    text_align="right",
                )

                line_map.text(
                    x="PrimaryEasting",
                    y="PrimaryNorthing",
                    text="Comments",
                    source=dsr_source,
                    color="red",
                    text_font_size="10pt",
                    text_align="left",
                )

                TOOLTIPS = [
                    ("Station", "@Station"),
                    ("Node", "@Node"),
                    ("WD Prim.", "@PrimaryElevation{3.1}"),
                    ("WD Sec.", "@SecondaryElevation{3.1}"),
                    ("ROV", "@ROV"),
                    ("Range Prim. 2 Sec.", "@Rangeprimarytosecondary{2.1}"),
                    ("Range 2 Prep.", "@RangetoPrePlot{2.1}"),
                    ("Dep date", "@TimeStamp"),
                ]
                line_map.add_tools(HoverTool(tooltips=TOOLTIPS, renderers=[dsr_dep_points]))

        # -------------------------------------------------
        # MULTISELECT
        # -------------------------------------------------
        options = []
        lock_code = []

        if len(df) > 0 and {"Station", "PrimaryEasting", "PrimaryNorthing"}.issubset(df.columns):
            for p in df.itertuples():
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

        button = Button(label="Hide Legend", button_type="success")
        if len(line_map.legend) > 0:
            callback = CustomJS(
                args=dict(plot=line_map, button=button, legend=line_map.legend[0]),
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
            button.js_on_click(callback)

        line_map.yaxis.formatter = PrintfTickFormatter(format="%d")
        line_map.xaxis.formatter = PrintfTickFormatter(format="%d")

        if len(line_map.legend) > 0:
            line_map.legend.click_policy = "hide"

        controls = column([button, multiselect], sizing_mode="stretch_height")
        layout = row([line_map, controls], sizing_mode="stretch_both")

        if isShow:
            #html = file_html(layout, CDN)
            #with open(f"{line}_view.html", "w", encoding="utf-8") as f:
            #   f.write(html)
            show(layout)
            return
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







