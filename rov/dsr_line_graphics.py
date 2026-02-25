import math
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
from bokeh.embed import json_item
from bokeh.layouts import column, gridplot, row
from bokeh.models import ColumnDataSource, HoverTool, Range1d, Button, CustomJS, FactorRange, LegendItem, Legend, Div
from bokeh.palettes import Category10, Turbo256, Category20
from bokeh.plotting import figure, show
import numpy as np
from bokeh.models import ColumnDataSource, ColorBar
from bokeh.transform import linear_cmap, factor_cmap
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
            title: str,
            message: str,
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
            y_axis_label="Water depth",
            category_col="ROV",
            line_col="Line",
            point_col=None,
            rov_col="ROV",
            ts_col="TimeStamp",
            require_category=True,
            reverse_y_if_negative=True,
            legend_title="Legend",
            x_tick_step=5,  # show each N label (others blank)
            x_tick_font_size="8pt",
            json_return=False,
            is_show=False,
    ):
        """
        3 stacked vbar plots (shared categorical x):
          - shared X (Stations) via shared FactorRange
          - merged toolbar for all plots (gridplot merge_tools=True)
          - one legend above the stack (attached to top plot 'above')
          - per plot: one numeric series (y_col) with separate renderer per category_col (e.g. ROV)
          - X labels vertical and not dense: only each `x_tick_step` label is shown (others blank)

        Returns:
          layout OR json_item(layout) if json_return=True
        """

        # ---------- unified error return (uses your self._error_layout)
        def _err(msg: str):
            try:
                return self._error_layout(msg, json_return=json_return, is_show=is_show)
            except TypeError:
                # fallback if your _error_layout has different signature
                return self._error_layout(msg)

        # ---------- df checks
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

        # At least one Y must exist
        if (y1_col not in d.columns) and (y2_col not in d.columns) and (y3_col not in d.columns):
            return _err(f"None of y-columns exist in df: '{y1_col}', '{y2_col}', '{y3_col}'.")

        # ---------- category
        if category_col not in d.columns:
            d[category_col] = ""
        else:
            d[category_col] = d[category_col].astype(str).fillna("")

        # ---------- numeric conversion
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")

        for yc in (y1_col, y2_col, y3_col):
            if yc in d.columns:
                d[yc] = pd.to_numeric(d[yc], errors="coerce")

        # ---------- drop invalid x
        d = d.dropna(subset=[x_col])
        if len(d) == 0:
            return _err(f"No valid '{x_col}' values after numeric conversion.")

        # ---------- require category
        if require_category:
            d = d[d[category_col].astype(str).str.strip().ne("")]
            if len(d) == 0:
                return _err(f"No rows with non-empty '{category_col}' after filtering.")

        # ---------- ensure at least some finite y exists
        def _finite_count(col: str) -> int:
            if col not in d.columns:
                return 0
            arr = pd.to_numeric(d[col], errors="coerce").to_numpy(dtype=float)
            return int(np.isfinite(arr).sum())

        if (_finite_count(y1_col) + _finite_count(y2_col) + _finite_count(y3_col)) == 0:
            return _err("No numeric Y values available to plot (all Y columns are empty/NaN).")

        # ---------- sort
        d = d.sort_values(by=[x_col])

        # ---------- station factors (categorical)
        stations = d[x_col].to_numpy(dtype=float)
        station_factors = [str(int(s)) if float(s).is_integer() else str(s) for s in stations]
        d["_station_factor"] = station_factors

        x_factors = d["_station_factor"].drop_duplicates().tolist()
        if not x_factors:
            return _err("No stations available after processing.")

        shared_x = FactorRange(*x_factors)

        # ---------- x label density (categorical-safe)
        try:
            step = int(x_tick_step)
            if step < 1:
                step = 1
        except Exception:
            step = 1

        # Only show each Nth label; others become ""
        x_label_overrides = {
            f: (f if (i % step == 0) else "")
            for i, f in enumerate(x_factors)
        }

        # ---------- y-range helper
        def _make_y_range(dframe, y_col):
            if y_col not in dframe.columns:
                return Range1d(start=-1, end=1)

            arr = pd.to_numeric(dframe[y_col], errors="coerce").to_numpy(dtype=float)
            arr = arr[np.isfinite(arr)]
            if arr.size == 0:
                return Range1d(start=-1, end=1)

            y_min, y_max = float(np.min(arr)), float(np.max(arr))
            pad = (y_max - y_min) * 0.05 if (y_max - y_min) > 0 else (abs(y_min) * 0.05 + 1.0)

            if reverse_y_if_negative and y_min < 0:
                return Range1d(start=y_max + pad, end=y_min - pad)  # reversed
            return Range1d(start=y_min - pad, end=y_max + pad)

        # ---------- palette (repeats)
        base_palette = [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
            "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
        ]

        categories = sorted(d[category_col].unique().tolist())
        if not categories:
            categories = [""]

        # ---------- build one plot
        def _build_one_plot(plot_title, y_col, y_label_text):
            # If missing or empty series -> create a “soft” plot with note (not a hard error)
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
                y_range=_make_y_range(d, y_col),
                x_axis_label="Station",
                y_axis_label=y_axis_label,
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
                sizing_mode="stretch_both",
                min_border_right=35,
            )

            p.xgrid.grid_line_alpha = 0.15
            p.ygrid.grid_line_alpha = 0.15

            # X axis style: vertical + not dense (categorical-safe)
            p.xaxis.major_label_orientation = 1.5708  # 90 degrees
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
                ],
            )
            p.add_tools(hover)

            renderers = []
            legend_items = []

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

            hover.renderers = renderers
            return p, legend_items

        # ---------- build 3 plots
        s1_label = y1_label or y1_col
        s2_label = y2_label or y2_col
        s3_label = y3_label or y3_col

        p1, leg_items1 = _build_one_plot(title1, y1_col, s1_label)
        p2, leg_items2 = _build_one_plot(title2, y2_col, s2_label)
        p3, leg_items3 = _build_one_plot(title3, y3_col, s3_label)

        # only bottom plot shows x-axis labels
        p1.xaxis.visible = False
        p2.xaxis.visible = False

        # ---------- one legend ABOVE all plots
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

        # ---------- merged toolbar for all
        stack = gridplot(
            [[p1], [p2], [p3]],
            merge_tools=True,
            toolbar_location="above",
            sizing_mode="stretch_both",
        )

        # ---------- legend toggle button
        btn_toggle = Button(label="Legend", button_type="default", width=80)
        if L is not None:
            btn_toggle.js_on_click(CustomJS(args=dict(leg=L), code="leg.visible = !leg.visible;"))
        else:
            btn_toggle.disabled = True

        layout = column(row(btn_toggle), stack, sizing_mode="stretch_both")

        if is_show and not json_return:
            show(layout)

        return json_item(layout) if json_return else layout











