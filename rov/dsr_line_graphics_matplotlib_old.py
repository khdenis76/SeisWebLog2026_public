import math
import os
import sqlite3
from io import BytesIO
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
from matplotlib.gridspec import GridSpec
from matplotlib import cm, colors
from matplotlib.lines import Line2D
from matplotlib.gridspec import GridSpec
from matplotlib.patches import Circle, Patch
from matplotlib.ticker import FuncFormatter
from pyproj import Transformer


class DSRLineGraphicsMatplotlib:
    """
    Matplotlib version of DSR plots.
    Each plot function can save image directly to file.

    File name always starts with line number from df["Line"].
    """

    def __init__(self, db_path):
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
    @staticmethod
    def _num(s):
        return pd.to_numeric(s, errors="coerce")

    @staticmethod
    def _stats_text(values, digits=2):
        s = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
        if s.empty:
            return "no data"
        return f"min:{s.min():.{digits}f}; max:{s.max():.{digits}f}; avg:{s.mean():.{digits}f}"

    @staticmethod
    def _bar_width(x):
        xs = pd.to_numeric(pd.Series(x), errors="coerce").dropna().tolist()
        if len(xs) <= 1:
            return 1.0
        diffs = [b - a for a, b in zip(xs, xs[1:]) if (b - a) > 0]
        step = min(diffs) if diffs else 1.0
        return step * 0.8

    @staticmethod
    def _style_ax(ax, xlabel=None, ylabel=None, title=None):
        if title:
            ax.set_title(title)
        if xlabel:
            ax.set_xlabel(xlabel)
        if ylabel:
            ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.30)
        ax.set_axisbelow(True)

    @staticmethod
    def empty_figure(title="No data", message="No data available", figsize=(14, 4)):
        fig, ax = plt.subplots(figsize=figsize)
        ax.text(0.5, 0.5, message, ha="center", va="center", transform=ax.transAxes)
        ax.set_title(title)
        ax.set_xticks([])
        ax.set_yticks([])
        ax.grid(False)
        fig.tight_layout()
        return fig

    @staticmethod
    def figure_to_bytes(fig, fmt="png", dpi=200, close=False):
        bio = BytesIO()
        fig.savefig(bio, format=fmt, dpi=dpi, bbox_inches="tight")
        bio.seek(0)
        data = bio.getvalue()
        if close:
            plt.close(fig)
        return data

    def _get_line_prefix(self, df, fallback="unknown_line"):
        if df is None or df.empty:
            return fallback
        if "Line" not in df.columns:
            return fallback

        line_values = df["Line"].dropna()
        if line_values.empty:
            return fallback

        line_value = line_values.iloc[0]
        try:
            line_value = int(float(line_value))
            return str(line_value)
        except Exception:
            return str(line_value).strip().replace(" ", "_")

    def _build_output_path(self, df, save_dir, suffix, file_name=None, ext="png"):
        save_dir = Path(save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)

        line_prefix = self._get_line_prefix(df)

        ext = str(ext).lower().lstrip(".")
        if file_name:
            final_name = str(file_name)
            if not Path(final_name).suffix:
                final_name = f"{final_name}.{ext}"
        else:
            final_name = f"{line_prefix}_{suffix}.{ext}"

        return save_dir / final_name

    @staticmethod
    def _save_figure(fig, out_path, dpi=200, close=False):
        fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
        if close:
            plt.close(fig)

    # =========================================================
    # water
    # =========================================================
    def two_series_vs_station(
        self,
        df,
        series1_col,
        series2_col,
        series1_label="Series 1",
        series2_label="Series 2",
        rov_col=None,
        x_col="Station",
        is_show=False,
        reverse_y_if_negative=False,
        title=None,
        y_label="Value",
        figsize=(16, 5),
        save_dir=".",
        file_name=None,
        suffix="water",
        ext="png",
        dpi=200,
        close=False,
    ):
        if df is None or df.empty:
            fig = self.empty_figure(title=title or "No data")
            out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        work = df.copy()
        work[x_col] = self._num(work[x_col])
        work[series1_col] = self._num(work[series1_col])
        work[series2_col] = self._num(work[series2_col])
        work = work.sort_values(x_col)

        fig, ax = plt.subplots(figsize=figsize)

        ax.plot(work[x_col], work[series1_col], marker="o", linewidth=1.5, label=series1_label)
        ax.plot(work[x_col], work[series2_col], marker="o", linewidth=1.5, label=series2_label)

        self._style_ax(
            ax,
            xlabel="Station",
            ylabel=y_label,
            title=title or f"{series1_label} / {series2_label}",
        )
        ax.legend()

        if reverse_y_if_negative:
            vals = pd.concat([work[series1_col], work[series2_col]]).dropna()
            if not vals.empty and vals.mean() < 0:
                ax.invert_yaxis()

        fig.tight_layout()

        out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
        self._save_figure(fig, out_path, dpi=dpi, close=False)

        if is_show:
            plt.show()

        if close:
            plt.close(fig)

        return fig, str(out_path)

    # =========================================================
    # primsec / ellipse / noderec
    # =========================================================
    def three_vbar_by_category_shared_x(
        self,
        df,
        rov_col=None,
        ts_col=None,
        x_col="Station",
        is_show=False,
        reverse_y_if_negative=False,
        y1_col=None,
        y2_col=None,
        y3_col=None,
        title1="Plot 1",
        title2="Plot 2",
        title3="Plot 3",
        y1_label="Y1",
        y2_label="Y2",
        y3_label="Y3",
        y_axis_label="Value",
        p1_line1_col=None,
        p1_line2_col=None,
        p2_line1_col=None,
        p2_line2_col=None,
        p3_line1_col=None,
        p3_line2_col=None,
        p1_line1_label="Line 1",
        p1_line2_label="Line 2",
        p2_line1_label="Line 1",
        p2_line2_label="Line 2",
        p3_line1_label="Line 1",
        p3_line2_label="Line 2",
        figsize=(16, 10),
        save_dir=".",
        file_name=None,
        suffix="three_plot",
        ext="png",
        dpi=200,
        close=False,
    ):
        if df is None or df.empty:
            fig = self.empty_figure("No data")
            out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        cols = [x_col, y1_col, y2_col, y3_col]
        for c in (
            p1_line1_col, p1_line2_col,
            p2_line1_col, p2_line2_col,
            p3_line1_col, p3_line2_col,
        ):
            if c:
                cols.append(c)

        work = df.copy()
        for c in cols:
            if c and c in work.columns:
                work[c] = self._num(work[c])

        if ts_col and ts_col in work.columns:
            work[ts_col] = pd.to_datetime(work[ts_col], errors="coerce")

        work = work.sort_values(x_col)
        x = work[x_col]
        width = self._bar_width(x)

        fig, axes = plt.subplots(3, 1, sharex=True, figsize=figsize)

        plot_defs = [
            (axes[0], y1_col, title1, y1_label, p1_line1_col, p1_line2_col, p1_line1_label, p1_line2_label),
            (axes[1], y2_col, title2, y2_label, p2_line1_col, p2_line2_col, p2_line1_label, p2_line2_label),
            (axes[2], y3_col, title3, y3_label, p3_line1_col, p3_line2_col, p3_line1_label, p3_line2_label),
        ]

        for ax, ycol, ttl, bar_lbl, l1, l2, l1_lbl, l2_lbl in plot_defs:
            ax.bar(x, work[ycol], width=width, alpha=0.85, label=bar_lbl)

            if l1 and l1 in work.columns:
                ax.plot(x, work[l1], marker="o", linewidth=1.5, label=l1_lbl)

            if l2 and l2 in work.columns:
                ax.plot(x, work[l2], marker="o", linewidth=1.5, label=l2_lbl)

            self._style_ax(
                ax,
                ylabel=y_axis_label,
                title=f"{ttl} ({self._stats_text(work[ycol])})"
            )
            ax.legend(loc="best")

            if reverse_y_if_negative:
                vals = pd.to_numeric(work[ycol], errors="coerce").dropna()
                if not vals.empty and vals.mean() < 0:
                    ax.invert_yaxis()

        axes[-1].set_xlabel("Station")
        fig.tight_layout()

        out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
        self._save_figure(fig, out_path, dpi=dpi, close=False)

        if is_show:
            plt.show()

        if close:
            plt.close(fig)

        return fig, str(out_path)

    # =========================================================
    # delta
    # =========================================================
    def dxdy_primary_secondary_with_hists(
            self,
            df,
            dx_p_col="dX_primary",
            dy_p_col="dY_primary",
            dx_s_col="dX_secondary",
            dy_s_col="dY_secondary",
            station_col="Station",
            title="DSR dX/dY (Primary & Secondary)",
            red_radius=None,
            red_is_show=True,
            red_radius_mode="max",  # "max" | "fixed"
            p_name="Primary",
            s_name="Secondary",
            bins=30,
            padding_ratio=0.10,
            show_station_labels=True,
            station_fontsize=7,
            max_station_labels=None,
            connect_pairs=True,
            pair_linewidth=0.9,
            pair_alpha=0.7,
            kde_points=200,
            kde_linewidth=1.4,
            std_linewidth=1.2,
            primary_color="#1f77b4",
            secondary_color="#ff7f0e",
            red_circle_color="#d62728",
            point_size=30,
            label_offset=0.06,
            show_pair_heatmap=True,
            show_worst_station=True,
            worst_station_color="#d62728",
            worst_station_marker_size=95,
            show_percentile_circles=True,
            p50_circle_color="#2ca02c",
            p95_circle_color="#9467bd",
            percentile_circle_linewidth=1.6,
            percentile_label_fontsize=9,
            show_colorbar=True,
            show_circle_legend=True,
            colorbar_label="Pair offset, m",
            qc_style=True,
            display_radius_mode="p95",  # "max" | "p95" | "p99" | "fixed"
            display_radius=None,
            display_radius_pad_ratio=0.20,
            max_to_display_ratio_for_clip=1.5,
            show_outside_max_arrow=True,
            is_show=False,
            figsize=(16, 16),
            save_dir=".",
            file_name=None,
            suffix="delta",
            ext="png",
            dpi=200,
            close=False,
    ):
        plt.rcParams.update({
            "font.size": 10,
            "axes.titlesize": 12,
            "axes.labelsize": 10,
            "legend.fontsize": 9,
        })

        if df is None or df.empty:
            fig = self.empty_figure(title=title)
            out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        work = df.copy()

        for c in [dx_p_col, dy_p_col, dx_s_col, dy_s_col]:
            if c in work.columns:
                work[c] = self._num(work[c])
            else:
                work[c] = pd.Series([pd.NA] * len(work), index=work.index)

        if station_col not in work.columns:
            work[station_col] = pd.Series([""] * len(work), index=work.index)

        xp = pd.to_numeric(work[dx_p_col], errors="coerce")
        yp = pd.to_numeric(work[dy_p_col], errors="coerce")
        xs = pd.to_numeric(work[dx_s_col], errors="coerce")
        ys = pd.to_numeric(work[dy_s_col], errors="coerce")
        stations = work[station_col]

        def _clean_series(series):
            return pd.to_numeric(pd.Series(series), errors="coerce").dropna().astype(float)

        def _station_text(value):
            if pd.isna(value):
                return ""
            try:
                fv = float(value)
                if fv.is_integer():
                    return str(int(fv))
            except Exception:
                pass
            return str(value)

        def _safe_radius(value):
            if value is None:
                return None
            try:
                v = float(value)
                if math.isfinite(v) and v > 0:
                    return v
            except Exception:
                pass
            return None

        def _kde_xy(series, points=200):
            s = _clean_series(series)
            n = len(s)
            if n < 2:
                return None, None

            std = float(s.std(ddof=1))
            if not math.isfinite(std) or std <= 0:
                return None, None

            bw = 1.06 * std * (n ** (-1.0 / 5.0))
            if not math.isfinite(bw) or bw <= 0:
                return None, None

            xmin = float(s.min())
            xmax = float(s.max())
            if xmin == xmax:
                xmin -= 1.0
                xmax += 1.0

            pad = max((xmax - xmin) * 0.15, bw * 2.0, 0.5)
            xmin -= pad
            xmax += pad

            grid = np.linspace(xmin, xmax, points)
            vals = s.to_numpy(dtype=float)
            coef = 1.0 / (n * bw * math.sqrt(2.0 * math.pi))
            dens = []

            for gx in grid:
                z = (gx - vals) / bw
                dens.append(float(coef * np.exp(-0.5 * z * z).sum()))

            return grid, np.array(dens, dtype=float)

        def _percentile(values, q):
            s = _clean_series(values)
            if s.empty:
                return None
            return float(s.quantile(q))

        def _add_hist_with_kde_std(
                ax,
                series,
                hist_title,
                value_label,
                color,
                orientation="vertical",
                title_pad=8,
        ):
            s = _clean_series(series)

            if s.empty:
                ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
                ax.set_title(hist_title, pad=title_pad, fontsize=10, weight="bold")
                ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.25)
                ax.set_axisbelow(True)
                return

            mean_v = float(s.mean())
            std_v = float(s.std(ddof=1)) if len(s) > 1 else 0.0

            ax.hist(
                s,
                bins=bins,
                density=True,
                alpha=0.25,
                orientation=orientation,
                color=color,
                edgecolor=color,
                linewidth=0.8,
                label="Histogram",
            )

            kde_x, kde_y = _kde_xy(s, points=kde_points)
            if kde_x is not None and kde_y is not None:
                if orientation == "vertical":
                    ax.plot(kde_x, kde_y, color=color, linewidth=kde_linewidth, label="KDE")
                else:
                    ax.plot(kde_y, kde_x, color=color, linewidth=kde_linewidth, label="KDE")

            if orientation == "vertical":
                ax.axvline(mean_v, color=color, linestyle="-", linewidth=std_linewidth, label="Mean")
                if std_v > 0:
                    ax.axvline(mean_v - std_v, color=color, linestyle="--", linewidth=std_linewidth, label="±1 STD")
                    ax.axvline(mean_v + std_v, color=color, linestyle="--", linewidth=std_linewidth)
                ax.set_ylabel("Density")
            else:
                ax.axhline(mean_v, color=color, linestyle="-", linewidth=std_linewidth, label="Mean")
                if std_v > 0:
                    ax.axhline(mean_v - std_v, color=color, linestyle="--", linewidth=std_linewidth, label="±1 STD")
                    ax.axhline(mean_v + std_v, color=color, linestyle="--", linewidth=std_linewidth)
                ax.set_xlabel("Density")

            ax.set_title(
                f"{hist_title}\nmean={mean_v:.2f}, std={std_v:.2f}",
                pad=title_pad,
                fontsize=10,
                weight="bold",
            )
            ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.25)
            ax.set_axisbelow(True)
            ax.legend(loc="upper right", fontsize=8, frameon=True)

        def _style_axis_qc(ax):
            if not qc_style:
                return
            ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.25)
            ax.set_axisbelow(True)
            for spine in ax.spines.values():
                spine.set_linewidth(1.0)

        # -------------------------------------------------
        # radial values
        # -------------------------------------------------
        rp = pd.Series((xp ** 2 + yp ** 2) ** 0.5, index=work.index)
        rs = pd.Series((xs ** 2 + ys ** 2) ** 0.5, index=work.index)
        pair_max = pd.concat([rp, rs], axis=1).max(axis=1)

        pair_lengths = []
        for i in range(len(work)):
            if (
                    pd.notna(xp.iloc[i]) and pd.notna(yp.iloc[i]) and
                    pd.notna(xs.iloc[i]) and pd.notna(ys.iloc[i])
            ):
                dx_pair = float(xs.iloc[i]) - float(xp.iloc[i])
                dy_pair = float(ys.iloc[i]) - float(yp.iloc[i])
                pair_lengths.append((i, (dx_pair ** 2 + dy_pair ** 2) ** 0.5))

        pair_len_only = [v for _, v in pair_lengths]
        pair_len_min = min(pair_len_only) if pair_len_only else 0.0
        pair_len_max = max(pair_len_only) if pair_len_only else 1.0

        valid_pair_max = pair_max.dropna()
        auto_red_radius = float(valid_pair_max.max()) if not valid_pair_max.empty else None

        if red_radius_mode == "max":
            circle_radius = auto_red_radius
        else:
            circle_radius = red_radius

        p50_radius = _percentile(valid_pair_max, 0.50) if show_percentile_circles else None
        p95_radius = _percentile(valid_pair_max, 0.95) if show_percentile_circles else None
        p99_radius = _percentile(valid_pair_max, 0.99) if not valid_pair_max.empty else None

        worst_idx = None
        if not valid_pair_max.empty:
            worst_idx = int(valid_pair_max.idxmax())

        # -------------------------------------------------
        # robust display radius
        # -------------------------------------------------
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
            allr = pd.concat([rp, rs], axis=0).dropna()
            plot_radius = float(allr.quantile(0.95)) if not allr.empty else 1.0

        plot_radius = max(float(plot_radius), 1.0)
        plot_lim = plot_radius * (1.0 + display_radius_pad_ratio)

        # -------------------------------------------------
        # layout
        # -------------------------------------------------
        fig = plt.figure(figsize=figsize)

        gs = GridSpec(
            3, 4,
            figure=fig,
            width_ratios=[0.75, 4.0, 0.12, 0.75],
            height_ratios=[0.75, 4.0, 0.87],
            wspace=0.16,
            hspace=0.18,
        )

        ax_histx_top = fig.add_subplot(gs[0, 1])
        ax_scatter = fig.add_subplot(gs[1, 1])
        ax_histx_bot = fig.add_subplot(gs[2, 1], sharex=ax_scatter)

        ax_histy_left = fig.add_subplot(gs[1, 0], sharey=ax_scatter)
        ax_cbar = fig.add_subplot(gs[1, 2])
        ax_histy_right = fig.add_subplot(gs[1, 3], sharey=ax_scatter)

        for pos in [(0, 0), (0, 2), (0, 3), (2, 0), (2, 2), (2, 3)]:
            ax_blank = fig.add_subplot(gs[pos])
            ax_blank.axis("off")

        # -------------------------------------------------
        # scatter base
        # -------------------------------------------------
        _style_axis_qc(ax_scatter)

        ax_scatter.scatter(
            xp, yp,
            s=point_size,
            alpha=0.95,
            color=primary_color,
            edgecolors="white" if qc_style else "none",
            linewidths=0.4,
            label=p_name,
            zorder=4,
        )
        ax_scatter.scatter(
            xs, ys,
            s=point_size,
            alpha=0.95,
            color=secondary_color,
            edgecolors="white" if qc_style else "none",
            linewidths=0.4,
            label=s_name,
            zorder=4,
        )

        cmap = cm.get_cmap("turbo")
        norm = colors.Normalize(
            vmin=pair_len_min,
            vmax=pair_len_max if pair_len_max > pair_len_min else pair_len_min + 1e-9
        )

        if connect_pairs:
            for i, plen in pair_lengths:
                x1 = float(xp.iloc[i])
                y1 = float(yp.iloc[i])
                x2 = float(xs.iloc[i])
                y2 = float(ys.iloc[i])

                line_color = cmap(norm(plen)) if show_pair_heatmap else (0.45, 0.45, 0.45, 1.0)

                ax_scatter.plot(
                    [x1, x2],
                    [y1, y2],
                    color=line_color,
                    linewidth=pair_linewidth,
                    alpha=pair_alpha,
                    zorder=2,
                )

        circle_handles = []

        if show_percentile_circles:
            if p50_radius is not None and math.isfinite(p50_radius) and p50_radius > 0:
                ax_scatter.add_patch(
                    plt.Circle(
                        (0, 0), p50_radius,
                        fill=False,
                        linewidth=percentile_circle_linewidth,
                        linestyle="--",
                        color=p50_circle_color,
                        zorder=1,
                    )
                )
                circle_handles.append(
                    Line2D([0], [0], color=p50_circle_color, lw=percentile_circle_linewidth, ls="--",
                           label=f"P50 = {p50_radius:.2f} m")
                )

            if p95_radius is not None and math.isfinite(p95_radius) and p95_radius > 0:
                ax_scatter.add_patch(
                    plt.Circle(
                        (0, 0), p95_radius,
                        fill=False,
                        linewidth=percentile_circle_linewidth,
                        linestyle=":",
                        color=p95_circle_color,
                        zorder=1,
                    )
                )
                circle_handles.append(
                    Line2D([0], [0], color=p95_circle_color, lw=percentile_circle_linewidth, ls=":",
                           label=f"P95 = {p95_radius:.2f} m")
                )

        max_circle_drawn = False
        if red_is_show and circle_radius is not None and math.isfinite(circle_radius) and circle_radius > 0:
            if circle_radius <= plot_lim * max_to_display_ratio_for_clip:
                ax_scatter.add_patch(
                    plt.Circle(
                        (0, 0), circle_radius,
                        fill=False,
                        linewidth=2.0,
                        linestyle="-",
                        color=red_circle_color,
                        zorder=1,
                    )
                )
                max_circle_drawn = True

            circle_handles.append(
                Line2D([0], [0], color=red_circle_color, lw=2.0, ls="-", label=f"Max = {circle_radius:.2f} m")
            )

        # worst station highlight
        if show_worst_station and worst_idx is not None:
            worst_points = []
            if pd.notna(xp.iloc[worst_idx]) and pd.notna(yp.iloc[worst_idx]):
                worst_points.append((float(xp.iloc[worst_idx]), float(yp.iloc[worst_idx])))
            if pd.notna(xs.iloc[worst_idx]) and pd.notna(ys.iloc[worst_idx]):
                worst_points.append((float(xs.iloc[worst_idx]), float(ys.iloc[worst_idx])))

            for wx, wy in worst_points:
                ax_scatter.scatter(
                    [wx], [wy],
                    s=140,
                    facecolors="none",
                    edgecolors=worst_station_color,
                    linewidths=2.5,
                    zorder=6,
                )
                ax_scatter.scatter(
                    [wx], [wy],
                    s=40,
                    color=worst_station_color,
                    zorder=7,
                )

            circle_handles.append(
                Line2D([0], [0], marker="o", color="none", markerfacecolor=worst_station_color,
                       markeredgecolor=worst_station_color, markersize=7, label="Worst station")
            )

        # robust limits
        xlim = (-plot_lim, plot_lim)
        ylim = (-plot_lim, plot_lim)
        ax_scatter.set_xlim(xlim)
        ax_scatter.set_ylim(ylim)

        ax_scatter.axhline(0, linewidth=1.0, color="#1565c0", alpha=0.9)
        ax_scatter.axvline(0, linewidth=1.0, color="#1565c0", alpha=0.9)

        title_parts = [title]
        if circle_radius is not None and math.isfinite(circle_radius):
            title_parts.append(f"Max={circle_radius:.2f} m")
        if p50_radius is not None and math.isfinite(p50_radius):
            title_parts.append(f"P50={p50_radius:.2f} m")
        if p95_radius is not None and math.isfinite(p95_radius):
            title_parts.append(f"P95={p95_radius:.2f} m")
        if plot_radius is not None and math.isfinite(plot_radius):
            title_parts.append(f"Display={plot_radius:.2f} m")

        ax_scatter.set_title(" | ".join(title_parts), pad=24, fontsize=14, weight="bold")
        ax_scatter.set_xlabel("dX, m", fontsize=12)
        ax_scatter.set_ylabel("dY, m", fontsize=12)
        ax_scatter.set_aspect("equal", adjustable="box")

        main_legend = ax_scatter.legend(
            loc="upper right",
            fontsize=10,
            frameon=True,
            framealpha=0.9,
        )
        if show_circle_legend and circle_handles:
            circle_legend = ax_scatter.legend(
                handles=circle_handles,
                loc="lower right",
                fontsize=9,
                title="Radial QC",
                frameon=True,
                framealpha=0.9,
            )
            ax_scatter.add_artist(main_legend)

        # station labels
        add_labels = show_station_labels
        if max_station_labels is not None and len(work) > max_station_labels:
            add_labels = False

        if add_labels:
            for i in range(len(work)):
                label = _station_text(stations.iloc[i])
                if not label:
                    continue

                has_p = pd.notna(xp.iloc[i]) and pd.notna(yp.iloc[i])
                has_s = pd.notna(xs.iloc[i]) and pd.notna(ys.iloc[i])

                try:
                    if has_p and has_s:
                        x1 = float(xp.iloc[i])
                        y1 = float(yp.iloc[i])
                        x2 = float(xs.iloc[i])
                        y2 = float(ys.iloc[i])

                        dx_pair = x2 - x1
                        dy_pair = y2 - y1
                        norm_pair = (dx_pair ** 2 + dy_pair ** 2) ** 0.5

                        if norm_pair > 0:
                            ox = -dy_pair / norm_pair
                            oy = dx_pair / norm_pair
                        else:
                            ox = 0.0
                            oy = 0.0

                        xm = (x1 + x2) / 2.0
                        ym = (y1 + y2) / 2.0

                        label_text = label
                        if show_worst_station and worst_idx is not None and i == worst_idx:
                            label_text = f"{label} (worst)"

                        ax_scatter.annotate(
                            label_text,
                            (xm + label_offset * ox, ym + label_offset * oy),
                            fontsize=station_fontsize,
                            alpha=0.95,
                            color="black",
                            zorder=7,
                        )
                    elif has_p:
                        ax_scatter.annotate(
                            label,
                            (float(xp.iloc[i]), float(yp.iloc[i])),
                            xytext=(3, 3),
                            textcoords="offset points",
                            fontsize=station_fontsize,
                            alpha=0.95,
                            color="black",
                            zorder=7,
                        )
                    elif has_s:
                        ax_scatter.annotate(
                            label,
                            (float(xs.iloc[i]), float(ys.iloc[i])),
                            xytext=(3, 3),
                            textcoords="offset points",
                            fontsize=station_fontsize,
                            alpha=0.95,
                            color="black",
                            zorder=7,
                        )
                except Exception:
                    pass

        # circle labels
        if show_percentile_circles:
            if p50_radius is not None and math.isfinite(p50_radius) and p50_radius <= plot_lim * 1.02:
                off = max(0.03 * p50_radius, 0.03)
                ax_scatter.text(
                    p50_radius - off, 0,
                    f"P50 {p50_radius:.2f}",
                    color=p50_circle_color,
                    fontsize=percentile_label_fontsize,
                    va="bottom", ha="left",
                    bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.70) if qc_style else None,
                )
            if p95_radius is not None and math.isfinite(p95_radius) and p95_radius <= plot_lim * 1.02:
                off = max(0.03 * p95_radius, 0.03)
                ax_scatter.text(
                    p95_radius - off, 0,
                    f"P95 {p95_radius:.2f}",
                    color=p95_circle_color,
                    fontsize=percentile_label_fontsize,
                    va="top", ha="left",
                    bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.70) if qc_style else None,
                )

        if max_circle_drawn and circle_radius is not None and math.isfinite(circle_radius):
            off = max(0.03 * circle_radius, 0.03)
            ax_scatter.text(
                circle_radius - off, 0,
                f"Max {circle_radius:.2f}",
                color=red_circle_color,
                fontsize=percentile_label_fontsize,
                va="center", ha="left",
                bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.70) if qc_style else None,
            )

        if (
                show_outside_max_arrow
                and circle_radius is not None
                and math.isfinite(circle_radius)
                and circle_radius > plot_lim
        ):
            ax_scatter.annotate(
                f"Max {circle_radius:.2f} m outside display range",
                xy=(plot_lim * 0.98, 0),
                xytext=(plot_lim * 0.35, plot_lim * 0.90),
                arrowprops=dict(arrowstyle="->", color=red_circle_color, lw=1.5),
                color=red_circle_color,
                fontsize=percentile_label_fontsize,
                ha="left",
                va="center",
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.8),
                zorder=8,
            )

        # -------------------------------------------------
        # histograms
        # -------------------------------------------------
        _style_axis_qc(ax_histx_top)
        _style_axis_qc(ax_histx_bot)
        _style_axis_qc(ax_histy_left)
        _style_axis_qc(ax_histy_right)

        _add_hist_with_kde_std(
            ax_histx_top,
            xp,
            "Primary dX",
            "dX, m",
            primary_color,
            orientation="vertical",
            title_pad=8,
        )
        _add_hist_with_kde_std(
            ax_histx_bot,
            xs,
            "Secondary dX",
            "dX, m",
            secondary_color,
            orientation="vertical",
            title_pad=12,
        )
        _add_hist_with_kde_std(
            ax_histy_left,
            yp,
            "Primary dY",
            "dY, m",
            primary_color,
            orientation="horizontal",
            title_pad=8,
        )
        _add_hist_with_kde_std(
            ax_histy_right,
            ys,
            "Secondary dY",
            "dY, m",
            secondary_color,
            orientation="horizontal",
            title_pad=8,
        )

        ax_histx_top.set_xlim(ax_scatter.get_xlim())
        ax_histx_bot.set_xlim(ax_scatter.get_xlim())
        ax_histy_left.set_ylim(ax_scatter.get_ylim())
        ax_histy_right.set_ylim(ax_scatter.get_ylim())

        ax_histx_top.tick_params(axis="x", labelbottom=False)
        ax_histx_top.set_xlabel("")
        ax_histx_top.spines["bottom"].set_visible(False)

        ax_histx_bot.set_xlabel("dX, m", fontsize=11)
        ax_histx_bot.spines["top"].set_visible(False)

        ax_histy_left.set_ylabel("dY, m", fontsize=11)
        ax_histy_right.set_ylabel("")
        ax_histy_left.tick_params(axis="y", labelleft=True)
        ax_histy_right.tick_params(axis="y", labelleft=False)

        # colorbar
        if show_colorbar and connect_pairs and show_pair_heatmap and pair_lengths:
            sm = cm.ScalarMappable(norm=norm, cmap=cmap)
            sm.set_array([])
            cbar = fig.colorbar(sm, cax=ax_cbar)
            cbar.set_label(colorbar_label, fontsize=10)
            cbar.ax.tick_params(labelsize=9)
        else:
            ax_cbar.axis("off")

        if qc_style:
            fig.patch.set_facecolor("white")
            for ax in [ax_histx_top, ax_histx_bot, ax_histy_left, ax_histy_right, ax_scatter]:
                ax.set_facecolor("white")

        fig.subplots_adjust(top=0.95, bottom=0.06, left=0.06, right=0.95)

        out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
        self._save_figure(fig, out_path, dpi=dpi, close=False)

        if is_show:
            plt.show()

        if close:
            plt.close(fig)

        return fig, str(out_path)

    # =========================================================
    # deplpre
    # =========================================================
    def deployment_offsets_vs_preplot(
        self,
        df,
        line=None,
        line_bearing=0.0,
        x_col="Station",
        pre_x_col="PreplotEasting",
        pre_y_col="PreplotNorthing",
        pri_x_col="PrimaryEasting",
        pri_y_col="PrimaryNorthing",
        sec_x_col="SecondaryEasting",
        sec_y_col="SecondaryNorthing",
        is_show=False,
        figsize=(16, 10),
        save_dir=".",
        file_name=None,
        suffix="deplpre",
        ext="png",
        dpi=200,
        close=False,
    ):
        if df is None or df.empty:
            fig = self.empty_figure(title=f"Line {line} — no data")
            out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        work = df.copy()
        for c in [
            x_col, pre_x_col, pre_y_col,
            pri_x_col, pri_y_col, sec_x_col, sec_y_col
        ]:
            if c in work.columns:
                work[c] = self._num(work[c])

        work = work.sort_values(x_col)

        work["dE_primary"] = work[pri_x_col] - work[pre_x_col]
        work["dN_primary"] = work[pri_y_col] - work[pre_y_col]
        work["dE_secondary"] = work[sec_x_col] - work[pre_x_col]
        work["dN_secondary"] = work[sec_y_col] - work[pre_y_col]

        fig, axes = plt.subplots(2, 1, sharex=True, figsize=figsize)

        axes[0].plot(work[x_col], work["dE_primary"], marker="o", linewidth=1.5, label="Primary dE")
        axes[0].plot(work[x_col], work["dE_secondary"], marker="o", linewidth=1.5, label="Secondary dE")
        self._style_ax(
            axes[0],
            ylabel="dE, m",
            title=f"Line {line} — Easting offset vs Preplot",
        )
        axes[0].legend()

        axes[1].plot(work[x_col], work["dN_primary"], marker="o", linewidth=1.5, label="Primary dN")
        axes[1].plot(work[x_col], work["dN_secondary"], marker="o", linewidth=1.5, label="Secondary dN")
        self._style_ax(
            axes[1],
            xlabel="Station",
            ylabel="dN, m",
            title=f"Line {line} — Northing offset vs Preplot",
        )
        axes[1].legend()

        fig.tight_layout()

        out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
        self._save_figure(fig, out_path, dpi=dpi, close=False)

        if is_show:
            plt.show()

        if close:
            plt.close(fig)

        return fig, str(out_path)

    # =========================================================
    # timing
    # =========================================================
    def graph_recover_time(
        self,
        df,
        line=None,
        is_deploy=True,
        x_col="Station",
        deploy_ts_col="TimeStamp",
        recover_ts_col="TimeStamp1",
        is_show=False,
        figsize=(16, 4.5),
        save_dir=".",
        file_name=None,
        suffix=None,
        ext="png",
        dpi=200,
        close=False,
    ):
        if suffix is None:
            suffix = "timing_deploy" if is_deploy else "timing_recover"

        if df is None or df.empty:
            fig = self.empty_figure(title=f"Line {line} — no data")
            out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        work = df.copy()
        ts_col = deploy_ts_col if is_deploy else recover_ts_col

        if ts_col not in work.columns:
            fig = self.empty_figure(title=f"Line {line}", message=f"Column {ts_col} not found")
            out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        work[x_col] = self._num(work[x_col])
        work[ts_col] = pd.to_datetime(work[ts_col], errors="coerce")
        work = work.sort_values(x_col)

        t0 = work[ts_col].dropna().min()
        if pd.isna(t0):
            fig = self.empty_figure(title=f"Line {line}", message=f"No valid {ts_col}")
            out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        work["hours_from_start"] = (work[ts_col] - t0).dt.total_seconds() / 3600.0

        fig, ax = plt.subplots(figsize=figsize)
        ax.bar(
            work[x_col],
            work["hours_from_start"],
            width=self._bar_width(work[x_col]),
            alpha=0.85,
        )

        self._style_ax(
            ax,
            xlabel="Station",
            ylabel="Hours",
            title=f"Line {line} — {'Deployment' if is_deploy else 'Recovery'} time from first point",
        )
        fig.tight_layout()

        out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
        self._save_figure(fig, out_path, dpi=dpi, close=False)

        if is_show:
            plt.show()

        if close:
            plt.close(fig)

        return fig, str(out_path)

    # =========================================================
    # map
    # =========================================================
    def plot_line_map(
        self,
        df,
        bbdata=None,
        cfg_row=None,
        line=None,
        pre_x_col="PreplotEasting",
        pre_y_col="PreplotNorthing",
        pri_x_col="PrimaryEasting",
        pri_y_col="PrimaryNorthing",
        sec_x_col="SecondaryEasting",
        sec_y_col="SecondaryNorthing",
        is_show=False,
        figsize=(10, 10),
        save_dir=".",
        file_name=None,
        suffix="map",
        ext="png",
        dpi=200,
        close=False,
    ):
        if df is None or df.empty:
            fig = self.empty_figure(title=f"Line {line} — no map data")
            out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        work = df.copy()
        for c in [pre_x_col, pre_y_col, pri_x_col, pri_y_col, sec_x_col, sec_y_col]:
            if c in work.columns:
                work[c] = self._num(work[c])

        fig, ax = plt.subplots(figsize=figsize)

        if pre_x_col in work.columns and pre_y_col in work.columns:
            ax.plot(
                work[pre_x_col],
                work[pre_y_col],
                marker="o",
                linewidth=1.2,
                label="Preplot",
            )

        if pri_x_col in work.columns and pri_y_col in work.columns:
            ax.scatter(
                work[pri_x_col],
                work[pri_y_col],
                s=20,
                label="Primary",
            )

        if sec_x_col in work.columns and sec_y_col in work.columns:
            ax.scatter(
                work[sec_x_col],
                work[sec_y_col],
                s=20,
                label="Secondary",
            )

        if bbdata is not None and len(bbdata) > 0:
            bb = bbdata.copy()

            for c in ["Easting", "Northing", "ROV_Easting", "ROV_Northing"]:
                if c in bb.columns:
                    bb[c] = self._num(bb[c])

            if "Easting" in bb.columns and "Northing" in bb.columns:
                ax.plot(bb["Easting"], bb["Northing"], linewidth=1.0, alpha=0.75, label="BlackBox")
            elif "ROV_Easting" in bb.columns and "ROV_Northing" in bb.columns:
                ax.plot(bb["ROV_Easting"], bb["ROV_Northing"], linewidth=1.0, alpha=0.75, label="BlackBox")

        self._style_ax(ax, xlabel="Easting", ylabel="Northing", title=f"Line {line} map")
        ax.set_aspect("equal", adjustable="datalim")
        ax.legend()
        fig.tight_layout()

        out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
        self._save_figure(fig, out_path, dpi=dpi, close=False)

        if is_show:
            plt.show()

        if close:
            plt.close(fig)

        return fig, str(out_path)

    def two_series_vs_station_with_diff_bar(
            self,
            df,
            series1_col,
            series2_col,
            series1_label="Series 1",
            series2_label="Series 2",
            diff_label=None,
            rov_col=None,
            x_col="Station",
            is_show=False,
            reverse_y_if_negative=False,
            title=None,
            y_label="Value",
            diff_y_label="Difference",
            diff_mode="series1_minus_series2",  # "series1_minus_series2" | "series2_minus_series1" | "abs"
            show_zero_line=True,
            line1_color="#1f77b4",
            line2_color="#ff7f0e",
            figsize=(18, 8),
            save_dir=".",
            file_name=None,
            suffix="water_diff",
            ext="png",
            dpi=200,
            close=False,
    ):

        if df is None or df.empty:
            fig = self.empty_figure(title=title or "No data")
            out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        work = df.copy()
        work[x_col] = self._num(work[x_col])
        work[series1_col] = self._num(work[series1_col])
        work[series2_col] = self._num(work[series2_col])

        if rov_col and rov_col in work.columns:
            work[rov_col] = work[rov_col].astype(str).fillna("").str.strip()
        else:
            rov_col = None

        work = work.sort_values(x_col).reset_index(drop=True)

        # ---------------------------------
        # difference
        # ---------------------------------
        if diff_mode == "series2_minus_series1":
            work["_diff_"] = work[series2_col] - work[series1_col]
            if diff_label is None:
                diff_label = f"{series2_label} - {series1_label}"
        elif diff_mode == "abs":
            work["_diff_"] = (work[series1_col] - work[series2_col]).abs()
            if diff_label is None:
                diff_label = f"|{series1_label} - {series2_label}|"
        else:
            work["_diff_"] = work[series1_col] - work[series2_col]
            if diff_label is None:
                diff_label = f"{series1_label} - {series2_label}"

        x = work[x_col]
        width = self._bar_width(x)

        # ---------------------------------
        # ROV colors for bars only
        # ---------------------------------
        rov_color_map = {}
        rov_handles = []

        if rov_col:
            rov_values = [v for v in work[rov_col].dropna().unique().tolist() if str(v).strip() != ""]
            rov_values = sorted(rov_values)

            cmap = cm.get_cmap("tab10", max(len(rov_values), 1))

            for i, rov in enumerate(rov_values):
                rov_color_map[rov] = cmap(i)

            bar_colors = [
                rov_color_map.get(str(v).strip(), "#999999")
                for v in work[rov_col]
            ]

            rov_handles = [
                Patch(facecolor=rov_color_map[rov], edgecolor="none", label=str(rov))
                for rov in rov_values
            ]
        else:
            bar_colors = ["#2ca02c" if (pd.notna(v) and v >= 0) else "#d62728" for v in work["_diff_"]]

        # ---------------------------------
        # layout
        # ---------------------------------
        fig = plt.figure(figsize=figsize)
        gs = GridSpec(
            2, 1,
            figure=fig,
            height_ratios=[1.35, 4.0],
            hspace=0.08,
        )

        ax_top = fig.add_subplot(gs[0, 0])
        ax_main = fig.add_subplot(gs[1, 0], sharex=ax_top)

        # ---------------------------------
        # top difference bars
        # ---------------------------------
        ax_top.bar(
            x,
            work["_diff_"],
            width=width,
            color=bar_colors,
            alpha=0.90,
            label=diff_label,
        )

        if show_zero_line:
            ax_top.axhline(0, color="black", linewidth=1.0, alpha=0.8)

        self._style_ax(
            ax_top,
            ylabel=diff_y_label,
            title=f"{title or f'{series1_label} / {series2_label}'} | Diff ({self._stats_text(work['_diff_'])})",
        )

        if rov_handles:
            ax_top.legend(handles=rov_handles, title="ROV", loc="best", fontsize=9)
        else:
            ax_top.legend(loc="best", fontsize=9)

        # ---------------------------------
        # main lines
        # ---------------------------------
        ax_main.plot(
            x,
            work[series1_col],
            marker="o",
            linewidth=1.5,
            color=line1_color,
            label=series1_label,
        )
        ax_main.plot(
            x,
            work[series2_col],
            marker="o",
            linewidth=1.5,
            color=line2_color,
            label=series2_label,
        )

        self._style_ax(
            ax_main,
            xlabel="Station",
            ylabel=y_label,
            title=None,
        )

        ax_main.legend(loc="best", fontsize=9)

        if reverse_y_if_negative:
            vals = pd.concat([work[series1_col], work[series2_col]]).dropna()
            if not vals.empty and vals.mean() < 0:
                ax_main.invert_yaxis()

        # ---------------------------------
        # x axis: every 2nd node label
        # ---------------------------------
        x_valid = pd.to_numeric(x, errors="coerce").dropna().tolist()
        if x_valid:
            ax_main.set_xticks(x_valid, minor=True)

            major_ticks = x_valid[::2]
            if major_ticks and x_valid[-1] != major_ticks[-1]:
                major_ticks.append(x_valid[-1])
            elif not major_ticks:
                major_ticks = x_valid

            ax_main.set_xticks(major_ticks)
            ax_main.set_xticklabels(
                [str(int(v)) if float(v).is_integer() else str(v) for v in major_ticks],
                rotation=90,
                fontsize=9
            )

            ax_main.tick_params(axis="x", which="major", length=6)
            ax_main.tick_params(axis="x", which="minor", length=3, color="gray")
            ax_main.grid(True, axis="x", which="major", linestyle="--", alpha=0.30)

        plt.setp(ax_top.get_xticklabels(), visible=False)

        fig.tight_layout()

        out_path = self._build_output_path(df, save_dir, suffix, file_name, ext)
        self._save_figure(fig, out_path, dpi=dpi, close=False)

        if is_show:
            plt.show()

        if close:
            plt.close(fig)

        return fig, str(out_path)

    def plot_project_map(
            self,
            rp_df,
            dsr_df=None,
            selected_line=None,
            *,
            # RP columns
            rp_x_col="X",
            rp_y_col="Y",
            rp_line_col="Line",
            rp_point_col="Point",
            # DSR columns
            dsr_x_col="PrimaryEasting",
            dsr_y_col="PrimaryNorthing",
            dsr_line_col="Line",
            dsr_station_col="Station",
            dsr_rov_col="ROV",
            dsr_node_col="Node",
            # title / style
            title="Project Map",
            show_all_dsr=True,
            show_selected_line_nodes=True,
            show_station_labels=False,
            show_preplot_points=False,
            preplot_color="#d0d0d0",
            preplot_selected_color="#4a4a4a",
            dsr_other_color="#d9d9d9",
            selected_line_width=1.8,
            other_line_width=0.6,
            point_size=10,
            selected_point_size=14,
            # scalebar
            scale_bar_length=None,
            scale_bar_units="m",
            scale_bar_location="lower right",
            # epsg
            source_epsg=None,
            target_epsg=None,
            # optional layers / shapes
            show_shapes=False,
            show_layers=False,
            csv_epsg=None,
            shapes_table="project_shapes",
            default_shape_epsg=None,
            # zebra
            zebra_frame=False,
            zebra_x_step=None,
            zebra_y_step=None,
            zebra_color="#efefef",
            zebra_alpha=0.35,
            # output
            figsize=(12, 12),
            save_dir=".",
            file_name=None,
            suffix="project_map",
            ext="png",
            dpi=200,
            is_show=False,
            close=False,
    ):
        """
        Project map with highlighted selected line.

        Requirements:
          - helpers already exist in class:
              _transform_xy_dataframe
              _add_zebra_frame
              add_csv_layers_to_map_matplotlib
              add_project_shapes_layers_matplotlib
        """


        def _safe_float(v):
            try:
                if pd.isna(v):
                    return None
                return float(v)
            except Exception:
                return None

        def _safe_line_label(v):
            try:
                fv = float(v)
                if float(fv).is_integer():
                    return str(int(fv))
                return str(fv)
            except Exception:
                return str(v)

        def _nice_scale_length(raw_length):
            if raw_length is None or raw_length <= 0:
                return 100.0
            exp = math.floor(math.log10(raw_length))
            base = raw_length / (10 ** exp)
            if base <= 1:
                nice = 1
            elif base <= 2:
                nice = 2
            elif base <= 5:
                nice = 5
            else:
                nice = 10
            return nice * (10 ** exp)

        def _draw_scale_bar(ax_, length=None, units="m", location="lower right"):
            x0, x1 = ax_.get_xlim()
            y0, y1 = ax_.get_ylim()

            xr = x1 - x0
            yr = y1 - y0
            if xr <= 0 or yr <= 0:
                return

            if length is None:
                length = _nice_scale_length(xr * 0.12)

            margin_x = xr * 0.04
            margin_y = yr * 0.05
            tick_h = yr * 0.010

            if location == "lower left":
                sx = x0 + margin_x
            else:
                sx = x1 - margin_x - length

            sy = y0 + margin_y

            ax_.plot([sx, sx + length], [sy, sy], color="black", lw=2.0, zorder=50)
            ax_.plot([sx, sx], [sy - tick_h, sy + tick_h], color="black", lw=1.5, zorder=50)
            ax_.plot([sx + length, sx + length], [sy - tick_h, sy + tick_h], color="black", lw=1.5, zorder=50)

            if str(units).lower() == "km":
                label = f"{length / 1000.0:g} km"
            else:
                if length >= 1000 and float(length) % 1000 == 0:
                    label = f"{int(length / 1000)} km"
                else:
                    label = f"{int(length)} m"

            ax_.text(
                sx + length / 2.0,
                sy + tick_h * 1.8,
                label,
                ha="center",
                va="bottom",
                fontsize=8,
                color="black",
                bbox=dict(boxstyle="round,pad=0.12", fc="white", ec="none", alpha=0.75),
                zorder=51,
            )

        # -------------------------------------------------
        # input checks
        # -------------------------------------------------
        if rp_df is None or rp_df.empty:
            fig = self.empty_figure(title=title, message="No RP preplot data")
            out_path = self._build_output_path(rp_df, save_dir, suffix, file_name, ext)
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        rp = rp_df.copy()
        dsr = dsr_df.copy() if dsr_df is not None else pd.DataFrame()

        # -------------------------------------------------
        # cleanup RP
        # -------------------------------------------------
        for c in [rp_x_col, rp_y_col]:
            if c in rp.columns:
                rp[c] = self._num(rp[c])

        if rp_line_col in rp.columns:
            rp[rp_line_col] = pd.to_numeric(rp[rp_line_col], errors="coerce")

        if rp_point_col in rp.columns:
            rp[rp_point_col] = pd.to_numeric(rp[rp_point_col], errors="coerce")

        rp = rp.dropna(subset=[rp_x_col, rp_y_col]).copy()

        # -------------------------------------------------
        # cleanup DSR
        # -------------------------------------------------
        if not dsr.empty:
            for c in [dsr_x_col, dsr_y_col]:
                if c in dsr.columns:
                    dsr[c] = self._num(dsr[c])

            if dsr_line_col in dsr.columns:
                dsr[dsr_line_col] = pd.to_numeric(dsr[dsr_line_col], errors="coerce")

            if dsr_station_col in dsr.columns:
                dsr[dsr_station_col] = pd.to_numeric(dsr[dsr_station_col], errors="coerce")

            if dsr_rov_col not in dsr.columns:
                dsr[dsr_rov_col] = ""

            dsr[dsr_rov_col] = dsr[dsr_rov_col].fillna("").astype(str).str.strip()
            dsr = dsr.dropna(subset=[dsr_x_col, dsr_y_col]).copy()

        # -------------------------------------------------
        # EPSG transform
        # -------------------------------------------------
        rp = self._transform_xy_dataframe(
            rp,
            rp_x_col,
            rp_y_col,
            source_epsg=source_epsg,
            target_epsg=target_epsg,
            out_x="_plot_x",
            out_y="_plot_y",
        )

        if not dsr.empty:
            dsr = self._transform_xy_dataframe(
                dsr,
                dsr_x_col,
                dsr_y_col,
                source_epsg=source_epsg,
                target_epsg=target_epsg,
                out_x="_plot_x",
                out_y="_plot_y",
            )

        # -------------------------------------------------
        # selected line
        # -------------------------------------------------
        if selected_line is None:
            if not dsr.empty and dsr_line_col in dsr.columns and not dsr[dsr_line_col].dropna().empty:
                selected_line = dsr[dsr_line_col].dropna().iloc[0]
            elif rp_line_col in rp.columns and not rp[rp_line_col].dropna().empty:
                selected_line = rp[rp_line_col].dropna().iloc[0]

        selected_line_num = _safe_float(selected_line)

        # -------------------------------------------------
        # figure
        # -------------------------------------------------
        fig, ax = plt.subplots(figsize=figsize)

        # -------------------------------------------------
        # optional shapes first
        # -------------------------------------------------
        if show_shapes:
            try:
                self.add_project_shapes_layers_matplotlib(
                    ax,
                    shapes_table=shapes_table,
                    default_src_epsg=default_shape_epsg or source_epsg,
                    target_epsg=target_epsg,
                )
            except Exception:
                pass

        # -------------------------------------------------
        # RP lines
        # -------------------------------------------------
        if rp_line_col in rp.columns:
            for line_value, grp in rp.groupby(rp_line_col, dropna=True):
                grp = grp.sort_values(rp_point_col) if rp_point_col in grp.columns else grp

                line_num = _safe_float(line_value)
                is_selected = (
                        selected_line_num is not None
                        and line_num is not None
                        and line_num == selected_line_num
                )

                ax.plot(
                    grp["_plot_x"],
                    grp["_plot_y"],
                    color=preplot_selected_color if is_selected else preplot_color,
                    linewidth=selected_line_width if is_selected else other_line_width,
                    alpha=0.95 if is_selected else 0.75,
                    zorder=10 if is_selected else 5,
                )

                if is_selected and show_preplot_points:
                    ax.scatter(
                        grp["_plot_x"],
                        grp["_plot_y"],
                        s=4,
                        color=preplot_selected_color,
                        alpha=0.8,
                        zorder=11,
                    )
        else:
            ax.plot(
                rp["_plot_x"],
                rp["_plot_y"],
                color=preplot_selected_color,
                linewidth=selected_line_width,
                alpha=0.9,
                zorder=10,
            )

        # -------------------------------------------------
        # background DSR
        # -------------------------------------------------
        if show_all_dsr and not dsr.empty:
            dsr_other = dsr.copy()

            if selected_line_num is not None and dsr_line_col in dsr_other.columns:
                dsr_other = dsr_other[
                    pd.to_numeric(dsr_other[dsr_line_col], errors="coerce") != selected_line_num
                    ].copy()

            if not dsr_other.empty:
                ax.scatter(
                    dsr_other["_plot_x"],
                    dsr_other["_plot_y"],
                    s=max(point_size * 0.40, 3),
                    color=dsr_other_color,
                    alpha=0.28,
                    edgecolors="none",
                    zorder=20,
                )

        # -------------------------------------------------
        # selected line nodes by ROV
        # -------------------------------------------------
        legend_handles = []

        marker_cycle = ["o", "s", "^", "D", "P", "X", "v", "<", ">", "*", "h", "8"]
        color_cycle = list(plt.cm.tab10.colors) + list(plt.cm.Set2.colors) + list(plt.cm.Dark2.colors)

        if show_selected_line_nodes and not dsr.empty:
            dsr_sel = dsr.copy()

            if selected_line_num is not None and dsr_line_col in dsr_sel.columns:
                dsr_sel = dsr_sel[
                    pd.to_numeric(dsr_sel[dsr_line_col], errors="coerce") == selected_line_num
                    ].copy()

            if not dsr_sel.empty:
                rov_values = sorted([
                    v for v in dsr_sel[dsr_rov_col].dropna().astype(str).unique().tolist()
                    if str(v).strip() != ""
                ])

                if not rov_values:
                    rov_values = ["N/A"]

                rov_marker_map = {rov: marker_cycle[i % len(marker_cycle)] for i, rov in enumerate(rov_values)}
                rov_color_map = {rov: color_cycle[i % len(color_cycle)] for i, rov in enumerate(rov_values)}

                for rov, grp in dsr_sel.groupby(dsr_rov_col, dropna=False):
                    rov_key = str(rov).strip() if pd.notna(rov) and str(rov).strip() else "N/A"
                    marker = rov_marker_map.get(rov_key, "o")
                    color = rov_color_map.get(rov_key, "#1f77b4")

                    ax.scatter(
                        grp["_plot_x"],
                        grp["_plot_y"],
                        s=selected_point_size,
                        marker=marker,
                        alpha=0.95,
                        color=color,
                        edgecolors="black",
                        linewidths=0.30,
                        zorder=30,
                    )

                    legend_handles.append(
                        Line2D(
                            [0], [0],
                            marker=marker,
                            color="none",
                            markerfacecolor=color,
                            markeredgecolor="black",
                            markeredgewidth=0.30,
                            markersize=6.5,
                            label=rov_key,
                        )
                    )

                    if show_station_labels and dsr_station_col in grp.columns:
                        for _, r in grp.iterrows():
                            st = r.get(dsr_station_col, "")
                            if pd.notna(st):
                                try:
                                    st_txt = str(int(float(st)))
                                except Exception:
                                    st_txt = str(st)

                                ax.annotate(
                                    st_txt,
                                    (r["_plot_x"], r["_plot_y"]),
                                    xytext=(3, 2),
                                    textcoords="offset points",
                                    fontsize=6,
                                    alpha=0.85,
                                    zorder=31,
                                )

        # -------------------------------------------------
        # optional csv layers after base map
        # -------------------------------------------------
        if show_layers:
            try:
                self.add_csv_layers_to_map_matplotlib(
                    ax,
                    csv_epsg=csv_epsg or source_epsg,
                    target_epsg=target_epsg,
                    show_labels=False,
                )
            except Exception:
                pass

        # -------------------------------------------------
        # selected line label
        # -------------------------------------------------
        if selected_line_num is not None and rp_line_col in rp.columns:
            rp_sel = rp[pd.to_numeric(rp[rp_line_col], errors="coerce") == selected_line_num].copy()
            if not rp_sel.empty:
                xmid = rp_sel["_plot_x"].median()
                ymid = rp_sel["_plot_y"].median()
                ax.text(
                    xmid,
                    ymid,
                    f"Line {_safe_line_label(selected_line_num)}",
                    fontsize=10,
                    weight="bold",
                    ha="center",
                    va="bottom",
                    bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="0.4", alpha=0.85),
                    zorder=40,
                )

        # -------------------------------------------------
        # style / aspect
        # -------------------------------------------------
        map_title = title
        if selected_line_num is not None:
            map_title = f"{title} — selected line {_safe_line_label(selected_line_num)}"

        self._style_ax(ax, xlabel="Easting", ylabel="Northing", title=map_title)
        ax.set_aspect("equal", adjustable="box")

        # -------------------------------------------------
        # zebra after limits are known
        # -------------------------------------------------
        if zebra_frame:
            try:
                self._add_zebra_frame(
                    ax,
                    x_step=zebra_x_step,
                    y_step=zebra_y_step,
                    color=zebra_color,
                    alpha=zebra_alpha,
                )
            except Exception:
                pass

        # redraw selected line above zebra if needed
        if zebra_frame and rp_line_col in rp.columns and selected_line_num is not None:
            rp_sel = rp[pd.to_numeric(rp[rp_line_col], errors="coerce") == selected_line_num].copy()
            if not rp_sel.empty:
                rp_sel = rp_sel.sort_values(rp_point_col) if rp_point_col in rp_sel.columns else rp_sel
                ax.plot(
                    rp_sel["_plot_x"],
                    rp_sel["_plot_y"],
                    color=preplot_selected_color,
                    linewidth=selected_line_width,
                    alpha=0.98,
                    zorder=41,
                )

        # -------------------------------------------------
        # scale bar
        # -------------------------------------------------
        _draw_scale_bar(
            ax,
            length=scale_bar_length,
            units=scale_bar_units,
            location=scale_bar_location,
        )

        # -------------------------------------------------
        # legend: only ROV + optional other matplotlib layers already added
        # -------------------------------------------------
        if legend_handles:
            seen = set()
            uniq = []
            for h in legend_handles:
                lbl = h.get_label()
                if lbl not in seen:
                    uniq.append(h)
                    seen.add(lbl)

            ax.legend(
                handles=uniq,
                loc="upper right",
                fontsize=9,
                title="ROV",
                framealpha=0.95,
            )

        fig.tight_layout()

        out_path = self._build_output_path(
            dsr if dsr is not None and not dsr.empty else rp,
            save_dir,
            suffix,
            file_name,
            ext,
        )
        self._save_figure(fig, out_path, dpi=dpi, close=False)

        if is_show:
            plt.show()

        if close:
            plt.close(fig)

        return fig, str(out_path)

    def _add_zebra_frame(
            self,
            ax,
            *,
            x_step=None,
            y_step=None,
            color="#f3f3f3",
            alpha=0.45,
    ):
        x0, x1 = ax.get_xlim()
        y0, y1 = ax.get_ylim()

        if x_step and x_step > 0:
            x = x0
            i = 0
            while x < x1:
                x_next = x + x_step
                if i % 2 == 0:
                    ax.axvspan(x, x_next, color=color, alpha=alpha, zorder=-20)
                x = x_next
                i += 1

        if y_step and y_step > 0:
            y = y0
            i = 0
            while y < y1:
                y_next = y + y_step
                if i % 2 == 0:
                    ax.axhspan(y, y_next, color=color, alpha=alpha * 0.55, zorder=-19)
                y = y_next
                i += 1

    def _transform_xy_dataframe(
            self,
            df,
            x_col,
            y_col,
            *,
            source_epsg=None,
            target_epsg=None,
            out_x="_plot_x",
            out_y="_plot_y",
    ):
        df = df.copy()

        df[out_x] = pd.to_numeric(df[x_col], errors="coerce")
        df[out_y] = pd.to_numeric(df[y_col], errors="coerce")

        if (
                source_epsg is None
                or target_epsg is None
                or int(source_epsg) == int(target_epsg)
        ):
            return df


        mask = df[out_x].notna() & df[out_y].notna()
        if mask.any():
            transformer = Transformer.from_crs(
                f"EPSG:{int(source_epsg)}",
                f"EPSG:{int(target_epsg)}",
                always_xy=True,
            )
            xx, yy = transformer.transform(
                df.loc[mask, out_x].to_numpy(),
                df.loc[mask, out_y].to_numpy(),
            )
            df.loc[mask, out_x] = xx
            df.loc[mask, out_y] = yy

        return df

    def add_csv_layers_to_map_matplotlib(
            self,
            ax,
            *,
            csv_epsg=None,
            target_epsg=None,
            max_labels=2000,
            show_labels=False,
    ):

        def _mpl_marker(marker):
            m = (marker or "").strip().lower()
            mapping = {
                "circle": "o",
                "square": "s",
                "triangle": "^",
                "diamond": "D",
                "cross": "x",
                "x": "x",
                "star": "*",
                "hex": "h",
            }
            return mapping.get(m, "o")

        transformer = None
        if csv_epsg and target_epsg and int(csv_epsg) != int(target_epsg):
            transformer = Transformer.from_crs(
                f"EPSG:{int(csv_epsg)}",
                f"EPSG:{int(target_epsg)}",
                always_xy=True,
            )

        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            cur.execute("""
                SELECT ID, Name, PointStyle, PointColor, PointSize
                FROM CSVLayers
                ORDER BY ID DESC
            """)
            layers = [dict(r) for r in cur.fetchall()]

            for layer in layers:
                cur.execute("""
                    SELECT Point, X, Y, Z, Attr1, Attr2, Attr3
                    FROM CSVpoints
                    WHERE Layer_FK = ?
                """, (layer["ID"],))
                pts = pd.DataFrame([dict(r) for r in cur.fetchall()])
                if pts.empty:
                    continue

                pts["X"] = pd.to_numeric(pts["X"], errors="coerce")
                pts["Y"] = pd.to_numeric(pts["Y"], errors="coerce")
                pts = pts.dropna(subset=["X", "Y"])
                if pts.empty:
                    continue

                if transformer is not None:
                    xx, yy = transformer.transform(pts["X"].to_numpy(), pts["Y"].to_numpy())
                    pts["_plot_x"] = xx
                    pts["_plot_y"] = yy
                else:
                    pts["_plot_x"] = pts["X"]
                    pts["_plot_y"] = pts["Y"]

                marker = _mpl_marker(layer.get("PointStyle"))
                color = layer.get("PointColor") or "#000000"
                size = float(layer.get("PointSize") or 6)

                ax.scatter(
                    pts["_plot_x"],
                    pts["_plot_y"],
                    s=size ** 2 / 2.5,
                    marker=marker,
                    color=color,
                    alpha=0.9,
                    linewidths=0.3,
                    edgecolors="black",
                    label=layer.get("Name") or f"Layer {layer['ID']}",
                    zorder=3,
                )

                if show_labels:
                    for _, r in pts.head(max_labels).iterrows():
                        ax.annotate(
                            str(r.get("Point", "")),
                            (r["_plot_x"], r["_plot_y"]),
                            xytext=(4, 4),
                            textcoords="offset points",
                            fontsize=7,
                            alpha=0.8,
                            zorder=4,
                        )

    def add_project_shapes_layers_matplotlib(
            self,
            ax,
            *,
            shapes_table="project_shapes",
            default_src_epsg=None,
            target_epsg=None,
            fill_alpha=0.20,
            line_alpha=0.95,
    ):

        with self._connect() as con:
            rows = con.execute(f"""
                SELECT
                    FullName,
                    FileName,
                    COALESCE(isFilled, 0) AS isFilled,
                    COALESCE(FillColor, '#000000') AS FillColor,
                    COALESCE(LineColor, '#000000') AS LineColor,
                    COALESCE(LineWidth, 1) AS LineWidth,
                    COALESCE(LineStyle, 'solid') AS LineStyle
                FROM {shapes_table}
                ORDER BY FileName, FullName
            """).fetchall()

        for r in rows:
            shp_path = r["FullName"]
            if not shp_path or not Path(shp_path).exists():
                continue

            gdf = gpd.read_file(shp_path)
            if gdf.empty:
                continue

            if gdf.crs is None and default_src_epsg is not None:
                gdf = gdf.set_crs(epsg=int(default_src_epsg))

            if target_epsg is not None and gdf.crs is not None:
                gdf = gdf.to_crs(epsg=int(target_epsg))

            try:
                gdf.plot(
                    ax=ax,
                    facecolor=r["FillColor"] if int(r["isFilled"]) == 1 else "none",
                    edgecolor=r["LineColor"],
                    linewidth=float(r["LineWidth"] or 1),
                    linestyle=r["LineStyle"] or "solid",
                    alpha=fill_alpha if int(r["isFilled"]) == 1 else line_alpha,
                    zorder=0,
                )
            except Exception:
                pass

    def draw_line_map_node_on_ax(
            self,
            ax,
            df,
            bbdata,
            cfg_row,
            node_row,
            line=None,

            station_col="Station",
            line_col="Line",
            rov_col="ROV",

            preplot_e_col="PreplotEasting",
            preplot_n_col="PreplotNorthing",

            dsr_primary_e_col="PrimaryEasting",
            dsr_primary_n_col="PrimaryNorthing",
            dsr_secondary_e_col="SecondaryEasting",
            dsr_secondary_n_col="SecondaryNorthing",

            bb_vessel_e_col="VesselEasting",
            bb_vessel_n_col="VesselNorthing",

            bb_rov1_ins_e_col="ROV1_INS_Easting",
            bb_rov1_ins_n_col="ROV1_INS_Northing",
            bb_rov1_usbl_e_col="ROV1_USBL_Easting",
            bb_rov1_usbl_n_col="ROV1_USBL_Northing",

            bb_rov2_ins_e_col="ROV2_INS_Easting",
            bb_rov2_ins_n_col="ROV2_INS_Northing",
            bb_rov2_usbl_e_col="ROV2_USBL_Easting",
            bb_rov2_usbl_n_col="ROV2_USBL_Northing",

            zoom_m=20.0,
            radius_m=5.0,

            show_preplot=True,
            show_radius=True,
            show_blackbox=True,
            show_vessel=True,
            show_dsr_deployment=True,
            show_station_primary_secondary=True,
            add_station_label=True,
            add_primary_secondary_distance=True,
            add_deploy_vertical=True,

            title_mode="station",
            legend=True,

            show_xlabel=True,
            show_ylabel=True,

            preplot_color="grey",
            radius_color="#4caf50",
            vessel_color="#1f77b4",
            deploy_color="#6a3d9a",
            station_primary_color="#1b9e77",
            station_secondary_color="#d95f02",

            bb_rov1_primary_color="#2ca02c",
            bb_rov1_secondary_color="#98df8a",
            bb_rov2_primary_color="#d62728",
            bb_rov2_secondary_color="#f28e8c",

            bb_linewidth=0.7,
            bb_marker_size=14,
            bb_linestyle_primary="--",
            bb_linestyle_secondary=":",
            vessel_linewidth=0.8,
            vessel_linestyle="-",

            dsr_deploy_marker_size=70,
            station_marker_size=46,
            center_marker_size=42,
            label_fontsize=11,
            tick_fontsize=8,
            title_fontsize=11,
    ):

        if df is None:
            df = pd.DataFrame()
        else:
            df = df.copy()

        if bbdata is None:
            bbdata = pd.DataFrame()
        else:
            bbdata = bbdata.copy()

        def _to_num(v):
            try:
                if pd.isna(v):
                    return np.nan
                return float(v)
            except Exception:
                return np.nan

        def _valid_xy(x, y):
            return pd.notna(x) and pd.notna(y) and np.isfinite(x) and np.isfinite(y)

        def _distance(x1, y1, x2, y2):
            return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)

        def _norm_name(v):
            if v is None:
                return ""
            return str(v).strip().upper()

        vessel_name = "Vessel"
        rov1_name = "ROV1"
        rov2_name = "ROV2"
        if cfg_row:
            vessel_name = str(cfg_row.get("Vessel_name") or "Vessel")
            rov1_name = str(cfg_row.get("rov1_name") or "ROV1")
            rov2_name = str(cfg_row.get("rov2_name") or "ROV2")

        station_value = node_row.get(station_col, "")
        line_value = line if line is not None else node_row.get(line_col, "")
        station_rov = str(node_row.get(rov_col, "")).strip()

        cx = _to_num(node_row.get(preplot_e_col))
        cy = _to_num(node_row.get(preplot_n_col))

        if not _valid_xy(cx, cy):
            cx = _to_num(node_row.get(dsr_primary_e_col))
            cy = _to_num(node_row.get(dsr_primary_n_col))

        if not _valid_xy(cx, cy):
            cx = _to_num(node_row.get(dsr_secondary_e_col))
            cy = _to_num(node_row.get(dsr_secondary_n_col))

        if not _valid_xy(cx, cy):
            ax.text(0.5, 0.5, "No valid coordinates", ha="center", va="center", transform=ax.transAxes)
            ax.set_axis_off()
            return ax

        for c in [
            preplot_e_col, preplot_n_col,
            dsr_primary_e_col, dsr_primary_n_col,
            dsr_secondary_e_col, dsr_secondary_n_col,
        ]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        for c in [
            bb_vessel_e_col, bb_vessel_n_col,
            bb_rov1_ins_e_col, bb_rov1_ins_n_col,
            bb_rov1_usbl_e_col, bb_rov1_usbl_n_col,
            bb_rov2_ins_e_col, bb_rov2_ins_n_col,
            bb_rov2_usbl_e_col, bb_rov2_usbl_n_col,
        ]:
            if c in bbdata.columns:
                bbdata[c] = pd.to_numeric(bbdata[c], errors="coerce")

        if show_preplot and {preplot_e_col, preplot_n_col}.issubset(df.columns):
            pp = df.loc[df[preplot_e_col].notna() & df[preplot_n_col].notna()]
            if not pp.empty:
                ax.scatter(
                    pp[preplot_e_col], pp[preplot_n_col],
                    s=16, c=preplot_color, alpha=0.70, marker="o",
                    label="Preplot", zorder=2,
                )

        if show_radius and radius_m and radius_m > 0:
            ax.add_patch(
                Circle(
                    (cx, cy), radius=radius_m, fill=False,
                    ec=radius_color, lw=1.3, zorder=3,
                    label=f"{int(radius_m) if float(radius_m).is_integer() else radius_m}m Radius",
                )
            )

        if show_blackbox and show_vessel and not bbdata.empty:
            if {bb_vessel_e_col, bb_vessel_n_col}.issubset(bbdata.columns):
                vessel = bbdata.loc[
                    bbdata[bb_vessel_e_col].notna() & bbdata[bb_vessel_n_col].notna()
                    ]
                if not vessel.empty:
                    ax.plot(
                        vessel[bb_vessel_e_col], vessel[bb_vessel_n_col],
                        color=vessel_color, lw=vessel_linewidth, ls=vessel_linestyle,
                        alpha=0.95, label=f"{vessel_name} Track", zorder=1,
                    )

        bb_primary_e = bb_primary_n = bb_secondary_e = bb_secondary_n = None
        bb_primary_label = bb_secondary_label = None
        bb_primary_color = bb_secondary_color = None

        station_rov_norm = _norm_name(station_rov)
        rov1_norm = _norm_name(rov1_name)
        rov2_norm = _norm_name(rov2_name)

        if station_rov_norm in {rov1_norm, "ROV1"}:
            bb_primary_e = bb_rov1_ins_e_col
            bb_primary_n = bb_rov1_ins_n_col
            bb_secondary_e = bb_rov1_usbl_e_col
            bb_secondary_n = bb_rov1_usbl_n_col
            bb_primary_label = f"{rov1_name} (Primary)"
            bb_secondary_label = f"{rov1_name} (Secondary)"
            bb_primary_color = bb_rov1_primary_color
            bb_secondary_color = bb_rov1_secondary_color

        elif station_rov_norm in {rov2_norm, "ROV2"}:
            bb_primary_e = bb_rov2_ins_e_col
            bb_primary_n = bb_rov2_ins_n_col
            bb_secondary_e = bb_rov2_usbl_e_col
            bb_secondary_n = bb_rov2_usbl_n_col
            bb_primary_label = f"{rov2_name} (Primary)"
            bb_secondary_label = f"{rov2_name} (Secondary)"
            bb_primary_color = bb_rov2_primary_color
            bb_secondary_color = bb_rov2_secondary_color

        if show_blackbox and not bbdata.empty and bb_primary_e and bb_primary_n:
            bbp = bbdata.loc[bbdata[bb_primary_e].notna() & bbdata[bb_primary_n].notna()]
            if not bbp.empty:
                ax.plot(
                    bbp[bb_primary_e], bbp[bb_primary_n],
                    color=bb_primary_color, lw=bb_linewidth, ls=bb_linestyle_primary,
                    alpha=0.85, zorder=4,
                )
                ax.scatter(
                    bbp[bb_primary_e], bbp[bb_primary_n],
                    s=bb_marker_size, c=bb_primary_color, marker="o",
                    label=bb_primary_label, zorder=5,
                )

        if show_blackbox and not bbdata.empty and bb_secondary_e and bb_secondary_n:
            bbs = bbdata.loc[bbdata[bb_secondary_e].notna() & bbdata[bb_secondary_n].notna()]
            if not bbs.empty:
                ax.plot(
                    bbs[bb_secondary_e], bbs[bb_secondary_n],
                    color=bb_secondary_color, lw=bb_linewidth, ls=bb_linestyle_secondary,
                    alpha=0.85, zorder=4,
                )
                ax.scatter(
                    bbs[bb_secondary_e], bbs[bb_secondary_n],
                    s=bb_marker_size, c=bb_secondary_color, marker="o",
                    label=bb_secondary_label, zorder=5,
                )

        px = _to_num(node_row.get(dsr_primary_e_col))
        py = _to_num(node_row.get(dsr_primary_n_col))
        sx = _to_num(node_row.get(dsr_secondary_e_col))
        sy = _to_num(node_row.get(dsr_secondary_n_col))

        if show_dsr_deployment and _valid_xy(px, py):
            ax.scatter(
                [px], [py],
                s=dsr_deploy_marker_size, c=deploy_color, marker="P",
                label="DSR Deployment", zorder=7,
            )
            if add_deploy_vertical:
                ax.axvline(px, color=deploy_color, lw=1.0, ls="--", alpha=0.6, zorder=1)

        if show_station_primary_secondary:
            if _valid_xy(px, py):
                ax.scatter(
                    [px], [py],
                    s=station_marker_size, c=station_primary_color, marker="^",
                    edgecolors="black", linewidths=0.5, zorder=8,
                )

            if _valid_xy(sx, sy):
                ax.scatter(
                    [sx], [sy],
                    s=station_marker_size, c=station_secondary_color, marker="s",
                    edgecolors="black", linewidths=0.5,
                    label="DSR Secondary" if legend else None,
                    zorder=8,
                )

            if _valid_xy(px, py) and _valid_xy(sx, sy):
                ax.plot([px, sx], [py, sy], color="black", lw=1.0, ls="-", zorder=7)

                if add_primary_secondary_distance:
                    dist = _distance(px, py, sx, sy)
                    mx = (px + sx) / 2.0
                    my = (py + sy) / 2.0
                    ax.text(
                        mx, my, f"{dist:.2f} m",
                        fontsize=8, ha="left", va="bottom", color="black",
                        bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.75),
                        zorder=9,
                    )

        ax.scatter([cx], [cy], s=center_marker_size, c="black", marker="+", linewidths=1.5, zorder=9)

        if add_station_label:
            ax.text(
                cx, cy, f"{station_value}",
                fontsize=label_fontsize, color=deploy_color,
                ha="center", va="bottom", zorder=10,
            )

        ax.set_xlim(cx - zoom_m, cx + zoom_m)
        ax.set_ylim(cy - zoom_m, cy + zoom_m)

        if title_mode == "full":
            ax.set_title(f"Line {line_value} | Station {station_value}", fontsize=title_fontsize)
        elif title_mode == "station":
            ax.set_title(f"Station {station_value}", fontsize=title_fontsize)

        if show_xlabel:
            ax.set_xlabel("Easting, m")
        else:
            ax.set_xlabel("")
            ax.tick_params(axis="x", labelbottom=False)

        if show_ylabel:
            ax.set_ylabel("Northing, m")
        else:
            ax.set_ylabel("")
            ax.tick_params(axis="y", labelleft=False)

        ax.set_aspect("equal", adjustable="box")
        ax.grid(True, alpha=0.25)

        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{x:.0f}"))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: f"{y:.0f}"))
        ax.tick_params(axis="both", labelsize=tick_fontsize)

        if legend:
            handles, labels = ax.get_legend_handles_labels()
            seen = set()
            hh, ll = [], []
            for h, l in zip(handles, labels):
                if not l or l == "_nolegend_" or l in seen:
                    continue
                seen.add(l)
                hh.append(h)
                ll.append(l)
            if hh:
                ax.legend(hh, ll, loc="upper right", fontsize=8, frameon=True)

        return ax

    def plot_one_line_map_page(
            self,
            df,
            bbdata,
            cfg_row,
            station_values,
            output_path,
            line=None,

            rows=3,
            cols=2,
            page_size="A4",
            orientation="portrait",
            dpi=180,

            station_col="Station",
            line_col="Line",

            page_title=None,
            show_page_legend=True,

            draw_kwargs=None,
    ):
        import matplotlib.pyplot as plt
        from matplotlib.lines import Line2D
        from matplotlib.patches import Patch

        if df is None or df.empty:
            raise ValueError("df is empty")

        draw_kwargs = draw_kwargs or {}

        if str(page_size).lower() == "letter":
            portrait = (8.5, 11.0)
            landscape = (11.0, 8.5)
        else:
            portrait = (8.27, 11.69)
            landscape = (11.69, 8.27)

        figsize = landscape if str(orientation).lower() == "landscape" else portrait
        fig, axes = plt.subplots(rows, cols, figsize=figsize)

        if hasattr(axes, "flatten"):
            axes = axes.flatten()
        else:
            axes = [axes]

        for i, ax in enumerate(axes):
            row_idx = i // cols
            col_idx = i % cols

            if i >= len(station_values):
                ax.set_axis_off()
                continue

            st = station_values[i]
            node_df = df.loc[df[station_col] == st]

            if node_df.empty:
                ax.text(0.5, 0.5, f"Station {st}\\nnot found", ha="center", va="center", transform=ax.transAxes)
                ax.set_axis_off()
                continue

            node_row = node_df.iloc[0]

            local_kwargs = dict(draw_kwargs)
            local_kwargs["legend"] = False
            local_kwargs["title_mode"] = "station"
            local_kwargs["show_ylabel"] = (col_idx == 0)
            local_kwargs["show_xlabel"] = (row_idx == rows - 1)

            self.draw_line_map_node_on_ax(
                ax=ax,
                df=df,
                bbdata=bbdata,
                cfg_row=cfg_row,
                node_row=node_row,
                line=line,
                station_col=station_col,
                line_col=line_col,
                **local_kwargs,
            )

        if page_title:
            fig.suptitle(page_title, fontsize=12, y=0.975)

        if show_page_legend:
            vessel_name = "Vessel"
            rov1_name = "ROV1"
            rov2_name = "ROV2"

            if cfg_row:
                vessel_name = str(cfg_row.get("Vessel_name") or "Vessel")
                rov1_name = str(cfg_row.get("rov1_name") or "ROV1")
                rov2_name = str(cfg_row.get("rov2_name") or "ROV2")

            legend_handles = [
                Line2D([0], [0], marker='o', color='grey', linestyle='None', markersize=5, label='Preplot'),
                Patch(fill=False, edgecolor='#4caf50', linewidth=1.3, label='Radius'),
                Line2D([0], [0], color='#1f77b4', linewidth=0.8, linestyle='-', label=f'{vessel_name} Track'),

                Line2D([0], [0], marker='o', color='#2ca02c', linewidth=0.7, linestyle='--', markersize=5,
                       label=f'{rov1_name} (Primary)'),
                Line2D([0], [0], marker='o', color='#98df8a', linewidth=0.7, linestyle=':', markersize=5,
                       label=f'{rov1_name} (Secondary)'),

                Line2D([0], [0], marker='o', color='#d62728', linewidth=0.7, linestyle='--', markersize=5,
                       label=f'{rov2_name} (Primary)'),
                Line2D([0], [0], marker='o', color='#f28e8c', linewidth=0.7, linestyle=':', markersize=5,
                       label=f'{rov2_name} (Secondary)'),

                Line2D([0], [0], marker='P', color='#6a3d9a', linestyle='None', markersize=7,
                       label='DSR Deployment'),
                Line2D([0], [0], marker='s', color='#d95f02', linestyle='None', markersize=6,
                       label='DSR Secondary'),
            ]

            fig.legend(
                handles=legend_handles,
                loc="upper center",
                bbox_to_anchor=(0.5, 0.935),
                ncol=4,
                fontsize=8,
                frameon=True,
            )

        fig.subplots_adjust(
            left=0.06,
            right=0.98,
            bottom=0.05,
            top=0.84,
            wspace=0.08,
            hspace=0.28,
        )

        fig.savefig(output_path, dpi=dpi, bbox_inches="tight")
        plt.close(fig)

        return output_path

    def plot_single_line_map_node(
            self,
            df,
            bbdata,
            cfg_row,
            station_value,
            output_path=None,
            figsize=(8, 6),
            dpi=180,
            **kwargs,
    ):

        station_col = kwargs.get("station_col", "Station")
        if df is None or len(df) == 0:
            raise ValueError("df is empty")

        node_df = df.loc[df[station_col] == station_value]
        if node_df.empty:
            raise ValueError(f"Station not found: {station_value}")

        node_row = node_df.iloc[0]

        fig, ax = plt.subplots(figsize=figsize)
        self.draw_line_map_node_on_ax(
            ax=ax,
            df=df,
            bbdata=bbdata,
            cfg_row=cfg_row,
            node_row=node_row,
            **kwargs,
        )
        fig.tight_layout()

        if output_path:
            fig.savefig(output_path, dpi=dpi, bbox_inches="tight")
            plt.close(fig)
            return output_path

        return fig

    def load_bbdata_for_line(self, line):
        conn = self._connect()

        sql = """
        SELECT
            bb.*,
            bf.ID as file_id,
            bf.Config_FK,
            cfg.Vessel_name,
            cfg.rov1_name,
            cfg.rov2_name
        FROM BlackBox bb
        LEFT JOIN BlackBox_Files bf ON bf.ID = bb.File_FK
        LEFT JOIN BBox_Configs_List cfg ON cfg.ID = bf.Config_FK
        WHERE bb.Line = ?
        ORDER BY bb.TimeStamp
        """

        df = pd.read_sql(sql, conn, params=[line])

        return df

    def get_bb_config_row(self, bbdata):
        if bbdata is None or bbdata.empty:
            return None

        row = bbdata.iloc[0]

        return {
            "Vessel_name": row.get("Vessel_name"),
            "rov1_name": row.get("rov1_name"),
            "rov2_name": row.get("rov2_name"),
            "gnss1_name": row.get("gnss1_name"),
            "gnss2_name": row.get("gnss2_name"),
            "Depth1_name": row.get("Depth1_name"),
            "Depth2_name": row.get("Depth2_name"),
        }

    def load_bbdata_for_dsr_line(
            self,
            line,
            dsr_table="DSR",
            line_col="Line",
            ts_start_col="TimeStamp",
            ts_end_col="TimeStamp1",
            pad_minutes=0,
    ):
        """
        Load BlackBox data for DSR line time window and return:
            bbdata, cfg_row, start_time, end_time
        """

        start_time, end_time = self.get_dsr_line_time_range(
            line=line,
            dsr_table=dsr_table,
            line_col=line_col,
            ts_start_col=ts_start_col,
            ts_end_col=ts_end_col,
        )

        if not start_time or not end_time:
            return pd.DataFrame(), None, start_time, end_time

        # optional time padding in SQL-friendly datetime strings
        if pad_minutes and str(pad_minutes).strip() != "0":
            conn = self._connect()
            sql = """
            SELECT
                datetime(?, ?),
                datetime(?, ?)
            """
            pad_minus = f"-{int(pad_minutes)} minutes"
            pad_plus = f"+{int(pad_minutes)} minutes"
            cur = conn.cursor()
            cur.execute(sql, [start_time, pad_minus, end_time, pad_plus])
            r = cur.fetchone()
            start_time = r[0]
            end_time = r[1]

        bbdata = self.load_bbdata_by_time_range(start_time, end_time, with_config=True)
        cfg_row = self.get_cfg_row_from_bbdata(bbdata)

        return bbdata, cfg_row, start_time, end_time

    def get_dsr_line_time_range(
            self,
            line,
            dsr_table="DSR",
            line_col="Line",
            ts_start_col="TimeStamp",
            ts_end_col="TimeStamp1",
    ):
        """
        Return (start_time, end_time) for one DSR line.

        Logic:
        - start_time = MIN(TimeStamp)
        - end_time   = MAX(TimeStamp1) if exists, otherwise MAX(TimeStamp)
        """

        conn = self._connect()
        cur = conn.cursor()

        sql = f"""
            SELECT
                MIN({ts_start_col}) AS start_time,
                MAX(COALESCE({ts_end_col}, {ts_start_col})) AS end_time
            FROM {dsr_table}
            WHERE {line_col} = ?
        """
        cur.execute(sql, [line])
        row = cur.fetchone()

        start_time = row[0] if row else None
        end_time = row[1] if row else None

        return start_time, end_time

    def load_bbdata_by_time_range(
            self,
            start_time,
            end_time,
            with_config=True,
    ):
        """
        Load BlackBox data for time range.
        """

        if not start_time or not end_time:
            return pd.DataFrame()

        conn = self._connect()

        if with_config:
            sql = """
            SELECT
                bb.*,
                bf.ID AS file_id,
                bf.Config_FK,
                cfg.ID AS cfg_id,
                cfg.Name AS config_name,
                cfg.Vessel_name,
                cfg.rov1_name,
                cfg.rov2_name,
                cfg.gnss1_name,
                cfg.gnss2_name,
                cfg.Depth1_name,
                cfg.Depth2_name
            FROM BlackBox bb
            LEFT JOIN BlackBox_Files bf
                ON bf.ID = bb.File_FK
            LEFT JOIN BBox_Configs_List cfg
                ON cfg.ID = bf.Config_FK
            WHERE bb.TimeStamp BETWEEN ? AND ?
            ORDER BY bb.TimeStamp
            """
        else:
            sql = """
            SELECT
                bb.*
            FROM BlackBox bb
            WHERE bb.TimeStamp BETWEEN ? AND ?
            ORDER BY bb.TimeStamp
            """

        df = pd.read_sql(sql, conn, params=[start_time, end_time])
        return df

    def get_cfg_row_from_bbdata(self, bbdata, prefer_config_fk=None):
        """
        Return one config row as dict from loaded bbdata.
        """

        if bbdata is None or bbdata.empty:
            return None

        work = bbdata.copy()

        if prefer_config_fk is not None and "Config_FK" in work.columns:
            tmp = work.loc[work["Config_FK"] == prefer_config_fk]
            if not tmp.empty:
                work = tmp

        row = work.iloc[0]

        return {
            "Vessel_name": row.get("Vessel_name"),
            "rov1_name": row.get("rov1_name"),
            "rov2_name": row.get("rov2_name"),
            "gnss1_name": row.get("gnss1_name"),
            "gnss2_name": row.get("gnss2_name"),
            "Depth1_name": row.get("Depth1_name"),
            "Depth2_name": row.get("Depth2_name"),
        }

    def plot_line_map_nodes_page(
            self,
            df,
            bbdata,
            cfg_row,
            station_values,
            output_path,
            line=None,

            rows=3,
            cols=2,
            page_size="A4",  # "A4" or "Letter"
            orientation="portrait",  # "portrait" or "landscape"
            dpi=180,

            page_title=None,
            show_page_legend=True,

            station_col="Station",
            line_col="Line",

            subplot_kwargs=None,
    ):
        """
        Save one page with multiple node maps.

        station_values : list of station values for this page
        subplot_kwargs : kwargs passed to draw_line_map_node_on_ax()
        """

        subplot_kwargs = subplot_kwargs or {}

        if df is None or df.empty:
            raise ValueError("df is empty")

        # -----------------------------
        # page size
        # -----------------------------
        if str(page_size).lower() == "letter":
            portrait = (8.5, 11.0)
            landscape = (11.0, 8.5)
        else:
            portrait = (8.27, 11.69)
            landscape = (11.69, 8.27)

        figsize = landscape if str(orientation).lower() == "landscape" else portrait

        fig, axes = plt.subplots(rows, cols, figsize=figsize)
        if hasattr(axes, "flatten"):
            axes = axes.flatten()
        else:
            axes = [axes]

        # -----------------------------
        # draw subplots
        # -----------------------------
        for i, ax in enumerate(axes):
            if i >= len(station_values):
                ax.set_axis_off()
                continue

            st = station_values[i]
            node_df = df.loc[df[station_col] == st]
            if node_df.empty:
                ax.text(0.5, 0.5, f"Station {st}\nnot found", ha="center", va="center", transform=ax.transAxes)
                ax.set_axis_off()
                continue

            node_row = node_df.iloc[0]

            local_kwargs = dict(subplot_kwargs)
            local_kwargs["legend"] = False
            local_kwargs["title_mode"] = "station"

            self.draw_line_map_node_on_ax(
                ax=ax,
                df=df,
                bbdata=bbdata,
                cfg_row=cfg_row,
                node_row=node_row,
                line=line,
                station_col=station_col,
                line_col=line_col,
                **local_kwargs,
            )

        # -----------------------------
        # page title
        # -----------------------------
        if page_title:
            fig.suptitle(page_title, fontsize=12, y=0.985)

        # -----------------------------
        # one common legend for page
        # -----------------------------
        if show_page_legend:
            vessel_name = "Vessel"
            rov1_name = "ROV1"
            rov2_name = "ROV2"

            if cfg_row:
                vessel_name = str(cfg_row.get("Vessel_name") or "Vessel")
                rov1_name = str(cfg_row.get("rov1_name") or "ROV1")
                rov2_name = str(cfg_row.get("rov2_name") or "ROV2")

            legend_handles = []

            # --- base layers ---
            legend_handles += [
                Line2D([0], [0], marker='o', color='grey', linestyle='None', markersize=5, label='Preplot'),
                Patch(fill=False, edgecolor='#4caf50', linewidth=1.3, label='Radius'),
                Line2D([0], [0], color='#1f77b4', linewidth=1.1, label=f'{vessel_name} Track'),
            ]

            # ---------------------------------------------------
            # detect ALL ROVs from bbdata
            # ---------------------------------------------------
            rov_names = set()

            if bbdata is not None and not bbdata.empty:
                # если есть имена в config
                if rov1_name:
                    rov_names.add(rov1_name)
                if rov2_name:
                    rov_names.add(rov2_name)

                # если вдруг есть колонка ROV в данных
                if "ROV" in bbdata.columns:
                    rov_names.update(bbdata["ROV"].dropna().astype(str).unique())

            rov_names = sorted(list(rov_names))

            # ---------------------------------------------------
            # colors (должны совпадать с draw function!)
            # ---------------------------------------------------
            primary_colors = {
                rov1_name: "#2ca02c",  # green
                rov2_name: "#d62728",  # red
            }

            secondary_colors = {
                rov1_name: "#98df8a",
                rov2_name: "#ff9896",
            }

            # fallback если больше ROV
            fallback_primary = "#d62728"
            fallback_secondary = "#f28e8c"

            # ---------------------------------------------------
            # add each ROV to legend
            # ---------------------------------------------------
            for rov in rov_names:
                p_col = primary_colors.get(rov, fallback_primary)
                s_col = secondary_colors.get(rov, fallback_secondary)

                legend_handles.append(
                    Line2D([0], [0], marker='o', color=p_col, linestyle='-', markersize=5,
                           label=f"{rov} (Primary)")
                )

                legend_handles.append(
                    Line2D([0], [0], marker='o', color=s_col, linestyle='-', markersize=5,
                           label=f"{rov} (Secondary)")
                )

            # ---------------------------------------------------
            # DSR
            # ---------------------------------------------------
            legend_handles += [
                Line2D([0], [0], marker='P', color='#6a3d9a', linestyle='None', markersize=7,
                       label='DSR Deployment'),
                Line2D([0], [0], marker='s', color='#d95f02', linestyle='None', markersize=6,
                       label='DSR Secondary'),
            ]

            fig.legend(
                handles=legend_handles,
                loc="upper center",
                bbox_to_anchor=(0.5, 0.94),
                ncol=4,
                fontsize=8,
                frameon=True,
            )

        top_rect = 0.90 if show_page_legend else 0.96
        if page_title:
            top_rect = min(top_rect, 0.93 if show_page_legend else 0.95)

        fig.tight_layout(rect=[0.03, 0.03, 0.97, top_rect])
        fig.savefig(output_path, dpi=dpi, bbox_inches="tight")
        plt.close(fig)
        return output_path

    def plot_all_line_map_node_pages(
            self,
            df,
            bbdata,
            cfg_row,
            output_dir,
            line=None,

            rows=3,
            cols=2,
            page_size="A4",
            orientation="portrait",
            dpi=180,

            station_col="Station",
            line_col="Line",

            page_prefix=None,
            subplot_kwargs=None,
    ):
        """
        Save all stations of one line into paged PNGs.
        """
        subplot_kwargs = subplot_kwargs or {}

        if df is None or df.empty:
            return []

        os.makedirs(output_dir, exist_ok=True)

        work_df = df.copy()
        try:
            work_df = work_df.sort_values(station_col).reset_index(drop=True)
        except Exception:
            work_df = work_df.reset_index(drop=True)

        if line is None:
            if line_col in work_df.columns and not work_df.empty:
                line = work_df.iloc[0][line_col]
            else:
                line = "line"

        stations = work_df[station_col].dropna().tolist()
        # unique keep order
        seen = set()
        stations = [x for x in stations if not (x in seen or seen.add(x))]

        per_page = rows * cols
        saved_pages = []

        if not page_prefix:
            page_prefix = f"{line}_node_maps"

        for page_no, start in enumerate(range(0, len(stations), per_page), start=1):
            station_values = stations[start:start + per_page]

            output_path = os.path.join(
                output_dir,
                f"{page_prefix}_page_{page_no:03d}.png"
            )

            self.plot_line_map_nodes_page(
                df=work_df,
                bbdata=bbdata,
                cfg_row=cfg_row,
                station_values=station_values,
                output_path=output_path,
                line=line,
                rows=rows,
                cols=cols,
                page_size=page_size,
                orientation=orientation,
                dpi=dpi,
                page_title=f"Line {line} | Node Maps | Page {page_no}",
                show_page_legend=True,
                station_col=station_col,
                line_col=line_col,
                subplot_kwargs=subplot_kwargs,
            )
            saved_pages.append(output_path)

        return saved_pages


    def plot_whole_line_node_pages(
            self,
            line,
            output_dir,
            rows=3,
            cols=2,
            page_size="A4",
            orientation="portrait",
            dpi=180,
            pad_minutes=10,
            draw_kwargs=None,
    ):
        import os

        draw_kwargs = draw_kwargs or {}

        df = self.read_dsr_for_line(line)
        bbdata, cfg_row, start_time, end_time = self.load_bbdata_for_dsr_line(
            line=line,
            pad_minutes=pad_minutes,
        )

        if df is None or df.empty:
            return []

        os.makedirs(output_dir, exist_ok=True)

        stations = (
            df.sort_values("Station")["Station"]
            .dropna()
            .unique()
            .tolist()
        )

        per_page = rows * cols
        saved = []

        for page_no, start in enumerate(range(0, len(stations), per_page), start=1):
            station_values = stations[start:start + per_page]
            out_path = os.path.join(output_dir, f"line_{line}_page_{page_no:03d}.png")

            self.plot_one_line_map_page(
                df=df,
                bbdata=bbdata,
                cfg_row=cfg_row,
                station_values=station_values,
                output_path=out_path,
                line=line,
                rows=rows,
                cols=cols,
                page_size=page_size,
                orientation=orientation,
                dpi=dpi,
                page_title=f"Line {line} | Node Maps | Page {page_no}",
                draw_kwargs=draw_kwargs,
            )
            saved.append(out_path)

        return saved

    def prepare_line_map_cache(
            self,
            df,
            bbdata,
            cfg_row,
            station_col="Station",
            rov_col="ROV",
            preplot_e_col="PreplotEasting",
            preplot_n_col="PreplotNorthing",
            dsr_primary_e_col="PrimaryEasting",
            dsr_primary_n_col="PrimaryNorthing",
            dsr_secondary_e_col="SecondaryEasting",
            dsr_secondary_n_col="SecondaryNorthing",
            bb_vessel_e_col="VesselEasting",
            bb_vessel_n_col="VesselNorthing",
            bb_rov1_ins_e_col="ROV1_INS_Easting",
            bb_rov1_ins_n_col="ROV1_INS_Northing",
            bb_rov1_usbl_e_col="ROV1_USBL_Easting",
            bb_rov1_usbl_n_col="ROV1_USBL_Northing",
            bb_rov2_ins_e_col="ROV2_INS_Easting",
            bb_rov2_ins_n_col="ROV2_INS_Northing",
            bb_rov2_usbl_e_col="ROV2_USBL_Easting",
            bb_rov2_usbl_n_col="ROV2_USBL_Northing",
            bb_stride=1,
    ):

        df = df.copy()
        bbdata = bbdata.copy() if bbdata is not None else pd.DataFrame()

        df_num_cols = [
            preplot_e_col, preplot_n_col,
            dsr_primary_e_col, dsr_primary_n_col,
            dsr_secondary_e_col, dsr_secondary_n_col,
            station_col,
        ]
        for c in df_num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        if rov_col in df.columns:
            df[rov_col] = df[rov_col].fillna("").astype(str).str.strip()
        else:
            df[rov_col] = ""

        bb_num_cols = [
            bb_vessel_e_col, bb_vessel_n_col,
            bb_rov1_ins_e_col, bb_rov1_ins_n_col,
            bb_rov1_usbl_e_col, bb_rov1_usbl_n_col,
            bb_rov2_ins_e_col, bb_rov2_ins_n_col,
            bb_rov2_usbl_e_col, bb_rov2_usbl_n_col,
        ]
        for c in bb_num_cols:
            if c in bbdata.columns:
                bbdata[c] = pd.to_numeric(bbdata[c], errors="coerce")

        if bb_stride and int(bb_stride) > 1 and not bbdata.empty:
            bbdata = bbdata.iloc[::int(bb_stride)].copy()

        vessel_name = "Vessel"
        rov1_name = "ROV1"
        rov2_name = "ROV2"
        if cfg_row:
            vessel_name = str(cfg_row.get("Vessel_name") or "Vessel")
            rov1_name = str(cfg_row.get("rov1_name") or "ROV1")
            rov2_name = str(cfg_row.get("rov2_name") or "ROV2")

        station_to_index = {}
        if station_col in df.columns:
            for i, st in enumerate(df[station_col].tolist()):
                if pd.notna(st) and st not in station_to_index:
                    station_to_index[st] = i

        def _safe_xy(frame, x_col, y_col):
            if frame is None or frame.empty:
                return pd.DataFrame(columns=[x_col, y_col])
            if x_col not in frame.columns or y_col not in frame.columns:
                return pd.DataFrame(columns=[x_col, y_col])
            return frame.loc[frame[x_col].notna() & frame[y_col].notna(), [x_col, y_col]].copy()

        cache = {
            "df": df,
            "bbdata": bbdata,
            "cfg_row": cfg_row,
            "station_col": station_col,
            "line_col": "Line",
            "rov_col": rov_col,
            "preplot_e_col": preplot_e_col,
            "preplot_n_col": preplot_n_col,
            "dsr_primary_e_col": dsr_primary_e_col,
            "dsr_primary_n_col": dsr_primary_n_col,
            "dsr_secondary_e_col": dsr_secondary_e_col,
            "dsr_secondary_n_col": dsr_secondary_n_col,
            "bb_vessel_e_col": bb_vessel_e_col,
            "bb_vessel_n_col": bb_vessel_n_col,
            "bb_rov1_ins_e_col": bb_rov1_ins_e_col,
            "bb_rov1_ins_n_col": bb_rov1_ins_n_col,
            "bb_rov1_usbl_e_col": bb_rov1_usbl_e_col,
            "bb_rov1_usbl_n_col": bb_rov1_usbl_n_col,
            "bb_rov2_ins_e_col": bb_rov2_ins_e_col,
            "bb_rov2_ins_n_col": bb_rov2_ins_n_col,
            "bb_rov2_usbl_e_col": bb_rov2_usbl_e_col,
            "bb_rov2_usbl_n_col": bb_rov2_usbl_n_col,
            "vessel_name": vessel_name,
            "rov1_name": rov1_name,
            "rov2_name": rov2_name,
            "station_to_index": station_to_index,
            "preplot_xy": _safe_xy(df, preplot_e_col, preplot_n_col),
            "vessel_xy": _safe_xy(bbdata, bb_vessel_e_col, bb_vessel_n_col),
            "rov1_primary_xy": _safe_xy(bbdata, bb_rov1_ins_e_col, bb_rov1_ins_n_col),
            "rov1_secondary_xy": _safe_xy(bbdata, bb_rov1_usbl_e_col, bb_rov1_usbl_n_col),
            "rov2_primary_xy": _safe_xy(bbdata, bb_rov2_ins_e_col, bb_rov2_ins_n_col),
            "rov2_secondary_xy": _safe_xy(bbdata, bb_rov2_usbl_e_col, bb_rov2_usbl_n_col),
        }

        return cache

    def draw_line_map_node_on_ax_fast(
            self,
            ax,
            cache,
            node_row,
            line=None,

            zoom_m=20.0,
            radius_m=5.0,

            show_preplot=True,
            show_radius=True,
            show_blackbox=True,
            show_vessel=True,
            show_dsr_deployment=True,
            show_station_primary_secondary=True,
            add_station_label=True,
            add_primary_secondary_distance=True,
            add_deploy_vertical=True,

            title_mode="station",
            legend=False,

            show_xlabel=True,
            show_ylabel=True,

            preplot_color="grey",
            radius_color="#4caf50",
            vessel_color="#1f77b4",
            deploy_color="#6a3d9a",
            station_primary_color="#1b9e77",
            station_secondary_color="#d95f02",

            bb_rov1_primary_color="#2ca02c",
            bb_rov1_secondary_color="#98df8a",
            bb_rov2_primary_color="#d62728",
            bb_rov2_secondary_color="#f28e8c",

            bb_linewidth=0.7,
            bb_marker_size=14,
            bb_linestyle_primary="--",
            bb_linestyle_secondary=":",
            vessel_linewidth=0.8,
            vessel_linestyle="-",

            dsr_deploy_marker_size=70,
            station_marker_size=46,
            center_marker_size=42,
            label_fontsize=11,
            tick_fontsize=8,
            title_fontsize=11,
    ):

        def _to_num(v):
            try:
                if pd.isna(v):
                    return np.nan
                return float(v)
            except Exception:
                return np.nan

        def _valid_xy(x, y):
            return pd.notna(x) and pd.notna(y) and np.isfinite(x) and np.isfinite(y)

        def _distance(x1, y1, x2, y2):
            return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)

        def _norm_name(v):
            if v is None:
                return ""
            return str(v).strip().upper()

        df = cache["df"]
        station_col = cache["station_col"]
        rov_col = cache["rov_col"]

        preplot_e_col = cache["preplot_e_col"]
        preplot_n_col = cache["preplot_n_col"]
        dsr_primary_e_col = cache["dsr_primary_e_col"]
        dsr_primary_n_col = cache["dsr_primary_n_col"]
        dsr_secondary_e_col = cache["dsr_secondary_e_col"]
        dsr_secondary_n_col = cache["dsr_secondary_n_col"]

        vessel_name = cache["vessel_name"]
        rov1_name = cache["rov1_name"]
        rov2_name = cache["rov2_name"]

        station_value = node_row.get(station_col, "")
        line_value = line if line is not None else node_row.get("Line", "")
        station_rov = str(node_row.get(rov_col, "")).strip()

        cx = _to_num(node_row.get(preplot_e_col))
        cy = _to_num(node_row.get(preplot_n_col))
        if not _valid_xy(cx, cy):
            cx = _to_num(node_row.get(dsr_primary_e_col))
            cy = _to_num(node_row.get(dsr_primary_n_col))
        if not _valid_xy(cx, cy):
            cx = _to_num(node_row.get(dsr_secondary_e_col))
            cy = _to_num(node_row.get(dsr_secondary_n_col))

        if not _valid_xy(cx, cy):
            ax.text(0.5, 0.5, "No valid coordinates", ha="center", va="center", transform=ax.transAxes)
            ax.set_axis_off()
            return ax

        if show_preplot and not cache["preplot_xy"].empty:
            ax.scatter(
                cache["preplot_xy"][preplot_e_col],
                cache["preplot_xy"][preplot_n_col],
                s=16,
                c=preplot_color,
                alpha=0.70,
                marker="o",
                label="Preplot",
                zorder=2,
            )

        if show_radius and radius_m and radius_m > 0:
            ax.add_patch(
                Circle(
                    (cx, cy),
                    radius=radius_m,
                    fill=False,
                    ec=radius_color,
                    lw=1.3,
                    zorder=3,
                    label=f"{int(radius_m) if float(radius_m).is_integer() else radius_m}m Radius",
                )
            )

        if show_blackbox and show_vessel and not cache["vessel_xy"].empty:
            vx = cache["vessel_xy"][cache["bb_vessel_e_col"]]
            vy = cache["vessel_xy"][cache["bb_vessel_n_col"]]
            ax.plot(
                vx, vy,
                color=vessel_color,
                lw=vessel_linewidth,
                ls=vessel_linestyle,
                alpha=0.95,
                label=f"{vessel_name} Track",
                zorder=1,
            )

        bb_primary_xy = None
        bb_secondary_xy = None
        bb_primary_label = None
        bb_secondary_label = None
        bb_primary_color = None
        bb_secondary_color = None

        station_rov_norm = _norm_name(station_rov)
        rov1_norm = _norm_name(rov1_name)
        rov2_norm = _norm_name(rov2_name)

        if station_rov_norm in {rov1_norm, "ROV1"}:
            bb_primary_xy = cache["rov1_primary_xy"]
            bb_secondary_xy = cache["rov1_secondary_xy"]
            bb_primary_label = f"{rov1_name} (Primary)"
            bb_secondary_label = f"{rov1_name} (Secondary)"
            bb_primary_color = bb_rov1_primary_color
            bb_secondary_color = bb_rov1_secondary_color
        elif station_rov_norm in {rov2_norm, "ROV2"}:
            bb_primary_xy = cache["rov2_primary_xy"]
            bb_secondary_xy = cache["rov2_secondary_xy"]
            bb_primary_label = f"{rov2_name} (Primary)"
            bb_secondary_label = f"{rov2_name} (Secondary)"
            bb_primary_color = bb_rov2_primary_color
            bb_secondary_color = bb_rov2_secondary_color

        if show_blackbox and bb_primary_xy is not None and not bb_primary_xy.empty:
            xcol = bb_primary_xy.columns[0]
            ycol = bb_primary_xy.columns[1]
            ax.plot(
                bb_primary_xy[xcol],
                bb_primary_xy[ycol],
                color=bb_primary_color,
                lw=bb_linewidth,
                ls=bb_linestyle_primary,
                alpha=0.85,
                zorder=4,
            )
            ax.scatter(
                bb_primary_xy[xcol],
                bb_primary_xy[ycol],
                s=bb_marker_size,
                c=bb_primary_color,
                marker="o",
                label=bb_primary_label,
                zorder=5,
            )

        if show_blackbox and bb_secondary_xy is not None and not bb_secondary_xy.empty:
            xcol = bb_secondary_xy.columns[0]
            ycol = bb_secondary_xy.columns[1]
            ax.plot(
                bb_secondary_xy[xcol],
                bb_secondary_xy[ycol],
                color=bb_secondary_color,
                lw=bb_linewidth,
                ls=bb_linestyle_secondary,
                alpha=0.85,
                zorder=4,
            )
            ax.scatter(
                bb_secondary_xy[xcol],
                bb_secondary_xy[ycol],
                s=bb_marker_size,
                c=bb_secondary_color,
                marker="o",
                label=bb_secondary_label,
                zorder=5,
            )

        px = _to_num(node_row.get(dsr_primary_e_col))
        py = _to_num(node_row.get(dsr_primary_n_col))
        sx = _to_num(node_row.get(dsr_secondary_e_col))
        sy = _to_num(node_row.get(dsr_secondary_n_col))

        if show_dsr_deployment and _valid_xy(px, py):
            ax.scatter(
                [px], [py],
                s=dsr_deploy_marker_size,
                c=deploy_color,
                marker="P",
                label="DSR Deployment",
                zorder=7,
            )
            if add_deploy_vertical:
                ax.axvline(px, color=deploy_color, lw=1.0, ls="--", alpha=0.6, zorder=1)

        if show_station_primary_secondary:
            if _valid_xy(px, py):
                ax.scatter(
                    [px], [py],
                    s=station_marker_size,
                    c=station_primary_color,
                    marker="^",
                    edgecolors="black",
                    linewidths=0.5,
                    zorder=8,
                )

            if _valid_xy(sx, sy):
                ax.scatter(
                    [sx], [sy],
                    s=station_marker_size,
                    c=station_secondary_color,
                    marker="s",
                    edgecolors="black",
                    linewidths=0.5,
                    label="DSR Secondary" if legend else None,
                    zorder=8,
                )

            if _valid_xy(px, py) and _valid_xy(sx, sy):
                ax.plot([px, sx], [py, sy], color="black", lw=1.0, ls="-", zorder=7)
                if add_primary_secondary_distance:
                    dist = _distance(px, py, sx, sy)
                    mx = (px + sx) / 2.0
                    my = (py + sy) / 2.0
                    ax.text(
                        mx, my, f"{dist:.2f} m",
                        fontsize=8,
                        ha="left",
                        va="bottom",
                        color="black",
                        bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.75),
                        zorder=9,
                    )

        ax.scatter([cx], [cy], s=center_marker_size, c="black", marker="+", linewidths=1.5, zorder=9)

        if add_station_label:
            ax.text(
                cx, cy, f"{station_value}",
                fontsize=label_fontsize,
                color=deploy_color,
                ha="center",
                va="bottom",
                zorder=10,
            )

        ax.set_xlim(cx - zoom_m, cx + zoom_m)
        ax.set_ylim(cy - zoom_m, cy + zoom_m)

        if title_mode == "full":
            ax.set_title(f"Line {line_value} | Station {station_value}", fontsize=title_fontsize)
        elif title_mode == "station":
            ax.set_title(f"Station {station_value}", fontsize=title_fontsize)

        if show_xlabel:
            ax.set_xlabel("Easting, m")
        else:
            ax.set_xlabel("")
            ax.tick_params(axis="x", labelbottom=False)

        if show_ylabel:
            ax.set_ylabel("Northing, m")
        else:
            ax.set_ylabel("")
            ax.tick_params(axis="y", labelleft=False)

        ax.set_aspect("equal", adjustable="box")
        ax.grid(True, alpha=0.25)
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{x:.0f}"))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: f"{y:.0f}"))
        ax.tick_params(axis="both", labelsize=tick_fontsize)

        if legend:
            handles, labels = ax.get_legend_handles_labels()
            seen = set()
            hh, ll = [], []
            for h, l in zip(handles, labels):
                if not l or l == "_nolegend_" or l in seen:
                    continue
                seen.add(l)
                hh.append(h)
                ll.append(l)
            if hh:
                ax.legend(hh, ll, loc="upper right", fontsize=8, frameon=True)

        return ax

    def plot_one_line_map_page_fast(
            self,
            cache,
            station_values,
            output_path,
            line=None,

            rows=3,
            cols=2,
            page_size="A4",
            orientation="portrait",
            dpi=180,

            page_title=None,
            show_page_legend=True,

            draw_kwargs=None,
    ):

        draw_kwargs = draw_kwargs or {}

        df = cache["df"]
        cfg_row = cache["cfg_row"]

        if str(page_size).lower() == "letter":
            portrait = (8.5, 11.0)
            landscape = (11.0, 8.5)
        else:
            portrait = (8.27, 11.69)
            landscape = (11.69, 8.27)

        figsize = landscape if str(orientation).lower() == "landscape" else portrait
        fig, axes = plt.subplots(rows, cols, figsize=figsize)

        if hasattr(axes, "flatten"):
            axes = axes.flatten()
        else:
            axes = [axes]

        station_map = cache.get("station_to_index", {})
        station_col = cache["station_col"]

        for i, ax in enumerate(axes):
            row_idx = i // cols
            col_idx = i % cols

            if i >= len(station_values):
                ax.set_axis_off()
                continue

            st = station_values[i]
            if st not in station_map:
                ax.text(0.5, 0.5, f"Station {st}\nnot found", ha="center", va="center", transform=ax.transAxes)
                ax.set_axis_off()
                continue

            node_row = df.iloc[station_map[st]]

            local_kwargs = dict(draw_kwargs)
            local_kwargs["legend"] = False
            local_kwargs["title_mode"] = "station"
            local_kwargs["show_ylabel"] = (col_idx == 0)
            local_kwargs["show_xlabel"] = (row_idx == rows - 1)

            self.draw_line_map_node_on_ax_fast(
                ax=ax,
                cache=cache,
                node_row=node_row,
                line=line,
                **local_kwargs,
            )

        if page_title:
            fig.suptitle(page_title, fontsize=12, y=0.975)

        if show_page_legend:
            vessel_name = cache["vessel_name"]
            rov1_name = cache["rov1_name"]
            rov2_name = cache["rov2_name"]

            legend_handles = [
                Line2D([0], [0], marker='o', color='grey', linestyle='None', markersize=5, label='Preplot'),
                Patch(fill=False, edgecolor='#4caf50', linewidth=1.3, label='Radius'),
                Line2D([0], [0], color='#1f77b4', linewidth=0.8, linestyle='-', label=f'{vessel_name} Track'),

                Line2D([0], [0], marker='o', color='#2ca02c', linewidth=0.7, linestyle='--', markersize=5,
                       label=f'{rov1_name} (Primary)'),
                Line2D([0], [0], marker='o', color='#98df8a', linewidth=0.7, linestyle=':', markersize=5,
                       label=f'{rov1_name} (Secondary)'),

                Line2D([0], [0], marker='o', color='#d62728', linewidth=0.7, linestyle='--', markersize=5,
                       label=f'{rov2_name} (Primary)'),
                Line2D([0], [0], marker='o', color='#f28e8c', linewidth=0.7, linestyle=':', markersize=5,
                       label=f'{rov2_name} (Secondary)'),

                Line2D([0], [0], marker='P', color='#6a3d9a', linestyle='None', markersize=7,
                       label='DSR Deployment'),
                Line2D([0], [0], marker='s', color='#d95f02', linestyle='None', markersize=6,
                       label='DSR Secondary'),
            ]

            fig.legend(
                handles=legend_handles,
                loc="upper center",
                bbox_to_anchor=(0.5, 0.935),
                ncol=4,
                fontsize=8,
                frameon=True,
            )

        fig.subplots_adjust(
            left=0.06,
            right=0.98,
            bottom=0.05,
            top=0.84,
            wspace=0.08,
            hspace=0.28,
        )

        fig.savefig(output_path, dpi=dpi, bbox_inches="tight")
        plt.close(fig)

        return output_path

    def plot_whole_line_node_pages_fast(
            self,
            line,
            output_dir,
            rows=3,
            cols=2,
            page_size="A4",
            orientation="portrait",
            dpi=180,
            pad_minutes=10,
            draw_kwargs=None,
            bb_stride=1,
    ):

        draw_kwargs = draw_kwargs or {}

        df = self.read_dsr_for_line(line)
        bbdata, cfg_row, start_time, end_time = self.load_bbdata_for_dsr_line(
            line=line,
            pad_minutes=pad_minutes,
        )

        if df is None or df.empty:
            return []

        cache = self.prepare_line_map_cache(
            df=df,
            bbdata=bbdata,
            cfg_row=cfg_row,
            bb_stride=bb_stride,
        )

        os.makedirs(output_dir, exist_ok=True)

        stations = (
            cache["df"].sort_values("Station")["Station"]
            .dropna()
            .unique()
            .tolist()
        )

        per_page = rows * cols
        saved = []

        for page_no, start in enumerate(range(0, len(stations), per_page), start=1):
            station_values = stations[start:start + per_page]
            out_path = os.path.join(output_dir, f"line_{line}_page_{page_no:03d}.png")

            self.plot_one_line_map_page_fast(
                cache=cache,
                station_values=station_values,
                output_path=out_path,
                line=line,
                rows=rows,
                cols=cols,
                page_size=page_size,
                orientation=orientation,
                dpi=dpi,
                page_title=f"Line {line} | Node Maps | Page {page_no}",
                draw_kwargs=draw_kwargs,
            )
            saved.append(out_path)

        return saved

    def plot_bbox_gnss_qc_timeseries(
            self,
            bbdata,
            dsr_df=None,
            *,
            title="GNSS QC",
            gnss1_label=None,
            gnss2_label=None,
            diff_good_max=19.9,
            diff_warn_max=29.9,
            time_start=None,
            time_end=None,
            station_label_step=20,
            station_label_fontsize=7,
            station_label_rotation=90,
            station_label_alpha=0.85,
            station_label_y_pad_ratio=0.03,
            max_station_lines=80,
            bb_stride=20,
            satellites_ylim=(0, 40),
            diffage_ylim=(0, 40),
            hdop_ylim=(0, 3),
            figsize=(18, 10),
            save_dir=".",
            file_name=None,
            suffix="bbox_gnss_qc",
            ext="png",
            dpi=200,
            is_show=False,
            close=False,
    ):
        """
        Fast matplotlib GNSS QC plot for one time window:
          1) Number of Satellites
          2) DiffAge
          3) HDOP

        DSR stations are shown as thin vertical lines only.
        GNSS legend names come from config labels if provided.
        """

        if bbdata is None or len(bbdata) == 0:
            fig = self.empty_figure(title=title, message="No BlackBox data")
            out_path = self._build_output_path(
                dsr_df if dsr_df is not None else bbdata, save_dir, suffix, file_name, ext
            )
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        df = bbdata.copy()
        df = df.loc[:, ~df.columns.duplicated()]

        if "T" in df.columns:
            df["T"] = pd.to_datetime(df["T"], errors="coerce")
        elif "TimeStamp" in df.columns:
            df["T"] = pd.to_datetime(df["TimeStamp"], errors="coerce")
        else:
            df["T"] = pd.NaT

        df = df.dropna(subset=["T"]).sort_values("T").reset_index(drop=True)

        if time_start is not None:
            time_start = pd.to_datetime(time_start, errors="coerce")
            df = df[df["T"] >= time_start].copy()

        if time_end is not None:
            time_end = pd.to_datetime(time_end, errors="coerce")
            df = df[df["T"] < time_end].copy()

        if df.empty:
            fig = self.empty_figure(title=title, message="No BlackBox data in selected time page")
            out_path = self._build_output_path(
                dsr_df if dsr_df is not None else bbdata, save_dir, suffix, file_name, ext
            )
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        num_cols = [
            "GNSS1_NOS", "GNSS1_DiffAge", "GNSS1_HDOP",
            "GNSS2_NOS", "GNSS2_DiffAge", "GNSS2_HDOP",
        ]
        for c in num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            else:
                df[c] = np.nan

        df.loc[df["GNSS1_NOS"] < 0, "GNSS1_NOS"] = np.nan
        df.loc[df["GNSS2_NOS"] < 0, "GNSS2_NOS"] = np.nan

        df.loc[(df["GNSS1_DiffAge"].notna()) & (df["GNSS1_DiffAge"] < 0), "GNSS1_DiffAge"] = np.nan
        df.loc[(df["GNSS2_DiffAge"].notna()) & (df["GNSS2_DiffAge"] < 0), "GNSS2_DiffAge"] = np.nan
        df.loc[(df["GNSS1_DiffAge"].notna()) & (df["GNSS1_DiffAge"] > 9999), "GNSS1_DiffAge"] = np.nan
        df.loc[(df["GNSS2_DiffAge"].notna()) & (df["GNSS2_DiffAge"] > 9999), "GNSS2_DiffAge"] = np.nan

        if bb_stride and int(bb_stride) > 1:
            df = df.iloc[::int(bb_stride)].copy()

        def _stats_text(series, digits=2):
            s = pd.to_numeric(series, errors="coerce").dropna()
            if s.empty:
                return "no data"
            return f"min:{s.min():.{digits}f}; max:{s.max():.{digits}f}; avg:{s.mean():.{digits}f}"

        g1 = str(gnss1_label).strip() if gnss1_label else "GNSS1"
        g2 = str(gnss2_label).strip() if gnss2_label else "GNSS2"

        station_events = []
        rov_color_map = {}

        if dsr_df is not None and len(dsr_df) > 0:
            dsr = dsr_df.copy()

            if "TimeStamp" in dsr.columns:
                dsr["TimeStamp"] = pd.to_datetime(dsr["TimeStamp"], errors="coerce")
            else:
                dsr["TimeStamp"] = pd.NaT

            if "Station" in dsr.columns:
                dsr["Station"] = pd.to_numeric(dsr["Station"], errors="coerce")
            else:
                dsr["Station"] = np.nan

            if "ROV" not in dsr.columns:
                dsr["ROV"] = ""

            dsr["ROV"] = dsr["ROV"].fillna("").astype(str).str.strip()
            dsr = dsr.dropna(subset=["TimeStamp"]).sort_values(["TimeStamp", "Station"]).copy()

            if time_start is not None:
                dsr = dsr[dsr["TimeStamp"] >= time_start].copy()

            if time_end is not None:
                dsr = dsr[dsr["TimeStamp"] < time_end].copy()

            if len(dsr) > max_station_lines:
                step = int(np.ceil(len(dsr) / float(max_station_lines)))
                dsr = dsr.iloc[::step].copy()

            rov_values = sorted([
                x for x in dsr["ROV"].dropna().unique().tolist()
                if str(x).strip() != ""
            ])

            cmap = plt.cm.get_cmap("tab10", max(len(rov_values), 1))
            for i, rov in enumerate(rov_values):
                rov_color_map[rov] = cmap(i)

            for _, r in dsr.iterrows():
                station_events.append({
                    "t": r["TimeStamp"],
                    "station": r["Station"],
                    "rov": r["ROV"],
                    "color": rov_color_map.get(r["ROV"], "#999999"),
                })

        fig, axes = plt.subplots(3, 1, sharex=True, figsize=figsize)
        fig.patch.set_facecolor("white")

        for ax in axes:
            ax.set_facecolor("white")

        ax = axes[0]
        ax.plot(
            df["T"], df["GNSS1_NOS"],
            linewidth=1.8, color="red",
            label=f"{g1} | {_stats_text(df['GNSS1_NOS'])}",
            zorder=3,
        )
        ax.plot(
            df["T"], df["GNSS2_NOS"],
            linewidth=1.8, color="blue",
            label=f"{g2} | {_stats_text(df['GNSS2_NOS'])}",
            zorder=3,
        )
        self._style_ax(ax, ylabel="Satellites", title=f"{title} — Number of Satellites")
        ax.grid(True, linestyle="--", alpha=0.30)
        ax.set_ylim(*satellites_ylim)
        ax.legend(loc="upper right", fontsize=8)

        ax = axes[1]
        ax.plot(
            df["T"], df["GNSS1_DiffAge"],
            linewidth=1.2, color="red",
            label=f"{g1} | {_stats_text(df['GNSS1_DiffAge'])}",
            zorder=3,
        )
        ax.plot(
            df["T"], df["GNSS2_DiffAge"],
            linewidth=1.2, color="blue",
            label=f"{g2} | {_stats_text(df['GNSS2_DiffAge'])}",
            zorder=3,
        )
        ax.axhline(diff_good_max, color="#2ca02c", linestyle="--", linewidth=1.0, alpha=0.8)
        ax.axhline(diff_warn_max, color="#ff7f0e", linestyle="--", linewidth=1.0, alpha=0.8)
        self._style_ax(
            ax,
            ylabel="DiffAge, s",
            title=f"{title} — DiffAge (good≤{diff_good_max:g}, warn≤{diff_warn_max:g})",
        )
        ax.grid(True, linestyle="--", alpha=0.30)
        ax.set_ylim(*diffage_ylim)
        ax.legend(loc="upper right", fontsize=8)

        ax = axes[2]
        ax.plot(
            df["T"], df["GNSS1_HDOP"],
            linewidth=1.8, color="red",
            label=f"{g1} | {_stats_text(df['GNSS1_HDOP'])}",
            zorder=3,
        )
        ax.plot(
            df["T"], df["GNSS2_HDOP"],
            linewidth=1.8, color="blue",
            label=f"{g2} | {_stats_text(df['GNSS2_HDOP'])}",
            zorder=3,
        )
        self._style_ax(ax, xlabel="Time", ylabel="HDOP", title=f"{title} — HDOP")
        ax.grid(True, linestyle="--", alpha=0.30)
        ax.set_ylim(*hdop_ylim)
        ax.legend(loc="upper right", fontsize=8)

        if station_events:
            for ax in axes:
                for ev in station_events:
                    ax.axvline(
                        ev["t"],
                        color=ev["color"],
                        linewidth=1.2,  # thicker
                        alpha=0.6,  # more visible
                        linestyle="-",  # solid line (better QC look)
                        zorder=2,
                    )

            axb = axes[2]
            ymin, ymax = axb.get_ylim()
            ytxt = ymin + (ymax - ymin) * station_label_y_pad_ratio

            for i, ev in enumerate(station_events):
                if station_label_step and i % int(station_label_step) != 0:
                    continue

                st = ev["station"]
                if pd.notna(st):
                    try:
                        st_txt = str(int(float(st)))
                    except Exception:
                        st_txt = str(st)

                    axb.text(
                        ev["t"],
                        ytxt,
                        st_txt,
                        rotation=90,
                        fontsize=9,  # bigger labels
                        fontweight="bold",  # strong visibility
                        color=ev["color"],  # ROV color
                        ha="center",
                        va="bottom",
                        alpha=1.0,
                        zorder=5,
                        bbox=dict(  # white background for readability
                            facecolor="white",
                            edgecolor="none",
                            alpha=0.7,
                            pad=0.5
                        ),
                    )

        if rov_color_map:
            rov_handles = [
                Line2D([0], [0], color=color, lw=1.2, linestyle="--", label=str(rov))
                for rov, color in rov_color_map.items()
            ]
            fig.legend(
                handles=rov_handles,
                loc="upper left",
                bbox_to_anchor=(0.065, 0.985),
                ncol=1,
                frameon=True,
                title="DSR Station Lines by ROV",
                fontsize=8,
                title_fontsize=9,
            )

        if time_start is not None or time_end is not None:
            t1 = pd.to_datetime(time_start).strftime("%Y-%m-%d %H:%M") if time_start is not None else "start"
            t2 = pd.to_datetime(time_end).strftime("%Y-%m-%d %H:%M") if time_end is not None else "end"
            fig.suptitle(
                f"{title}\nPage window: {t1} -> {t2}",
                y=0.995,
                fontsize=12,
            )

        fig.subplots_adjust(
            left=0.06,
            right=0.99,
            bottom=0.08,
            top=0.90,
            hspace=0.18,
        )

        out_path = self._build_output_path(
            dsr_df if dsr_df is not None else bbdata, save_dir, suffix, file_name, ext
        )
        self._save_figure(fig, out_path, dpi=dpi, close=False)

        if is_show:
            plt.show()

        if close:
            plt.close(fig)

        return fig, str(out_path)

    def plot_bbox_gnss_qc_for_line_paged(
            self,
            line,
            *,
            hours_per_page=12,
            pad_minutes=0,
            save_dir=".",
            file_prefix=None,
            ext="png",
            dpi=200,
            figsize=(18, 10),
            is_show=False,
            close=True,
    ):
        """
        Build paged GNSS QC plots for one DSR line.
        Each page covers `hours_per_page` hours.
        Uses gnss1_name / gnss2_name from config for legend labels.
        """

        dsr_df = self.read_dsr_for_line(line)
        bbdata, cfg_row, start_time, end_time = self.load_bbdata_for_dsr_line(
            line=line,
            pad_minutes=pad_minutes,
        )

        if bbdata is None or bbdata.empty:
            print("No bbdata loaded")
            return []

        bb = bbdata.copy()
        if "T" in bb.columns:
            bb["T"] = pd.to_datetime(bb["T"], errors="coerce")
        elif "TimeStamp" in bb.columns:
            bb["T"] = pd.to_datetime(bb["TimeStamp"], errors="coerce")
        else:
            print("No timestamp column in bbdata")
            return []

        bb = bb.dropna(subset=["T"]).sort_values("T").reset_index(drop=True)
        if bb.empty:
            print("bbdata empty after timestamp cleanup")
            return []

        tmin = bb["T"].min()
        tmax = bb["T"].max()

        if pd.isna(tmin) or pd.isna(tmax):
            print("Invalid time range")
            return []

        gnss1_label = None
        gnss2_label = None
        if cfg_row:
            gnss1_label = cfg_row.get("gnss1_name") or "GNSS1"
            gnss2_label = cfg_row.get("gnss2_name") or "GNSS2"

        hours_per_page = max(float(hours_per_page), 1.0)
        page_delta = pd.Timedelta(hours=hours_per_page)

        saved_paths = []
        page_no = 1
        cur_start = tmin.floor("min")

        while cur_start < tmax:
            cur_end = cur_start + page_delta

            if file_prefix:
                fname = f"{file_prefix}_page_{page_no:03d}.{ext.lstrip('.')}"
            else:
                fname = f"{line}_bbox_gnss_qc_page_{page_no:03d}.{ext.lstrip('.')}"

            _, out_path = self.plot_bbox_gnss_qc_timeseries(
                bbdata=bb,
                dsr_df=dsr_df,
                title=f"Line {line} GNSS QC | Page {page_no}",
                gnss1_label=gnss1_label,
                gnss2_label=gnss2_label,
                time_start=cur_start,
                time_end=cur_end,
                station_label_step=1,
                station_label_fontsize=7,
                station_label_rotation=90,
                station_label_alpha=0.85,
                station_label_y_pad_ratio=0.03,
                max_station_lines=80,
                bb_stride=20,
                satellites_ylim=(0, 40),
                diffage_ylim=(0, 40),
                hdop_ylim=(0, 3),
                figsize=figsize,
                save_dir=save_dir,
                file_name=fname,
                suffix="bbox_gnss_qc",
                ext=ext,
                dpi=dpi,
                is_show=is_show,
                close=close,
            )

            saved_paths.append(out_path)
            page_no += 1
            cur_start = cur_end

        return saved_paths


    def plot_bbox_gnss_qc_for_line(
            self,
            line,
            *,
            pad_minutes=0,
            title=None,
            figsize=(18, 10),
            save_dir=".",
            file_name=None,
            suffix="bbox_gnss_qc",
            ext="png",
            dpi=200,
            is_show=False,
            close=False,
    ):
        dsr_df = self.read_dsr_for_line(line)
        bbdata, cfg_row, start_time, end_time = self.load_bbdata_for_dsr_line(
            line=line,
            pad_minutes=pad_minutes,
        )

        if title is None:
            title = f"Line {line} GNSS QC"

        return self.plot_bbox_gnss_qc_timeseries(
            bbdata=bbdata,
            dsr_df=dsr_df,
            title=title,
            figsize=figsize,
            save_dir=save_dir,
            file_name=file_name,
            suffix=suffix,
            ext=ext,
            dpi=dpi,
            is_show=is_show,
            close=close,
        )
    def plot_bbox_motion_qc_timeseries(
            self,
            bbdata,
            dsr_df=None,
            *,
            metric="SOG",   # "SOG" | "COG" | "HDG"
            title="BBox Motion QC",
            vessel_label=None,
            rov1_label=None,
            rov2_label=None,

            vessel_col=None,
            rov1_col=None,
            rov2_col=None,

            time_start=None,
            time_end=None,

            station_label_step=20,
            station_label_fontsize=7,
            station_label_rotation=90,
            station_label_alpha=0.85,
            station_label_y_pad_ratio=0.03,
            max_station_lines=80,

            bb_stride=20,
            figsize=(18, 10),
            save_dir=".",
            file_name=None,
            suffix="bbox_motion_qc",
            ext="png",
            dpi=200,
            is_show=False,
            close=False,

            line_width=1.6,
            break_heading_wrap=True,
            y_lim=None,
    ):
        """
        Generic paged BBox QC plot for one metric:
            - Vessel
            - ROV1
            - ROV2

        Works like GNSS paged QC but for:
            SOG / COG / HDG
        """

        if bbdata is None or len(bbdata) == 0:
            fig = self.empty_figure(title=title, message="No BlackBox data")
            out_path = self._build_output_path(
                dsr_df if dsr_df is not None else bbdata, save_dir, suffix, file_name, ext
            )
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        df = bbdata.copy()
        df = df.loc[:, ~df.columns.duplicated()]

        if "T" in df.columns:
            df["T"] = pd.to_datetime(df["T"], errors="coerce")
        elif "TimeStamp" in df.columns:
            df["T"] = pd.to_datetime(df["TimeStamp"], errors="coerce")
        else:
            df["T"] = pd.NaT

        df = df.dropna(subset=["T"]).sort_values("T").reset_index(drop=True)

        if time_start is not None:
            time_start = pd.to_datetime(time_start, errors="coerce")
            df = df[df["T"] >= time_start].copy()

        if time_end is not None:
            time_end = pd.to_datetime(time_end, errors="coerce")
            df = df[df["T"] < time_end].copy()

        if df.empty:
            fig = self.empty_figure(title=title, message="No BlackBox data in selected time page")
            out_path = self._build_output_path(
                dsr_df if dsr_df is not None else bbdata, save_dir, suffix, file_name, ext
            )
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        metric = str(metric).strip().upper()

        if vessel_col is None:
            vessel_col = f"Vessel{metric}"
        if rov1_col is None:
            rov1_col = f"ROV1_{metric}"
        if rov2_col is None:
            rov2_col = f"ROV2_{metric}"

        for c in [vessel_col, rov1_col, rov2_col]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            else:
                df[c] = np.nan

        # basic cleanup
        if metric == "SOG":
            for c in [vessel_col, rov1_col, rov2_col]:
                df.loc[(df[c].notna()) & (df[c] < 0), c] = np.nan
                df.loc[(df[c].notna()) & (df[c] > 25), c] = np.nan
        elif metric in {"COG", "HDG"}:
            for c in [vessel_col, rov1_col, rov2_col]:
                df.loc[(df[c].notna()) & (df[c] < 0), c] = np.nan
                df.loc[(df[c].notna()) & (df[c] > 360), c] = np.nan

        if bb_stride and int(bb_stride) > 1:
            df = df.iloc[::int(bb_stride)].copy()

        def _stats_text(series, digits=2):
            s = pd.to_numeric(series, errors="coerce").dropna()
            if s.empty:
                return "no data"
            return f"min:{s.min():.{digits}f}; max:{s.max():.{digits}f}; avg:{s.mean():.{digits}f}"

        def _break_angle_wrap(series, threshold=180.0):
            s = pd.to_numeric(series, errors="coerce").copy()
            if s.empty:
                return s
            s = s.reset_index(drop=True)
            for i in range(1, len(s)):
                a = s.iloc[i - 1]
                b = s.iloc[i]
                if pd.notna(a) and pd.notna(b):
                    if abs(float(b) - float(a)) > threshold:
                        s.iloc[i] = np.nan
            return s

        v_label = str(vessel_label).strip() if vessel_label else "Vessel"
        r1_label = str(rov1_label).strip() if rov1_label else "ROV1"
        r2_label = str(rov2_label).strip() if rov2_label else "ROV2"

        station_events = []
        rov_color_map = {}

        if dsr_df is not None and len(dsr_df) > 0:
            dsr = dsr_df.copy()

            if "TimeStamp" in dsr.columns:
                dsr["TimeStamp"] = pd.to_datetime(dsr["TimeStamp"], errors="coerce")
            else:
                dsr["TimeStamp"] = pd.NaT

            if "Station" in dsr.columns:
                dsr["Station"] = pd.to_numeric(dsr["Station"], errors="coerce")
            else:
                dsr["Station"] = np.nan

            if "ROV" not in dsr.columns:
                dsr["ROV"] = ""

            dsr["ROV"] = dsr["ROV"].fillna("").astype(str).str.strip()
            dsr = dsr.dropna(subset=["TimeStamp"]).sort_values(["TimeStamp", "Station"]).copy()

            if time_start is not None:
                dsr = dsr[dsr["TimeStamp"] >= time_start].copy()

            if time_end is not None:
                dsr = dsr[dsr["TimeStamp"] < time_end].copy()

            if len(dsr) > max_station_lines:
                step = int(np.ceil(len(dsr) / float(max_station_lines)))
                dsr = dsr.iloc[::step].copy()

            rov_values = sorted([
                x for x in dsr["ROV"].dropna().unique().tolist()
                if str(x).strip() != ""
            ])

            cmap = plt.cm.get_cmap("tab10", max(len(rov_values), 1))
            for i, rov in enumerate(rov_values):
                rov_color_map[rov] = cmap(i)

            for _, r in dsr.iterrows():
                station_events.append({
                    "t": r["TimeStamp"],
                    "station": r["Station"],
                    "rov": r["ROV"],
                    "color": rov_color_map.get(r["ROV"], "#999999"),
                })

        fig, axes = plt.subplots(3, 1, sharex=True, figsize=figsize)
        fig.patch.set_facecolor("white")
        for ax in axes:
            ax.set_facecolor("white")

        plot_defs = [
            (axes[0], vessel_col, v_label, "#1f77b4"),
            (axes[1], rov1_col,   r1_label, "#d62728"),
            (axes[2], rov2_col,   r2_label, "#2ca02c"),
        ]

        for ax, col, lbl, color in plot_defs:
            y = df[col].copy()

            if metric in {"COG", "HDG"} and break_heading_wrap:
                y = _break_angle_wrap(y, threshold=180.0)

            ax.plot(
                df["T"], y,
                linewidth=line_width,
                color=color,
                label=f"{lbl} | {_stats_text(df[col])}",
                zorder=3,
            )

            if metric == "SOG":
                ylabel = "SOG, kn"
                ttl = f"{title} — {lbl} SOG"
            elif metric == "COG":
                ylabel = "COG, deg"
                ttl = f"{title} — {lbl} COG"
            else:
                ylabel = "HDG, deg"
                ttl = f"{title} — {lbl} HDG"

            self._style_ax(ax, ylabel=ylabel, title=ttl)
            ax.grid(True, linestyle="--", alpha=0.30)

            if y_lim is not None:
                ax.set_ylim(*y_lim)
            elif metric in {"COG", "HDG"}:
                ax.set_ylim(0, 360)

            ax.legend(loc="upper right", fontsize=8)

        axes[-1].set_xlabel("Time")

        if station_events:
            for ax in axes:
                for ev in station_events:
                    ax.axvline(
                        ev["t"],
                        color=ev["color"],
                        linewidth=1.2,
                        alpha=0.6,
                        linestyle="-",
                        zorder=2,
                    )

            axb = axes[2]
            ymin, ymax = axb.get_ylim()
            ytxt = ymin + (ymax - ymin) * station_label_y_pad_ratio

            for i, ev in enumerate(station_events):
                if station_label_step and i % int(station_label_step) != 0:
                    continue

                st = ev["station"]
                if pd.notna(st):
                    try:
                        st_txt = str(int(float(st)))
                    except Exception:
                        st_txt = str(st)

                    axb.text(
                        ev["t"],
                        ytxt,
                        st_txt,
                        rotation=station_label_rotation,
                        fontsize=station_label_fontsize,
                        fontweight="bold",
                        color=ev["color"],
                        ha="center",
                        va="bottom",
                        alpha=station_label_alpha,
                        zorder=5,
                        bbox=dict(facecolor="white", edgecolor="none", alpha=0.7, pad=0.5),
                    )

        if rov_color_map:
            rov_handles = [
                Line2D([0], [0], color=color, lw=1.2, linestyle="--", label=str(rov))
                for rov, color in rov_color_map.items()
            ]
            fig.legend(
                handles=rov_handles,
                loc="upper left",
                bbox_to_anchor=(0.065, 0.985),
                ncol=1,
                frameon=True,
                title="DSR Station Lines by ROV",
                fontsize=8,
                title_fontsize=9,
            )

        if time_start is not None or time_end is not None:
            t1 = pd.to_datetime(time_start).strftime("%Y-%m-%d %H:%M") if time_start is not None else "start"
            t2 = pd.to_datetime(time_end).strftime("%Y-%m-%d %H:%M") if time_end is not None else "end"
            fig.suptitle(
                f"{title}\nPage window: {t1} -> {t2}",
                y=0.995,
                fontsize=12,
            )

        fig.subplots_adjust(
            left=0.06,
            right=0.99,
            bottom=0.08,
            top=0.90,
            hspace=0.18,
        )

        out_path = self._build_output_path(
            dsr_df if dsr_df is not None else bbdata, save_dir, suffix, file_name, ext
        )
        self._save_figure(fig, out_path, dpi=dpi, close=False)

        if is_show:
            plt.show()

        if close:
            plt.close(fig)

        return fig, str(out_path)


    def plot_bbox_motion_qc_for_line_paged(
            self,
            line,
            *,
            metric="SOG",   # "SOG" | "COG" | "HDG"
            hours_per_page=12,
            pad_minutes=0,
            save_dir=".",
            file_prefix=None,
            ext="png",
            dpi=200,
            figsize=(18, 10),
            is_show=False,
            close=True,
            bb_stride=20,
            y_lim=None,
    ):
        """
        Build paged motion QC plots for one DSR line.
        One page covers `hours_per_page` hours.
        Same idea as plot_bbox_gnss_qc_for_line_paged().
        """

        dsr_df = self.read_dsr_for_line(line)
        bbdata, cfg_row, start_time, end_time = self.load_bbdata_for_dsr_line(
            line=line,
            pad_minutes=pad_minutes,
        )

        if bbdata is None or bbdata.empty:
            print("No bbdata loaded")
            return []

        bb = bbdata.copy()
        if "T" in bb.columns:
            bb["T"] = pd.to_datetime(bb["T"], errors="coerce")
        elif "TimeStamp" in bb.columns:
            bb["T"] = pd.to_datetime(bb["TimeStamp"], errors="coerce")
        else:
            print("No timestamp column in bbdata")
            return []

        bb = bb.dropna(subset=["T"]).sort_values("T").reset_index(drop=True)
        if bb.empty:
            print("bbdata empty after timestamp cleanup")
            return []

        tmin = bb["T"].min()
        tmax = bb["T"].max()

        if pd.isna(tmin) or pd.isna(tmax):
            print("Invalid time range")
            return []

        vessel_label = "Vessel"
        rov1_label = "ROV1"
        rov2_label = "ROV2"

        if cfg_row:
            vessel_label = cfg_row.get("Vessel_name") or "Vessel"
            rov1_label = cfg_row.get("rov1_name") or "ROV1"
            rov2_label = cfg_row.get("rov2_name") or "ROV2"

        metric = str(metric).strip().upper()
        hours_per_page = max(float(hours_per_page), 1.0)
        page_delta = pd.Timedelta(hours=hours_per_page)

        saved_paths = []
        page_no = 1
        cur_start = tmin.floor("min")

        while cur_start < tmax:
            cur_end = cur_start + page_delta

            if file_prefix:
                fname = f"{file_prefix}_page_{page_no:03d}.{ext.lstrip('.')}"
            else:
                fname = f"{line}_bbox_{metric.lower()}_qc_page_{page_no:03d}.{ext.lstrip('.')}"

            _, out_path = self.plot_bbox_motion_qc_timeseries(
                bbdata=bb,
                dsr_df=dsr_df,
                metric=metric,
                title=f"Line {line} {metric} QC | Page {page_no}",
                vessel_label=vessel_label,
                rov1_label=rov1_label,
                rov2_label=rov2_label,
                time_start=cur_start,
                time_end=cur_end,
                station_label_step=1,
                station_label_fontsize=7,
                station_label_rotation=90,
                station_label_alpha=0.85,
                station_label_y_pad_ratio=0.03,
                max_station_lines=80,
                bb_stride=bb_stride,
                figsize=figsize,
                save_dir=save_dir,
                file_name=fname,
                suffix=f"bbox_{metric.lower()}_qc",
                ext=ext,
                dpi=dpi,
                is_show=is_show,
                close=close,
                y_lim=y_lim,
            )

            saved_paths.append(out_path)
            page_no += 1
            cur_start = cur_end

        return saved_paths


    def plot_bbox_sog_qc_for_line_paged(
            self,
            line,
            *,
            hours_per_page=12,
            pad_minutes=0,
            save_dir=".",
            file_prefix=None,
            ext="png",
            dpi=200,
            figsize=(18, 10),
            is_show=False,
            close=True,
            bb_stride=20,
            y_lim=(0, 8),
    ):
        return self.plot_bbox_motion_qc_for_line_paged(
            line=line,
            metric="SOG",
            hours_per_page=hours_per_page,
            pad_minutes=pad_minutes,
            save_dir=save_dir,
            file_prefix=file_prefix,
            ext=ext,
            dpi=dpi,
            figsize=figsize,
            is_show=is_show,
            close=close,
            bb_stride=bb_stride,
            y_lim=y_lim,
        )


    def plot_bbox_cog_qc_for_line_paged(
            self,
            line,
            *,
            hours_per_page=12,
            pad_minutes=0,
            save_dir=".",
            file_prefix=None,
            ext="png",
            dpi=200,
            figsize=(18, 10),
            is_show=False,
            close=True,
            bb_stride=20,
    ):
        return self.plot_bbox_motion_qc_for_line_paged(
            line=line,
            metric="COG",
            hours_per_page=hours_per_page,
            pad_minutes=pad_minutes,
            save_dir=save_dir,
            file_prefix=file_prefix,
            ext=ext,
            dpi=dpi,
            figsize=figsize,
            is_show=is_show,
            close=close,
            bb_stride=bb_stride,
            y_lim=(0, 360),
        )


    def plot_bbox_hdg_qc_for_line_paged(
            self,
            line,
            *,
            hours_per_page=12,
            pad_minutes=0,
            save_dir=".",
            file_prefix=None,
            ext="png",
            dpi=200,
            figsize=(18, 10),
            is_show=False,
            close=True,
            bb_stride=20,
    ):
        return self.plot_bbox_motion_qc_for_line_paged(
            line=line,
            metric="HDG",
            hours_per_page=hours_per_page,
            pad_minutes=pad_minutes,
            save_dir=save_dir,
            file_prefix=file_prefix,
            ext=ext,
            dpi=dpi,
            figsize=figsize,
            is_show=is_show,
            close=close,
            bb_stride=bb_stride,
            y_lim=(0, 360),
        )

    def plot_bbox_motion_qc_combined_timeseries(
            self,
            bbdata,
            dsr_df=None,
            *,
            title="BBox Motion QC",
            vessel_label=None,
            rov1_label=None,
            rov2_label=None,
            time_start=None,
            time_end=None,
            station_label_step=10,
            station_fontsize=8,
            station_label_rotation=90,
            station_label_alpha=0.85,
            station_label_y_pad_ratio=0.03,
            max_station_lines=80,
            bb_stride=20,
            figsize=(18, 10),
            save_dir=".",
            file_name=None,
            suffix="bbox_motion_qc_combined",
            ext="png",
            dpi=200,
            is_show=False,
            close=False,
    ):
        """
        3 stacked plots with shared X axis:
            1) SOG
            2) COG
            3) HDG

        One horizontal common legend for all 3 plots.
        DSR station lines are colored by ROV.
        X axis is forced to selected page window only.
        """

        if bbdata is None or len(bbdata) == 0:
            fig = self.empty_figure(title=title, message="No BlackBox data")
            out_path = self._build_output_path(
                dsr_df if dsr_df is not None else bbdata, save_dir, suffix, file_name, ext
            )
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        df = bbdata.copy()
        df = df.loc[:, ~df.columns.duplicated()]

        # -------------------------
        # time
        # -------------------------
        if "T" in df.columns:
            df["T"] = pd.to_datetime(df["T"], errors="coerce")
        elif "TimeStamp" in df.columns:
            df["T"] = pd.to_datetime(df["TimeStamp"], errors="coerce")
        else:
            df["T"] = pd.NaT

        df = df.dropna(subset=["T"]).sort_values("T").reset_index(drop=True)

        if time_start is not None:
            time_start = pd.to_datetime(time_start, errors="coerce")
            df = df[df["T"] >= time_start].copy()

        if time_end is not None:
            time_end = pd.to_datetime(time_end, errors="coerce")
            df = df[df["T"] < time_end].copy()

        if df.empty:
            fig = self.empty_figure(title=title, message="No BlackBox data in selected time page")
            out_path = self._build_output_path(
                dsr_df if dsr_df is not None else bbdata, save_dir, suffix, file_name, ext
            )
            self._save_figure(fig, out_path, dpi=dpi, close=close)
            if is_show:
                plt.show()
            return fig, str(out_path)

        if bb_stride and int(bb_stride) > 1:
            df = df.iloc[::int(bb_stride)].copy()

        # -------------------------
        # labels
        # -------------------------
        v_label = str(vessel_label).strip() if vessel_label else "Vessel"
        r1_label = str(rov1_label).strip() if rov1_label else "ROV1"
        r2_label = str(rov2_label).strip() if rov2_label else "ROV2"

        # -------------------------
        # helpers
        # -------------------------
        def _num(col_name):
            if col_name in df.columns:
                return pd.to_numeric(df[col_name], errors="coerce")
            return pd.Series(np.nan, index=df.index)

        def _break_wrap(series, threshold=180.0):
            s = pd.to_numeric(series, errors="coerce").copy().reset_index(drop=True)
            if s.empty:
                return s
            for i in range(1, len(s)):
                a = s.iloc[i - 1]
                b = s.iloc[i]
                if pd.notna(a) and pd.notna(b):
                    if abs(float(b) - float(a)) > threshold:
                        s.iloc[i] = np.nan
            return s

        def _stats(series, digits=2):
            s = pd.to_numeric(series, errors="coerce").dropna()
            if s.empty:
                return "no data"
            return f"min:{s.min():.{digits}f}  avg:{s.mean():.{digits}f}  max:{s.max():.{digits}f}"

        # -------------------------
        # series
        # -------------------------
        sog_v = _num("VesselSOG")
        sog_r1 = _num("ROV1_SOG")
        sog_r2 = _num("ROV2_SOG")

        cog_v = _break_wrap(_num("VesselCOG"))
        cog_r1 = _break_wrap(_num("ROV1_COG"))
        cog_r2 = _break_wrap(_num("ROV2_COG"))

        hdg_v = _break_wrap(_num("VesselHDG"))
        hdg_r1 = _break_wrap(_num("ROV1_HDG"))
        hdg_r2 = _break_wrap(_num("ROV2_HDG"))

        # cleanup
        sog_v = sog_v.where((sog_v >= 0) & (sog_v <= 5), np.nan)
        sog_r1 = sog_r1.where((sog_r1 >= 0) & (sog_r1 <= 5), np.nan)
        sog_r2 = sog_r2.where((sog_r2 >= 0) & (sog_r2 <= 5), np.nan)

        for s in [cog_v, cog_r1, cog_r2, hdg_v, hdg_r1, hdg_r2]:
            s[(s < 0) | (s > 360)] = np.nan

        # -------------------------
        # figure
        # -------------------------
        fig, axes = plt.subplots(3, 1, sharex=True, figsize=figsize)
        fig.patch.set_facecolor("white")

        for ax in axes:
            ax.set_facecolor("white")
            ax.grid(True, linestyle="--", alpha=0.30)
            ax.set_axisbelow(True)

        vessel_color = "#1f77b4"
        rov1_color = "#d62728"
        rov2_color = "#2ca02c"

        # -------------------------
        # SOG
        # -------------------------
        axes[0].plot(df["T"], sog_v, color=vessel_color, lw=1.8, label=v_label, zorder=3)
        axes[0].plot(df["T"], sog_r1, color=rov1_color, lw=1.5, label=r1_label, zorder=3)
        axes[0].plot(df["T"], sog_r2, color=rov2_color, lw=1.5, label=r2_label, zorder=3)
        axes[0].set_ylabel("SOG, kn")
        axes[0].set_ylim(0, 5)
        axes[0].set_title(f"SOG | {_stats(sog_v)}")

        # -------------------------
        # COG
        # -------------------------
        axes[1].plot(df["T"], cog_v, color=vessel_color, lw=1.8, zorder=3)
        axes[1].plot(df["T"], cog_r1, color=rov1_color, lw=1.5, zorder=3)
        axes[1].plot(df["T"], cog_r2, color=rov2_color, lw=1.5, zorder=3)
        axes[1].set_ylabel("COG, deg")
        axes[1].set_ylim(-10, 370)
        axes[1].set_yticks(np.arange(0, 361, 60))
        axes[1].set_title(f"COG | {_stats(cog_v)}")

        # -------------------------
        # HDG
        # -------------------------
        axes[2].plot(df["T"], hdg_v, color=vessel_color, lw=1.8, zorder=3)
        axes[2].plot(df["T"], hdg_r1, color=rov1_color, lw=1.5, zorder=3)
        axes[2].plot(df["T"], hdg_r2, color=rov2_color, lw=1.5, zorder=3)
        axes[2].set_ylabel("HDG, deg")
        axes[2].set_ylim(-10, 370)
        axes[2].set_yticks(np.arange(0, 361, 60))
        axes[2].set_title(f"HDG | {_stats(hdg_v)}")
        axes[2].set_xlabel("Time")

        # -------------------------
        # DSR lines + labels
        # -------------------------
        rov_color_map = {}
        rov_line_handles = []

        if dsr_df is not None and len(dsr_df) > 0:
            dsr = dsr_df.copy()

            if "TimeStamp" in dsr.columns:
                dsr["TimeStamp"] = pd.to_datetime(dsr["TimeStamp"], errors="coerce")
            else:
                dsr["TimeStamp"] = pd.NaT

            if "Station" in dsr.columns:
                dsr["Station"] = pd.to_numeric(dsr["Station"], errors="coerce")
            else:
                dsr["Station"] = np.nan

            if "ROV" not in dsr.columns:
                dsr["ROV"] = ""

            dsr["ROV"] = dsr["ROV"].fillna("").astype(str).str.strip()
            dsr = dsr.dropna(subset=["TimeStamp"]).sort_values(["TimeStamp", "Station"]).copy()

            if time_start is not None:
                dsr = dsr[dsr["TimeStamp"] >= time_start].copy()

            if time_end is not None:
                dsr = dsr[dsr["TimeStamp"] < time_end].copy()

            if len(dsr) > max_station_lines:
                step = int(np.ceil(len(dsr) / float(max_station_lines)))
                dsr = dsr.iloc[::step].copy()

            rov_values = sorted([
                x for x in dsr["ROV"].dropna().unique().tolist()
                if str(x).strip() != ""
            ])

            cmap = plt.cm.get_cmap("tab10", max(len(rov_values), 1))
            for i, rov in enumerate(rov_values):
                rov_color_map[rov] = cmap(i)

            for ax in axes:
                for _, r in dsr.iterrows():
                    ax.axvline(
                        r["TimeStamp"],
                        color=rov_color_map.get(r["ROV"], "#999999"),
                        lw=1.2,
                        alpha=0.60,
                        linestyle="-",
                        zorder=2,
                    )

            # labels only on bottom plot
            axb = axes[2]
            ymin, ymax = axb.get_ylim()
            ytxt = ymin + (ymax - ymin) * station_label_y_pad_ratio

            for i, (_, r) in enumerate(dsr.iterrows()):
                if station_label_step and i % int(station_label_step) != 0:
                    continue

                st = r.get("Station")
                if pd.notna(st):
                    try:
                        st_txt = str(int(float(st)))
                    except Exception:
                        st_txt = str(st)

                    axb.text(
                        r["TimeStamp"],
                        ytxt,
                        st_txt,
                        rotation=station_label_rotation,
                        fontsize=station_fontsize,
                        color=rov_color_map.get(r["ROV"], "black"),
                        ha="center",
                        va="bottom",
                        alpha=station_label_alpha,
                        zorder=5,
                        bbox=dict(facecolor="white", edgecolor="none", alpha=0.7, pad=0.35),
                    )

            rov_line_handles = [
                Line2D([0], [0], color=color, lw=1.5, linestyle="-", label=f"{rov} ROV")
                for rov, color in rov_color_map.items()
            ]

        # -------------------------
        # force x range to page only
        # -------------------------
        if time_start is not None and time_end is not None:
            for ax in axes:
                ax.set_xlim(time_start, time_end)
                ax.margins(x=0)

        # -------------------------
        # main title
        # -------------------------
        if time_start is not None or time_end is not None:
            t1 = pd.to_datetime(time_start).strftime("%Y-%m-%d %H:%M") if time_start is not None else "start"
            t2 = pd.to_datetime(time_end).strftime("%Y-%m-%d %H:%M") if time_end is not None else "end"
            main_title = f"{title}\nPage window: {t1} -> {t2}"
        else:
            main_title = title

        fig.suptitle(main_title, y=0.985, fontsize=15)

        # -------------------------
        # common legend top-right
        # -------------------------
        motion_handles = [
            Line2D([0], [0], color=vessel_color, lw=1.8, label=v_label),
            Line2D([0], [0], color=rov1_color, lw=1.5, label=r1_label),
            Line2D([0], [0], color=rov2_color, lw=1.5, label=r2_label),
        ]

        legend_handles = motion_handles + rov_line_handles

        if legend_handles:
            fig.legend(
                handles=legend_handles,
                loc="upper right",
                bbox_to_anchor=(0.985, 0.955),
                ncol=min(len(legend_handles), 5),
                frameon=True,
                fontsize=9,
                title="Motion / ROV lines",
                title_fontsize=10,
                columnspacing=1.4,
                handlelength=2.0,
                handletextpad=0.5,
                borderaxespad=0.2,
            )

        # -------------------------
        # layout
        # -------------------------
        fig.subplots_adjust(
            left=0.06,
            right=0.99,
            top=0.84,
            bottom=0.08,
            hspace=0.16,
        )

        out_path = self._build_output_path(
            dsr_df if dsr_df is not None else bbdata,
            save_dir,
            suffix,
            file_name,
            ext
        )

        self._save_figure(fig, out_path, dpi=dpi, close=False)

        if is_show:
            plt.show()

        if close:
            plt.close(fig)

        return fig, str(out_path)

    def plot_bbox_motion_qc_for_line_paged_combined(
            self,
            line,
            *,
            hours_per_page=12,
            pad_minutes=0,
            save_dir=".",
            file_prefix=None,
            ext="png",
            dpi=200,
            figsize=(18, 10),
            is_show=False,
            close=True,
            bb_stride=20,
    ):
        """
        Build paged motion QC plots for one DSR line.
        Each page contains 3 stacked plots with shared X axis:
            1) SOG
            2) COG
            3) HDG

        Title format:
            Line XXXX Motion QC | Page X of N
        """

        dsr_df = self.read_dsr_for_line(line)
        bbdata, cfg_row, start_time, end_time = self.load_bbdata_for_dsr_line(
            line=line,
            pad_minutes=pad_minutes,
        )

        if bbdata is None or bbdata.empty:
            print("No bbdata loaded")
            return []

        bb = bbdata.copy()
        bb = bb.loc[:, ~bb.columns.duplicated()]

        if "T" in bb.columns:
            bb["T"] = pd.to_datetime(bb["T"], errors="coerce")
        elif "TimeStamp" in bb.columns:
            bb["T"] = pd.to_datetime(bb["TimeStamp"], errors="coerce")
        else:
            print("No timestamp column in bbdata")
            return []

        bb = bb.dropna(subset=["T"]).sort_values("T").reset_index(drop=True)

        if bb.empty:
            print("bbdata empty after timestamp cleanup")
            return []

        tmin = bb["T"].min()
        tmax = bb["T"].max()

        if pd.isna(tmin) or pd.isna(tmax):
            print("Invalid time range")
            return []

        vessel_label = "Vessel"
        rov1_label = "ROV1"
        rov2_label = "ROV2"

        if cfg_row:
            vessel_label = cfg_row.get("Vessel_name") or "Vessel"
            rov1_label = cfg_row.get("rov1_name") or "ROV1"
            rov2_label = cfg_row.get("rov2_name") or "ROV2"

        hours_per_page = max(float(hours_per_page), 1.0)
        page_delta = pd.Timedelta(hours=hours_per_page)

        page_windows = []
        cur_start = tmin.floor("min")

        while cur_start < tmax:
            cur_end = cur_start + page_delta
            page_windows.append((cur_start, cur_end))
            cur_start = cur_end

        total_pages = len(page_windows)
        saved_paths = []

        for page_no, (cur_start, cur_end) in enumerate(page_windows, start=1):
            if file_prefix:
                fname = f"{file_prefix}_page_{page_no:03d}.{ext.lstrip('.')}"
            else:
                fname = f"{line}_bbox_motion_qc_page_{page_no:03d}.{ext.lstrip('.')}"

            _, out_path = self.plot_bbox_motion_qc_combined_timeseries(
                bbdata=bb,
                dsr_df=dsr_df,
                title=f"Line {line} Motion QC | Page {page_no} of {total_pages}",
                vessel_label=vessel_label,
                rov1_label=rov1_label,
                rov2_label=rov2_label,
                time_start=cur_start,
                time_end=cur_end,
                station_label_step=1,
                station_fontsize=8,
                station_label_rotation=90,
                station_label_alpha=0.85,
                station_label_y_pad_ratio=0.03,
                max_station_lines=80,
                bb_stride=bb_stride,
                figsize=figsize,
                save_dir=save_dir,
                file_name=fname,
                suffix="bbox_motion_qc_combined",
                ext=ext,
                dpi=dpi,
                is_show=is_show,
                close=close,
            )

            saved_paths.append(out_path)

        return saved_paths

