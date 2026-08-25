import base64
import html
import math
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.offline.offline import get_plotlyjs
from bokeh.embed import components
from bokeh.models import ColumnDataSource, HoverTool, Span, LinearColorMapper, ColorBar, Range1d
from bokeh.palettes import Turbo256
from bokeh.plotting import figure
from bokeh.resources import INLINE

from rov.reports.node_position_comparison import NodePositionComparisonReport


class InteractiveNodePositionComparisonReport:
    """Generate a portable interactive HTML position report for one receiver line."""

    COLORS = ("#1769aa", "#ef7d22", "#14875d", "#c73535", "#6f4ca5")

    def __init__(self, db_path, project=None, logo_path=None):
        self.pdf_report = NodePositionComparisonReport(
            db_path=db_path, project=project, logo_path=logo_path
        )

    @staticmethod
    def _finite(values):
        values = pd.to_numeric(values, errors="coerce").to_numpy(float)
        return values[np.isfinite(values)]

    @classmethod
    def _stats(cls, values):
        values = cls._finite(values)
        if not len(values):
            return {"nodes": 0, "mean": None, "std": None, "p50": None, "p95": None}
        return {
            "nodes": int(len(values)),
            "mean": float(np.mean(values)),
            "std": float(np.std(values)),
            "p50": float(np.percentile(values, 50)),
            "p95": float(np.percentile(values, 95)),
        }

    @staticmethod
    def _fmt(value):
        return "—" if value is None or not np.isfinite(value) else f"{value:.2f}"

    def _station_plot(self, df, title, fields, x_range=None):
        source_data = {"station": df["Station"].tolist()}
        for field, _label, _color, _dash in fields:
            source_data[field] = pd.to_numeric(df[field], errors="coerce").tolist()
        source = ColumnDataSource(source_data)
        plot = figure(
            height=390, sizing_mode="stretch_width", title=title,
            x_axis_label="Station", y_axis_label="Offset (m)",
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
            x_range=x_range,
        )
        renderers = []
        for field, label, color, dash in fields:
            line = plot.line("station", field, source=source, line_width=2,
                             color=color, line_dash=dash, legend_label=label)
            points = plot.scatter("station", field, source=source, size=5,
                                  color=color, alpha=0.72, legend_label=label)
            renderers.extend((line, points))
        plot.add_tools(HoverTool(
            renderers=renderers,
            tooltips=[("Station", "@station{0}"), ("Value", "$y{0.00} m")],
            mode="vline",
        ))
        plot.legend.click_policy = "hide"
        plot.legend.location = "top_right"
        return plot

    def _cdf_metrics_plot(self, df, prefix, title):
        plot = figure(
            height=390, sizing_mode="stretch_width", title=title,
            x_axis_label="Absolute offset (m)", y_axis_label="Cumulative nodes (%)",
            tools="pan,wheel_zoom,box_zoom,reset,save", active_scroll="wheel_zoom",
        )
        metrics = (("dx", "dE"), ("dy", "dN"), ("dr", "Radial"),
                   ("il", "In-Line"), ("xl", "Cross-Line"))
        radial_values = np.array([])
        plotted = False
        for index, (suffix, label) in enumerate(metrics):
            values = np.sort(np.abs(self._finite(df[f"{prefix}_{suffix}"])))
            if not len(values):
                continue
            cdf = np.arange(1, len(values) + 1, dtype=float) / len(values) * 100.0
            source = ColumnDataSource({"offset": values, "cdf": cdf})
            renderer = plot.line("offset", "cdf", source=source,
                                 color=self.COLORS[index], line_width=2.4,
                                 legend_label=(f"{label} · P50 {np.percentile(values, 50):.2f} · "
                                               f"P95 {np.percentile(values, 95):.2f} m"))
            plotted = True
            plot.add_tools(HoverTool(renderers=[renderer], tooltips=[
                ("Metric", label), ("Offset", "@offset{0.00} m"), ("CDF", "@cdf{0.0}%")
            ]))
            if suffix == "dr":
                radial_values = values
        if len(radial_values):
            for percentile, marker_color in ((50, "#6f4ca5"), (95, "#c73535")):
                x = float(np.percentile(radial_values, percentile))
                plot.add_layout(Span(location=x, dimension="height", line_color=marker_color,
                                     line_dash="dashed", line_width=2))
        if plotted:
            plot.legend.location = "bottom_right"
            plot.legend.click_policy = "hide"
        return plot

    @staticmethod
    def _ellipse_stats(df, x_col, y_col):
        work = pd.DataFrame({"x": pd.to_numeric(df[x_col], errors="coerce"),
                             "y": pd.to_numeric(df[y_col], errors="coerce")}).dropna()
        if len(work) < 2:
            return None
        covariance = np.cov(work["x"], work["y"])
        eigenvalues, eigenvectors = np.linalg.eigh(covariance)
        order = np.argsort(eigenvalues)[::-1]
        eigenvalues, eigenvectors = eigenvalues[order], eigenvectors[:, order]
        semi_axes = np.sqrt(np.maximum(eigenvalues, 0) * 5.991)
        angle = math.atan2(float(eigenvectors[1, 0]), float(eigenvectors[0, 0]))
        return {
            "min_x": float(work["x"].min()), "max_x": float(work["x"].max()),
            "avg_x": float(work["x"].mean()), "min_y": float(work["y"].min()),
            "max_y": float(work["y"].max()), "avg_y": float(work["y"].mean()),
            "mean_x": float(work["x"].mean()), "mean_y": float(work["y"].mean()),
            "major": float(semi_axes[0] * 2), "minor": float(semi_axes[1] * 2),
            "angle_rad": angle, "azimuth": float((90.0 - np.degrees(angle)) % 180.0),
        }

    def _xy_plot(self, df, title, x_col, y_col, rov_col, qc_limit):
        plot = figure(
            height=440, sizing_mode="stretch_width", title=title,
            x_axis_label="Delta Easting (m)", y_axis_label="Delta Northing (m)",
            tools="pan,wheel_zoom,box_zoom,reset,save", active_scroll="wheel_zoom",
            match_aspect=True,
        )
        rovs = df.get(rov_col, pd.Series(["Unknown"] * len(df))).fillna("Unknown").astype(str)
        renderers = []
        for index, rov in enumerate(sorted(rovs.unique())):
            mask = rovs == rov
            source = ColumnDataSource({
                "x": pd.to_numeric(df.loc[mask, x_col], errors="coerce"),
                "y": pd.to_numeric(df.loc[mask, y_col], errors="coerce"),
                "station": df.loc[mask, "Station"].astype(str),
                "node": df.loc[mask, "Node"].fillna("").astype(str),
            })
            renderer = plot.scatter("x", "y", source=source, size=7, alpha=0.72,
                                    color=self.COLORS[index % len(self.COLORS)], legend_label=rov)
            renderers.append(renderer)
        angle = np.linspace(0, 2 * np.pi, 181)
        plot.line(qc_limit * np.cos(angle), qc_limit * np.sin(angle),
                  color="#c73535", line_dash="dashed", line_width=2,
                  legend_label=f"QC limit {qc_limit:.2f} m")
        ellipse = self._ellipse_stats(df, x_col, y_col)
        if ellipse:
            plot.ellipse(x=[ellipse["mean_x"]], y=[ellipse["mean_y"]],
                         width=[ellipse["major"]], height=[ellipse["minor"]],
                         angle=[ellipse["angle_rad"]], fill_alpha=0.08,
                         fill_color="#6f4ca5", line_color="#6f4ca5",
                         line_width=2, line_dash="dashed", legend_label="95% ellipse")
        plot.add_tools(HoverTool(renderers=renderers, tooltips=[
            ("Station", "@station"), ("Node", "@node"),
            ("Delta E", "@x{0.00} m"), ("Delta N", "@y{0.00} m"),
        ]))
        plot.legend.click_policy = "hide"
        return plot

    def _water_depth_plot(self, df, x_range=None):
        plot = figure(
            height=420, sizing_mode="stretch_width", title="Deployment and Recovery Water Depth",
            x_axis_label="Station", y_axis_label="Water depth (m)",
            tools="pan,wheel_zoom,box_zoom,reset,save,hover", active_scroll="wheel_zoom",
            tooltips=[("Station", "@station"), ("Value", "$y{0.00} m")],
            x_range=x_range,
        )
        source = ColumnDataSource({
            "station": df["Station"],
            "deployment": pd.to_numeric(df["dep_z"], errors="coerce").abs(),
            "recovery": pd.to_numeric(df["rcv_z"], errors="coerce").abs(),
        })
        for field, label, color, dash in (
            ("deployment", "Deployment", self.COLORS[0], "solid"),
            ("recovery", "Recovery", self.COLORS[1], "dashed"),
        ):
            plot.line("station", field, source=source, color=color, line_width=2,
                      line_dash=dash, legend_label=label)
        plot.legend.click_policy = "hide"
        return plot

    def _water_depth_difference_plot(self, df, x_range=None):
        deployment = pd.to_numeric(df["dep_z"], errors="coerce").abs()
        recovery = pd.to_numeric(df["rcv_z"], errors="coerce").abs()
        source = ColumnDataSource({
            "station": df["Station"],
            "difference": recovery - deployment,
            "deployment": deployment,
            "recovery": recovery,
        })
        plot = figure(
            height=420, sizing_mode="stretch_width",
            title="Recovery Water Depth minus Deployment Water Depth",
            x_axis_label="Station", y_axis_label="Water-depth difference (m)",
            tools="pan,wheel_zoom,box_zoom,reset,save", active_scroll="wheel_zoom",
            x_range=x_range,
        )
        renderer = plot.line("station", "difference", source=source,
                             color=self.COLORS[3], line_width=2.2,
                             legend_label="Recovery - Deployment")
        plot.scatter("station", "difference", source=source, color=self.COLORS[3],
                     size=5, alpha=0.7)
        plot.add_layout(Span(location=0, dimension="width", line_color="#263b5e",
                             line_dash="dashed", line_width=1.5))
        plot.add_tools(HoverTool(renderers=[renderer], tooltips=[
            ("Station", "@station{0}"),
            ("Deployment depth", "@deployment{0.00} m"),
            ("Recovery depth", "@recovery{0.00} m"),
            ("Difference", "@difference{0.00} m"),
        ], mode="vline"))
        plot.legend.location = "top_right"
        return plot

    def _nearest_station_distance_plot(self, df, mode, x_range=None):
        if mode == "deployment":
            x_col, y_col, rov_col = "dep_x", "dep_y", "ROV"
            title = "Distance Between Nearest Deployment Stations"
        else:
            x_col, y_col, rov_col = "rcv_x", "rcv_y", "ROV1"
            title = "Distance Between Nearest Recovery Stations"

        work = pd.DataFrame({
            "station": pd.to_numeric(df["Station"], errors="coerce"),
            "x": pd.to_numeric(df[x_col], errors="coerce"),
            "y": pd.to_numeric(df[y_col], errors="coerce"),
            "rov": df.get(rov_col, pd.Series(["Unknown"] * len(df))).fillna("Unknown").astype(str),
        }).dropna(subset=["station", "x", "y"]).sort_values("station")
        work["previous_station"] = work["station"].shift(1)
        work["distance"] = np.hypot(work["x"].diff(), work["y"].diff())

        station_steps = np.diff(np.sort(work["station"].unique()))
        bar_width = float(np.nanmedian(station_steps) * 0.38) if len(station_steps) else 1.0
        plot = figure(
            height=420, sizing_mode="stretch_width", title=title,
            x_axis_label="Station", y_axis_label="Distance to previous station (m)",
            tools="pan,wheel_zoom,box_zoom,reset,save", active_scroll="wheel_zoom",
            x_range=x_range,
        )
        renderers = []
        for index, rov in enumerate(sorted(work["rov"].unique())):
            selected = work[(work["rov"] == rov) & work["distance"].notna()]
            source = ColumnDataSource({
                "station": selected["station"], "previous_station": selected["previous_station"],
                "distance": selected["distance"], "rov": selected["rov"],
            })
            renderer = plot.vbar(x="station", top="distance", bottom=0, width=bar_width,
                                 source=source, color=self.COLORS[index % len(self.COLORS)],
                                 alpha=0.78, legend_label=rov)
            renderers.append(renderer)
        if renderers:
            plot.add_tools(HoverTool(renderers=renderers, tooltips=[
                ("ROV", "@rov"), ("Previous station", "@previous_station{0}"),
                ("Station", "@station{0}"), ("Distance", "@distance{0.00} m"),
            ]))
            plot.legend.click_policy = "hide"
            plot.legend.location = "top_right"
        return plot

    def _boxplot(self, df, title, prefix):
        labels, q1s, q2s, q3s, lows, highs = [], [], [], [], [], []
        for suffix, label in (("dx", "dE"), ("dy", "dN"), ("il", "In-Line"),
                              ("xl", "Cross-Line"), ("dr", "Radial")):
            values = self._finite(df[f"{prefix}_{suffix}"])
            if not len(values):
                continue
            q1, q2, q3 = np.percentile(values, [25, 50, 75])
            iqr = q3 - q1
            labels.append(label); q1s.append(q1); q2s.append(q2); q3s.append(q3)
            lows.append(max(values.min(), q1 - 1.5 * iqr)); highs.append(min(values.max(), q3 + 1.5 * iqr))
        source = ColumnDataSource(dict(label=labels, q1=q1s, q2=q2s, q3=q3s, low=lows, high=highs))
        plot = figure(x_range=labels, height=420, sizing_mode="stretch_width",
                      title=f"Boxplots — {title}", y_axis_label="Offset (m)",
                      tools="pan,wheel_zoom,box_zoom,reset,save,hover",
                      tooltips=[("Comparison", "@label"), ("Median", "@q2{0.00} m"),
                                ("Q1", "@q1{0.00} m"), ("Q3", "@q3{0.00} m")])
        if labels:
            plot.segment("label", "high", "label", "q3", source=source, color="#263b5e")
            plot.segment("label", "low", "label", "q1", source=source, color="#263b5e")
            plot.vbar("label", 0.65, "q2", "q3", source=source, fill_color="#8eb6d8", line_color="#263b5e")
            plot.vbar("label", 0.65, "q1", "q2", source=source, fill_color="#bdd5e8", line_color="#263b5e")
        plot.xaxis.major_label_orientation = 0.7
        return plot

    def _heatmap(self, df, comparisons, qc_limit):
        station, comparison, radial = [], [], []
        for title, prefix, _color in comparisons:
            values = pd.to_numeric(df[f"{prefix}_dr"], errors="coerce")
            station.extend(df["Station"].astype(str)); comparison.extend([title] * len(df)); radial.extend(values)
        finite_radial = np.asarray(radial, dtype=float)
        finite_radial = finite_radial[np.isfinite(finite_radial)]
        high = max(qc_limit, float(np.percentile(finite_radial, 98))) if len(finite_radial) else qc_limit
        mapper = LinearColorMapper(palette=Turbo256, low=0, high=high)
        source = ColumnDataSource(dict(station=station, comparison=comparison, radial=radial))
        heatmap_rows = [c[0] for c in comparisons[:3]] + [" "] + [c[0] for c in comparisons[3:]]
        plot = figure(x_range=list(dict.fromkeys(station)), y_range=heatmap_rows[::-1],
                      height=470, sizing_mode="stretch_width", title="Radial Offset Heatmap",
                      x_axis_label="Station", tools="pan,wheel_zoom,box_zoom,reset,save,hover",
                      tooltips=[("Station", "@station"), ("Comparison", "@comparison"),
                                ("Radial", "@radial{0.00} m")])
        plot.rect("station", "comparison", 1, 1, source=source,
                  fill_color={"field": "radial", "transform": mapper}, line_color=None)
        plot.add_layout(ColorBar(color_mapper=mapper, title="Offset (m)"), "right")
        plot.xaxis.major_label_orientation = 1.2
        return plot

    def _offset_histogram(self, df, title, prefix, suffix, metric_label, color):
        values = self._finite(df[f"{prefix}_{suffix}"])
        std = float(np.std(values)) if len(values) else float("nan")
        std_text = f"{std:.2f} m" if np.isfinite(std) else "—"
        plot = figure(height=400, sizing_mode="stretch_width",
                      title=f"{title} — {metric_label} · STD {std_text}",
                      x_axis_label=f"{metric_label} (m)", y_axis_label="Nodes (%)",
                      tools="pan,wheel_zoom,box_zoom,reset,save,hover", active_scroll="wheel_zoom")
        if len(values):
            lower = 0.0 if suffix == "dr" else float(np.floor(values.min() / 0.5) * 0.5)
            upper = max(10.0, float(np.ceil(values.max() / 0.5) * 0.5)) if suffix == "dr" else float(np.ceil(values.max() / 0.5) * 0.5)
            if upper <= lower:
                upper = lower + 0.5
            edges = np.arange(lower, upper + 0.5, 0.5)
            counts, edges = np.histogram(values, bins=edges)
            percent = counts / len(values) * 100.0
            centers = (edges[:-1] + edges[1:]) / 2.0
            source = ColumnDataSource(dict(offset=centers, percentage=percent,
                                           left=edges[:-1], right=edges[1:]))
            renderer = plot.quad(top="percentage", bottom=0, left="left", right="right",
                                 source=source, fill_color=color, fill_alpha=0.45,
                                 line_color=color, legend_label="Histogram")
            plot.add_tools(HoverTool(renderers=[renderer], tooltips=[
                ("Offset", "@offset{0.00} m"), ("Nodes", "@percentage{0.0}%")
            ]))
            bandwidth = max(1.06 * max(std, 1e-6) * len(values) ** (-0.2), 0.15)
            grid = np.linspace(lower, upper, 240)
            density = np.exp(-0.5 * ((grid[:, None] - values[None, :]) / bandwidth) ** 2).mean(axis=1)
            density /= bandwidth * np.sqrt(2 * np.pi)
            kde_percent = density * 0.5 * 100.0
            plot.line(grid, kde_percent, color="#132a4f", line_width=3, legend_label="KDE")
        if len(values):
            plot.legend.click_policy = "hide"
        return plot

    @staticmethod
    def _sector_values(df, radial_col, azimuth_col, sector_width=10.0):
        radial = pd.to_numeric(df[radial_col], errors="coerce")
        azimuth = pd.to_numeric(df[azimuth_col], errors="coerce")
        mask = radial.notna() & azimuth.notna()
        work = pd.DataFrame({"radial": radial[mask], "azimuth": azimuth[mask]})
        if work.empty:
            return np.array([]), np.array([]), np.array([]), np.array([])
        work["sector"] = np.floor((work["azimuth"] % 360.0) / sector_width).astype(int)
        grouped = work.groupby("sector")["radial"].agg(["mean", "count"]).reindex(
            range(int(360 / sector_width)), fill_value=0
        )
        theta = grouped.index.to_numpy(float) * sector_width + sector_width / 2.0
        percentage = grouped["count"].to_numpy(float) / len(work) * 100.0
        return theta, grouped["mean"].fillna(0).to_numpy(float), percentage, grouped["count"].to_numpy(int)

    def _polar_html(self, df, title, radial_col, azimuth_col, percentage_scale_max):
        theta, average, percentage, counts = self._sector_values(df, radial_col, azimuth_col)
        dominant = int(np.argmax(counts)) if len(counts) else 0
        custom = np.column_stack((percentage, counts)) if len(counts) else np.empty((0, 2))
        fig = go.Figure(go.Barpolar(
            r=average, theta=theta, width=[10.0] * len(theta), customdata=custom,
            marker=dict(color=percentage, colorscale="Turbo", cmin=0,
                        cmax=percentage_scale_max, showscale=True,
                        colorbar=dict(title="Nodes (%)", thickness=14)),
            hovertemplate="Sector %{theta:.0f}°<br>Average offset %{r:.2f} m<br>Nodes %{customdata[1]:.0f} (%{customdata[0]:.1f}%)<extra></extra>",
        ))
        annotation = "No valid nodes"
        if len(counts) and counts[dominant] > 0:
            annotation = (f"Dominant sector: {theta[dominant]:.0f}° · "
                          f"{counts[dominant]} nodes ({percentage[dominant]:.1f}%)")
        fig.update_layout(
            title=title, height=500, margin=dict(l=55, r=80, t=60, b=40),
            template="plotly_white", showlegend=False,
            polar=dict(
                angularaxis=dict(direction="clockwise", rotation=90),
                radialaxis=dict(title="Average radial offset (m)"),
            ),
            annotations=[dict(text=annotation, x=0.5, y=-0.08, xref="paper", yref="paper",
                              showarrow=False)],
        )
        return pio.to_html(fig, full_html=False, include_plotlyjs=False,
                           config={"responsive": True, "displaylogo": False})

    def _polar_statistics_html(self, df, title, prefix):
        radial = pd.to_numeric(df[f"{prefix}_dr"], errors="coerce")
        azimuth = pd.to_numeric(df[f"{prefix}_az"], errors="coerce") % 360.0
        work = pd.DataFrame({"radial": radial, "azimuth": azimuth}).dropna()

        if work.empty:
            mean_direction = mean_vector = circular_std = None
        else:
            radians = np.radians(work["azimuth"].to_numpy(float))
            mean_sin = float(np.mean(np.sin(radians)))
            mean_cos = float(np.mean(np.cos(radians)))
            mean_direction = (math.degrees(math.atan2(mean_sin, mean_cos)) + 360.0) % 360.0
            mean_vector = float(np.hypot(mean_sin, mean_cos))
            circular_std = float(np.degrees(np.sqrt(max(0.0, -2.0 * np.log(max(mean_vector, 1e-12))))))

        directional_rows = (
            ("Mean direction", f"{mean_direction:.1f}°" if mean_direction is not None else "—"),
            ("Mean vector length", f"{mean_vector:.2f}" if mean_vector is not None else "—"),
            ("Circular std. dev.", f"{circular_std:.1f}°" if circular_std is not None else "—"),
            ("Observations", str(len(work))),
        )
        directional_body = "".join(
            f"<tr><td>{label}</td><td>{value}</td></tr>" for label, value in directional_rows
        )

        sector_rows = []
        total = len(work)
        for sector_start in range(0, 360, 45):
            sector_end = sector_start + 45
            selected = work[(work["azimuth"] >= sector_start) & (work["azimuth"] < sector_end)]
            count = len(selected)
            percentage = count / total * 100.0 if total else 0.0
            average_radial = float(selected["radial"].mean()) if count else None
            sector_rows.append((sector_start, sector_end, count, percentage, average_radial))
        dominant_count = max((row[2] for row in sector_rows), default=0)
        sector_body = "".join(
            f'<tr class="{"dominant-sector" if count == dominant_count and count > 0 else ""}">'
            f"<td>{start}-{end}</td><td>{count}</td><td>{percentage:.1f}%</td>"
            f"<td>{average:.2f}</td></tr>" if average is not None else
            f'<tr class="{"dominant-sector" if count == dominant_count and count > 0 else ""}">'
            f"<td>{start}-{end}</td><td>{count}</td><td>{percentage:.1f}%</td><td>—</td></tr>"
            for start, end, count, percentage, average in sector_rows
        )

        return (
            f'<div class="panel polar-stat-panel"><h2>{html.escape(title)}</h2>'
            f'<table class="polar-stat-table"><thead><tr><th>Directional Statistics</th><th>Value</th></tr></thead>'
            f'<tbody>{directional_body}</tbody></table>'
            f'<table class="polar-sector-table"><thead><tr><th>Sector (°)</th><th>Nodes</th>'
            f'<th>Line nodes (%)</th><th>Avg radial (m)</th></tr></thead><tbody>{sector_body}</tbody></table></div>'
        )

    def generate_html(self, line, output_dir):
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        df = self.pdf_report.load_line_data(line)
        if df.empty:
            raise ValueError(f"No data found for line {line}")

        comparisons = [
            ("Deployment vs Preplot", "dep_pp", self.COLORS[0]),
            ("Recovery vs Preplot", "rcv_pp", self.COLORS[1]),
            ("REC_DB vs Preplot", "rec_pp", self.COLORS[2]),
            ("REC_DB vs Deployment", "fb_dep", self.COLORS[3]),
            ("REC_DB vs Recovery", "fb_rcv", self.COLORS[4]),
        ]
        qc = self.pdf_report.load_node_qc_settings()
        qc_limit = float(qc.get("max_radial_offset") or 30.0)
        sector_percentages = [
            self._sector_values(df, f"{prefix}_dr", f"{prefix}_az")[2]
            for _title, prefix, _color in comparisons
        ]
        percentage_scale_max = max(
            [float(np.max(values)) for values in sector_percentages if len(values)] or [1.0]
        )

        station_values = pd.to_numeric(df["Station"], errors="coerce").dropna()
        shared_station_range = Range1d(start=float(station_values.min()), end=float(station_values.max()))
        series = (
            ("dep_pp", "Deployment", self.COLORS[0], "solid"),
            ("rcv_pp", "Recovery", self.COLORS[1], "dashed"),
            ("rec_pp", "REC_DB", self.COLORS[2], "dotted"),
        )
        bokeh_plots = {
            "station_de": self._station_plot(df, "Delta Easting vs Station",
                [(f"{p}_dx", f"{label} dE", color, dash) for p, label, color, dash in series], shared_station_range),
            "station_dn": self._station_plot(df, "Delta Northing vs Station",
                [(f"{p}_dy", f"{label} dN", color, dash) for p, label, color, dash in series], shared_station_range),
            "station_radial": self._station_plot(df, "Radial Offset vs Station",
                [(f"{p}_dr", f"{label} radial", color, dash) for p, label, color, dash in series], shared_station_range),
            "station_inline": self._station_plot(df, "In-Line Offset vs Station",
                [(f"{p}_il", f"{label} in-line", color, dash) for p, label, color, dash in series], shared_station_range),
            "station_xline": self._station_plot(df, "Cross-Line Offset vs Station",
                [(f"{p}_xl", f"{label} cross-line", color, dash) for p, label, color, dash in series], shared_station_range),
            "station_depth": self._water_depth_plot(df, shared_station_range),
            "station_depth_difference": self._water_depth_difference_plot(df, shared_station_range),
            "deployment_recovery_delta": self._station_plot(
                df, "Deployment vs Recovery — Delta Easting and Delta Northing", [
                    ("dep_rcv_dx", "Deployment - Recovery dE", self.COLORS[0], "solid"),
                    ("dep_rcv_dy", "Deployment - Recovery dN", self.COLORS[1], "dashed"),
                ], shared_station_range
            ),
            "nearest_station_deployment": self._nearest_station_distance_plot(
                df, "deployment", shared_station_range
            ),
            "nearest_station_recovery": self._nearest_station_distance_plot(
                df, "recovery", shared_station_range
            ),
        }
        xy_specs = [
            ("Deployment vs Preplot", "dep_pp_dx", "dep_pp_dy", "ROV"),
            ("Recovery vs Preplot", "rcv_pp_dx", "rcv_pp_dy", "ROV1"),
            ("REC_DB vs Preplot", "rec_pp_dx", "rec_pp_dy", "ROV1"),
            ("REC_DB vs Deployment", "fb_dep_dx", "fb_dep_dy", "ROV"),
            ("REC_DB vs Recovery", "fb_rcv_dx", "fb_rcv_dy", "ROV1"),
        ]
        for index, (title, x_col, y_col, rov_col) in enumerate(xy_specs):
            bokeh_plots[f"xy_{index}"] = self._xy_plot(df, title, x_col, y_col, rov_col, qc_limit)
        bokeh_plots["heatmap"] = self._heatmap(df, comparisons, qc_limit)
        for index, (title, prefix, color) in enumerate(comparisons):
            bokeh_plots[f"cdf_{index}"] = self._cdf_metrics_plot(df, prefix, f"CDF — {title}")
            bokeh_plots[f"box_{index}"] = self._boxplot(df, title, prefix)
            if index < 3:
                for suffix, metric_label, metric_color in (
                    ("dx", "Delta Easting", self.COLORS[0]),
                    ("dy", "Delta Northing", self.COLORS[1]),
                    ("dr", "Radial Offset", color),
                ):
                    bokeh_plots[f"hist_{index}_{suffix}"] = self._offset_histogram(
                        df, title, prefix, suffix, metric_label, metric_color
                    )

        scripts, divs = components(bokeh_plots)
        polar_divs = [
            self._polar_html(df, title, f"{prefix}_dr", f"{prefix}_az", percentage_scale_max)
            for title, prefix, _color in comparisons
        ]
        polar_statistics_panels = [
            self._polar_statistics_html(df, title, prefix)
            for title, prefix, _color in comparisons
        ]

        logo_path = self.pdf_report._find_tgs_logo()
        logo_mime = "image/png" if logo_path.suffix.lower() == ".png" else "image/jpeg"
        logo = base64.b64encode(logo_path.read_bytes()).decode("ascii")
        summary = self.pdf_report.load_line_summary(line)
        project = self.pdf_report.load_project_main()

        cards = []
        for title, prefix, _color in comparisons[:3]:
            stats = self._stats(df[f"{prefix}_dr"])
            cards.append(
                f'<div class="metric"><span>{html.escape(title)}</span>'
                f'<b>{stats["nodes"]} nodes</b><small>Mean {self._fmt(stats["mean"])} m · '
                f'P50 {self._fmt(stats["p50"])} m · P95 {self._fmt(stats["p95"])} m</small></div>'
            )
        primary_max = max([
            float(np.nanmax(self._finite(df[f"{prefix}_dr"])))
            for _title, prefix, _color in comparisons[:3]
            if len(self._finite(df[f"{prefix}_dr"]))
        ] or [0.0])
        overall_status = "PASS" if primary_max <= qc_limit else "WARNING"

        percentile_rows = []
        for title, prefix, _color in comparisons:
            values = self._finite(df[f"{prefix}_dr"])
            if len(values):
                percentile_rows.append(
                    f"<tr><td>{html.escape(title)}</td><td>{len(values)}</td>"
                    f"<td>{np.mean(values):.2f}</td><td>{np.std(values):.2f}</td>"
                    f"<td>{np.percentile(values, 50):.2f}</td><td>{np.percentile(values, 95):.2f}</td>"
                    f"<td>{np.max(values):.2f}</td></tr>"
                )
            else:
                percentile_rows.append(
                    f"<tr><td>{html.escape(title)}</td><td>0</td>"
                    f"<td>—</td><td>—</td><td>—</td><td>—</td><td>—</td></tr>"
                )

        xy_panels = []
        for index, (title, x_col, y_col, _rov_col) in enumerate(xy_specs):
            stats = self._ellipse_stats(df, x_col, y_col)
            if stats:
                stat_rows = (
                    ("Min ΔX (m)", stats["min_x"]), ("Max ΔX (m)", stats["max_x"]),
                    ("Avg ΔX (m)", stats["avg_x"]), ("Min ΔY (m)", stats["min_y"]),
                    ("Max ΔY (m)", stats["max_y"]), ("Avg ΔY (m)", stats["avg_y"]),
                    ("95% Ellipse Major (m)", stats["major"]),
                    ("95% Ellipse Minor (m)", stats["minor"]),
                    ("95% Ellipse Azimuth (°)", stats["azimuth"]),
                )
                table = "".join(
                    f'<tr class="{"avg-row" if label.startswith("Avg") else ""}"><td>{label}</td><td>{value:.2f}</td></tr>'
                    for label, value in stat_rows
                )
            else:
                table = '<tr><td colspan="2">Insufficient data</td></tr>'
            xy_panels.append(
                f'<div class="panel xy-panel">{divs[f"xy_{index}"]}'
                f'<table class="xy-table"><thead><tr><th>{html.escape(title)}</th><th>Value</th></tr></thead>'
                f'<tbody>{table}</tbody></table></div>'
            )

        line_fields = [
            ("PlannedPoints", "Planned nodes"), ("DeployedCount", "Deployed"),
            ("RetrievedCount", "Recovered"), ("ProcessedCount", "Processed"),
            ("FirstDeployTime", "First deployment"), ("LastDeployTime", "Last deployment"),
            ("StartOfRec", "Start of recovery"), ("EndOfRec", "End of recovery"),
            ("DeployedPct", "Deployment (%)"), ("RetrievedPct", "Recovery (%)"),
            ("ProcessedPct", "Processing (%)"),
        ]
        line_rows = "".join(
            f"<tr><th>{html.escape(label)}</th><td>{html.escape(str(summary.get(key, '—') or '—'))}</td></tr>"
            for key, label in line_fields
        )

        table_cols = [
            ("Line", "Line"), ("Station", "Station"), ("Node", "Node ID"),
            ("ROV", "Dep ROV"), ("ROV1", "Rec ROV"),
            ("pp_x", "Preplot X"), ("pp_y", "Preplot Y"),
            ("dep_x", "Deployment X"), ("dep_y", "Deployment Y"), ("dep_z", "Deployment Z"),
            ("rcv_x", "Recovery X"), ("rcv_y", "Recovery Y"), ("rcv_z", "Recovery Z"),
            ("fb_x", "REC_DB X"), ("fb_y", "REC_DB Y"), ("fb_z", "REC_DB Z"),
            ("dep_pp_dx", "Dep dE"), ("dep_pp_dy", "Dep dN"),
            ("dep_pp_il", "Dep IL"), ("dep_pp_xl", "Dep XL"), ("dep_pp_dr", "Dep radial"),
            ("rcv_pp_dx", "Rec dE"), ("rcv_pp_dy", "Rec dN"),
            ("rcv_pp_il", "Rec IL"), ("rcv_pp_xl", "Rec XL"), ("rcv_pp_dr", "Rec radial"),
            ("rec_pp_dx", "REC_DB dE"), ("rec_pp_dy", "REC_DB dN"),
            ("rec_pp_il", "REC_DB IL"), ("rec_pp_xl", "REC_DB XL"), ("rec_pp_dr", "REC_DB radial"),
        ]
        head = "".join(f"<th>{html.escape(label)}</th>" for _col, label in table_cols)
        rows = []
        for _, row in df.iterrows():
            cells = []
            for col, _label in table_cols:
                value = row.get(col)
                if col in {"Line", "Station"} and pd.notna(value):
                    value = str(int(value))
                elif col not in {"Node", "ROV", "ROV1"} and pd.notna(value):
                    value = self._fmt(float(value)) if pd.notna(value) else "—"
                elif pd.isna(value):
                    value = ""
                cells.append(f"<td>{html.escape(str(value))}</td>")
            rows.append("<tr>" + "".join(cells) + "</tr>")

        project_name = project.get("name") or ""
        planned = summary.get("PlannedPoints", "—")
        output_path = output_dir / f"{line}_Node_Position_Comparison_Interactive.html"
        document = f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Line {line} Interactive Position Report</title>{INLINE.render_css()}
{INLINE.render_js()}<script>{get_plotlyjs()}</script>
<style>
body{{margin:0;background:#f3f6f9;color:#132a4f;font-family:Arial,sans-serif}}main{{max-width:1500px;margin:auto;padding:18px}}
header{{display:flex;align-items:center;gap:18px;background:#fff;border:2px solid #12336b;padding:14px 18px}}header img{{width:58px;height:66px;object-fit:contain}}h1{{font-size:25px;margin:0}}.sub{{margin-top:6px;color:#53647d}}
.metrics{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:16px 0}}.metric{{background:#fff;border:1px solid #ccd5e1;padding:13px;border-radius:6px}}.metric span,.metric small{{display:block}}.metric b{{display:block;font-size:18px;margin:7px 0}}
.tabs{{display:flex;gap:7px;flex-wrap:wrap;margin-bottom:12px}}button{{padding:9px 14px;border:1px solid #6e819e;background:#fff;color:#132a4f;border-radius:4px;cursor:pointer}}button.active{{background:#12336b;color:#fff}}section.page{{display:none}}section.page.active{{display:block}}.grid{{display:grid;grid-template-columns:1fr 1fr;gap:14px}}.xy-grid,.three-col{{grid-template-columns:repeat(3,1fr)}}.panel{{background:#fff;border:1px solid #ccd5e1;padding:8px;min-width:0}}.wide{{grid-column:1/-1}}.table-wrap{{max-height:680px;overflow:auto;background:#fff}}table{{border-collapse:collapse;width:100%;font-size:12px}}th,td{{padding:7px 9px;border-bottom:1px solid #e2e7ed;text-align:right;white-space:nowrap}}th{{position:sticky;top:0;background:#12336b;color:#fff}}@media(max-width:1100px){{.xy-grid,.three-col{{grid-template-columns:1fr 1fr}}}}@media(max-width:900px){{.grid,.metrics,.xy-grid,.three-col{{grid-template-columns:1fr}}.wide{{grid-column:auto}}}}
.summary-table th{{position:static;text-align:left;width:55%}}.summary-table td{{text-align:left}}.xy-table{{margin-top:8px}}.xy-table th{{position:static;background:#dfeaf3;color:#172a49}}.xy-table td:first-child{{text-align:left}}.xy-table .avg-row td{{font-weight:bold;color:#1549d1}}.note{{color:#53647d}}
.polar-stat-panel h2{{text-align:center}}.polar-stat-table{{width:70%;margin:0 auto 22px}}.polar-sector-table{{width:100%}}.polar-stat-table th,.polar-sector-table th{{position:static;background:#287fb5;color:#fff;text-align:center}}.polar-stat-table td,.polar-sector-table td{{text-align:center}}.polar-stat-table td:first-child{{text-align:left}}.dominant-sector td{{font-weight:bold;background:#e8f3fa}}
</style></head><body><main><header><img alt="TGS logo" src="data:{logo_mime};base64,{logo}"><div><h1>NODE POSITION COMPARISON — INTERACTIVE REPORT</h1><div class="sub">Project: {html.escape(str(project_name))} · Receiver Line: {line} · Planned nodes: {planned}</div></div></header>
<div class="metrics">{''.join(cards)}</div><nav class="tabs"><button class="active" data-page="summary">Summary</button><button data-page="xy">XY offsets</button><button data-page="station">Station plots</button><button data-page="depl-vs-preplot">Depl vs Preplot</button><button data-page="cdf">CDF</button><button data-page="histograms">Histograms</button><button data-page="boxplots">Boxplots</button><button data-page="heatmap">Heatmap</button><button data-page="polar">Polar plots</button><button data-page="polar-stats">Polar statistics</button><button data-page="nodes">Node table</button></nav>
<section id="summary" class="page active"><h2>Executive Summary - Overall QC Status: {overall_status}</h2><p class="note">Maximum primary radial offset: {primary_max:.2f} m · Project radial QC limit: {qc_limit:.2f} m</p><div class="grid"><div class="panel"><h2>Line Information</h2><table class="summary-table"><tbody>{line_rows}</tbody></table></div><div class="panel"><h2>Radial Offset Statistics</h2><table><thead><tr><th>Comparison</th><th>Nodes</th><th>Mean</th><th>STD</th><th>P50</th><th>P95</th><th>Max</th></tr></thead><tbody>{''.join(percentile_rows)}</tbody></table><p class="note">All offset values are in meters.</p></div></div></section>
<section id="xy" class="page"><div class="grid xy-grid">{''.join(xy_panels)}</div></section>
<section id="station" class="page"><div class="grid">{''.join(f'<div class="panel">{divs[key]}</div>' for key in ('station_de','station_dn','station_radial','station_inline','station_xline'))}</div></section>
<section id="depl-vs-preplot" class="page"><div class="grid"><div class="panel wide">{divs['deployment_recovery_delta']}</div><div class="panel wide">{divs['station_depth']}</div><div class="panel wide">{divs['station_depth_difference']}</div></div></section>
<section id="cdf" class="page"><div class="grid">{''.join(f'<div class="panel">{divs[f"cdf_{index}"]}</div>' for index in range(5))}</div></section>
<section id="histograms" class="page"><div class="grid three-col">{''.join(f'<div class="panel">{divs[f"hist_{index}_{suffix}"]}</div>' for index in range(3) for suffix in ("dx","dy","dr"))}</div></section>
<section id="boxplots" class="page"><div class="grid">{''.join(f'<div class="panel">{divs[f"box_{index}"]}</div>' for index in range(5))}</div></section>
<section id="heatmap" class="page"><div class="grid"><div class="panel wide">{divs['heatmap']}</div><div class="panel">{divs['nearest_station_deployment']}</div><div class="panel">{divs['nearest_station_recovery']}</div></div></section>
<section id="polar" class="page"><div class="grid">{''.join(f'<div class="panel">{item}</div>' for item in polar_divs)}</div></section>
<section id="polar-stats" class="page"><div class="grid xy-grid">{''.join(polar_statistics_panels)}</div></section>
<section id="nodes" class="page"><div class="table-wrap"><table><thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table></div></section>
{scripts}<script>document.querySelectorAll('.tabs button').forEach(function(button){{button.addEventListener('click',function(){{document.querySelectorAll('.tabs button,.page').forEach(function(item){{item.classList.remove('active')}});button.classList.add('active');document.getElementById(button.dataset.page).classList.add('active');window.dispatchEvent(new Event('resize'));}})}});</script>
</main></body></html>'''
        output_path.write_text(document, encoding="utf-8")
        return output_path
