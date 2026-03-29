"""
Chart generation utilities.

This first version produces static PNG charts using matplotlib. Static
images are practical for:
- HTML preview
- PDF export
- long-term report snapshot storage
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


class ReportCharts:
    """
    Build static chart images for report sections.
    """

    def __init__(self, raw: Dict[str, pd.DataFrame], metrics: Dict[str, Any], output_dir: str):
        self.raw = raw
        self.metrics = metrics
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def build_all(self, report_type: str = "weekly") -> Dict[str, str]:
        """
        Build all available charts and return a dict of relative file paths.
        """
        charts = {}

        daily = self.metrics.get("daily", {})
        fleet = self.metrics.get("fleet", {})

        dep_daily = daily.get("deployment_daily", [])
        if dep_daily:
            charts["daily_deployment_bar"] = self._build_bar_chart(
                dep_daily,
                x_key="date",
                y_key="count",
                title="Daily Deployment",
                filename="daily_deployment_bar.png",
                xlabel="Date",
                ylabel="Nodes",
            )

        shots_daily = daily.get("shots_daily", [])
        if shots_daily:
            charts["daily_shots_bar"] = self._build_bar_chart(
                shots_daily,
                x_key="date",
                y_key="count",
                title="Daily Shot Count",
                filename="daily_shots_bar.png",
                xlabel="Date",
                ylabel="Shots",
            )

        by_vessel = fleet.get("by_vessel", [])
        if by_vessel:
            charts["vessel_pie"] = self._build_pie_chart(
                by_vessel,
                label_key="name",
                value_key="count",
                title="DSR Records by Vessel",
                filename="vessel_pie.png",
            )

        by_rov = fleet.get("by_rov", [])
        if by_rov:
            charts["rov_pie"] = self._build_pie_chart(
                by_rov,
                label_key="name",
                value_key="count",
                title="DSR Records by ROV",
                filename="rov_pie.png",
            )

        qc = self.metrics.get("qc", {})
        dsr = self.raw.get("dsr", pd.DataFrame())
        for key, candidates, title, filename in [
            ("battery_life_hist", ["BatteryLifeDays", "BatteryLife", "battery_life_days"], "Battery Life", "battery_life_hist.png"),
            ("radial_offset_hist", ["RadialOffset", "Radial_Offset", "radial_offset"], "Radial Offset", "radial_offset_hist.png"),
        ]:
            col = next((c for c in candidates if c in dsr.columns), None)
            if col:
                charts[key] = self._build_histogram(dsr, col, title, filename)

        return charts

    def _build_bar_chart(self, rows, x_key, y_key, title, filename, xlabel, ylabel):
        """
        Build a simple bar chart from a list of dictionaries.
        """
        df = pd.DataFrame(rows)
        output = self.output_dir / filename

        fig, ax = plt.subplots(figsize=(10, 4.5))
        ax.bar(df[x_key], df[y_key])
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.tick_params(axis="x", rotation=45)
        fig.tight_layout()
        fig.savefig(output, dpi=150)
        plt.close(fig)
        return output.name

    def _build_pie_chart(self, rows, label_key, value_key, title, filename):
        """
        Build a simple pie chart.
        """
        df = pd.DataFrame(rows)
        output = self.output_dir / filename

        fig, ax = plt.subplots(figsize=(7, 7))
        ax.pie(df[value_key], labels=df[label_key], autopct="%1.1f%%")
        ax.set_title(title)
        fig.tight_layout()
        fig.savefig(output, dpi=150)
        plt.close(fig)
        return output.name

    def _build_histogram(self, df: pd.DataFrame, col: str, title: str, filename: str):
        """
        Build histogram for a numeric column if values exist.
        """
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            return ""

        output = self.output_dir / filename
        fig, ax = plt.subplots(figsize=(10, 4.5))
        ax.hist(series, bins=25)
        ax.set_title(title)
        ax.set_xlabel(col)
        ax.set_ylabel("Frequency")
        fig.tight_layout()
        fig.savefig(output, dpi=150)
        plt.close(fig)
        return output.name
