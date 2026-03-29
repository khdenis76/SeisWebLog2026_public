"""
Plotly chart generation for interactive SeisWebLog reports.
"""

from __future__ import annotations

from pathlib import Path
import plotly.express as px
import pandas as pd


class ReportCharts:
    """Build chart blocks for the report."""

    def __init__(self, metrics, export_dir=None):
        self.metrics = metrics
        self.export_dir = Path(export_dir) if export_dir else None
        if self.export_dir:
            self.export_dir.mkdir(parents=True, exist_ok=True)

    def _fig_to_payload(self, fig, name):
        payload = {
            "kind": "plotly",
            "title": name.replace("_", " ").title(),
            "html": fig.to_html(full_html=False, include_plotlyjs=False, config={"responsive": True}),
            "image_path": "",
        }
        if self.export_dir:
            image_path = self.export_dir / f"{name}.png"
            try:
                fig.write_image(str(image_path), scale=2)
                payload["image_path"] = str(image_path)
            except Exception:
                payload["image_path"] = ""
        return payload

    def daily_activity_chart(self):
        df = pd.DataFrame(self.metrics.get("daily_activity", []))
        if df.empty:
            return {"kind": "plotly", "title": "Daily Activity", "html": "<div class='empty-plot'>No data</div>", "image_path": ""}
        fig = px.bar(df, x="day", y=["deployed", "recovered"], barmode="group", title="Daily Deployment / Recovery")
        fig.update_layout(height=420, margin=dict(l=30, r=20, t=60, b=30))
        return self._fig_to_payload(fig, "daily_activity")

    def pie_chart(self, rows, title, name):
        df = pd.DataFrame(rows)
        if df.empty:
            return {"kind": "plotly", "title": title, "html": "<div class='empty-plot'>No data</div>", "image_path": ""}
        fig = px.pie(df, names="name", values="count", hole=0.45, title=title)
        fig.update_layout(height=420, margin=dict(l=20, r=20, t=60, b=20))
        return self._fig_to_payload(fig, name)

    def histogram(self, values, title, name):
        if not values:
            return {"kind": "plotly", "title": title, "html": "<div class='empty-plot'>No data</div>", "image_path": ""}
        df = pd.DataFrame({"value": values})
        fig = px.histogram(df, x="value", nbins=30, title=title)
        fig.update_layout(height=420, margin=dict(l=30, r=20, t=60, b=30))
        return self._fig_to_payload(fig, name)

    def build_all(self):
        fleet = self.metrics.get("fleet", {})
        qc = self.metrics.get("qc", {})
        return {
            "daily_activity": self.daily_activity_chart(),
            "vessel_contribution": self.pie_chart(fleet.get("vessel_summary", []), "Deployment / Recovery by Vessel", "vessel_contribution"),
            "rov_contribution": self.pie_chart(fleet.get("rov_summary", []), "Deployment / Recovery by ROV", "rov_contribution"),
            "battery_histogram": self.histogram(qc.get("battery_values", []), "Battery Life Histogram", "battery_histogram"),
            "radial_offset_histogram": self.histogram(qc.get("radial_offset_values", []), "Radial Offset from Preplot", "radial_offset_histogram"),
        }
