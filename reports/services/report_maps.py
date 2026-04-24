"""
Bokeh map generation for SeisWebLog reports.
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd
from bokeh.embed import components
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.plotting import figure


class ReportMaps:
    """Build Bokeh map figures and return embeddable script/div pairs."""

    def __init__(self, raw, export_dir=None):
        self.raw = raw
        self.export_dir = Path(export_dir) if export_dir else None
        if self.export_dir:
            self.export_dir.mkdir(parents=True, exist_ok=True)

    def _empty_payload(self, title):
        return {"kind": "bokeh", "title": title, "script": "", "div": "<div class='empty-plot'>No map data</div>", "image_path": ""}

    def _build_xy_map(self, df: pd.DataFrame, x_col: str, y_col: str, title: str, color: str = "#59c1ff"):
        if df.empty or x_col not in df.columns or y_col not in df.columns:
            return self._empty_payload(title)
        src = ColumnDataSource(df.fillna(""))
        p = figure(
            height=500,
            sizing_mode="stretch_width",
            title=title,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
            background_fill_color="#0b1420",
            border_fill_color="#0b1420",
        )
        p.scatter(x=x_col, y=y_col, source=src, size=6, alpha=0.8, color=color)
        tooltips = [(c, f"@{c}") for c in [c for c in ["Line", "Station", "Status", "ROV", "Vessel"] if c in df.columns]]
        if tooltips:
            p.add_tools(HoverTool(tooltips=tooltips))
        p.grid.grid_line_alpha = 0.15
        p.xaxis.axis_label = x_col
        p.yaxis.axis_label = y_col
        p.title.text_color = "#dceaf7"
        p.xaxis.axis_label_text_color = "#9db4c9"
        p.yaxis.axis_label_text_color = "#9db4c9"
        p.xaxis.major_label_text_color = "#9db4c9"
        p.yaxis.major_label_text_color = "#9db4c9"
        p.xgrid.grid_line_color = "#24415f"
        p.ygrid.grid_line_color = "#24415f"
        script, div = components(p)
        return {"kind": "bokeh", "title": title, "script": script, "div": div, "image_path": ""}

    def build_all(self):
        dsr = self.raw.get("dsr", pd.DataFrame()).copy()
        shots = self.raw.get("shots", pd.DataFrame()).copy()
        dep_df = dsr[dsr.get("Status", "").astype(str).str.lower() == "deployed"].copy() if not dsr.empty and "Status" in dsr.columns else pd.DataFrame()
        rec_df = dsr[dsr.get("Status", "").astype(str).str.lower() == "recovered"].copy() if not dsr.empty and "Status" in dsr.columns else pd.DataFrame()
        shot_x = next((c for c in ["SRC_X", "Shot_X", "x"] if c in shots.columns), None)
        shot_y = next((c for c in ["SRC_Y", "Shot_Y", "y"] if c in shots.columns), None)
        return {
            "deployment_map": self._build_xy_map(dep_df, "REC_X", "REC_Y", "Deployment Progress Map"),
            "recovery_map": self._build_xy_map(rec_df, "REC_X", "REC_Y", "Recovery Progress Map", color="#86f2a5"),
            "shooting_map": self._build_xy_map(shots, shot_x or "x", shot_y or "y", "Shooting Progress Map", color="#ffbc6a"),
        }
