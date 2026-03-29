"""
Map generation utilities.

This first implementation creates static XY scatter maps from available
project coordinates. It does not depend on web tiles, so it is suitable
for server-side report generation and PDF export.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


class ReportMaps:
    """
    Build static map images from project data.
    """

    def __init__(self, raw: Dict[str, pd.DataFrame], output_dir: str):
        self.raw = raw
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def build_all(self, report_type: str = "weekly") -> Dict[str, str]:
        """
        Build all available map images.
        """
        maps = {}
        dsr = self.raw.get("dsr", pd.DataFrame())
        shots = self.raw.get("shots", pd.DataFrame())

        if not dsr.empty:
            dep = self._filter_status(dsr, "Deployed")
            rec = self._filter_status(dsr, "Recovered")

            if not dep.empty:
                img = self._build_xy_map(dep, title="Deployment Progress Map", filename="deployment_map.png")
                if img:
                    maps["deployment_map"] = img

            if not rec.empty:
                img = self._build_xy_map(rec, title="Recovery Progress Map", filename="recovery_map.png")
                if img:
                    maps["recovery_map"] = img

        if not shots.empty:
            img = self._build_xy_map(shots, title="Shooting Progress Map", filename="shooting_map.png")
            if img:
                maps["shooting_map"] = img

        return maps

    def _filter_status(self, df: pd.DataFrame, status: str) -> pd.DataFrame:
        """
        Filter DSR records by Status value.
        """
        if "Status" not in df.columns:
            return pd.DataFrame()
        return df[df["Status"].astype(str).str.strip().eq(status)].copy()

    def _pick_xy_columns(self, df: pd.DataFrame):
        """
        Try several likely XY column names used in SeisWebLog projects.
        """
        x_candidates = ["REC_X", "Easting", "PreplotEasting", "X", "x"]
        y_candidates = ["REC_Y", "Northing", "PreplotNorthing", "Y", "y"]

        x_col = next((c for c in x_candidates if c in df.columns), None)
        y_col = next((c for c in y_candidates if c in df.columns), None)
        return x_col, y_col

    def _build_xy_map(self, df: pd.DataFrame, title: str, filename: str) -> str:
        """
        Create a static XY scatter map.
        """
        x_col, y_col = self._pick_xy_columns(df)
        if not x_col or not y_col:
            return ""

        temp = df.copy()
        temp[x_col] = pd.to_numeric(temp[x_col], errors="coerce")
        temp[y_col] = pd.to_numeric(temp[y_col], errors="coerce")
        temp = temp.dropna(subset=[x_col, y_col])
        if temp.empty:
            return ""

        output = self.output_dir / filename

        fig, ax = plt.subplots(figsize=(8, 8))
        ax.scatter(temp[x_col], temp[y_col], s=8)
        ax.set_title(title)
        ax.set_xlabel(x_col)
        ax.set_ylabel(y_col)
        ax.axis("equal")
        fig.tight_layout()
        fig.savefig(output, dpi=150)
        plt.close(fig)
        return output.name
