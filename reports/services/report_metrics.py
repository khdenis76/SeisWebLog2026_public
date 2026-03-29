"""
Metric calculation layer for report generation.

This module converts raw data frames into compact dictionaries that are
safe to serialize to JSON and easy to display in templates.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


class ReportMetrics:
    """
    Convert raw data frames into summary metrics.
    """

    def __init__(self, raw: Dict[str, pd.DataFrame]):
        self.raw = raw

    def build_all(self, report_type: str = "weekly") -> Dict[str, Any]:
        """
        Build all metric groups used by the report preview.
        """
        return {
            "summary": self.build_summary(),
            "fleet": self.build_fleet_metrics(),
            "qc": self.build_qc_metrics(),
            "daily": self.build_daily_metrics(),
            "report_type": report_type,
        }

    def build_summary(self) -> Dict[str, Any]:
        """
        Build the top summary numbers shown in the report header/cards.
        """
        dsr = self.raw.get("dsr", pd.DataFrame())
        shots = self.raw.get("shots", pd.DataFrame())

        deployed = 0
        recovered = 0
        unique_lines = 0

        if not dsr.empty:
            if "Status" in dsr.columns:
                deployed = int(dsr[dsr["Status"].astype(str).str.strip().eq("Deployed")].shape[0])
                recovered = int(dsr[dsr["Status"].astype(str).str.strip().eq("Recovered")].shape[0])

            line_col = next((c for c in ["Line", "line", "dsr_line"] if c in dsr.columns), None)
            if line_col:
                unique_lines = int(dsr[line_col].astype(str).nunique())

        return {
            "deployed_nodes": deployed,
            "recovered_nodes": recovered,
            "shot_count": int(len(shots.index)) if not shots.empty else 0,
            "active_lines": unique_lines,
            "has_dsr_data": not dsr.empty,
            "has_shot_data": not shots.empty,
        }

    def build_fleet_metrics(self) -> Dict[str, Any]:
        """
        Build per-vessel and per-ROV summaries.
        """
        dsr = self.raw.get("dsr", pd.DataFrame())
        result: Dict[str, Any] = {
            "by_vessel": [],
            "by_rov": [],
        }

        if dsr.empty:
            return result

        vessel_col = next((c for c in ["Vessel", "vessel_name", "VesselName"] if c in dsr.columns), None)
        rov_col = next((c for c in ["ROV", "rov", "dsr_rov"] if c in dsr.columns), None)

        if vessel_col:
            vessel_counts = (
                dsr[vessel_col]
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .value_counts()
                .reset_index()
            )
            vessel_counts.columns = ["name", "count"]
            result["by_vessel"] = vessel_counts.to_dict(orient="records")

        if rov_col:
            rov_counts = (
                dsr[rov_col]
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .value_counts()
                .reset_index()
            )
            rov_counts.columns = ["name", "count"]
            result["by_rov"] = rov_counts.to_dict(orient="records")

        return result

    def build_qc_metrics(self) -> Dict[str, Any]:
        """
        Build simple QC statistics used in the first version.

        The code tries several likely column names because SeisWebLog
        tables may differ slightly between projects.
        """
        dsr = self.raw.get("dsr", pd.DataFrame())
        qc: Dict[str, Any] = {
            "battery_life": {},
            "radial_offset": {},
        }

        if dsr.empty:
            return qc

        for label, candidates in {
            "battery_life": ["BatteryLifeDays", "BatteryLife", "battery_life_days"],
            "radial_offset": ["RadialOffset", "Radial_Offset", "radial_offset"],
        }.items():
            col = next((c for c in candidates if c in dsr.columns), None)
            if not col:
                continue

            series = pd.to_numeric(dsr[col], errors="coerce").dropna()
            if series.empty:
                continue

            qc[label] = {
                "count": int(series.count()),
                "min": round(float(series.min()), 3),
                "max": round(float(series.max()), 3),
                "avg": round(float(series.mean()), 3),
            }

        return qc

    def build_daily_metrics(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Build daily tables for simple preview charts/tables.
        """
        dsr = self.raw.get("dsr", pd.DataFrame())
        shots = self.raw.get("shots", pd.DataFrame())

        result = {
            "deployment_daily": [],
            "shots_daily": [],
        }

        if not dsr.empty:
            ts_col = next((c for c in ["dsr_timestamp", "Timestamp", "timestamp"] if c in dsr.columns), None)
            status_col = "Status" if "Status" in dsr.columns else None
            if ts_col and status_col:
                temp = dsr.copy()
                temp[ts_col] = pd.to_datetime(temp[ts_col], errors="coerce")
                temp = temp.dropna(subset=[ts_col])
                temp["date"] = temp[ts_col].dt.date.astype(str)
                dep = (
                    temp[temp[status_col].astype(str).str.strip().eq("Deployed")]
                    .groupby("date")
                    .size()
                    .reset_index(name="count")
                )
                result["deployment_daily"] = dep.to_dict(orient="records")

        if not shots.empty:
            shot_ts_col = next((c for c in ["shot_time", "source_time", "Timestamp"] if c in shots.columns), None)
            if shot_ts_col:
                temp = shots.copy()
                temp[shot_ts_col] = pd.to_datetime(temp[shot_ts_col], errors="coerce")
                temp = temp.dropna(subset=[shot_ts_col])
                temp["date"] = temp[shot_ts_col].dt.date.astype(str)
                shp = temp.groupby("date").size().reset_index(name="count")
                result["shots_daily"] = shp.to_dict(orient="records")

        return result
