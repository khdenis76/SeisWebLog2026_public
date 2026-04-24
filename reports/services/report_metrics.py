"""
Metric aggregation for SeisWebLog reports.
"""

from __future__ import annotations

import pandas as pd


class ReportMetrics:
    """Build summary/fleet/QC metrics from raw DataFrames."""

    def __init__(self, raw):
        self.raw = raw

    @staticmethod
    def _safe_len(df: pd.DataFrame) -> int:
        return 0 if df is None or df.empty else int(len(df))

    def build_summary(self):
        dsr = self.raw.get("dsr", pd.DataFrame())
        shots = self.raw.get("shots", pd.DataFrame())
        status_col = "Status" if "Status" in dsr.columns else None
        deployed = int((dsr[status_col].astype(str).str.lower() == "deployed").sum()) if status_col else 0
        recovered = int((dsr[status_col].astype(str).str.lower() == "recovered").sum()) if status_col else 0
        total_nodes = self._safe_len(dsr)
        shot_count = self._safe_len(shots)
        line_col = "Line" if "Line" in dsr.columns else None
        active_lines = int(dsr[line_col].nunique()) if line_col else 0
        return {
            "deployed_nodes": deployed,
            "recovered_nodes": recovered,
            "shot_count": shot_count,
            "total_nodes": total_nodes,
            "active_lines": active_lines,
        }

    def build_daily_activity(self):
        dsr = self.raw.get("dsr", pd.DataFrame()).copy()
        if dsr.empty:
            return pd.DataFrame(columns=["day", "deployed", "recovered"])
        time_col = "Timestamp" if "Timestamp" in dsr.columns else ("Timestamp1" if "Timestamp1" in dsr.columns else None)
        if not time_col:
            return pd.DataFrame(columns=["day", "deployed", "recovered"])
        dsr["day"] = pd.to_datetime(dsr[time_col], errors="coerce").dt.date
        dsr["status_l"] = dsr.get("Status", "").astype(str).str.lower()
        daily = (
            dsr.groupby("day", dropna=True)["status_l"]
            .agg(
                deployed=lambda s: int((s == "deployed").sum()),
                recovered=lambda s: int((s == "recovered").sum()),
            )
            .reset_index()
        )
        return daily

    def build_fleet_metrics(self):
        dsr = self.raw.get("dsr", pd.DataFrame()).copy()
        blackbox = self.raw.get("blackbox", pd.DataFrame()).copy()
        vessel_col = next((c for c in ["Vessel", "vessel_name", "VesselName"] if c in dsr.columns), None)
        rov_col = next((c for c in ["ROV", "ROV1", "rov_name"] if c in dsr.columns), None)
        vessel_summary = dsr.groupby(vessel_col).size().reset_index(name="count") if vessel_col else pd.DataFrame(columns=["name", "count"])
        if not vessel_summary.empty:
            vessel_summary.columns = ["name", "count"]
        rov_summary = dsr.groupby(rov_col).size().reset_index(name="count") if rov_col else pd.DataFrame(columns=["name", "count"])
        if not rov_summary.empty:
            rov_summary.columns = ["name", "count"]
        speed_col = next((c for c in ["Speed", "speed", "VesselSpeed"] if c in blackbox.columns), None)
        vessel_bb_col = next((c for c in ["Vessel", "vessel_name", "VesselName"] if c in blackbox.columns), None)
        avg_speed = blackbox.groupby(vessel_bb_col)[speed_col].mean().reset_index() if speed_col and vessel_bb_col else pd.DataFrame(columns=["name", "avg_speed"])
        if not avg_speed.empty:
            avg_speed.columns = ["name", "avg_speed"]
            avg_speed["avg_speed"] = avg_speed["avg_speed"].round(2)
        return {
            "vessel_summary": vessel_summary,
            "rov_summary": rov_summary,
            "avg_speed": avg_speed,
        }

    def build_qc_metrics(self):
        dsr = self.raw.get("dsr", pd.DataFrame()).copy()
        if {"REC_X", "REC_Y", "PreplotEasting", "PreplotNorthing"}.issubset(dsr.columns):
            dsr["radial_offset"] = (
                ((pd.to_numeric(dsr["REC_X"], errors="coerce") - pd.to_numeric(dsr["PreplotEasting"], errors="coerce")) ** 2)
                + ((pd.to_numeric(dsr["REC_Y"], errors="coerce") - pd.to_numeric(dsr["PreplotNorthing"], errors="coerce")) ** 2)
            ) ** 0.5
        else:
            dsr["radial_offset"] = pd.Series(dtype=float)
        battery_col = next((c for c in ["BatteryLifeDays", "battery_life_days", "BatteryLife"] if c in dsr.columns), None)
        battery_values = pd.to_numeric(dsr[battery_col], errors="coerce").dropna().tolist() if battery_col else []
        radial_values = pd.to_numeric(dsr["radial_offset"], errors="coerce").dropna().tolist()
        return {
            "battery_values": battery_values,
            "radial_offset_values": radial_values,
            "battery_avg": round(float(pd.Series(battery_values).mean()), 2) if battery_values else None,
            "radial_avg": round(float(pd.Series(radial_values).mean()), 2) if radial_values else None,
        }

    def build_all(self):
        return {
            "summary": self.build_summary(),
            "daily_activity": self.build_daily_activity().to_dict(orient="records"),
            "fleet": {key: df.to_dict(orient="records") for key, df in self.build_fleet_metrics().items()},
            "qc": self.build_qc_metrics(),
        }
