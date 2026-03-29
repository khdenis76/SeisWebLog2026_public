"""
Main orchestration layer for SeisWebLog report generation.
"""

from __future__ import annotations

from pathlib import Path
from .report_queries import ReportQueries
from .report_metrics import ReportMetrics
from .report_charts import ReportCharts
from .report_maps import ReportMaps


class SurveyReportBuilder:
    """Build one full report payload."""

    def __init__(self, pdb, project=None, export_root=None, config=None):
        self.pdb = pdb
        self.project = project
        self.export_root = Path(export_root) if export_root else None
        self.config = config or {}

    def build(self, start_date, end_date, report_type="weekly", narrative=""):
        queries = ReportQueries(self.project.db_path)
        raw = queries.collect_all(start_date, end_date)
        metrics = ReportMetrics(raw).build_all()
        charts_dir = (self.export_root / "charts") if self.export_root else None
        maps_dir = (self.export_root / "maps") if self.export_root else None
        charts = ReportCharts(metrics, export_dir=charts_dir).build_all()
        maps_ = ReportMaps(raw, export_dir=maps_dir).build_all()
        project_info = raw.get("project_info")
        project_name = ""
        if project_info is not None and not project_info.empty:
            project_name = str(project_info.iloc[0].to_dict().get("project_name", ""))
        return {
            "project_name": project_name,
            "report_type": report_type,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "narrative": narrative or "",
            "metrics": metrics,
            "charts": charts,
            "maps": maps_,
        }
