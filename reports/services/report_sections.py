"""
Build template-friendly report sections.

This module translates raw metrics, charts, and maps into a single
presentation structure used by the HTML templates.
"""
from __future__ import annotations

from typing import Any, Dict


class ReportSections:
    """
    Convert computed data into template-oriented sections.
    """

    def __init__(
        self,
        raw: Dict[str, Any],
        metrics: Dict[str, Any],
        charts: Dict[str, str],
        maps_: Dict[str, str],
        project_name: str,
        start_date,
        end_date,
        report_type: str,
        options: Dict[str, Any],
    ):
        self.raw = raw
        self.metrics = metrics
        self.charts = charts
        self.maps_ = maps_
        self.project_name = project_name
        self.start_date = start_date
        self.end_date = end_date
        self.report_type = report_type
        self.options = options

    def build(self) -> Dict[str, Any]:
        """
        Build one unified structure for template rendering.
        """
        return {
            "header": self._build_header(),
            "summary": self.metrics.get("summary", {}),
            "fleet": self.metrics.get("fleet", {}),
            "qc": self.metrics.get("qc", {}),
            "daily": self.metrics.get("daily", {}),
            "charts": self.charts,
            "maps": self.maps_,
            "options": self.options,
        }

    def _build_header(self) -> Dict[str, Any]:
        """
        Build header metadata for the report cover/top area.
        """
        title = self.options.get("title") or f"{self.report_type.title()} Report"
        return {
            "title": title,
            "project_name": self.project_name,
            "report_type": self.report_type,
            "start_date": str(self.start_date),
            "end_date": str(self.end_date),
        }
