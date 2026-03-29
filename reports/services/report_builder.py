"""
Main orchestration layer for report generation.

This class ties together:
- raw data extraction
- metric calculation
- chart generation
- map generation
- final section assembly
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from .report_charts import ReportCharts
from .report_maps import ReportMaps
from .report_metrics import ReportMetrics
from .report_queries import ReportQueries
from .report_sections import ReportSections


class ReportBuilder:
    """
    Build a complete report payload for a selected period.
    """

    def __init__(self, db_path: str, media_root: str, media_url: str, project_name: str = ""):
        self.db_path = db_path
        self.media_root = Path(media_root)
        self.media_url = media_url.rstrip("/")
        self.project_name = project_name

    def build(self, start_date, end_date, report_type: str = "weekly", options: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """
        Build the complete report dictionary.

        The returned structure is intended to be stored in JSONField and
        reused both for HTML preview and PDF rendering.
        """
        options = options or {}

        asset_root = self.media_root / "reports" / "generated" / f"{report_type}_{start_date}_{end_date}"
        charts_dir = asset_root / "charts"
        maps_dir = asset_root / "maps"
        charts_dir.mkdir(parents=True, exist_ok=True)
        maps_dir.mkdir(parents=True, exist_ok=True)

        raw = ReportQueries(self.db_path).collect_all(start_date, end_date)
        metrics = ReportMetrics(raw).build_all(report_type=report_type)
        charts = ReportCharts(raw, metrics, str(charts_dir)).build_all(report_type=report_type)
        maps_ = ReportMaps(raw, str(maps_dir)).build_all(report_type=report_type)

        sections = ReportSections(
            raw=raw,
            metrics=metrics,
            charts={k: self._to_media_url(asset_root / "charts" / v) for k, v in charts.items() if v},
            maps_={k: self._to_media_url(asset_root / "maps" / v) for k, v in maps_.items() if v},
            project_name=self.project_name,
            start_date=start_date,
            end_date=end_date,
            report_type=report_type,
            options=options,
        ).build()

        # Keep raw frames out of JSON storage. Only summary information is
        # returned in the payload.
        return {
            "header": sections["header"],
            "summary": sections["summary"],
            "fleet": sections["fleet"],
            "qc": sections["qc"],
            "daily": sections["daily"],
            "charts": sections["charts"],
            "maps": sections["maps"],
            "options": sections["options"],
        }

    def _to_media_url(self, absolute_path: Path) -> str:
        """
        Convert an absolute file path under MEDIA_ROOT to MEDIA_URL path.
        """
        relative = absolute_path.relative_to(self.media_root).as_posix()
        return f"{self.media_url}/{relative}"
