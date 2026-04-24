"""
PDF export utilities for SeisWebLog reports.
"""

from __future__ import annotations

from pathlib import Path
from django.template.loader import render_to_string


class PDFExporter:
    """HTML-to-PDF exporter using WeasyPrint."""

    @staticmethod
    def export(template_name, context, output_path):
        from weasyprint import HTML
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        html = render_to_string(template_name, context)
        HTML(string=html, base_url=str(output_path.parent)).write_pdf(str(output_path))
        return str(output_path)
