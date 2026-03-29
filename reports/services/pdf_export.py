"""
PDF export service.

This module uses WeasyPrint when available. If WeasyPrint is not
installed yet, it raises a clear RuntimeError so the integration point
is obvious during setup.
"""
from __future__ import annotations

from pathlib import Path

from django.template.loader import render_to_string


class PDFExporter:
    """
    Convert a report HTML template to a PDF file.
    """

    @staticmethod
    def export(template_name: str, context: dict, output_path: str) -> str:
        """
        Render HTML and write a PDF file.
        """
        try:
            from weasyprint import HTML
        except ImportError as exc:
            raise RuntimeError(
                "WeasyPrint is not installed. Install it before using PDF export."
            ) from exc

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        html_string = render_to_string(template_name, context)
        HTML(string=html_string, base_url=str(output.parent)).write_pdf(str(output))
        return str(output)
