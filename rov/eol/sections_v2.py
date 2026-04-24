from __future__ import annotations

import os
import re
from pathlib import Path
from bs4 import BeautifulSoup

from .latex_utils import tex_escape, tex_path


# =========================================================
# Bootstrap color maps
# =========================================================
BOOTSTRAP_BG_COLORS = {
    "table-primary": "CFE2FF",
    "table-secondary": "E2E3E5",
    "table-success": "D1E7DD",
    "table-danger": "F8D7DA",
    "table-warning": "FFF3CD",
    "table-info": "CFF4FC",
    "table-light": "F8F9FA",
    "table-dark": "212529",
}

BOOTSTRAP_TEXT_COLORS = {
    "text-primary": "0D6EFD",
    "text-secondary": "6C757D",
    "text-success": "198754",
    "text-danger": "DC3545",
    "text-warning": "FFC107",
    "text-info": "0DCAF0",
    "text-light": "F8F9FA",
    "text-dark": "212529",
}


# =========================================================
# Helpers
# =========================================================
def _classes(tag):
    return tag.get("class") or []


def _bg_color(tag):
    for c in _classes(tag):
        if c in BOOTSTRAP_BG_COLORS:
            return BOOTSTRAP_BG_COLORS[c]
    return ""


def _text_color(tag):
    for c in _classes(tag):
        if c in BOOTSTRAP_TEXT_COLORS:
            return BOOTSTRAP_TEXT_COLORS[c]
    return ""


def _inline_color(style):
    if not style:
        return ""
    m = re.search(r"color\s*:\s*#([0-9a-fA-F]{6})", style)
    return m.group(1).upper() if m else ""


def _inline_bg(style):
    if not style:
        return ""
    m = re.search(r"background-color\s*:\s*#([0-9a-fA-F]{6})", style)
    return m.group(1).upper() if m else ""


def _bold(tag):
    if "fw-bold" in _classes(tag):
        return True
    style = tag.get("style") or ""
    return "font-weight:bold" in style or "font-weight:700" in style


def _italic(tag):
    if "fst-italic" in _classes(tag):
        return True
    style = tag.get("style") or ""
    return "font-style:italic" in style


def _format_text(text, *, color="", bold=False, italic=False):
    out = tex_escape(text)

    if italic:
        out = rf"\textit{{{out}}}"

    if bold:
        out = rf"\textbf{{{out}}}"

    if color:
        out = rf"\textcolor[HTML]{{{color}}}{{{out}}}"

    return out


# =========================================================
# HTML → LaTeX cell
# =========================================================
def html_cell_to_latex(cell, *, image_width="0.2\\textwidth", image_height="25mm"):
    img = cell.find("img")

    if img:
        src = img.get("src") or ""
        if src:
            value = rf"\includegraphics[width={image_width},height={image_height},keepaspectratio]{{{tex_path(src)}}}"
        else:
            value = ""
    else:
        text = cell.get_text(" ", strip=True)

        color = (
            _inline_color(cell.get("style"))
            or _text_color(cell)
        )

        value = _format_text(
            text,
            color=color,
            bold=_bold(cell),
            italic=_italic(cell),
        )

    bg = _inline_bg(cell.get("style")) or _bg_color(cell)

    if bg:
        return rf"\cellcolor[HTML]{{{bg}}}{value}"

    return value


# =========================================================
# HTML TABLE → LATEX
# =========================================================
def html_table_to_latex(
    html: str,
    *,
    table_widths=None,
    image_width="0.2\\textwidth",
    image_height="25mm",
):
    soup = BeautifulSoup(html or "", "html.parser")
    table = soup.find("table")

    if not table:
        return tex_escape(html)

    rows = []

    for tr in table.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue

        is_header = any(c.name == "th" for c in cells)

        row = [
            html_cell_to_latex(
                c,
                image_width=image_width,
                image_height=image_height,
            )
            for c in cells
        ]

        rows.append((is_header, tr, row))

    col_count = max(len(r) for _, _, r in rows)

    if table_widths and len(table_widths) == col_count:
        spec = "|".join([f"p{{{w}\\textwidth}}" for w in table_widths])
    else:
        w = round(0.92 / col_count, 3)
        spec = "|".join([f"p{{{w}\\textwidth}}" for _ in range(col_count)])

    body = []
    body.append(r"\renewcommand{\arraystretch}{1.25}")
    body.append(rf"\begin{{tabular}}{{|{spec}|}}")
    body.append(r"\hline")

    for is_header, tr, row in rows:
        while len(row) < col_count:
            row.append("")

        row_bg = _inline_bg(tr.get("style")) or _bg_color(tr)

        if row_bg:
            body.append(rf"\rowcolor[HTML]{{{row_bg}}}")

        body.append(" & ".join(row) + r" \\ \hline")

    body.append(r"\end{tabular}")

    return "\n".join(body)


# =========================================================
# UNIVERSAL SECTION BUILDER
# =========================================================
def build_universal_section(
    generator,
    line: str,
    *,
    key: str,
    section_title: str,
    section_number: str | None = None,
    numbering: bool = True,
    orientation: str = "portrait",
    content_type: str = "text",
    content=None,
    label: str = "",
    width_ratio: float = 0.97,
    height_ratio: float = 0.82,
    include_total_pages: bool = True,
    html_table_options: dict | None = None,
) -> dict:

    html_table_options = html_table_options or {}

    if numbering:
        if not section_number:
            section_number = generator.next_section_number()
        display_title = f"{section_number} {section_title}"
    else:
        display_title = section_title

    label_block = rf"\label{{{label}}}" if label else ""

    heading = rf"""
\subsection*{{{tex_escape(display_title)}}}
{label_block}
\addcontentsline{{toc}}{{subsection}}{{{tex_escape(display_title)}}}
"""

    def page(body):
        return rf"""
\clearpage
\pagestyle{{swlportrait}}

{body}

\clearpage
"""

    # TEXT
    if content_type == "text":
        return {
            "key": key,
            "title": section_title,
            "latex": page(heading + tex_escape(str(content or ""))),
        }

    # TABLE
    if content_type == "table":
        rows = content or []
        table = "\n".join(f"{tex_escape(k)} & {tex_escape(v)} \\\\" for k, v in rows)

        latex = rf"""
\begin{{tabular}}{{ll}}
{table}
\end{{tabular}}
"""
        return {
            "key": key,
            "title": section_title,
            "latex": page(heading + latex),
        }

    # HTML TABLE
    if content_type == "html_table":
        table = html_table_to_latex(str(content), **html_table_options)

        return {
            "key": key,
            "title": section_title,
            "latex": page(heading + table),
        }
        # GENERIC GRID SECTION
    if content_type == "grid":
        rows = []

        if isinstance(content, dict):
            rows = content.get("rows") or []
        elif isinstance(content, list):
            rows = content

        latex_grid = grid_section_to_latex(
            rows,
            **html_table_options,
        )

        if orientation == "landscape":
            return {
                "key": key,
                "title": section_title,
                "latex": rf"""
                           \clearpage
                           \begin{{landscape}}
                           \thispagestyle{{swllandscape}}
                           
                           {heading}
                           
                           {latex_grid}
                           
                           \end{{landscape}}
                           \clearpage
                           """,
            }

        return {
            "key": key,
            "title": section_title,
            "latex": page(heading + latex_grid),
        }
    # IMAGE
    if content_type == "image":
        if not content:
            return {"key": key, "title": section_title, "latex": page("No image")}

        return {
            "key": key,
            "title": section_title,
            "latex": page(
                heading
                + rf"\includegraphics[width={width_ratio}\textwidth]{{{tex_path(content)}}}"
            ),
        }

    return {
        "key": key,
        "title": section_title,
        "latex": page(heading + "Unsupported content type"),
    }
# =========================================================
# GENERIC GRID SECTION HELPERS
# =========================================================

def grid_cell_to_latex(cell: dict) -> str:
    """
    Convert one grid cell to LaTeX.

    Supported cell types:
        - text
        - html_table
        - image
    """
    if not isinstance(cell, dict):
        return ""

    ctype = cell.get("type", "text")
    content = cell.get("content", "")
    options = cell.get("options") or {}

    # ---------------- TEXT ----------------
    if ctype == "text":
        return tex_escape(str(content or ""))

    # ---------------- HTML TABLE ----------------
    if ctype == "html_table":
        return html_table_to_latex(
            str(content or ""),
            **options,
        )

    # ---------------- IMAGE ----------------
    if ctype == "image":
        if not content:
            return tex_escape("No image")

        width = options.get("width", "0.95\\linewidth")
        height = options.get("height", "")
        keepaspectratio = options.get("keepaspectratio", True)

        parts = [f"width={width}"]

        if height:
            parts.append(f"height={height}")

        if keepaspectratio:
            parts.append("keepaspectratio")

        opts = ",".join(parts)

        return rf"""
\begin{{center}}
\includegraphics[{opts}]{{{tex_path(content)}}}
\end{{center}}
"""

    return tex_escape(f"Unsupported cell type: {ctype}")


def grid_section_to_latex(
    rows,
    *,
    total_units=12,
    gap="0.012\\linewidth",
    font_size="\\scriptsize",
    default_row_vspace="4mm",
):
    """
    Bootstrap-like LaTeX grid.

    Example:
        rows = [
            {
                "height": "140mm",
                "cells": [
                    {"span": 4, "type": "html_table", "content": left_html},
                    {"span": 4, "type": "html_table", "content": mid_html},
                    {"span": 4, "type": "image", "content": chart_path},
                ],
            }
        ]

    span works like Bootstrap col-*
        span 12 = full width
        span 6  = half width
        span 4  = one third
        span 3  = one quarter
    """
    if not rows:
        return ""

    out = []

    for row in rows:
        if isinstance(row, dict):
            cells = row.get("cells") or []
            row_height = row.get("height", "")
            row_vspace = row.get("vspace", default_row_vspace)
        else:
            cells = row or []
            row_height = ""
            row_vspace = default_row_vspace

        cells = [c for c in cells if isinstance(c, dict)]

        if not cells:
            continue

        # available linewidth after gaps
        gap_count = max(len(cells) - 1, 0)

        # Convert numeric gap ratio only approximately.
        # Keep page safe by using 0.98 linewidth total content width.
        available_ratio = 0.98 - (0.012 * gap_count)

        out.append(r"\noindent")

        for i, cell in enumerate(cells):
            span = int(cell.get("span", total_units))
            span = max(1, min(span, total_units))

            width_ratio = available_ratio * span / total_units
            width = f"{width_ratio:.3f}\\linewidth"

            cell_latex = grid_cell_to_latex(cell)

            if row_height:
                begin_minipage = rf"\begin{{minipage}}[t][{row_height}][t]{{{width}}}"
            else:
                begin_minipage = rf"\begin{{minipage}}[t]{{{width}}}"

            out.append(
                rf"""
{begin_minipage}
{font_size}
{cell_latex}
\end{{minipage}}
"""
            )

            if i < len(cells) - 1:
                out.append(rf"\hspace{{{gap}}}")

        if row_vspace:
            out.append(rf"\vspace{{{row_vspace}}}")

    return "\n".join(out)