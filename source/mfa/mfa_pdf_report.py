from __future__ import annotations

import io
import sqlite3
from pathlib import Path


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _find_logo(logo_path=None):
    candidates = []
    if logo_path:
        candidates.append(Path(logo_path))

    try:
        from django.conf import settings
        if settings.configured:
            candidates.append(
                Path(settings.BASE_DIR) / "logos" / "2024_TGS_logo_blue.png"
            )
    except (ImportError, AttributeError):
        pass

    module_dir = Path(__file__).resolve().parent
    candidates.extend([
        module_dir.parents[1] / "logos" / "2024_TGS_logo_blue.png",
        Path.cwd() / "logos" / "2024_TGS_logo_blue.png",
    ])
    return next((path for path in candidates if path.is_file()), None)


def _project_info(conn, overrides=None):
    info = {}
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_main'"
    ).fetchone()
    if exists:
        row = conn.execute("SELECT * FROM project_main LIMIT 1").fetchone()
        if row:
            raw = {key.lower(): row[key] for key in row.keys()}
            aliases = {
                "Project": ("name", "project", "project_name"),
                "Project ID": ("project_id", "projectid", "job_number"),
                "Client": ("client", "client_name"),
                "Contractor": ("contractor", "contractor_name"),
                "Location": ("location", "area", "country"),
            }
            for label, names in aliases.items():
                value = next((raw.get(name) for name in names if raw.get(name)), None)
                if value is not None:
                    info[label] = str(value)
    if overrides:
        info.update({str(k): str(v) for k, v in overrides.items() if v is not None})
    return info


def _page_series(conn, page_id):
    rows = conn.execute(
        """
        SELECT r.RowNo, r.Shot, v.ColumnIndex, v.ColumnName, v.ValueReal
        FROM MFA_Rows r
        JOIN MFA_Values v ON v.Row_FK = r.ID
        WHERE r.Page_FK = ?
          AND r.Shot IS NOT NULL
          AND v.ValueReal IS NOT NULL
          AND LOWER(v.ColumnName) NOT LIKE '% quality'
        ORDER BY v.ColumnIndex, r.RowNo
        """,
        (page_id,),
    ).fetchall()
    result = {}
    for row in rows:
        values = result.setdefault(row["ColumnName"], {"shot": [], "value": []})
        values["shot"].append(row["Shot"])
        values["value"].append(row["ValueReal"])
    return result


def _chart_image(page, series, plt, series_per_panel=8):
    names = list(series)
    groups = [
        names[index:index + series_per_panel]
        for index in range(0, len(names), series_per_panel)
    ] or [[]]
    if len(groups) > 4:
        groups = groups[:3] + [[name for group in groups[3:] for name in group]]

    fig, axes = plt.subplots(
        len(groups), 1, figsize=(11.2, 6.25), dpi=150, squeeze=False,
    )
    palette = plt.get_cmap("tab20").colors
    for group_index, group in enumerate(groups):
        ax = axes[group_index][0]
        for series_index, name in enumerate(group):
            values = series[name]
            ax.plot(
                values["shot"], values["value"],
                linewidth=0.9,
                color=palette[series_index % len(palette)],
                label=name,
            )
        ax.set_xlabel("Shot", fontsize=7)
        ax.set_ylabel("Value", fontsize=7)
        ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.55)
        ax.tick_params(labelsize=6)
        if group:
            ax.legend(
                loc="upper center", bbox_to_anchor=(0.5, 1.01),
                ncol=min(4, len(group)), fontsize=5.2, frameon=False,
            )
        else:
            ax.text(
                0.5, 0.5, "No numeric values available",
                ha="center", va="center", transform=ax.transAxes,
            )
    fig.tight_layout(pad=0.8, h_pad=1.1)
    image = io.BytesIO()
    fig.savefig(image, format="png", dpi=170, bbox_inches="tight")
    plt.close(fig)
    image.seek(0)
    return image


def _draw_logo(pdf, logo, x, y, width, height, ImageReader):
    if logo:
        pdf.drawImage(
            ImageReader(str(logo)), x, y, width=width, height=height,
            preserveAspectRatio=True, mask="auto", anchor="w",
        )
    else:
        pdf.setFillColorRGB(0.02, 0.24, 0.47)
        pdf.setFont("Helvetica-Bold", 28)
        pdf.drawString(x, y + height * 0.25, "TGS")


def _header_footer(pdf, width, height, logo, page_no, total_pages, ImageReader):
    _draw_logo(pdf, logo, 38, height - 38, 62, 24, ImageReader)
    pdf.setStrokeColorRGB(0.12, 0.28, 0.45)
    pdf.setLineWidth(0.7)
    pdf.line(36, height - 42, width - 36, height - 42)
    pdf.line(36, 30, width - 36, 30)
    pdf.setFillColorRGB(0.32, 0.32, 0.32)
    pdf.setFont("Helvetica", 7.5)
    pdf.drawString(38, 18, "MFA Quality Control Report")
    pdf.drawRightString(width - 38, 18, f"Page {page_no} of {total_pages}")


def _cover(
        pdf, width, height, logo, title, file_row, project,
        prepared_by, total_pages, colors, ImageReader,
):
    _draw_logo(pdf, logo, 58, height - 112, 145, 54, ImageReader)
    pdf.setFillColor(colors.HexColor("#17365D"))
    pdf.rect(0, height - 240, width, 90, fill=1, stroke=0)
    pdf.setFillColor(colors.white)
    pdf.setFont("Helvetica-Bold", 24)
    pdf.drawCentredString(width / 2, height - 198, title)
    pdf.setFont("Helvetica", 13)
    pdf.drawCentredString(width / 2, height - 220, str(file_row["FileName"]))

    details = list(project.items()) + [
        ("MFA line", file_row["LineName"] or "-"),
        ("Line", file_row["Line"] if file_row["Line"] is not None else "-"),
        ("Sequence", file_row["Seq"] if file_row["Seq"] is not None else "-"),
        ("Attempt", file_row["Attempt"] or "-"),
        ("Vessel", file_row["vessel_name"] or "-"),
        ("Shots", file_row["TotalShots"] or 0),
    ]
    if prepared_by:
        details.append(("Prepared by", prepared_by))

    start_y = height - 286
    for index, (label, value) in enumerate(details):
        y = start_y - index * 20
        fill = colors.HexColor("#D9EAF7") if index % 2 == 0 else colors.white
        pdf.setFillColor(fill)
        pdf.rect(165, y - 5, width - 330, 18, fill=1, stroke=0)
        pdf.setFillColor(colors.HexColor("#17365D"))
        pdf.setFont("Helvetica-Bold", 9)
        pdf.drawString(180, y, str(label))
        pdf.setFont("Helvetica", 9)
        pdf.drawString(310, y, str(value)[:80])
    _header_footer(pdf, width, height, logo, 1, total_pages, ImageReader)


def generate_mfa_pdf_report(
        db_path,
        file_id,
        output_path,
        logo_path=None,
        report_title="MFA QUALITY CONTROL REPORT",
        prepared_by=None,
        project_info=None,
):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.utils import ImageReader
    from reportlab.pdfgen import canvas

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logo = _find_logo(logo_path)

    with _connect(db_path) as conn:
        file_row = conn.execute(
            """
            SELECT f.*, pf.vessel_name AS vessel_name
            FROM MFA_Files f
            LEFT JOIN project_fleet pf ON pf.id = f.Vessel_FK
            WHERE f.ID = ?
            """,
            (file_id,),
        ).fetchone()
        if not file_row:
            raise ValueError(f"MFA file ID {file_id} was not found")
        pages = conn.execute(
            """
            SELECT ID, PageNo, PageTitle, FirstOnlineShot,
                   LastOnlineShot, RowCount
            FROM MFA_Pages WHERE File_FK = ? ORDER BY PageNo
            """,
            (file_id,),
        ).fetchall()
        project = _project_info(conn, project_info)

        page_size = landscape(A4)
        width, height = page_size
        pdf = canvas.Canvas(str(output_path), pagesize=page_size)
        pdf.setTitle(f"{report_title} - {file_row['FileName']}")
        total_pages = len(pages) + 1
        _cover(
            pdf, width, height, logo, report_title, file_row, project,
            prepared_by, total_pages, colors, ImageReader,
        )
        pdf.showPage()

        for report_page_no, page in enumerate(pages, start=2):
            chart = _chart_image(page, _page_series(conn, page["ID"]), plt)
            pdf.setFont("Helvetica-Bold", 14)
            pdf.setFillColor(colors.HexColor("#17365D"))
            pdf.drawString(42, height - 62, f"Page {page['PageNo']} - {page['PageTitle']}")
            pdf.setFont("Helvetica", 8)
            pdf.setFillColor(colors.HexColor("#555555"))
            pdf.drawString(
                42, height - 76,
                f"First shot: {page['FirstOnlineShot'] or '-'}    "
                f"Last shot: {page['LastOnlineShot'] or '-'}    "
                f"Samples: {page['RowCount'] or 0}",
            )
            pdf.drawImage(
                ImageReader(chart), 36, 42,
                width=width - 72, height=height - 126,
                preserveAspectRatio=True, anchor="c",
            )
            _header_footer(
                pdf, width, height, logo, report_page_no, total_pages, ImageReader,
            )
            pdf.showPage()
            chart.close()
        pdf.save()

    return output_path
