from .latex_utils import tex_escape, tex_path


# ---------------------------------------------------------
# generic helpers
# ---------------------------------------------------------
def _selected(generator):
    return set(generator.options.get("sections") or [])


def _landscape_logo_latex(generator) -> str:
    tgs_logo_path = generator.get_tgs_logo_path()
    if tgs_logo_path and generator.options.get("include_tgs_logo", True):
        return rf"\includegraphics[height=20pt]{{{tex_path(tgs_logo_path)}}}"
    return ""


def _landscape_header_manual(generator, line: str) -> str:
    logo = _landscape_logo_latex(generator)
    line_txt = tex_escape(str(line))

    return rf"""
\noindent
\begin{{tikzpicture}}

\shade[left color=headerblueA,right color=headerblueB]
    (0,0) rectangle (\linewidth,1.2cm);

\draw[color=softline, line width=0.8pt]
    (0,0) -- (\linewidth,0);

\node[anchor=west] at (0.3cm,0.6cm)
{{{logo}}};

\node[anchor=center] at (0.5\linewidth,0.6cm)
{{\fontsize{{14}}{{16}}\selectfont\bfseries\color{{titleblue}} End Of Line Report}};

\node[anchor=east] at (\linewidth-0.3cm,0.6cm)
{{\fontsize{{11}}{{13}}\selectfont\bfseries\color{{titleblue}} Line {line_txt}}};

\end{{tikzpicture}}

\vspace*{{6mm}}
"""


def _landscape_footer_manual(generator) -> str:
    project_name = tex_escape(
        generator.options.get("project_name")
        or getattr(generator.prj_info, "ProjectName", "")
        or "Project"
    )
    page_numbers = bool(generator.options.get("include_page_numbers", True))
    footer_right = r"Page \thepage\ of \pageref{LastPage}" if page_numbers else ""

    return rf"""
\vspace*{{4mm}}

\noindent
\begin{{tikzpicture}}

\shade[left color=headerblueA,right color=headerblueB]
    (0,0) rectangle (\linewidth,1.2cm);

\draw[color=softline, line width=0.8pt]
    (0,1.2cm) -- (\linewidth,1.2cm);

\node[anchor=west] at (0.3cm,0.6cm)
{{\fontsize{{10}}{{12}}\selectfont {project_name}}};

\node[anchor=center] at (0.5\linewidth,0.6cm)
{{\fontsize{{10}}{{12}}\selectfont End Of Line Report}};

\node[anchor=east] at (\linewidth-0.3cm,0.6cm)
{{\fontsize{{10}}{{12}}\selectfont {footer_right}}};

\end{{tikzpicture}}
"""



def build_chapter_section(title: str, number: str, body: str, label: str = "") -> dict:
    label_block = rf"\label{{{label}}}" if label else ""
    latex = rf"""
\clearpage
\section*{{{number}. {tex_escape(title)}}}
{label_block}
\addcontentsline{{toc}}{{section}}{{{number}. {tex_escape(title)}}}

{body}
"""
    return {
        "key": f"chapter_{number}",
        "title": title,
        "latex": latex,
    }


def build_subsection_text(number: str, title: str, body: str, label: str = "") -> str:
    label_block = rf"\label{{{label}}}" if label else ""
    return rf"""
\subsection*{{{number} {tex_escape(title)}}}
{label_block}
\addcontentsline{{toc}}{{subsection}}{{{number} {tex_escape(title)}}}

{body}
"""


def build_single_image_block(
    generator,
    line: str,
    *,
    key: str,
    section_number: str,
    section_title: str,
    label: str,
    landscape: bool = False,
    width_ratio: float = 0.98,
    height_ratio: float = 0.86,
) -> str:
    img = generator.get_chart(line, key)

    if not img:
        return build_subsection_text(
            section_number,
            section_title,
            r"\vspace*{4mm} Chart not available." + "\n",
            label=label,
        )

    if landscape:
        return rf"""
\clearpage
\newgeometry{{top=12mm,bottom=12mm,left=12mm,right=12mm}}

\begin{{landscape}}
\thispagestyle{{empty}}

{_landscape_header_manual(generator, line)}

\begin{{center}}
{{\Large\bfseries {tex_escape(section_title)}}}\par
\vspace{{2mm}}

\includegraphics[
    width=0.97\linewidth,
    height=0.62\textheight,
    keepaspectratio
]{{{tex_path(img)}}}
\end{{center}}

\vspace*{{2mm}}

{_landscape_footer_manual(generator)}

\end{{landscape}}

\restoregeometry
"""

    return build_subsection_text(
        section_number,
        section_title,
        rf"""
\vspace*{{4mm}}
\begin{{center}}
\includegraphics[
    width={width_ratio}\textwidth,
    height={height_ratio}\textheight,
    keepaspectratio
]{{{tex_path(img)}}}
\end{{center}}

\clearpage
""",
        label=label,
    )


def build_paged_image_group_block(
    generator,
    line: str,
    *,
    key: str,
    section_number: str,
    section_title: str,
    label: str,
    landscape: bool = False,
    width_ratio: float = 0.96,
    height_ratio: float = 0.86,
    include_total_pages_in_heading: bool = True,
) -> str:

    pages = generator.get_chart_group(line, key)

    if not pages:
        return build_subsection_text(
            section_number,
            section_title,
            r"\vspace*{4mm} Charts not available." + "\n",
            label=label,
        )

    blocks = []
    total_pages = len(pages)

    intro_title = section_title
    if include_total_pages_in_heading:
        intro_title = f"{section_title} (pages = {total_pages})"

    blocks.append(
        build_subsection_text(
            section_number,
            intro_title,
            "",
            label=label,
        )
    )

    for idx, img in enumerate(pages, start=1):
        if landscape:
            blocks.append(
                rf"""
\clearpage
\newgeometry{{top=12mm,bottom=12mm,left=12mm,right=12mm}}

\begin{{landscape}}
\thispagestyle{{empty}}

{_landscape_header_manual(generator, line)}

\begin{{center}}
{{\Large\bfseries {tex_escape(section_title)}}}\par
\vspace{{1mm}}
{{\normalsize Page {idx} of {total_pages}}}\par
\vspace{{2mm}}

\includegraphics[
    width=0.98\linewidth,
    height=0.60\textheight,
    keepaspectratio
]{{{tex_path(img)}}}
\end{{center}}

\vspace*{{2mm}}

{_landscape_footer_manual(generator)}

\end{{landscape}}

\restoregeometry
"""
            )
        else:
            blocks.append(
                rf"""
\clearpage
\begin{{center}}
{{\normalsize Page {idx} of {total_pages}}}\par
\vspace{{4mm}}

\includegraphics[
    width={width_ratio}\textwidth,
    height={height_ratio}\textheight,
    keepaspectratio
]{{{tex_path(img)}}}
\end{{center}}

\clearpage
"""
            )

    return "\n".join(blocks)


def _simple_kv_table(title, rows):
    body_rows = []
    for label, value in rows:
        body_rows.append(
            rf"\rowcolor[HTML]{{F7FBFF}}"
            rf"{tex_escape(label)} & {tex_escape(value)} \\ \hline"
        )

    return rf"""
{{\large\color[HTML]{{0B4F6C}}\textbf{{{tex_escape(title)}}}}}\\[0.15cm]

\arrayrulecolor[HTML]{{7AA6C2}}
\renewcommand{{\arraystretch}}{{1.30}}
\begin{{tabular}}{{|p{{0.36\textwidth}}|p{{0.56\textwidth}}|}}
\hline
\rowcolor[HTML]{{0B4F6C}}
\textcolor{{white}}{{Item}} & \textcolor{{white}}{{Value}} \\ \hline
{chr(10).join(body_rows)}
\end{{tabular}}

\vspace{{0.45cm}}
"""


# ---------------------------------------------------------
# front / toc / standalone sections
# ---------------------------------------------------------
def build_front_page_section(generator, line: str) -> dict:
    return {
        "key": "front_page",
        "title": "Front Page",
        "latex": generator.render_front_page(line),
    }


def build_toc_section(generator, line: str) -> dict:
    return {
        "key": "table_of_contents",
        "title": "Table of Contents",
        "latex": r"\tableofcontents" + "\n" + r"\newpage",
    }


def build_line_summary_section(generator, line: str) -> dict:
    summary = generator.get_line_summary(line)

    def g(key):
        return summary.get(key, "")

    latex = rf"""
\clearpage
\section*{{3. Line Summary}}
\label{{sec:line-summary}}
\addcontentsline{{toc}}{{section}}{{3. Line Summary}}

{_simple_kv_table("General", [
    ("Line", g("Line")),
    ("Planned Points", g("Planned Points")),
    ("Stations", g("Stations")),
    ("Nodes", g("Nodes")),
    ("Min Station", g("Min Station")),
    ("Max Station", g("Max Station")),
])}

{_simple_kv_table("Progress", [
    ("Deployed Count", g("Deployed Count")),
    ("Retrieved Count", g("Retrieved Count")),
    ("Processed Count", g("Processed Count")),
    ("Deployment %", g("Deployment %")),
    ("Recovery %", g("Recovery %")),
    ("Processed %", g("Processed %")),
])}

{_simple_kv_table("ROV Summary", [
    ("Deployment ROVs", g("Deployment ROVs")),
    ("Deployment by ROV", g("Deployment by ROV")),
    ("Recovery ROVs", g("Recovery ROVs")),
    ("Recovery by ROV", g("Recovery by ROV")),
])}

{_simple_kv_table("Timing", [
    ("Start of Deployment", g("Start of Deployment")),
    ("End of Deployment", g("End of Deployment")),
    ("Deployment Duration, h", g("Deployment Duration, h")),
    ("Start of Recovery", g("Start of Recovery")),
    ("End of Recovery", g("End of Recovery")),
    ("Recovery Duration, h", g("Recovery Duration, h")),
])}

\newpage

{_simple_kv_table("Offset and Elevation QC", [
    ("Average Radial Offset to Preplot, m", g("Average Radial Offset to Preplot, m")),
    ("Minimum Radial Offset to Preplot, m", g("Minimum Radial Offset to Preplot, m")),
    ("Maximum Radial Offset to Preplot, m", g("Maximum Radial Offset to Preplot, m")),
    ("Average Range Primary to Secondary, m", g("Average Range Primary to Secondary, m")),
    ("Minimum Range Primary to Secondary, m", g("Minimum Range Primary to Secondary, m")),
    ("Maximum Range Primary to Secondary, m", g("Maximum Range Primary to Secondary, m")),
    ("Average Primary Elevation, m", g("Average Primary Elevation, m")),
    ("Minimum Primary Elevation, m", g("Minimum Primary Elevation, m")),
    ("Maximum Primary Elevation, m", g("Maximum Primary Elevation, m")),
    ("Average Secondary Elevation, m", g("Average Secondary Elevation, m")),
    ("Minimum Secondary Elevation, m", g("Minimum Secondary Elevation, m")),
    ("Maximum Secondary Elevation, m", g("Maximum Secondary Elevation, m")),
    ("Primary e95, m", g("Primary e95, m")),
    ("Primary n95, m", g("Primary n95, m")),
])}

\clearpage
"""
    return {
        "key": "line_summary",
        "title": "Line Summary",
        "latex": latex,
    }


def build_project_map_section(generator, line: str) -> dict:
    return {
        "key": "project_map",
        "title": "Project Map",
        "latex": build_single_image_block(
            generator,
            line,
            key="project_map",
            section_number="4.",
            section_title="Project Map",
            label="sec:project-map",
            landscape=True,
        ),
    }


def build_dsr_info_table_section(generator, line: str) -> dict:
    info = generator.get_dsr_info_table(line)
    rows = "\n".join(
        rf"{tex_escape(k)} & {tex_escape(v)} \\ \hline" for k, v in info.items()
    )

    latex = rf"""
\clearpage
\section*{{5. Info Table from DSR}}
\label{{sec:dsr-info}}
\addcontentsline{{toc}}{{section}}{{5. Info Table from DSR}}

\begin{{longtable}}{{|p{{0.35\textwidth}}|p{{0.58\textwidth}}|}}
\hline
\textbf{{Field}} & \textbf{{Value}} \\ \hline
{rows}
\end{{longtable}}

\clearpage
"""
    return {
        "key": "dsr_info_table",
        "title": "Info Table from DSR",
        "latex": latex,
    }


# ---------------------------------------------------------
# chapter 6 deployment
# ---------------------------------------------------------
def build_deployment_summary_statistic_section(generator, line: str) -> str:
    s = generator.get_line_summary(line)

    rows = [
        ("Deployment %", s.get("Deployment %", "")),
        ("Deployed Count", s.get("Deployed Count", "")),
        ("Deployment ROVs", s.get("Deployment ROVs", "")),
        ("Deployment by ROV", s.get("Deployment by ROV", "")),
        ("Start of Deployment", s.get("Start of Deployment", "")),
        ("End of Deployment", s.get("End of Deployment", "")),
        ("Deployment Duration, h", s.get("Deployment Duration, h", "")),
        ("Average Radial Offset to Preplot, m", s.get("Average Radial Offset to Preplot, m", "")),
        ("Maximum Radial Offset to Preplot, m", s.get("Maximum Radial Offset to Preplot, m", "")),
        ("Average Range Primary to Secondary, m", s.get("Average Range Primary to Secondary, m", "")),
        ("Maximum Range Primary to Secondary, m", s.get("Maximum Range Primary to Secondary, m", "")),
    ]

    return build_subsection_text(
        "6.1",
        "Summary Statistic",
        _simple_kv_table("Deployment Summary", rows) + "\n\\clearpage\n",
        label="sec:deploy-summary",
    )


def build_deployment_chapter_section(generator, line: str) -> dict:
    sel = _selected(generator)
    blocks = []

    if "deployment_summary_statistic" in sel or "deployment" in sel:
        blocks.append(build_deployment_summary_statistic_section(generator, line))

    if "deployment_primary_secondary" in sel or "deployment" in sel:
        blocks.append(
            build_single_image_block(
                generator,
                line,
                key="deployment_primary_secondary",
                section_number="6.2",
                section_title="Primary vs Secondary",
                label="sec:deploy-primary-secondary",
                landscape=False,
                width_ratio=0.97,
                height_ratio=0.82,
            )
        )

    if "deployment_preplot" in sel or "deployment" in sel:
        blocks.append(
            build_single_image_block(
                generator,
                line,
                key="deployment_vs_preplot",
                section_number="6.3",
                section_title="Primary and Secondary vs Preplot",
                label="sec:deploy-preplot",
                landscape=True,
            )
        )

    if "deployment_single_node_map" in sel or "deployment" in sel:
        blocks.append(
            build_paged_image_group_block(
                generator,
                line,
                key="deployment_single_node_map",
                section_number="6.4",
                section_title="Single Node Maps",
                label="sec:single-node-maps",
                landscape=False,
                width_ratio=0.95,
                height_ratio=0.84,
                include_total_pages_in_heading=True,
            )
        )

    if "deployment_water_depth" in sel or "deployment" in sel:
        blocks.append(
            build_single_image_block(
                generator,
                line,
                key="water_depth",
                section_number="6.5",
                section_title="Water Depth QC",
                label="sec:deploy-water-depth",
                landscape=False,
                width_ratio=0.98,
                height_ratio=0.82,
            )
        )

    if not blocks:
        blocks.append(build_subsection_text("6.1", "Summary Statistic", "No deployment sections selected."))

    return build_chapter_section(
        "Deployment",
        "6",
        "\n".join(blocks),
        label="sec:deployment",
    )


# ---------------------------------------------------------
# chapter 7 bbox
# ---------------------------------------------------------
def build_bbox_chapter_section(generator, line: str) -> dict:
    sel = set(generator.options.get("sections") or [])
    blocks = []

    if "bbox_qc_gnss" in sel or "bbox_qc" in sel:
        blocks.append(
            build_paged_image_group_block(
                generator,
                line,
                key="bbox_qc_gnss",
                section_number="5.1",
                section_title="GNSS QC",
                label="sec:bbox-gnss",
                landscape=True,
                width_ratio=0.98,
                height_ratio=0.84,
                include_total_pages_in_heading=True,
            )
        )

    if "bbox_qc_motion" in sel or "bbox_qc" in sel:
        blocks.append(
            build_paged_image_group_block(
                generator,
                line,
                key="bbox_qc_motion",
                section_number="5.2",
                section_title="Motion QC",
                label="sec:bbox-motion",
                landscape=True,
                width_ratio=0.98,
                height_ratio=0.84,
                include_total_pages_in_heading=True,
            )
        )

    if not blocks:
        blocks.append(
            build_subsection_text(
                "5.1",
                "GNSS QC",
                "No Black Box QC sections selected."
            )
        )

    return build_chapter_section(
        "Black Box QC",
        "5",
        "\n".join(blocks),
        label="sec:bbox-qc",
    )


# ---------------------------------------------------------
# chapter 8 source
# ---------------------------------------------------------
def build_source_chapter_section(generator, line: str) -> dict:
    txt = generator.get_source_summary_text(line)

    body = build_subsection_text(
        "8.1",
        "Source Summary",
        rf"""
{tex_escape(txt)}

\clearpage
""",
        label="sec:source-summary",
    )

    return build_chapter_section(
        "Source",
        "8",
        body,
        label="sec:source",
    )


# ---------------------------------------------------------
# chapter 9 recovery
# ---------------------------------------------------------
def build_recovery_chapter_section(generator, line: str) -> dict:
    txt = generator.get_recovery_summary_text(line)

    body = build_subsection_text(
        "9.1",
        "Recovery QC package",
        rf"""
{tex_escape(txt)}

\clearpage
""",
        label="sec:recovery-package",
    )

    return build_chapter_section(
        "Recovery",
        "9",
        body,
        label="sec:recovery",
    )


# ---------------------------------------------------------
# chapter 10 final comparison
# ---------------------------------------------------------
def build_final_comparison_section(generator, line: str) -> dict:
    txt = generator.get_final_comparison_text(line)

    latex = rf"""
\clearpage
\section*{{10. Final Comparison}}
\label{{sec:final-comparison}}
\addcontentsline{{toc}}{{section}}{{10. Final Comparison}}

{tex_escape(txt)}

\clearpage
"""
    return {
        "key": "final_comparison",
        "title": "Final Comparison",
        "latex": latex,
    }


# ---------------------------------------------------------
# chapter 11 comments
# ---------------------------------------------------------
def build_comments_section(generator, line: str) -> dict:
    comments_text = generator.options.get("comments_text") or ""

    latex = rf"""
\clearpage
\section*{{11. Comments}}
\label{{sec:comments}}
\addcontentsline{{toc}}{{section}}{{11. Comments}}

{tex_escape(comments_text)}

\clearpage
"""
    return {
        "key": "comments",
        "title": "Comments",
        "latex": latex,
    }
def build_universal_section(
    generator,
    line: str,
    *,
    key: str,
    section_number: str,
    section_title: str,
    label: str = "",
    orientation: str = "portrait",
    content_type: str = "auto",
    text: str = "",
    table_rows=None,
    width_ratio: float = 0.97,
    height_ratio: float = 0.82,
    include_total_pages: bool = True,
) -> str:

    orientation = (orientation or "portrait").lower().strip()
    content_type = (content_type or "auto").lower().strip()
    is_landscape = orientation == "landscape"
    table_rows = table_rows or []

    images = []

    if content_type in ("auto", "image"):
        images = generator.get_chart_group(line, key)

    if content_type == "auto":
        if images:
            content_type = "image"
        elif table_rows:
            content_type = "table"
        else:
            content_type = "text"

    def _subsection_heading(title_suffix=""):
        title = section_title
        if title_suffix:
            title = f"{section_title} {title_suffix}"

        label_block = rf"\label{{{label}}}" if label else ""

        return rf"""
\subsection*{{{section_number} {tex_escape(title)}}}
{label_block}
\addcontentsline{{toc}}{{subsection}}{{{section_number} {tex_escape(title)}}}
"""

    def _portrait_page(body: str, clear_before=True, clear_after=True) -> str:
        clear_before_block = r"\clearpage" if clear_before else ""
        clear_after_block = r"\clearpage" if clear_after else ""

        return rf"""
{clear_before_block}
\pagestyle{{swlportrait}}

{body}

{clear_after_block}
"""

    def _landscape_page(body: str) -> str:
        return rf"""
\clearpage
\begingroup
\newgeometry{{top=12mm,bottom=12mm,left=12mm,right=12mm}}

\begin{{landscape}}
\thispagestyle{{empty}}
\pagestyle{{empty}}

{_landscape_header_manual(generator, line)}

{body}

\vfill
{_landscape_footer_manual(generator)}

\end{{landscape}}

\restoregeometry
\endgroup
"""

    def _image_body(img_path: str, page_txt: str = "") -> str:
        if is_landscape:
            page_block = ""
            if page_txt:
                page_block = rf"\vspace{{1mm}}{{\normalsize {tex_escape(page_txt)}}}\par"

            return rf"""
\begin{{center}}
{{\Large\bfseries {tex_escape(section_title)}}}\par
{page_block}
\vspace{{2mm}}

\includegraphics[
    width={width_ratio}\linewidth,
    height={height_ratio}\textheight,
    keepaspectratio
]{{{tex_path(img_path)}}}
\end{{center}}
"""

        page_block = ""
        if page_txt:
            page_block = rf"{{\normalsize {tex_escape(page_txt)}}}\par\vspace{{3mm}}"

        return rf"""
\begin{{center}}
{page_block}
\includegraphics[
    width={width_ratio}\textwidth,
    height={height_ratio}\textheight,
    keepaspectratio
]{{{tex_path(img_path)}}}
\end{{center}}
"""

    if content_type == "text":
        body = _subsection_heading() + "\n" + tex_escape(text or "No text available.")
        if is_landscape:
            return _landscape_page(body)
        return _portrait_page(body)

    if content_type == "table":
        table = _simple_kv_table(section_title, table_rows)
        body = _subsection_heading() + "\n" + table
        if is_landscape:
            return _landscape_page(body)
        return _portrait_page(body)

    if content_type == "image":
        if not images:
            body = _subsection_heading() + "\n" + r"\vspace*{4mm} Chart not available."
            if is_landscape:
                return _landscape_page(body)
            return _portrait_page(body)

        blocks = []
        total = len(images)

        heading_suffix = ""
        if include_total_pages and total > 1:
            heading_suffix = f"(pages = {total})"

        if total > 1:
            blocks.append(
                _portrait_page(
                    _subsection_heading(heading_suffix),
                    clear_before=True,
                    clear_after=False,
                )
            )

        for idx, img in enumerate(images, start=1):
            page_txt = f"Page {idx} of {total}" if total > 1 else ""

            body = ""
            if total == 1:
                body += _subsection_heading()

            body += _image_body(img, page_txt)

            if is_landscape:
                blocks.append(_landscape_page(body))
            else:
                blocks.append(_portrait_page(body))

        return "\n".join(blocks)

    body = _subsection_heading() + "\n" + tex_escape("Unsupported section content type.")
    return _portrait_page(body)

# ---------------------------------------------------------
# registry
# ---------------------------------------------------------
SECTION_BUILDERS = {
    "front_page": build_front_page_section,
    "table_of_contents": build_toc_section,
    "line_summary": build_line_summary_section,
    "project_map": build_project_map_section,
    "dsr_info_table": build_dsr_info_table_section,
    "deployment": build_deployment_chapter_section,
    "bbox_qc": build_bbox_chapter_section,
    "source": build_source_chapter_section,
    "recovery": build_recovery_chapter_section,
    "final_comparison": build_final_comparison_section,
    "comments": build_comments_section,

    "deployment_summary_statistic": build_deployment_chapter_section,
    "deployment_primary_secondary": build_deployment_chapter_section,
    "deployment_preplot": build_deployment_chapter_section,
    "deployment_single_node_map": build_deployment_chapter_section,
    "deployment_water_depth": build_deployment_chapter_section,
    "bbox_qc_gnss": build_bbox_chapter_section,
    "bbox_qc_motion": build_bbox_chapter_section,
    "deployment_vs_bbox": build_bbox_chapter_section,
    "source_summary": build_source_chapter_section,
    "recovery_qc_package": build_recovery_chapter_section,
}