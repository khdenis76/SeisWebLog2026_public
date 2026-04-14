from .latex_utils import tex_escape, tex_path


def build_single_image_section(
    generator,
    line: str,
    *,
    key: str,
    section_title: str,
    label: str,
    landscape: bool = False,
    width_ratio: float = 0.98,
    height_ratio: float = 0.86,
) -> dict:
    img = generator.get_chart(line, key)

    if not img:
        latex = rf"""
\clearpage
\section{{{tex_escape(section_title)}}}
\label{{{label}}}

\vspace*{{6mm}}
Chart not available.

\newpage
"""
        return {
            "key": key,
            "title": section_title,
            "latex": latex,
        }

    if landscape:
        logo = ""
        tgs_logo_path = generator.get_tgs_logo_path()
        if tgs_logo_path and generator.options.get("include_tgs_logo", True):
            logo = rf"\includegraphics[height=16pt]{{{tex_path(tgs_logo_path)}}}"

        line_txt = tex_escape(str(line))

        latex = rf"""
\clearpage
\begin{{landscape}}
\thispagestyle{{empty}}

% horizontal landscape header
\noindent
\begin{{tikzpicture}}
    \shade[left color=headerblueA,right color=headerblueB]
        (0,0) rectangle (\linewidth,1.0);

    \draw[color=softline, line width=0.6pt]
        (0,0) -- (\linewidth,0);

    \node[anchor=west] at (0.4,0.5) {{{logo}}};
    \node[anchor=center] at (0.5*\linewidth,0.5)
        {{\fontsize{{13}}{{15}}\selectfont\bfseries\color{{titleblue}} End Of Line Report}};
    \node[anchor=east] at (\linewidth-0.4,0.5)
        {{\fontsize{{10}}{{12}}\selectfont\bfseries\color{{titleblue}} Line {line_txt}}};
\end{{tikzpicture}}

\vspace{{8mm}}

\begin{{center}}
{{\Large\bfseries {tex_escape(section_title)}}}\par
\vspace{{8mm}}
\includegraphics[width=0.90\linewidth,height=0.74\textheight,keepaspectratio]{{{tex_path(img)}}}
\end{{center}}

\end{{landscape}}
\clearpage
"""
    else:
        latex = rf"""
\clearpage
\thispagestyle{{swlportrait}}

\section{{{tex_escape(section_title)}}}
\label{{{label}}}

\vspace*{{6mm}}
\begin{{center}}
\includegraphics[width={width_ratio}\textwidth,height={height_ratio}\textheight,keepaspectratio]{{{tex_path(img)}}}
\end{{center}}

\clearpage
"""

    return {
        "key": key,
        "title": section_title,
        "latex": latex,
    }


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
        return tex_escape(summary.get(key, ""))

    def block(title, rows):
        body_rows = []

        for label, value in rows:
            label_tex = tex_escape(label)
            value_tex = tex_escape(value)

            body_rows.append(
                rf"\rowcolor[HTML]{{F7FBFF}}"
                rf"{label_tex} & {value_tex} \\ \hline"
            )

        return rf"""

\par
{{\large\color[HTML]{{0B4F6C}}\textbf{{{tex_escape(title)}}}}}\\[0.15cm]

\par
\arrayrulecolor[HTML]{{7AA6C2}}
\renewcommand{{\arraystretch}}{{1.35}}

\begin{{tabular}}{{|p{{0.36\textwidth}}|p{{0.56\textwidth}}|}}
\hline
\rowcolor[HTML]{{0B4F6C}}
\textcolor{{white}}{{Item}} & \textcolor{{white}}{{Value}} \\ \hline
{chr(10).join(body_rows)}
\end{{tabular}}

\vspace{{0.45cm}}
"""

    latex = rf"""
\section{{Line Summary}}
\label{{sec:line-summary}}

{block("General", [
    ("Line", g("Line")),
    ("Planned Points", g("Planned Points")),
    ("Stations", g("Stations")),
    ("Nodes", g("Nodes")),
    ("Min Station", g("Min Station")),
    ("Max Station", g("Max Station")),
])}

{block("Progress", [
    ("Deployed Count", g("Deployed Count")),
    ("Retrieved Count", g("Retrieved Count")),
    ("Processed Count", g("Processed Count")),
    ("Deployment %", g("Deployment %")),
    ("Recovery %", g("Recovery %")),
    ("Processed %", g("Processed %")),
])}

{block("ROV Summary", [
    ("Deployment ROVs", g("Deployment ROVs")),
    ("Deployment by ROV", g("Deployment by ROV")),
    ("Recovery ROVs", g("Recovery ROVs")),
    ("Recovery by ROV", g("Recovery by ROV")),
])}

{block("Timing", [
    ("Start of Deployment", g("Start of Deployment")),
    ("End of Deployment", g("End of Deployment")),
    ("Deployment Duration, h", g("Deployment Duration, h")),
    ("Start of Recovery", g("Start of Recovery")),
    ("End of Recovery", g("End of Recovery")),
    ("Recovery Duration, h", g("Recovery Duration, h")),
])}

\newpage

{block("Primary Secondary Delta QC", [
    ("Average Delta Easting Primary to Secondary, m", g("Average Delta Easting Primary to Secondary, m")),
    ("Minimum Delta Easting Primary to Secondary, m", g("Minimum Delta Easting Primary to Secondary, m")),
    ("Maximum Delta Easting Primary to Secondary, m", g("Maximum Delta Easting Primary to Secondary, m")),
    ("Average Delta Northing Primary to Secondary, m", g("Average Delta Northing Primary to Secondary, m")),
    ("Minimum Delta Northing Primary to Secondary, m", g("Minimum Delta Northing Primary to Secondary, m")),
    ("Maximum Delta Northing Primary to Secondary, m", g("Maximum Delta Northing Primary to Secondary, m")),
])}

{block("Recovery Primary Secondary Delta QC", [
    ("Average Delta Easting Recovery Primary to Secondary, m", g("Average Delta Easting Recovery Primary to Secondary, m")),
    ("Minimum Delta Easting Recovery Primary to Secondary, m", g("Minimum Delta Easting Recovery Primary to Secondary, m")),
    ("Maximum Delta Easting Recovery Primary to Secondary, m", g("Maximum Delta Easting Recovery Primary to Secondary, m")),
    ("Average Delta Northing Recovery Primary to Secondary, m", g("Average Delta Northing Recovery Primary to Secondary, m")),
    ("Minimum Delta Northing Recovery Primary to Secondary, m", g("Minimum Delta Northing Recovery Primary to Secondary, m")),
    ("Maximum Delta Northing Recovery Primary to Secondary, m", g("Maximum Delta Northing Recovery Primary to Secondary, m")),
])}

{block("Sigma QC", [
    ("Average Sigma Primary Easting, m", g("Average Sigma Primary Easting, m")),
    ("Minimum Sigma Primary Easting, m", g("Minimum Sigma Primary Easting, m")),
    ("Maximum Sigma Primary Easting, m", g("Maximum Sigma Primary Easting, m")),
    ("Average Sigma Primary Northing, m", g("Average Sigma Primary Northing, m")),
    ("Minimum Sigma Primary Northing, m", g("Minimum Sigma Primary Northing, m")),
    ("Maximum Sigma Primary Northing, m", g("Maximum Sigma Primary Northing, m")),
    ("Average Sigma Secondary Easting, m", g("Average Sigma Secondary Easting, m")),
    ("Minimum Sigma Secondary Easting, m", g("Minimum Sigma Secondary Easting, m")),
    ("Maximum Sigma Secondary Easting, m", g("Maximum Sigma Secondary Easting, m")),
    ("Average Sigma Secondary Northing, m", g("Average Sigma Secondary Northing, m")),
    ("Minimum Sigma Secondary Northing, m", g("Minimum Sigma Secondary Northing, m")),
    ("Maximum Sigma Secondary Northing, m", g("Maximum Sigma Secondary Northing, m")),
])}

{block("Offset and Elevation QC", [
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

\newpage
"""

    return {
        "key": "line_summary",
        "title": "Line Summary",
        "latex": latex,
    }


def build_project_map_section(generator, line: str) -> dict:
    return build_single_image_section(
        generator,
        line,
        key="project_map",
        section_title="Project Map",
        label="sec:project-map",
        landscape=True,
        width_ratio=0.99,
        height_ratio=0.90,
    )


def build_water_depth_section(generator, line: str) -> dict:
    return build_single_image_section(
        generator,
        line,
        key="water_depth",
        section_title="Water Depth - Primary vs Secondary",
        label="sec:water-depth",
        landscape=False,
        width_ratio=0.98,
        height_ratio=0.82,
    )


def build_dsr_info_table_section(generator, line: str) -> dict:
    info = generator.get_dsr_info_table(line)

    rows = []
    for key, value in info.items():
        rows.append(rf"{tex_escape(key)} & {tex_escape(value)} \\ \hline")

    latex = rf"""
\section{{Info Table from DSR}}
\label{{sec:dsr-info}}

\begin{{longtable}}{{|p{{0.35\textwidth}}|p{{0.58\textwidth}}|}}
\hline
\textbf{{Field}} & \textbf{{Value}} \\ \hline
{chr(10).join(rows)}
\end{{longtable}}

\newpage
"""
    return {
        "key": "dsr_info_table",
        "title": "Info Table from DSR",
        "latex": latex,
    }


def build_deployment_vs_preplot_section(generator, line: str) -> dict:
    return build_single_image_section(
        generator,
        line,
        key="deployment_vs_preplot",
        section_title="Deployment - Primary and Secondary vs Preplot",
        label="sec:deployment-vs-preplot",
        landscape=True,
        width_ratio=0.99,
        height_ratio=0.88,
    )


def build_comments_section(generator, line: str) -> dict:
    comments_text = generator.options.get("comments_text") or ""

    latex = rf"""
\section{{Comments}}
\label{{sec:comments}}

{tex_escape(comments_text)}

\newpage
"""
    return {
        "key": "comments",
        "title": "Comments",
        "latex": latex,
    }
def build_qc_summary_section(generator, line: str) -> dict:
    s = generator.get_line_summary(line)

    def val(key, default=""):
        return str(s.get(key, default) or "").strip()

    def num(key, default=0.0):
        try:
            txt = str(s.get(key, "")).replace(",", "").strip()
            return float(txt) if txt != "" else float(default)
        except Exception:
            return float(default)

    deployment_pct = num("Deployment %")
    recovery_pct = num("Recovery %")
    processed_pct = num("Processed %")

    max_rad = num("Maximum Radial Offset to Preplot, m")
    avg_rad = num("Average Radial Offset to Preplot, m")
    max_range_ps = num("Maximum Range Primary to Secondary, m")
    avg_range_ps = num("Average Range Primary to Secondary, m")

    avg_de = num("Average Delta Easting Primary to Secondary, m")
    avg_dn = num("Average Delta Northing Primary to Secondary, m")
    max_de = num("Maximum Delta Easting Primary to Secondary, m")
    max_dn = num("Maximum Delta Northing Primary to Secondary, m")

    def status_color(ok: bool) -> str:
        return "2E7D32" if ok else "C62828"

    deploy_ok = deployment_pct >= 99.0
    recovery_ok = recovery_pct >= 99.0
    process_ok = processed_pct >= 95.0
    offset_ok = max_rad <= 10.0
    ps_ok = max_range_ps <= 5.0

    overview_rows = [
        ("Deployment Completion", f"{deployment_pct:.1f}%", deploy_ok),
        ("Recovery Completion", f"{recovery_pct:.1f}%", recovery_ok),
        ("Processed Completion", f"{processed_pct:.1f}%", process_ok),
        ("Max Radial Offset to Preplot", f"{max_rad:.2f} m", offset_ok),
        ("Max Primary-Secondary Range", f"{max_range_ps:.2f} m", ps_ok),
    ]

    def overview_table():
        rows = []
        for label, value, ok in overview_rows:
            rows.append(
                rf"\rowcolor[HTML]{{F7FBFF}}"
                rf"{tex_escape(label)} & {tex_escape(value)} & "
                rf"\textcolor[HTML]{{{status_color(ok)}}}{{\textbf{{{'OK' if ok else 'CHECK'}}}}} \\ \hline"
            )
        return rf"""
\begin{{tabularx}}{{\textwidth}}{{|p{{0.50\textwidth}}|p{{0.22\textwidth}}|p{{0.18\textwidth}}|}}
\hline
\rowcolor[HTML]{{0B4F6C}}
\textcolor{{white}}{{\textbf{{Metric}}}} &
\textcolor{{white}}{{\textbf{{Value}}}} &
\textcolor{{white}}{{\textbf{{Status}}}} \\ \hline
{chr(10).join(rows)}
\end{{tabularx}}
"""

    comments = []

    if deploy_ok and recovery_ok:
        comments.append("Deployment and recovery completion are within expected limits.")
    else:
        comments.append("Completion percentages should be reviewed before report release.")

    comments.append(
        f"Average radial offset to preplot is {avg_rad:.2f} m and maximum radial offset is {max_rad:.2f} m."
    )
    comments.append(
        f"Average primary-secondary range is {avg_range_ps:.2f} m and maximum primary-secondary range is {max_range_ps:.2f} m."
    )
    comments.append(
        f"Primary-secondary mean deltas are ΔE = {avg_de:.2f} m and ΔN = {avg_dn:.2f} m; maxima are ΔE = {max_de:.2f} m and ΔN = {max_dn:.2f} m."
    )

    bullet_text = "\n".join([rf"\item {tex_escape(x)}" for x in comments])

    latex = rf"""
\section{{QC Summary}}
\label{{sec:qc-summary}}

{{\large\color[HTML]{{0B4F6C}}\textbf{{Overview}}}}\\[0.2cm]

{overview_table()}

\vspace{{0.5cm}}

{{\large\color[HTML]{{0B4F6C}}\textbf{{Automatic Remarks}}}}\\[0.2cm]

\begin{{itemize}}
{bullet_text}
\end{{itemize}}

\newpage
"""
    return {
        "key": "qc_summary",
        "title": "QC Summary",
        "latex": latex,
    }


SECTION_BUILDERS = {
    "front_page": build_front_page_section,
    "table_of_contents": build_toc_section,
    "qc_summary": build_qc_summary_section,
    "line_summary": build_line_summary_section,
    "project_map": build_project_map_section,
    "water_depth": build_water_depth_section,
    "dsr_info_table": build_dsr_info_table_section,
    "deployment_vs_preplot": build_deployment_vs_preplot_section,
    "comments": build_comments_section,
}