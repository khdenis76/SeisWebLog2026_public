"""
EOL Report Generator V2 - TRUE MODULAR MODE

This generator does NOT generate charts.

It only renders content passed in options["report_sections"] / options["layout"].

Supported section content is handled by sections_v2.py:
    - title_page
    - toc
    - text
    - table
    - html_table
    - image
    - image_group
    - image_folder

Usage:
    from rov.eol.generator_v2 import EOLReportGeneratorV2

    generator = EOLReportGeneratorV2(
        db_path=DB_PATH,
        request_user=None,
        options=OPTIONS,
    )

    pdf_path = generator.build_line_report("12001")
"""

from __future__ import annotations

import shutil
import subprocess
import zipfile
from pathlib import Path

from django.contrib.staticfiles import finders

from .latex_utils import tex_escape, tex_path
from .sections_v2 import build_universal_section


class EOLReportGeneratorV2:
    """
    True modular LaTeX-based End Of Line report generator.

    Important:
        V2 does not create matplotlib charts.
        V2 does not call DSRLineGraphicsMatplotlib.
        V2 does not automatically build project maps, BBox plots, etc.

    You pass existing content through options["report_sections"].
    """

    def __init__(self, db_path, request_user=None, options=None):
        self.db_path = str(db_path) if db_path else ""
        self.request_user = request_user
        self.options = options or {}

        reports_dir = self.options.get("reports_dir")
        if not reports_dir:
            raise RuntimeError("reports_dir is not configured.")

        self.reports_root = Path(reports_dir)
        self.reports_root.mkdir(parents=True, exist_ok=True)

        self.tmp_root = self.reports_root / "_tmp_eol_v2"
        self.tmp_root.mkdir(parents=True, exist_ok=True)

        self._section_counter = 0
        self._charts = {}

        # Optional project info. Do not fail V2 if ProjectDB cannot load.
        self.prj_info = None
        try:
            from core.projectdb import ProjectDB

            if self.db_path:
                self.pdb = ProjectDB(self.db_path)
                self.prj_info = self.pdb.get_main()
            else:
                self.pdb = None
        except Exception:
            self.pdb = None
            self.prj_info = None

    # ---------------------------------------------------------
    # public API
    # ---------------------------------------------------------
    def build_zip_for_lines(self, lines):
        out_paths = []

        try:
            for line in lines:
                out_paths.append(self.build_line_report(line))

            zip_path = self.tmp_root / "EOL_Reports_V2.zip"

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for p in out_paths:
                    zf.write(p, arcname=Path(p).name)

            return str(zip_path)

        except Exception:
            self.cleanup()
            raise

    def get_line_output_dir(self, line: str) -> Path:
        line = str(line).strip()

        out_dir = self.reports_root / f"{line}_EOL_V2"
        out_dir.mkdir(parents=True, exist_ok=True)

        return out_dir

    def build_line_report(self, line):
        """
        Build one PDF report.

        True modular mode:
            - no chart generation
            - no automatic DSR plots
            - no automatic BBox plots
            - no matplotlib calls
        """
        line = str(line).strip()

        self.reset_numbering()
        self._charts[str(line)] = {}

        line_out_dir = self.get_line_output_dir(line)
        line_tmp_dir = line_out_dir / "_compile"
        line_tmp_dir.mkdir(parents=True, exist_ok=True)

        tex_path_file = line_tmp_dir / f"EOL_Line_{line}.tex"

        tex_content = self.render_report_tex(line=line)
        tex_path_file.write_text(tex_content, encoding="utf-8")

        try:
            self._compile_latex(tex_path_file)
        finally:
            self._copy_latex_outputs(tex_path_file, line_out_dir)

        pdf_path = line_out_dir / f"EOL_Line_{line}.pdf"

        if not pdf_path.exists():
            raise RuntimeError(
                f"PDF was not created. Check LaTeX files in: {line_out_dir}"
            )

        return str(pdf_path)

    def cleanup(self):
        shutil.rmtree(self.tmp_root, ignore_errors=True)

    # ---------------------------------------------------------
    # numbering
    # ---------------------------------------------------------
    def reset_numbering(self):
        self._section_counter = 0

    def next_section_number(self) -> str:
        self._section_counter += 1
        return str(self._section_counter)

    # ---------------------------------------------------------
    # layout
    # ---------------------------------------------------------
    def get_layout(self) -> list[dict]:
        """
        V2 reads only user-provided layout.

        Preferred:
            options["report_sections"]

        Alternative:
            options["layout"]

        No default full report layout is used.
        This prevents accidental chart generation or accidental sections.
        """
        layout = self.options.get("report_sections")

        if isinstance(layout, list) and layout:
            return layout

        layout = self.options.get("layout")

        if isinstance(layout, list) and layout:
            return layout

        return []

    def build_sections(self, line: str):
        result = []

        for cfg in self.get_layout():
            section = self.build_section_from_config(line, cfg)

            if section:
                result.append(section)

        return result

    def build_section_from_config(self, line: str, cfg: dict):
        """
        Convert one section config dictionary to LaTeX section dict.

        This function only passes content to sections_v2.build_universal_section().
        It does not create charts.
        """
        if not isinstance(cfg, dict):
            return None

        key = cfg.get("key") or "section"
        title = (
            cfg.get("section_title")
            or cfg.get("title")
            or str(key).replace("_", " ").title()
        )

        return build_universal_section(
            self,
            line,
            key=key,
            section_title=title,
            section_number=cfg.get("section_number"),
            numbering=cfg.get("numbering", True),
            orientation=cfg.get("orientation", "portrait"),
            content_type=cfg.get("content_type", "text"),
            content=cfg.get("content", ""),
            label=cfg.get("label", ""),
            width_ratio=float(cfg.get("width_ratio", 0.97)),
            height_ratio=float(cfg.get("height_ratio", 0.82)),
            include_total_pages=bool(cfg.get("include_total_pages", True)),
            html_table_options=cfg.get("html_table_options") or {},
        )

    # ---------------------------------------------------------
    # optional content helpers
    # ---------------------------------------------------------
    def get_chart(self, line: str, key: str):
        """
        Backward-compatible helper.

        V2 does not generate charts, but if you manually put something into
        self._charts[line][key], sections can still read it.
        """
        charts = self._charts.get(str(line), {})
        return charts.get(key)

    def get_chart_group(self, line: str, key: str):
        """
        Backward-compatible helper.

        V2 does not generate charts.
        """
        value = self.get_chart(line, key)

        if not value:
            return []

        if isinstance(value, (list, tuple)):
            return list(value)

        return [value]

    # ---------------------------------------------------------
    # static assets
    # ---------------------------------------------------------
    def get_tgs_logo_path(self) -> str:
        rel_path = "baseproject/images/2024_TGS_logo_blue.png"
        abs_path = finders.find(rel_path)

        if abs_path and Path(abs_path).exists():
            return abs_path

        return ""

    def get_seisweblog_logo_path(self) -> str:
        rel_path = "baseproject/images/seisweblog_logo.png"
        abs_path = finders.find(rel_path)

        if abs_path and Path(abs_path).exists():
            return abs_path

        return ""

    def get_front_page_background_path(self) -> str:
        rel_path = "baseproject/images/eol_front_bg.png"
        abs_path = finders.find(rel_path)

        if abs_path and Path(abs_path).exists():
            return abs_path

        return ""

    # ---------------------------------------------------------
    # common report metadata
    # ---------------------------------------------------------
    def get_report_title(self, line: str) -> str:
        return (
            self.options.get("report_title")
            or self.options.get("title")
            or f"EOL Report - Line {line}"
        )

    def get_project_name(self) -> str:
        return (
            self.options.get("project_name")
            or getattr(self.prj_info, "ProjectName", "")
            or getattr(self.prj_info, "name", "")
            or "Project"
        )

    def get_client_name(self) -> str:
        return (
            self.options.get("client_name")
            or getattr(self.prj_info, "ClientName", "")
            or ""
        )

    def get_vessel_name_for_line(self, line: str) -> str:
        return self.options.get("vessel_name") or ""

    # ---------------------------------------------------------
    # LaTeX rendering
    # ---------------------------------------------------------
    def render_report_tex(self, line: str) -> str:
        sections = self.build_sections(line)
        body = "\n".join(section.get("latex", "") for section in sections)

        project_name = tex_escape(self.get_project_name())
        report_title = tex_escape(self.get_report_title(line))
        line_txt = tex_escape(str(line))
        prepared_by = tex_escape(self.options.get("prepared_by") or "")

        tgs_logo_path = self.get_tgs_logo_path()

        if tgs_logo_path and self.options.get("include_tgs_logo", True):
            tgs_logo_latex = (
                rf"\includegraphics[height=20pt]{{{tex_path(tgs_logo_path)}}}"
            )
        else:
            tgs_logo_latex = ""

        page_size = str(self.options.get("page_size") or "A4").lower()
        paper_opt = "letterpaper" if page_size == "letter" else "a4paper"

        page_numbers = bool(self.options.get("include_page_numbers", True))

        footer_left = project_name
        footer_center = self.options.get("footer_center") or "End Of Line Report"
        footer_right = (
            r"Page \thepage\ of \pageref{LastPage}"
            if page_numbers
            else ""
        )

        return rf"""
\documentclass[11pt,{paper_opt}]{{article}}

\usepackage[utf8]{{inputenc}}
\usepackage[T1]{{fontenc}}
\usepackage{{graphicx}}
\usepackage{{geometry}}
\usepackage{{hyperref}}
\usepackage{{pdflscape}}
\usepackage{{fancyhdr}}
\usepackage[table]{{xcolor}}
\usepackage{{eso-pic}}
\usepackage{{tikz}}
\usepackage{{tabularx}}
\usepackage{{longtable}}
\usepackage{{array}}
\usepackage{{booktabs}}
\usepackage{{float}}
\usepackage{{lastpage}}
\usepackage{{multirow}}
\usepackage{{makecell}}

\geometry{{
    top=14mm,
    bottom=14mm,
    left=14mm,
    right=14mm,
    headheight=56pt,
    headsep=30mm,
    footskip=30mm
}}

\setlength{{\parindent}}{{0pt}}
\setlength{{\parskip}}{{6pt}}
\setcounter{{secnumdepth}}{{0}}

\definecolor{{headerblueA}}{{RGB}}{{230,242,250}}
\definecolor{{headerblueB}}{{RGB}}{{188,220,238}}
\definecolor{{titleblue}}{{RGB}}{{11,79,108}}
\definecolor{{softline}}{{RGB}}{{120,170,198}}

\hypersetup{{
    colorlinks=true,
    linkcolor=blue,
    urlcolor=blue,
    pdftitle={{{report_title}}},
    pdfauthor={{{prepared_by}}}
}}

\fancypagestyle{{swlportrait}}{{%
    \fancyhf{{}}

    % ---------------- HEADER ----------------
    \fancyhead[L]{{
    \begin{{tikzpicture}}[remember picture,overlay]
        \shade[left color=headerblueA,right color=headerblueB]
            (current page.north west)
            rectangle ([yshift=-40pt]current page.north east);

        \draw[color=softline, line width=0.8pt]
            ([yshift=-40pt]current page.north west) --
            ([yshift=-40pt]current page.north east);

        \node[anchor=west] at ([xshift=6mm,yshift=-20pt]current page.north west)
        {{{tgs_logo_latex}}};

        \node[anchor=center] at ([yshift=-20pt]current page.north)
        {{\fontsize{{14}}{{16}}\selectfont\bfseries\color{{titleblue}} End Of Line Report}};

        \node[anchor=east] at ([xshift=-6mm,yshift=-20pt]current page.north east)
        {{\fontsize{{11}}{{13}}\selectfont\bfseries\color{{titleblue}} Line {line_txt}}};
    \end{{tikzpicture}}
    }}

    % ---------------- FOOTER ----------------
    \fancyfoot[L]{{
    \begin{{tikzpicture}}[remember picture,overlay]
        \shade[left color=headerblueA,right color=headerblueB]
            ([yshift=40pt]current page.south west)
            rectangle (current page.south east);

        \draw[color=softline, line width=0.8pt]
            ([yshift=40pt]current page.south west) --
            ([yshift=40pt]current page.south east);

        \node[anchor=west] at ([xshift=6mm,yshift=20pt]current page.south west)
        {{\fontsize{{10}}{{12}}\selectfont {footer_left}}};

        \node[anchor=center] at ([yshift=20pt]current page.south)
        {{\fontsize{{10}}{{12}}\selectfont {footer_center}}};

        \node[anchor=east] at ([xshift=-6mm,yshift=20pt]current page.south east)
        {{\fontsize{{10}}{{12}}\selectfont {footer_right}}};
    \end{{tikzpicture}}
    }}

    \renewcommand{{\headrulewidth}}{{0pt}}
    \renewcommand{{\footrulewidth}}{{0pt}}
}}

\fancypagestyle{{swllandscape}}{{%
    \fancyhf{{}}
    \renewcommand{{\headrulewidth}}{{0pt}}
    \renewcommand{{\footrulewidth}}{{0pt}}
}}

\pagestyle{{swlportrait}}

\begin{{document}}

{body}

\end{{document}}
"""

    # ---------------------------------------------------------
    # compile
    # ---------------------------------------------------------
    def _compile_latex(self, tex_path_file: Path):
        workdir = tex_path_file.parent
        tex_name = tex_path_file.name

        runs = int(self.options.get("latex_runs") or 2)

        for _ in range(runs):
            proc = subprocess.run(
                [
                    "pdflatex",
                    "-interaction=nonstopmode",
                    "-halt-on-error",
                    tex_name,
                ],
                cwd=str(workdir),
                capture_output=True,
                text=True,
            )

            if proc.returncode != 0:
                log_file = tex_path_file.with_suffix(".log")
                err_text = proc.stdout + "\n" + proc.stderr

                raise RuntimeError(
                    f"LaTeX compilation failed for {tex_name}\n"
                    f"Workdir: {workdir}\n"
                    f"Log file: {log_file}\n\n"
                    f"{err_text}"
                )

    def _copy_latex_outputs(self, tex_path_file: Path, final_dir: Path) -> dict:
        final_dir.mkdir(parents=True, exist_ok=True)

        copied = {}

        for ext in [".tex", ".pdf", ".log", ".aux", ".toc", ".out"]:
            src = tex_path_file.with_suffix(ext)

            if src.exists():
                dst = final_dir / src.name
                shutil.copy2(src, dst)
                copied[ext.lstrip(".")] = str(dst)

        return copied


# Backward-compatible alias, optional.
EOLReportGenerator = EOLReportGeneratorV2
