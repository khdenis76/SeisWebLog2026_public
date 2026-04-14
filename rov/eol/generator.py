import math
import shutil
import subprocess
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from django.contrib.staticfiles import finders

from .latex_utils import tex_escape, tex_path
from .sections import SECTION_BUILDERS
from rov.dsr_line_graphics_matplotlib import DSRLineGraphicsMatplotlib

from core.projectdb import ProjectDB
from rov.dsrclass import DSRDB


class EOLReportGenerator:
    """
    LaTeX-based End Of Line report generator.
    """

    def __init__(self, db_path, request_user, options):
        self.db_path = str(db_path)
        self.request_user = request_user
        self.options = options or {}

        self.pdb = ProjectDB(self.db_path)
        self.dsrdb = DSRDB(self.db_path)
        self.prj_info = self.pdb.get_main()

        reports_dir = self.options.get("reports_dir")
        self.reports_root = Path(reports_dir) if reports_dir else None
        if self.reports_root:
            self.reports_root.mkdir(parents=True, exist_ok=True)
        else:
            raise RuntimeError("reports_dir is not configured.")

        # compile inside reports folder, not C:\Users\...\AppData\Local\Temp
        self.tmp_root = self.reports_root / "_tmp_eol"
        self.tmp_root.mkdir(parents=True, exist_ok=True)

        self._charts = {}

    # ---------------------------------------------------------
    # public API
    # ---------------------------------------------------------
    def build_zip_for_lines(self, lines):
        out_paths = []
        try:
            for line in lines:
                out_paths.append(self.build_line_report(line))

            zip_path = self.tmp_root / "EOL_Reports.zip"
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for p in out_paths:
                    zf.write(p, arcname=Path(p).name)
            return str(zip_path)
        except Exception:
            self.cleanup()
            raise

    def get_line_output_dir(self, line: str) -> Path:
        line = str(line).strip()

        if not self.reports_root:
            raise RuntimeError("reports_dir is not configured.")

        out_dir = self.reports_root / f"{line}_EOL"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

    def build_line_report(self, line):
        line = str(line).strip()

        line_out_dir = self.get_line_output_dir(line)
        line_tmp_dir = line_out_dir / "_compile"
        line_tmp_dir.mkdir(parents=True, exist_ok=True)

        self._prepare_assets(line=line, out_dir=line_tmp_dir)

        tex_path_file = line_tmp_dir / f"EOL_Line_{line}.tex"
        tex_content = self.render_report_tex(line=line)
        tex_path_file.write_text(tex_content, encoding="utf-8")

        try:
            self._compile_latex(tex_path_file)
        finally:
            # always copy whatever exists for debugging
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
    # helpers
    # ---------------------------------------------------------
    def get_report_title(self, line: str) -> str:
        return f"EOL Report - Line {line}"

    def get_chart(self, line: str, key: str):
        charts = getattr(self, "_charts", {}).get(str(line), {})
        return charts.get(key)

    def get_chart_group(self, line: str, key: str):
        charts = getattr(self, "_charts", {}).get(str(line), {})
        value = charts.get(key)
        if not value:
            return []
        if isinstance(value, (list, tuple)):
            return list(value)
        return [value]

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

    # ---------------------------------------------------------
    # LaTeX rendering
    # ---------------------------------------------------------
    def build_sections(self, line: str):
        selected = list(self.options.get("sections") or [])
        result = []

        for key in selected:
            builder = SECTION_BUILDERS.get(key)
            if not builder:
                continue
            section = builder(self, line)
            if section:
                result.append(section)

        return result

    def render_report_tex(self, line: str) -> str:
        sections = self.build_sections(line)
        body = "\n".join(section["latex"] for section in sections)

        project_name = tex_escape(self.options.get("project_name") or "Project")
        report_title = tex_escape(self.get_report_title(line))
        line_txt = tex_escape(str(line))
        prepared_by = tex_escape(self.options.get("prepared_by") or "")

        tgs_logo_path = self.get_tgs_logo_path()
        if tgs_logo_path and self.options.get("include_tgs_logo", True):
            tgs_logo_latex = rf"\includegraphics[height=20pt]{{{tex_path(tgs_logo_path)}}}"
        else:
            tgs_logo_latex = ""

        return rf"""
    \documentclass[11pt,a4paper]{{article}}

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

    % bigger gap under header
    \geometry{{margin=14mm,headheight=56pt,headsep=24mm,footskip=12mm}}

    \setlength{{\parindent}}{{0pt}}
    \setlength{{\parskip}}{{6pt}}

    % gradient header colors
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

    % ---------- portrait page style ----------
    \fancypagestyle{{swlportrait}}{{%
        \fancyhf{{}}
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
        \fancyfoot[L]{{\small {project_name}}}
        \fancyfoot[C]{{\small End Of Line Report}}
        \fancyfoot[R]{{\small Page \thepage}}
        \renewcommand{{\headrulewidth}}{{0pt}}
        \renewcommand{{\footrulewidth}}{{0.4pt}}
    }}

    \pagestyle{{swlportrait}}

    \begin{{document}}

    {body}

    \end{{document}}
    """

    def render_front_page(self, line: str) -> str:
        now = datetime.now()
        date_str = now.strftime("%d %b %Y")
        time_str = now.strftime("%H:%M")

        bg_image = self.get_front_page_background_path()
        tgs_logo = self.get_tgs_logo_path()

        project_name = tex_escape(
            self.options.get("project_name")
            or getattr(self.prj_info, "name", "")
            or "Project"
        )
        vessel_name = tex_escape(self.options.get("vessel_name") or "")
        client_name = tex_escape(self.options.get("client_name") or "")
        prepared_by = tex_escape(self.options.get("prepared_by") or "")
        line_txt = tex_escape(str(line))

        bg_block = ""
        if bg_image:
            bg_block = rf"""
    \AddToShipoutPictureBG*{{
        \includegraphics[width=\paperwidth,height=\paperheight]{{{tex_path(bg_image)}}}
    }}
    """

        tgs_logo_block = ""
        if tgs_logo and self.options.get("include_tgs_logo", True):
            tgs_logo_block = rf"\includegraphics[height=1.15cm]{{{tex_path(tgs_logo)}}}"

        return rf"""
    \begin{{titlepage}}
    {bg_block}
    \thispagestyle{{empty}}

    \vspace*{{0.6cm}}

    \noindent
    {tgs_logo_block}

    \vspace*{{2.0cm}}

    \begin{{center}}
    {{\fontsize{{28}}{{32}}\selectfont\bfseries\color[HTML]{{0B4F6C}} END OF LINE REPORT}}\\[0.3cm]
    {{\fontsize{{20}}{{24}}\selectfont\bfseries\color[HTML]{{114E66}} Line {line_txt}}}
    \end{{center}}

    \vspace{{1.2cm}}

    \begin{{center}}
    \color[HTML]{{0B4F6C}}
    \rule{{0.72\textwidth}}{{0.8pt}}
    \end{{center}}

    \vspace{{1.0cm}}

    \begin{{center}}
    \renewcommand{{\arraystretch}}{{1.55}}
    \setlength{{\tabcolsep}}{{8pt}}
    \begin{{tabular}}{{p{{3.2cm}} p{{9.2cm}}}}
    {{\bfseries\color[HTML]{{0B4F6C}} Project}} & {project_name} \\
    {{\bfseries\color[HTML]{{0B4F6C}} Line}} & {line_txt} \\
    {{\bfseries\color[HTML]{{0B4F6C}} Vessel}} & {vessel_name} \\
    {{\bfseries\color[HTML]{{0B4F6C}} Client}} & {client_name} \\
    {{\bfseries\color[HTML]{{0B4F6C}} Date}} & {date_str} \\
    {{\bfseries\color[HTML]{{0B4F6C}} Time}} & {time_str} \\
    {{\bfseries\color[HTML]{{0B4F6C}} Prepared By}} & {prepared_by} \\
    \end{{tabular}}
    \end{{center}}

    \end{{titlepage}}
    """

    # ---------------------------------------------------------
    # pdflatex compile
    # ---------------------------------------------------------
    def _compile_latex(self, tex_path_file: Path):
        workdir = tex_path_file.parent
        tex_name = tex_path_file.name

        for _ in range(2):
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

    # ---------------------------------------------------------
    # data adapters
    # ---------------------------------------------------------
    def get_line_summary(self, line: str) -> dict:
        line = str(line).strip()

        self.dsrdb.ensure_dsr_line_summary_ready(rebuild_if_empty=True)

        with self.dsrdb._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM DSR_LineSummary
                WHERE CAST(Line AS TEXT) = ?
                LIMIT 1
                """,
                (line,),
            ).fetchone()

        if not row:
            return {
                "Line": line,
                "Status": "No summary data found",
            }

        def _v(key, default=""):
            val = row[key] if key in row.keys() else default
            if val is None:
                return default
            return val

        def _fmt_num(val, nd=1):
            if val in (None, ""):
                return ""
            try:
                return f"{float(val):,.{nd}f}"
            except Exception:
                return str(val)

        def _fmt_int(val):
            if val in (None, ""):
                return ""
            try:
                return f"{int(round(float(val))):,}"
            except Exception:
                return str(val)

        return {
            "Line": _v("Line"),
            "Planned Points": _fmt_int(_v("PlannedPoints")),
            "Stations": _fmt_int(_v("Stations")),
            "Nodes": _fmt_int(_v("Nodes")),
            "Min Station": _fmt_int(_v("MinStation")),
            "Max Station": _fmt_int(_v("MaxStation")),

            "Deployed Count": _fmt_int(_v("DeployedCount")),
            "Retrieved Count": _fmt_int(_v("RetrievedCount")),
            "Processed Count": _fmt_int(_v("ProcessedCount")),

            "Deployment %": _fmt_num(_v("DeployedPct")),
            "Recovery %": _fmt_num(_v("RetrievedPct")),
            "Processed %": _fmt_num(_v("ProcessedPct")),

            "Deployment ROVs": _v("DepROVs"),
            "Deployment by ROV": _v("DepROVStats"),
            "Recovery ROVs": _v("RecROVs"),
            "Recovery by ROV": _v("RecROVStats"),

            "Start of Deployment": _v("FirstDeployTime"),
            "End of Deployment": _v("LastDeployTime"),
            "Deployment Duration, h": _fmt_num(_v("DeploymentHours"), 2),

            "Start of Recovery": _v("StartOfRec"),
            "End of Recovery": _v("EndOfRec"),
            "Recovery Duration, h": _fmt_num(_v("RecDuration"), 2),

            "Average Delta Easting Primary to Secondary, m": _fmt_num(_v("AvgDeltaE"), 2),
            "Minimum Delta Easting Primary to Secondary, m": _fmt_num(_v("MinDeltaE"), 2),
            "Maximum Delta Easting Primary to Secondary, m": _fmt_num(_v("MaxDeltaE"), 2),

            "Average Delta Northing Primary to Secondary, m": _fmt_num(_v("AvgDeltaN"), 2),
            "Minimum Delta Northing Primary to Secondary, m": _fmt_num(_v("MinDeltaN"), 2),
            "Maximum Delta Northing Primary to Secondary, m": _fmt_num(_v("MaxDeltaN"), 2),

            "Average Delta Easting Recovery Primary to Secondary, m": _fmt_num(_v("AvgDeltaE1"), 2),
            "Minimum Delta Easting Recovery Primary to Secondary, m": _fmt_num(_v("MinDeltaE1"), 2),
            "Maximum Delta Easting Recovery Primary to Secondary, m": _fmt_num(_v("MaxDeltaE1"), 2),

            "Average Delta Northing Recovery Primary to Secondary, m": _fmt_num(_v("AvgDeltaN1"), 2),
            "Minimum Delta Northing Recovery Primary to Secondary, m": _fmt_num(_v("MinDeltaN1"), 2),
            "Maximum Delta Northing Recovery Primary to Secondary, m": _fmt_num(_v("MaxDeltaN1"), 2),

            "Average Sigma Primary Easting, m": _fmt_num(_v("AvgSigma"), 2),
            "Minimum Sigma Primary Easting, m": _fmt_num(_v("MinSigma"), 2),
            "Maximum Sigma Primary Easting, m": _fmt_num(_v("MaxSigma"), 2),

            "Average Sigma Primary Northing, m": _fmt_num(_v("AvgSigma1"), 2),
            "Minimum Sigma Primary Northing, m": _fmt_num(_v("MinSigma1"), 2),
            "Maximum Sigma Primary Northing, m": _fmt_num(_v("MaxSigma1"), 2),

            "Average Sigma Secondary Easting, m": _fmt_num(_v("AvgSigma2"), 2),
            "Minimum Sigma Secondary Easting, m": _fmt_num(_v("MinSigma2"), 2),
            "Maximum Sigma Secondary Easting, m": _fmt_num(_v("MaxSigma2"), 2),

            "Average Sigma Secondary Northing, m": _fmt_num(_v("AvgSigma3"), 2),
            "Minimum Sigma Secondary Northing, m": _fmt_num(_v("MinSigma3"), 2),
            "Maximum Sigma Secondary Northing, m": _fmt_num(_v("MaxSigma3"), 2),

            "Average Radial Offset to Preplot, m": _fmt_num(_v("AvgRadOffset"), 2),
            "Minimum Radial Offset to Preplot, m": _fmt_num(_v("MinRadOffset"), 2),
            "Maximum Radial Offset to Preplot, m": _fmt_num(_v("MaxRadOffset"), 2),

            "Average Range Primary to Secondary, m": _fmt_num(_v("AvgRangePrimToSec"), 2),
            "Minimum Range Primary to Secondary, m": _fmt_num(_v("MinRangePrimToSec"), 2),
            "Maximum Range Primary to Secondary, m": _fmt_num(_v("MaxRangePrimToSec"), 2),

            "Average Primary Elevation, m": _fmt_num(_v("AvgPrimaryElevation"), 2),
            "Minimum Primary Elevation, m": _fmt_num(_v("MinPrimaryElevation"), 2),
            "Maximum Primary Elevation, m": _fmt_num(_v("MaxPrimaryElevation"), 2),

            "Average Secondary Elevation, m": _fmt_num(_v("AvgSecondaryElevation"), 2),
            "Minimum Secondary Elevation, m": _fmt_num(_v("MinSecondaryElevation"), 2),
            "Maximum Secondary Elevation, m": _fmt_num(_v("MaxSecondaryElevation"), 2),

            "Primary e95, m": _fmt_num(_v("Primary_e95"), 2),
            "Primary n95, m": _fmt_num(_v("Primary_n95"), 2),
        }

    def get_dsr_info_table(self, line: str) -> dict:
        return {
            "Line": line,
            "DB Path": self.db_path,
            "Prepared By": self.options.get("prepared_by") or "",
            "Client": self.options.get("client_name") or "",
        }

    # ---------------------------------------------------------
    # chart generation
    # ---------------------------------------------------------
    def _prepare_assets(self, line: str, out_dir: Path):
        self._charts[str(line)] = self._collect_or_generate_charts(line, out_dir)

    def _collect_or_generate_charts(self, line: str, out_dir: Path) -> dict:
        charts = {}
        out_dir.mkdir(parents=True, exist_ok=True)

        g = DSRLineGraphicsMatplotlib(self.db_path)

        df = g.read_dsr_for_line(int(line))
        if df is None or df.empty:
            return charts

        df = df.copy()

        def num(c):
            if c not in df.columns:
                df[c] = pd.NA
            df[c] = pd.to_numeric(df[c], errors="coerce")

        for c in [
            "Sigma1", "Sigma2", "Sigma3", "Sigma4", "Sigma5",
            "PreplotEasting", "PreplotNorthing",
            "PrimaryEasting", "PrimaryNorthing",
            "SecondaryEasting", "SecondaryNorthing",
            "PrimaryElevation", "SecondaryElevation",
            "DeltaEprimarytosecondary", "DeltaNprimarytosecondary",
            "Rangeprimarytosecondary",
            "PrimaryEasting1", "PrimaryNorthing1",
        ]:
            num(c)

        df["Primary_e95"] = df["Sigma1"] * math.sqrt(5.991)
        df["Primary_n95"] = df["Sigma2"] * math.sqrt(5.991)
        df["Primary_z95"] = df["Sigma5"] * math.sqrt(5.991)
        df["Secondary_e95"] = df["Sigma3"] * math.sqrt(5.991)
        df["Secondary_n95"] = df["Sigma4"] * math.sqrt(5.991)

        df["dX_primary"] = df["PreplotEasting"] - df["PrimaryEasting"]
        df["dY_primary"] = df["PreplotNorthing"] - df["PrimaryNorthing"]
        df["dX_secondary"] = df["PreplotEasting"] - df["SecondaryEasting"]
        df["dY_secondary"] = df["PreplotNorthing"] - df["SecondaryNorthing"]

        if "PrimaryEasting1" in df.columns:
            df["dX_primary1"] = df["PreplotEasting"] - df["PrimaryEasting1"]
        if "PrimaryNorthing1" in df.columns:
            df["dY_primary1"] = df["PreplotNorthing"] - df["PrimaryNorthing1"]

        try:
            with g._connect() as conn:
                rp_df = pd.read_sql_query(
                    "SELECT * FROM RPPreplot ORDER BY Line, Point",
                    conn
                )

            _, path = g.plot_project_map(
                rp_df=rp_df,
                dsr_df=df,
                selected_line=int(line),
                show_station_labels=True,
                save_dir=out_dir,
                suffix="project_map",
                is_show=False,
                figsize=(16, 10),
                close=True,
            )
            charts["project_map"] = path
        except Exception:
            pass

        try:
            _, path = g.two_series_vs_station_with_diff_bar(
                df=df,
                series1_col="PrimaryElevation",
                series2_col="SecondaryElevation",
                series1_label="PRIMARY DEPTH",
                series2_label="SECONDARY DEPTH",
                y_label="Depth",
                diff_y_label="Primary - Secondary",
                save_dir=out_dir,
                suffix="water",
                rov_col="ROV",
                is_show=False,
                close=True,
            )
            charts["water_depth"] = path
        except Exception:
            pass

        try:
            _, path = g.three_vbar_by_category_shared_x(
                df=df,
                rov_col="ROV",
                is_show=False,
                reverse_y_if_negative=False,
                y1_col="DeltaEprimarytosecondary",
                y2_col="DeltaNprimarytosecondary",
                y3_col="Rangeprimarytosecondary",
                title1="Δ Easting Primary(INS) to Secondary(USBL)",
                title2="Δ Northing Primary(INS) to Secondary(USBL)",
                title3="Radial Offset Primary(INS) to Secondary(USBL)",
                y1_label="ΔE",
                y2_label="ΔN",
                y3_label="Rad. Offset",
                y_axis_label="Offset, m",
                save_dir=out_dir,
                suffix="primsec",
                close=True,
            )
            charts["primary_secondary_offsets"] = path
        except Exception:
            pass

        try:
            _, path = g.dxdy_primary_secondary_with_hists(
                df=df,
                dx_p_col="dX_primary",
                dy_p_col="dY_primary",
                dx_s_col="dX_secondary",
                dy_s_col="dY_secondary",
                title="DSR dX/dY (Primary & Secondary)",
                red_radius=20,
                red_is_show=True,
                p_name="Primary",
                s_name="Secondary",
                bins=40,
                padding_ratio=0.10,
                is_show=False,
                save_dir=out_dir,
                suffix="delta",
                close=True,
            )
            charts["delta_scatter"] = path
        except Exception:
            pass

        try:
            _, path = g.deployment_offsets_vs_preplot(
                df=df,
                line=int(line),
                line_bearing=0,
                save_dir=out_dir,
                suffix="deplpre",
                is_show=False,
                close=True,
            )
            charts["deployment_vs_preplot"] = path
        except Exception:
            pass

        try:
            _, path = g.graph_recover_time(
                df=df,
                line=int(line),
                is_deploy=False,
                save_dir=out_dir,
                suffix="timing_recover",
                is_show=False,
                close=True,
            )
            charts["recovery_timing"] = path
        except Exception:
            pass

        if bool(self.options.get("include_deploy_timing", False)):
            try:
                _, path = g.graph_recover_time(
                    df=df,
                    line=int(line),
                    is_deploy=True,
                    save_dir=out_dir,
                    suffix="timing_deploy",
                    is_show=False,
                    close=True,
                )
                charts["deployment_timing"] = path
            except Exception:
                pass

        return charts