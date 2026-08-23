import math
import shutil
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Circle, Ellipse, Rectangle


class NodePositionComparisonReport:
    """
    LaTeX PDF report for node position comparison.

    Report structure:
    - Page 1: branded front page with TGS logo
    - Every content page: TGS report header and page frame
    - Page 2: line information from DSR_LineSummary
    - Next pages: node comparison table, split automatically by row count
    - Next page: XY offsets analysis with red QC circle and 95% coherence ellipse
    - Next page: Station vs dX/dY plots
    - Last page: Station vs In-Line, X-Line, and Radial offsets
    """

    def __init__(self, db_path, project=None, logo_path=None):
        self.db_path = Path(db_path)
        self.project = project
        self.logo_path = Path(logo_path) if logo_path else None

    def _find_tgs_logo(self):
        """Locate ``logos/2024_TGS_logo_blue.png`` from the program root."""
        candidates = []
        if self.logo_path:
            candidates.append(self.logo_path)

        module_path = Path(__file__).resolve()
        if len(module_path.parents) >= 3:
            candidates.append(module_path.parents[2] / "logos" / "2024_TGS_logo_blue.png")

        candidates.extend([
            Path.cwd() / "logos" / "2024_TGS_logo_blue.png",
            self.db_path.parent / "logos" / "2024_TGS_logo_blue.png",
            self.db_path.parent.parent / "logos" / "2024_TGS_logo_blue.png",
        ])

        for candidate in candidates:
            try:
                candidate = candidate.expanduser().resolve()
            except Exception:
                continue
            if candidate.is_file():
                return candidate

        checked = "\n - ".join(str(p) for p in candidates)
        raise FileNotFoundError(
            "TGS logo was not found. Expected 2024_TGS_logo_blue.png. "
            f"Checked:\n - {checked}"
        )

    def _prepare_tgs_logo(self, build_dir):
        """Copy the logo into the LaTeX build folder and return its path."""
        source = self._find_tgs_logo()
        assets_dir = Path(build_dir) / "assets"
        assets_dir.mkdir(parents=True, exist_ok=True)
        target = assets_dir / "tgs_logo.png"
        shutil.copy2(source, target)
        return target

    # ------------------------------------------------------------------
    # DATA
    # ------------------------------------------------------------------
    def load_line_data(self, line):
        """
        Load DSR deployment/preplot coordinates and REC_DB first-break coordinates.
        REC_DB can contain multiple records per station, so REC_X/Y/Z are averaged.
        """
        sql = """
            WITH rec AS (
                SELECT
                    Line,
                    Point AS Station,
                    AVG(REC_X) AS REC_X,
                    AVG(REC_Y) AS REC_Y,
                    AVG(REC_Z) AS REC_Z
                FROM REC_DB
                WHERE Line = ?
                GROUP BY Line, Point
            )
            SELECT
                d.Line,
                d.Station,
                d.Node,
                d.ROV,
                d.ROV1,

                d.PreplotEasting AS pp_x,
                d.PreplotNorthing AS pp_y,

                d.PrimaryEasting AS dep_x,
                d.PrimaryNorthing AS dep_y,
                d.PrimaryElevation AS dep_z,

                d.PrimaryEasting1 AS rcv_x,
                d.PrimaryNorthing1 AS rcv_y,
                d.PrimaryElevation1 AS rcv_z,

                rec.REC_X AS fb_x,
                rec.REC_Y AS fb_y,
                rec.REC_Z AS fb_z

            FROM DSR d
            LEFT JOIN rec
                ON rec.Line = d.Line
               AND rec.Station = d.Station
            WHERE d.Line = ?
              AND d.PreplotEasting IS NOT NULL
              AND d.PreplotNorthing IS NOT NULL
              AND d.PrimaryEasting IS NOT NULL
              AND d.PrimaryNorthing IS NOT NULL
            ORDER BY d.Station;
        """

        with sqlite3.connect(str(self.db_path)) as conn:
            df = pd.read_sql_query(sql, conn, params=[line, line])

        if df.empty:
            return df

        return self._calculate_offsets(df)

    def load_line_summary(self, line):
        sql = """
            SELECT
                Line,
                PlannedPoints,
                Stations,
                Nodes,
                MinStation,
                MaxStation,
                DeployedCount,
                RetrievedCount,
                ProcessedCount,
                FirstDeployTime,
                LastDeployTime,
                DeploymentHours,
                StartOfRec,
                EndOfRec,
                RecDuration,
                DeployedPct,
                RetrievedPct,
                ProcessedPct,
                AvgDeltaE,
                MinDeltaE,
                MaxDeltaE,
                AvgDeltaN,
                MinDeltaN,
                MaxDeltaN,
                Primary_e95,
                Primary_n95,
                AvgRadOffset,
                MinRadOffset,
                MaxRadOffset
            FROM DSR_LineSummary
            WHERE Line = ?
            LIMIT 1;
        """

        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(sql, [line]).fetchone()
        except Exception as exc:
            print(f"[NodePositionReport] load_line_summary failed: {exc}")
            return {}

        if not row:
            return {}

        return dict(row)

    def load_project_main(self):
        sql = """
            SELECT
                name,
                location,
                area,
                client,
                contractor,
                project_client_id,
                project_contractor_id,
                epsg
            FROM project_main
            WHERE id = 1
            LIMIT 1;
        """

        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(sql).fetchone()
        except Exception as exc:
            print(f"[NodePositionReport] load_project_main failed: {exc}")
            return {}

        if not row:
            return {}

        return dict(row)

    def load_node_qc_settings(self):
        """
        Load ALL fields from project_node_qc table.
        Returns dict with lowercase keys.
        """
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute("""
                    SELECT *
                    FROM project_node_qc
                    LIMIT 1;
                """).fetchone()

            if not row:
                return {}

            return {k.lower(): row[k] for k in row.keys()}

        except Exception as exc:
            print(f"[NodeQC] load_node_qc_settings failed: {exc}")
            return {}

    def _calculate_offsets(self, df):
        """
        Calculate all position differences used in the table and charts.

        Table columns keep the original names used by the existing report.
        Plot pages also get clear measurement-direction columns:
        - dep_pp_*  = Deployment - Preplot
        - rec_pp_*  = REC_DB / First Break - Preplot
        - rcv_pp_* = Recovery - Preplot
        - dep_rcv_* = Deployment - Recovery
        - fb_rcv_* = REC_DB / First Break - Recovery
        - dep_rec_* = Deployment - REC_DB / First Break (legacy plot name)
        """
        df = df.copy()

        # Existing table convention: Preplot vs Deployment = Preplot - Deployment
        df["pp_dep_dx"] = df["pp_x"] - df["dep_x"]
        df["pp_dep_dy"] = df["pp_y"] - df["dep_y"]
        df["pp_dep_dz"] = 0 - df["dep_z"]
        df["pp_dep_dr"] = (df["pp_dep_dx"] ** 2 + df["pp_dep_dy"] ** 2) ** 0.5

        # Existing table convention: First Break vs Deployment = REC_DB - Deployment
        df["fb_dep_dx"] = df["fb_x"] - df["dep_x"]
        df["fb_dep_dy"] = df["fb_y"] - df["dep_y"]
        df["fb_dep_dz"] = df["fb_z"] - df["dep_z"]
        df["fb_dep_dr"] = (df["fb_dep_dx"] ** 2 + df["fb_dep_dy"] ** 2) ** 0.5

        # Existing table convention: First Break vs Preplot = REC_DB - Preplot
        df["fb_pp_dx"] = df["fb_x"] - df["pp_x"]
        df["fb_pp_dy"] = df["fb_y"] - df["pp_y"]
        df["fb_pp_dz"] = df["fb_z"] - 0
        df["fb_pp_dr"] = (df["fb_pp_dx"] ** 2 + df["fb_pp_dy"] ** 2) ** 0.5

        # Plot/report naming convention requested by user.
        df["dep_pp_dx"] = df["dep_x"] - df["pp_x"]
        df["dep_pp_dy"] = df["dep_y"] - df["pp_y"]
        df["dep_pp_dr"] = (df["dep_pp_dx"] ** 2 + df["dep_pp_dy"] ** 2) ** 0.5

        df["rec_pp_dx"] = df["fb_x"] - df["pp_x"]
        df["rec_pp_dy"] = df["fb_y"] - df["pp_y"]
        df["rec_pp_dr"] = (df["rec_pp_dx"] ** 2 + df["rec_pp_dy"] ** 2) ** 0.5

        df["rcv_pp_dx"] = df["rcv_x"] - df["pp_x"]
        df["rcv_pp_dy"] = df["rcv_y"] - df["pp_y"]
        df["rcv_pp_dr"] = (df["rcv_pp_dx"] ** 2 + df["rcv_pp_dy"] ** 2) ** 0.5

        df["dep_rcv_dx"] = df["dep_x"] - df["rcv_x"]
        df["dep_rcv_dy"] = df["dep_y"] - df["rcv_y"]
        df["dep_rcv_dr"] = (df["dep_rcv_dx"] ** 2 + df["dep_rcv_dy"] ** 2) ** 0.5

        df["fb_rcv_dx"] = df["fb_x"] - df["rcv_x"]
        df["fb_rcv_dy"] = df["fb_y"] - df["rcv_y"]
        df["fb_rcv_dr"] = (df["fb_rcv_dx"] ** 2 + df["fb_rcv_dy"] ** 2) ** 0.5

        df["dep_rec_dx"] = df["dep_x"] - df["fb_x"]
        df["dep_rec_dy"] = df["dep_y"] - df["fb_y"]
        df["dep_rec_dr"] = (df["dep_rec_dx"] ** 2 + df["dep_rec_dy"] ** 2) ** 0.5

        # Azimuth from North, clockwise. Positive dX = East, positive dY = North.
        for prefix in ("dep_pp", "rcv_pp", "rec_pp", "dep_rcv", "dep_rec", "fb_dep", "fb_rcv"):
            df[f"{prefix}_az"] = (
                np.degrees(np.arctan2(df[f"{prefix}_dx"], df[f"{prefix}_dy"])) + 360.0
            ) % 360.0

        # Approximate line direction from first/last preplot point.
        dx_line = df["pp_x"].iloc[-1] - df["pp_x"].iloc[0]
        dy_line = df["pp_y"].iloc[-1] - df["pp_y"].iloc[0]
        length = (dx_line ** 2 + dy_line ** 2) ** 0.5 or 1.0

        ux = dx_line / length
        uy = dy_line / length

        # In-line / X-line offsets with requested direction: Deployment/REC_DB vs Preplot.
        df["dep_il"] = df["dep_pp_dx"] * ux + df["dep_pp_dy"] * uy
        df["dep_xl"] = -df["dep_pp_dx"] * uy + df["dep_pp_dy"] * ux

        df["fb_pp_il"] = df["rec_pp_dx"] * ux + df["rec_pp_dy"] * uy
        df["fb_pp_xl"] = -df["rec_pp_dx"] * uy + df["rec_pp_dy"] * ux

        # In-line / X-line components for every coordinate comparison.
        for prefix in ("dep_pp", "rcv_pp", "rec_pp", "dep_rcv", "dep_rec", "fb_dep", "fb_rcv"):
            df[f"{prefix}_il"] = df[f"{prefix}_dx"] * ux + df[f"{prefix}_dy"] * uy
            df[f"{prefix}_xl"] = -df[f"{prefix}_dx"] * uy + df[f"{prefix}_dy"] * ux

        return df

    # ------------------------------------------------------------------
    # MAIN
    # ------------------------------------------------------------------
    def generate_pdf(self, line, output_dir):
        """
        Generate PDF and return final PDF path.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        df = self.load_line_data(line)

        if df.empty:
            raise ValueError(f"No data found for line {line}")

        build_dir = output_dir / f"_build_node_position_{line}"
        build_dir.mkdir(parents=True, exist_ok=True)

        charts_dir = build_dir / "charts"
        charts_dir.mkdir(parents=True, exist_ok=True)

        logo_file = self._prepare_tgs_logo(build_dir)
        self._make_charts(df, charts_dir, line)

        tex_path = build_dir / f"{line}_Node_Position_Comparison.tex"
        pdf_path = output_dir / f"{line}_Node_Position_Comparison.pdf"

        tex_path.write_text(
            self._build_tex(
                line=line,
                df=df,
                charts_dir=charts_dir,
                logo_file=logo_file,
            ),
            encoding="utf-8",
        )

        self._run_pdflatex(tex_path, build_dir)

        built_pdf = build_dir / f"{line}_Node_Position_Comparison.pdf"
        if not built_pdf.exists():
            raise RuntimeError(f"LaTeX did not create PDF: {built_pdf}")

        shutil.copy2(built_pdf, pdf_path)
        return pdf_path

    def _run_pdflatex(self, tex_path, build_dir):
        """
        Run pdflatex three times so LastPage is resolved.
        """
        for _ in range(3):
            result = subprocess.run(
                [
                    "pdflatex",
                    "-interaction=nonstopmode",
                    "-halt-on-error",
                    tex_path.name,
                ],
                cwd=str(build_dir),
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                log_path = build_dir / "latex_error.log"
                log_path.write_text(
                    result.stdout + "\n\n" + result.stderr,
                    encoding="utf-8",
                )
                raise RuntimeError(f"LaTeX failed. See log: {log_path}")

    # ------------------------------------------------------------------
    # LATEX
    # ------------------------------------------------------------------
    def _build_tex(self, line, df, charts_dir, logo_file):
        project_name = (
            getattr(self.project, "name", "SeisWebLog Project")
            if self.project
            else "SeisWebLog Project"
        )

        project_main = self.load_project_main()

        project_name = project_main.get("name") or project_name
        location = project_main.get("location", "")
        area = project_main.get("area", "")
        client = project_main.get("client", "")
        contractor = project_main.get("contractor", "")
        project_client_id = project_main.get("project_client_id", "")
        project_contractor_id = project_main.get("project_contractor_id", "")
        epsg = project_main.get("epsg", "")

        generated_on = datetime.now().strftime("%Y-%m-%d %H:%M")
        tgs_logo_file = self._tex_path(logo_file)

        chart_xy_preplot_file = self._tex_path(charts_dir / "node_position_xy_preplot_page.png")
        chart_xy_cross_file = self._tex_path(charts_dir / "node_position_xy_cross_page.png")
        chart_offsets_file = self._tex_path(
            charts_dir / "node_position_offsets_page.png"
        )
        chart_il_xl_file = self._tex_path(
            charts_dir / "node_position_il_xl_radial_page.png"
        )
        polar_group_2_file = self._tex_path(charts_dir / "node_position_polar_group_2_page.png")
        polar_group_3_file = self._tex_path(charts_dir / "node_position_polar_group_3_page.png")
        polar_stats_file = self._tex_path(charts_dir / "node_position_polar_statistics_page.png")
        cdf_files = {
            key: self._tex_path(charts_dir / f"node_position_cdf_boxplots_{key}_page.png")
            for key in ("dep_pp", "rcv_pp", "rec_pp", "fb_dep", "fb_rcv")
        }
        chart_heatmap_profile_file = self._tex_path(
            charts_dir / "node_position_heatmap_depth_profile_page.png"
        )
        directional_files = {
            key: self._tex_path(charts_dir / f"node_position_directional_{key}_page.png")
            for key in ("dep_pp", "rcv_pp", "rec_pp", "fb_dep", "fb_rcv")
        }
        chart_executive_file = self._tex_path(
            charts_dir / "node_position_executive_summary_page.png"
        )
        chart_line_info_file = self._tex_path(
            charts_dir / "node_position_line_information_page.png"
        )
        chart_qc_dashboard_file = self._tex_path(
            charts_dir / "node_position_qc_dashboard_page.png"
        )

        table_pages = self._build_table_pages(df, rows_per_page=50)

        template = r"""
\documentclass[8pt,landscape]{article}

\usepackage[a4paper,left=7mm,right=7mm,top=24mm,bottom=10mm]{geometry}
\usepackage{graphicx}
\usepackage{array}
\usepackage{booktabs}
\usepackage{multirow}
\usepackage[table]{xcolor}
\usepackage{colortbl}
\usepackage{tikz}
\usepackage{lastpage}
\usepackage{float}
\usepackage{caption}

\setlength{\parindent}{0pt}
\setlength{\tabcolsep}{1.5pt}
\renewcommand{\arraystretch}{1.05}

\definecolor{swlnavy}{HTML}{002060}
\definecolor{ppbg}{HTML}{FFF2CC}
\definecolor{depbg}{HTML}{C6EFCE}
\definecolor{fbbg}{HTML}{D9EAD3}
\definecolor{cmpgreen}{HTML}{D9EAD3}
\definecolor{cmporange}{HTML}{FCE4D6}
\definecolor{cmpred}{HTML}{F4CCCC}
\definecolor{infobg}{HTML}{EAF2F8}

\newcommand{\swlpageframe}{
\begin{tikzpicture}[remember picture,overlay]
\draw[swlnavy,line width=1.1pt]
([xshift=4mm,yshift=-4mm]current page.north west)
rectangle
([xshift=-4mm,yshift=4mm]current page.south east);

\end{tikzpicture}
}

% Branded header for every page after the cover.
% The title and project line are separate nodes, giving reliable vertical spacing.
% The compact logo remains fully inside the page frame.
\newcommand{\swlreportheader}{
\begin{tikzpicture}[remember picture,overlay]
\node[anchor=north west,inner sep=0pt] at
([xshift=12mm,yshift=-7.5mm]current page.north west)
{\includegraphics[height=7mm,keepaspectratio]{@@TGS_LOGO@@}};

\node[anchor=north west,text=swlnavy] at
([xshift=24mm,yshift=-7.0mm]current page.north west)
{\fontsize{10.5}{12}\selectfont\bfseries NODE POSITION COMPARISON REPORT};

\node[anchor=north west,text=swlnavy] at
([xshift=24mm,yshift=-15.0mm]current page.north west)
{\fontsize{7.5}{9}\selectfont\normalfont Project: @@HEADER_PROJECT@@\quad\textbar\quad Receiver Line: @@LINE@@};

\node[anchor=north east,text=swlnavy] at
([xshift=-11mm,yshift=-9mm]current page.north east)
{\fontsize{7.5}{9}\selectfont Page \thepage\ of \pageref{LastPage}};

\draw[swlnavy,line width=0.55pt]
([xshift=12mm,yshift=-23mm]current page.north west) --
([xshift=-12mm,yshift=-23mm]current page.north east);
\end{tikzpicture}
}

\newcommand{\swlreportpage}{
\swlpageframe
\swlreportheader
}

\begin{document}
\pagestyle{empty}

\setlength{\floatsep}{0pt}
\setlength{\textfloatsep}{0pt}
\setlength{\intextsep}{0pt}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% FRONT PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\thispagestyle{empty}
\swlpageframe

\begin{center}
\vspace*{6mm}

\includegraphics[height=34mm,keepaspectratio]{@@TGS_LOGO@@}\\[7mm]

{\Huge\bfseries\textcolor{swlnavy}{NODE POSITIONS COMPARISON REPORT}}\\[5mm]

{\Large\itshape\textcolor{swlnavy}{Pre-plot vs Deployment vs First Break Processing Data}}
\end{center}

\vspace*{\fill}

\begin{center}
\renewcommand{\arraystretch}{1.35}
\begin{tabular}{>{\bfseries}p{48mm} c p{100mm}}
Project Name & : & @@PROJECT@@ \\
Location & : & @@LOCATION@@ \\
Area & : & @@AREA@@ \\
Client & : & @@CLIENT@@ \\
Contractor & : & @@CONTRACTOR@@ \\
Client Project ID & : & @@PROJECT_CLIENT_ID@@ \\
Contractor Project ID & : & @@PROJECT_CONTRACTOR_ID@@ \\
EPSG & : & @@EPSG@@ \\
Line & : & @@LINE@@ \\
Report Type & : & Node Positions Comparison \\
Generated By & : & SeisWebLog \\
Generated On & : & @@GENERATED@@ \\
\end{tabular}
\renewcommand{\arraystretch}{1.05}
\end{center}

\vspace*{\fill}

\begin{center}
\textcolor{swlnavy}{\rule{55mm}{0.5pt}}
\quad
{\bfseries\textcolor{swlnavy}{SeisWebLog - ROV Data Management \& Reporting System}}
\quad
\textcolor{swlnavy}{\rule{55mm}{0.5pt}}
\end{center}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% EXECUTIVE SUMMARY PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
\swlreportpage
\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth,height=151mm,keepaspectratio]{@@CHART_EXECUTIVE@@}
\end{figure}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% LINE INFORMATION PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
\swlreportpage
\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth,height=151mm,keepaspectratio]{@@CHART_LINE_INFO@@}
\end{figure}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% QC DASHBOARD PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
\swlreportpage
\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth,height=151mm,keepaspectratio]{@@CHART_QC_DASHBOARD@@}
\end{figure}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TABLE PAGES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
@@TABLE_PAGES@@

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% XY ANALYSIS PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
\swlreportpage

\vspace*{1mm}

\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth]{@@CHART_XY_PREPLOT@@}
\end{figure}

\newpage
\swlreportpage
\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth]{@@CHART_XY_CROSS@@}
\end{figure}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% STATION DX / DY PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
\swlreportpage

\begin{center}
{\Large\bfseries\textcolor{swlnavy}{STATION VS $\Delta X$ / $\Delta Y$ / DEPLOYMENT Z}}
\end{center}

\vspace{1mm}

\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth,height=147mm,keepaspectratio]{@@CHART_OFFSETS@@}
\end{figure}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% INLINE / CROSSLINE / RADIAL PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
\swlreportpage

\begin{center}
{\Large\bfseries\textcolor{swlnavy}{INLINE / CROSSLINE / RADIAL OFFSETS VS STATION}}
\end{center}

\vspace{1mm}

\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth,height=147mm,keepaspectratio]{@@CHART_IL_XL@@}
\end{figure}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% CDF AND BOXPLOTS PAGES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
@@CDF_PAGES@@

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% HEATMAP AND DEPTH PROFILE PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
\swlreportpage
\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth,height=151mm,keepaspectratio]{@@CHART_HEATMAP_PROFILE@@}
\end{figure}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% DIRECTIONAL ANALYSIS PAGES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
@@DIRECTIONAL_PAGES@@

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% POLAR PAGES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
@@POLAR_PAGES@@

\end{document}
"""

        replacements = {
            "@@PROJECT@@": self._tex(project_name),
            "@@HEADER_PROJECT@@": self._tex(project_name),
            "@@TGS_LOGO@@": tgs_logo_file,
            "@@LOCATION@@": self._tex(location),
            "@@AREA@@": self._tex(area),
            "@@CLIENT@@": self._tex(client),
            "@@CONTRACTOR@@": self._tex(contractor),
            "@@PROJECT_CLIENT_ID@@": self._tex(project_client_id),
            "@@PROJECT_CONTRACTOR_ID@@": self._tex(project_contractor_id),
            "@@EPSG@@": self._tex(epsg),
            "@@LINE@@": str(line),
            "@@GENERATED@@": generated_on,
            "@@CHART_EXECUTIVE@@": chart_executive_file,
            "@@CHART_LINE_INFO@@": chart_line_info_file,
            "@@CHART_QC_DASHBOARD@@": chart_qc_dashboard_file,
            "@@TABLE_PAGES@@": table_pages,
            "@@CHART_XY_PREPLOT@@": chart_xy_preplot_file,
            "@@CHART_XY_CROSS@@": chart_xy_cross_file,
            "@@CHART_OFFSETS@@": chart_offsets_file,
            "@@CHART_IL_XL@@": chart_il_xl_file,
            "@@POLAR_PAGES@@": "\n".join(
                r"""\newpage
\swlreportpage
\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth,height=151mm,keepaspectratio]{%s}
\end{figure}""" % chart_file
                for chart_file in (polar_group_2_file, polar_group_3_file, polar_stats_file)
            ),
            "@@CDF_PAGES@@": "\n".join(
                r"""\newpage
\swlreportpage
\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth,height=151mm,keepaspectratio]{%s}
\end{figure}""" % cdf_files[key]
                for key in ("dep_pp", "rcv_pp", "rec_pp", "fb_dep", "fb_rcv")
            ),
            "@@CHART_HEATMAP_PROFILE@@": chart_heatmap_profile_file,
            "@@DIRECTIONAL_PAGES@@": "\n".join(
                r"""\newpage
\swlreportpage
\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth,height=151mm,keepaspectratio]{%s}
\end{figure}""" % directional_files[key]
                for key in ("dep_pp", "rcv_pp", "rec_pp", "fb_dep", "fb_rcv")
            ),
        }

        for old, new in replacements.items():
            template = template.replace(old, str(new))

        return template

    def _build_line_info_page(self, line):
        info = self.load_line_summary(line)

        if not info:
            return r"""
\swlreportpage
\begin{center}
{\Large\bfseries\textcolor{swlnavy}{LINE INFORMATION}}

\vspace{12mm}
{\large No DSR\_LineSummary record found.}
\end{center}
"""

        def v(key, ndigits=1):
            value = info.get(key)

            if value is None:
                return ""

            try:
                if isinstance(value, float):
                    return f"{value:.{ndigits}f}"
            except Exception:
                pass

            return self._tex(value)

        template = r"""
\swlreportpage

\begin{center}
{\Large\bfseries\textcolor{swlnavy}{LINE INFORMATION SUMMARY}}
\end{center}

\vspace{3mm}

\rowcolors{2}{white}{infobg}

\begin{tabular}{>{\bfseries}p{42mm} c p{34mm} >{\bfseries}p{42mm} c p{34mm} >{\bfseries}p{42mm} c p{34mm}}
Line & : & @@LINE@@ &
Preplot Stations & : & @@STATIONS@@ &
 & & \\

Nodes & : & @@NODES@@ &
Min Station & : & @@MINST@@ &
Max Station & : & @@MAXST@@ \\

Deployed Nodes & : & @@DEPLOYED@@ &
Retrieved Nodes & : & @@RETRIEVED@@ &
Processed Nodes & : & @@PROCESSED@@ \\

Deployed \% & : & @@DEPLOYEDPCT@@ &
Retrieved \% & : & @@RETRIEVEDPCT@@ &
Processed \% & : & @@PROCESSEDPCT@@ \\

Start of Deploy Time & : & @@FIRSTDEP@@ &
End of Deploy Time & : & @@LASTDEP@@ &
Deployment Hours & : & @@DEPHOURS@@ \\

Start Of Recovery & : & @@STARTREC@@ &
End Of Recovery & : & @@ENDREC@@ &
Recovery Duration & : & @@RECDUR@@ \\

Avg $\Delta$ Easting & : & @@AVGDE@@ &
Min $\Delta$ Easting & : & @@MINDE@@ &
Max $\Delta$ Easting & : & @@MAXDE@@ \\

Avg $\Delta$ Northing & : & @@AVGDN@@ &
Min $\Delta$ Northing & : & @@MINDN@@ &
Max $\Delta$ Northing & : & @@MAXDN@@ \\

Primary E 95\% & : & @@E95@@ &
Primary N95\% & : & @@N95@@ &
Avg Radial Offset & : & @@AVGRO@@ \\

Min Radial Offset & : & @@MINRO@@ &
Max Radial Offset & : & @@MAXRO@@ &
 & & \\

\end{tabular}

\rowcolors{2}{}{}

\vspace{8mm}

\begin{center}
\begin{tabular}{|p{70mm}|p{70mm}|p{70mm}|}
\hline
\cellcolor{depbg}\textbf{Deployment Status} &
\cellcolor{fbbg}\textbf{Processing Status} &
\cellcolor{cmporange}\textbf{QC Summary} \\
\hline
Deployed: @@DEPLOYED@@ / @@PLANNED@@ (@@DEPLOYEDPCT@@\%) &
Processed: @@PROCESSED@@ / @@STATIONS@@ (@@PROCESSEDPCT@@\%) &
Max Radial Offset: @@MAXRO@@ m \\
\hline
Retrieved: @@RETRIEVED@@ / @@PLANNED@@ (@@RETRIEVEDPCT@@\%) &
First Break records loaded: @@PROCESSED@@ &
Primary E95 / N95: @@E95@@ / @@N95@@ m \\
\hline
\end{tabular}
\end{center}
"""

        replacements = {
            "@@LINE@@": v("Line", 0),
            "@@PLANNED@@": v("PlannedPoints", 0),
            "@@STATIONS@@": v("Stations", 0),
            "@@NODES@@": v("Nodes", 0),
            "@@MINST@@": v("MinStation", 0),
            "@@MAXST@@": v("MaxStation", 0),
            "@@DEPLOYED@@": v("DeployedCount", 0),
            "@@RETRIEVED@@": v("RetrievedCount", 0),
            "@@PROCESSED@@": v("ProcessedCount", 0),
            "@@DEPLOYEDPCT@@": v("DeployedPct", 1),
            "@@RETRIEVEDPCT@@": v("RetrievedPct", 1),
            "@@PROCESSEDPCT@@": v("ProcessedPct", 1),
            "@@FIRSTDEP@@": v("FirstDeployTime"),
            "@@LASTDEP@@": v("LastDeployTime"),
            "@@DEPHOURS@@": v("DeploymentHours", 2),
            "@@STARTREC@@": v("StartOfRec"),
            "@@ENDREC@@": v("EndOfRec"),
            "@@RECDUR@@": v("RecDuration", 2),
            "@@AVGDE@@": v("AvgDeltaE", 2),
            "@@MINDE@@": v("MinDeltaE", 2),
            "@@MAXDE@@": v("MaxDeltaE", 2),
            "@@AVGDN@@": v("AvgDeltaN", 2),
            "@@MINDN@@": v("MinDeltaN", 2),
            "@@MAXDN@@": v("MaxDeltaN", 2),
            "@@E95@@": v("Primary_e95", 2),
            "@@N95@@": v("Primary_n95", 2),
            "@@AVGRO@@": v("AvgRadOffset", 2),
            "@@MINRO@@": v("MinRadOffset", 2),
            "@@MAXRO@@": v("MaxRadOffset", 2),
        }

        for old, new in replacements.items():
            template = template.replace(old, str(new))

        return template

    def _build_table_pages(self, df, rows_per_page=50):
        """
        Build comparison table pages.

        - 50 rows per full page.
        - Continuous numbering across pages.
        - No fixed-height stretching.
        - Title and table stay together on the same PDF page.
        """
        pages = []
        total_rows = len(df)
        total_pages = max(1, math.ceil(total_rows / rows_per_page))

        for page_idx, start in enumerate(range(0, total_rows, rows_per_page), start=1):
            part = df.iloc[start:start + rows_per_page]
            rows_tex = self._table_rows(part, start_index=start + 1)

            page_tex = rf"""
\swlreportpage
\vspace*{{1mm}}
\begin{{center}}
{{\large\bfseries\textcolor{{swlnavy}}{{NODE POSITIONS COMPARISON TABLE}}}}\\[-0.8mm]
{{\scriptsize Line table page {page_idx} of {total_pages}}}
\end{{center}}
\vspace*{{-3mm}}

\fontsize{{5.20}}{{5.95}}\selectfont
\renewcommand{{\arraystretch}}{{1.08}}
\noindent\resizebox{{\textwidth}}{{!}}{{%
\begin{{tabular}}{{|c|c|c|c|c|cc|ccc|ccc|rrrr|rrrr|rrrr|}}
\hline
{self._latex_table_header()}
{rows_tex}
\hline
\end{{tabular}}
}}
"""
            pages.append(page_tex)
            if page_idx < total_pages:
                pages.append(r"\newpage")

        return "\n".join(pages)

    def _latex_table_header(self):
        return r"""
\multirow{2}{*}{\textbf{\#}} &
\multirow{2}{*}{\textbf{Line}} &
\multirow{2}{*}{\textbf{Station}} &
\multirow{2}{*}{\textbf{ROV}} &
\multirow{2}{*}{\textbf{Node ID}} &
\multicolumn{2}{c|}{\cellcolor{ppbg}\textbf{\textcolor{orange!80!black}{Preplot Coordinates}}} &
\multicolumn{3}{c|}{\cellcolor{depbg}\textbf{\textcolor{green!50!black}{Deployment(INS/USBL) Coordinates}}} &
\multicolumn{3}{c|}{\cellcolor{fbbg}\textbf{First Break Coordinates}} &
\multicolumn{4}{c|}{\cellcolor{cmpgreen}\textbf{\textcolor{green!50!black}{Deployment vs Pre-plot}}} &
\multicolumn{4}{c|}{\cellcolor{cmporange}\textbf{\textcolor{orange!80!black}{First Break vs Deployment}}} &
\multicolumn{4}{c|}{\cellcolor{cmpred}\textbf{\textcolor{red}{First Break vs Pre-plot}}} \\
\cline{6-25}
 & & & & &
\cellcolor{ppbg}\textbf{\textcolor{orange!80!black}{X}} &
\cellcolor{ppbg}\textbf{\textcolor{orange!80!black}{Y}} &
\cellcolor{depbg}\textbf{\textcolor{green!50!black}{X}} &
\cellcolor{depbg}\textbf{\textcolor{green!50!black}{Y}} &
\cellcolor{depbg}\textbf{\textcolor{green!50!black}{Z}} &
\cellcolor{fbbg}\textbf{X} &
\cellcolor{fbbg}\textbf{Y} &
\cellcolor{fbbg}\textbf{Z} &
\cellcolor{cmpgreen}\textbf{\textcolor{green!50!black}{$\Delta X$}} &
\cellcolor{cmpgreen}\textbf{\textcolor{green!50!black}{$\Delta Y$}} &
\cellcolor{cmpgreen}\textbf{\textcolor{green!50!black}{$\Delta R$}} &
\cellcolor{cmpgreen}\textbf{\textcolor{green!50!black}{$\Delta Z$}} &
\cellcolor{cmporange}\textbf{\textcolor{orange!80!black}{$\Delta X$}} &
\cellcolor{cmporange}\textbf{\textcolor{orange!80!black}{$\Delta Y$}} &
\cellcolor{cmporange}\textbf{\textcolor{orange!80!black}{$\Delta R$}} &
\cellcolor{cmporange}\textbf{\textcolor{orange!80!black}{$\Delta Z$}} &
\cellcolor{cmpred}\textbf{\textcolor{red}{$\Delta X$}} &
\cellcolor{cmpred}\textbf{\textcolor{red}{$\Delta Y$}} &
\cellcolor{cmpred}\textbf{\textcolor{red}{$\Delta R$}} &
\cellcolor{cmpred}\textbf{\textcolor{red}{$\Delta Z$}} \\
\hline
"""


    def _table_rows(self, df, start_index=1):
        rows = []
        for idx, (_, r) in enumerate(df.iterrows(), start=start_index):
            rov = r.get("ROV", "")
            vals = [
                str(idx),
                self._fmt_int(r["Line"]),
                self._fmt_int(r["Station"]),
                self._tex(rov),
                self._tex(r["Node"]),

                self._fmt(r["pp_x"]),
                self._fmt(r["pp_y"]),

                self._fmt(r["dep_x"]),
                self._fmt(r["dep_y"]),
                self._fmt(r["dep_z"]),

                self._fmt(r["fb_x"]),
                self._fmt(r["fb_y"]),
                self._fmt(r["fb_z"]),

                self._fmt(r["pp_dep_dx"]),
                self._fmt(r["pp_dep_dy"]),
                self._fmt(r["pp_dep_dr"]),
                self._fmt(r["pp_dep_dz"]),

                self._fmt(r["fb_dep_dx"]),
                self._fmt(r["fb_dep_dy"]),
                self._fmt(r["fb_dep_dr"]),
                self._fmt(r["fb_dep_dz"]),

                self._fmt(r["fb_pp_dx"]),
                self._fmt(r["fb_pp_dy"]),
                self._fmt(r["fb_pp_dr"]),
                self._fmt(r["fb_pp_dz"]),
            ]
            rows.append(" & ".join(vals) + r" \\")
        return "\n".join(rows)

    # ------------------------------------------------------------------
    # CHARTS
    # ------------------------------------------------------------------
    def _make_charts(self, df, charts_dir, line):
        qc = self.load_node_qc_settings()
        max_radial_offset = qc.get("max_radial_offset") or 30.0
        self._make_executive_summary_page(df, charts_dir, line, float(max_radial_offset))
        self._make_line_information_page(df, charts_dir, line)
        self._make_qc_dashboard_page(df, charts_dir, line, float(max_radial_offset))
        self._make_xy_offsets_analysis_pages(df=df, charts_dir=charts_dir, max_radial_offset=float(max_radial_offset))
        self._make_chart_page_offsets(df, charts_dir)
        self._make_chart_page_il_xl_radial(df, charts_dir)
        self._make_cdf_boxplots_page(df, charts_dir, float(max_radial_offset))
        self._make_heatmap_depth_profile_page(df, charts_dir, float(max_radial_offset))
        self._make_directional_analysis_page(df, charts_dir)
        self._make_polar_offsets_page(df, charts_dir)

    def _dashboard_status(self, value, warning_limit, fail_limit=None):
        """Return label and display color for a QC metric."""
        if value is None or not np.isfinite(value):
            return "N/A", "#7f8c8d"
        fail_limit = fail_limit if fail_limit is not None else warning_limit * 1.25
        if value <= warning_limit:
            return "PASS", "#2ca02c"
        if value <= fail_limit:
            return "WARNING", "#f2a900"
        return "FAIL", "#d62728"

    def _add_card(self, ax, x, y, w, h, title, value, subtitle="", edge="#d7e0ea", value_color="#0b2a55"):
        from matplotlib.patches import FancyBboxPatch
        card = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.008,rounding_size=0.012",
                               linewidth=0.9, edgecolor=edge, facecolor="white")
        ax.add_patch(card)
        ax.text(x + w/2, y + h*0.66, str(value), ha="center", va="center",
                fontsize=18, fontweight="bold", color=value_color)
        ax.text(x + w/2, y + h*0.36, title, ha="center", va="center",
                fontsize=8.5, fontweight="bold", color="#142b4a")
        if subtitle:
            ax.text(x + w/2, y + h*0.15, subtitle, ha="center", va="center",
                    fontsize=6.8, color="#4a5b70")

    def _make_executive_summary_page(self, df, charts_dir, line, max_radial_offset):
        info = self.load_line_summary(line)
        planned = int(info.get("PlannedPoints") or len(df))
        deployed = int(info.get("DeployedCount") or len(df))
        recovered = int(info.get("RetrievedCount") or 0)
        processed = int(info.get("ProcessedCount") or df["fb_x"].notna().sum())
        dep_pct = 100.0 * deployed / planned if planned else 0.0
        rec_pct = 100.0 * recovered / planned if planned else 0.0
        proc_pct = 100.0 * processed / planned if planned else 0.0

        radial = pd.to_numeric(df.get("dep_pp_dr"), errors="coerce").dropna()
        avg_rad = float(radial.mean()) if len(radial) else float("nan")
        max_rad = float(radial.max()) if len(radial) else float("nan")
        p95_rad = float(radial.quantile(0.95)) if len(radial) else float("nan")
        avg_il = float(pd.to_numeric(df.get("dep_il"), errors="coerce").mean())
        avg_xl = float(pd.to_numeric(df.get("dep_xl"), errors="coerce").mean())
        dp_rec = float(pd.to_numeric(df.get("dep_rec_dr"), errors="coerce").mean())
        mean_depth = float(pd.to_numeric(df.get("dep_z"), errors="coerce").abs().mean())

        overall, overall_color = self._dashboard_status(max_rad, max_radial_offset)
        project = self.load_project_main()

        fig = plt.figure(figsize=(16.5, 9.0))
        ax = fig.add_axes([0.03, 0.04, 0.94, 0.91]); ax.set_xlim(0,1); ax.set_ylim(0,1); ax.axis("off")
        ax.text(0.0, 0.965, "EXECUTIVE SUMMARY", fontsize=20, fontweight="bold", color="#0b2a55", va="top")
        ax.text(0.0, 0.925, f"Receiver Line {line} | {project.get('name','')}", fontsize=9, color="#53657a")

        cards=[("Planned Nodes",planned,""),("Deployed",deployed,f"{dep_pct:.1f}%"),("Recovered",recovered,f"{rec_pct:.1f}%"),("Processed",processed,f"{proc_pct:.1f}%")]

        # Center the four summary cards as one horizontal group.
        card_w = 0.185
        card_gap = 0.018
        cards_total_w = len(cards) * card_w + (len(cards) - 1) * card_gap
        cards_x0 = (1.0 - cards_total_w) / 2.0
        for i, (t, v, sub) in enumerate(cards):
            self._add_card(
                ax,
                cards_x0 + i * (card_w + card_gap),
                0.74,
                card_w,
                0.14,
                t,
                v,
                sub,
            )

        # KPI table
        from matplotlib.patches import FancyBboxPatch
        left=FancyBboxPatch((0.0,0.24),0.63,0.45,boxstyle="round,pad=0.008",linewidth=.8,edgecolor="#cbd6e2",facecolor="#f8fafc")
        ax.add_patch(left); ax.text(0.02,0.655,"KEY PERFORMANCE INDICATORS",fontsize=10,fontweight="bold",color="#0b2a55")
        rows=[("Average radial offset",avg_rad,"m"),("95th percentile radial",p95_rad,"m"),("Maximum radial offset",max_rad,"m"),("Average inline offset",avg_il,"m"),("Average crossline offset",avg_xl,"m"),("Mean deployment vs REC_DB radial",dp_rec,"m"),("Mean water depth",mean_depth,"m")]
        y=0.615
        for label,val,unit in rows:
            ax.plot([0.02,0.61],[y-0.018,y-0.018],color="#e1e7ee",lw=.6)
            ax.text(0.025,y,label,fontsize=8.2,color="#23384f",va="center")
            ax.text(0.60,y,(f"{val:.2f} {unit}" if np.isfinite(val) else "N/A"),fontsize=8.4,fontweight="bold",ha="right",color="#142b4a",va="center")
            y-=0.053

        status_box=FancyBboxPatch((0.66,0.43),0.32,0.26,boxstyle="round,pad=0.008",linewidth=.8,edgecolor="#cbd6e2",facecolor="white")
        ax.add_patch(status_box); ax.text(0.82,0.65,"QC OVERALL RESULT",ha="center",fontsize=10,fontweight="bold",color="#0b2a55")
        ax.text(0.82,0.555,overall,ha="center",fontsize=23,fontweight="bold",color=overall_color)
        ax.text(0.82,0.47,f"Maximum radial offset {max_rad:.2f} m\nQC threshold {max_radial_offset:.2f} m",ha="center",fontsize=8,color="#45566a")

        # QC status summary
        # Move the QC status table slightly lower to add breathing room below the KPI panels.
        table_ax=fig.add_axes([0.065,0.045,0.87,0.19]); table_ax.axis("off")
        metrics=[("Inline offset",abs(avg_il),max_radial_offset), ("Crossline offset",abs(avg_xl),max_radial_offset), ("Radial offset",max_rad,max_radial_offset), ("DP vs REC_DB radial",dp_rec,max_radial_offset)]
        data=[]
        for name,val,limit in metrics:
            st,_=self._dashboard_status(val,limit); data.append([name,f"{val:.2f} m",st])
        tbl=table_ax.table(cellText=data,colLabels=["Parameter","Result","Status"],cellLoc="left",colLoc="left",loc="center",bbox=[0,0,1,1])
        tbl.auto_set_font_size(False); tbl.set_fontsize(8)
        for (r,c),cell in tbl.get_celld().items():
            cell.set_linewidth(.5); cell.set_edgecolor("#cbd6e2")
            if r==0: cell.set_facecolor("#eaf2f8"); cell.set_text_props(weight="bold",color="#0b2a55")
            elif c==2:
                st=data[r-1][2]; cell.set_text_props(weight="bold",color={"PASS":"#2ca02c","WARNING":"#f2a900","FAIL":"#d62728"}.get(st,"#555"))
        fig.savefig(charts_dir / "node_position_executive_summary_page.png", dpi=180, bbox_inches="tight")
        plt.close(fig)

    def _make_line_information_page(self, df, charts_dir, line):
        info=self.load_line_summary(line); project=self.load_project_main()
        fig=plt.figure(figsize=(16.5,9.0)); ax=fig.add_axes([0.03,0.04,0.94,0.91]); ax.set_xlim(0,1); ax.set_ylim(0,1); ax.axis("off")
        ax.text(0,0.965,"LINE INFORMATION SUMMARY",fontsize=20,fontweight="bold",color="#0b2a55",va="top")
        ax.text(0,0.925,f"Project: {project.get('name','')} | Area: {project.get('area','')}",fontsize=9,color="#53657a")
        left_items=[("Line",line),("Preplot Stations",info.get("Stations",len(df))),("Nodes",info.get("Nodes",len(df))),("Min Station",info.get("MinStation",df['Station'].min())),("Max Station",info.get("MaxStation",df['Station'].max())),("Deployed Nodes",info.get("DeployedCount",len(df))),("Recovered Nodes",info.get("RetrievedCount",0)),("Processed Nodes",info.get("ProcessedCount",df['fb_x'].notna().sum())),("Start of Deployment",info.get("FirstDeployTime","")),("End of Deployment",info.get("LastDeployTime","")),("Deployment Duration",info.get("DeploymentHours","")),("Start of Recovery",info.get("StartOfRec","")),("End of Recovery",info.get("EndOfRec","")),("Recovery Duration",info.get("RecDuration",""))]
        y=.86
        for i,(k,v) in enumerate(left_items):
            if i%2: ax.add_patch(plt.Rectangle((0, y-.018), .52, .04,facecolor="#f4f7fa",edgecolor="none"))
            ax.text(.015,y,k,fontsize=8.5,fontweight="bold",color="#263b53",va="center"); ax.text(.31,y,str(v if v is not None else ""),fontsize=8.5,color="#152b45",va="center"); y-=.047
        # right statistics tables
        stats=[("Delta Easting",df['dep_pp_dx']), ("Delta Northing",df['dep_pp_dy']), ("Radial Offset",df['dep_pp_dr'])]
        ax.text(.60,.86,"OFFSET STATISTICS",fontsize=11,fontweight="bold",color="#0b2a55")
        y=.81
        for name,series in stats:
            ser=pd.to_numeric(series,errors='coerce').dropna(); vals=[ser.mean(),ser.min(),ser.max()]
            ax.text(.60,y,name,fontsize=8.2,fontweight="bold",color="#263b53");
            ax.text(.78,y,f"Avg {vals[0]:.2f}",fontsize=8); ax.text(.87,y,f"Min {vals[1]:.2f}",fontsize=8); ax.text(.95,y,f"Max {vals[2]:.2f}",fontsize=8,ha='right'); y-=.065
        ax.text(.60,.57,"RADIAL OFFSET SUMMARY",fontsize=11,fontweight="bold",color="#0b2a55")
        rad=pd.to_numeric(df['dep_pp_dr'],errors='coerce').dropna(); summ=[("P50",rad.quantile(.5)),("P95",rad.quantile(.95)),("Average",rad.mean()),("Minimum",rad.min()),("Maximum",rad.max())]
        y=.52
        for k,v in summ: ax.text(.62,y,k,fontsize=8.5,fontweight="bold"); ax.text(.93,y,f"{v:.2f} m",fontsize=8.5,ha='right'); y-=.052
        # donuts
        vals=[("DEPLOYMENT",int(info.get('DeployedCount') or len(df)),int(info.get('PlannedPoints') or len(df))), ("RECOVERY",int(info.get('RetrievedCount') or 0),int(info.get('PlannedPoints') or len(df))), ("PROCESSED",int(info.get('ProcessedCount') or df['fb_x'].notna().sum()),int(info.get('PlannedPoints') or len(df)))]
        # Center all three donut charts horizontally and move the group lower.
        donut_w = 0.15
        donut_gap = 0.035
        donuts_total_w = len(vals) * donut_w + (len(vals) - 1) * donut_gap
        donuts_x0 = (1.0 - donuts_total_w) / 2.0
        for i,(name,count,total) in enumerate(vals):
            a=fig.add_axes([donuts_x0+i*(donut_w+donut_gap),.035,donut_w,.22]); a.axis('equal'); pct=count/total if total else 0; a.pie([pct,max(0,1-pct)],startangle=90,counterclock=False,colors=['#2ca02c','#e5e9ee'],wedgeprops=dict(width=.22,edgecolor='white')); a.text(0,0,f"{pct*100:.1f}%",ha='center',va='center',fontsize=11,fontweight='bold'); a.set_title(f"{name}\n{count} / {total}",fontsize=8,fontweight='bold'); a.axis('off')
        fig.savefig(charts_dir / "node_position_line_information_page.png",dpi=180,bbox_inches='tight'); plt.close(fig)

    def _make_qc_dashboard_page(self, df, charts_dir, line, max_radial_offset):
        radial=pd.to_numeric(df['dep_pp_dr'],errors='coerce').dropna(); il=pd.to_numeric(df['dep_il'],errors='coerce').dropna(); xl=pd.to_numeric(df['dep_xl'],errors='coerce').dropna(); dp=pd.to_numeric(df['dep_rec_dr'],errors='coerce').dropna()
        metrics=[("Average Radial",radial.mean(),max_radial_offset),("Maximum Radial",radial.max(),max_radial_offset),("P95 Radial",radial.quantile(.95),max_radial_offset),("Average Inline",abs(il.mean()),max_radial_offset),("Average Crossline",abs(xl.mean()),max_radial_offset),("DP vs REC_DB",dp.mean(),max_radial_offset)]
        fig=plt.figure(figsize=(16.5,9.0)); ax=fig.add_axes([.03,.04,.94,.91]); ax.set_xlim(0,1); ax.set_ylim(0,1); ax.axis('off')
        ax.text(0,.965,"QC DASHBOARD",fontsize=20,fontweight='bold',color='#0b2a55',va='top'); ax.text(0,.925,f"Receiver Line {line} | QC radial limit {max_radial_offset:.2f} m",fontsize=9,color='#53657a')
        for i,(name,val,limit) in enumerate(metrics):
            row=i//3; col=i%3; x=.02+col*.325; y=.60-row*.30; st,color=self._dashboard_status(float(val),limit)
            from matplotlib.patches import FancyBboxPatch, Wedge
            box=FancyBboxPatch((x,y),.29,.24,boxstyle='round,pad=.008',linewidth=.8,edgecolor='#cad5e0',facecolor='white'); ax.add_patch(box)
            ax.text(x+.145,y+.205,name,ha='center',fontsize=9,fontweight='bold',color='#0b2a55')
            # gauge arc
            center=(x+.145,y+.105); ax.add_patch(Wedge(center,.075,0,180,width=.014,facecolor='#e5e9ee',edgecolor='none'))
            frac=min(max(float(val)/max(limit,1e-9),0),1.5); angle=min(frac,1.0)*180; ax.add_patch(Wedge(center,.075,180-angle,180,width=.014,facecolor=color,edgecolor='none'))
            ax.text(center[0],center[1]-.005,f"{val:.2f} m",ha='center',va='center',fontsize=13,fontweight='bold',color='#142b4a')
            ax.text(center[0],y+.035,st,ha='center',fontsize=8.5,fontweight='bold',color=color)
        # distribution bar
        bins=[0,1,2,max_radial_offset,float('inf')]; labels=['0-1 m','1-2 m',f'2-{max_radial_offset:.1f} m',f'>{max_radial_offset:.1f} m']; counts=[]
        for lo,hi in zip(bins[:-1],bins[1:]): counts.append(int(((radial>=lo)&(radial<hi)).sum()))
        bax=fig.add_axes([.11,.08,.78,.19]); pos=np.arange(len(labels)); bax.barh(pos,counts); bax.set_yticks(pos,labels); bax.invert_yaxis(); bax.set_xlabel('Nodes'); bax.set_title('RADIAL OFFSET DISTRIBUTION',fontsize=10,fontweight='bold'); bax.grid(axis='x',alpha=.25)
        for p,c in zip(pos,counts): bax.text(c+.2,p,str(c),va='center',fontsize=8)
        fig.savefig(charts_dir / "node_position_qc_dashboard_page.png",dpi=180,bbox_inches='tight'); plt.close(fig)

    def _make_xy_offsets_analysis_pages(self, df, charts_dir, max_radial_offset):
        pages = [
            ("PREPLOT COORDINATE COMPARISONS", "node_position_xy_preplot_page.png", [
                ("Deployment vs Preplot", "dep_pp_dx", "dep_pp_dy", "ROV"),
                ("Recovery vs Preplot", "rcv_pp_dx", "rcv_pp_dy", "ROV1"),
                ("REC_DB vs Preplot", "rec_pp_dx", "rec_pp_dy", "ROV"),
            ]),
            ("INTER-COORDINATE COMPARISONS", "node_position_xy_cross_page.png", [
                ("Deployment vs Recovery", "dep_rcv_dx", "dep_rcv_dy", "ROV1"),
                ("REC_DB vs Deployment", "fb_dep_dx", "fb_dep_dy", "ROV"),
                ("REC_DB vs Recovery", "fb_rcv_dx", "fb_rcv_dy", "ROV1"),
            ]),
        ]
        for page_title, filename, specs in pages:
            self._make_xy_offsets_analysis_page(
                df, charts_dir, max_radial_offset, page_title, filename, specs
            )

    def _make_xy_offsets_analysis_page(self, df, charts_dir, max_radial_offset,
                                       page_title, filename, plot_specs):
        """
        Single-page XY cross-plot dashboard.

        Improvements:
        - no duplicated LaTeX/image page title
        - ROV legend no longer overlaps with title/subtitle
        - all labels use requested direction names
        - plots zoom to the data cloud; QC circle is allowed to be clipped/outside
        - maximum offset point is highlighted by a red star
        """
        fig = plt.figure(figsize=(18, 11))
        gs = fig.add_gridspec(
            3,
            3,
            height_ratios=[0.34, 1.0, 0.42],
            hspace=0.26,
            wspace=0.22,
        )

        title_ax = fig.add_subplot(gs[0, :])
        title_ax.axis("off")
        title_ax.text(
            0.5,
            0.90,
            page_title,
            ha="center",
            va="center",
            fontsize=24,
            fontweight="bold",
        )
        title_ax.text(
            0.5,
            0.68,
            "Cross plot of position differences in meters",
            ha="center",
            va="center",
            fontsize=12,
        )

        rov_values = []
        for _, _, _, color_col in plot_specs:
            if color_col in df.columns:
                rov_values.extend(str(x) for x in df[color_col].dropna().unique())
        rovs = sorted(set(rov_values))
        cmap = plt.get_cmap("tab10")
        rov_colors = {rov: cmap(i % 10) for i, rov in enumerate(rovs)}

        handles = []
        labels = []
        for rov, color in rov_colors.items():
            handles.append(
                plt.Line2D(
                    [0],
                    [0],
                    marker="o",
                    color="w",
                    markerfacecolor=color,
                    markeredgecolor="black",
                    markersize=7,
                )
            )
            labels.append(rov)

        if handles:
            title_ax.legend(
                handles,
                labels,
                title="ROV",
                loc="lower center",
                bbox_to_anchor=(0.5, 0.04),
                ncol=min(len(handles), 8),
                fontsize=9,
                title_fontsize=10,
                frameon=True,
            )

        for col_idx, (title, xcol, ycol, color_col) in enumerate(plot_specs):
            ax = fig.add_subplot(gs[1, col_idx])
            self._draw_xy_offset_analysis_plot(
                ax=ax,
                df=df,
                xcol=xcol,
                ycol=ycol,
                title=title,
                max_radial_offset=max_radial_offset,
                rov_colors=rov_colors,
                color_col=color_col,
            )

            stats_ax = fig.add_subplot(gs[2, col_idx])
            self._draw_xy_stats_box(
                ax=stats_ax,
                df=df,
                xcol=xcol,
                ycol=ycol,
                title=title,
            )

        fig.text(
            0.03,
            0.035,
            f"Red dashed circle: project_node_qc.max_radial_offset = {max_radial_offset:.2f} m",
            fontsize=10,
            color="red",
        )
        fig.text(
            0.03,
            0.015,
            "Blue dashed ellipse: 95% coherence ellipse; red star: maximum offset point",
            fontsize=10,
            color="blue",
        )

        fig.savefig(
            charts_dir / filename,
            dpi=180,
            bbox_inches="tight",
        )
        plt.close(fig)

    def _draw_xy_offset_analysis_plot(self, ax, df, xcol, ycol, title, max_radial_offset, rov_colors, color_col="ROV"):
        base_cols = [xcol, ycol, "Station"]
        if color_col in df.columns:
            base_cols.append(color_col)

        clean = df[base_cols].dropna(subset=[xcol, ycol]).copy()

        if clean.empty:
            ax.set_title(title, fontsize=12, fontweight="bold")
            ax.grid(True, linewidth=0.3, alpha=0.55)
            return

        if color_col in clean.columns:
            for rov, part in clean.groupby(color_col):
                ax.scatter(
                    part[xcol],
                    part[ycol],
                    s=22,
                    color=rov_colors.get(str(rov), "#333333"),
                    edgecolors="black",
                    linewidths=0.25,
                    alpha=0.88,
                    label=str(rov),
                    zorder=3,
                )
        else:
            ax.scatter(
                clean[xcol],
                clean[ycol],
                s=22,
                edgecolors="black",
                linewidths=0.25,
                alpha=0.88,
                zorder=3,
            )

        radial = (clean[xcol] ** 2 + clean[ycol] ** 2) ** 0.5
        max_idx = radial.idxmax()
        max_row = clean.loc[max_idx]
        max_rad = float(radial.loc[max_idx])

        # Red star = maximum offset point.
        ax.scatter(
            [max_row[xcol]],
            [max_row[ycol]],
            s=90,
            marker="*",
            color="red",
            edgecolors="black",
            linewidths=0.45,
            zorder=7,
        )
        ax.text(
            max_row[xcol],
            max_row[ycol],
            f" {int(max_row['Station'])}",
            fontsize=7,
            color="red",
            fontweight="bold",
            zorder=8,
        )

        # Label only outliers so the plot stays readable.
        label_limit = radial.quantile(0.93) if len(radial) else 0
        for _, r in clean.iterrows():
            rr = math.sqrt(float(r[xcol]) ** 2 + float(r[ycol]) ** 2)
            if rr >= label_limit and r.name != max_idx:
                ax.text(r[xcol], r[ycol], str(int(r["Station"])), fontsize=6, zorder=6)

        # QC circle: clipped to axes. The axes are zoomed to the data cloud, so the
        # circle can be partly or fully outside the visible area without destroying
        # page layout.
        circle = Circle(
            (0, 0),
            max_radial_offset,
            fill=False,
            linestyle="--",
            linewidth=1.0,
            color="red",
            clip_on=True,
            zorder=2,
        )
        ax.add_patch(circle)

        ellipse = self._make_95_confidence_ellipse(clean[xcol], clean[ycol])
        if ellipse:
            ax.add_patch(ellipse)

        ax.axhline(0, linewidth=0.85, color="blue", alpha=0.65, zorder=1)
        ax.axvline(0, linewidth=0.85, color="blue", alpha=0.65, zorder=1)

        # Zoom to points. Do NOT force the QC circle into the limits.
        x_abs = clean[xcol].abs()
        y_abs = clean[ycol].abs()
        x_lim = max(float(x_abs.quantile(0.985)) * 1.20, abs(float(max_row[xcol])) * 1.08, 3.0)
        y_lim = max(float(y_abs.quantile(0.985)) * 1.20, abs(float(max_row[ycol])) * 1.08, 3.0)
        lim = math.ceil(max(x_lim, y_lim))
        lim = max(lim, 4)

        ax.set_xlim(-lim, lim)
        ax.set_ylim(-lim, lim)
        ax.set_aspect("equal", adjustable="box")
        ax.grid(True, linewidth=0.3, alpha=0.55)

        ax.set_title(title, fontsize=12, fontweight="bold")
        ax.set_xlabel("ΔX (m)", fontsize=10)
        ax.set_ylabel("ΔY (m)", fontsize=10)
        ax.tick_params(labelsize=8)

        ax.text(
            0.97,
            0.96,
            f"QC R = {max_radial_offset:.2f} m",
            transform=ax.transAxes,
            ha="right",
            va="top",
            color="red",
            fontsize=8,
            fontweight="bold",
        )
        ax.text(
            0.97,
            0.89,
            f"Max = {max_rad:.2f} m",
            transform=ax.transAxes,
            ha="right",
            va="top",
            color="red",
            fontsize=8,
            fontweight="bold",
        )
        ax.text(
            0.97,
            0.82,
            "95% Ellipse",
            transform=ax.transAxes,
            ha="right",
            va="top",
            color="blue",
            fontsize=8,
            fontweight="bold",
        )

    def _make_95_confidence_ellipse(self, x, y):
        x = np.asarray(pd.Series(x).dropna(), dtype=float)
        y = np.asarray(pd.Series(y).dropna(), dtype=float)

        if len(x) < 3 or len(y) < 3:
            return None

        data = np.column_stack([x, y])
        cov = np.cov(data, rowvar=False)

        if not np.isfinite(cov).all():
            return None

        vals, vecs = np.linalg.eigh(cov)
        order = vals.argsort()[::-1]
        vals = vals[order]
        vecs = vecs[:, order]

        chi2_95 = 5.991
        width = 2.0 * math.sqrt(max(vals[0], 0) * chi2_95)
        height = 2.0 * math.sqrt(max(vals[1], 0) * chi2_95)
        angle = math.degrees(math.atan2(vecs[1, 0], vecs[0, 0]))

        return Ellipse(
            xy=(float(np.mean(x)), float(np.mean(y))),
            width=width,
            height=height,
            angle=angle,
            fill=False,
            linestyle="--",
            linewidth=1.0,
            color="blue",
        )

    def _draw_xy_stats_box(self, ax, df, xcol, ycol, title):
        ax.axis("off")

        x = df[xcol].dropna()
        y = df[ycol].dropna()
        ellipse_info = self._ellipse_95_stats(x, y)

        rows = [
            ["Min ΔX (m)", self._safe_stat(x, "min")],
            ["Max ΔX (m)", self._safe_stat(x, "max")],
            ["Avg ΔX (m)", self._safe_stat(x, "mean")],
            ["Min ΔY (m)", self._safe_stat(y, "min")],
            ["Max ΔY (m)", self._safe_stat(y, "max")],
            ["Avg ΔY (m)", self._safe_stat(y, "mean")],
            ["95% Ellipse Major (m)", ellipse_info["major"]],
            ["95% Ellipse Minor (m)", ellipse_info["minor"]],
            ["95% Ellipse Azimuth (°)", ellipse_info["azimuth"]],
        ]

        table = ax.table(
            cellText=rows,
            colLabels=[title, "Value"],
            loc="center",
            cellLoc="left",
            colLoc="center",
            bbox=[0.02, 0.02, 0.96, 0.96],
        )

        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.scale(1, 1.15)

        for (row, col), cell in table.get_celld().items():
            cell.set_linewidth(0.4)
            if row == 0:
                cell.set_text_props(weight="bold", ha="center")
                cell.set_facecolor("#eaf2f8")
            if row in [3, 6]:
                cell.set_text_props(color="blue", weight="bold")

    def _ellipse_95_stats(self, x, y):
        x = np.asarray(pd.Series(x).dropna(), dtype=float)
        y = np.asarray(pd.Series(y).dropna(), dtype=float)

        if len(x) < 3 or len(y) < 3:
            return {"major": "", "minor": "", "azimuth": ""}

        data = np.column_stack([x, y])
        cov = np.cov(data, rowvar=False)

        if not np.isfinite(cov).all():
            return {"major": "", "minor": "", "azimuth": ""}

        vals, vecs = np.linalg.eigh(cov)
        order = vals.argsort()[::-1]
        vals = vals[order]
        vecs = vecs[:, order]

        chi2_95 = 5.991
        major = 2.0 * math.sqrt(max(vals[0], 0) * chi2_95)
        minor = 2.0 * math.sqrt(max(vals[1], 0) * chi2_95)
        azimuth = math.degrees(math.atan2(vecs[1, 0], vecs[0, 0]))

        return {
            "major": f"{major:.2f}",
            "minor": f"{minor:.2f}",
            "azimuth": f"{azimuth:.1f}",
        }

    def _safe_stat(self, s, stat):
        if s is None or len(s) == 0:
            return ""

        if stat == "min":
            return f"{float(s.min()):.2f}"
        if stat == "max":
            return f"{float(s.max()):.2f}"
        if stat == "mean":
            return f"{float(s.mean()):.2f}"
        return ""

    def _make_chart_page_offsets(self, df, charts_dir):
        """
        Station trend page.

        Page contains:
        - Station vs ΔX
        - Station vs ΔY
        - Station vs Deployment Z from DSR.PrimaryElevation

        X axis uses sequential node position, but labels show real Station numbers.
        """
        fig = plt.figure(figsize=(18.0, 10.8))
        gs = fig.add_gridspec(3, 1)

        ax_dx = fig.add_subplot(gs[0, 0])
        ax_dy = fig.add_subplot(gs[1, 0])
        ax_z = fig.add_subplot(gs[2, 0])

        self._draw_delta_station(
            ax_dx,
            df,
            ["dep_pp_dx", "rec_pp_dx", "dep_rec_dx"],
            ["Deployment vs Preplot", "REC_DB vs Preplot", "Deployment vs REC_DB"],
            "STATION vs ΔX",
            "ΔX (m)",
        )

        self._draw_delta_station(
            ax_dy,
            df,
            ["dep_pp_dy", "rec_pp_dy", "dep_rec_dy"],
            ["Deployment vs Preplot", "REC_DB vs Preplot", "Deployment vs REC_DB"],
            "STATION vs ΔY",
            "ΔY (m)",
        )

        self._draw_delta_station(
            ax_z,
            df,
            ["dep_z"],
            ["Deployment Z (PrimaryElevation)"],
            "STATION vs DEPLOYMENT Z",
            "Z (m)",
        )

        plot_df = df.copy().reset_index(drop=True)
        if not plot_df.empty and "dep_z" in plot_df.columns:
            x = np.arange(1, len(plot_df) + 1, dtype=float)
            z = plot_df["dep_z"]

            if z.notna().any():
                max_idx = int(z.idxmax())
                min_idx = int(z.idxmin())

                ax_z.scatter(
                    [x[max_idx]],
                    [plot_df.loc[max_idx, "dep_z"]],
                    s=60,
                    marker="^",
                    color="red",
                    zorder=6,
                    label="Max Z",
                )
                ax_z.scatter(
                    [x[min_idx]],
                    [plot_df.loc[min_idx, "dep_z"]],
                    s=60,
                    marker="v",
                    color="blue",
                    zorder=6,
                    label="Min Z",
                )
                ax_z.text(
                    x[max_idx],
                    plot_df.loc[max_idx, "dep_z"],
                    f" {self._fmt_int(plot_df.loc[max_idx, 'Station'])}",
                    fontsize=6,
                    color="red",
                    fontweight="bold",
                )
                ax_z.text(
                    x[min_idx],
                    plot_df.loc[min_idx, "dep_z"],
                    f" {self._fmt_int(plot_df.loc[min_idx, 'Station'])}",
                    fontsize=6,
                    color="blue",
                    fontweight="bold",
                )
                ax_z.legend(fontsize=6, loc="upper right")

        fig.subplots_adjust(
            left=0.055,
            right=0.985,
            top=0.94,
            bottom=0.12,
            hspace=0.58,
        )

        fig.savefig(charts_dir / "node_position_offsets_page.png", dpi=180)
        plt.close(fig)

    def _make_chart_page_il_xl_radial(self, df, charts_dir):
        fig = plt.figure(figsize=(18.0, 10.6))
        gs = fig.add_gridspec(3, 1)
        ax_il = fig.add_subplot(gs[0, 0])
        ax_xl = fig.add_subplot(gs[1, 0])
        ax_radial = fig.add_subplot(gs[2, 0])
        self._draw_delta_station(ax_il, df, ["dep_il", "fb_pp_il"], ["Deployment vs Preplot", "REC_DB vs Preplot"], "STATION vs IN-LINE OFFSET", "In-Line Offset (m)")
        self._draw_delta_station(ax_xl, df, ["dep_xl", "fb_pp_xl"], ["Deployment vs Preplot", "REC_DB vs Preplot"], "STATION vs X-LINE OFFSET", "X-Line Offset (m)")
        self._draw_delta_station(ax_radial, df, ["dep_pp_dr", "rec_pp_dr"], ["Deployment vs Preplot", "REC_DB vs Preplot"], "STATION vs RADIAL OFFSET", "Radial Offset (m)")
        fig.subplots_adjust(left=0.055, right=0.985, top=0.93, bottom=0.13, hspace=0.58)
        fig.savefig(charts_dir / "node_position_il_xl_radial_page.png", dpi=180)
        plt.close(fig)

    def _make_cdf_boxplots_page(self, df, charts_dir, max_radial_offset):
        """Create one CDF/boxplot page for each requested coordinate comparison."""
        specs = [
            ("dep_pp", "Deployment vs Preplot", "dep_z", None),
            ("rcv_pp", "Recovery vs Preplot", "rcv_z", None),
            ("rec_pp", "REC_DB vs Preplot", "fb_z", None),
            ("fb_dep", "REC_DB vs Deployment", "fb_z", "dep_z"),
            ("fb_rcv", "REC_DB vs Recovery", "fb_z", "rcv_z"),
        ]
        for prefix, title, z_a, z_b in specs:
            self._make_cdf_boxplots_single(
                df, charts_dir, max_radial_offset, prefix, title, z_a, z_b
            )

    def _make_cdf_boxplots_single(self, df, charts_dir, max_radial_offset,
                                  prefix, comparison_title, z_a, z_b):
        plot_df = df.copy().reset_index(drop=True)
        dx_signed = pd.to_numeric(plot_df[f"{prefix}_dx"], errors="coerce").dropna().to_numpy(float)
        dy_signed = pd.to_numeric(plot_df[f"{prefix}_dy"], errors="coerce").dropna().to_numpy(float)
        radial = pd.to_numeric(plot_df[f"{prefix}_dr"], errors="coerce").dropna().to_numpy(float)
        inline_signed = pd.to_numeric(plot_df[f"{prefix}_il"], errors="coerce").dropna().to_numpy(float)
        crossline_signed = pd.to_numeric(plot_df[f"{prefix}_xl"], errors="coerce").dropna().to_numpy(float)
        delta_x = np.abs(dx_signed)
        delta_y = np.abs(dy_signed)
        inline = np.abs(inline_signed)
        crossline = np.abs(crossline_signed)
        if z_b:
            z_delta = (
                np.abs(pd.to_numeric(plot_df[z_a], errors="coerce"))
                - np.abs(pd.to_numeric(plot_df[z_b], errors="coerce"))
            ).dropna().to_numpy(float)
        else:
            z_delta = np.array([], dtype=float)

        fig = plt.figure(figsize=(18, 10.3))
        gs = fig.add_gridspec(2, 2, width_ratios=[1.65, 1.0], height_ratios=[1.05, 1.0], hspace=0.36, wspace=0.22)
        fig.suptitle(f"CDF & BOXPLOTS - {comparison_title}", fontsize=20, fontweight="bold", y=0.98)

        ax_cdf = fig.add_subplot(gs[0, 0])
        cdf_series = (
            (radial, "Radial offset", "#1f77b4", "-"),
            (delta_x, "|Delta X|", "#ff7f0e", "-"),
            (delta_y, "|Delta Y|", "#2ca02c", "-"),
            (inline, "|In-line offset|", "#9467bd", "--"),
            (crossline, "|X-line offset|", "#d62728", "-."),
        )
        for values, label, color, linestyle in cdf_series:
            values = values[np.isfinite(values)]
            if values.size:
                x = np.sort(values)
                y = np.arange(1, len(x) + 1) / len(x) * 100.0
                ax_cdf.plot(x, y, linewidth=1.8, color=color, linestyle=linestyle, label=label)
        ax_cdf.axvline(max_radial_offset, linestyle="--", linewidth=1.0, label=f"QC limit ({max_radial_offset:.1f} m)")
        ax_cdf.set_title("CUMULATIVE DISTRIBUTION FUNCTION (CDF)", fontsize=11, fontweight="bold")
        ax_cdf.set_xlabel("Absolute offset (m)")
        ax_cdf.set_ylabel("Cumulative percentage (%)")
        ax_cdf.set_ylim(0, 101)
        ax_cdf.grid(True, linewidth=0.35, alpha=0.55)
        ax_cdf.legend(fontsize=7.5, loc="lower right", ncol=2)

        ax_pct = fig.add_subplot(gs[0, 1])
        ax_pct.axis("off")
        percentiles = [50, 75, 90, 95, 99]
        rows = []
        for q in percentiles:
            def qv(a):
                return f"{np.nanpercentile(a, q):.2f}" if len(a) else ""
            rows.append([f"{q}%" + (" (Median)" if q == 50 else ""), qv(radial), qv(delta_x), qv(delta_y), qv(inline), qv(crossline)])
        tbl = ax_pct.table(cellText=rows, colLabels=["Percentile", "Radial", "|dX|", "|dY|", "|IL|", "|XL|"], cellLoc="center", colLoc="center", bbox=[0.00, 0.26, 1.00, 0.62])
        tbl.auto_set_font_size(False); tbl.set_fontsize(8.2); tbl.scale(1, 1.35)
        for (r, c), cell in tbl.get_celld().items():
            cell.set_linewidth(0.5)
            if r == 0:
                cell.set_facecolor("#eaf2f8"); cell.set_text_props(weight="bold")
        ax_pct.set_title("PERCENTILE SUMMARY (m)", fontsize=11, fontweight="bold", pad=10)
        ax_pct.text(0.05, 0.15, "Green: good (< 1 m)\nYellow: acceptable (1–2 m)\nRed: warning (> 2 m)", transform=ax_pct.transAxes, fontsize=9, va="top")

        ax_box = fig.add_subplot(gs[1, :])
        datasets = [dx_signed, dy_signed, inline_signed, crossline_signed, radial]
        labels = ["Delta X (m)", "Delta Y (m)", "In-line (m)", "X-line (m)", "Radial (m)"]
        if z_delta.size:
            datasets.append(z_delta)
            labels.append("Depth dZ (m)")
        box = ax_box.boxplot(datasets, labels=labels, patch_artist=True, showmeans=True,
                             meanprops=dict(marker="D", markerfacecolor="white", markeredgecolor="black", markersize=4))
        for patch in box["boxes"]:
            patch.set_alpha(0.65)
        ax_box.axhline(0, linewidth=0.7, alpha=0.7)
        ax_box.grid(True, axis="y", linewidth=0.35, alpha=0.5)
        ax_box.set_title("BOXPLOTS", fontsize=11, fontweight="bold")
        ax_box.text(0.99, 0.96, "Box: 25–75%\nLine: median\nWhiskers: 1.5×IQR\nDots: outliers", transform=ax_box.transAxes, ha="right", va="top", fontsize=8)

        fig.subplots_adjust(left=0.055, right=0.97, top=0.91, bottom=0.08)
        fig.savefig(charts_dir / f"node_position_cdf_boxplots_{prefix}_page.png", dpi=180, bbox_inches="tight")
        plt.close(fig)

    def _make_heatmap_depth_profile_page(self, df, charts_dir, max_radial_offset):
        """Create three shared-scale heatmaps and a combined depth/offset profile."""
        plot_df = df.copy().sort_values("Station").reset_index(drop=True)
        station = plot_df["Station"].to_numpy(float)
        dep_radial = pd.to_numeric(plot_df["dep_pp_dr"], errors="coerce").to_numpy(float)
        rcv_radial = pd.to_numeric(plot_df["rcv_pp_dr"], errors="coerce").to_numpy(float)
        recdb_radial = pd.to_numeric(plot_df["rec_pp_dr"], errors="coerce").to_numpy(float)
        dep_depth = np.abs(pd.to_numeric(plot_df["dep_z"], errors="coerce").to_numpy(float))
        rcv_depth = np.abs(pd.to_numeric(plot_df["rcv_z"], errors="coerce").to_numpy(float))

        radial_parts = [
            x[np.isfinite(x)] for x in (dep_radial, rcv_radial, recdb_radial)
            if np.any(np.isfinite(x))
        ]
        finite_radials = np.concatenate(radial_parts) if radial_parts else np.array([], dtype=float)
        color_max = max(max_radial_offset, float(np.nanpercentile(finite_radials, 98))) if finite_radials.size else max_radial_offset

        fig = plt.figure(figsize=(18, 10.3))
        gs = fig.add_gridspec(5, 1, height_ratios=[0.28, 0.28, 0.28, 1.48, 0.48], hspace=0.42)
        fig.suptitle("RADIAL OFFSET HEATMAPS & WATER DEPTH PROFILE", fontsize=20, fontweight="bold", y=0.985)

        heat_specs = [
            ("Deployment vs Preplot", dep_radial),
            ("Recovery vs Preplot", rcv_radial),
            ("REC_DB vs Preplot", recdb_radial),
        ]
        tick_idx = np.linspace(0, len(station) - 1, min(8, len(station)), dtype=int)
        heat_axes = []
        im = None
        for idx, (title, values) in enumerate(heat_specs):
            ax_hm = fig.add_subplot(gs[idx, 0])
            heat_axes.append(ax_hm)
            heat = np.tile(values, (4, 1))
            im = ax_hm.imshow(heat, aspect="auto", interpolation="nearest", cmap="RdYlGn_r",
                              vmin=0, vmax=color_max)
            ax_hm.set_yticks([])
            ax_hm.set_title(title, loc="left", fontsize=9, fontweight="bold", pad=2)
            ax_hm.set_xticks(tick_idx)
            if idx == 2:
                ax_hm.set_xticklabels([str(int(station[i])) for i in tick_idx], fontsize=8)
                ax_hm.set_xlabel("Station", fontsize=9)
            else:
                ax_hm.set_xticklabels([])
                ax_hm.tick_params(axis="x", length=0)

        # One compact shared vertical color bar to the left of all heatmaps.
        cax = fig.add_axes([0.045, 0.615, 0.012, 0.205])
        cbar = fig.colorbar(im, cax=cax, orientation="vertical")
        cbar.set_label("Radial offset (m)", fontsize=8)
        cbar.ax.tick_params(labelsize=7)

        ax_depth = fig.add_subplot(gs[3, 0])
        ax_rad = ax_depth.twinx()
        lines = []
        lines += ax_depth.plot(station, dep_depth, color="#222222", linestyle="-", linewidth=1.7,
                               label="Deployment water depth")
        lines += ax_depth.plot(station, rcv_depth, color="#777777", linestyle="--", linewidth=1.6,
                               label="Recovery water depth")
        lines += ax_rad.plot(station, dep_radial, color="#1f77b4", linestyle="-", linewidth=1.35,
                             label="Deployment radial offset")
        lines += ax_rad.plot(station, rcv_radial, color="#ff7f0e", linestyle="--", linewidth=1.35,
                             label="Recovery radial offset")
        lines += ax_rad.plot(station, recdb_radial, color="#2ca02c", linestyle="-.", linewidth=1.35,
                             label="REC_DB radial offset")
        ax_depth.set_xlabel("Station")
        ax_depth.set_ylabel("Water depth (m)")
        ax_rad.set_ylabel("Radial offset (m)")
        ax_depth.grid(True, linewidth=0.35, alpha=0.5)
        ax_depth.set_title("DEPLOYMENT / RECOVERY WATER DEPTH & RADIAL OFFSETS", fontsize=11, fontweight="bold")
        ax_depth.legend(lines, [line.get_label() for line in lines], loc="upper right", fontsize=8,
                        ncol=2, frameon=True)

        ax_sum = fig.add_subplot(gs[4, 0]); ax_sum.axis("off")
        def stats(values, decimals):
            clean = values[np.isfinite(values)]
            if not clean.size:
                return ["", "", ""]
            return [f"{np.min(clean):.{decimals}f}", f"{np.mean(clean):.{decimals}f}", f"{np.max(clean):.{decimals}f}"]
        summary = [
            ["PROFILE SUMMARY", "Min", "Average", "Max"],
            ["Deployment water depth (m)", *stats(dep_depth, 1)],
            ["Recovery water depth (m)", *stats(rcv_depth, 1)],
            ["Deployment radial offset (m)", *stats(dep_radial, 2)],
            ["Recovery radial offset (m)", *stats(rcv_radial, 2)],
            ["REC_DB radial offset (m)", *stats(recdb_radial, 2)],
        ]
        tbl = ax_sum.table(cellText=summary[1:], colLabels=summary[0], cellLoc="center", colLoc="center",
                           bbox=[0.18, 0.00, 0.64, 0.98])
        tbl.auto_set_font_size(False); tbl.set_fontsize(8.5); tbl.scale(1, 1.18)
        for (r, c), cell in tbl.get_celld().items():
            cell.set_linewidth(0.5)
            if r == 0:
                cell.set_facecolor("#eaf2f8"); cell.set_text_props(weight="bold")

        fig.subplots_adjust(left=0.12, right=0.95, top=0.91, bottom=0.055)
        fig.savefig(charts_dir / "node_position_heatmap_depth_profile_page.png", dpi=180, bbox_inches="tight")
        plt.close(fig)

    def _make_directional_analysis_page(self, df, charts_dir):
        """Create five rose-diagram pages using a different color per comparison."""
        specs = [
            ("dep_pp", "Deployment vs Preplot", "#1f77b4"),
            ("rcv_pp", "Recovery vs Preplot", "#ff7f0e"),
            ("rec_pp", "REC_DB vs Preplot", "#2ca02c"),
            ("fb_dep", "REC_DB vs Deployment", "#9467bd"),
            ("fb_rcv", "REC_DB vs Recovery", "#d62728"),
        ]
        for prefix, title, color in specs:
            self._make_directional_analysis_single(df, charts_dir, prefix, title, color)

    def _make_directional_analysis_single(self, df, charts_dir, prefix, comparison_title, color):
        clean = df[[f"{prefix}_az", f"{prefix}_dr"]].dropna().copy()
        az_deg = clean[f"{prefix}_az"].to_numpy(float)
        radial = clean[f"{prefix}_dr"].to_numpy(float)
        theta = np.deg2rad(az_deg)

        # Circular mean and concentration.
        sin_mean = float(np.mean(np.sin(theta))) if len(theta) else 0.0
        cos_mean = float(np.mean(np.cos(theta))) if len(theta) else 0.0
        mean_dir = (math.degrees(math.atan2(sin_mean, cos_mean)) + 360.0) % 360.0
        vector_len = math.hypot(sin_mean, cos_mean)
        circ_std = math.degrees(math.sqrt(max(0.0, -2.0 * math.log(max(vector_len, 1e-12)))))

        bins_deg = np.arange(0, 361, 15)
        counts, _ = np.histogram(az_deg, bins=bins_deg)
        centers = np.deg2rad((bins_deg[:-1] + bins_deg[1:]) / 2.0)
        widths = np.deg2rad(np.diff(bins_deg))

        fig = plt.figure(figsize=(18, 10.3))
        gs = fig.add_gridspec(2, 2, width_ratios=[1.55, 0.85], height_ratios=[1.0, 0.30], hspace=0.30, wspace=0.25)
        fig.suptitle(f"DIRECTIONAL ANALYSIS - {comparison_title}", fontsize=20, fontweight="bold", y=0.98, color=color)

        ax = fig.add_subplot(gs[0, 0], projection="polar")
        ax.bar(centers, counts, width=widths, bottom=0.0, alpha=0.78, color=color, edgecolor="black", linewidth=0.35)
        ax.set_theta_zero_location("N"); ax.set_theta_direction(-1)
        ax.set_title(f"ROSE DIAGRAM - {comparison_title}", fontsize=11, fontweight="bold", pad=18, color=color)
        ax.grid(True, linewidth=0.35, alpha=0.55)
        ax.plot([math.radians(mean_dir), math.radians(mean_dir)], [0, max(counts.max(), 1)], linewidth=2.2, linestyle="--", color=color, label=f"Mean {mean_dir:.1f}°")
        ax.legend(loc="lower left", bbox_to_anchor=(-0.12, -0.10), fontsize=8)

        ax_tbl = fig.add_subplot(gs[0, 1]); ax_tbl.axis("off")
        stats_rows = [
            ["Mean direction", f"{mean_dir:.1f}°"],
            ["Mean vector length", f"{vector_len:.2f}"],
            ["Circular std. dev.", f"{circ_std:.1f}°"],
            ["Observations", str(len(clean))],
        ]
        stats = ax_tbl.table(cellText=stats_rows, colLabels=["DIRECTIONAL STATISTICS", "Value"], cellLoc="center", colLoc="center", bbox=[0.02, 0.57, 0.96, 0.38])
        stats.auto_set_font_size(False); stats.set_fontsize(9)
        for (r, c), cell in stats.get_celld().items():
            cell.set_linewidth(0.5)
            if r == 0:
                cell.set_facecolor(color); cell.set_text_props(weight="bold", color="white")

        sectors = [(0,45),(45,90),(90,135),(135,180),(180,225),(225,270),(270,315),(315,360)]
        sec_rows = []
        sec_means = []
        for lo, hi in sectors:
            mask = (az_deg >= lo) & (az_deg < hi)
            m = float(np.nanmean(radial[mask])) if np.any(mask) else float("nan")
            sec_means.append(m)
            sec_rows.append([f"{lo}–{hi}", "" if not np.isfinite(m) else f"{m:.2f}"])
        sec = ax_tbl.table(cellText=sec_rows, colLabels=["Sector (°)", "Avg radial (m)"], cellLoc="center", colLoc="center", bbox=[0.02, 0.02, 0.96, 0.48])
        sec.auto_set_font_size(False); sec.set_fontsize(9)
        for (r, c), cell in sec.get_celld().items():
            cell.set_linewidth(0.5)
            if r == 0:
                cell.set_facecolor(color); cell.set_text_props(weight="bold", color="white")

        best_i = int(np.nanargmax(sec_means)) if any(np.isfinite(sec_means)) else 0
        lo, hi = sectors[best_i]
        directions = ["NNE", "ENE", "ESE", "SSE", "SSW", "WSW", "WNW", "NNW"]
        ax_note = fig.add_subplot(gs[1, :]); ax_note.axis("off")
        ax_note.text(0.03, 0.5, f"Dominant radial-offset sector: {lo}°–{hi}° ({directions[best_i]} direction).", fontsize=13, fontweight="bold", color=color, va="center")
        ax_note.add_patch(Rectangle((0.01, 0.12), 0.98, 0.75, fill=False, edgecolor=color, linewidth=1.2, transform=ax_note.transAxes, clip_on=False))

        fig.subplots_adjust(left=0.06, right=0.95, top=0.90, bottom=0.08)
        fig.savefig(charts_dir / f"node_position_directional_{prefix}_page.png", dpi=180, bbox_inches="tight")
        plt.close(fig)

    def _make_polar_offsets_page(self, df, charts_dir):
        specs = [
            ("dep_pp", "Deployment vs Preplot", "dep_pp_az", "dep_pp_dr", "ROV"),
            ("rcv_pp", "Recovery vs Preplot", "rcv_pp_az", "rcv_pp_dr", "ROV1"),
            ("rec_pp", "REC_DB vs Preplot", "rec_pp_az", "rec_pp_dr", "ROV"),
            ("fb_dep", "REC_DB vs Deployment", "fb_dep_az", "fb_dep_dr", "ROV"),
            ("fb_rcv", "REC_DB vs Recovery", "fb_rcv_az", "fb_rcv_dr", "ROV1"),
        ]

        rov_values = []
        for color_col in ("ROV", "ROV1"):
            if color_col in df.columns:
                rov_values.extend(str(x) for x in df[color_col].dropna().unique())
        rovs = sorted(set(rov_values))
        cmap = plt.get_cmap("tab10")
        rov_colors = {rov: cmap(i % 10) for i, rov in enumerate(rovs)}

        groups = [
            (specs[:2], "node_position_polar_group_2_page.png"),
            (specs[2:], "node_position_polar_group_3_page.png"),
        ]
        for group_specs, filename in groups:
            fig = plt.figure(figsize=(16.2, 9.4))
            gs = fig.add_gridspec(1, len(group_specs), wspace=0.30)
            for idx, spec in enumerate(group_specs):
                ax = fig.add_subplot(gs[0, idx], projection="polar")
                self._draw_polar_comparison(ax, df, spec, rov_colors)

            if rovs:
                handles = [
                    plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=rov_colors[rov],
                               markeredgecolor="black", markersize=7, label=rov)
                    for rov in rovs
                ]
                fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, 0.91),
                           ncol=min(len(rovs), 8), title="ROV", fontsize=9, frameon=True)
            fig.suptitle("POLAR OFFSET VS AZIMUTH", fontsize=20, fontweight="bold", y=0.97)
            fig.text(0.5, 0.045, "Azimuth is clockwise from North. Radius is horizontal offset in meters. Red star marks the maximum offset.", ha="center", fontsize=10)
            fig.subplots_adjust(left=0.04, right=0.98, top=0.82, bottom=0.11)
            fig.savefig(charts_dir / filename, dpi=180)
            plt.close(fig)

        self._make_polar_statistics_table(df, charts_dir, specs)

    def _draw_polar_comparison(self, ax, df, spec, rov_colors):
        _key, title, az_col, r_col, color_col = spec
        cols = [az_col, r_col, "Station"] + ([color_col] if color_col in df.columns else [])
        clean = df[cols].dropna(subset=[az_col, r_col]).copy()
        ax.set_theta_zero_location("N")
        ax.set_theta_direction(-1)
        ax.set_title(title, fontsize=12, fontweight="bold", pad=18)
        ax.grid(True, linewidth=0.35, alpha=0.65)
        ax.tick_params(labelsize=8)
        if clean.empty:
            return

        radius_all = clean[r_col].to_numpy(dtype=float)
        q95 = float(np.nanquantile(radius_all, 0.95))
        q98 = float(np.nanquantile(radius_all, 0.98))
        rmax_visible = max(q95 * 1.45, q98 * 1.12, 2.0)
        if color_col in clean.columns:
            for rov, part in clean.groupby(color_col):
                ax.scatter(np.deg2rad(part[az_col].to_numpy(float)), part[r_col].to_numpy(float),
                           s=20, alpha=0.86, color=rov_colors.get(str(rov), "#333333"),
                           edgecolors="black", linewidths=0.18)
        else:
            ax.scatter(np.deg2rad(clean[az_col].to_numpy(float)), clean[r_col].to_numpy(float),
                       s=20, alpha=0.86, edgecolors="black", linewidths=0.18)

        max_row = clean.loc[clean[r_col].idxmax()]
        true_max = float(max_row[r_col])
        theta = math.radians(float(max_row[az_col]))
        shown_radius = min(true_max, rmax_visible * 0.96)
        ax.scatter([theta], [shown_radius], s=95, marker="*", color="red",
                   edgecolors="black", linewidths=0.4, zorder=6)
        ax.text(theta, shown_radius, f" {int(max_row['Station'])} / {true_max:.1f} m",
                fontsize=7, color="red", fontweight="bold", zorder=7)
        ax.set_ylim(0, rmax_visible)

    def _make_polar_statistics_table(self, df, charts_dir, specs):
        rows = []
        for _key, title, az_col, r_col, _color_col in specs:
            clean = df[[az_col, r_col]].dropna().copy()
            if clean.empty:
                rows.append([title, "0", "", "", "", "", "", ""])
                continue
            radial = clean[r_col].astype(float)
            azimuth = np.deg2rad(clean[az_col].astype(float).to_numpy())
            mean_dir = (math.degrees(math.atan2(np.mean(np.sin(azimuth)), np.mean(np.cos(azimuth)))) + 360.0) % 360.0
            rows.append([
                title, str(len(clean)), f"{radial.mean():.2f}", f"{radial.std(ddof=1):.2f}",
                f"{radial.median():.2f}", f"{radial.quantile(.95):.2f}",
                f"{radial.max():.2f}", f"{mean_dir:.1f}°",
            ])

        fig, ax = plt.subplots(figsize=(16.2, 9.4))
        ax.axis("off")
        fig.suptitle("POLAR OFFSET STATISTICS SUMMARY", fontsize=22, fontweight="bold", y=0.94)
        columns = ["Comparison", "Nodes", "Mean radial\n(m)", "Std radial\n(m)",
                   "Median radial\n(m)", "P95 radial\n(m)", "Maximum radial\n(m)", "Mean azimuth"]
        table = ax.table(cellText=rows, colLabels=columns, cellLoc="center", colLoc="center",
                         colWidths=[0.25, 0.08, 0.11, 0.11, 0.11, 0.11, 0.13, 0.10],
                         bbox=[0.03, 0.30, 0.94, 0.42])
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 1.45)
        for (row, col), cell in table.get_celld().items():
            cell.set_linewidth(0.55)
            if row == 0:
                cell.set_facecolor("#d9eaf7")
                cell.set_text_props(weight="bold")
            elif col == 0:
                cell.set_facecolor("#f3f7fa")
                cell.set_text_props(weight="bold", ha="left")
            elif row % 2 == 0:
                cell.set_facecolor("#f8fafc")
        fig.text(0.5, 0.22, "All radial values are horizontal offsets in meters. Mean azimuth is measured clockwise from North.", ha="center", fontsize=11)
        fig.savefig(charts_dir / "node_position_polar_statistics_page.png", dpi=180, bbox_inches="tight")
        plt.close(fig)

    def _draw_delta_station(self, ax, df, cols, labels, title, ylabel):
        """
        Draw station-trend plots using sequential node index on the X axis,
        but display only real station numbers as vertical tick labels.

        This removes the large empty gap between separated station ranges and
        avoids showing artificial 1,2,3,... index labels.
        """
        plot_df = df.copy().reset_index(drop=True)
        x = np.arange(1, len(plot_df) + 1, dtype=float)
        station_labels = [
            self._fmt_int(st) if not pd.isna(st) else ""
            for st in plot_df["Station"]
        ]

        for col, label in zip(cols, labels):
            ax.plot(
                x,
                plot_df[col],
                marker="o",
                markersize=2.4,
                linewidth=0.85,
                label=label,
            )

        ax.axhline(0, linewidth=0.65, color="#4F81BD", alpha=0.85)
        ax.grid(True, which="major", linewidth=0.28, alpha=0.45)
        ax.set_axisbelow(True)

        ax.set_xlim(0.5, len(plot_df) + 0.5)
        ax.set_xticks(x)
        ax.set_xticklabels(
            station_labels,
            rotation=90,
            ha="center",
            va="top",
            fontsize=4.4,
        )

        ax.set_title(title, fontsize=10, fontweight="bold")
        ax.set_xlabel("Station", fontsize=7)
        ax.set_ylabel(ylabel, fontsize=7)
        ax.legend(fontsize=6, loc="upper right")
        ax.tick_params(axis="y", labelsize=6)
        ax.tick_params(axis="x", length=2, pad=1)

    # ------------------------------------------------------------------
    # FORMATTERS
    # ------------------------------------------------------------------
    def _fmt(self, value):
        if value is None or pd.isna(value):
            return ""
        return f"{float(value):.1f}"

    def _fmt_int(self, value):
        if value is None or pd.isna(value):
            return ""
        return str(int(value))

    def _tex(self, value):
        """
        Escape text for LaTeX and remove invalid control chars.
        """

        if value is None:
            return ""

        s = str(value)

        # Remove hidden control chars.
        s = "".join(
            ch for ch in s
            if ord(ch) >= 32 or ch in "\n\r\t"
        )

        # Escape each original character once.  Using translate avoids
        # re-escaping the backslashes inserted for LaTeX commands.
        replacements = {
            ord("\\"): r"\textbackslash{}",
            ord("&"): r"\&",
            ord("%"): r"\%",
            ord("$"): r"\$",
            ord("#"): r"\#",
            ord("_"): r"\_",
            ord("{"): r"\{",
            ord("}"): r"\}",
            ord("~"): r"\textasciitilde{}",
            ord("^"): r"\textasciicircum{}",
        }

        return s.translate(replacements)

    def _tex_path(self, path):
        return str(Path(path)).replace("\\", "/")
