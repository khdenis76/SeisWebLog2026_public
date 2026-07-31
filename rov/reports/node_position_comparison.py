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
from matplotlib.patches import Circle, Ellipse


class NodePositionComparisonReport:
    """
    LaTeX PDF report for node position comparison.

    Report structure:
    - Page 1: front page
    - Page 2: line information from DSR_LineSummary
    - Next pages: node comparison table, split automatically by row count
    - Next page: XY offsets analysis with red QC circle and 95% coherence ellipse
    - Next page: Station vs dX/dY plots
    - Last page: Station vs In-Line, X-Line, and Radial offsets
    """

    def __init__(self, db_path, project=None):
        self.db_path = Path(db_path)
        self.project = project

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

                d.PreplotEasting AS pp_x,
                d.PreplotNorthing AS pp_y,

                d.PrimaryEasting AS dep_x,
                d.PrimaryNorthing AS dep_y,
                d.PrimaryElevation AS dep_z,

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
        - dep_rec_* = Deployment - REC_DB / First Break
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

        df["dep_rec_dx"] = df["dep_x"] - df["fb_x"]
        df["dep_rec_dy"] = df["dep_y"] - df["fb_y"]
        df["dep_rec_dr"] = (df["dep_rec_dx"] ** 2 + df["dep_rec_dy"] ** 2) ** 0.5

        # Azimuth from North, clockwise. Positive dX = East, positive dY = North.
        for prefix in ("dep_pp", "rec_pp", "dep_rec"):
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

        self._make_charts(df, charts_dir)

        tex_path = build_dir / f"{line}_Node_Position_Comparison.tex"
        pdf_path = output_dir / f"{line}_Node_Position_Comparison.pdf"

        tex_path.write_text(
            self._build_tex(line=line, df=df, charts_dir=charts_dir),
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
    def _build_tex(self, line, df, charts_dir):
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

        chart_xy_analysis_file = self._tex_path(
            charts_dir / "node_position_xy_analysis_page.png"
        )
        chart_offsets_file = self._tex_path(
            charts_dir / "node_position_offsets_page.png"
        )
        chart_il_xl_file = self._tex_path(
            charts_dir / "node_position_il_xl_radial_page.png"
        )
        chart_polar_file = self._tex_path(
            charts_dir / "node_position_polar_offsets_page.png"
        )

        table_pages = self._build_table_pages(df, rows_per_page=50)
        line_info_page = self._build_line_info_page(line)

        template = r"""
\documentclass[8pt,landscape]{article}

\usepackage[a4paper,margin=7mm]{geometry}
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

\node[anchor=south east] at
([xshift=-8mm,yshift=6mm]current page.south east)
{\textcolor{swlnavy}{\scriptsize Page \thepage\ of \pageref{LastPage}}};
\end{tikzpicture}
}

\begin{document}

\setlength{\floatsep}{0pt}
\setlength{\textfloatsep}{0pt}
\setlength{\intextsep}{0pt}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% FRONT PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\thispagestyle{empty}
\swlpageframe

\begin{center}
\vspace*{10mm}

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
%% LINE INFO PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
@@LINE_INFO_PAGE@@

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TABLE PAGES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
@@TABLE_PAGES@@

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% XY ANALYSIS PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
\swlpageframe

\vspace*{-1mm}

\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth]{@@CHART_XY_ANALYSIS@@}
\end{figure}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% STATION DX / DY PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
\swlpageframe

\begin{center}
{\Large\bfseries\textcolor{swlnavy}{STATION VS $\Delta X$ / $\Delta Y$ / DEPLOYMENT Z}}
\end{center}

\vspace{1mm}

\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth]{@@CHART_OFFSETS@@}
\end{figure}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% INLINE / CROSSLINE / RADIAL PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
\swlpageframe

\begin{center}
{\Large\bfseries\textcolor{swlnavy}{INLINE / CROSSLINE / RADIAL OFFSETS VS STATION}}
\end{center}

\vspace{1mm}

\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth]{@@CHART_IL_XL@@}
\end{figure}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% POLAR PAGE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newpage
\swlpageframe

\begin{center}
{\Large\bfseries\textcolor{swlnavy}{POLAR OFFSET VS AZIMUTH}}
\end{center}

\vspace{1mm}

\begin{figure}[H]
\centering
\includegraphics[width=0.985\linewidth]{@@CHART_POLAR@@}
\end{figure}

\end{document}
"""

        replacements = {
            "@@PROJECT@@": self._tex(project_name),
            "@@LOCATION@@": self._tex(location),
            "@@AREA@@": self._tex(area),
            "@@CLIENT@@": self._tex(client),
            "@@CONTRACTOR@@": self._tex(contractor),
            "@@PROJECT_CLIENT_ID@@": self._tex(project_client_id),
            "@@PROJECT_CONTRACTOR_ID@@": self._tex(project_contractor_id),
            "@@EPSG@@": self._tex(epsg),
            "@@LINE@@": str(line),
            "@@GENERATED@@": generated_on,
            "@@LINE_INFO_PAGE@@": line_info_page,
            "@@TABLE_PAGES@@": table_pages,
            "@@CHART_XY_ANALYSIS@@": chart_xy_analysis_file,
            "@@CHART_OFFSETS@@": chart_offsets_file,
            "@@CHART_IL_XL@@": chart_il_xl_file,
            "@@CHART_POLAR@@": chart_polar_file,
        }

        for old, new in replacements.items():
            template = template.replace(old, str(new))

        return template

    def _build_line_info_page(self, line):
        info = self.load_line_summary(line)

        if not info:
            return r"""
\swlpageframe
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
\swlpageframe

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
\swlpageframe
\vspace*{{-3mm}}
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
    def _make_charts(self, df, charts_dir):
        qc = self.load_node_qc_settings()
        max_radial_offset = qc.get("max_radial_offset") or 30.0
        self._make_xy_offsets_analysis_page(df=df, charts_dir=charts_dir, max_radial_offset=float(max_radial_offset))
        self._make_chart_page_offsets(df, charts_dir)
        self._make_chart_page_il_xl_radial(df, charts_dir)
        self._make_polar_offsets_page(df, charts_dir)

    def _make_xy_offsets_analysis_page(self, df, charts_dir, max_radial_offset):
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
            "XY OFFSETS ANALYSIS",
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

        plot_specs = [
            ("Deployment vs Preplot", "dep_pp_dx", "dep_pp_dy"),
            ("REC_DB vs Deployment", "fb_dep_dx", "fb_dep_dy"),
            ("REC_DB vs Preplot", "rec_pp_dx", "rec_pp_dy"),
        ]

        rovs = sorted([str(x) for x in df["ROV"].dropna().unique()]) if "ROV" in df.columns else []
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

        for col_idx, (title, xcol, ycol) in enumerate(plot_specs):
            ax = fig.add_subplot(gs[1, col_idx])
            self._draw_xy_offset_analysis_plot(
                ax=ax,
                df=df,
                xcol=xcol,
                ycol=ycol,
                title=title,
                max_radial_offset=max_radial_offset,
                rov_colors=rov_colors,
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
            charts_dir / "node_position_xy_analysis_page.png",
            dpi=180,
            bbox_inches="tight",
        )
        plt.close(fig)

    def _draw_xy_offset_analysis_plot(self, ax, df, xcol, ycol, title, max_radial_offset, rov_colors):
        base_cols = [xcol, ycol, "Station"]
        if "ROV" in df.columns:
            base_cols.append("ROV")

        clean = df[base_cols].dropna(subset=[xcol, ycol]).copy()

        if clean.empty:
            ax.set_title(title, fontsize=12, fontweight="bold")
            ax.grid(True, linewidth=0.3, alpha=0.55)
            return

        if "ROV" in clean.columns:
            for rov, part in clean.groupby("ROV"):
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

    def _make_polar_offsets_page(self, df, charts_dir):
        specs = [
            ("Deployment vs Preplot", "dep_pp_az", "dep_pp_dr"),
            ("REC_DB vs Preplot", "rec_pp_az", "rec_pp_dr"),
            ("Deployment vs REC_DB", "dep_rec_az", "dep_rec_dr"),
        ]

        fig = plt.figure(figsize=(16.2, 9.4))
        gs = fig.add_gridspec(1, 3, wspace=0.28)

        rovs = sorted([str(x) for x in df["ROV"].dropna().unique()]) if "ROV" in df.columns else []
        cmap = plt.get_cmap("tab10")
        rov_colors = {rov: cmap(i % 10) for i, rov in enumerate(rovs)}

        for idx, (title, az_col, r_col) in enumerate(specs):
            ax = fig.add_subplot(gs[0, idx], projection="polar")
            cols = [az_col, r_col, "Station"] + (["ROV"] if "ROV" in df.columns else [])
            clean = df[cols].dropna(subset=[az_col, r_col]).copy()

            if clean.empty:
                ax.set_title(title, fontsize=12, fontweight="bold", pad=16)
                continue

            radius_all = clean[r_col].to_numpy(dtype=float)
            q95 = float(np.nanquantile(radius_all, 0.95)) if len(radius_all) else 1.0
            q98 = float(np.nanquantile(radius_all, 0.98)) if len(radius_all) else q95
            rmax_visible = max(q95 * 1.45, q98 * 1.12, 2.0)

            if "ROV" in clean.columns and rovs:
                for rov, part in clean.groupby("ROV"):
                    ax.scatter(
                        np.deg2rad(part[az_col].to_numpy(dtype=float)),
                        part[r_col].to_numpy(dtype=float),
                        s=22,
                        alpha=0.86,
                        color=rov_colors.get(str(rov), "#333333"),
                        edgecolors="black",
                        linewidths=0.20,
                        label=str(rov),
                    )
            else:
                ax.scatter(
                    np.deg2rad(clean[az_col].to_numpy(dtype=float)),
                    clean[r_col].to_numpy(dtype=float),
                    s=22,
                    alpha=0.86,
                    edgecolors="black",
                    linewidths=0.20,
                )

            max_pos = clean[r_col].idxmax()
            max_row = clean.loc[max_pos]
            max_theta = math.radians(float(max_row[az_col]))
            true_max_radius = float(max_row[r_col])
            shown_radius = min(true_max_radius, rmax_visible * 0.96)

            ax.scatter([max_theta], [shown_radius], s=105, marker="*", color="red", edgecolors="black", linewidths=0.45, zorder=6)
            ax.text(max_theta, shown_radius, f" {int(max_row['Station'])} / R={true_max_radius:.1f}", fontsize=7, color="red", fontweight="bold", zorder=7)

            ax.set_ylim(0, rmax_visible)
            ax.set_theta_zero_location("N")
            ax.set_theta_direction(-1)
            ax.set_title(title, fontsize=12, fontweight="bold", pad=16)
            ax.grid(True, linewidth=0.35, alpha=0.65)
            ax.tick_params(labelsize=8)

        if rovs:
            handles = [
                plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=rov_colors[rov], markeredgecolor="black", markersize=7, label=rov)
                for rov in rovs
            ]
            fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, 0.94), ncol=min(len(rovs), 8), title="ROV", fontsize=8, title_fontsize=9, frameon=True)

        fig.text(0.5, 0.045, "Azimuth is measured clockwise from North. Radius is horizontal offset in meters. Red star marks maximum offset; label shows true radius.", ha="center", fontsize=10)
        fig.subplots_adjust(left=0.04, right=0.98, top=0.84, bottom=0.12)
        fig.savefig(charts_dir / "node_position_polar_offsets_page.png", dpi=180)
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
        if value is None:
            return ""

        s = str(value)

        # Remove control characters
        s = "".join(ch for ch in s if ord(ch) >= 32 or ch in "\n\r\t")

        # Normalize Windows paths BEFORE escaping
        s = s.replace("\\", "/")

        replacements = (
            ("&", r"\&"),
            ("%", r"\%"),
            ("$", r"\$"),
            ("#", r"\#"),
            ("_", r"\_"),
            ("{", r"\{"),
            ("}", r"\}"),
            ("~", r"\textasciitilde{}"),
            ("^", r"\textasciicircum{}"),
        )

        for old, new in replacements:
            s = s.replace(old, new)

        return s

    def _tex_path(self, path):
        return str(Path(path)).replace("\\", "/")
