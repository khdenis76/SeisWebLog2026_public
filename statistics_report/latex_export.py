from __future__ import annotations

import math
import shutil
import subprocess
import tempfile
from pathlib import Path


TGS_BLUE = "#073b78"
TGS_CYAN = "#00a6d6"
COLORS = ["#1677ff", "#ff8c1a", "#20a44b", "#dc3545", "#7c4dff", "#00a6d6"]


def tex_escape(value) -> str:
    if value is None:
        return ""
    text = str(value).replace("\\", "/")
    replacements = {"&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#", "_": r"\_", "{": r"\{", "}": r"\}", "~": r"\textasciitilde{}", "^": r"\textasciicircum{}", "→": r"$\rightarrow$", "°": r"\textdegree{}", "—": "--"}
    for old, new in replacements.items():
        text = text.replace(old, new)
    return "".join(ch for ch in text if ch.isprintable())


def _fmt(value):
    if value is None:
        return "--"
    return f"{value:.3f}" if isinstance(value, float) else tex_escape(value)


def _rows(rows, columns):
    return "\n".join(" & ".join(_fmt(row.get(key)) for key in columns) + r" \\" for row in rows)


def _selected_rows(payload):
    return [row for row in payload["comparisons"] if row["key"] == payload["selected_comparison"]]


def _chart_data(payload):
    import pandas as pd
    data = pd.DataFrame(payload.get("detail", []))
    rov_field = payload.get("selected_rov_field")
    if data.empty:
        return data
    data["PlotROV"] = data.get(rov_field, "All").fillna("Unknown").astype(str) if rov_field and rov_field in data else "All"
    for field in ("dx", "dy", "inline", "crossline", "offset", "bearing", "Station"):
        if field in data:
            data[field] = pd.to_numeric(data[field], errors="coerce")
    return data


def _save_figure(fig, path):
    fig.savefig(path, dpi=190, bbox_inches="tight", facecolor="white")
    fig.clf()


def create_matplotlib_pages(payload, output_dir: Path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd

    plt.rcParams.update({"axes.titleweight": "bold", "axes.titlesize": 12, "axes.labelsize": 9, "font.size": 8, "grid.alpha": .22})
    data = _chart_data(payload)
    paths = []
    if data.empty:
        return paths
    rovs = sorted(data.PlotROV.unique())
    color = {rov: COLORS[i % len(COLORS)] for i, rov in enumerate(rovs)}
    selected = _selected_rows(payload)[0]
    limit = float(selected.get("limit") or 10)

    # EOL production section: put operational completion and day-by-day work
    # before the coordinate-QC pages.
    production = payload.get("production", {})
    status = pd.DataFrame(production.get("status", []))
    if not status.empty:
        fig = plt.figure(figsize=(14, 7.5)); gs = fig.add_gridspec(2, 4, height_ratios=[1, 2.2])
        stage_colors = {"Planned": "#64748b", "Deployment": "#1677ff", "Deployed": "#1677ff",
                        "Recovery": "#20a44b", "Recovered": "#20a44b",
                        "Processing": "#ff8c1a", "Processed": "#ff8c1a"}
        for i, row in status.head(4).reset_index(drop=True).iterrows():
            ax = fig.add_subplot(gs[0, i]); ax.axis("off")
            c = stage_colors.get(str(row.get("phase")), COLORS[i % len(COLORS)])
            ax.add_patch(plt.Rectangle((.03, .08), .94, .84, transform=ax.transAxes,
                                       facecolor="#f8fafc", edgecolor=c, linewidth=2.5))
            ax.text(.5, .64, f"{int(row.get('nodes', 0)):,}", ha="center", va="center",
                    fontsize=23, fontweight="bold", color=c)
            ax.text(.5, .34, str(row.get("phase", "")), ha="center", fontsize=11, fontweight="bold")
            ax.text(.5, .18, f"{float(row.get('percent', 0)):.1f}% of plan", ha="center", fontsize=9, color="#475569")
        ax = fig.add_subplot(gs[1, :])
        names = status.phase.astype(str); values = pd.to_numeric(status.nodes, errors="coerce").fillna(0)
        bars = ax.bar(names, values, color=[stage_colors.get(n, COLORS[i % len(COLORS)]) for i, n in enumerate(names)])
        ax.bar_label(bars, labels=[f"{int(v):,}\n{float(p):.1f}%" for v, p in zip(values, status.percent)], padding=4, fontweight="bold")
        ax.set_ylabel("Nodes"); ax.set_title("PLANNED AND COMPLETED NODES"); ax.grid(axis="y"); ax.set_axisbelow(True)
        fig.suptitle("PROJECT PRODUCTION STATUS", fontsize=18, fontweight="bold", color=TGS_BLUE)
        p=output_dir/"00a_project_production.png"; _save_figure(fig,p); paths.append(p)

    daily_all = pd.DataFrame(production.get("daily", []))
    predictions = {r.get("phase"): r for r in production.get("prediction", [])}
    planned = max(1, int(status.iloc[0].nodes)) if not status.empty else 1
    for page_no, phase in enumerate(("Deployment", "Recovery"), start=1):
        daily = daily_all[daily_all.phase == phase].copy() if not daily_all.empty else pd.DataFrame()
        if daily.empty:
            continue
        daily["date"] = pd.to_datetime(daily.date, errors="coerce"); daily = daily.dropna(subset=["date"])
        pivot = daily.pivot_table(index="date", columns="rov", values="nodes", aggfunc="sum", fill_value=0).sort_index()
        fig, (ax, ax2) = plt.subplots(2, 1, figsize=(14, 8), gridspec_kw={"height_ratios": [2.1, 1]}, sharex=True)
        pivot.plot(kind="bar", stacked=True, ax=ax, width=.84,
                   color=[COLORS[i % len(COLORS)] for i in range(len(pivot.columns))])
        totals = pivot.sum(axis=1); average = float(totals.mean()) if len(totals) else 0
        ax.plot(range(len(totals)), totals, color="#111827", marker="o", ms=3, lw=1.7, label="Daily total")
        ax.axhline(average, color="#dc3545", ls="--", lw=1.8, label=f"Average {average:.1f} nodes/day")
        ax.set_ylabel("Nodes"); ax.grid(axis="y"); ax.legend(ncol=min(5, len(pivot.columns)+2), loc="upper left")
        progress = totals.cumsum() / planned * 100
        ax2.fill_between(range(len(progress)), progress.to_numpy(), color="#00a6d6", alpha=.22)
        ax2.plot(range(len(progress)), progress, color=TGS_BLUE, marker="o", ms=3, lw=2.2)
        ax2.set_ylabel("Plan complete (%)"); ax2.set_xlabel("Production date"); ax2.set_ylim(0, max(100, float(progress.max()) * 1.08)); ax2.grid()
        labels = [d.strftime("%Y-%m-%d") for d in pivot.index]
        step = max(1, len(labels)//18); ax2.set_xticks(range(0, len(labels), step), labels[::step], rotation=45, ha="right")
        pred = predictions.get(phase, {})
        fig.suptitle(f"{phase.upper()} - DAY-BY-DAY PRODUCTION\n"
                     f"Completed {int(pred.get('completed', totals.sum())):,} of {int(pred.get('planned', planned)):,} | "
                     f"Average {float(pred.get('average_nodes_day', average)):.1f} nodes/day | "
                     f"Predicted completion {pred.get('predicted_completion', '--')}",
                     fontsize=16, fontweight="bold", color=TGS_BLUE)
        p=output_dir/f"00{page_no+1}_{phase.lower()}_daily.png"; _save_figure(fig,p); paths.append(p)

    if not daily_all.empty:
        table = daily_all.pivot_table(index="date", columns=["phase", "rov"], values="nodes", aggfunc="sum", fill_value=0).sort_index()
        fig, ax = plt.subplots(figsize=(14, max(6, min(10, .28*len(table)+2))))
        image = ax.imshow(table.to_numpy(), aspect="auto", cmap="YlGnBu")
        ax.set_yticks(range(len(table)), [str(x)[:10] for x in table.index], fontsize=7)
        ax.set_xticks(range(len(table.columns)), [f"{a}\n{b}" for a,b in table.columns], rotation=45, ha="right")
        maximum = max(1, float(table.to_numpy().max()))
        for y in range(len(table)):
            for x in range(len(table.columns)):
                v=int(table.iloc[y,x]); ax.text(x,y,str(v),ha="center",va="center",fontsize=6,color="white" if v > maximum*.55 else "#0f172a")
        fig.colorbar(image, ax=ax, label="Nodes"); ax.set_title("PROJECT DAILY PRODUCTION BY PHASE AND ROV", fontsize=16, fontweight="bold", color=TGS_BLUE)
        p=output_dir/"00d_project_daily_matrix.png"; _save_figure(fig,p); paths.append(p)

    # 1 - executive QC dashboard
    fig = plt.figure(figsize=(14, 7.5)); grid = fig.add_gridspec(3, 4, height_ratios=[1, 1, 1.15])
    cards = [("Records", selected["count"], TGS_BLUE), ("Mean radial", f"{selected.get('mean') or 0:.2f} m", TGS_CYAN), ("CEP95", f"{selected.get('cep95') or 0:.2f} m", "#7c4dff"), ("Maximum", f"{selected.get('max') or 0:.2f} m", "#dc3545" if (selected.get('max') or 0)>limit else "#20a44b"), ("Bias DE", f"{selected.get('bias_de') or 0:.2f} m", "#1677ff"), ("Bias DN", f"{selected.get('bias_dn') or 0:.2f} m", "#ff8c1a"), ("In-line avg", f"{selected.get('inline_mean') or 0:.2f} m", "#20a44b"), ("X-line avg", f"{selected.get('crossline_mean') or 0:.2f} m", "#dc3545")]
    for i,(title,value,c) in enumerate(cards):
        ax=fig.add_subplot(grid[i//4,i%4]);ax.axis("off");ax.add_patch(plt.Rectangle((.02,.08),.96,.84,transform=ax.transAxes,facecolor="#f7fafc",edgecolor=c,linewidth=2));ax.text(.5,.62,str(value),ha="center",va="center",fontsize=19,fontweight="bold",color=c);ax.text(.5,.28,title,ha="center",va="center",fontsize=9,color="#334155")
    ax=fig.add_subplot(grid[2,:]);bins=[0,1,2,5,np.inf];labels=["0-1 m","1-2 m","2-5 m",">5 m"];counts=pd.cut(data.offset,bins=bins,labels=labels,include_lowest=True).value_counts().reindex(labels).fillna(0);bars=ax.barh(labels,counts,color=["#20a44b","#78c850","#ffb020","#dc3545"]);ax.bar_label(bars);ax.set_title("RADIAL OFFSET DISTRIBUTION");ax.set_xlabel("Nodes");ax.grid(axis="x")
    fig.suptitle(f"QC DASHBOARD — {payload['selected_name']}",fontsize=17,fontweight="bold",color=TGS_BLUE);p=output_dir/"01_qc_dashboard.png";_save_figure(fig,p);paths.append(p)

    # 2 - distributions (fixed operational range; nodes plus KDE and STD)
    fig,axes=plt.subplots(1,3,figsize=(14,6));fields=[("dx","Delta Easting"),("dy","Delta Northing"),("offset","Radial Offset")]
    for ax,(field,title) in zip(axes,fields):
        for rov in rovs:
            vals=data.loc[data.PlotROV==rov,field].dropna();lo,hi=(-10,10) if field!="offset" else (0,10);vals=vals[(vals>=lo)&(vals<=hi)];edges=np.arange(lo,hi+.5,.5);ax.hist(vals,bins=edges,density=False,alpha=.3,color=color[rov],label=f"{rov}: N={len(vals)}, STD={vals.std(ddof=1) if len(vals)>1 else 0:.2f} m")
            if len(vals)>3:
                try:
                    from scipy.stats import gaussian_kde
                    x=np.linspace(lo,hi,250);ax.plot(x,gaussian_kde(vals)(x)*len(vals)*.5,color=color[rov],lw=2)
                except Exception: pass
        ax.axvline(0,color="#555",ls="--",lw=1);ax.set_xlim(lo,hi);ax.set_title(title);ax.set_xlabel("metres");ax.set_ylabel("Nodes per 0.5 m bin");ax.grid();ax.legend()
    fig.suptitle("OFFSET DISTRIBUTIONS BY ROV",fontsize=17,fontweight="bold",color=TGS_BLUE);p=output_dir/"02_distributions.png";_save_figure(fig,p);paths.append(p)

    # 3 - bullseye
    fig,ax=plt.subplots(figsize=(10,8));
    for r in (1,2,5,limit): ax.add_patch(plt.Circle((0,0),r,fill=False,color="#dc3545" if r==limit else "#b8c2cc",ls="-" if r==limit else "--",lw=2 if r==limit else 1))
    for rov in rovs:
        s=data[data.PlotROV==rov];ax.scatter(s.dx,s.dy,s=22,alpha=.65,label=rov,color=color[rov])
    ax.axhline(0,color="#777",ls="--");ax.axvline(0,color="#777",ls="--");ax.set_aspect("equal","box");ax.set_xlabel("DE (m)");ax.set_ylabel("DN (m)");ax.set_title("BULLSEYE — SIGNED DE / DN");ax.grid();ax.legend();p=output_dir/"03_bullseye.png";_save_figure(fig,p);paths.append(p)

    # 4 - separate ECDF panels versus RPPreplot, all limited to 10 m
    ecdf=pd.DataFrame(payload.get("ecdf_series",[]));fig,axes=plt.subplots(1,3,figsize=(14,5.5),sharey=True)
    for ax,phase in zip(axes,("Deployment","Recovery","REC_DB")):
        phase_data=ecdf[ecdf.phase==phase] if not ecdf.empty else pd.DataFrame()
        if not phase_data.empty:
            for i,(rov,subset) in enumerate(phase_data.groupby("rov")):
                ax.plot(subset.offset,subset.percent,lw=2.4,label=f"{rov}",color=COLORS[i%len(COLORS)])
        ax.axhline(95,color="#dc3545",ls="--",lw=1);ax.set_xlim(0,10);ax.set_ylim(0,101);ax.set_title(f"{phase} vs RPPreplot");ax.set_xlabel("Radial offset (m)");ax.grid();ax.legend()
    axes[0].set_ylabel("Cumulative probability (%)");fig.suptitle("RADIAL ECDF BY ROV",fontsize=16,fontweight="bold",color=TGS_BLUE);p=output_dir/"04_ecdf.png";_save_figure(fig,p);paths.append(p)

    # Whole-project coordinate comparison page.  It deliberately uses all
    # comparison rows, even when the web dashboard is focused on one solution.
    overall = pd.DataFrame([r for r in payload.get("comparisons", []) if r.get("rov") == "All"])
    if not overall.empty:
        for c in ("cep95", "mean", "bias_de", "bias_dn", "count"):
            overall[c] = pd.to_numeric(overall[c], errors="coerce").fillna(0)
        overall = overall.sort_values(["phase", "name"])
        fig, axes = plt.subplots(1, 2, figsize=(14, 7), gridspec_kw={"width_ratios": [1.35, 1]})
        y = np.arange(len(overall))
        axes[0].barh(y, overall.cep95, color=[COLORS[i%len(COLORS)] for i in range(len(overall))], alpha=.86, label="CEP95")
        axes[0].scatter(overall["mean"], y, color="#111827", marker="D", s=28, label="Mean")
        axes[0].set_yticks(y, [str(n)[:48] for n in overall.name]); axes[0].invert_yaxis()
        axes[0].set_xlabel("Radial offset (m)"); axes[0].set_title("MEAN AND CEP95"); axes[0].grid(axis="x"); axes[0].legend()
        axes[1].scatter(overall.bias_de, overall.bias_dn,
                        s=np.clip(overall["count"]/20, 35, 300), c=range(len(overall)), cmap="tab20", alpha=.8)
        for _,r in overall.iterrows():
            axes[1].annotate(str(r["name"])[:28], (r.bias_de,r.bias_dn), fontsize=6, xytext=(3,3), textcoords="offset points")
        axes[1].axhline(0,color="#777",ls="--"); axes[1].axvline(0,color="#777",ls="--")
        axes[1].set_xlabel("Bias DE (m)"); axes[1].set_ylabel("Bias DN (m)"); axes[1].set_title("2D BIAS"); axes[1].grid()
        fig.suptitle("ALL COORDINATE COMPARISONS",fontsize=17,fontweight="bold",color=TGS_BLUE)
        p=output_dir/"04b_all_coordinate_comparisons.png";_save_figure(fig,p);paths.append(p)

    # 5 - offset series
    fig,axes=plt.subplots(3,1,figsize=(14,8),sharex=True);metrics=[("inline","In-line offset"),("crossline","Cross-line offset"),("offset","Radial offset")]
    for ax,(field,title) in zip(axes,metrics):
        for rov in rovs:
            s=data[data.PlotROV==rov].sort_values("Station");ax.plot(s.Station,s[field],lw=1.6,label=rov,color=color[rov])
        ax.axhline(0,color="#777",ls="--");ax.set_ylabel("m");ax.set_title(title);ax.grid();ax.legend(ncol=len(rovs))
    axes[-1].set_xlabel("Station");fig.suptitle("IN-LINE / CROSS-LINE / RADIAL SERIES",fontsize=16,fontweight="bold",color=TGS_BLUE);p=output_dir/"05_offset_series.png";_save_figure(fig,p);paths.append(p)

    # 6 - progress map
    fig,ax=plt.subplots(figsize=(14,7));layers=[("RPPreplot","RPPreplotEasting","RPPreplotNorthing","#6c757d","+"),("Deployment","PrimaryEasting","PrimaryNorthing","#1677ff","s"),("Recovery","PrimaryEasting1","PrimaryNorthing1","#20a44b","^"),("REC_DB","REC_X","REC_Y","#ff8c1a","o")]
    for label,x,y,c,m in layers:
        if x in data and y in data: ax.scatter(pd.to_numeric(data[x],errors="coerce"),pd.to_numeric(data[y],errors="coerce"),s=18,alpha=.65,label=label,color=c,marker=m)
    ax.set_aspect("equal","datalim");ax.set_xlabel("Easting");ax.set_ylabel("Northing");ax.set_title("PROGRESS MAP — PREPLOT / DEPLOYMENT / RECOVERY / REC_DB");ax.grid();ax.legend();p=output_dir/"06_progress_map.png";_save_figure(fig,p);paths.append(p)

    # 7 - separate deployment and recovery stacked bars with cumulative progress
    fig,axes=plt.subplots(2,1,figsize=(14,8),sharex=False);predictions={r["phase"]:r for r in payload.get("production",{}).get("prediction",[])};planned=max(1,payload.get("production",{}).get("status",[{"nodes":1}])[0]["nodes"])
    for ax,(timecol,rovcol,title) in zip(axes,[("TimeStamp","ROV","Deployment by day"),("TimeStamp1","ROV1","Recovery by day")]):
        if timecol not in data: continue
        temp=pd.DataFrame({"Date":pd.to_datetime(data[timecol],errors="coerce").dt.date,"ROV":data.get(rovcol,"Unknown")}).dropna(subset=["Date"]);pivot=temp.groupby(["Date","ROV"]).size().unstack(fill_value=0);pivot.plot(kind="bar",stacked=True,ax=ax,color=[COLORS[i%len(COLORS)] for i in range(len(pivot.columns))],width=.85);ax.set_title(title);ax.set_ylabel("Nodes");ax.grid(axis="y");ax.legend(ncol=max(1,len(pivot.columns)))
        phase="Deployment" if timecol=="TimeStamp" else "Recovery";progress=pivot.sum(axis=1).cumsum()/planned*100;right=ax.twinx();right.plot(range(len(progress)),progress,color="#111827",marker="o",ms=3,lw=1.8);right.set_ylim(0,100);right.set_ylabel("Progress (% of RPPreplot)");pred=predictions.get(phase,{});ax.set_title(f"{phase}: average {pred.get('average_nodes_day','--')} nodes/day · predicted {pred.get('predicted_completion','--')}")
    fig.suptitle("PRODUCTION PROGRESS",fontsize=16,fontweight="bold",color=TGS_BLUE);p=output_dir/"07_daily_progress.png";_save_figure(fig,p);paths.append(p)

    # 8 - separate deployment/recovery polar plots for every ROV
    polar=pd.DataFrame(payload.get("phase_polar",[]));groups=list(polar.groupby(["phase","rov"])) if not polar.empty else []
    if groups:
        fig=plt.figure(figsize=(14,8));cols=min(3,len(groups));rows=math.ceil(len(groups)/cols)
        for i,((phase,rov),subset) in enumerate(groups):
            ax=fig.add_subplot(rows,cols,i+1,projection="polar");angles=np.radians(subset.sector);ax.bar(angles,subset.percent,width=np.radians(22.5),align="edge",alpha=.78,color=COLORS[i%len(COLORS)],edgecolor="white");ax.set_theta_zero_location("N");ax.set_theta_direction(-1);ax.set_title(f"{phase} — {rov}")
        fig.suptitle("DEPLOYMENT / RECOVERY OFFSET DIRECTION BY ROV (%)",fontsize=16,fontweight="bold",color=TGS_BLUE);p=output_dir/"08_polar.png";_save_figure(fig,p);paths.append(p)
    return paths


def build_latex(payload, charts, logo_name="tgs_logo.png"):
    summary_rows = [{"metric": key.replace("_", " ").title(), "value": value} for key, value in payload["summary"].items()]
    selected = _selected_rows(payload)
    all_comparisons = [row for row in payload["comparisons"] if row["rov"] == "All"]
    daily = payload.get("production", {}).get("daily", [])
    daily_deployment = [row for row in daily if row.get("phase") == "Deployment"]
    daily_recovery = [row for row in daily if row.get("phase") == "Recovery"]
    columns = ["phase", "rov", "count", "bias_de", "bias_dn", "inline_mean", "crossline_mean", "mean", "cep50", "cep95", "std", "max", "out_pct"]
    chart_pages = "\n".join(
        rf"\clearpage\section*{{{tex_escape(path.stem.split('_', 1)[-1].replace('_',' ').title())}}}"
        rf"\begin{{center}}\includegraphics[width=0.98\textwidth,height=0.82\textheight,keepaspectratio]{{{path.name}}}\end{{center}}"
        for path in charts
    )
    return rf"""\documentclass[9pt,a4paper]{{article}}\usepackage[utf8]{{inputenc}}\usepackage[T1]{{fontenc}}\usepackage{{graphicx,xcolor,longtable,booktabs,array,geometry,fancyhdr,textcomp}}\geometry{{landscape,margin=10mm,headheight=24pt}}\definecolor{{tgsblue}}{{HTML}}{{073B78}}\definecolor{{tgscyan}}{{HTML}}{{00A6D6}}\pagestyle{{fancy}}\fancyhf{{}}\lhead{{\includegraphics[height=14pt]{{{logo_name}}}}}\chead{{\color{{tgsblue}}\bfseries Receiver Statistics}}\rhead{{\small {tex_escape(payload.get('project_name'))}}}\cfoot{{\thepage}}\setlength{{\parindent}}{{0pt}}\renewcommand{{\arraystretch}}{{1.18}}\begin{{document}}
\begin{{center}}\vspace*{{18mm}}\includegraphics[width=34mm]{{{logo_name}}}\\[9mm]{{\Huge\color{{tgsblue}}\bfseries RECEIVER STATISTICS REPORT}}\\[3mm]{{\Large {tex_escape(payload.get('project_name'))}}}\\[2mm]{{\color{{tgscyan}} {tex_escape(payload.get('selected_name'))}}}\end{{center}}\vfill\begin{{center}}SeisWebLog - DSR Primary / Secondary / REC\_DB Statistical Analysis\end{{center}}\clearpage
\section*{{Executive Summary}}\begin{{longtable}}{{p{{65mm}}r}}\toprule\textbf{{Metric}}&\textbf{{Value}}\\\midrule
{_rows(summary_rows,['metric','value'])}
\bottomrule\end{{longtable}}
\section*{{Selected Comparison by ROV}}\scriptsize\begin{{longtable}}{{llrrrrrrrrrrr}}\toprule Phase&ROV&N&Bias DE&Bias DN&In-line&X-line&Radial&CEP50&CEP95&STD&Max&Out \%\\\midrule
{_rows(selected,columns)}
\bottomrule\end{{longtable}}
\section*{{Production versus RPPreplot}}\begin{{longtable}}{{lrr}}\toprule Stage&Nodes&Percent\\\midrule
{_rows(payload.get('production',{}).get('status',[]),['phase','nodes','percent'])}
\bottomrule\end{{longtable}}
\section*{{Production Prediction}}\begin{{longtable}}{{lrrrrrr}}\toprule Phase&Completed&Planned&Average/day&Remaining&Days left&Predicted completion\\\midrule
{_rows(payload.get('production',{}).get('prediction',[]),['phase','completed','planned','average_nodes_day','remaining','days_left','predicted_completion'])}
\bottomrule\end{{longtable}}
\clearpage\section*{{All Coordinate Comparisons}}\scriptsize\begin{{longtable}}{{p{{55mm}}rrrrrrrr}}\toprule Comparison&N&Bias DE&Bias DN&In-line&X-line&Radial&CEP95&STD\\\midrule
{_rows(all_comparisons,['name','count','bias_de','bias_dn','inline_mean','crossline_mean','mean','cep95','std'])}
\bottomrule\end{{longtable}}
\clearpage\section*{{Deployment - Daily Production by ROV}}\scriptsize
\begin{{longtable}}{{llllr}}\toprule Phase&Date&ROV&Daily share (\%)&Nodes\\\midrule
{_rows(daily_deployment,['phase','date','rov','percent','nodes'])}
\bottomrule\end{{longtable}}
\clearpage\section*{{Recovery - Daily Production by ROV}}\scriptsize
\begin{{longtable}}{{llllr}}\toprule Phase&Date&ROV&Daily share (\%)&Nodes\\\midrule
{_rows(daily_recovery,['phase','date','rov','percent','nodes'])}
\bottomrule\end{{longtable}}
{chart_pages}\end{{document}}"""


def render_pdf(payload, logo_path: Path) -> bytes:
    executable = shutil.which("pdflatex")
    if not executable:
        raise RuntimeError("pdflatex was not found. Install MiKTeX and make sure pdflatex is available in PATH.")
    with tempfile.TemporaryDirectory(prefix="swl_receiver_statistics_") as temp_name:
        temp = Path(temp_name);logo_target=temp/"tgs_logo.png"
        if logo_path.exists(): shutil.copy2(logo_path,logo_target)
        charts=create_matplotlib_pages(payload,temp)
        tex_path=temp/"receiver_statistics.tex";tex_path.write_text(build_latex(payload,charts,logo_target.name),encoding="utf-8")
        command=[executable,"-interaction=nonstopmode","-halt-on-error",tex_path.name];creationflags=getattr(subprocess,"CREATE_NO_WINDOW",0)
        for _ in range(2):
            process=subprocess.run(command,cwd=temp,capture_output=True,text=True,creationflags=creationflags)
            if process.returncode: raise RuntimeError("LaTeX could not create the PDF.\n"+(process.stdout+"\n"+process.stderr)[-4000:])
        pdf_path=temp/"receiver_statistics.pdf"
        if not pdf_path.exists(): raise RuntimeError("LaTeX completed without creating receiver_statistics.pdf.")
        return pdf_path.read_bytes()
