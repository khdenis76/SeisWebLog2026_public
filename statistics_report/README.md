# Receiver Statistics

## Static PDF report

The PDF export is an EOL-style, multi-page LaTeX/Matplotlib report. It includes
whole-project completion against RPPreplot, separate deployment and recovery
day-by-day production by ROV, cumulative progress and prediction, a daily
production matrix and tables, all coordinate-comparison statistics, ECDF,
histograms/KDE, bullseye, in-line/cross-line/radial series, progress mapping,
and separate deployment/recovery polar QC pages. Node-level lists are excluded.

Independent SeisWebLog Django app for DSR, REC_DB and RPPreplot statistics.

The app reads the selected project's `project.sqlite3` directly. It does not
copy operational data into the root Django database and has no migrations.

DSR and REC_DB are joined at point level with `DSR.LinePointIdx =
REC_DB.LinePointIdx`. RPPreplot is the authoritative planned-position source and
is joined by `RPPreplot.Line = DSR.Line` and `RPPreplot.Point = DSR.Station`.
REC_DB comparisons join RPPreplot by `REC_DB.Line` and `REC_DB.Point`. REC_DB map
layers always use the processed `REC_X` and `REC_Y` coordinates.

## Features

- whole-database, line, station, ROV and date/time filters;
- deployment, recovery or either timestamp selection;
- day, ISO week, month, receiver-line and ROV grouping;
- thirteen Primary/Secondary/REC_DB coordinate comparisons;
- production-versus-RPPreplot totals for planned, deployed, recovered and
  processed nodes, with percentages;
- daily deployment, recovery and processing statistics for every ROV in nodes
  and percentage of that day's production;
- completion prediction using the selected period's actual average nodes/day;
- separate stacked Deployment and Recovery daily bar charts by ROV, each with
  a cumulative percentage-of-RPPreplot progress line;
- deployment statistics grouped by `DSR.ROV` and recovery statistics grouped by `DSR.ROV1`;
- signed DE/DN, horizontal bias, CEP50/90/95/99, STD, RMS and QC-limit compliance;
- `Sigma/Sigma1` as deployment Primary 95% E/N uncertainty and `Sigma6/Sigma7`
  as recovery Primary 95% E/N uncertainty (with Sigma2/3 and Sigma8/9 for Secondary);
- receiver-line selection in a modal with Select All and Clear All;
- empirical radial CDF (ECDF) and a bias-aware fitted Rice CDF;
- day, ISO week, month and inclusive date-period filtering using the deployment
  `Day/Week/Month/Year` fields and recovery `Day1/Week1/Month1/Year1` fields;
- Bokeh 0.5 m histograms (nodes and percentage hover, STD and KDE), bullseye
  with radial colour scale, ROV marker shapes and ROV 95% envelopes, ECDF,
  in-line/cross-line/radial series, daily
  progress and coordinate progress map with click-to-hide legends;
- three side-by-side 0--10 m radial ECDF panels for Deployment Primary,
  Recovery Primary and REC_DB versus RPPreplot, split by ROV;
- Plotly is used only for light-theme polar and donut charts; deployment and
  recovery polar charts are separated for every ROV;
- the LaTeX PDF uses colourful high-resolution Matplotlib pages and omits the
  node-level list;
- Plotly histogram, CDF and polar percentage chart;
- Bokeh interactive line-offset plot;
- CSV, Excel, LaTeX PDF and self-contained interactive HTML exports;
- TGS logo on screen and in exported reports.

The app is registered at `/statistics-report/` and linked under **QC reports**.

PDF export runs `pdflatex` twice. MiKTeX must be installed and `pdflatex`
must be available in the Windows PATH.
