# DataViewer 2.4 ribbon update

New top-level ribbon tabs:

- Project: change project, reload, open project folder, exit.
- Home: map navigation and quick layer visibility.
- Layers: layer visibility controls.
- Receiver QC: DSR line/station navigation, QC and custom layers.
- BlackBox: QC and track controls.
- Export: map image, selected layer CSV and visible-layer CSV export.
- Reports: project summary, receiver-line report and reports folder.
- View: grid and side panel.
- Tools: distance measurement.

Project switching reads `core_project` from the root SeisWebLog `db.sqlite3` and opens a new DataViewer window without restarting Python manually.

Exports and reports are stored by default under:

`<project>/reports/dataviewer2/`
