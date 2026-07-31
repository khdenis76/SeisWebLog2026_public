# SeisWebLog DataViewer 2.3 — Layer Navigation Milestone

This build is based on the uploaded DataViewer 2.2 source and keeps the existing map, BlackBox QC, DSR QC and measurement functionality.

## Added in 2.3

### DSR hierarchy

Every DSR point layer now exposes a lazy hierarchy:

- DSR layer
  - receiver line
    - station

Receiver-line nodes are created when the layer loads. Station nodes are created only when a line is expanded, avoiding a large startup cost.

- Click a line to synchronize the DSR ribbon.
- Double-click a line to zoom to its extent.
- Check a line to create a high-contrast line overlay.
- Click a station to select it, show its record, highlight it and zoom to it.
- Double-click station zoom remains supported.

### Station stepping

The DSR QC ribbon now includes:

- Previous
- Next
- Auto zoom
- Zoom line
- Zoom station

Left and right keyboard arrows also step through stations on the selected line.

### Persistent custom DSR layers

The DSR QC ribbon includes **Create layer** and **Manage**.

A custom layer can define:

- X field
- Y field
- one filter condition
- optional categorized styling field, for example ROV
- default color and point size
- startup visibility
- line/station hierarchy

Definitions are saved per project in:

`<project>/config/dataviewer2.json`

They are recreated automatically when that project opens again.

Categorized layers use stable high-contrast colors for each category value.

### Shape styling

Shape layers now use visible defaults on the dark map. Black or very dark database colors fall back to bright cyan.

Right-click a shape layer and choose **Style…** to edit:

- outline color
- outline width
- solid/dashed/dotted outline
- polygon fill on/off
- fill color
- fill opacity
- overall layer opacity
- point size

Shape style overrides are saved in the same project configuration and restored automatically.

## Important implementation notes

- DSR stations are navigation tree items, not individual PyQtGraph objects.
- Checked receiver lines use one overlay graphics layer per selected line.
- Custom categorized layers use one graphics layer per category, not per point.
- Project SQLite schemas are not modified.
- Shape CRS transformation remains serialized to avoid Windows PROJ heap corruption.

## Install

Replace the existing `dataviewer2` folder with this one, or merge the changed files.

Compile-check:

```bat
python -m py_compile dataviewer2\*.py
```

Run:

```bat
python -X faulthandler -m dataviewer2.app
```
