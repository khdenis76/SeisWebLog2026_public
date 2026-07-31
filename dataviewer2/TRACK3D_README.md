# 3D DSR & BlackBox Viewer

New ribbon tab: **3D Tracks** → **Open 3D tracks**.

## Features

- Separate PyVista/VTK window.
- Select a DSR receiver line.
- DSR coordinate modes: primary/secondary deployment, primary/secondary recovery, and preplot.
- Select a BlackBox file and turn individual X/Y/Z tracks on or off.
- Automatic coordinate-series discovery for common prefixes such as GNSS1, GNSS2, Vessel, ROV1, ROV2, INS, USBL and TMS.
- Source Z, negative depth, or negative absolute-depth display conventions.
- Vertical exaggeration, line width and point-size controls.
- Optional DSR stations, station labels, BBOX samples, grid and screenshot export.

## Dependencies

```bash
pip install pyvista pyvistaqt vtk
```

The rest of DataViewer2 continues to run when these optional packages are absent.
