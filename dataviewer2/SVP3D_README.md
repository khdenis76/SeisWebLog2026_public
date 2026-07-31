# DataViewer2 3D Water Column Viewer

Open **SVP 3D > Open 3D viewer**.

## Optional dependencies

```bash
pip install pyvista pyvistaqt vtk scipy
```

DataViewer2 still starts when these packages are missing; the SVP window displays installation instructions.

## Current features

- Loads SVP cast positions and profiles dynamically from `SVP_Profiles` / `SVP_Data`.
- Loads `RPPreplot.X/Y` as the preplot reference layer.
- Builds a 3D sound-velocity cube using IDW or nearest-neighbour interpolation.
- Displays volume rendering, SVP cast lines, RP Preplot points, and a movable horizontal depth slice.
- Exports the generated cube to `.vti` for ParaView/VTK.
- Grid resolution, opacity, and colour map controls.

## Coordinate convention

The database stores positive depth. In the rendered scene cast lines and the displayed slice are placed below zero. The VTI grid itself stores depth as positive Z so it remains compatible with standard VTK tools.
