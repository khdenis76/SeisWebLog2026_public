# DataViewer 2 — DSR QC update

Replace/add these files under `dataviewer2`:

- `dsr_qc.py` (new)
- `models.py`
- `repository.py`
- `ribbon.py`
- `main_window.py`
- `layers.py`
- `bbox.py`

## DSR QC workflow

1. Open the **DSR QC** ribbon tab.
2. Select a receiver line.
3. The Station list is filled from loaded DSR Primary records.
4. Use **Zoom line**, **Zoom station**, or **Open QC**.
5. In the QC window, choose any numeric DSR parameters.
6. Use grouped Primary/Secondary, stacked, or overlay layout.
7. Choose Line or Bar rendering.
8. Right-click a parameter to change color or line thickness.
9. Click a graph point to select/highlight the nearest station on the map.

The repository discovers numeric DSR fields dynamically so project-specific offset,
sigma, uncertainty, elevation, depth, and other numeric columns are available.
