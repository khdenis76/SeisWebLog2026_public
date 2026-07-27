# SeisWebLog Fast DataViewer

A separate optimized project-map viewer for SeisWebLog. It leaves the existing `dataviewer` unchanged.

## Main improvements

- SQLite loading runs in `QThreadPool` workers, not the GUI thread.
- Map loaders select only required coordinate and metadata columns.
- Data is returned as NumPy arrays instead of repeatedly copied Pandas DataFrames.
- RPPreplot is drawn as a single grouped curve with NaN separators.
- Scatter points are filtered to the current viewport and capped at 30,000 displayed points.
- Point symbols disappear automatically at wide project extents.
- Scatter metadata stores only small display indexes; full records remain in layer arrays.
- Supports RPPreplot, DSR primary, DSR recovery primary, and REC_DB.
- Multi-waypoint measurement shows segment distance, bearing, and cumulative distance.
- Measurement clicks snap to the closest visible point.

## Install

```bash
pip install -r requirements.txt
```

## Run

From the folder containing this README:

```bash
python -m dataviewer_fast.app "G:\\path\\to\\project"
```

You may also pass the SQLite file directly:

```bash
python -m dataviewer_fast.app "G:\\path\\to\\project\\data\\project.sqlite3"
```

On Windows you can drag a project directory onto `run_dataviewer_fast.bat`.

## Controls

- Mouse wheel: zoom.
- Left drag: pan/zoom according to PyQtGraph defaults.
- Start measurement, then left-click: add waypoint.
- Backspace: remove the last waypoint.
- Escape: clear measurement.
- Layer checkboxes: show/hide layers.
- Click a plotted point: show its metadata.

## Integration

Copy the `dataviewer_fast` folder into the SeisWebLog repository root beside the current `dataviewer` folder. Keep it separate until it is tested with your production database.

The repository code uses several possible schema variants. The loader detects common RPPreplot coordinate names (`X/Y`, `Easting/Northing`, or `PreplotEasting/PreplotNorthing`). DSR follows the current SeisWebLog names.

## Notes

- This first version focuses on fast project mapping and measurement. Existing BlackBox/time-series windows remain in the original DataViewer.
- Shape-file support is not yet included because the public repository does not expose the project-shape schema clearly enough to implement it safely.
- For very large layers, a future version can add a `scipy.spatial.cKDTree` and tiled spatial SQL queries.
