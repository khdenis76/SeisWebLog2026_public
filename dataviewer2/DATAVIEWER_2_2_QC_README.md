# DataViewer 2.2 QC interface update

This build keeps the DataViewer 2 map/ribbon architecture and replaces the BlackBox and DSR QC workbenches with a consistent compact interface.

## Main changes

- Compact two-row control bars instead of one long crowded toolbar.
- Narrow parameter panel with color swatches instead of full-width saturated color cells.
- Presets for common QC groups.
- Stacked or overlay graph layouts.
- Maximum chart count control.
- Human-readable elapsed-time axes (seconds, minutes, or hours), without `ks`/`Ms` prefixes.
- Human-readable numeric axes without automatic scientific notation for ordinary values.
- Robust Y ranges using 1st/99th percentiles.
- BlackBox invalid/sentinel QC values above `1e8` are ignored in QC charts.
- DSR administrative fields and flags are hidden from the normal parameter list.
- DSR defaults to actual offset parameters instead of unrelated numeric database fields.
- Empty or invalid selections display an explanatory message instead of a blank white panel.
- DSR station markers are clickable using `sigPointsClicked`; no `setClickable()` call.
- Map synchronization signals remain unchanged.

## Files changed

- `bbox.py`
- `dsr_qc.py`
- `qc_widgets.py` (new shared QC styling/axis helpers)

The existing `main_window.py`, repository, models, ribbon, map layers, shape loading and measurement modules remain compatible.
