# DataViewer 2.3 — Icon Edition

This build uses the SVG files already stored in `dataviewer2/icons`.

## Main changes

- Added `icons_manager.py` as the single semantic icon registry.
- Replaced Qt standard ribbon icons with colored SVG icons.
- Added icons to layer groups, map layers, receiver lines, and stations.
- Added icons to layer context-menu actions.
- Added icons to BlackBox and DSR QC windows and their main controls.
- Added a DataViewer application/window icon.
- SVGs are tinted at runtime, with a safe direct-file fallback when QtSvg is unavailable.

To change an icon later, edit only `IconManager.FILES` or `IconManager.COLORS` in
`icons_manager.py`; the rest of the interface does not need to change.
