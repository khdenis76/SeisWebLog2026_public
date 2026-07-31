from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Iterable

from PySide6.QtGui import QIcon


class LucideIcons:
    """Resolve shared SVG icons from ``dataviewer2/icons``.

    No icon files are copied into the OCR package. The resolver searches the
    normal SeisWebLog sibling location, the current working directory, parent
    directories, and an optional ``SEISWEBLOG_ICON_DIR`` environment variable.
    Icons are found recursively, so category/subfolder changes do not break the
    OCR interface as long as the SVG filename remains the same.
    """

    ENV_NAME = "SEISWEBLOG_ICON_DIR"

    def __init__(self, start: Path | None = None):
        self.start = (start or Path(__file__)).resolve()
        self.root = self._find_icon_root()

    def _candidate_roots(self) -> Iterable[Path]:
        env = os.getenv(self.ENV_NAME, "").strip()
        if env:
            yield Path(env).expanduser()

        # Installed/project layout: SeisWebLog/ocr/core -> SeisWebLog/dataviewer2/icons
        for parent in [self.start.parent, *self.start.parents]:
            yield parent / "dataviewer2" / "icons"
            yield parent / "icons"

        cwd = Path.cwd().resolve()
        for parent in [cwd, *cwd.parents]:
            yield parent / "dataviewer2" / "icons"
            yield parent / "icons"

    def _find_icon_root(self) -> Path | None:
        seen: set[Path] = set()
        for candidate in self._candidate_roots():
            try:
                candidate = candidate.resolve()
            except OSError:
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            if candidate.is_dir() and any(candidate.rglob("*.svg")):
                return candidate
        return None

    @lru_cache(maxsize=256)
    def path(self, *names: str) -> Path | None:
        if self.root is None:
            return None

        normalized = []
        for name in names:
            stem = Path(str(name)).stem.strip()
            if stem:
                normalized.extend((stem, stem.replace("_", "-"), stem.replace("-", "_")))

        # First try exact filename matches recursively.
        for stem in dict.fromkeys(normalized):
            matches = sorted(self.root.rglob(f"{stem}.svg"), key=lambda p: (len(p.parts), str(p).lower()))
            if matches:
                return matches[0]

        # Last resort: case-insensitive stem match.
        wanted = {s.casefold() for s in normalized}
        for svg in self.root.rglob("*.svg"):
            if svg.stem.casefold() in wanted:
                return svg
        return None

    def icon(self, *names: str) -> QIcon:
        path = self.path(*names)
        return QIcon(str(path)) if path else QIcon()

    @property
    def available(self) -> bool:
        return self.root is not None
