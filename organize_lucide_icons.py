#!/usr/bin/env python3
"""
Organize Lucide SVG icons for SeisWebLog.

Expected starting layout:
    SeisWebLogFolder/
        icon/
            *.svg
        dataviewer2/          # or dataview2/

Result:
    dataviewer2/
        icons/
            actions/
            files/
            navigation/
            layers/
            database/
            charts/
            media/
            status/
            layout/
            users/
            devices/
            miscellaneous/

Usage:
    python organize_lucide_icons.py --dry-run
    python organize_lucide_icons.py
    python organize_lucide_icons.py --copy
    python organize_lucide_icons.py --root "C:/path/to/SeisWebLogFolder"
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


CATEGORY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "navigation": (
        "map", "navigation", "compass", "locate", "location", "pin",
        "route", "milestone", "waypoint", "crosshair", "target",
        "radar", "satellite", "move", "pan", "zoom", "scan",
    ),
    "layers": (
        "layers", "layer", "square", "circle", "triangle", "polygon",
        "pentagon", "hexagon", "shape", "boxes", "box-select",
    ),
    "database": (
        "database", "server", "table", "sheet", "rows", "columns",
        "hard-drive", "archive", "storage", "binary",
    ),
    "charts": (
        "chart", "activity", "trending", "bar-chart", "line-chart",
        "pie-chart", "area-chart", "scatter", "gauge", "percent",
    ),
    "files": (
        "file", "folder", "clipboard", "notebook", "book", "receipt",
        "document", "paperclip", "package", "archive",
    ),
    "media": (
        "image", "video", "camera", "film", "music", "audio",
        "mic", "play", "pause", "volume", "headphones",
    ),
    "status": (
        "alert", "circle-alert", "info", "help", "check", "x-circle",
        "badge", "shield", "lock", "unlock", "bell", "loader",
        "clock", "timer", "calendar",
    ),
    "layout": (
        "layout", "panel", "sidebar", "menu", "grid", "list",
        "columns", "rows", "maximize", "minimize", "monitor",
        "window", "dock",
    ),
    "users": (
        "user", "users", "contact", "id-card", "badge", "person",
    ),
    "devices": (
        "ship", "anchor", "radio", "antenna", "wifi", "bluetooth",
        "cpu", "laptop", "computer", "printer", "smartphone",
        "tablet", "usb", "cable", "router",
    ),
    "actions": (
        "add", "plus", "minus", "remove", "delete", "trash",
        "edit", "pencil", "save", "download", "upload", "import",
        "export", "copy", "cut", "paste", "undo", "redo", "refresh",
        "rotate", "search", "filter", "settings", "wrench", "tool",
        "eye", "mouse-pointer", "pointer", "hand", "select",
        "arrow", "chevron", "expand", "shrink", "log-in", "log-out",
    ),
}


def find_project_root(explicit_root: str | None) -> Path:
    if explicit_root:
        root = Path(explicit_root).expanduser().resolve()
    else:
        root = Path(__file__).resolve().parent

    if not (root / "icon").exists():
        raise FileNotFoundError(
            f'Cannot find source icon folder: "{root / "icon"}"\n'
            "Place this script in SeisWebLogFolder or pass --root."
        )
    return root


def find_dataviewer_folder(root: Path) -> Path:
    candidates = (
        root / "dataviewer2",
        root / "dataview2",
        root / "DataViewer2",
        root / "Dataviewer2",
    )
    for candidate in candidates:
        if candidate.is_dir():
            return candidate

    # Use the project-standard name when it does not exist yet.
    target = root / "dataviewer2"
    target.mkdir(parents=True, exist_ok=True)
    return target


def categorize(icon_name: str) -> str:
    stem = Path(icon_name).stem.lower().replace("_", "-")

    # More specific categories are checked before generic actions.
    order = (
        "navigation",
        "layers",
        "database",
        "charts",
        "files",
        "media",
        "status",
        "layout",
        "users",
        "devices",
        "actions",
    )

    for category in order:
        for keyword in CATEGORY_KEYWORDS[category]:
            if keyword in stem:
                return category

    return "miscellaneous"


def unique_destination(destination: Path) -> Path:
    if not destination.exists():
        return destination

    counter = 2
    while True:
        candidate = destination.with_name(
            f"{destination.stem}_{counter}{destination.suffix}"
        )
        if not candidate.exists():
            return candidate
        counter += 1


def organize(
    root: Path,
    *,
    dry_run: bool,
    copy_files: bool,
) -> tuple[int, dict[str, int]]:
    source_dir = root / "icon"
    dataviewer_dir = find_dataviewer_folder(root)
    target_root = dataviewer_dir / "icons"

    svg_files = sorted(
        path for path in source_dir.rglob("*")
        if path.is_file() and path.suffix.lower() == ".svg"
    )

    if not svg_files:
        raise FileNotFoundError(f'No SVG files found under "{source_dir}".')

    counts: dict[str, int] = {}
    operation = "COPY" if copy_files else "MOVE"

    for source in svg_files:
        category = categorize(source.name)
        destination_dir = target_root / category
        destination = unique_destination(destination_dir / source.name)

        counts[category] = counts.get(category, 0) + 1
        print(f"{operation}: {source.relative_to(root)} -> {destination.relative_to(root)}")

        if dry_run:
            continue

        destination_dir.mkdir(parents=True, exist_ok=True)

        if copy_files:
            shutil.copy2(source, destination)
        else:
            shutil.move(str(source), str(destination))

    if not dry_run and not copy_files:
        # Remove empty source subdirectories, then source folder if empty.
        for directory in sorted(
            (p for p in source_dir.rglob("*") if p.is_dir()),
            key=lambda p: len(p.parts),
            reverse=True,
        ):
            try:
                directory.rmdir()
            except OSError:
                pass

        try:
            source_dir.rmdir()
        except OSError:
            pass

    return len(svg_files), counts


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Move Lucide SVG icons into categorized DataViewer2 folders."
    )
    parser.add_argument(
        "--root",
        help="Path to SeisWebLogFolder. Defaults to the script folder.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show planned changes without moving or copying files.",
    )
    parser.add_argument(
        "--copy",
        action="store_true",
        help="Copy icons instead of moving them.",
    )
    args = parser.parse_args()

    try:
        root = find_project_root(args.root)
        total, counts = organize(
            root,
            dry_run=args.dry_run,
            copy_files=args.copy,
        )
    except (FileNotFoundError, PermissionError, OSError) as exc:
        print(f"\nERROR: {exc}")
        return 1

    print("\nSummary")
    print("-" * 40)
    for category, count in sorted(counts.items()):
        print(f"{category:18} {count:5}")
    print("-" * 40)
    print(f"{'Total':18} {total:5}")

    if args.dry_run:
        print("\nDry run only. No files were changed.")
    elif args.copy:
        print("\nIcons copied successfully.")
    else:
        print("\nIcons moved successfully.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
