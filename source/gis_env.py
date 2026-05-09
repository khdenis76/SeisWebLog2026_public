from pathlib import Path
import os
import sys
import subprocess


def find_qgis_root() -> Path | None:
    """
    Auto-detect QGIS / OSGeo4W installation on Windows.
    Returns QGIS root folder or None.
    """

    search_roots = [
        Path(r"C:\Program Files"),
        Path(r"C:\Program Files (x86)"),
        Path(r"C:\OSGeo4W"),
        Path(r"C:\OSGeo4W64"),
    ]

    candidates = []

    for root in search_roots:
        if not root.exists():
            continue

        # Normal QGIS installs:
        # C:\Program Files\QGIS 3.xx
        for p in root.glob("QGIS*"):
            if p.is_dir():
                candidates.append(p)

        # OSGeo4W root install:
        if (root / "bin" / "gdalinfo.exe").exists():
            candidates.append(root)

    # Fallback: search gdalinfo.exe in common folders
    for root in search_roots:
        if not root.exists():
            continue

        try:
            for gdalinfo in root.rglob("gdalinfo.exe"):
                # Usually:
                # C:\Program Files\QGIS 3.xx\bin\gdalinfo.exe
                candidates.append(gdalinfo.parent.parent)
        except PermissionError:
            continue

    if not candidates:
        return None

    # Remove duplicates
    unique = []
    seen = set()

    for c in candidates:
        c = c.resolve()
        if c not in seen:
            unique.append(c)
            seen.add(c)

    # Prefer newest QGIS folder name
    unique = sorted(unique, key=lambda p: str(p).lower())

    return unique[-1]


def configure_qgis_env(qgis_root: str | Path | None = None, verbose: bool = True) -> Path:
    """
    Configure current Python process to use QGIS / GDAL libraries.

    Use before:
        from osgeo import gdal
        import geopandas
        import rasterio
    """

    if qgis_root is None:
        qgis_root = find_qgis_root()

    if qgis_root is None:
        raise FileNotFoundError(
            "QGIS / OSGeo4W installation was not found. "
            "Install QGIS or OSGeo4W, then try again."
        )

    qgis_root = Path(qgis_root)

    bin_paths = [
        qgis_root / "bin",
        qgis_root / "apps" / "qgis" / "bin",
        qgis_root / "apps" / "Qt5" / "bin",
        qgis_root / "apps" / "Qt6" / "bin",
    ]

    python_site_paths = [
        qgis_root / "apps" / "Python311" / "Lib" / "site-packages",
        qgis_root / "apps" / "Python312" / "Lib" / "site-packages",
        qgis_root / "apps" / "Python310" / "Lib" / "site-packages",
        qgis_root / "apps" / "qgis" / "python",
    ]

    # Add DLL folders for Python 3.8+
    if hasattr(os, "add_dll_directory"):
        for p in bin_paths:
            if p.exists():
                os.add_dll_directory(str(p))

    # Update PATH
    path_parts = os.environ.get("PATH", "").split(os.pathsep)

    for p in bin_paths:
        if p.exists() and str(p) not in path_parts:
            os.environ["PATH"] = str(p) + os.pathsep + os.environ.get("PATH", "")

    # Add Python package paths
    for p in python_site_paths:
        if p.exists() and str(p) not in sys.path:
            sys.path.insert(0, str(p))

    # Useful QGIS variables
    os.environ.setdefault("QGIS_PREFIX_PATH", str(qgis_root / "apps" / "qgis"))
    os.environ.setdefault("QT_PLUGIN_PATH", str(qgis_root / "apps" / "qgis" / "qtplugins"))

    if verbose:
        print(f"QGIS root: {qgis_root}")
        print("QGIS/GDAL environment configured.")

    return qgis_root


def test_gdal(verbose: bool = True) -> bool:
    """
    Test if GDAL can be imported.
    """
    try:
        from osgeo import gdal

        if verbose:
            print("GDAL import OK")
            print("GDAL version:", gdal.VersionInfo())

        return True

    except Exception as exc:
        if verbose:
            print("GDAL import FAILED")
            print(exc)

        return False


def test_gdalinfo(verbose: bool = True) -> bool:
    """
    Test if gdalinfo.exe is available from PATH.
    """
    try:
        result = subprocess.run(
            ["gdalinfo", "--version"],
            capture_output=True,
            text=True,
            check=False,
        )

        if verbose:
            print(result.stdout.strip() or result.stderr.strip())

        return result.returncode == 0

    except Exception as exc:
        if verbose:
            print("gdalinfo test FAILED")
            print(exc)

        return False


if __name__ == "__main__":
    root = configure_qgis_env(verbose=True)
    test_gdal(verbose=True)
    test_gdalinfo(verbose=True)