import os
import sys
import shutil
import zipfile
import requests
from datetime import datetime
from pathlib import Path

REMOTE_VERSION_URL = "https://raw.githubusercontent.com/khdenis76/SeisWebLog2026_public/main/version.txt"
REMOTE_ZIP_URL = "https://github.com/khdenis76/SeisWebLog2026_public/archive/refs/heads/main.zip"

PROJECT_ROOT = Path(__file__).resolve().parent
BACKUP_DIR = PROJECT_ROOT / "backup_updates"
TMP_DIR = PROJECT_ROOT / "update_tmp"

# Backup is for project code/files only.
# NEVER touch Django DB (db.sqlite3).
# data/ is also skipped to avoid backing up large project databases by default.
INCLUDE_DATA_IN_BACKUP = False

SKIP_DIRS_BACKUP = {
    ".git",
    ".idea",
    "__pycache__",
    "myenv",
    "node_modules",
    "backup_updates",
    "update_tmp",
}

SKIP_DIRS_UPDATE = {
    ".git",
    ".idea",
    "__pycache__",
    "myenv",
    "node_modules",
    "backup_updates",
    "update_tmp",
    "data",
    "logs",
    "media",
}

SKIP_FILE_EXTENSIONS_UPDATE = {
    ".sqlite3",
    ".log",
}

SKIP_FILE_NAMES_UPDATE = {
    ".env",
    "db.sqlite3",
}

SKIP_FILE_NAMES_BACKUP = {
    "db.sqlite3",
}

def read_local_version() -> str:
    version_file = PROJECT_ROOT / "version.txt"
    if version_file.exists():
        return version_file.read_text(encoding="utf-8").strip()
    return "0.0.0.0"

def get_remote_version() -> str:
    print(f"REMOTE_VERSION_URL = {REMOTE_VERSION_URL}")
    response = requests.get(
        REMOTE_VERSION_URL,
        timeout=20,
        headers={
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )
    response.raise_for_status()
    print(f"HTTP status = {response.status_code}")
    print(f"DEBUG remote raw = [{response.text}]")
    return response.text.strip()

def parse_version(version_text: str) -> list[int]:
    parts = []
    for part in version_text.strip().split("."):
        try:
            parts.append(int(part))
        except ValueError:
            parts.append(0)
    return parts

def normalize_version(version_text: str, length: int = 4) -> list[int]:
    parts = parse_version(version_text)
    while len(parts) < length:
        parts.append(0)
    return parts

def is_remote_newer(local_version: str, remote_version: str) -> bool:
    return normalize_version(remote_version) > normalize_version(local_version)

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def should_skip_backup_dir(dirname: str) -> bool:
    return dirname in SKIP_DIRS_BACKUP

def should_skip_update_dir(dirname: str) -> bool:
    return dirname in SKIP_DIRS_UPDATE

def should_skip_update_file(filename: str) -> bool:
    if filename in SKIP_FILE_NAMES_UPDATE:
        return True
    return Path(filename).suffix.lower() in SKIP_FILE_EXTENSIONS_UPDATE

def create_backup_zip() -> Path:
    ensure_dir(BACKUP_DIR)

    local_version = read_local_version().replace(" ", "_")
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"backup_{local_version}_{stamp}.zip"
    backup_path = BACKUP_DIR / backup_name

    print(f"[1/4] Creating backup: {backup_path}")

    with zipfile.ZipFile(backup_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(PROJECT_ROOT):
            root_path = Path(root)
            dirs[:] = [d for d in dirs if not should_skip_backup_dir(d)]

            if not INCLUDE_DATA_IN_BACKUP and "data" in dirs:
                dirs.remove("data")

            for file_name in files:
                if file_name in SKIP_FILE_NAMES_BACKUP:
                    continue

                file_path = root_path / file_name
                rel_file = file_path.relative_to(PROJECT_ROOT)

                if "backup_updates" in rel_file.parts or "update_tmp" in rel_file.parts:
                    continue

                zf.write(file_path, rel_file.as_posix())

    print("[OK] Backup created.")
    return backup_path

def download_zip(zip_path: Path) -> None:
    print("[2/4] Downloading update archive...")
    with requests.get(
        REMOTE_ZIP_URL,
        stream=True,
        timeout=60,
        headers={
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    ) as response:
        response.raise_for_status()
        with open(zip_path, "wb") as fh:
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if chunk:
                    fh.write(chunk)
    print("[OK] Download complete.")

def extract_zip(zip_path: Path, extract_to: Path) -> Path:
    print("[3/4] Extracting archive...")

    if extract_to.exists():
        shutil.rmtree(extract_to)

    extract_to.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_to)

    candidates = [p for p in extract_to.iterdir() if p.is_dir()]
    if not candidates:
        raise RuntimeError("Extracted archive folder not found.")

    print("[OK] Extract complete.")
    return candidates[0]

def copy_update_files(src_root: Path, dst_root: Path) -> None:
    print("[4/4] Applying update...")

    for root, dirs, files in os.walk(src_root):
        root_path = Path(root)
        rel_root = root_path.relative_to(src_root)

        dirs[:] = [d for d in dirs if not should_skip_update_dir(d)]

        target_dir = dst_root / rel_root
        ensure_dir(target_dir)

        for file_name in files:
            if should_skip_update_file(file_name):
                continue

            src_file = root_path / file_name
            dst_file = target_dir / file_name

            ensure_dir(dst_file.parent)
            shutil.copy2(src_file, dst_file)

    print("[OK] Update applied.")

def cleanup_tmp() -> None:
    if TMP_DIR.exists():
        shutil.rmtree(TMP_DIR, ignore_errors=True)

def main() -> int:
    try:
        print("=" * 60)
        print("SeisWebLog Updater")
        print("=" * 60)

        local_version = read_local_version()
        print(f"Local version : {local_version}")

        remote_version = get_remote_version()
        print(f"Remote version: {remote_version}")

        print(f"Parsed local  = {normalize_version(local_version)}")
        print(f"Parsed remote = {normalize_version(remote_version)}")
        print(f"is_remote_newer = {is_remote_newer(local_version, remote_version)}")

        if not is_remote_newer(local_version, remote_version):
            print("You already have the latest version.")
            return 0

        backup_path = create_backup_zip()

        ensure_dir(TMP_DIR)
        zip_path = TMP_DIR / "update.zip"

        download_zip(zip_path)
        extracted_root = extract_zip(zip_path, TMP_DIR / "unzipped")
        copy_update_files(extracted_root, PROJECT_ROOT)
        cleanup_tmp()

        print()
        print("Update completed successfully.")
        print(f"Backup saved to: {backup_path}")
        print("If needed, you can roll back later with restore_project.bat")
        return 0

    except Exception as exc:
        print()
        print("Update failed:")
        print(str(exc))
        print("Project files were not deleted.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
