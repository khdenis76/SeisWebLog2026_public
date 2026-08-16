import os
import sys
import shutil
import zipfile
import requests
import urllib3
from datetime import datetime
from pathlib import Path

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REMOTE_VERSION_URL = "https://raw.githubusercontent.com/khdenis76/SeisWebLog2026_public/main/version.txt"
REMOTE_ZIP_URL = "https://github.com/khdenis76/SeisWebLog2026_public/archive/refs/heads/main.zip"

PROJECT_ROOT = Path(__file__).resolve().parent
BACKUP_DIR = PROJECT_ROOT / "backup_updates"
TMP_DIR = PROJECT_ROOT / "update_tmp"

INCLUDE_DATA_IN_BACKUP = False

SKIP_DIRS_BACKUP = {
    ".git",".idea","__pycache__","myenv","node_modules","backup_updates","update_tmp",
}

SKIP_DIRS_UPDATE = {
    ".git",".idea","__pycache__","myenv","node_modules",
    "backup_updates","update_tmp","data","logs","media",
}

SKIP_FILE_EXTENSIONS_UPDATE = {".sqlite3",".log"}
# Never overwrite the batch file that is currently driving this Python process.
# cmd.exe reads batch files incrementally, so replacing it mid-run can make it
# resume at the same byte offset in different content and execute a broken line.
SKIP_FILE_NAMES_UPDATE = {".env", "db.sqlite3", "update_project.bat"}
SKIP_FILE_NAMES_BACKUP = {"db.sqlite3"}


def status(message: str) -> None:
    print(message, flush=True)


def format_size(byte_count: int) -> str:
    size = float(byte_count)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def show_download_progress(downloaded: int, total: int) -> None:
    if total > 0:
        percent = min(100.0, downloaded * 100.0 / total)
        filled = min(30, int(percent * 30 / 100))
        bar = "#" * filled + "-" * (30 - filled)
        text = f"\rDownloading update.zip [{bar}] {percent:6.2f}%  {format_size(downloaded)} / {format_size(total)}"
    else:
        text = f"\rDownloading update.zip  {format_size(downloaded)}"
    print(text, end="", flush=True)

def read_local_version() -> str:
    version_file = PROJECT_ROOT / "version.txt"
    if version_file.exists():
        return version_file.read_text(encoding="utf-8").strip()
    return "0.0.0.0"

def get_remote_version() -> str:
    response = requests.get(
        REMOTE_VERSION_URL,
        timeout=60,
        verify=False,
        headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
    )
    response.raise_for_status()
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
    backup_path = BACKUP_DIR / f"backup_{local_version}_{stamp}.zip"

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
    return backup_path

def download_zip(zip_path: Path) -> None:
    with requests.get(
        REMOTE_ZIP_URL,
        stream=True,
        timeout=300,
        verify=False,
        headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
    ) as response:
        response.raise_for_status()
        total = int(response.headers.get("Content-Length", 0) or 0)
        downloaded = 0
        show_download_progress(downloaded, total)
        with open(zip_path, "wb") as fh:
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if chunk:
                    fh.write(chunk)
                    downloaded += len(chunk)
                    show_download_progress(downloaded, total)
        print(flush=True)

def extract_zip(zip_path: Path, extract_to: Path) -> Path:
    if extract_to.exists():
        shutil.rmtree(extract_to)

    extract_to.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.infolist()
        total = len(members)
        for index, member in enumerate(members, 1):
            status(f"Extracting [{index}/{total}]: {member.filename}")
            zf.extract(member, extract_to)

    candidates = [p for p in extract_to.iterdir() if p.is_dir()]
    if not candidates:
        raise RuntimeError("Extracted archive folder not found.")
    return candidates[0]

def copy_update_files(src_root: Path, dst_root: Path) -> None:
    for root, dirs, files in os.walk(src_root):
        root_path = Path(root)
        rel_root = root_path.relative_to(src_root)

        dirs[:] = [d for d in dirs if not should_skip_update_dir(d)]

        target_dir = dst_root / rel_root
        ensure_dir(target_dir)

        for file_name in files:
            if should_skip_update_file(file_name):
                continue

            relative_file = rel_root / file_name
            status(f"Installing: {relative_file.as_posix()}")
            shutil.copy2(root_path / file_name, target_dir / file_name)

def cleanup_tmp() -> None:
    if TMP_DIR.exists():
        shutil.rmtree(TMP_DIR, ignore_errors=True)

def main() -> int:
    try:
        status("Checking for updates...")
        local_version = read_local_version()
        remote_version = get_remote_version()
        status(f"Installed version: {local_version}")
        status(f"Available version: {remote_version}")

        if not is_remote_newer(local_version, remote_version):
            status("You already have the latest version.")
            return 0

        status("Creating backup...")
        backup_path = create_backup_zip()
        status(f"Backup created: {backup_path}")

        ensure_dir(TMP_DIR)
        zip_path = TMP_DIR / "update.zip"

        download_zip(zip_path)
        status("Download complete. Extracting files...")
        extracted_root = extract_zip(zip_path, TMP_DIR / "unzipped")
        status("Installing update files...")
        copy_update_files(extracted_root, PROJECT_ROOT)
        status("Cleaning temporary files...")
        cleanup_tmp()

        status(f"Update completed. Backup: {backup_path}")
        return 0

    except Exception as exc:
        print("Update failed:")
        print(str(exc))
        return 1

if __name__ == "__main__":
    sys.exit(main())
