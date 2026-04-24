import sys
import shutil
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
BACKUP_DIR = PROJECT_ROOT / "backup_updates"

# NEVER touch Django DB.
# db.sqlite3 must survive restore.
SKIP_DIRS_RESTORE = {
    ".git",
    ".idea",
    "__pycache__",
    "myenv",
    "node_modules",
    "backup_updates",
    "update_tmp",
    "logs",
    "media",
    "data",
}

SKIP_FILE_NAMES_RESTORE = {
    "db.sqlite3",
}

def list_backups() -> list[Path]:
    if not BACKUP_DIR.exists():
        return []
    return sorted(BACKUP_DIR.glob("backup_*.zip"), reverse=True)

def select_backup(backups: list[Path]) -> Path:
    print("Available backups:")
    for index, backup_file in enumerate(backups, start=1):
        print(f"{index}. {backup_file.name}")

    print()
    choice = input("Select backup number to restore: ").strip()
    selected_index = int(choice) - 1

    if selected_index < 0 or selected_index >= len(backups):
        raise ValueError("Invalid backup selection.")

    return backups[selected_index]

def clear_project_files() -> None:
    for item in PROJECT_ROOT.iterdir():
        if item.name in SKIP_DIRS_RESTORE:
            continue
        if item.name in SKIP_FILE_NAMES_RESTORE:
            continue

        if item.is_dir():
            shutil.rmtree(item, ignore_errors=True)
        else:
            try:
                item.unlink()
            except Exception:
                pass

def restore_from_zip(zip_path: Path) -> None:
    print(f"Restoring from {zip_path.name} ...")
    clear_project_files()

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = [m for m in zf.namelist() if Path(m).name not in SKIP_FILE_NAMES_RESTORE]
        for member in members:
            zf.extract(member, PROJECT_ROOT)

    print("Restore completed.")

def main() -> int:
    backups = list_backups()
    if not backups:
        print("No backup archives found in backup_updates.")
        return 1

    try:
        backup = select_backup(backups)
        restore_from_zip(backup)
        return 0
    except Exception as exc:
        print(f"Restore failed: {exc}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
