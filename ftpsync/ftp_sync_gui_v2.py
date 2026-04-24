import os
import json
import time
import fnmatch
import traceback
import calendar
import threading
import uuid
import shutil
from dataclasses import fields
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple

from PySide6 import QtCore, QtWidgets, QtGui

from ftplib import FTP, FTP_TLS

try:
    import paramiko
except Exception:
    paramiko = None

# Always store config next to this script (not where BAT is launched)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "ftp_sync_config.json")


# ----------------------------
# Models
# ----------------------------
@dataclass
class ServerProfile:
    name: str
    protocol: str = "ftp"  # ftp | ftps | sftp
    host: str = ""
    port: int = 21
    user: str = "anonymous"
    password: str = ""
    passive: bool = True
    timeout: int = 30

    # FTP MDTM timezone handling: shift remote MDTM to UTC epoch (minutes).
    # Example: server MDTM is local UTC+3 -> tz_offset_min = -180
    tz_offset_min: int = 0


@dataclass
class JobGroup:
    name: str
    color: str = "#2E86FF"
    enabled: bool = True

    # schedule defaults for jobs in this group (if job inherits schedule)
    schedule_mode: str = "manual"  # manual | once | daily | interval
    run_at: str = ""               # "YYYY-MM-DD HH:MM"
    interval_min: int = 0          # for interval mode


@dataclass
class SyncJob:
    id: str = ""
    group: str = "Default"
    enabled: bool = True

    server_name: str = ""
    remote_dir: str = "/"
    mask: str = "*.*"
    local_dir: str = ""
    direction: str = "download"  # download | upload | both
    recursive: bool = True

    new_only: bool = False  # download missing only (ignore changed)

    # scheduling
    inherit_group_schedule: bool = True
    schedule_mode: str = "manual"  # manual | once | daily | interval
    run_at: str = ""               # "YYYY-MM-DD HH:MM"
    interval_min: int = 0

    # monitor polling (keeps connection open and repeats cycle)
    monitor: bool = False
    interval_sec: int = 60


@dataclass
class RemoteEntry:
    name: str
    is_dir: bool
    size: Optional[int] = None
    mtime: Optional[int] = None  # epoch seconds UTC


# ----------------------------
# Persistence
# ----------------------------
def _dc_filter_kwargs(dc_cls, d: dict) -> dict:
    allowed = {f.name for f in fields(dc_cls)}
    return {k: v for k, v in (d or {}).items() if k in allowed}
from datetime import datetime
import shutil
from dataclasses import fields

def _dc_filter_kwargs(dc_cls, d: dict) -> dict:
    allowed = {f.name for f in fields(dc_cls)}
    return {k: v for k, v in (d or {}).items() if k in allowed}

def load_config():
    # ALWAYS return: servers, jobs, groups, concurrency
    default_groups = [JobGroup(name="Default", color="#2E86FF", enabled=True)]

    if not os.path.exists(CONFIG_FILE):
        return [], [], default_groups, 2

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        if not raw:
            raise ValueError("Config is empty")
        data = json.loads(raw)
    except Exception:
        # backup broken config and start clean
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        bad_path = CONFIG_FILE + f".broken_{ts}"
        try:
            shutil.copy2(CONFIG_FILE, bad_path)
        except Exception:
            pass
        return [], [], default_groups, 2

    servers = [ServerProfile(**_dc_filter_kwargs(ServerProfile, x)) for x in data.get("servers", [])]

    groups = [JobGroup(**_dc_filter_kwargs(JobGroup, x)) for x in data.get("groups", [])]
    if not groups:
        groups = default_groups

    jobs = []
    for x in data.get("jobs", []):
        j = SyncJob(**_dc_filter_kwargs(SyncJob, x))
        if not getattr(j, "id", ""):
            j.id = str(uuid.uuid4())
        if not getattr(j, "group", ""):
            j.group = "Default"
        jobs.append(j)

    concurrency = int(data.get("concurrency", 2))
    return servers, jobs, groups, max(1, concurrency)


def save_config(servers: List[ServerProfile], jobs: List[SyncJob], groups: List[JobGroup], concurrency: int) -> None:
    data = {
        "servers": [asdict(s) for s in servers],
        "groups": [asdict(g) for g in groups],
        "jobs": [asdict(j) for j in jobs],
        "concurrency": int(concurrency),
    }
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# ----------------------------
# Helpers
# ----------------------------
def _join_remote(base: str, rel: str) -> str:
    base = base or "/"
    rel = rel.lstrip("/")
    if base.endswith("/"):
        return base + rel
    return base + "/" + rel
def _dc_filter_kwargs(dc_cls, d: dict) -> dict:
    allowed = {f.name for f in fields(dc_cls)}
    return {k: v for k, v in (d or {}).items() if k in allowed}

def parse_run_at(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M")
    except Exception:
        return None


def fmt_schedule(mode: str, run_at: str, interval_min: int) -> str:
    mode = (mode or "manual").lower()
    if mode == "manual":
        return "manual"
    if mode in ("once", "daily"):
        return f"{mode} {run_at or '—'}"
    if mode == "interval":
        return f"every {interval_min} min" if interval_min else "interval —"
    return mode


def status_color_hex(state: str) -> str:
    return {
        "idle": "#9aa0a6",
        "queued": "#b0b4b8",
        "running": "#2E86FF",
        "ok": "#2ecc71",
        "error": "#e74c3c",
        "stopped": "#f1c40f",
    }.get(state, "#9aa0a6")


def make_dot_item(color_hex: str) -> QtWidgets.QTableWidgetItem:
    it = QtWidgets.QTableWidgetItem("●")
    it.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
    it.setFlags(it.flags() & ~QtCore.Qt.ItemFlag.ItemIsEditable)
    it.setForeground(QtGui.QBrush(QtGui.QColor(color_hex)))
    return it


def ensure_default_group(groups: List[JobGroup]) -> None:
    if not any(g.name == "Default" for g in groups):
        groups.insert(0, JobGroup(name="Default", color="#2E86FF", enabled=True))


# ----------------------------
# Remote client abstraction
# ----------------------------
class RemoteClientBase:
    def connect(self): ...
    def close(self): ...
    def list_dir(self, path: str) -> List[RemoteEntry]: ...
    def download(self, remote_path: str, local_path: str, progress_cb=None): ...
    def upload(self, local_path: str, remote_path: str, progress_cb=None): ...
    def ensure_dir(self, path: str): ...
    def stat(self, path: str) -> Optional[RemoteEntry]: ...


# ----------------------------
# FTP/FTPS client
# ----------------------------
class FtpClient(RemoteClientBase):
    def __init__(self, prof: ServerProfile):
        self.p = prof
        self.ftp = None
        self.protocol = prof.protocol.lower().strip()

    def connect(self):
        ftp = FTP_TLS() if self.protocol == "ftps" else FTP()
        ftp.connect(self.p.host, self.p.port, timeout=self.p.timeout)
        ftp.login(self.p.user, self.p.password)
        if self.protocol == "ftps":
            ftp.prot_p()
        ftp.set_pasv(self.p.passive)
        self.ftp = ftp

    def close(self):
        try:
            if self.ftp:
                self.ftp.quit()
        except Exception:
            try:
                if self.ftp:
                    self.ftp.close()
            except Exception:
                pass
        self.ftp = None

    def _cwd(self, path: str):
        self.ftp.cwd(path or "/")

    def _mdtm_epoch(self, name: str) -> Optional[int]:
        try:
            resp = self.ftp.sendcmd(f"MDTM {name}")  # 213 YYYYMMDDHHMMSS
            parts = resp.split()
            if len(parts) >= 2 and parts[0] == "213":
                s = parts[1].strip()
                if len(s) == 14:
                    tt = time.strptime(s, "%Y%m%d%H%M%S")
                    epoch = calendar.timegm(tt)
                    epoch += int(self.p.tz_offset_min) * 60
                    return int(epoch)
        except Exception:
            return None
        return None

    def _size(self, name: str) -> Optional[int]:
        try:
            return self.ftp.size(name)
        except Exception:
            return None

    def list_dir(self, path: str) -> List[RemoteEntry]:
        self._cwd(path)

        # best: MLSD
        out: List[RemoteEntry] = []
        try:
            for name, facts in self.ftp.mlsd():
                if name in (".", ".."):
                    continue
                t = facts.get("type", "")
                is_dir = (t == "dir")
                size = None
                mtime = None

                if not is_dir and "size" in facts:
                    try:
                        size = int(facts["size"])
                    except Exception:
                        size = None

                if "modify" in facts:
                    s = facts["modify"]
                    if len(s) == 14:
                        try:
                            tt = time.strptime(s, "%Y%m%d%H%M%S")
                            mtime = calendar.timegm(tt) + int(self.p.tz_offset_min) * 60
                        except Exception:
                            mtime = None

                out.append(RemoteEntry(name=name, is_dir=is_dir, size=size, mtime=mtime))
            return out
        except Exception:
            pass

        # fallback: NLST then try cwd to detect dir (slower)
        try:
            names = self.ftp.nlst()
        except Exception:
            names = []

        cur = self.ftp.pwd()
        for n in names:
            if n in (".", ".."):
                continue
            is_dir = False
            try:
                self.ftp.cwd(n)
                is_dir = True
                self.ftp.cwd(cur)
            except Exception:
                is_dir = False

            if is_dir:
                out.append(RemoteEntry(name=n, is_dir=True))
            else:
                out.append(RemoteEntry(name=n, is_dir=False, size=self._size(n), mtime=self._mdtm_epoch(n)))

        return out

    def ensure_dir(self, path: str):
        if not path or path == "/":
            return
        parts = [p for p in path.split("/") if p]
        cur = "/"
        for part in parts:
            nxt = cur + part if cur.endswith("/") else cur + "/" + part
            try:
                self.ftp.cwd(nxt)
            except Exception:
                try:
                    self.ftp.mkd(nxt)
                except Exception:
                    pass
            cur = nxt + "/"

    def stat(self, path: str) -> Optional[RemoteEntry]:
        try:
            d, name = os.path.split(path.rstrip("/"))
            if not d:
                d = "/"
            self._cwd(d)

            # test directory
            cur = self.ftp.pwd()
            try:
                self.ftp.cwd(name)
                self.ftp.cwd(cur)
                return RemoteEntry(name=name, is_dir=True)
            except Exception:
                return RemoteEntry(name=name, is_dir=False, size=self._size(name), mtime=self._mdtm_epoch(name))
        except Exception:
            return None

    def download(self, remote_path: str, local_path: str, progress_cb=None):
        d, name = os.path.split(remote_path.rstrip("/"))
        if not d:
            d = "/"
        self._cwd(d)

        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            def _write(chunk: bytes):
                f.write(chunk)
                if progress_cb:
                    # FTP download uses delta-only
                    progress_cb(len(chunk), None, None)

            self.ftp.retrbinary(f"RETR {name}", _write, blocksize=64 * 1024)

        m = self._mdtm_epoch(name)
        if m is not None:
            try:
                os.utime(local_path, (m, m))
            except Exception:
                pass

    def upload(self, local_path: str, remote_path: str, progress_cb=None):
        d, name = os.path.split(remote_path.rstrip("/"))
        if not d:
            d = "/"
        self.ensure_dir(d)
        self._cwd(d)

        total = os.path.getsize(local_path)
        done = 0

        def _cb(block: bytes):
            nonlocal done
            done += len(block)
            if progress_cb:
                # FTP upload provides delta + done/total
                progress_cb(len(block), done, total)

        with open(local_path, "rb") as f:
            self.ftp.storbinary(f"STOR {name}", f, blocksize=64 * 1024, callback=_cb)


# ----------------------------
# SFTP client (Paramiko)
# ----------------------------
class SftpClient(RemoteClientBase):
    def __init__(self, prof: ServerProfile):
        if paramiko is None:
            raise RuntimeError("Paramiko is not installed. Run: pip install paramiko")
        self.p = prof
        self.transport = None
        self.sftp = None

    def connect(self):
        t = paramiko.Transport((self.p.host, self.p.port))
        t.banner_timeout = self.p.timeout
        t.auth_timeout = self.p.timeout
        t.connect(username=self.p.user, password=self.p.password)
        self.transport = t
        self.sftp = paramiko.SFTPClient.from_transport(t)

    def close(self):
        try:
            if self.sftp:
                self.sftp.close()
        except Exception:
            pass
        try:
            if self.transport:
                self.transport.close()
        except Exception:
            pass
        self.sftp = None
        self.transport = None

    def list_dir(self, path: str) -> List[RemoteEntry]:
        path = path or "/"
        out: List[RemoteEntry] = []
        for a in self.sftp.listdir_attr(path):
            is_dir = bool(a.st_mode & 0o040000)
            out.append(RemoteEntry(name=a.filename, is_dir=is_dir, size=int(a.st_size), mtime=int(a.st_mtime)))
        return out

    def ensure_dir(self, path: str):
        if not path or path in ("/", "."):
            return
        parts = [p for p in path.split("/") if p]
        cur = "/"
        for part in parts:
            nxt = cur + part if cur.endswith("/") else cur + "/" + part
            try:
                self.sftp.stat(nxt)
            except Exception:
                try:
                    self.sftp.mkdir(nxt)
                except Exception:
                    pass
            cur = nxt + "/"

    def stat(self, path: str) -> Optional[RemoteEntry]:
        try:
            s = self.sftp.stat(path)
            is_dir = bool(s.st_mode & 0o040000)
            return RemoteEntry(
                name=os.path.basename(path.rstrip("/")),
                is_dir=is_dir,
                size=int(getattr(s, "st_size", 0)),
                mtime=int(getattr(s, "st_mtime", 0)),
            )
        except Exception:
            return None

    def download(self, remote_path: str, local_path: str, progress_cb=None):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        def cb(transferred, total):
            # absolute totals
            if progress_cb:
                progress_cb(None, int(transferred), int(total))

        self.sftp.get(remote_path, local_path, callback=cb)

        try:
            st = self.sftp.stat(remote_path)
            os.utime(local_path, (int(st.st_mtime), int(st.st_mtime)))
        except Exception:
            pass

    def upload(self, local_path: str, remote_path: str, progress_cb=None):
        d = os.path.dirname(remote_path.rstrip("/")) or "/"
        self.ensure_dir(d)

        def cb(transferred, total):
            if progress_cb:
                progress_cb(None, int(transferred), int(total))

        self.sftp.put(local_path, remote_path, callback=cb)


def make_client(profile: ServerProfile) -> RemoteClientBase:
    proto = (profile.protocol or "ftp").lower().strip()
    if proto == "sftp":
        return SftpClient(profile)
    return FtpClient(profile)


# ----------------------------
# Sync logic
# ----------------------------
def walk_remote(client: RemoteClientBase, base_dir: str, recursive: bool) -> List[Tuple[str, RemoteEntry]]:
    """
    Returns list of (relative_path, RemoteEntry) for files only.
    """
    results: List[Tuple[str, RemoteEntry]] = []

    def _walk(cur_dir: str, rel_prefix: str):
        entries = client.list_dir(cur_dir)
        for e in entries:
            if e.name in (".", ".."):
                continue
            rpath = _join_remote(cur_dir, e.name)
            rel = e.name if not rel_prefix else f"{rel_prefix}/{e.name}"
            if e.is_dir:
                if recursive:
                    _walk(rpath, rel)
            else:
                results.append((rel, e))

    _walk(base_dir, "")
    return results


def local_walk(base_dir: str, recursive: bool) -> List[str]:
    out: List[str] = []
    if recursive:
        for root, _, files in os.walk(base_dir):
            for f in files:
                rel = os.path.relpath(os.path.join(root, f), base_dir).replace("\\", "/")
                out.append(rel)
    else:
        for f in os.listdir(base_dir):
            p = os.path.join(base_dir, f)
            if os.path.isfile(p):
                out.append(f)
    return out


def should_transfer(remote_e: Optional[RemoteEntry], local_path: str) -> bool:
    if not os.path.exists(local_path):
        return True
    if remote_e is None:
        return False
    try:
        ls = os.path.getsize(local_path)
        lm = int(os.path.getmtime(local_path))
    except Exception:
        return True

    if remote_e.size is not None and remote_e.size != ls:
        return True
    if remote_e.mtime is not None and remote_e.mtime > lm + 1:
        return True
    return False


def should_download(remote_e: Optional[RemoteEntry], local_path: str, new_only: bool) -> bool:
    if new_only:
        return not os.path.exists(local_path)
    return should_transfer(remote_e, local_path)


def should_upload(local_path: str, remote_e: Optional[RemoteEntry]) -> bool:
    if remote_e is None:
        return True
    try:
        ls = os.path.getsize(local_path)
        lm = int(os.path.getmtime(local_path))
    except Exception:
        return True

    if remote_e.size is not None and remote_e.size != ls:
        return True
    if remote_e.mtime is not None and lm > remote_e.mtime + 1:
        return True
    return False


# ----------------------------
# Remote folder picker (tree)
# ----------------------------
class RemoteFolderPicker(QtWidgets.QDialog):
    def __init__(self, parent, profile: ServerProfile):
        super().__init__(parent)
        self.setWindowTitle("Select Remote Folder")
        self.resize(620, 480)
        self.profile = profile
        self.client: Optional[RemoteClientBase] = None

        self.tree = QtWidgets.QTreeWidget()
        self.tree.setHeaderLabels(["Remote folders"])
        self.tree.itemExpanded.connect(self._on_expand)

        self.status = QtWidgets.QLabel("Connecting...")

        btns = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.StandardButton.Ok |
            QtWidgets.QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addWidget(self.status)
        layout.addWidget(self.tree, 1)
        layout.addWidget(btns)

        self._connect_and_load()

    def _connect_and_load(self):
        try:
            self.client = make_client(self.profile)
            self.client.connect()
            self.status.setText(f"Connected: {self.profile.name}")

            self.tree.clear()
            root_path = "/"
            root = QtWidgets.QTreeWidgetItem([root_path])
            root.setData(0, QtCore.Qt.ItemDataRole.UserRole, root_path)
            root.addChild(QtWidgets.QTreeWidgetItem(["(loading...)"]))
            self.tree.addTopLevelItem(root)
            root.setExpanded(True)
        except Exception as e:
            self.status.setText("Failed to connect")
            QtWidgets.QMessageBox.critical(self, "Error", str(e))

    def _on_expand(self, item: QtWidgets.QTreeWidgetItem):
        if not self.client:
            return
        path = item.data(0, QtCore.Qt.ItemDataRole.UserRole)
        if not path:
            return

        # if placeholder exists -> load children
        if item.childCount() == 1 and item.child(0).text(0) == "(loading...)":
            item.takeChild(0)
        elif item.childCount() > 0:
            return

        try:
            entries = self.client.list_dir(path)
            dirs = [e for e in entries if e.is_dir and e.name not in (".", "..")]
            dirs.sort(key=lambda x: x.name.lower())
            for d in dirs:
                child_path = _join_remote(path, d.name)
                child = QtWidgets.QTreeWidgetItem([d.name])
                child.setData(0, QtCore.Qt.ItemDataRole.UserRole, child_path)
                child.addChild(QtWidgets.QTreeWidgetItem(["(loading...)"]))
                item.addChild(child)
        except Exception:
            pass

    def selected_folder(self) -> str:
        it = self.tree.currentItem()
        if not it:
            return "/"
        path = it.data(0, QtCore.Qt.ItemDataRole.UserRole)
        return path or "/"

    def closeEvent(self, event):
        try:
            if self.client:
                self.client.close()
        except Exception:
            pass
        event.accept()


# ----------------------------
# Dialogs: Server / Group / Job
# ----------------------------
class ServerDialog(QtWidgets.QDialog):
    def __init__(self, parent=None, server: Optional[ServerProfile] = None):
        super().__init__(parent)
        self.setWindowTitle("Server Profile")
        self.setModal(True)

        self.name = QtWidgets.QLineEdit()
        self.protocol = QtWidgets.QComboBox()
        self.protocol.addItems(["ftp", "ftps", "sftp"])

        self.host = QtWidgets.QLineEdit()
        self.port = QtWidgets.QSpinBox()
        self.port.setRange(1, 65535)
        self.port.setValue(21)

        self.user = QtWidgets.QLineEdit()
        self.password = QtWidgets.QLineEdit()
        self.password.setEchoMode(QtWidgets.QLineEdit.EchoMode.Password)

        self.passive = QtWidgets.QCheckBox("Passive mode (FTP/FTPS)")
        self.passive.setChecked(True)

        self.timeout = QtWidgets.QSpinBox()
        self.timeout.setRange(1, 9999)
        self.timeout.setValue(30)

        self.tz_offset = QtWidgets.QSpinBox()
        self.tz_offset.setRange(-24 * 60, 24 * 60)
        self.tz_offset.setValue(0)
        self.tz_offset.setSuffix(" min")
        self.tz_hint = QtWidgets.QLabel("FTP tz offset: keep 0 normally. Use only if MDTM times are wrong.")

        form = QtWidgets.QFormLayout()
        form.addRow("Name:", self.name)
        form.addRow("Protocol:", self.protocol)
        form.addRow("Host:", self.host)
        form.addRow("Port:", self.port)
        form.addRow("User:", self.user)
        form.addRow("Password:", self.password)
        form.addRow("", self.passive)
        form.addRow("Timeout (s):", self.timeout)
        form.addRow("FTP tz offset:", self.tz_offset)
        form.addRow("", self.tz_hint)

        btns = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.StandardButton.Ok |
            QtWidgets.QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(btns)

        self.protocol.currentTextChanged.connect(self._proto_changed)
        self._proto_changed(self.protocol.currentText())

        if server:
            self.name.setText(server.name)
            self.protocol.setCurrentText(server.protocol)
            self.host.setText(server.host)
            self.port.setValue(server.port)
            self.user.setText(server.user)
            self.password.setText(server.password)
            self.passive.setChecked(server.passive)
            self.timeout.setValue(server.timeout)
            self.tz_offset.setValue(server.tz_offset_min)
            self._proto_changed(server.protocol)

    def _proto_changed(self, p: str):
        p = (p or "").lower()
        is_ftp = p in ("ftp", "ftps")
        self.passive.setEnabled(is_ftp)
        self.tz_offset.setEnabled(is_ftp)
        if p == "sftp" and self.port.value() == 21:
            self.port.setValue(22)
        if is_ftp and self.port.value() == 22:
            self.port.setValue(21)

    def get_value(self) -> ServerProfile:
        return ServerProfile(
            name=self.name.text().strip(),
            protocol=self.protocol.currentText().strip(),
            host=self.host.text().strip(),
            port=int(self.port.value()),
            user=self.user.text().strip(),
            password=self.password.text(),
            passive=bool(self.passive.isChecked()),
            timeout=int(self.timeout.value()),
            tz_offset_min=int(self.tz_offset.value()),
        )


class GroupDialog(QtWidgets.QDialog):
    def __init__(self, parent=None, group: Optional[JobGroup] = None):
        super().__init__(parent)
        self.setWindowTitle("Group")
        self.setModal(True)

        self.name = QtWidgets.QLineEdit()
        self.enabled = QtWidgets.QCheckBox("Enabled")
        self.enabled.setChecked(True)

        self.color_btn = QtWidgets.QPushButton("Pick color…")
        self.color_preview = QtWidgets.QLabel("     ")
        self.color_preview.setAutoFillBackground(True)
        self._color = "#2E86FF"

        self.schedule_mode = QtWidgets.QComboBox()
        self.schedule_mode.addItems(["manual", "once", "daily", "interval"])

        self.run_at = QtWidgets.QDateTimeEdit()
        self.run_at.setCalendarPopup(True)
        self.run_at.setDisplayFormat("yyyy-MM-dd HH:mm")
        self.run_at.setDateTime(QtCore.QDateTime.currentDateTime())

        self.interval_min = QtWidgets.QSpinBox()
        self.interval_min.setRange(0, 999999)
        self.interval_min.setValue(0)

        self.color_btn.clicked.connect(self._pick_color)
        self.schedule_mode.currentTextChanged.connect(self._schedule_changed)
        self._schedule_changed(self.schedule_mode.currentText())

        row_color = QtWidgets.QHBoxLayout()
        row_color.addWidget(self.color_btn)
        row_color.addWidget(self.color_preview)
        row_color.addStretch(1)

        form = QtWidgets.QFormLayout()
        form.addRow("Name:", self.name)
        form.addRow("", self.enabled)
        form.addRow("Color:", row_color)
        form.addRow("Schedule:", self.schedule_mode)
        form.addRow("Run at:", self.run_at)
        form.addRow("Interval (min):", self.interval_min)

        btns = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.StandardButton.Ok |
            QtWidgets.QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(btns)

        if group:
            self.name.setText(group.name)
            self.enabled.setChecked(group.enabled)
            self._set_color(group.color)
            self.schedule_mode.setCurrentText(group.schedule_mode or "manual")
            dt = parse_run_at(group.run_at)
            if dt:
                self.run_at.setDateTime(QtCore.QDateTime(dt))
            self.interval_min.setValue(int(group.interval_min or 0))
            self._schedule_changed(self.schedule_mode.currentText())

    def _set_color(self, color: str):
        self._color = color or "#2E86FF"
        pal = self.color_preview.palette()
        pal.setColor(QtGui.QPalette.ColorRole.Window, QtGui.QColor(self._color))
        self.color_preview.setPalette(pal)

    def _pick_color(self):
        c = QtWidgets.QColorDialog.getColor(QtGui.QColor(self._color), self, "Select color")
        if c.isValid():
            self._set_color(c.name())

    def _schedule_changed(self, mode: str):
        mode = (mode or "manual").lower()
        self.run_at.setEnabled(mode in ("once", "daily"))
        self.interval_min.setEnabled(mode == "interval")

    def get_value(self) -> JobGroup:
        run_at_str = ""
        if self.schedule_mode.currentText() in ("once", "daily"):
            run_at_str = self.run_at.dateTime().toString("yyyy-MM-dd HH:mm")

        return JobGroup(
            name=self.name.text().strip(),
            color=self._color,
            enabled=bool(self.enabled.isChecked()),
            schedule_mode=self.schedule_mode.currentText(),
            run_at=run_at_str,
            interval_min=int(self.interval_min.value()),
        )


class JobDialog(QtWidgets.QDialog):
    def __init__(self, parent=None, servers: List[ServerProfile] = None, groups: List[JobGroup] = None, job: Optional[SyncJob] = None):
        super().__init__(parent)
        self.setWindowTitle("Sync Job")
        self.setModal(True)
        self.servers = servers or []
        self.groups = groups or []

        self.enabled = QtWidgets.QCheckBox("Enabled")
        self.enabled.setChecked(True)

        self.group = QtWidgets.QComboBox()
        self.group.addItems([g.name for g in self.groups] or ["Default"])

        self.server = QtWidgets.QComboBox()
        self.server.addItems([s.name for s in self.servers])

        self.remote_dir = QtWidgets.QLineEdit("/")
        self.pick_remote = QtWidgets.QPushButton("Pick remote…")

        self.mask = QtWidgets.QLineEdit("*.*")

        self.local_dir = QtWidgets.QLineEdit()
        self.pick_local = QtWidgets.QPushButton("Browse…")

        self.direction = QtWidgets.QComboBox()
        self.direction.addItems(["download", "upload", "both"])

        self.recursive = QtWidgets.QCheckBox("Recursive (include subfolders)")
        self.recursive.setChecked(True)

        self.new_only = QtWidgets.QCheckBox("Download only NEW files (missing only)")
        self.new_only.setChecked(False)

        # Scheduling
        self.inherit_group_schedule = QtWidgets.QCheckBox("Inherit group schedule")
        self.inherit_group_schedule.setChecked(True)

        self.schedule_mode = QtWidgets.QComboBox()
        self.schedule_mode.addItems(["manual", "once", "daily", "interval"])

        self.run_at = QtWidgets.QDateTimeEdit()
        self.run_at.setCalendarPopup(True)
        self.run_at.setDisplayFormat("yyyy-MM-dd HH:mm")
        self.run_at.setDateTime(QtCore.QDateTime.currentDateTime())

        self.interval_min = QtWidgets.QSpinBox()
        self.interval_min.setRange(0, 999999)
        self.interval_min.setValue(0)

        # Monitor polling
        self.monitor = QtWidgets.QCheckBox("Monitor (polling)")
        self.interval_sec = QtWidgets.QSpinBox()
        self.interval_sec.setRange(5, 999999)
        self.interval_sec.setValue(60)

        self.pick_local.clicked.connect(self._pick_local)
        self.pick_remote.clicked.connect(self._pick_remote)

        self.schedule_mode.currentTextChanged.connect(self._schedule_changed)
        self.inherit_group_schedule.toggled.connect(self._inherit_changed)
        self._schedule_changed(self.schedule_mode.currentText())
        self._inherit_changed(self.inherit_group_schedule.isChecked())

        row_remote = QtWidgets.QHBoxLayout()
        row_remote.addWidget(self.remote_dir, 1)
        row_remote.addWidget(self.pick_remote)

        row_local = QtWidgets.QHBoxLayout()
        row_local.addWidget(self.local_dir, 1)
        row_local.addWidget(self.pick_local)

        form = QtWidgets.QFormLayout()
        form.addRow("", self.enabled)
        form.addRow("Group:", self.group)
        form.addRow("Server:", self.server)
        form.addRow("Remote dir:", row_remote)
        form.addRow("Mask:", self.mask)
        form.addRow("Local dir:", row_local)
        form.addRow("Direction:", self.direction)
        form.addRow("", self.recursive)
        form.addRow("", self.new_only)
        form.addRow("", self.inherit_group_schedule)
        form.addRow("Schedule:", self.schedule_mode)
        form.addRow("Run at:", self.run_at)
        form.addRow("Interval (min):", self.interval_min)
        form.addRow("", self.monitor)
        form.addRow("Poll interval (s):", self.interval_sec)

        btns = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.StandardButton.Ok |
            QtWidgets.QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(btns)

        if job:
            self.enabled.setChecked(bool(job.enabled))
            self.group.setCurrentText(job.group)
            self.server.setCurrentText(job.server_name)
            self.remote_dir.setText(job.remote_dir)
            self.mask.setText(job.mask)
            self.local_dir.setText(job.local_dir)
            self.direction.setCurrentText(job.direction)
            self.recursive.setChecked(bool(job.recursive))
            self.new_only.setChecked(bool(job.new_only))

            self.inherit_group_schedule.setChecked(bool(job.inherit_group_schedule))
            self.schedule_mode.setCurrentText(job.schedule_mode or "manual")
            dt = parse_run_at(job.run_at)
            if dt:
                self.run_at.setDateTime(QtCore.QDateTime(dt))
            self.interval_min.setValue(int(job.interval_min or 0))

            self.monitor.setChecked(bool(job.monitor))
            self.interval_sec.setValue(int(job.interval_sec or 60))

            self._schedule_changed(self.schedule_mode.currentText())
            self._inherit_changed(self.inherit_group_schedule.isChecked())

    def _pick_local(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Select local folder", self.local_dir.text() or os.getcwd())
        if d:
            self.local_dir.setText(d)

    def _pick_remote(self):
        srv_name = self.server.currentText()
        prof = next((s for s in self.servers if s.name == srv_name), None)
        if not prof:
            return
        dlg = RemoteFolderPicker(self, prof)
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            self.remote_dir.setText(dlg.selected_folder())

    def _schedule_changed(self, mode: str):
        mode = (mode or "manual").lower()
        self.run_at.setEnabled((not self.inherit_group_schedule.isChecked()) and mode in ("once", "daily"))
        self.interval_min.setEnabled((not self.inherit_group_schedule.isChecked()) and mode == "interval")

    def _inherit_changed(self, inherit: bool):
        self.schedule_mode.setEnabled(not inherit)
        # Re-evaluate run_at/interval enable based on current mode
        self._schedule_changed(self.schedule_mode.currentText())

    def get_value(self, existing: Optional[SyncJob] = None) -> SyncJob:
        # run_at only if not inherit and mode requires it
        run_at_str = ""
        mode = self.schedule_mode.currentText()
        if (not self.inherit_group_schedule.isChecked()) and mode in ("once", "daily"):
            run_at_str = self.run_at.dateTime().toString("yyyy-MM-dd HH:mm")

        j = SyncJob(
            id=(existing.id if existing and existing.id else str(uuid.uuid4())),
            group=self.group.currentText() or "Default",
            enabled=bool(self.enabled.isChecked()),
            server_name=self.server.currentText(),
            remote_dir=self.remote_dir.text().strip() or "/",
            mask=self.mask.text().strip() or "*.*",
            local_dir=self.local_dir.text().strip(),
            direction=self.direction.currentText(),
            recursive=bool(self.recursive.isChecked()),
            new_only=bool(self.new_only.isChecked()),
            inherit_group_schedule=bool(self.inherit_group_schedule.isChecked()),
            schedule_mode=mode,
            run_at=run_at_str,
            interval_min=int(self.interval_min.value()),
            monitor=bool(self.monitor.isChecked()),
            interval_sec=int(self.interval_sec.value()),
        )
        return j


# ----------------------------
# Worker (QRunnable + signals)
# ----------------------------
class JobSignals(QtCore.QObject):
    log = QtCore.Signal(str)
    status = QtCore.Signal(str)
    progress = QtCore.Signal(str, int, int)  # label, done, total
    finished = QtCore.Signal(str, str)       # job_id, label
    error = QtCore.Signal(str, str, str)     # job_id, label, traceback


class JobRunnable(QtCore.QRunnable):
    def __init__(self, profile: ServerProfile, job: SyncJob, stop_event: threading.Event):
        super().__init__()
        self.profile = profile
        self.job = job
        self.job_id = job.id
        self.stop_event = stop_event
        self.signals = JobSignals()
        self.setAutoDelete(True)

        # scheduler runtime memory
        self._last_cycle_ok = True

    def run(self):
        label = f"{self.job.group} | {self.job.server_name} | {self.job.remote_dir}"
        client: Optional[RemoteClientBase] = None
        try:
            self.signals.status.emit(f"[{label}] Connecting...")
            client = make_client(self.profile)
            client.connect()
            self.signals.log.emit(f"[{label}] Connected ({self.profile.protocol.upper()})")

            while not self.stop_event.is_set():
                self._cycle(client, label)
                if not self.job.monitor:
                    break

                self.signals.status.emit(f"[{label}] Monitoring... sleep {self.job.interval_sec}s")
                for _ in range(self.job.interval_sec):
                    if self.stop_event.is_set():
                        break
                    time.sleep(1)

            self.signals.status.emit(f"[{label}] Stopped.")
            self.signals.finished.emit(self.job_id, label)

        except Exception:
            self.signals.error.emit(self.job_id, label, traceback.format_exc())
        finally:
            try:
                if client:
                    client.close()
            except Exception:
                pass

    def _cycle(self, client: RemoteClientBase, label: str):
        j = self.job
        if not j.local_dir:
            self.signals.log.emit(f"[{label}] Local dir is empty -> skip")
            return

        os.makedirs(j.local_dir, exist_ok=True)

        # list remote recursively
        self.signals.status.emit(f"[{label}] Listing remote...")
        remote_files = walk_remote(client, j.remote_dir, j.recursive)

        mask = j.mask.strip() or "*.*"
        remote_files = [(rel, e) for (rel, e) in remote_files if fnmatch.fnmatch(os.path.basename(rel), mask)]
        self.signals.log.emit(f"[{label}] Remote matched {len(remote_files)} files (mask={mask}, recursive={j.recursive})")

        # index for uploads (only within mask set)
        remote_index: Dict[str, RemoteEntry] = {rel: e for rel, e in remote_files}

        # downloads
        if j.direction in ("download", "both"):
            self.signals.status.emit(f"[{label}] Checking downloads...")
            for rel, e in remote_files:
                if self.stop_event.is_set():
                    return
                local_path = os.path.join(j.local_dir, rel.replace("/", os.sep))
                if should_download(e, local_path, j.new_only):
                    remote_path = _join_remote(j.remote_dir, rel)
                    self._download_one(client, label, remote_path, local_path, e)

        if self.stop_event.is_set():
            return

        # uploads
        if j.direction in ("upload", "both"):
            self.signals.status.emit(f"[{label}] Checking uploads...")
            local_list = local_walk(j.local_dir, j.recursive)
            local_list = [rel for rel in local_list if fnmatch.fnmatch(os.path.basename(rel), mask)]

            for rel in local_list:
                if self.stop_event.is_set():
                    return
                local_path = os.path.join(j.local_dir, rel.replace("/", os.sep))
                re = remote_index.get(rel)
                if re is None:
                    remote_path = _join_remote(j.remote_dir, rel)
                    re = client.stat(remote_path)

                if should_upload(local_path, re):
                    remote_path = _join_remote(j.remote_dir, rel)
                    self._upload_one(client, label, local_path, remote_path)

        self.signals.status.emit(f"[{label}] Cycle complete.")

    def _download_one(self, client: RemoteClientBase, label: str, remote_path: str, local_path: str, e: RemoteEntry):
        total = int(e.size) if e.size is not None else 0
        done = 0
        file_label = f"[{label}] DL {remote_path}"

        def cb(delta, transferred=None, to_be=None):
            nonlocal done, total
            if transferred is None:
                done += int(delta or 0)
            else:
                done = int(transferred)
                total = int(to_be or total or 0)
            self.signals.progress.emit(file_label, done, total)

        self.signals.log.emit(f"{file_label} -> {local_path}")
        client.download(remote_path, local_path, progress_cb=cb)
        # finalize
        if total:
            self.signals.progress.emit(file_label, total, total)
        else:
            self.signals.progress.emit(file_label, done, done)

    def _upload_one(self, client: RemoteClientBase, label: str, local_path: str, remote_path: str):
        total = os.path.getsize(local_path)
        done = 0
        file_label = f"[{label}] UL {remote_path}"

        def cb(delta, transferred=None, to_be=None):
            nonlocal done, total
            if transferred is None:
                # ftp provides delta/done/total; but sometimes we pass done already
                if isinstance(delta, int):
                    done = min(total, done + delta)
            else:
                done = int(transferred)
                total = int(to_be or total)
            self.signals.progress.emit(file_label, done, total)

        self.signals.log.emit(f"{file_label} <- {local_path}")
        client.upload(local_path, remote_path, progress_cb=cb)
        self.signals.progress.emit(file_label, total, total)


# ----------------------------
# Main Window
# ----------------------------
class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("FTP/FTPS/SFTP Sync (Groups + Schedule)")
        self.resize(1280, 740)

        self.servers, self.jobs, self.groups, self.concurrency = load_config()
        ensure_default_group(self.groups)

        # Job runtime states
        self.job_state: Dict[str, str] = {}        # job_id -> state
        self.job_last_error: Dict[str, str] = {}  # job_id -> tb
        self.job_last_run_ts: Dict[str, float] = {}  # for interval schedules
        self.job_last_run_day: Dict[str, str] = {}   # for daily schedules "YYYY-MM-DD"

        # Thread pool
        self.pool = QtCore.QThreadPool.globalInstance()
        self.pool.setMaxThreadCount(int(self.concurrency))
        self.stop_event = threading.Event()
        self.running_ids: set[str] = set()

        # Scheduler timer
        self.scheduler = QtCore.QTimer(self)
        self.scheduler.setInterval(1000)
        self.scheduler.timeout.connect(self._scheduler_tick)
        self.scheduler.start()

        # -------- Left: Servers + Groups (tabs)
        self.tabs = QtWidgets.QTabWidget()

        # Servers tab
        self.server_list = QtWidgets.QListWidget()
        self.btn_add_server = QtWidgets.QPushButton("Add")
        self.btn_edit_server = QtWidgets.QPushButton("Edit")
        self.btn_del_server = QtWidgets.QPushButton("Delete")
        self.btn_test_server = QtWidgets.QPushButton("Test")

        self.btn_add_server.clicked.connect(self.add_server)
        self.btn_edit_server.clicked.connect(self.edit_server)
        self.btn_del_server.clicked.connect(self.del_server)
        self.btn_test_server.clicked.connect(self.test_connect)

        srv_btns = QtWidgets.QHBoxLayout()
        srv_btns.addWidget(self.btn_add_server)
        srv_btns.addWidget(self.btn_edit_server)
        srv_btns.addWidget(self.btn_del_server)
        srv_btns.addWidget(self.btn_test_server)

        srv_tab = QtWidgets.QWidget()
        srv_layout = QtWidgets.QVBoxLayout(srv_tab)
        srv_layout.addWidget(self.server_list, 1)
        srv_layout.addLayout(srv_btns)
        self.tabs.addTab(srv_tab, "Servers")

        # Groups tab
        self.group_list = QtWidgets.QListWidget()
        self.btn_add_group = QtWidgets.QPushButton("Add")
        self.btn_edit_group = QtWidgets.QPushButton("Edit")
        self.btn_del_group = QtWidgets.QPushButton("Delete")

        self.btn_add_group.clicked.connect(self.add_group)
        self.btn_edit_group.clicked.connect(self.edit_group)
        self.btn_del_group.clicked.connect(self.del_group)

        grp_btns = QtWidgets.QHBoxLayout()
        grp_btns.addWidget(self.btn_add_group)
        grp_btns.addWidget(self.btn_edit_group)
        grp_btns.addWidget(self.btn_del_group)

        grp_tab = QtWidgets.QWidget()
        grp_layout = QtWidgets.QVBoxLayout(grp_tab)
        grp_layout.addWidget(self.group_list, 1)
        grp_layout.addLayout(grp_btns)
        self.tabs.addTab(grp_tab, "Groups")

        self._refresh_servers()
        self._refresh_groups()

        # -------- Right: Jobs table + controls
        self.group_filter = QtWidgets.QComboBox()
        self.group_filter.addItem("All")
        for g in self.groups:
            self.group_filter.addItem(g.name)
        self.group_filter.currentTextChanged.connect(self._refresh_jobs)

        self.job_table = QtWidgets.QTableWidget(0, 11)
        self.job_table.setHorizontalHeaderLabels([
            "St", "Grp", "Server", "Remote dir", "Mask", "Local dir",
            "Dir", "Rec", "New", "Sched", "Mon"
        ])
        self.job_table.horizontalHeader().setStretchLastSection(True)
        self.job_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.job_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)

        self.btn_add_job = QtWidgets.QPushButton("Add Job")
        self.btn_edit_job = QtWidgets.QPushButton("Edit Job")
        self.btn_del_job = QtWidgets.QPushButton("Delete Job")

        self.btn_run_selected = QtWidgets.QPushButton("Run Selected")
        self.btn_run_group = QtWidgets.QPushButton("Run Group")
        self.btn_run_all = QtWidgets.QPushButton("Run All")
        self.btn_stop_all = QtWidgets.QPushButton("Stop All")
        self.btn_stop_all.setEnabled(False)

        self.btn_add_job.clicked.connect(self.add_job)
        self.btn_edit_job.clicked.connect(self.edit_job)
        self.btn_del_job.clicked.connect(self.del_job)

        self.btn_run_selected.clicked.connect(self.run_selected)
        self.btn_run_group.clicked.connect(self.run_group)
        self.btn_run_all.clicked.connect(self.run_all)
        self.btn_stop_all.clicked.connect(self.stop_all)

        self.conc_spin = QtWidgets.QSpinBox()
        self.conc_spin.setRange(1, 32)
        self.conc_spin.setValue(int(self.concurrency))
        self.conc_spin.valueChanged.connect(self._set_concurrency)

        top_row = QtWidgets.QHBoxLayout()
        top_row.addWidget(QtWidgets.QLabel("Group filter:"))
        top_row.addWidget(self.group_filter)
        top_row.addStretch(1)
        top_row.addWidget(QtWidgets.QLabel("Concurrency:"))
        top_row.addWidget(self.conc_spin)
        top_row.addWidget(self.btn_run_selected)
        top_row.addWidget(self.btn_run_group)
        top_row.addWidget(self.btn_run_all)
        top_row.addWidget(self.btn_stop_all)

        job_row = QtWidgets.QHBoxLayout()
        job_row.addWidget(self.btn_add_job)
        job_row.addWidget(self.btn_edit_job)
        job_row.addWidget(self.btn_del_job)
        job_row.addStretch(1)

        self.status_lbl = QtWidgets.QLabel("Idle.")
        self.progress_bar = QtWidgets.QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_detail = QtWidgets.QLabel("")

        self.log_box = QtWidgets.QPlainTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setMaximumBlockCount(8000)

        right_w = QtWidgets.QWidget()
        right_layout = QtWidgets.QVBoxLayout(right_w)
        right_layout.addLayout(job_row)
        right_layout.addLayout(top_row)
        right_layout.addWidget(self.job_table, 1)
        right_layout.addWidget(self.status_lbl)
        right_layout.addWidget(self.progress_bar)
        right_layout.addWidget(self.progress_detail)
        right_layout.addWidget(QtWidgets.QLabel("Log"))
        right_layout.addWidget(self.log_box, 1)

        splitter = QtWidgets.QSplitter()
        splitter.addWidget(self.tabs)
        splitter.addWidget(right_w)
        splitter.setStretchFactor(1, 1)
        self.setCentralWidget(splitter)

        self._refresh_jobs()

    # ---------------- UI helpers ----------------
    def closeEvent(self, event):
        save_config(self.servers, self.jobs, self.groups, int(self.conc_spin.value()))
        self.stop_all()
        event.accept()

    def log(self, msg: str):
        self.log_box.appendPlainText(msg)

    def _set_concurrency(self, v: int):
        self.pool.setMaxThreadCount(int(v))
        save_config(self.servers, self.jobs, self.groups, int(v))

    def _refresh_servers(self):
        self.server_list.clear()
        for s in self.servers:
            self.server_list.addItem(f"{s.name}  ({s.protocol.upper()} {s.host}:{s.port})")

    def _refresh_groups(self):
        self.group_list.clear()
        for g in self.groups:
            txt = f"{g.name}  [{'ON' if g.enabled else 'OFF'}]  {fmt_schedule(g.schedule_mode, g.run_at, g.interval_min)}"
            it = QtWidgets.QListWidgetItem(txt)
            it.setForeground(QtGui.QBrush(QtGui.QColor(g.color)))
            self.group_list.addItem(it)

        # refresh group filter content too
        cur = self.group_filter.currentText() if hasattr(self, "group_filter") else "All"
        if hasattr(self, "group_filter"):
            self.group_filter.blockSignals(True)
            self.group_filter.clear()
            self.group_filter.addItem("All")
            for gg in self.groups:
                self.group_filter.addItem(gg.name)
            self.group_filter.setCurrentText(cur if cur else "All")
            self.group_filter.blockSignals(False)

    def _selected_job_row(self) -> int:
        rows = self.job_table.selectionModel().selectedRows()
        return rows[0].row() if rows else -1

    def _selected_group_name(self) -> Optional[str]:
        idx = self.group_list.currentRow()
        if idx < 0 or idx >= len(self.groups):
            return None
        return self.groups[idx].name

    def _get_group(self, name: str) -> Optional[JobGroup]:
        for g in self.groups:
            if g.name == name:
                return g
        return None

    def _effective_schedule(self, job: SyncJob) -> Tuple[str, str, int]:
        if job.inherit_group_schedule:
            g = self._get_group(job.group)
            if g:
                return (g.schedule_mode or "manual", g.run_at or "", int(g.interval_min or 0))
        return (job.schedule_mode or "manual", job.run_at or "", int(job.interval_min or 0))

    def _refresh_jobs(self):
        flt = self.group_filter.currentText() if hasattr(self, "group_filter") else "All"
        visible_jobs = [j for j in self.jobs if (flt == "All" or j.group == flt)]

        self.job_table.setRowCount(0)
        for j in visible_jobs:
            r = self.job_table.rowCount()
            self.job_table.insertRow(r)

            state = self.job_state.get(j.id, "idle")
            grp = self._get_group(j.group)
            grp_color = grp.color if grp else "#9aa0a6"

            sched_mode, run_at, interval_min = self._effective_schedule(j)
            sched_txt = fmt_schedule(sched_mode, run_at, interval_min)
            if j.inherit_group_schedule:
                sched_txt = f"(grp) {sched_txt}"

            # Status dot
            self.job_table.setItem(r, 0, make_dot_item(status_color_hex(state)))

            # Group colored dot (and name)
            grp_item = QtWidgets.QTableWidgetItem(j.group)
            grp_item.setForeground(QtGui.QBrush(QtGui.QColor(grp_color)))
            self.job_table.setItem(r, 1, grp_item)

            # Normal columns
            vals = [
                j.server_name,
                j.remote_dir,
                j.mask,
                j.local_dir,
                j.direction,
                "Y" if j.recursive else "N",
                "Y" if j.new_only else "N",
                sched_txt,
                "Y" if j.monitor else "N",
            ]
            cols = [2, 3, 4, 5, 6, 7, 8, 9, 10]
            for c, v in zip(cols, vals):
                it = QtWidgets.QTableWidgetItem(str(v))
                self.job_table.setItem(r, c, it)

            # Grey out disabled jobs
            if not j.enabled or (grp and not grp.enabled):
                for c in range(self.job_table.columnCount()):
                    item = self.job_table.item(r, c)
                    if item:
                        item.setForeground(QtGui.QBrush(QtGui.QColor("#7a7a7a")))

        self.job_table.resizeColumnsToContents()

    # ---------------- Scheduler ----------------
    def _scheduler_tick(self):
        # Find due jobs and run them (if not running already)
        now = datetime.now()
        for j in self.jobs:
            if not j.enabled:
                continue
            g = self._get_group(j.group)
            if g and not g.enabled:
                continue
            if j.id in self.running_ids:
                continue

            mode, run_at, interval_min = self._effective_schedule(j)
            mode = (mode or "manual").lower()
            if mode == "manual":
                continue

            if mode == "once":
                dt = parse_run_at(run_at)
                if dt and now >= dt:
                    self._start_jobs([j], reason="schedule once")
                    # auto disable that schedule after fire
                    if j.inherit_group_schedule and g:
                        g.schedule_mode = "manual"
                    else:
                        j.schedule_mode = "manual"
                    save_config(self.servers, self.jobs, self.groups, int(self.conc_spin.value()))
                    self._refresh_groups()
                    self._refresh_jobs()

            elif mode == "daily":
                dt = parse_run_at(run_at)
                if dt:
                    target = now.replace(hour=dt.hour, minute=dt.minute, second=0, microsecond=0)
                    today = now.strftime("%Y-%m-%d")
                    if now >= target and self.job_last_run_day.get(j.id) != today:
                        self._start_jobs([j], reason="schedule daily")
                        self.job_last_run_day[j.id] = today

            elif mode == "interval" and interval_min and interval_min > 0:
                last_ts = self.job_last_run_ts.get(j.id, 0.0)
                if time.time() - last_ts >= interval_min * 60:
                    self._start_jobs([j], reason="schedule interval")
                    self.job_last_run_ts[j.id] = time.time()

    # ---------------- Run controls ----------------
    def _start_jobs(self, jobs: List[SyncJob], reason: str = "manual"):
        if not jobs:
            return
        self.stop_event.clear()
        self.btn_stop_all.setEnabled(True)

        started = 0
        for j in jobs:
            if j.id in self.running_ids:
                continue

            prof = next((s for s in self.servers if s.name == j.server_name), None)
            if not prof:
                self.log(f"[ERROR] Server not found: {j.server_name} (job {j.remote_dir})")
                self.job_state[j.id] = "error"
                continue

            self.job_state[j.id] = "queued"
            self.running_ids.add(j.id)

            r = JobRunnable(prof, j, self.stop_event)
            r.signals.log.connect(self.log)
            r.signals.status.connect(self._on_status)
            r.signals.progress.connect(self._on_progress)
            r.signals.error.connect(self._on_error)
            r.signals.finished.connect(self._on_finished)

            self.pool.start(r)
            self.job_state[j.id] = "running"
            started += 1

        if started:
            self.log(f"--- Started {started} job(s) ({reason}) ---")
            self._refresh_jobs()

    def run_selected(self):
        row = self._selected_job_row()
        if row < 0:
            return
        flt = self.group_filter.currentText()
        visible_jobs = [j for j in self.jobs if (flt == "All" or j.group == flt)]
        if row >= len(visible_jobs):
            return
        j = visible_jobs[row]
        if not j.enabled:
            return
        g = self._get_group(j.group)
        if g and not g.enabled:
            return
        self._start_jobs([j], reason="manual selected")

    def run_group(self):
        gname = self.group_filter.currentText()
        if gname == "All":
            QtWidgets.QMessageBox.information(self, "Select group", "Choose a group in filter first (not All).")
            return
        g = self._get_group(gname)
        if g and not g.enabled:
            QtWidgets.QMessageBox.warning(self, "Group disabled", "This group is disabled.")
            return
        jobs = [j for j in self.jobs if j.group == gname and j.enabled]
        self._start_jobs(jobs, reason=f"manual group {gname}")

    def run_all(self):
        jobs = []
        for j in self.jobs:
            if not j.enabled:
                continue
            g = self._get_group(j.group)
            if g and not g.enabled:
                continue
            jobs.append(j)
        self._start_jobs(jobs, reason="manual all")

    def stop_all(self):
        self.stop_event.set()
        # mark running as stopped (UI)
        for jid in list(self.running_ids):
            self.job_state[jid] = "stopped"
        self.status_lbl.setText("Stopping...")
        self.btn_stop_all.setEnabled(False)
        self._refresh_jobs()

    # ---------------- Job signals ----------------
    def _on_status(self, s: str):
        self.status_lbl.setText(s)

    def _on_progress(self, label: str, done: int, total: int):
        if total and total > 0:
            pct = int((done / total) * 100)
            pct = max(0, min(100, pct))
            self.progress_bar.setValue(pct)
            self.progress_detail.setText(f"{label}  {pct}%  ({done}/{total})")
        else:
            self.progress_bar.setValue(0)
            self.progress_detail.setText(f"{label}  {done} bytes")

    def _on_error(self, job_id: str, job_label: str, tb: str):
        self.job_state[job_id] = "error"
        self.job_last_error[job_id] = tb
        self.running_ids.discard(job_id)
        self.log(f"[{job_label}] ERROR:\n{tb}")
        self._refresh_jobs()

    def _on_finished(self, job_id: str, job_label: str):
        if self.job_state.get(job_id) == "running":
            self.job_state[job_id] = "ok"
        self.running_ids.discard(job_id)
        self.log(f"[{job_label}] FINISHED")
        if not self.running_ids:
            self.status_lbl.setText("Idle.")
            self.progress_bar.setValue(0)
            self.progress_detail.setText("")
            self.btn_stop_all.setEnabled(False)
        self._refresh_jobs()

    # ---------------- Servers ----------------
    def add_server(self):
        dlg = ServerDialog(self)
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            s = dlg.get_value()
            if not s.name:
                QtWidgets.QMessageBox.warning(self, "Error", "Server name required.")
                return
            if any(x.name == s.name for x in self.servers):
                QtWidgets.QMessageBox.warning(self, "Error", "Server name already exists.")
                return
            self.servers.append(s)
            self._refresh_servers()
            save_config(self.servers, self.jobs, self.groups, int(self.conc_spin.value()))

    def edit_server(self):
        idx = self.server_list.currentRow()
        if idx < 0 or idx >= len(self.servers):
            return
        cur = self.servers[idx]
        dlg = ServerDialog(self, cur)
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            s = dlg.get_value()
            if not s.name:
                QtWidgets.QMessageBox.warning(self, "Error", "Server name required.")
                return
            if s.name != cur.name and any(x.name == s.name for x in self.servers):
                QtWidgets.QMessageBox.warning(self, "Error", "Server name already exists.")
                return
            old = cur.name
            self.servers[idx] = s
            for j in self.jobs:
                if j.server_name == old:
                    j.server_name = s.name
            self._refresh_servers()
            self._refresh_jobs()
            save_config(self.servers, self.jobs, self.groups, int(self.conc_spin.value()))

    def del_server(self):
        idx = self.server_list.currentRow()
        if idx < 0 or idx >= len(self.servers):
            return
        name = self.servers[idx].name
        if QtWidgets.QMessageBox.question(self, "Delete", f"Delete server '{name}'?") != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.servers.pop(idx)
        # remove jobs using it
        self.jobs = [j for j in self.jobs if j.server_name != name]
        self._refresh_servers()
        self._refresh_jobs()
        save_config(self.servers, self.jobs, self.groups, int(self.conc_spin.value()))

    def test_connect(self):
        idx = self.server_list.currentRow()
        if idx < 0 or idx >= len(self.servers):
            return
        prof = self.servers[idx]
        try:
            c = make_client(prof)
            c.connect()
            c.close()
            QtWidgets.QMessageBox.information(self, "OK", f"Connected: {prof.protocol.upper()} {prof.host}:{prof.port}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Failed", str(e))

    # ---------------- Groups ----------------
    def add_group(self):
        dlg = GroupDialog(self)
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            g = dlg.get_value()
            if not g.name:
                QtWidgets.QMessageBox.warning(self, "Error", "Group name required.")
                return
            if any(x.name == g.name for x in self.groups):
                QtWidgets.QMessageBox.warning(self, "Error", "Group name already exists.")
                return
            self.groups.append(g)
            self._refresh_groups()
            self._refresh_jobs()
            save_config(self.servers, self.jobs, self.groups, int(self.conc_spin.value()))

    def edit_group(self):
        idx = self.group_list.currentRow()
        if idx < 0 or idx >= len(self.groups):
            return
        cur = self.groups[idx]
        if cur.name == "Default":
            # allow edit, but not delete later
            pass
        dlg = GroupDialog(self, cur)
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            g = dlg.get_value()
            if not g.name:
                QtWidgets.QMessageBox.warning(self, "Error", "Group name required.")
                return
            if g.name != cur.name and any(x.name == g.name for x in self.groups):
                QtWidgets.QMessageBox.warning(self, "Error", "Group name already exists.")
                return
            old = cur.name
            self.groups[idx] = g
            for j in self.jobs:
                if j.group == old:
                    j.group = g.name
            ensure_default_group(self.groups)
            self._refresh_groups()
            self._refresh_jobs()
            save_config(self.servers, self.jobs, self.groups, int(self.conc_spin.value()))

    def del_group(self):
        idx = self.group_list.currentRow()
        if idx < 0 or idx >= len(self.groups):
            return
        g = self.groups[idx]
        if g.name == "Default":
            QtWidgets.QMessageBox.warning(self, "Not allowed", "Default group cannot be deleted.")
            return
        if QtWidgets.QMessageBox.question(self, "Delete", f"Delete group '{g.name}'? Jobs will move to Default.") != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.groups.pop(idx)
        for j in self.jobs:
            if j.group == g.name:
                j.group = "Default"
        ensure_default_group(self.groups)
        self._refresh_groups()
        self._refresh_jobs()
        save_config(self.servers, self.jobs, self.groups, int(self.conc_spin.value()))

    # ---------------- Jobs ----------------
    def add_job(self):
        if not self.servers:
            QtWidgets.QMessageBox.warning(self, "No servers", "Add a server first.")
            return
        dlg = JobDialog(self, self.servers, self.groups)
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            j = dlg.get_value()
            if not j.local_dir:
                QtWidgets.QMessageBox.warning(self, "Error", "Local dir required.")
                return
            if not j.id:
                j.id = str(uuid.uuid4())
            self.jobs.append(j)
            self._refresh_jobs()
            save_config(self.servers, self.jobs, self.groups, int(self.conc_spin.value()))

    def edit_job(self):
        row = self._selected_job_row()
        if row < 0:
            return
        flt = self.group_filter.currentText()
        visible_jobs = [j for j in self.jobs if (flt == "All" or j.group == flt)]
        if row >= len(visible_jobs):
            return
        cur = visible_jobs[row]

        dlg = JobDialog(self, self.servers, self.groups, cur)
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            j = dlg.get_value(existing=cur)
            if not j.local_dir:
                QtWidgets.QMessageBox.warning(self, "Error", "Local dir required.")
                return

            # replace in self.jobs by id
            for i in range(len(self.jobs)):
                if self.jobs[i].id == cur.id:
                    self.jobs[i] = j
                    break

            self._refresh_jobs()
            save_config(self.servers, self.jobs, self.groups, int(self.conc_spin.value()))

    def del_job(self):
        row = self._selected_job_row()
        if row < 0:
            return
        flt = self.group_filter.currentText()
        visible_jobs = [j for j in self.jobs if (flt == "All" or j.group == flt)]
        if row >= len(visible_jobs):
            return
        cur = visible_jobs[row]

        if QtWidgets.QMessageBox.question(self, "Delete", f"Delete job:\n{cur.group} | {cur.server_name} | {cur.remote_dir}?") != QtWidgets.QMessageBox.StandardButton.Yes:
            return

        self.jobs = [j for j in self.jobs if j.id != cur.id]
        self.job_state.pop(cur.id, None)
        self.running_ids.discard(cur.id)
        self._refresh_jobs()
        save_config(self.servers, self.jobs, self.groups, int(self.conc_spin.value()))


def main():
    app = QtWidgets.QApplication([])
    w = MainWindow()
    w.show()
    app.exec()


if __name__ == "__main__":
    main()