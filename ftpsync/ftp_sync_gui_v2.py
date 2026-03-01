import threading
import os
import json
import time
import fnmatch
import traceback
import calendar
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Tuple

from PySide6 import QtCore, QtWidgets

from ftplib import FTP, FTP_TLS, error_perm

try:
    import paramiko
except Exception:
    paramiko = None
import os
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

    # FTP MDTM timezone handling:
    # Many servers are UTC (correct). Some return local server time.
    # Set tz_offset_min to shift remote MDTM to UTC epoch.
    # Example: server MDTM is local UTC+3 -> tz_offset_min = -180 to convert to UTC.
    tz_offset_min: int = 0

    # SFTP only: host key policy (simple default)
    sftp_allow_unknown_host: bool = True


@dataclass
class SyncJob:
    server_name: str
    remote_dir: str
    mask: str = "*.*"
    local_dir: str = ""
    direction: str = "download"  # download | upload | both
    recursive: bool = True

    monitor: bool = False
    interval_sec: int = 60


@dataclass
class RemoteEntry:
    name: str
    is_dir: bool
    size: Optional[int] = None
    mtime: Optional[int] = None  # epoch seconds (UTC epoch)


# ----------------------------
# Persistence
# ----------------------------
def load_config() -> Tuple[List[ServerProfile], List[SyncJob], int]:
    if not os.path.exists(CONFIG_FILE):
        return [], [], 2
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    servers = [ServerProfile(**x) for x in data.get("servers", [])]
    jobs = [SyncJob(**x) for x in data.get("jobs", [])]
    concurrency = int(data.get("concurrency", 2))
    return servers, jobs, max(1, concurrency)


def save_config(servers: List[ServerProfile], jobs: List[SyncJob], concurrency: int) -> None:
    data = {
        "servers": [asdict(s) for s in servers],
        "jobs": [asdict(j) for j in jobs],
        "concurrency": int(concurrency),
    }
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


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


def _join_remote(protocol: str, base: str, name: str) -> str:
    if protocol in ("ftp", "ftps"):
        # remote paths are POSIX-like
        if base.endswith("/"):
            return base + name
        if base == "":
            return "/" + name
        return base + "/" + name
    else:
        # sftp is also POSIX
        if base.endswith("/"):
            return base + name
        if base == "":
            return "/" + name
        return base + "/" + name


# ----------------------------
# FTP/FTPS client
# ----------------------------
class FtpClient(RemoteClientBase):
    def __init__(self, prof: ServerProfile):
        self.p = prof
        self.ftp = None
        self.protocol = "ftps" if prof.protocol == "ftps" else "ftp"

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
        if not path:
            path = "/"
        self.ftp.cwd(path)

    def _parse_mdtm_to_epoch_utc(self, mdtm_resp: str) -> Optional[int]:
        # resp like '213 20260101123456'
        try:
            parts = mdtm_resp.split()
            if len(parts) >= 2 and parts[0] == "213":
                s = parts[1].strip()
                if len(s) == 14:
                    t = time.strptime(s, "%Y%m%d%H%M%S")
                    # interpret as UTC by default (RFC-ish)
                    epoch = calendar.timegm(t)
                    # apply optional tz shift (in minutes)
                    epoch += int(self.p.tz_offset_min) * 60
                    return int(epoch)
        except Exception:
            return None
        return None

    def _mdtm_epoch(self, name: str) -> Optional[int]:
        try:
            resp = self.ftp.sendcmd(f"MDTM {name}")
            return self._parse_mdtm_to_epoch_utc(resp)
        except Exception:
            return None

    def _size(self, name: str) -> Optional[int]:
        try:
            return self.ftp.size(name)
        except Exception:
            return None

    def list_dir(self, path: str) -> List[RemoteEntry]:
        self._cwd(path)

        # Try MLSD first (best)
        entries: List[RemoteEntry] = []
        try:
            for name, facts in self.ftp.mlsd():
                if name in (".", ".."):
                    continue
                t = facts.get("type", "")
                is_dir = (t == "dir")
                size = None
                mtime = None
                if not is_dir:
                    if "size" in facts:
                        try:
                            size = int(facts["size"])
                        except Exception:
                            size = None
                # MLSD "modify" is UTC timestamp YYYYMMDDHHMMSS
                if "modify" in facts:
                    s = facts["modify"]
                    if len(s) == 14:
                        try:
                            tt = time.strptime(s, "%Y%m%d%H%M%S")
                            mtime = calendar.timegm(tt) + int(self.p.tz_offset_min) * 60
                        except Exception:
                            mtime = None
                entries.append(RemoteEntry(name=name, is_dir=is_dir, size=size, mtime=mtime))
            return entries
        except Exception:
            pass

        # Fallback: NLST names, and per-file size+mdtm
        names: List[str] = []
        try:
            names = self.ftp.nlst()
        except Exception:
            # LIST parsing fallback
            lines: List[str] = []
            try:
                self.ftp.retrlines("LIST", lines.append)
                # naive: last token
                names = [ln.split()[-1] for ln in lines if ln.strip()]
            except Exception:
                names = []

        # Determine dirs by trying cwd into them (expensive but works)
        # We keep it simple for tree browsing.
        for n in names:
            if n in (".", ".."):
                continue
            is_dir = False
            try:
                cur = self.ftp.pwd()
                self.ftp.cwd(n)
                is_dir = True
                self.ftp.cwd(cur)
            except Exception:
                is_dir = False

            if is_dir:
                entries.append(RemoteEntry(name=n, is_dir=True))
            else:
                entries.append(RemoteEntry(name=n, is_dir=False, size=self._size(n), mtime=self._mdtm_epoch(n)))

        return entries

    def ensure_dir(self, path: str):
        # recursively create directories (FTP)
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
        # Try to stat a file (size + mdtm) by changing into its directory
        try:
            dirp, name = os.path.split(path.rstrip("/"))
            if not dirp:
                dirp = "/"
            self._cwd(dirp)
            # determine if dir by cwd attempt
            try:
                cur = self.ftp.pwd()
                self.ftp.cwd(name)
                self.ftp.cwd(cur)
                return RemoteEntry(name=name, is_dir=True)
            except Exception:
                return RemoteEntry(name=name, is_dir=False, size=self._size(name), mtime=self._mdtm_epoch(name))
        except Exception:
            return None

    def download(self, remote_path: str, local_path: str, progress_cb=None):
        dirp, name = os.path.split(remote_path.rstrip("/"))
        if not dirp:
            dirp = "/"
        self._cwd(dirp)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        with open(local_path, "wb") as f:
            def _write(chunk: bytes):
                f.write(chunk)
                if progress_cb:
                    progress_cb(len(chunk))
            self.ftp.retrbinary(f"RETR {name}", _write, blocksize=64 * 1024)

        # set local mtime from MDTM if possible
        m = self._mdtm_epoch(name)
        if m is not None:
            try:
                os.utime(local_path, (m, m))
            except Exception:
                pass

    def upload(self, local_path: str, remote_path: str, progress_cb=None):
        dirp, name = os.path.split(remote_path.rstrip("/"))
        if not dirp:
            dirp = "/"
        self.ensure_dir(dirp)
        self._cwd(dirp)

        total = os.path.getsize(local_path)
        done = 0

        def _cb(block: bytes):
            nonlocal done
            done += len(block)
            if progress_cb:
                progress_cb(len(block), done, total)

        with open(local_path, "rb") as f:
            # storbinary callback receives each block
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
        self.protocol = "sftp"

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
        if not path:
            path = "."
        out: List[RemoteEntry] = []
        for a in self.sftp.listdir_attr(path):
            is_dir = bool(a.st_mode & 0o040000)  # S_IFDIR
            out.append(RemoteEntry(name=a.filename, is_dir=is_dir, size=a.st_size, mtime=int(a.st_mtime)))
        return out

    def ensure_dir(self, path: str):
        if not path or path in ("/", "."):
            return
        # build path parts
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
            return RemoteEntry(name=os.path.basename(path.rstrip("/")), is_dir=is_dir, size=s.st_size, mtime=int(s.st_mtime))
        except Exception:
            return None

    def download(self, remote_path: str, local_path: str, progress_cb=None):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        done = 0
        total = None
        try:
            st = self.sftp.stat(remote_path)
            total = int(st.st_size)
        except Exception:
            total = None

        def cb(transferred, to_be_transferred):
            # paramiko callback gives absolute totals
            if progress_cb:
                delta = transferred - cb.last
                cb.last = transferred
                progress_cb(delta, transferred, to_be_transferred)

        cb.last = 0
        self.sftp.get(remote_path, local_path, callback=cb)

        # preserve remote mtime if possible
        try:
            st = self.sftp.stat(remote_path)
            os.utime(local_path, (int(st.st_mtime), int(st.st_mtime)))
        except Exception:
            pass

    def upload(self, local_path: str, remote_path: str, progress_cb=None):
        dirp = os.path.dirname(remote_path.rstrip("/"))
        self.ensure_dir(dirp)

        def cb(transferred, to_be_transferred):
            if progress_cb:
                delta = transferred - cb.last
                cb.last = transferred
                progress_cb(delta, transferred, to_be_transferred)

        cb.last = 0
        self.sftp.put(local_path, remote_path, callback=cb)


# ----------------------------
# Sync logic (recursive + mtime/size)
# ----------------------------
def make_client(profile: ServerProfile) -> RemoteClientBase:
    proto = profile.protocol.lower().strip()
    if proto == "sftp":
        return SftpClient(profile)
    elif proto == "ftps":
        return FtpClient(profile)
    else:
        return FtpClient(profile)


def walk_remote(client: RemoteClientBase, protocol: str, base_dir: str, recursive: bool) -> List[Tuple[str, RemoteEntry]]:
    """
    Returns list of (relative_path, RemoteEntry) for files only.
    base_dir is remote folder selected.
    """
    results: List[Tuple[str, RemoteEntry]] = []

    def _walk(cur_dir: str, rel_prefix: str):
        entries = client.list_dir(cur_dir)
        for e in entries:
            if e.name in (".", ".."):
                continue
            remote_path = _join_remote(protocol, cur_dir, e.name)
            rel_path = e.name if not rel_prefix else (rel_prefix + "/" + e.name)
            if e.is_dir:
                if recursive:
                    _walk(remote_path, rel_path)
            else:
                results.append((rel_path, e))

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
    """
    mtime+size detection:
      - if local missing -> True
      - if size known and differs -> True
      - if mtime known and remote newer -> True
    """
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
# Folder picker dialog (tree browser)
# ----------------------------
class RemoteFolderPicker(QtWidgets.QDialog):
    def __init__(self, parent, profile: ServerProfile):
        super().__init__(parent)
        self.setWindowTitle("Select Remote Folder")
        self.resize(600, 450)
        self.profile = profile
        self.client = None
        self.protocol = profile.protocol.lower().strip()

        self.tree = QtWidgets.QTreeWidget()
        self.tree.setHeaderLabels(["Folder"])
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

        self._connect_and_load_root()

    def _connect_and_load_root(self):
        try:
            self.client = make_client(self.profile)
            self.client.connect()
            self.status.setText(f"Connected: {self.profile.name}")
            self.tree.clear()

            root_path = "/" if self.protocol != "sftp" else "/"
            root_item = QtWidgets.QTreeWidgetItem([root_path])
            root_item.setData(0, QtCore.Qt.ItemDataRole.UserRole, root_path)
            self.tree.addTopLevelItem(root_item)

            # add lazy placeholder
            root_item.addChild(QtWidgets.QTreeWidgetItem(["(loading...)"]))
            root_item.setExpanded(True)
        except Exception as e:
            self.status.setText("Failed to connect")
            QtWidgets.QMessageBox.critical(self, "Error", str(e))

    def _on_expand(self, item: QtWidgets.QTreeWidgetItem):
        path = item.data(0, QtCore.Qt.ItemDataRole.UserRole)
        if not path or not self.client:
            return

        # if already loaded (no placeholder), skip
        if item.childCount() == 1 and item.child(0).text(0) == "(loading...)":
            item.takeChild(0)
        elif item.childCount() > 0:
            return

        try:
            entries = self.client.list_dir(path)
            dirs = [e for e in entries if e.is_dir and e.name not in (".", "..")]
            dirs.sort(key=lambda x: x.name.lower())

            for d in dirs:
                child_path = _join_remote(self.protocol, path, d.name)
                child = QtWidgets.QTreeWidgetItem([d.name])
                child.setData(0, QtCore.Qt.ItemDataRole.UserRole, child_path)
                # placeholder for lazy load
                child.addChild(QtWidgets.QTreeWidgetItem(["(loading...)"]))
                item.addChild(child)

        except Exception:
            # If listing fails, just show nothing
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
# Server + Job dialogs
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
        self.tz_hint = QtWidgets.QLabel("FTP MDTM offset: use only if server times are wrong. Usually 0.")

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
        if p == "sftp":
            if self.port.value() == 21:
                self.port.setValue(22)
        else:
            if self.port.value() == 22:
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


class JobDialog(QtWidgets.QDialog):
    def __init__(self, parent=None, servers: List[ServerProfile] = None, job: Optional[SyncJob] = None):
        super().__init__(parent)
        self.setWindowTitle("Sync Job")
        self.setModal(True)
        self.servers = servers or []

        self.server = QtWidgets.QComboBox()
        self.server.addItems([s.name for s in self.servers])

        self.remote_dir = QtWidgets.QLineEdit("/")
        self.pick_remote = QtWidgets.QPushButton("Pick remote...")

        self.mask = QtWidgets.QLineEdit("*.*")

        self.local_dir = QtWidgets.QLineEdit()
        self.pick_local = QtWidgets.QPushButton("Browse...")

        self.direction = QtWidgets.QComboBox()
        self.direction.addItems(["download", "upload", "both"])

        self.recursive = QtWidgets.QCheckBox("Recursive (include subfolders)")
        self.recursive.setChecked(True)

        self.monitor = QtWidgets.QCheckBox("Monitor (polling)")
        self.interval = QtWidgets.QSpinBox()
        self.interval.setRange(5, 999999)
        self.interval.setValue(60)

        self.pick_local.clicked.connect(self._pick_local)
        self.pick_remote.clicked.connect(self._pick_remote)

        row_remote = QtWidgets.QHBoxLayout()
        row_remote.addWidget(self.remote_dir, 1)
        row_remote.addWidget(self.pick_remote)

        row_local = QtWidgets.QHBoxLayout()
        row_local.addWidget(self.local_dir, 1)
        row_local.addWidget(self.pick_local)

        form = QtWidgets.QFormLayout()
        form.addRow("Server:", self.server)
        form.addRow("Remote dir:", row_remote)
        form.addRow("Mask:", self.mask)
        form.addRow("Local dir:", row_local)
        form.addRow("Direction:", self.direction)
        form.addRow("", self.recursive)
        form.addRow("", self.monitor)
        form.addRow("Interval (s):", self.interval)

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
            self.server.setCurrentText(job.server_name)
            self.remote_dir.setText(job.remote_dir)
            self.mask.setText(job.mask)
            self.local_dir.setText(job.local_dir)
            self.direction.setCurrentText(job.direction)
            self.recursive.setChecked(job.recursive)
            self.monitor.setChecked(job.monitor)
            self.interval.setValue(job.interval_sec)

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

    def get_value(self) -> SyncJob:
        return SyncJob(
            server_name=self.server.currentText(),
            remote_dir=self.remote_dir.text().strip() or "/",
            mask=self.mask.text().strip() or "*.*",
            local_dir=self.local_dir.text().strip(),
            direction=self.direction.currentText(),
            recursive=self.recursive.isChecked(),
            monitor=self.monitor.isChecked(),
            interval_sec=int(self.interval.value()),
        )


# ----------------------------
# Worker signals + runnable (QThreadPool)
# ----------------------------
class JobSignals(QtCore.QObject):
    log = QtCore.Signal(str)
    status = QtCore.Signal(str)
    progress = QtCore.Signal(str, int, int)  # label, done, total
    finished = QtCore.Signal(str)            # job label
    error = QtCore.Signal(str, str)          # job label, traceback


class JobRunnable(QtCore.QRunnable):
    def __init__(self, profile: ServerProfile, job: SyncJob, stop_event: threading.Event):
        super().__init__()
        self.profile = profile
        self.job = job
        self.protocol = profile.protocol.lower().strip()
        self.signals = JobSignals()
        self.stop_event = stop_event
        self.setAutoDelete(True)

    def run(self):
        label = f"{self.job.server_name} | {self.job.remote_dir}"
        client = None
        try:
            self.signals.status.emit(f"[{label}] Connecting...")
            client = make_client(self.profile)
            client.connect()
            self.signals.log.emit(f"[{label}] Connected ({self.protocol})")

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
        except Exception:
            self.signals.error.emit(label, traceback.format_exc())
        finally:
            try:
                if client:
                    client.close()
            except Exception:
                pass
            self.signals.finished.emit(label)

    def _cycle(self, client: RemoteClientBase, label: str):
        job = self.job
        if not job.local_dir:
            self.signals.log.emit(f"[{label}] Local dir empty -> skip")
            return

        os.makedirs(job.local_dir, exist_ok=True)

        # Remote recursive listing (files)
        self.signals.status.emit(f"[{label}] Listing remote...")
        remote_files = walk_remote(client, self.protocol, job.remote_dir, job.recursive)

        # Filter by mask on basename
        mask = job.mask.strip() or "*.*"
        remote_files = [(rel, e) for (rel, e) in remote_files if fnmatch.fnmatch(os.path.basename(rel), mask)]
        self.signals.log.emit(f"[{label}] Remote matched {len(remote_files)} files (mask={mask}, recursive={job.recursive})")

        # Build remote index for upload comparisons (by relative path)
        remote_index: Dict[str, RemoteEntry] = {rel: e for rel, e in remote_files}

        # DOWNLOAD
        if job.direction in ("download", "both"):
            self.signals.status.emit(f"[{label}] Checking downloads...")
            for rel, e in remote_files:
                if self.stop_event.is_set():
                    return
                local_path = os.path.join(job.local_dir, rel.replace("/", os.sep))
                if should_transfer(e, local_path):
                    remote_path = _join_remote(self.protocol, job.remote_dir, rel)
                    self._download_one(client, label, remote_path, local_path, e)

        # UPLOAD
        if self.stop_event.is_set():
            return
        if job.direction in ("upload", "both"):
            self.signals.status.emit(f"[{label}] Checking uploads...")
            local_list = local_walk(job.local_dir, job.recursive)
            local_list = [rel for rel in local_list if fnmatch.fnmatch(os.path.basename(rel), mask)]

            for rel in local_list:
                if self.stop_event.is_set():
                    return
                local_path = os.path.join(job.local_dir, rel.replace("/", os.sep))

                # stat remote if not in index
                re = remote_index.get(rel)
                if re is None:
                    # attempt stat on remote target
                    remote_path = _join_remote(self.protocol, job.remote_dir, rel)
                    re = client.stat(remote_path)

                if should_upload(local_path, re):
                    remote_path = _join_remote(self.protocol, job.remote_dir, rel)
                    self._upload_one(client, label, local_path, remote_path)

        self.signals.status.emit(f"[{label}] Cycle complete.")

    def _download_one(self, client: RemoteClientBase, label: str, remote_path: str, local_path: str, e: RemoteEntry):
        total = int(e.size) if e.size is not None else 0
        done = 0
        file_label = f"[{label}] DL {remote_path}"

        def cb(delta, transferred=None, to_be=None):
            nonlocal done, total
            # FTP path uses cb(delta)
            if transferred is None:
                done += int(delta)
            else:
                done = int(transferred)
                total = int(to_be or total or 0)
            self.signals.progress.emit(file_label, done, total)

        self.signals.log.emit(f"{file_label} -> {local_path}")
        client.download(remote_path, local_path, progress_cb=cb)
        self.signals.progress.emit(file_label, total or done, total or done)

    def _upload_one(self, client: RemoteClientBase, label: str, local_path: str, remote_path: str):
        total = os.path.getsize(local_path)
        done = 0
        file_label = f"[{label}] UL {remote_path}"

        def cb(delta, transferred=None, to_be=None):
            nonlocal done, total
            # FTP upload gives (delta, done, total), SFTP gives absolute totals
            if transferred is None:
                done += int(delta)
            else:
                done = int(transferred)
                total = int(to_be or total)
            self.signals.progress.emit(file_label, done, total)

        self.signals.log.emit(f"{file_label} <- {local_path}")
        client.upload(local_path, remote_path, progress_cb=cb)
        self.signals.progress.emit(file_label, total, total)


# ----------------------------
# Main window
# ----------------------------
class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("FTP/FTPS/SFTP Folder Sync (PySide6)")
        self.resize(1200, 700)

        self.servers, self.jobs, self.concurrency = load_config()

        # Thread pool
        self.pool = QtCore.QThreadPool.globalInstance()
        self.pool.setMaxThreadCount(int(self.concurrency))
        self.stop_event = threading.Event()

        # UI: servers
        self.server_list = QtWidgets.QListWidget()
        self.server_list.setMinimumWidth(280)
        self._refresh_servers()

        self.btn_add_server = QtWidgets.QPushButton("Add")
        self.btn_edit_server = QtWidgets.QPushButton("Edit")
        self.btn_del_server = QtWidgets.QPushButton("Delete")
        self.btn_test_server = QtWidgets.QPushButton("Test Connect")

        self.btn_add_server.clicked.connect(self.add_server)
        self.btn_edit_server.clicked.connect(self.edit_server)
        self.btn_del_server.clicked.connect(self.del_server)
        self.btn_test_server.clicked.connect(self.test_connect)

        srv_btn_row = QtWidgets.QHBoxLayout()
        srv_btn_row.addWidget(self.btn_add_server)
        srv_btn_row.addWidget(self.btn_edit_server)
        srv_btn_row.addWidget(self.btn_del_server)
        srv_btn_row.addWidget(self.btn_test_server)

        left = QtWidgets.QVBoxLayout()
        left.addWidget(QtWidgets.QLabel("Servers"))
        left.addWidget(self.server_list, 1)
        left.addLayout(srv_btn_row)
        left_w = QtWidgets.QWidget()
        left_w.setLayout(left)

        # UI: jobs
        self.job_table = QtWidgets.QTableWidget(0, 8)
        self.job_table.setHorizontalHeaderLabels(
            ["Server", "Remote dir", "Mask", "Local dir", "Direction", "Recursive", "Monitor", "Interval"]
        )
        self.job_table.horizontalHeader().setStretchLastSection(True)
        self.job_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.job_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self._refresh_jobs()

        self.btn_add_job = QtWidgets.QPushButton("Add Job")
        self.btn_edit_job = QtWidgets.QPushButton("Edit Job")
        self.btn_del_job = QtWidgets.QPushButton("Delete Job")
        self.btn_run_job = QtWidgets.QPushButton("Run Selected")
        self.btn_run_all = QtWidgets.QPushButton("Run All")
        self.btn_stop_all = QtWidgets.QPushButton("Stop All")
        self.btn_stop_all.setEnabled(False)

        self.btn_add_job.clicked.connect(self.add_job)
        self.btn_edit_job.clicked.connect(self.edit_job)
        self.btn_del_job.clicked.connect(self.del_job)
        self.btn_run_job.clicked.connect(self.run_selected)
        self.btn_run_all.clicked.connect(self.run_all)
        self.btn_stop_all.clicked.connect(self.stop_all)

        # Concurrency control
        self.conc_spin = QtWidgets.QSpinBox()
        self.conc_spin.setRange(1, 32)
        self.conc_spin.setValue(int(self.concurrency))
        self.conc_spin.valueChanged.connect(self._set_concurrency)

        top_row = QtWidgets.QHBoxLayout()
        top_row.addWidget(self.btn_add_job)
        top_row.addWidget(self.btn_edit_job)
        top_row.addWidget(self.btn_del_job)
        top_row.addStretch(1)
        top_row.addWidget(QtWidgets.QLabel("Concurrency:"))
        top_row.addWidget(self.conc_spin)
        top_row.addWidget(self.btn_run_job)
        top_row.addWidget(self.btn_run_all)
        top_row.addWidget(self.btn_stop_all)

        self.status_lbl = QtWidgets.QLabel("Idle.")
        self.progress_bar = QtWidgets.QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_detail = QtWidgets.QLabel("")

        self.log_box = QtWidgets.QPlainTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setMaximumBlockCount(8000)

        right = QtWidgets.QVBoxLayout()
        right.addWidget(QtWidgets.QLabel("Jobs"))
        right.addWidget(self.job_table, 1)
        right.addLayout(top_row)
        right.addWidget(self.status_lbl)
        right.addWidget(self.progress_bar)
        right.addWidget(self.progress_detail)
        right.addWidget(QtWidgets.QLabel("Log"))
        right.addWidget(self.log_box, 1)

        right_w = QtWidgets.QWidget()
        right_w.setLayout(right)

        splitter = QtWidgets.QSplitter()
        splitter.addWidget(left_w)
        splitter.addWidget(right_w)
        splitter.setStretchFactor(1, 1)
        self.setCentralWidget(splitter)

        self._running = 0

    def closeEvent(self, event):
        save_config(self.servers, self.jobs, int(self.conc_spin.value()))
        self.stop_all()
        event.accept()

    def log(self, s: str):
        self.log_box.appendPlainText(s)

    def _set_concurrency(self, v: int):
        self.pool.setMaxThreadCount(int(v))
        save_config(self.servers, self.jobs, int(v))

    def _refresh_servers(self):
        self.server_list.clear()
        for s in self.servers:
            self.server_list.addItem(f"{s.name}  ({s.protocol.upper()} {s.host}:{s.port})")

    def _refresh_jobs(self):
        self.job_table.setRowCount(0)
        for j in self.jobs:
            r = self.job_table.rowCount()
            self.job_table.insertRow(r)
            vals = [
                j.server_name,
                j.remote_dir,
                j.mask,
                j.local_dir,
                j.direction,
                "Yes" if j.recursive else "No",
                "Yes" if j.monitor else "No",
                str(j.interval_sec),
            ]
            for c, v in enumerate(vals):
                self.job_table.setItem(r, c, QtWidgets.QTableWidgetItem(str(v)))

    def _selected_server_index(self) -> int:
        return self.server_list.currentRow()

    def _selected_job_index(self) -> int:
        rows = self.job_table.selectionModel().selectedRows()
        if not rows:
            return -1
        return rows[0].row()

    def _find_server(self, name: str) -> Optional[ServerProfile]:
        for s in self.servers:
            if s.name == name:
                return s
        return None

    # ---------------- server actions ----------------
    def add_server(self):
        dlg = ServerDialog(self)
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            s = dlg.get_value()
            if not s.name:
                QtWidgets.QMessageBox.warning(self, "Error", "Name required.")
                return
            if self._find_server(s.name):
                QtWidgets.QMessageBox.warning(self, "Error", "Name exists.")
                return
            self.servers.append(s)
            self._refresh_servers()
            save_config(self.servers, self.jobs, int(self.conc_spin.value()))

    def edit_server(self):
        idx = self._selected_server_index()
        if idx < 0:
            return
        # list shows extra text; map index to servers
        server = self.servers[idx]
        dlg = ServerDialog(self, server)
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            s = dlg.get_value()
            if not s.name:
                QtWidgets.QMessageBox.warning(self, "Error", "Name required.")
                return
            if s.name != server.name and self._find_server(s.name):
                QtWidgets.QMessageBox.warning(self, "Error", "Name exists.")
                return
            old = server.name
            self.servers[idx] = s
            for j in self.jobs:
                if j.server_name == old:
                    j.server_name = s.name
            self._refresh_servers()
            self._refresh_jobs()
            save_config(self.servers, self.jobs, int(self.conc_spin.value()))

    def del_server(self):
        idx = self._selected_server_index()
        if idx < 0:
            return
        name = self.servers[idx].name
        if QtWidgets.QMessageBox.question(self, "Delete", f"Delete server '{name}'?") != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.servers.pop(idx)
        self.jobs = [j for j in self.jobs if j.server_name != name]
        self._refresh_servers()
        self._refresh_jobs()
        save_config(self.servers, self.jobs, int(self.conc_spin.value()))

    def test_connect(self):
        idx = self._selected_server_index()
        if idx < 0:
            return
        prof = self.servers[idx]
        try:
            c = make_client(prof)
            c.connect()
            c.close()
            QtWidgets.QMessageBox.information(self, "OK", f"Connected OK: {prof.protocol.upper()} {prof.host}:{prof.port}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Failed", str(e))

    # ---------------- job actions ----------------
    def add_job(self):
        if not self.servers:
            QtWidgets.QMessageBox.warning(self, "No servers", "Add a server first.")
            return
        dlg = JobDialog(self, self.servers)
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            j = dlg.get_value()
            if not j.local_dir:
                QtWidgets.QMessageBox.warning(self, "Error", "Local dir required.")
                return
            self.jobs.append(j)
            self._refresh_jobs()
            save_config(self.servers, self.jobs, int(self.conc_spin.value()))

    def edit_job(self):
        idx = self._selected_job_index()
        if idx < 0:
            return
        dlg = JobDialog(self, self.servers, self.jobs[idx])
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            j = dlg.get_value()
            if not j.local_dir:
                QtWidgets.QMessageBox.warning(self, "Error", "Local dir required.")
                return
            self.jobs[idx] = j
            self._refresh_jobs()
            save_config(self.servers, self.jobs, int(self.conc_spin.value()))

    def del_job(self):
        idx = self._selected_job_index()
        if idx < 0:
            return
        j = self.jobs[idx]
        if QtWidgets.QMessageBox.question(self, "Delete", f"Delete job:\n{j.server_name} {j.remote_dir}?") != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.jobs.pop(idx)
        self._refresh_jobs()
        save_config(self.servers, self.jobs, int(self.conc_spin.value()))

    # ---------------- run actions ----------------
    def run_selected(self):
        idx = self._selected_job_index()
        if idx < 0:
            return
        self._start_jobs([self.jobs[idx]])

    def run_all(self):
        if not self.jobs:
            return
        self._start_jobs(self.jobs)

    def stop_all(self):
        self.stop_event.set()
        self.status_lbl.setText("Stopping...")
        self.btn_stop_all.setEnabled(False)

    def _start_jobs(self, jobs: List[SyncJob]):
        # reset stop flag, start runnables
        self.stop_event.clear()
        self.btn_stop_all.setEnabled(True)

        started = 0
        for job in jobs:
            prof = self._find_server(job.server_name)
            if not prof:
                self.log(f"[ERROR] Server not found for job: {job.server_name}")
                continue

            r = JobRunnable(prof, job, self.stop_event)
            r.signals.log.connect(self.log)
            r.signals.status.connect(self._on_status)
            r.signals.progress.connect(self._on_progress)
            r.signals.error.connect(self._on_error)
            r.signals.finished.connect(self._on_finished)

            self._running += 1
            self.pool.start(r)
            started += 1

        self.log(f"--- Started {started} job(s) ---")

    def _on_status(self, s: str):
        self.status_lbl.setText(s)

    def _on_progress(self, label: str, done: int, total: int):
        # show a single “latest update” global progress
        if total and total > 0:
            pct = int((done / total) * 100)
            pct = max(0, min(100, pct))
            self.progress_bar.setValue(pct)
            self.progress_detail.setText(f"{label}  {pct}%  ({done}/{total})")
        else:
            self.progress_bar.setValue(0)
            self.progress_detail.setText(f"{label}  {done} bytes")

    def _on_error(self, job_label: str, tb: str):
        self.log(f"[{job_label}] ERROR:\n{tb}")

    def _on_finished(self, job_label: str):
        self.log(f"[{job_label}] FINISHED")
        self._running -= 1
        if self._running <= 0:
            self._running = 0
            self.status_lbl.setText("Idle.")
            self.progress_bar.setValue(0)
            self.progress_detail.setText("")
            self.btn_stop_all.setEnabled(False)


def main():
    app = QtWidgets.QApplication([])
    w = MainWindow()
    w.show()
    app.exec()


if __name__ == "__main__":
    main()