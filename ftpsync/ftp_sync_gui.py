import os
import json
import time
import fnmatch
import traceback
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Tuple

from PySide6 import QtCore, QtWidgets
from ftplib import FTP, FTP_TLS, error_perm


CONFIG_FILE = "ftp_sync_config.json"


# ----------------------------
# Data models
# ----------------------------
@dataclass
class FtpServerProfile:
    name: str
    host: str
    port: int = 21
    user: str = "anonymous"
    password: str = ""
    tls: bool = False
    passive: bool = True
    timeout: int = 30


@dataclass
class SyncJob:
    server_name: str
    remote_dir: str
    mask: str = "*.*"
    local_dir: str = ""
    direction: str = "download"  # "download" | "upload" | "both"
    monitor: bool = False
    interval_sec: int = 60


# ----------------------------
# FTP helper
# ----------------------------
class FtpClient:
    def __init__(self, profile: FtpServerProfile):
        self.profile = profile
        self.ftp = None

    def connect(self):
        if self.profile.tls:
            ftp = FTP_TLS()
        else:
            ftp = FTP()
        ftp.connect(self.profile.host, self.profile.port, timeout=self.profile.timeout)
        ftp.login(self.profile.user, self.profile.password)
        if self.profile.tls:
            # Encrypt data channel
            ftp.prot_p()
        ftp.set_pasv(self.profile.passive)
        self.ftp = ftp
        return ftp

    def close(self):
        try:
            if self.ftp is not None:
                self.ftp.quit()
        except Exception:
            try:
                if self.ftp is not None:
                    self.ftp.close()
            except Exception:
                pass
        self.ftp = None

    def cwd(self, path: str):
        self.ftp.cwd(path)

    def list_names(self) -> List[str]:
        # Prefer NLST (names only)
        try:
            names = self.ftp.nlst()
            return names
        except error_perm:
            # some servers dislike nlst without args, try LIST parsing
            lines = []
            self.ftp.retrlines("LIST", lines.append)
            # naive parse: last token as name
            out = []
            for ln in lines:
                parts = ln.split()
                if parts:
                    out.append(parts[-1])
            return out

    def size(self, filename: str) -> Optional[int]:
        try:
            return self.ftp.size(filename)
        except Exception:
            return None

    def mdtm_epoch(self, filename: str) -> Optional[int]:
        # MDTM returns YYYYMMDDHHMMSS
        try:
            resp = self.ftp.sendcmd(f"MDTM {filename}")
            # resp like: '213 20260101123456'
            parts = resp.split()
            if len(parts) >= 2 and parts[0] == "213":
                s = parts[1].strip()
                if len(s) == 14:
                    # convert to epoch (UTC)
                    t = time.strptime(s, "%Y%m%d%H%M%S")
                    return int(time.mktime(t))
            return None
        except Exception:
            return None

    def download_file(self, remote_name: str, local_path: str, progress_cb=None):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            def _write(chunk: bytes):
                f.write(chunk)
                if progress_cb:
                    progress_cb(len(chunk))
            self.ftp.retrbinary(f"RETR {remote_name}", _write)

    def upload_file(self, local_path: str, remote_name: str, progress_cb=None):
        with open(local_path, "rb") as f:
            def _read_and_callback(block):
                if progress_cb:
                    progress_cb(len(block))
                return block
            # ftplib calls callback itself; simplest is storbinary without custom iterator
            self.ftp.storbinary(f"STOR {remote_name}", f)


# ----------------------------
# Worker thread
# ----------------------------
class SyncWorker(QtCore.QObject):
    log = QtCore.Signal(str)
    status = QtCore.Signal(str)
    progress = QtCore.Signal(int, int, str)  # done_bytes, total_bytes, label
    finished = QtCore.Signal()
    error = QtCore.Signal(str)

    def __init__(self, profile: FtpServerProfile, job: SyncJob):
        super().__init__()
        self.profile = profile
        self.job = job
        self._stop = False

    @QtCore.Slot()
    def run(self):
        ftp_client = FtpClient(self.profile)
        try:
            self.status.emit("Connecting...")
            ftp_client.connect()
            self.log.emit(f"Connected to {self.profile.host}:{self.profile.port} as {self.profile.user}")

            while not self._stop:
                self._run_one_cycle(ftp_client)

                if not self.job.monitor:
                    break

                self.status.emit(f"Waiting {self.job.interval_sec}s (monitoring)...")
                for _ in range(self.job.interval_sec):
                    if self._stop:
                        break
                    time.sleep(1)

            self.status.emit("Stopped.")
        except Exception:
            msg = traceback.format_exc()
            self.error.emit(msg)
        finally:
            ftp_client.close()
            self.finished.emit()

    def stop(self):
        self._stop = True

    def _run_one_cycle(self, ftp_client: FtpClient):
        job = self.job
        if not job.remote_dir:
            self.log.emit("Remote dir is empty. Skipping.")
            return
        if not job.local_dir:
            self.log.emit("Local dir is empty. Skipping.")
            return

        self.status.emit("Listing remote...")
        ftp_client.cwd(job.remote_dir)
        names = ftp_client.list_names()

        # Filter by mask and exclude '.' '..'
        mask = job.mask.strip() or "*.*"
        remote_files = [n for n in names if n not in (".", "..") and fnmatch.fnmatch(n, mask)]

        self.log.emit(f"Remote files matched ({mask}): {len(remote_files)}")

        # Decide transfers
        os.makedirs(job.local_dir, exist_ok=True)

        if job.direction in ("download", "both"):
            self._do_downloads(ftp_client, remote_files)

        if self._stop:
            return

        if job.direction in ("upload", "both"):
            self._do_uploads(ftp_client, mask)

        self.status.emit("Cycle complete.")

    def _do_downloads(self, ftp_client: FtpClient, remote_files: List[str]):
        job = self.job
        self.status.emit("Checking downloads...")
        to_download = []

        for name in remote_files:
            remote_size = ftp_client.size(name)
            remote_m = ftp_client.mdtm_epoch(name)
            local_path = os.path.join(job.local_dir, name)

            if not os.path.exists(local_path):
                to_download.append((name, local_path, remote_size))
                continue

            # Compare size or mtime if available
            local_size = os.path.getsize(local_path)
            local_m = int(os.path.getmtime(local_path))

            if remote_size is not None and local_size != remote_size:
                to_download.append((name, local_path, remote_size))
            elif remote_m is not None and remote_m > local_m + 1:
                to_download.append((name, local_path, remote_size))

        self.log.emit(f"Downloads needed: {len(to_download)}")
        for name, local_path, remote_size in to_download:
            if self._stop:
                return
            label = f"DL {name}"
            self.status.emit(f"Downloading {name}...")
            done = 0
            total = remote_size or 0

            def cb(nbytes):
                nonlocal done
                done += nbytes
                # if total unknown, just show done
                self.progress.emit(done, total, label)

            ftp_client.download_file(name, local_path, progress_cb=cb)
            # Set local mtime to remote mdtm if we have it
            rm = ftp_client.mdtm_epoch(name)
            if rm is not None:
                try:
                    os.utime(local_path, (rm, rm))
                except Exception:
                    pass
            self.log.emit(f"Downloaded: {name} -> {local_path}")

    def _do_uploads(self, ftp_client: FtpClient, mask: str):
        job = self.job
        self.status.emit("Checking uploads...")

        # Remote list again for existence checks
        try:
            remote_names = set(ftp_client.list_names())
        except Exception:
            remote_names = set()

        local_files = [
            f for f in os.listdir(job.local_dir)
            if os.path.isfile(os.path.join(job.local_dir, f)) and fnmatch.fnmatch(f, mask)
        ]

        to_upload = []
        for f in local_files:
            local_path = os.path.join(job.local_dir, f)
            local_size = os.path.getsize(local_path)
            local_m = int(os.path.getmtime(local_path))

            if f not in remote_names:
                to_upload.append((f, local_path, local_size))
                continue

            remote_size = ftp_client.size(f)
            remote_m = ftp_client.mdtm_epoch(f)

            if remote_size is not None and remote_size != local_size:
                to_upload.append((f, local_path, local_size))
            elif remote_m is not None and local_m > remote_m + 1:
                to_upload.append((f, local_path, local_size))

        self.log.emit(f"Uploads needed: {len(to_upload)}")
        for name, local_path, local_size in to_upload:
            if self._stop:
                return
            label = f"UL {name}"
            self.status.emit(f"Uploading {name}...")
            done = 0
            total = local_size

            def cb(nbytes):
                nonlocal done
                done += nbytes
                self.progress.emit(done, total, label)

            # (ftplib storbinary doesn't give per-chunk callback easily without custom file wrapper;
            # this cb might not be called. Keep it for future extension.)
            ftp_client.upload_file(local_path, name, progress_cb=None)
            self.progress.emit(total, total, label)
            self.log.emit(f"Uploaded: {local_path} -> {name}")


# ----------------------------
# Config persistence
# ----------------------------
def load_config() -> Tuple[List[FtpServerProfile], List[SyncJob]]:
    if not os.path.exists(CONFIG_FILE):
        return [], []
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    servers = [FtpServerProfile(**x) for x in data.get("servers", [])]
    jobs = [SyncJob(**x) for x in data.get("jobs", [])]
    return servers, jobs


def save_config(servers: List[FtpServerProfile], jobs: List[SyncJob]) -> None:
    data = {
        "servers": [asdict(s) for s in servers],
        "jobs": [asdict(j) for j in jobs],
    }
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# ----------------------------
# UI dialogs
# ----------------------------
class ServerDialog(QtWidgets.QDialog):
    def __init__(self, parent=None, server: Optional[FtpServerProfile] = None):
        super().__init__(parent)
        self.setWindowTitle("FTP Server")
        self.setModal(True)

        self.name = QtWidgets.QLineEdit()
        self.host = QtWidgets.QLineEdit()
        self.port = QtWidgets.QSpinBox()
        self.port.setRange(1, 65535)
        self.port.setValue(21)

        self.user = QtWidgets.QLineEdit()
        self.password = QtWidgets.QLineEdit()
        self.password.setEchoMode(QtWidgets.QLineEdit.EchoMode.Password)

        self.tls = QtWidgets.QCheckBox("Use TLS (FTPS)")
        self.passive = QtWidgets.QCheckBox("Passive mode")
        self.passive.setChecked(True)

        self.timeout = QtWidgets.QSpinBox()
        self.timeout.setRange(1, 9999)
        self.timeout.setValue(30)

        form = QtWidgets.QFormLayout()
        form.addRow("Name:", self.name)
        form.addRow("Host:", self.host)
        form.addRow("Port:", self.port)
        form.addRow("User:", self.user)
        form.addRow("Password:", self.password)
        form.addRow("", self.tls)
        form.addRow("", self.passive)
        form.addRow("Timeout (s):", self.timeout)

        btns = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.StandardButton.Ok |
            QtWidgets.QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(btns)

        if server:
            self.name.setText(server.name)
            self.host.setText(server.host)
            self.port.setValue(server.port)
            self.user.setText(server.user)
            self.password.setText(server.password)
            self.tls.setChecked(server.tls)
            self.passive.setChecked(server.passive)
            self.timeout.setValue(server.timeout)

    def get_value(self) -> FtpServerProfile:
        return FtpServerProfile(
            name=self.name.text().strip(),
            host=self.host.text().strip(),
            port=int(self.port.value()),
            user=self.user.text().strip(),
            password=self.password.text(),
            tls=bool(self.tls.isChecked()),
            passive=bool(self.passive.isChecked()),
            timeout=int(self.timeout.value()),
        )


class JobDialog(QtWidgets.QDialog):
    def __init__(self, parent=None, servers: List[FtpServerProfile] = None, job: Optional[SyncJob] = None):
        super().__init__(parent)
        self.setWindowTitle("Sync Job")
        self.setModal(True)
        servers = servers or []

        self.server = QtWidgets.QComboBox()
        self.server.addItems([s.name for s in servers])

        self.remote_dir = QtWidgets.QLineEdit("/")
        self.mask = QtWidgets.QLineEdit("*.*")
        self.local_dir = QtWidgets.QLineEdit()
        self.local_browse = QtWidgets.QPushButton("Browse...")

        self.direction = QtWidgets.QComboBox()
        self.direction.addItems(["download", "upload", "both"])

        self.monitor = QtWidgets.QCheckBox("Monitor (polling)")
        self.interval = QtWidgets.QSpinBox()
        self.interval.setRange(5, 999999)
        self.interval.setValue(60)

        self.local_browse.clicked.connect(self._pick_local)

        row_local = QtWidgets.QHBoxLayout()
        row_local.addWidget(self.local_dir, 1)
        row_local.addWidget(self.local_browse)

        form = QtWidgets.QFormLayout()
        form.addRow("Server:", self.server)
        form.addRow("Remote dir:", self.remote_dir)
        form.addRow("Mask:", self.mask)
        form.addRow("Local dir:", row_local)
        form.addRow("Direction:", self.direction)
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
            idx = self.server.findText(job.server_name)
            if idx >= 0:
                self.server.setCurrentIndex(idx)
            self.remote_dir.setText(job.remote_dir)
            self.mask.setText(job.mask)
            self.local_dir.setText(job.local_dir)
            self.direction.setCurrentText(job.direction)
            self.monitor.setChecked(job.monitor)
            self.interval.setValue(job.interval_sec)

    def _pick_local(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Select local folder", self.local_dir.text() or os.getcwd())
        if d:
            self.local_dir.setText(d)

    def get_value(self) -> SyncJob:
        return SyncJob(
            server_name=self.server.currentText(),
            remote_dir=self.remote_dir.text().strip() or "/",
            mask=self.mask.text().strip() or "*.*",
            local_dir=self.local_dir.text().strip(),
            direction=self.direction.currentText(),
            monitor=self.monitor.isChecked(),
            interval_sec=int(self.interval.value()),
        )


# ----------------------------
# Main window
# ----------------------------
class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("FTP Folder Sync (PySide6)")
        self.resize(1100, 650)

        self.servers, self.jobs = load_config()

        # Left: servers
        self.server_list = QtWidgets.QListWidget()
        self.server_list.setMinimumWidth(260)
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
        left.addWidget(QtWidgets.QLabel("FTP Servers"))
        left.addWidget(self.server_list, 1)
        left.addLayout(srv_btn_row)

        left_w = QtWidgets.QWidget()
        left_w.setLayout(left)

        # Right: jobs table
        self.job_table = QtWidgets.QTableWidget(0, 6)
        self.job_table.setHorizontalHeaderLabels(["Server", "Remote dir", "Mask", "Local dir", "Direction", "Monitor"])
        self.job_table.horizontalHeader().setStretchLastSection(True)
        self.job_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.job_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self._refresh_jobs()

        self.btn_add_job = QtWidgets.QPushButton("Add Job")
        self.btn_edit_job = QtWidgets.QPushButton("Edit Job")
        self.btn_del_job = QtWidgets.QPushButton("Delete Job")
        self.btn_run_job = QtWidgets.QPushButton("Run Selected")
        self.btn_stop = QtWidgets.QPushButton("Stop")
        self.btn_stop.setEnabled(False)

        self.btn_add_job.clicked.connect(self.add_job)
        self.btn_edit_job.clicked.connect(self.edit_job)
        self.btn_del_job.clicked.connect(self.del_job)
        self.btn_run_job.clicked.connect(self.run_job)
        self.btn_stop.clicked.connect(self.stop_job)

        job_btn_row = QtWidgets.QHBoxLayout()
        job_btn_row.addWidget(self.btn_add_job)
        job_btn_row.addWidget(self.btn_edit_job)
        job_btn_row.addWidget(self.btn_del_job)
        job_btn_row.addStretch(1)
        job_btn_row.addWidget(self.btn_run_job)
        job_btn_row.addWidget(self.btn_stop)

        # Log + progress
        self.status_lbl = QtWidgets.QLabel("Idle.")
        self.progress_bar = QtWidgets.QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)

        self.log_box = QtWidgets.QPlainTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setMaximumBlockCount(5000)

        right = QtWidgets.QVBoxLayout()
        right.addWidget(QtWidgets.QLabel("Sync Jobs"))
        right.addWidget(self.job_table, 1)
        right.addLayout(job_btn_row)
        right.addWidget(self.status_lbl)
        right.addWidget(self.progress_bar)
        right.addWidget(QtWidgets.QLabel("Log"))
        right.addWidget(self.log_box, 1)

        right_w = QtWidgets.QWidget()
        right_w.setLayout(right)

        splitter = QtWidgets.QSplitter()
        splitter.addWidget(left_w)
        splitter.addWidget(right_w)
        splitter.setStretchFactor(1, 1)
        self.setCentralWidget(splitter)

        # Worker thread refs
        self._thread = None
        self._worker = None

    # ---------- helpers ----------
    def closeEvent(self, event):
        save_config(self.servers, self.jobs)
        if self._worker:
            self._worker.stop()
        event.accept()

    def _refresh_servers(self):
        self.server_list.clear()
        for s in self.servers:
            self.server_list.addItem(s.name)

    def _refresh_jobs(self):
        self.job_table.setRowCount(0)
        for j in self.jobs:
            r = self.job_table.rowCount()
            self.job_table.insertRow(r)
            vals = [j.server_name, j.remote_dir, j.mask, j.local_dir, j.direction, "Yes" if j.monitor else "No"]
            for c, v in enumerate(vals):
                self.job_table.setItem(r, c, QtWidgets.QTableWidgetItem(str(v)))

    def _selected_server_index(self) -> int:
        row = self.server_list.currentRow()
        return row

    def _selected_job_index(self) -> int:
        rows = self.job_table.selectionModel().selectedRows()
        if not rows:
            return -1
        return rows[0].row()

    def _find_server(self, name: str) -> Optional[FtpServerProfile]:
        for s in self.servers:
            if s.name == name:
                return s
        return None

    def log(self, msg: str):
        self.log_box.appendPlainText(msg)

    # ---------- server actions ----------
    def add_server(self):
        dlg = ServerDialog(self)
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            s = dlg.get_value()
            if not s.name:
                QtWidgets.QMessageBox.warning(self, "Error", "Server name is required.")
                return
            if self._find_server(s.name):
                QtWidgets.QMessageBox.warning(self, "Error", "Server name already exists.")
                return
            self.servers.append(s)
            self._refresh_servers()
            save_config(self.servers, self.jobs)

    def edit_server(self):
        idx = self._selected_server_index()
        if idx < 0:
            return
        current = self.servers[idx]
        dlg = ServerDialog(self, current)
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            s = dlg.get_value()
            if not s.name:
                QtWidgets.QMessageBox.warning(self, "Error", "Server name is required.")
                return
            # If renamed, ensure unique
            if s.name != current.name and self._find_server(s.name):
                QtWidgets.QMessageBox.warning(self, "Error", "Server name already exists.")
                return
            # Update server + jobs referencing old name
            old_name = current.name
            self.servers[idx] = s
            for j in self.jobs:
                if j.server_name == old_name:
                    j.server_name = s.name
            self._refresh_servers()
            self._refresh_jobs()
            save_config(self.servers, self.jobs)

    def del_server(self):
        idx = self._selected_server_index()
        if idx < 0:
            return
        name = self.servers[idx].name
        if QtWidgets.QMessageBox.question(self, "Delete", f"Delete server '{name}'?") != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.servers.pop(idx)
        # Also remove jobs for that server
        self.jobs = [j for j in self.jobs if j.server_name != name]
        self._refresh_servers()
        self._refresh_jobs()
        save_config(self.servers, self.jobs)

    def test_connect(self):
        idx = self._selected_server_index()
        if idx < 0:
            return
        prof = self.servers[idx]
        self.log(f"Testing connect: {prof.name} ...")
        try:
            c = FtpClient(prof)
            c.connect()
            pwd = c.ftp.pwd()
            c.close()
            QtWidgets.QMessageBox.information(self, "OK", f"Connected OK.\nPWD: {pwd}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Failed", str(e))

    # ---------- job actions ----------
    def add_job(self):
        if not self.servers:
            QtWidgets.QMessageBox.warning(self, "No servers", "Add a server first.")
            return
        dlg = JobDialog(self, self.servers)
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            j = dlg.get_value()
            if not j.local_dir:
                QtWidgets.QMessageBox.warning(self, "Error", "Local dir is required.")
                return
            self.jobs.append(j)
            self._refresh_jobs()
            save_config(self.servers, self.jobs)

    def edit_job(self):
        idx = self._selected_job_index()
        if idx < 0:
            return
        dlg = JobDialog(self, self.servers, self.jobs[idx])
        if dlg.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            j = dlg.get_value()
            if not j.local_dir:
                QtWidgets.QMessageBox.warning(self, "Error", "Local dir is required.")
                return
            self.jobs[idx] = j
            self._refresh_jobs()
            save_config(self.servers, self.jobs)

    def del_job(self):
        idx = self._selected_job_index()
        if idx < 0:
            return
        j = self.jobs[idx]
        if QtWidgets.QMessageBox.question(self, "Delete", f"Delete job:\n{j.server_name} {j.remote_dir} ?") != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.jobs.pop(idx)
        self._refresh_jobs()
        save_config(self.servers, self.jobs)

    def run_job(self):
        if self._worker:
            QtWidgets.QMessageBox.warning(self, "Busy", "A job is already running.")
            return
        idx = self._selected_job_index()
        if idx < 0:
            return
        job = self.jobs[idx]
        prof = self._find_server(job.server_name)
        if not prof:
            QtWidgets.QMessageBox.critical(self, "Error", f"Server profile not found: {job.server_name}")
            return

        self.progress_bar.setValue(0)
        self.status_lbl.setText("Starting...")
        self.log(f"--- RUN JOB: {job.server_name} {job.remote_dir} mask={job.mask} dir={job.direction} monitor={job.monitor} ---")

        self._thread = QtCore.QThread(self)
        self._worker = SyncWorker(prof, job)
        self._worker.moveToThread(self._thread)

        self._thread.started.connect(self._worker.run)
        self._worker.log.connect(self.log)
        self._worker.status.connect(self.status_lbl.setText)
        self._worker.progress.connect(self._on_progress)
        self._worker.error.connect(self._on_error)
        self._worker.finished.connect(self._on_finished)

        self.btn_stop.setEnabled(True)
        self.btn_run_job.setEnabled(False)

        self._thread.start()

    def stop_job(self):
        if self._worker:
            self.log("Stopping...")
            self._worker.stop()
            self.btn_stop.setEnabled(False)

    def _on_progress(self, done: int, total: int, label: str):
        if total and total > 0:
            pct = int((done / total) * 100)
            pct = max(0, min(100, pct))
            self.progress_bar.setValue(pct)
            self.status_lbl.setText(f"{label}: {pct}% ({done}/{total})")
        else:
            # unknown total
            self.progress_bar.setValue(0)
            self.status_lbl.setText(f"{label}: {done} bytes")

    def _on_error(self, msg: str):
        self.log("ERROR:\n" + msg)
        QtWidgets.QMessageBox.critical(self, "Error", "Worker error. See log.")

    def _on_finished(self):
        self.log("--- JOB FINISHED ---")
        self.status_lbl.setText("Idle.")
        self.progress_bar.setValue(0)

        if self._thread:
            self._thread.quit()
            self._thread.wait(2000)
        self._thread = None
        self._worker = None

        self.btn_stop.setEnabled(False)
        self.btn_run_job.setEnabled(True)


def main():
    app = QtWidgets.QApplication([])
    w = MainWindow()
    w.show()
    app.exec()


if __name__ == "__main__":
    main()