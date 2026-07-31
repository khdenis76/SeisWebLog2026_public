from __future__ import annotations

from pathlib import Path
import traceback
import numpy as np
from PySide6 import QtCore, QtGui, QtWidgets

from .data import Track3DRepository, Track3DDataError, DsrLine3D, BBoxTrack3D

try:
    import pyvista as pv
    from pyvistaqt import QtInteractor
    PYVISTA_AVAILABLE = True
except Exception:
    pv = None
    QtInteractor = None
    PYVISTA_AVAILABLE = False


class DsrBBox3DWindow(QtWidgets.QMainWindow):
    COLORS = ['#00d4ff', '#ff9f1c', '#7bd389', '#ef476f', '#c77dff', '#ffd166', '#06d6a0', '#f78c6b']

    def __init__(self, project_path: str | Path, parent=None) -> None:
        super().__init__(parent)
        self.project_path = Path(project_path)
        self.repo = Track3DRepository(project_path)
        self.dsr_data: DsrLine3D | None = None
        self.bbox_tracks: list[BBoxTrack3D] = []
        self.setWindowTitle('3D DSR & BlackBox Viewer')
        self.resize(1500, 920)
        self._build_ui()
        self.reload_sources()

    def _build_ui(self) -> None:
        root = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout(root)
        layout.setContentsMargins(5, 5, 5, 5)

        controls = QtWidgets.QScrollArea()
        controls.setWidgetResizable(True)
        controls.setMaximumWidth(350)
        body = QtWidgets.QWidget()
        form = QtWidgets.QVBoxLayout(body)

        title = QtWidgets.QLabel('3D DSR + BBOX Tracks')
        title.setStyleSheet('font-size:18px;font-weight:600')
        form.addWidget(title)
        self.info = QtWidgets.QLabel()
        self.info.setWordWrap(True)
        form.addWidget(self.info)

        dsr_box = QtWidgets.QGroupBox('DSR receiver line')
        dsr_form = QtWidgets.QFormLayout(dsr_box)
        self.line_combo = QtWidgets.QComboBox(); self.line_combo.setEditable(True)
        self.position_combo = QtWidgets.QComboBox(); self.position_combo.addItems([o[0] for o in self.repo.dsr_position_options()])
        self.show_dsr = QtWidgets.QCheckBox('Show DSR line'); self.show_dsr.setChecked(True)
        self.dsr_points = QtWidgets.QCheckBox('Show station points'); self.dsr_points.setChecked(True)
        self.dsr_labels = QtWidgets.QCheckBox('Show station labels')
        self.load_dsr_btn = QtWidgets.QPushButton('Load DSR line')
        dsr_form.addRow('Line', self.line_combo)
        dsr_form.addRow('Position', self.position_combo)
        dsr_form.addRow(self.show_dsr)
        dsr_form.addRow(self.dsr_points)
        dsr_form.addRow(self.dsr_labels)
        dsr_form.addRow(self.load_dsr_btn)
        form.addWidget(dsr_box)

        bbox_box = QtWidgets.QGroupBox('BlackBox X / Y / Z tracks')
        bbox_form = QtWidgets.QVBoxLayout(bbox_box)
        file_row = QtWidgets.QFormLayout()
        self.file_combo = QtWidgets.QComboBox()
        file_row.addRow('File', self.file_combo)
        bbox_form.addLayout(file_row)
        self.track_list = QtWidgets.QListWidget()
        self.track_list.setMinimumHeight(170)
        bbox_form.addWidget(self.track_list)
        self.show_bbox_points = QtWidgets.QCheckBox('Show BBOX samples')
        self.show_bbox_lines = QtWidgets.QCheckBox('Show BBOX lines'); self.show_bbox_lines.setChecked(True)
        bbox_form.addWidget(self.show_bbox_lines); bbox_form.addWidget(self.show_bbox_points)
        self.load_bbox_btn = QtWidgets.QPushButton('Load selected BBOX file')
        bbox_form.addWidget(self.load_bbox_btn)
        form.addWidget(bbox_box)

        display_box = QtWidgets.QGroupBox('3D display')
        display_form = QtWidgets.QFormLayout(display_box)
        self.z_mode = QtWidgets.QComboBox(); self.z_mode.addItems(['Use source Z', 'Depth below surface (-Z)', 'Absolute depth (-|Z|)'])
        self.vertical_exaggeration = QtWidgets.QDoubleSpinBox(); self.vertical_exaggeration.setRange(0.1, 100.0); self.vertical_exaggeration.setValue(1.0); self.vertical_exaggeration.setSingleStep(0.5)
        self.line_width = QtWidgets.QDoubleSpinBox(); self.line_width.setRange(1.0, 12.0); self.line_width.setValue(3.0)
        self.point_size = QtWidgets.QDoubleSpinBox(); self.point_size.setRange(2.0, 20.0); self.point_size.setValue(7.0)
        self.show_grid = QtWidgets.QCheckBox('Show grid'); self.show_grid.setChecked(True)
        display_form.addRow('Z convention', self.z_mode)
        display_form.addRow('Vertical exaggeration', self.vertical_exaggeration)
        display_form.addRow('Line width', self.line_width)
        display_form.addRow('Point size', self.point_size)
        display_form.addRow(self.show_grid)
        form.addWidget(display_box)

        action_row = QtWidgets.QHBoxLayout()
        self.plot_btn = QtWidgets.QPushButton('Plot / Refresh')
        self.zoom_btn = QtWidgets.QPushButton('Zoom all')
        action_row.addWidget(self.plot_btn); action_row.addWidget(self.zoom_btn)
        form.addLayout(action_row)
        self.export_btn = QtWidgets.QPushButton('Export screenshot')
        form.addWidget(self.export_btn)
        form.addStretch(1)
        controls.setWidget(body)
        layout.addWidget(controls)

        if PYVISTA_AVAILABLE:
            self.plotter = QtInteractor(root)
            self.plotter.set_background('#111317')
            layout.addWidget(self.plotter.interactor, 1)
        else:
            self.plotter = None
            message = QtWidgets.QTextBrowser()
            message.setHtml('<h2>PyVista is not installed</h2><p>Install:</p><pre>pip install pyvista pyvistaqt vtk</pre>')
            layout.addWidget(message, 1)
        self.setCentralWidget(root)

        self.load_dsr_btn.clicked.connect(self.load_dsr)
        self.load_bbox_btn.clicked.connect(self.load_bbox)
        self.file_combo.currentIndexChanged.connect(self.load_bbox)
        self.plot_btn.clicked.connect(self.render_scene)
        self.zoom_btn.clicked.connect(lambda: self.plotter.reset_camera() if self.plotter else None)
        self.export_btn.clicked.connect(self.export_screenshot)
        for widget in (self.show_dsr, self.dsr_points, self.dsr_labels, self.show_bbox_points, self.show_bbox_lines, self.show_grid):
            widget.toggled.connect(self.render_scene)
        self.track_list.itemChanged.connect(lambda *_: self.render_scene())
        self.z_mode.currentTextChanged.connect(self.render_scene)
        self.vertical_exaggeration.valueChanged.connect(self.render_scene)
        self.line_width.valueChanged.connect(self.render_scene)
        self.point_size.valueChanged.connect(self.render_scene)

    def reload_sources(self) -> None:
        try:
            lines = self.repo.dsr_lines()
            self.line_combo.clear(); self.line_combo.addItems([str(v) for v in lines])
            files = self.repo.bbox_files()
            self.file_combo.blockSignals(True); self.file_combo.clear()
            for f in files:
                self.file_combo.addItem(f'{f.label} | {f.row_count:,} rows', f.file_id)
            self.file_combo.blockSignals(False)
            self.info.setText(f'Database: {self.repo.db_path}\nDSR lines: {len(lines)}\nBlackBox files: {len(files)}')
            if lines: self.load_dsr()
            if files: self.load_bbox()
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, '3D data', str(exc))

    def load_dsr(self) -> None:
        try:
            text = self.line_combo.currentText().strip()
            if not text: return
            self.dsr_data = self.repo.load_dsr_line(int(float(text)), self.position_combo.currentText())
            self.statusBar().showMessage(f'Loaded DSR line {self.dsr_data.line}: {self.dsr_data.x.size:,} positions', 5000)
            self.render_scene()
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, 'Load DSR line', str(exc))

    def load_bbox(self) -> None:
        try:
            file_id = self.file_combo.currentData()
            if file_id is None: return
            self.bbox_tracks = self.repo.load_bbox_tracks(int(file_id))
            self.track_list.blockSignals(True); self.track_list.clear()
            for track in self.bbox_tracks:
                item = QtWidgets.QListWidgetItem(f'{track.name} ({track.x.size:,}) — {track.z_source}')
                item.setData(QtCore.Qt.ItemDataRole.UserRole, track.name)
                item.setFlags(item.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
                item.setCheckState(QtCore.Qt.CheckState.Checked)
                self.track_list.addItem(item)
            self.track_list.blockSignals(False)
            self.statusBar().showMessage(f'Loaded {len(self.bbox_tracks)} BBOX coordinate series', 5000)
            self.render_scene()
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, 'Load BlackBox tracks', f'{exc}\n\n{traceback.format_exc()[-1800:]}')

    def _z(self, values: np.ndarray) -> np.ndarray:
        z = np.asarray(values, float).copy()
        mode = self.z_mode.currentText()
        if mode == 'Depth below surface (-Z)': z = -z
        elif mode == 'Absolute depth (-|Z|)': z = -np.abs(z)
        return z * float(self.vertical_exaggeration.value())

    def _selected_track_names(self) -> set[str]:
        return {self.track_list.item(i).data(QtCore.Qt.ItemDataRole.UserRole) for i in range(self.track_list.count()) if self.track_list.item(i).checkState() == QtCore.Qt.CheckState.Checked}

    def render_scene(self) -> None:
        if not self.plotter: return
        self.plotter.clear()
        width = float(self.line_width.value()); point_size = float(self.point_size.value())
        if self.show_dsr.isChecked() and self.dsr_data is not None:
            d = self.dsr_data
            pts = np.c_[d.x, d.y, self._z(d.z)]
            if pts.shape[0] >= 2:
                poly = pv.lines_from_points(pts, close=False)
                self.plotter.add_mesh(poly, color='#ffffff', line_width=width, label=f'DSR {d.line} — {d.label}')
            if self.dsr_points.isChecked():
                self.plotter.add_points(pts, color='#ffffff', point_size=point_size, render_points_as_spheres=True)
            if self.dsr_labels.isChecked() and pts.shape[0] <= 300:
                labels = [str(int(s)) if np.isfinite(s) else '' for s in d.station]
                self.plotter.add_point_labels(pts, labels, font_size=9, text_color='white', point_size=0, shape=None)

        selected = self._selected_track_names()
        for idx, track in enumerate(self.bbox_tracks):
            if track.name not in selected: continue
            pts = np.c_[track.x, track.y, self._z(track.z)]
            finite = np.isfinite(pts).all(axis=1); pts = pts[finite]
            if pts.shape[0] < 1: continue
            color = self.COLORS[idx % len(self.COLORS)]
            if self.show_bbox_lines.isChecked() and pts.shape[0] >= 2:
                self.plotter.add_mesh(pv.lines_from_points(pts, close=False), color=color, line_width=width, label=track.name)
            if self.show_bbox_points.isChecked():
                self.plotter.add_points(pts, color=color, point_size=max(2.0, point_size * 0.7), render_points_as_spheres=False)
        if self.show_grid.isChecked():
            self.plotter.show_grid(xlabel='Easting', ylabel='Northing', zlabel='Z / Depth', color='gray')
        else:
            self.plotter.show_axes()
        try: self.plotter.add_legend(bcolor='#20242a', face=None)
        except Exception: pass
        self.plotter.reset_camera(); self.plotter.render()

    def export_screenshot(self) -> None:
        if not self.plotter: return
        default = self.project_path / 'dsr_bbox_3d.png'
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, 'Export 3D screenshot', str(default), 'PNG image (*.png);;JPEG image (*.jpg)')
        if path:
            self.plotter.screenshot(path)
            self.statusBar().showMessage(f'Saved {path}', 5000)
