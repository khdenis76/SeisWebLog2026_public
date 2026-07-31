from __future__ import annotations

from pathlib import Path
import traceback
import numpy as np
from PySide6 import QtCore, QtWidgets

from .data import SvpDataRepository
from .cube import build_scalar_cube, ScalarCube

try:
    import pyvista as pv
    from pyvistaqt import QtInteractor
    PYVISTA_AVAILABLE = True
except Exception:
    pv = None
    QtInteractor = None
    PYVISTA_AVAILABLE = False


class WaterColumnWindow(QtWidgets.QMainWindow):
    def __init__(self, project_path: str | Path, parent=None) -> None:
        super().__init__(parent)
        self.project_path = Path(project_path)
        self.repo = SvpDataRepository(project_path)
        self.casts = []
        self.cube: ScalarCube | None = None
        self.setWindowTitle("3D Water Column Viewer")
        self.resize(1450, 900)
        self._build_ui()
        self.reload_data()

    def _build_ui(self) -> None:
        root = QtWidgets.QWidget(); layout = QtWidgets.QHBoxLayout(root); layout.setContentsMargins(5,5,5,5)
        controls = QtWidgets.QWidget(); controls.setMaximumWidth(310); form = QtWidgets.QVBoxLayout(controls)
        title = QtWidgets.QLabel("SVP 3D Water Column"); title.setStyleSheet("font-size:18px;font-weight:600"); form.addWidget(title)
        self.info = QtWidgets.QLabel(); self.info.setWordWrap(True); form.addWidget(self.info)
        layers = QtWidgets.QGroupBox("Layers"); ll = QtWidgets.QVBoxLayout(layers)
        self.show_cube = QtWidgets.QCheckBox("Interpolated volume"); self.show_cube.setChecked(True)
        self.show_casts = QtWidgets.QCheckBox("SVP casts"); self.show_casts.setChecked(True)
        self.show_rp = QtWidgets.QCheckBox("RP Preplot"); self.show_rp.setChecked(True)
        self.show_slice = QtWidgets.QCheckBox("Horizontal slice")
        for w in (self.show_cube,self.show_casts,self.show_rp,self.show_slice): ll.addWidget(w); w.toggled.connect(self.render_scene)
        form.addWidget(layers)
        cube_box = QtWidgets.QGroupBox("Cube generation"); cf = QtWidgets.QFormLayout(cube_box)
        self.parameter = QtWidgets.QComboBox(); self.parameter.addItems(["Velocity", "Salinity", "Temperature"]); self.parameter.setCurrentText("Velocity")
        self.method = QtWidgets.QComboBox(); self.method.addItems(["IDW", "Nearest"])
        self.nx = QtWidgets.QSpinBox(); self.nx.setRange(10,150); self.nx.setValue(40)
        self.ny = QtWidgets.QSpinBox(); self.ny.setRange(10,150); self.ny.setValue(40)
        self.nz = QtWidgets.QSpinBox(); self.nz.setRange(10,250); self.nz.setValue(80)
        self.opacity = QtWidgets.QDoubleSpinBox(); self.opacity.setRange(.01,1); self.opacity.setSingleStep(.05); self.opacity.setValue(.25)
        self.cmap = QtWidgets.QComboBox(); self.cmap.addItems(["turbo", "viridis", "plasma", "coolwarm", "jet"])
        for label,w in (("Parameter",self.parameter),("Interpolation",self.method),("X cells",self.nx),("Y cells",self.ny),("Depth cells",self.nz),("Opacity",self.opacity),("Color map",self.cmap)): cf.addRow(label,w)
        self.build_btn = QtWidgets.QPushButton("Build / Rebuild cube"); self.build_btn.clicked.connect(self.build_cube); cf.addRow(self.build_btn)
        form.addWidget(cube_box)
        slice_box = QtWidgets.QGroupBox("Horizontal slice"); sf=QtWidgets.QVBoxLayout(slice_box)
        self.depth_slider=QtWidgets.QSlider(QtCore.Qt.Orientation.Horizontal); self.depth_slider.setRange(0,1000); self.depth_slider.valueChanged.connect(self._slice_changed)
        self.depth_label=QtWidgets.QLabel("Depth: 0 m"); sf.addWidget(self.depth_label); sf.addWidget(self.depth_slider); form.addWidget(slice_box)
        self.export_btn=QtWidgets.QPushButton("Export interpolated cube to VTI"); self.export_btn.clicked.connect(self.export_vti); form.addWidget(self.export_btn)
        self.reload_btn=QtWidgets.QPushButton("Reload SVP data"); self.reload_btn.clicked.connect(self.reload_data); form.addWidget(self.reload_btn)
        form.addStretch(1); layout.addWidget(controls)
        if PYVISTA_AVAILABLE:
            self.plotter = QtInteractor(root); self.plotter.set_background("#111317"); layout.addWidget(self.plotter.interactor, 1)
        else:
            self.plotter = None
            missing = QtWidgets.QTextBrowser(); missing.setHtml("<h2>PyVista is not installed</h2><p>Install the 3D dependencies:</p><pre>pip install pyvista pyvistaqt vtk scipy</pre><p>The rest of DataViewer2 can continue to run without them.</p>")
            layout.addWidget(missing,1)
        self.setCentralWidget(root)
        self.opacity.valueChanged.connect(self.render_scene); self.cmap.currentTextChanged.connect(self.render_scene); self.parameter.currentTextChanged.connect(self._parameter_changed)

    def reload_data(self) -> None:
        try:
            self.casts = self.repo.load_casts()
            self.rp_x, self.rp_y = self.repo.load_rp_preplot()
            self.info.setText(f"Project: {self.project_path.name}\nSVP casts: {len(self.casts)}\nRP points: {len(self.rp_x)}")
            self.cube = None
            if self.casts and PYVISTA_AVAILABLE:
                self.build_cube()
            else:
                self.render_scene()
        except Exception as exc:
            self.info.setText(str(exc)); QtWidgets.QMessageBox.warning(self,"SVP data",str(exc))

    def _parameter_changed(self, parameter: str) -> None:
        self.cube = None
        self.show_cube.setText(f"{parameter} volume")
        self.build_btn.setText(f"Build / Rebuild {parameter.lower()} cube")
        self.export_btn.setText(f"Export {parameter.lower()} cube to VTI")
        if self.casts and PYVISTA_AVAILABLE:
            self.build_cube()

    def build_cube(self) -> None:
        if not self.casts: return
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.CursorShape.WaitCursor)
        try:
            self.cube = build_scalar_cube(self.casts,self.parameter.currentText(),self.nx.value(),self.ny.value(),self.nz.value(),self.method.currentText())
            self.depth_slider.setRange(0, max(0,len(self.cube.depth)-1)); self.depth_slider.setValue(len(self.cube.depth)//2)
            self.statusBar().showMessage(f"Built {self.cube.parameter} cube ({self.cube.unit})", 5000)
            self.render_scene()
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self,"Build interpolation cube",f"{exc}\n\n{traceback.format_exc()[-2000:]}")
        finally:
            QtWidgets.QApplication.restoreOverrideCursor()

    def _grid(self):
        c=self.cube
        grid=pv.ImageData(dimensions=(len(c.x),len(c.y),len(c.depth)), spacing=(float(c.x[1]-c.x[0]),float(c.y[1]-c.y[0]),float(c.depth[1]-c.depth[0])), origin=(float(c.x[0]),float(c.y[0]),0.0))
        grid.point_data[c.scalar_name] = c.values.transpose(2,1,0).ravel(order="F")
        return grid

    def render_scene(self) -> None:
        if not self.plotter: return
        self.plotter.clear()
        if self.show_rp.isChecked() and getattr(self,"rp_x",np.empty(0)).size:
            pts=np.c_[self.rp_x,self.rp_y,np.zeros_like(self.rp_x)]; self.plotter.add_points(pts,color="white",point_size=2,opacity=.25,render_points_as_spheres=False,label="RP Preplot")
        if self.show_casts.isChecked() and self.casts:
            pts=np.asarray([[c.x,c.y,0] for c in self.casts]); self.plotter.add_points(pts,color="orange",point_size=10,render_points_as_spheres=True,label="SVP casts")
            for c in self.casts:
                self.plotter.add_lines(np.asarray([[c.x,c.y,0],[c.x,c.y,-float(np.nanmax(c.depth))]]),color="orange",width=1)
        if self.cube is not None:
            grid=self._grid(); scalar_name=self.cube.scalar_name; values=grid.point_data[scalar_name]; finite=values[np.isfinite(values)]
            clim=(float(np.nanpercentile(finite,2)),float(np.nanpercentile(finite,98))) if finite.size else None
            if self.show_cube.isChecked(): self.plotter.add_volume(grid,scalars=scalar_name,cmap=self.cmap.currentText(),opacity=float(self.opacity.value()),clim=clim,shade=False)
            if self.show_slice.isChecked():
                idx=min(self.depth_slider.value(),len(self.cube.depth)-1); z=-float(self.cube.depth[idx])
                plane=grid.slice(normal=(0,0,1),origin=(float(np.mean(self.cube.x)),float(np.mean(self.cube.y)),float(self.cube.depth[idx])))
                plane.points[:,2]=z; self.plotter.add_mesh(plane,scalars=scalar_name,cmap=self.cmap.currentText(),clim=clim)
        self.plotter.show_axes(); self.plotter.reset_camera(); self.plotter.render()

    def _slice_changed(self,index:int) -> None:
        if self.cube is not None and len(self.cube.depth): self.depth_label.setText(f"Depth: {self.cube.depth[min(index,len(self.cube.depth)-1)]:.1f} m")
        if self.show_slice.isChecked(): self.render_scene()

    def export_vti(self) -> None:
        if self.cube is None or not PYVISTA_AVAILABLE:
            QtWidgets.QMessageBox.information(self,"Export","Build a cube first."); return
        default_name = f"{self.cube.file_stem}.vti"
        path,_=QtWidgets.QFileDialog.getSaveFileName(self,f"Export {self.cube.parameter.lower()} cube",str(self.project_path / default_name),"VTK Image Data (*.vti)")
        if path:
            if not path.lower().endswith('.vti'): path += '.vti'
            self._grid().save(path); self.statusBar().showMessage(f"Saved {path}",5000)
