from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
import json
import math
import numpy as np
from PySide6 import QtCore, QtGui, QtWidgets

from .surface_data import SurfaceDataRepository, SurfaceDataError, SurfacePoints

try:
    import pyvista as pv
    from pyvistaqt import QtInteractor
    PYVISTA_AVAILABLE = True
except Exception:
    pv = None
    QtInteractor = None
    PYVISTA_AVAILABLE = False

try:
    from scipy.spatial import cKDTree
except Exception:
    cKDTree = None


@dataclass
class SceneDefinition:
    name: str
    kind: str
    source: str
    x_field: str
    y_field: str
    z_field: str
    color_field: str = ""
    end_x_field: str = ""
    end_y_field: str = ""
    end_z_field: str = ""
    line_filter: int | None = None
    color: str = "#00e5ff"
    style_mode: str = "Single symbol"
    category_field: str = ""
    symbol: str = "Sphere"
    cmap: str = "turbo"
    opacity: float = 0.85
    size: float = 7.0
    width: float = 1.5
    z_mode: str = "Use source Z"
    z_scale: float = 1.0
    z_offset: float = 0.0
    style: str = "Surface"
    labels: bool = False
    label_field: str = ""
    label_field2: str = ""
    label_separator: str = " / "
    max_labels: int = 100
    visible: bool = True


class Surface3DWindow(QtWidgets.QMainWindow):
    """Multi-surface 3D scene with point/vector overlays and optional labels."""

    COLORS = ["#00e5ff", "#ffd740", "#ff6e40", "#ea80fc", "#69f0ae", "#82b1ff", "#ff80ab", "#b2ff59"]

    def __init__(self, project_path: str | Path, parent=None) -> None:
        super().__init__(parent)
        self.project_path = Path(project_path)
        self.repo = SurfaceDataRepository(project_path)
        self.definitions: list[SceneDefinition] = []
        self.data_cache: dict[str, SurfacePoints] = {}
        self.origin = np.zeros(2, float)
        self.config_path = self.repo.project_path / "config" / "dataviewer2_surfaces.json"
        self.setWindowTitle("3D Surface Workbench — DataViewer 2")
        self.resize(1550, 930)
        self._build_ui()
        self._load_sources()
        self._load_config()

    def _build_ui(self) -> None:
        root = QtWidgets.QWidget(); outer = QtWidgets.QHBoxLayout(root); outer.setContentsMargins(4,4,4,4)
        left = QtWidgets.QTabWidget(); left.setMinimumWidth(370); left.setMaximumWidth(420); outer.addWidget(left)

        sp = QtWidgets.QWidget(); sl = QtWidgets.QVBoxLayout(sp)
        box=QtWidgets.QGroupBox("Surface definition"); f=QtWidgets.QFormLayout(box)
        self.s_source=QtWidgets.QComboBox(); self.s_x=QtWidgets.QComboBox(); self.s_y=QtWidgets.QComboBox(); self.s_z=QtWidgets.QComboBox(); self.s_color_field=QtWidgets.QComboBox()
        self.s_line=QtWidgets.QComboBox(); self.s_line.addItem("All lines",None)
        self.s_cmap=QtWidgets.QComboBox(); self.s_cmap.addItems(["turbo","viridis","terrain","gist_earth","plasma","inferno"])
        self.s_style=QtWidgets.QComboBox(); self.s_style.addItems(["Surface","Surface + edges","Points"])
        self.s_opacity=QtWidgets.QDoubleSpinBox(); self.s_opacity.setRange(0.05,1); self.s_opacity.setValue(.85); self.s_opacity.setSingleStep(.05)
        self.s_scale=QtWidgets.QDoubleSpinBox(); self.s_scale.setRange(.01,100); self.s_scale.setValue(1); self.s_scale.setSingleStep(.5)
        self.s_offset=QtWidgets.QDoubleSpinBox(); self.s_offset.setRange(-100000,100000); self.s_offset.setValue(0)
        self.s_zmode=QtWidgets.QComboBox(); self.s_zmode.addItems(["Use source Z","Depth below sea level (-|Z|)","Invert source Z (-Z)"])
        self.s_name=QtWidgets.QLineEdit(); self.s_name.setPlaceholderText("Automatic name")
        for label,w in (("Source",self.s_source),("X field",self.s_x),("Y field",self.s_y),("Z field",self.s_z),("Color field",self.s_color_field),("Line filter",self.s_line),("Style",self.s_style),("Color map",self.s_cmap),("Opacity",self.s_opacity),("Vertical scale",self.s_scale),("Z offset",self.s_offset),("Z convention",self.s_zmode),("Name",self.s_name)): f.addRow(label+":",w)
        sl.addWidget(box); self.add_surface_btn=QtWidgets.QPushButton("Add surface"); sl.addWidget(self.add_surface_btn)
        left.addTab(sp,"Add surface")

        op=QtWidgets.QWidget(); ol=QtWidgets.QVBoxLayout(op)
        ob=QtWidgets.QGroupBox("Overlay definition"); of=QtWidgets.QFormLayout(ob)
        self.o_type=QtWidgets.QComboBox(); self.o_type.addItems(["Point","Vector"])
        self.o_source=QtWidgets.QComboBox(); self.o_x=QtWidgets.QComboBox(); self.o_y=QtWidgets.QComboBox(); self.o_z=QtWidgets.QComboBox()
        self.o_ex=QtWidgets.QComboBox(); self.o_ey=QtWidgets.QComboBox(); self.o_ez=QtWidgets.QComboBox()
        self.o_line=QtWidgets.QComboBox(); self.o_line.addItem("All lines",None)
        self.o_color=QtWidgets.QPushButton("#00e5ff"); self.o_color_value="#00e5ff"
        self.o_style_mode=QtWidgets.QComboBox(); self.o_style_mode.addItems(["Single symbol","Categorized"])
        self.o_category_field=QtWidgets.QComboBox(); self.o_category_field.addItem("(none)","")
        self.o_symbol=QtWidgets.QComboBox(); self.o_symbol.addItems(["Sphere","Cube","Cone","Diamond"])
        self.o_size=QtWidgets.QDoubleSpinBox(); self.o_size.setRange(1,30); self.o_size.setValue(8)
        self.o_width=QtWidgets.QDoubleSpinBox(); self.o_width.setRange(.2,12); self.o_width.setValue(1.5)
        self.o_zmode=QtWidgets.QComboBox(); self.o_zmode.addItems(["Use source Z","Depth below sea level (-|Z|)","Invert source Z (-Z)","Fixed Z = 0"])
        self.o_zscale=QtWidgets.QDoubleSpinBox(); self.o_zscale.setRange(.01,100); self.o_zscale.setValue(1.0); self.o_zscale.setSingleStep(.5)
        self.o_zoffset=QtWidgets.QDoubleSpinBox(); self.o_zoffset.setRange(-100000,100000); self.o_zoffset.setValue(0.0)
        self.o_labels=QtWidgets.QCheckBox("Show labels")
        self.o_label_field=QtWidgets.QComboBox()
        self.o_label_field2=QtWidgets.QComboBox(); self.o_label_field2.addItem("(none)","")
        self.o_label_separator=QtWidgets.QLineEdit(" / "); self.o_label_separator.setMaximumWidth(80)
        self.o_max_labels=QtWidgets.QSpinBox(); self.o_max_labels.setRange(1,1000); self.o_max_labels.setValue(100)
        self.o_name=QtWidgets.QLineEdit(); self.o_name.setPlaceholderText("Automatic name")
        for label,w in (("Type",self.o_type),("Source",self.o_source),("Start/point X",self.o_x),("Start/point Y",self.o_y),("Start/point Z",self.o_z),("Vector end X",self.o_ex),("Vector end Y",self.o_ey),("Vector end Z",self.o_ez),("Line filter",self.o_line),("Color",self.o_color),("Style mode",self.o_style_mode),("Category field",self.o_category_field),("Point symbol",self.o_symbol),("Point size",self.o_size),("Line width",self.o_width),("Z convention",self.o_zmode),("Z scale",self.o_zscale),("Z offset",self.o_zoffset),("Label field 1",self.o_label_field),("Label field 2",self.o_label_field2),("Label separator",self.o_label_separator),("Maximum labels",self.o_max_labels),("Name",self.o_name)): of.addRow(label+":",w)
        of.addRow(self.o_labels); ol.addWidget(ob); self.add_overlay_btn=QtWidgets.QPushButton("Add overlay"); ol.addWidget(self.add_overlay_btn)
        left.addTab(op,"Add overlay")

        scene=QtWidgets.QWidget(); scl=QtWidgets.QVBoxLayout(scene)
        self.scene_tree=QtWidgets.QTreeWidget(); self.scene_tree.setHeaderLabels(["Scene object","Type"]); self.scene_tree.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu); scl.addWidget(self.scene_tree)
        row=QtWidgets.QHBoxLayout(); self.refresh_btn=QtWidgets.QPushButton("Refresh"); self.zoom_btn=QtWidgets.QPushButton("Zoom all"); self.top_btn=QtWidgets.QPushButton("Top view"); row.addWidget(self.refresh_btn); row.addWidget(self.zoom_btn); row.addWidget(self.top_btn); scl.addLayout(row)
        self.grid_check=QtWidgets.QCheckBox("Show axes/grid"); self.grid_check.setChecked(True); scl.addWidget(self.grid_check)
        self.screenshot_btn=QtWidgets.QPushButton("Export screenshot"); scl.addWidget(self.screenshot_btn)
        left.addTab(scene,"Scene")

        if PYVISTA_AVAILABLE:
            self.plotter=QtInteractor(root); self.plotter.set_background("#111317"); outer.addWidget(self.plotter.interactor,1)
            try:
                self.plotter.enable_point_picking(
                    callback=self._picked_point,
                    show_message=False,
                    show_point=False,
                    pickable_window=True,
                )
            except Exception:
                pass
        else:
            self.plotter=None; msg=QtWidgets.QTextBrowser(); msg.setHtml("<h2>3D dependencies are not installed</h2><pre>python -m pip install pyvista pyvistaqt vtk scipy</pre>"); outer.addWidget(msg,1)
        self.setCentralWidget(root)

        self.s_source.currentTextChanged.connect(self._surface_source_changed); self.o_source.currentTextChanged.connect(self._overlay_source_changed); self.o_type.currentTextChanged.connect(self._overlay_type_changed)
        self.o_color.clicked.connect(self._choose_color); self.add_surface_btn.clicked.connect(self.add_surface); self.add_overlay_btn.clicked.connect(self.add_overlay)
        self.refresh_btn.clicked.connect(self.render_scene); self.zoom_btn.clicked.connect(lambda:self.plotter.reset_camera() if self.plotter else None); self.top_btn.clicked.connect(lambda:self.plotter.view_xy() if self.plotter else None)
        self.grid_check.toggled.connect(self.render_scene); self.screenshot_btn.clicked.connect(self.export_screenshot); self.scene_tree.itemChanged.connect(self._visibility_changed); self.scene_tree.customContextMenuRequested.connect(self._scene_menu)

    def _load_sources(self):
        sources=self.repo.available_sources(); self.s_source.addItems(sources); self.o_source.addItems(sources)
        if sources: self._surface_source_changed(sources[0]); self._overlay_source_changed(sources[0])

    @staticmethod
    def _set_combo(combo, values, selected=""):
        combo.blockSignals(True); combo.clear(); combo.addItems(values)
        if selected:
            i=combo.findText(selected)
            if i>=0: combo.setCurrentIndex(i)
        combo.blockSignals(False)

    def _surface_source_changed(self,source):
        fields=self.repo.numeric_columns(source); x,y,z=self.repo.default_fields(source)
        for combo,sel in ((self.s_x,x),(self.s_y,y),(self.s_z,z),(self.s_color_field,z)): self._set_combo(combo,fields,sel)
        self.s_line.clear(); self.s_line.addItem("All lines",None)
        for v in self.repo.line_values(source): self.s_line.addItem(str(v),v)
        note=self.repo.source_note(source)
        if note:self.statusBar().showMessage(note,6000)

    def _overlay_source_changed(self,source):
        fields=self.repo.numeric_columns(source); x,y,z=self.repo.default_fields(source)
        for combo,sel in ((self.o_x,x),(self.o_y,y),(self.o_z,z),(self.o_ex,"PrimaryEasting"),(self.o_ey,"PrimaryNorthing"),(self.o_ez,"PrimaryElevation")): self._set_combo(combo,fields,sel)
        candidates=self.repo.label_candidates(source)
        self._set_combo(self.o_label_field,candidates)
        self.o_label_field2.blockSignals(True); self.o_label_field2.clear(); self.o_label_field2.addItem("(none)","")
        for value in candidates: self.o_label_field2.addItem(value,value)
        self.o_label_field2.blockSignals(False)
        self.o_category_field.blockSignals(True); self.o_category_field.clear(); self.o_category_field.addItem("(none)","")
        for value in candidates: self.o_category_field.addItem(value,value)
        self.o_category_field.blockSignals(False)
        note=self.repo.source_note(source)
        if note:self.statusBar().showMessage(note,6000)
        self.o_line.clear(); self.o_line.addItem("All lines",None)
        for v in self.repo.line_values(source): self.o_line.addItem(str(v),v)

    def _overlay_type_changed(self,value):
        enabled=value=="Vector"
        for w in (self.o_ex,self.o_ey,self.o_ez): w.setEnabled(enabled)

    def _choose_color(self):
        c=QtWidgets.QColorDialog.getColor(QtGui.QColor(self.o_color_value),self)
        if c.isValid(): self.o_color_value=c.name(); self.o_color.setText(c.name()); self.o_color.setStyleSheet(f"background:{c.name()}")

    def _z(self,values,mode,scale=1.0,offset=0.0):
        z=np.asarray(values,float).copy()
        if mode=="Depth below sea level (-|Z|)": z=-np.abs(z)
        elif mode=="Invert source Z (-Z)": z=-z
        elif mode=="Fixed Z = 0": z[:]=0
        return z*float(scale)+float(offset)

    def add_surface(self):
        name=self.s_name.text().strip() or f"{self.s_source.currentText()} — {self.s_z.currentText()}"
        definition=SceneDefinition(name,"surface",self.s_source.currentText(),self.s_x.currentText(),self.s_y.currentText(),self.s_z.currentText(),color_field=self.s_color_field.currentText(),line_filter=self.s_line.currentData(),cmap=self.s_cmap.currentText(),opacity=float(self.s_opacity.value()),z_mode=self.s_zmode.currentText(),z_scale=float(self.s_scale.value()),z_offset=float(self.s_offset.value()),style=self.s_style.currentText())
        self._append_definition(definition)

    def add_overlay(self):
        kind=self.o_type.currentText().lower(); name=self.o_name.text().strip() or f"{self.o_source.currentText()} — {self.o_type.currentText()} {len(self.definitions)+1}"
        definition=SceneDefinition(name,kind,self.o_source.currentText(),self.o_x.currentText(),self.o_y.currentText(),self.o_z.currentText(),end_x_field=self.o_ex.currentText(),end_y_field=self.o_ey.currentText(),end_z_field=self.o_ez.currentText(),line_filter=self.o_line.currentData(),color=self.o_color_value,style_mode=self.o_style_mode.currentText(),category_field=str(self.o_category_field.currentData() or ""),symbol=self.o_symbol.currentText(),size=float(self.o_size.value()),width=float(self.o_width.value()),z_mode=self.o_zmode.currentText(),z_scale=float(self.o_zscale.value()),z_offset=float(self.o_zoffset.value()),labels=self.o_labels.isChecked(),label_field=self.o_label_field.currentText(),label_field2=str(self.o_label_field2.currentData() or ""),label_separator=self.o_label_separator.text(),max_labels=int(self.o_max_labels.value()))
        self._append_definition(definition)

    def _append_definition(self,d):
        self.definitions=[x for x in self.definitions if x.name!=d.name]; self.definitions.append(d); self._rebuild_tree(); self._save_config(); self.render_scene()

    def _rebuild_tree(self):
        self.scene_tree.blockSignals(True); self.scene_tree.clear()
        groups={k:QtWidgets.QTreeWidgetItem([k.title()+"s",""]) for k in ("surface","point","vector")}
        for g in groups.values(): self.scene_tree.addTopLevelItem(g); g.setExpanded(True)
        for d in self.definitions:
            item=QtWidgets.QTreeWidgetItem([d.name,d.kind]); item.setData(0,QtCore.Qt.ItemDataRole.UserRole,d.name); item.setFlags(item.flags()|QtCore.Qt.ItemFlag.ItemIsUserCheckable); item.setCheckState(0,QtCore.Qt.CheckState.Checked if d.visible else QtCore.Qt.CheckState.Unchecked); groups[d.kind].addChild(item)
        self.scene_tree.blockSignals(False)

    def _visibility_changed(self,item,column):
        name=item.data(0,QtCore.Qt.ItemDataRole.UserRole)
        if not name:return
        d=next((x for x in self.definitions if x.name==name),None)
        if d: d.visible=item.checkState(0)==QtCore.Qt.CheckState.Checked; self._save_config(); self.render_scene()

    def _scene_menu(self,pos):
        item=self.scene_tree.itemAt(pos); name=item.data(0,QtCore.Qt.ItemDataRole.UserRole) if item else None
        if not name:return
        menu=QtWidgets.QMenu(self); duplicate=menu.addAction("Duplicate"); delete=menu.addAction("Delete")
        action=menu.exec(self.scene_tree.viewport().mapToGlobal(pos))
        d=next((x for x in self.definitions if x.name==name),None)
        if action is delete: self.definitions=[x for x in self.definitions if x.name!=name]; self._rebuild_tree(); self._save_config(); self.render_scene()
        elif action is duplicate and d:
            raw=asdict(d); raw["name"]=d.name+" copy"; self.definitions.append(SceneDefinition(**raw)); self._rebuild_tree(); self._save_config(); self.render_scene()

    def _load_data(self,d):
        key=f"{d.source}|{d.x_field}|{d.y_field}|{d.z_field}|{d.color_field}|{d.label_field}|{d.label_field2}|{d.label_separator}|{d.category_field}|{d.line_filter}"
        if key not in self.data_cache:
            label_fields=[field for field in (d.label_field,d.label_field2) if d.labels and field]
            self.data_cache[key]=self.repo.load_points(d.source,d.x_field,d.y_field,d.z_field,color_field=d.color_field or d.z_field,label_fields=label_fields,label_separator=d.label_separator,metadata_fields=[d.category_field] if d.category_field else None,line_filter=d.line_filter,max_points=150000)
        return self.data_cache[key]

    def _add_symbol_points(self, pts, color, size, symbol, label):
        if symbol == "Sphere":
            self.plotter.add_points(pts, color=color, point_size=size, render_points_as_spheres=True, label=label)
            return
        cloud = pv.PolyData(pts)
        scale = max(float(size) * 0.08, 0.1)
        if symbol == "Cube":
            geom = pv.Cube(x_length=scale, y_length=scale, z_length=scale)
        elif symbol == "Cone":
            geom = pv.Cone(height=scale * 1.6, radius=scale * 0.6, direction=(0, 0, 1))
        else:
            geom = pv.Octahedron().scale(scale, inplace=False)
        glyph = cloud.glyph(scale=False, orient=False, geom=geom)
        self.plotter.add_mesh(glyph, color=color, label=label)

    def render_scene(self):
        if not self.plotter:return
        self.plotter.clear(); visible=[d for d in self.definitions if d.visible]
        all_xy=[]
        for d in visible:
            try:
                p=self._load_data(d); all_xy.append(np.c_[p.x,p.y])
            except Exception: pass
        if all_xy:
            xy=np.vstack(all_xy); self.origin=np.array([np.nanmedian(xy[:,0]),np.nanmedian(xy[:,1])])
        for d in visible:
            try:
                p=self._load_data(d); x=p.x-self.origin[0]; y=p.y-self.origin[1]; z=self._z(p.z,d.z_mode,d.z_scale,d.z_offset); pts=np.c_[x,y,z]; finite=np.isfinite(pts).all(axis=1); pts=pts[finite]; scalars=p.color_values[finite]
                if pts.shape[0]<1:continue
                if d.kind=="surface":
                    cloud=pv.PolyData(pts); cloud["values"]=scalars
                    if pts.shape[0]>=3:
                        mesh=cloud.delaunay_2d()
                        self.plotter.add_mesh(
                            mesh, scalars="values", cmap=d.cmap, opacity=d.opacity,
                            show_edges=d.style=="Surface + edges",
                            point_size=4 if d.style!="Points" else 7,
                            render_points_as_spheres=False, label=d.name,
                            scalar_bar_args={
                                "title": d.color_field or d.z_field,
                                "color": "white",
                                "title_font_size": 12,
                                "label_font_size": 10,
                                "fmt": "%.4g",
                            },
                        )
                    else:self.plotter.add_points(cloud,color=d.color,point_size=d.size,label=d.name)
                elif d.kind=="point":
                    if d.style_mode == "Categorized" and d.category_field in p.metadata:
                        categories = np.asarray(p.metadata[d.category_field], dtype=object)[finite]
                        unique_values = list(dict.fromkeys(str(v) if v is not None else "NULL" for v in categories))
                        symbols = ["Sphere", "Cube", "Cone", "Diamond"]
                        for category_index, category in enumerate(unique_values):
                            mask = np.asarray([(str(v) if v is not None else "NULL") == category for v in categories], dtype=bool)
                            color = self.COLORS[category_index % len(self.COLORS)]
                            symbol = symbols[category_index % len(symbols)]
                            self._add_symbol_points(pts[mask], color, d.size, symbol, f"{d.name} — {category}")
                    else:
                        self._add_symbol_points(pts, d.color, d.size, d.symbol, d.name)
                    if d.labels and p.labels.size:
                        idx=np.linspace(0,pts.shape[0]-1,min(pts.shape[0],d.max_labels),dtype=int); labels=[str(p.labels[finite][i]) for i in idx]
                        self.plotter.add_point_labels(pts[idx],labels,font_size=9,text_color="white",point_size=0,shape="rounded_rect",fill_shape=True,shape_color="#111317",shape_opacity=0.75)
                elif d.kind=="vector":
                    end=self.repo.load_points(d.source,d.end_x_field,d.end_y_field,d.end_z_field or d.z_field,line_filter=d.line_filter,max_points=150000)
                    n=min(pts.shape[0],end.x.size,5000); starts=pts[:n]; ends=np.c_[end.x[:n]-self.origin[0],end.y[:n]-self.origin[1],self._z(end.z[:n],d.z_mode,d.z_scale,d.z_offset)]; vectors=ends-starts
                    pdata=pv.PolyData(starts); pdata["vectors"]=vectors; glyph=pdata.glyph(orient="vectors",scale=False,factor=1.0,geom=pv.Arrow(tip_length=.25,tip_radius=.08,shaft_radius=.025)); self.plotter.add_mesh(glyph,color=d.color,opacity=d.opacity,label=d.name)
            except Exception as exc:
                self.statusBar().showMessage(f"{d.name}: {exc}",6000)
        if self.grid_check.isChecked(): self.plotter.show_grid(xlabel=f"Local Easting + {self.origin[0]:.2f}",ylabel=f"Local Northing + {self.origin[1]:.2f}",zlabel="Z")
        else:self.plotter.show_axes()
        try:self.plotter.add_legend(bcolor="#20242a")
        except Exception:pass
        self.plotter.reset_camera(); self.plotter.render()

    def _picked_point(self, point) -> None:
        try:
            values=np.asarray(point,dtype=float).reshape(-1)
            if values.size < 3 or not np.isfinite(values[:3]).all():
                return
            easting=values[0]+self.origin[0]
            northing=values[1]+self.origin[1]
            self.statusBar().showMessage(
                f"Easting {easting:,.3f}   |   Northing {northing:,.3f}   |   Z {values[2]:,.3f}",
                10000,
            )
        except Exception:
            pass

    def _save_config(self):
        try:
            self.config_path.parent.mkdir(parents=True,exist_ok=True)
            payload={}
            if self.config_path.exists():
                try:payload=json.loads(self.config_path.read_text(encoding="utf-8"))
                except Exception:payload={}
            payload["surface_3d"]=[asdict(d) for d in self.definitions]
            self.config_path.write_text(json.dumps(payload,indent=2,ensure_ascii=False),encoding="utf-8")
        except Exception:pass

    def _load_config(self):
        if not self.config_path.exists():return
        try:
            payload=json.loads(self.config_path.read_text(encoding="utf-8")); self.definitions=[SceneDefinition(**raw) for raw in payload.get("surface_3d",[])]; self._rebuild_tree()
            if self.definitions: QtCore.QTimer.singleShot(0,self.render_scene)
        except Exception:pass

    def export_screenshot(self):
        if not self.plotter:return
        path,_=QtWidgets.QFileDialog.getSaveFileName(self,"Export 3D screenshot",str(self.repo.project_path/"surface3d.png"),"PNG (*.png);;JPEG (*.jpg)")
        if path:self.plotter.screenshot(path)
