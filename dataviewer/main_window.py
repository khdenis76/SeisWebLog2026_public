import math
from pathlib import Path

import numpy as np
import pandas as pd
import pyqtgraph as pg
from PySide6 import QtCore, QtWidgets
from PySide6.QtGui import QAction
from PySide6.QtGui import QCursor

from .plots.plots_manager import PlotManager
from .db.django_db import DjangoDb, DjangoDbError
from .ui.left_panel import LeftPanel
from .ui.central_tabs import CentralTabs
from .ui.right_panel import RightPanel
from .plots.plot_factory import PlotFactory
from .db.project_db import ProjectDb, ProjectDbError
from .ui.dsr_mdi_window import DsrMdiWindow
from .ui.bb_mdi_window import BbMdiWindow
from .config_store import ConfigStore
from .ui.plot_window import PlotWindow
from .config import (
    APP_NAME,
    APP_VERSION,
    DEFAULT_WINDOW_WIDTH,
    DEFAULT_WINDOW_HEIGHT,
    PLOT_QUALITY_PRESETS,
)


def fill_table_from_df(table: QtWidgets.QTableWidget, df: pd.DataFrame, max_rows: int = 500):
    table.clear()
    if df is None or df.empty:
        table.setRowCount(0)
        table.setColumnCount(0)
        return

    preview = df.head(max_rows).copy()
    table.setRowCount(len(preview))
    table.setColumnCount(len(preview.columns))
    table.setHorizontalHeaderLabels(preview.columns.astype(str).tolist())

    for r in range(len(preview)):
        for c in range(len(preview.columns)):
            v = preview.iat[r, c]
            table.setItem(r, c, QtWidgets.QTableWidgetItem("" if pd.isna(v) else str(v)))

    table.resizeColumnsToContents()


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} v{APP_VERSION}")
        self.resize(DEFAULT_WINDOW_WIDTH, DEFAULT_WINDOW_HEIGHT)
        self.quality_mode = "Balanced"
        self.dsr_mdi_window = None
        self.bb_mdi_window = None
        self.config_store = ConfigStore("dataviewer")
        self.plot_settings = self.config_store.load()

        self.projects_df = pd.DataFrame()
        self.current_project = None

        # runtime caches / plot items
        self.dsr_line_df = None
        self.rp_layer = None
        self.rp_circle_item = None
        self.dsr_primary_layer = None
        self.dsr_secondary_layer = None

        self.rp_circle_df = None
        self.rp_circle_radius = 25.0

        self._build_menu()
        self._build_toolbar()
        self._build_ui()
        self._wire()
        try:
            self.left.apply_plot_settings(self.plot_settings)
        except Exception as e:
            print("[Config apply] error:", e)
        self.statusBar().showMessage("Ready")

        # auto-load at start
        self.load_projects()
        self.plot_manager = PlotManager()
        self.depth_windows = {}
        self.plot_settings = {
            "rp": {"point_size": 6},
            "dsr": {
                "depth": True,
                "sigmas": True,
                "radial": True,
                "primary_point_size": 6,
                "secondary_point_size": 6,
            },
            "bb": {
                "bb_vessel": True,
                "bb_rov1_ins": True,
                "bb_rov2_ins": True,
                "bb_rov1_usbl": True,
                "bb_rov2_usbl": True,
            },
        }

    def _build_menu(self):
        menubar = self.menuBar()
        file_menu = menubar.addMenu("&File")

        self.act_exit = QAction("Exit", self)
        file_menu.addAction(self.act_exit)

        view_menu = menubar.addMenu("&View")
        self.act_toggle_left = QAction("Toggle Left Panel", self)
        self.act_toggle_left.setCheckable(True)
        self.act_toggle_left.setChecked(True)
        view_menu.addAction(self.act_toggle_left)

        self.act_toggle_right = QAction("Toggle Right Panel", self)
        self.act_toggle_right.setCheckable(True)
        self.act_toggle_right.setChecked(True)
        view_menu.addAction(self.act_toggle_right)

    def _build_toolbar(self):
        self.toolbar = self.addToolBar("Main")
        self.toolbar.setMovable(False)

        self.act_reload = QAction("Reload", self)
        self.act_zoom_full = QAction("Zoom Full", self)
        self.act_toggle_legend = QAction("Legend", self)
        self.act_toggle_legend.setCheckable(True)
        self.act_toggle_legend.setChecked(True)

        self.cmb_quality = QtWidgets.QComboBox()
        self.cmb_quality.addItems(list(PLOT_QUALITY_PRESETS.keys()))
        self.cmb_quality.setCurrentText(self.quality_mode)
        self.cmb_quality.setToolTip("Plot quality preset")

        self.toolbar.addAction(self.act_reload)
        self.toolbar.addAction(self.act_zoom_full)
        self.toolbar.addAction(self.act_toggle_legend)
        self.toolbar.addSeparator()
        self.toolbar.addWidget(QtWidgets.QLabel("Quality: "))
        self.toolbar.addWidget(self.cmb_quality)

    def _build_ui(self):
        # 3-way splitter: Left panel | Center tabs | Right panel
        self.splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        self.setCentralWidget(self.splitter)

        self.left = LeftPanel()
        self.tabs = CentralTabs()

        # map hover
        self._hover_proxy = pg.SignalProxy(
            self.tabs.map_plot.scene().sigMouseMoved,
            rateLimit=30,
            slot=self._on_map_mouse_moved,
        )

        self.right = RightPanel()

        # Keep map scale equal (Easting/Northing)
        try:
            self.tabs.map_plot.getViewBox().setAspectLocked(True, ratio=1)
            self.tabs.map_plot.getPlotItem().setClipToView(True)
            self.tabs.map_plot.getPlotItem().setDownsampling(auto=True, mode="peak")
        except Exception:
            pass

        self.splitter.addWidget(self.left)
        self.splitter.addWidget(self.tabs)
        self.splitter.addWidget(self.right)

        self.splitter.setStretchFactor(0, 0)
        self.splitter.setStretchFactor(1, 1)
        self.splitter.setStretchFactor(2, 0)
        self.splitter.setSizes([360, 900, 320])
        self.splitter.setChildrenCollapsible(False)

    def _wire(self):
        self.act_exit.triggered.connect(self.close)
        self.act_toggle_left.triggered.connect(self._toggle_left)
        self.act_toggle_right.triggered.connect(self._toggle_right)

        self.right.closeRequested.connect(self._close_right_panel)
        self.act_reload.triggered.connect(self.load_projects)
        self.act_zoom_full.triggered.connect(self._zoom_map_full)
        self.act_toggle_legend.triggered.connect(self._toggle_legend_visibility)
        self.cmb_quality.currentTextChanged.connect(self._on_quality_mode_changed)

        self.left.reloadProjectsClicked.connect(self.load_projects)
        self.left.projectChanged.connect(self.on_project_changed)

        # DSR
        self.left.dsrLineClicked.connect(self.on_dsr_line_clicked)
        self.left.dsrStationClicked.connect(self.on_dsr_station_clicked)
        self.left.dsrSelectionChanged.connect(self.on_dsr_selection_changed)

        # BlackBox
        self.left.blackBoxSelectionChanged.connect(self.on_blackbox_selection_changed)
        self.left.blackBoxRowClicked.connect(self.on_blackbox_row_clicked)

        # Red radius
        self.left.rpRadiusChanged.connect(self._on_rp_radius_changed)
        self.left.plotsSettingsChanged.connect(self.on_plot_settings_changed)

    def _toggle_legend_visibility(self):
        if getattr(self.tabs, "legend", None) is None:
            return
        self.tabs.legend.setVisible(self.act_toggle_legend.isChecked())

    def _zoom_map_full(self):
        try:
            vb = self.tabs.map_plot.getViewBox()
            vb.autoRange()
        except Exception:
            pass

    def _on_quality_mode_changed(self, mode: str):
        if mode in PLOT_QUALITY_PRESETS:
            self.quality_mode = mode
            self.statusBar().showMessage(f"Plot quality: {mode}", 3000)
            try:
                if getattr(self, "current_project", None):
                    self.on_plot_settings_changed(self.plot_settings)
            except Exception:
                pass

    def _quality(self) -> dict:
        return PLOT_QUALITY_PRESETS.get(self.quality_mode, PLOT_QUALITY_PRESETS["Balanced"])

    def _toggle_left(self):
        self.left.setVisible(self.act_toggle_left.isChecked())

    def _toggle_right(self):
        self.right.setVisible(self.act_toggle_right.isChecked())

    def _close_right_panel(self):
        self.right.setVisible(False)
        self.act_toggle_right.setChecked(False)

    def load_projects(self):
        try:
            db = DjangoDb("", "db.sqlite3")
            df = db.read_projects()

            self.projects_df = df
            names = df["name"].astype(str).tolist() if not df.empty else []
            self.left.set_projects(names)

            if df.empty:
                self.left.set_project_dir("-")
                self.current_project = None
                self.statusBar().showMessage("No projects found in core_project", 8000)
                return

            self.left.cmb_project.setCurrentIndex(0)
            self.on_project_changed(self.left.cmb_project.currentText())

            fill_table_from_df(self.tabs.table, df, max_rows=500)
            self.statusBar().showMessage(f"Loaded {len(df)} projects")

        except DjangoDbError as e:
            self.projects_df = pd.DataFrame()
            self.left.set_projects([])
            self.left.set_project_dir("-")
            self.current_project = None
            self.statusBar().showMessage(str(e), 12000)

    def on_project_changed(self, project_name: str):
        if self.projects_df is None or self.projects_df.empty:
            self.current_project = None
            self.left.set_project_dir("-")
            return

        row = self.projects_df[self.projects_df["name"].astype(str) == str(project_name)]
        if row.empty:
            self.current_project = None
            self.left.set_project_dir("-")
            return

        r = row.iloc[0]
        root_path = str(r["root_path"])
        folder_name = str(r["folder_name"])
        project_dir = Path(root_path) / folder_name

        self.current_project = {
            "id": int(r["id"]),
            "name": str(r["name"]),
            "root_path": root_path,
            "folder_name": folder_name,
            "project_dir": str(project_dir),
        }

        self.left.set_project_dir(str(project_dir))
        self.right.set_metric("Project", str(project_name))
        self.right.append_text(f"Loaded project: {project_name}")

        # RPPreplot
        try:
            pdb = ProjectDb(project_dir)
            df = pdb.read_rp_preplot()

            if self.rp_layer:
                if self.rp_layer.get("curve"):
                    self.tabs.map_plot.removeItem(self.rp_layer["curve"])
                if self.rp_layer.get("scatter"):
                    self.tabs.map_plot.removeItem(self.rp_layer["scatter"])
            self.rp_layer = None

            layer = PlotFactory.create_scatter_layer(
                df,
                x_col="X",
                y_col="Y",
                group_col="Line",
                order_col="Point",
                plot_name="RPPreplot",
                plot_id="rp_layer",
                point_size=6,
                point_color="blue",
                line_color="blue",
                line_width=1.5,
                scatter_max_points=self._quality()["rp_scatter_max_points"],
                interactive_max_points=self._quality()["interactive_max_points"],
                interactive=False,
            )
            self.rp_layer = layer
            if layer.get("curve"):
                self.tabs.map_plot.addItem(layer["curve"])
            if layer.get("scatter"):
                self.tabs.map_plot.addItem(layer["scatter"])

            self.rp_circle_df = df

            try:
                self.left.set_rp_radius(self.rp_circle_radius)
            except Exception:
                pass

            self._redraw_rp_circle_layer()
            self.right.set_metric("Visible", f"RP rows: {len(df):,}")

        except ProjectDbError as e:
            self.statusBar().showMessage(str(e), 10000)

        # DSR lines
        try:
            pdb = ProjectDb(project_dir)
            df_sum = pdb.read_v_dsr_lines()
            self.left.set_dsr_lines_table(df_sum)
        except ProjectDbError as e:
            self.statusBar().showMessage(str(e), 10000)
            self.left.set_dsr_lines_table(None)

        # BlackBox files
        try:
            pdb = ProjectDb(project_dir)
            df_bb = pdb.read_blackbox_files()
            self.left.set_blackbox_files_table(df_bb)
        except ProjectDbError as e:
            self.statusBar().showMessage(str(e), 10000)
            self.left.set_blackbox_files_table(None)

        self.statusBar().showMessage(f"Selected project: {self.current_project['name']}")

    def on_dsr_line_clicked(self, line: int):
        if not self.current_project:
            return
        self._show_loading(f"Loading DSR line {line} ...")
        try:
            self.current_dsr_line = int(line)

            pdb = ProjectDb(self.current_project["project_dir"])

            # -----------------------------
            # Station table + DSR line data
            # -----------------------------
            df_st = pdb.read_dsr_stations_for_line(line)
            self.current_dsr_stations_df = df_st.copy() if df_st is not None else None
            self.left.set_dsr_stations_table(df_st)

            self.dsr_line_df = pdb.read_dsr_for_line(line)
            if self.dsr_line_df is None or self.dsr_line_df.empty:
                self.statusBar().showMessage(f"Line {line}: no DSR data found", 8000)
                return

            depth_df, st_col = self._make_station_depth_df(self.dsr_line_df)

            # -----------------------------
            # BlackBox for this line
            # -----------------------------
            bb_sel = self.plot_settings.get("bb", {})

            try:
                self.bb_configs = pdb.get_blackbox_configs_by_dsr_line(line)
                self.bb_line_df = pdb.read_blackbox_for_line_by_date1_window(line)

                if self.bb_line_df is not None and not self.bb_line_df.empty and self.bb_configs:
                    cfg_id = self.bb_configs[0][0]
                    if "config_id" in self.bb_line_df.columns:
                        self.bb_line_df = self.bb_line_df[
                            self.bb_line_df["config_id"] == cfg_id
                            ].copy()

                if bb_sel.get("show_tracks_window", True):
                    self.plot_blackbox_tracks(self.bb_line_df)

                if bb_sel.get("show_timeseries_window", True):
                    self.show_blackbox_timeseries_window(line, df_st=df_st)

            except Exception as e:
                print("[BlackBox load] error:", e)
                self.bb_line_df = None

            # -----------------------------
            # Plot settings
            # -----------------------------
            dsr_sel = self.plot_settings.get("dsr", {})

            # -----------------------------
            # DSR MDI host window
            # -----------------------------
            mdi = self._get_dsr_mdi_window(line)

            base_plot = None

            # -----------------------------
            # Depth vs Station
            # -----------------------------
            if dsr_sel.get("depth", True):
                w_depth = PlotWindow(title=f"Depth vs Station - Line {line}")
                w_depth.state["current_line"] = int(line)
                w_depth.state["on_station_selected"] = (
                    lambda st, ln=line: self.on_dsr_station_clicked(ln, st, from_graph=True)
                )

                PlotFactory.build_two_series_vs_station(
                    w_depth,
                    depth_df,
                    station_col=st_col,
                    series1_col="PrimaryElevation",
                    series2_col="SecondaryElevation",
                    y_label="Elevation",
                    title=f"Depth vs Station - Line {line}",
                    time_col="Deploy T" if "Deploy T" in depth_df.columns else None,
                )

                mdi.add_plot_window(
                    "depth_vs_station",
                    w_depth,
                    f"Depth - Line {line}",
                )
                base_plot = w_depth.plot

            # -----------------------------
            # Sigma vs Station
            # -----------------------------
            if dsr_sel.get("sigmas", True):
                w_sig = PlotWindow(title=f"Sigma vs Station - Line {line}")
                w_sig.state["current_line"] = int(line)
                w_sig.state["on_station_selected"] = (
                    lambda st, ln=line: self.on_dsr_station_clicked(ln, st, from_graph=True)
                )

                PlotFactory.build_two_series_vs_station(
                    w_sig,
                    depth_df,
                    station_col=st_col,
                    series1_col="Sigma1",
                    series2_col="Sigma2",
                    y_label="Sigma",
                    title=f"Sigma vs Station - Line {line}",
                    time_col="Deploy T" if "Deploy T" in depth_df.columns else None,
                )

                mdi.add_plot_window(
                    "sigmas_vs_station",
                    w_sig,
                    f"Sigmas - Line {line}",
                )

                if base_plot is not None:
                    try:
                        w_sig.plot.setXLink(base_plot)
                    except Exception:
                        pass

            # -----------------------------
            # Radial Offset vs Station
            # -----------------------------
            if dsr_sel.get("radial", True):
                w_rad = PlotWindow(title=f"Radial Offset - Line {line}")
                w_rad.state["current_line"] = int(line)
                w_rad.state["on_station_selected"] = (
                    lambda st, ln=line: self.on_dsr_station_clicked(ln, st, from_graph=True)
                )

                PlotFactory.build_two_series_vs_station(
                    w_rad,
                    depth_df,
                    station_col=st_col,
                    series1_col="DeltaEprimarytosecondary",
                    series2_col="DeltaNprimarytosecondary",
                    y_label="Radial Offset",
                    title=f"Radial Offset - Line {line}",
                    time_col="Deploy T" if "Deploy T" in depth_df.columns else None,
                )

                mdi.add_plot_window(
                    "radial_offset_vs_station",
                    w_rad,
                    f"Radial - Line {line}",
                )

                if base_plot is not None:
                    try:
                        w_rad.plot.setXLink(base_plot)
                    except Exception:
                        pass

            # -----------------------------
            # Arrange DSR subwindows
            # -----------------------------
            try:
                mdi.mdi.tileSubWindows()
            except Exception:
                pass

            # -----------------------------
            # Draw DSR points on main map
            # -----------------------------
            self.plot_dsr_primary_secondary(self.dsr_line_df)

            # -----------------------------
            # Initial station selection
            # red marker + table + map + BB marker
            # -----------------------------
            try:
                first_station = None

                if df_st is not None and not df_st.empty:
                    for cand in ("Station", "LinePoint"):
                        if cand in df_st.columns:
                            vals = pd.to_numeric(df_st[cand], errors="coerce").dropna()
                            if not vals.empty:
                                first_station = int(vals.iloc[0])
                                break

                if first_station is None and depth_df is not None and not depth_df.empty and st_col in depth_df.columns:
                    vals = pd.to_numeric(depth_df[st_col], errors="coerce").dropna()
                    if not vals.empty:
                        first_station = int(vals.iloc[0])

                if first_station is not None:
                    self.on_dsr_station_clicked(line, first_station)

            except Exception as e:
                print("[Initial station select] error:", e)

            # -----------------------------
            # Zoom main map to DSR line extent
            # only if station focus did not already handle it
            # -----------------------------
            try:
                dzoom = None

                if "PrimaryEasting" in self.dsr_line_df.columns and "PrimaryNorthing" in self.dsr_line_df.columns:
                    dzoom = self.dsr_line_df[["PrimaryEasting", "PrimaryNorthing"]].copy()
                    dzoom["PrimaryEasting"] = pd.to_numeric(dzoom["PrimaryEasting"], errors="coerce")
                    dzoom["PrimaryNorthing"] = pd.to_numeric(dzoom["PrimaryNorthing"], errors="coerce")
                    dzoom = dzoom.dropna(subset=["PrimaryEasting", "PrimaryNorthing"])

                    if not dzoom.empty:
                        xmin = float(dzoom["PrimaryEasting"].min())
                        xmax = float(dzoom["PrimaryEasting"].max())
                        ymin = float(dzoom["PrimaryNorthing"].min())
                        ymax = float(dzoom["PrimaryNorthing"].max())

                        vb = self.tabs.map_plot.getViewBox()
                        vb.setXRange(xmin, xmax, padding=0.05)
                        vb.setYRange(ymin, ymax, padding=0.05)

                elif "SecondaryEasting" in self.dsr_line_df.columns and "SecondaryNorthing" in self.dsr_line_df.columns:
                    dzoom = self.dsr_line_df[["SecondaryEasting", "SecondaryNorthing"]].copy()
                    dzoom["SecondaryEasting"] = pd.to_numeric(dzoom["SecondaryEasting"], errors="coerce")
                    dzoom["SecondaryNorthing"] = pd.to_numeric(dzoom["SecondaryNorthing"], errors="coerce")
                    dzoom = dzoom.dropna(subset=["SecondaryEasting", "SecondaryNorthing"])

                    if not dzoom.empty:
                        xmin = float(dzoom["SecondaryEasting"].min())
                        xmax = float(dzoom["SecondaryEasting"].max())
                        ymin = float(dzoom["SecondaryNorthing"].min())
                        ymax = float(dzoom["SecondaryNorthing"].max())

                        vb = self.tabs.map_plot.getViewBox()
                        vb.setXRange(xmin, xmax, padding=0.05)
                        vb.setYRange(ymin, ymax, padding=0.05)

            except Exception:
                try:
                    if self.rp_layer and self.rp_layer.get("curve"):
                        bounds = self.rp_layer["curve"].boundingRect()
                        vb = self.tabs.map_plot.getViewBox()
                        vb.setRange(bounds, padding=0.02)
                except Exception:
                    pass

            # -----------------------------
            # Right panel / status
            # -----------------------------
            try:
                self.right.set_metric("Selected Line", str(line))
                self.right.set_metric("Stations", f"{len(df_st):,}" if df_st is not None else "0")
                self.right.set_metric("DSR Rows", f"{len(self.dsr_line_df):,}")
                if self.bb_line_df is not None:
                    self.right.set_metric("BB Rows", f"{len(self.bb_line_df):,}")
            except Exception:
                pass

            self.statusBar().showMessage(
                f"Line {line}: stations {len(df_st) if df_st is not None else 0}, dsr rows {len(self.dsr_line_df)}",
                8000,
            )

        except ProjectDbError as e:
            self.statusBar().showMessage(str(e), 10000)
            self.left.set_dsr_stations_table(None)
            self.dsr_line_df = None
        finally:
            self._hide_loading()

    def on_dsr_selection_changed(self, lines: list):
        self.statusBar().showMessage(f"Selected lines: {', '.join(map(str, lines))}")

    def on_blackbox_selection_changed(self, file_ids: list):
        self.statusBar().showMessage(f"BlackBox selected files: {file_ids}")

    def on_blackbox_row_clicked(self, file_id: int):
        self.statusBar().showMessage(f"BlackBox file clicked: {file_id}")

    def plot_dsr_primary_secondary(self, df):
        if self.dsr_primary_layer:
            if self.dsr_primary_layer.get("curve"):
                self.tabs.map_plot.removeItem(self.dsr_primary_layer["curve"])
            if self.dsr_primary_layer.get("scatter"):
                self.tabs.map_plot.removeItem(self.dsr_primary_layer["scatter"])
        if self.dsr_secondary_layer:
            if self.dsr_secondary_layer.get("curve"):
                self.tabs.map_plot.removeItem(self.dsr_secondary_layer["curve"])
            if self.dsr_secondary_layer.get("scatter"):
                self.tabs.map_plot.removeItem(self.dsr_secondary_layer["scatter"])

        self.dsr_primary_layer = None
        self.dsr_secondary_layer = None
        ps1 = int(self.plot_settings.get("dsr", {}).get("primary_point_size", 6))
        ps2 = int(self.plot_settings.get("dsr", {}).get("secondary_point_size", 6))
        if df is None or df.empty:
            return

        if "PrimaryEasting" in df.columns and "PrimaryNorthing" in df.columns:
            self.dsr_primary_layer = PlotFactory.create_scatter_layer(
                df,
                x_col="PrimaryEasting",
                y_col="PrimaryNorthing",
                connect_points=False,
                plot_name="DSR Primary",
                plot_id="dsr_primary",
                point_size=ps1,
                point_shape="o",
                point_color="yellow",
                scatter_max_points=self._quality()["dsr_scatter_max_points"],
                interactive_max_points=self._quality()["interactive_max_points"],
            )
            if self.dsr_primary_layer["scatter"]:
                self.tabs.map_plot.addItem(self.dsr_primary_layer["scatter"])

        if "SecondaryEasting" in df.columns and "SecondaryNorthing" in df.columns:
            self.dsr_secondary_layer = PlotFactory.create_scatter_layer(
                df,
                x_col="SecondaryEasting",
                y_col="SecondaryNorthing",
                connect_points=False,
                plot_name="DSR Secondary",
                plot_id="dsr_secondary",
                point_size=ps2,
                point_shape="t",
                point_color="cyan",
                scatter_max_points=self._quality()["dsr_scatter_max_points"],
                interactive_max_points=self._quality()["interactive_max_points"],
            )
            if self.dsr_secondary_layer["scatter"]:
                self.tabs.map_plot.addItem(self.dsr_secondary_layer["scatter"])

    def zoom_map_to_xy(self, x: float, y: float, half_size_m: float = 50.0):
        x0 = x - half_size_m
        x1 = x + half_size_m
        y0 = y - half_size_m
        y1 = y + half_size_m

        vb = self.tabs.map_plot.getViewBox()
        vb.setXRange(x0, x1, padding=0)
        vb.setYRange(y0, y1, padding=0)

    def _update_station_selection_lines(self, line: int, station: int):
        st = float(station)

        for key, w in self._iter_dsr_plot_widgets():
            if w is None:
                continue
            if not hasattr(w, "items"):
                continue
            if not hasattr(w, "state"):
                continue

            try:
                if int(w.state.get("current_line", -1)) != int(line):
                    continue
            except Exception:
                continue

            sel = w.items.get("sel_line")
            if sel is None:
                continue

            try:
                sel.setPos(st)
                sel.setVisible(True)
                sel.setZValue(10000)
                w.state["selected_station"] = int(station)

                if hasattr(w, "plot") and w.plot is not None:
                    try:
                        w.plot.getViewBox().update()
                    except Exception:
                        pass
                    try:
                        w.plot.repaint()
                    except Exception:
                        pass
            except Exception:
                continue

    def on_dsr_station_clicked(self, line: int, station: int, from_graph: bool = False):
        if not self.current_project:
            return

        try:
            stf = float(station)
            sti = int(round(float(station)))
        except Exception:
            return

        # -------------------------------------------------
        # 1) Move red vertical line in all DSR MDI graphs
        # -------------------------------------------------
        for key, w in self._iter_dsr_plot_widgets():
            if w is None or not hasattr(w, "items") or not hasattr(w, "state"):
                continue

            try:
                if int(w.state.get("current_line", -1)) != int(line):
                    continue
            except Exception:
                continue

            sel = w.items.get("sel_line")
            if sel is not None:
                try:
                    sel.setPos(stf)
                    sel.setVisible(True)
                    sel.setZValue(10000)
                    w.state["selected_station"] = sti

                    if hasattr(w, "plot") and w.plot is not None:
                        try:
                            w.plot.getViewBox().update()
                        except Exception:
                            pass
                        try:
                            w.plot.repaint()
                        except Exception:
                            pass
                except Exception:
                    pass

        # -------------------------------------------------
        # 2) Highlight/select station row in LEFT table
        # -------------------------------------------------
        table = getattr(self.left, "tbl_dsr_stations", None)
        deploy_ts = None

        if table is not None:
            try:
                table.blockSignals(True)
                table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
                table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
                table.clearSelection()

                station_col = None
                deploy_col = None

                for c in range(table.columnCount()):
                    hdr = table.horizontalHeaderItem(c)
                    txt = hdr.text().strip() if hdr else ""

                    if txt in ("Station", "LinePoint"):
                        station_col = c
                    if txt in ("Deploy T", "DeployT", "DeployTime", "Deploy_Time"):
                        deploy_col = c

                found_row = None
                if station_col is not None:
                    for r in range(table.rowCount()):
                        item = table.item(r, station_col)
                        if item is None:
                            continue

                        try:
                            row_station = int(round(float(item.text())))
                        except Exception:
                            continue

                        if row_station == sti:
                            found_row = r
                            break

                if found_row is not None:
                    table.selectRow(found_row)
                    table.setCurrentCell(found_row, station_col if station_col is not None else 0)

                    item = table.item(found_row, station_col if station_col is not None else 0)
                    if item is not None:
                        table.scrollToItem(
                            item,
                            QtWidgets.QAbstractItemView.ScrollHint.PositionAtCenter,
                        )

                    if deploy_col is not None:
                        dep_item = table.item(found_row, deploy_col)
                        if dep_item is not None:
                            deploy_ts = pd.to_datetime(dep_item.text(), errors="coerce")

            except Exception as e:
                print("[DSR] table highlight error:", e)
            finally:
                try:
                    table.blockSignals(False)
                except Exception:
                    pass

        # -------------------------------------------------
        # 3) Zoom CENTRAL MAP to selected station
        # -------------------------------------------------
        try:
            if self.dsr_line_df is None or self.dsr_line_df.empty:
                self.statusBar().showMessage("No DSR data loaded", 5000)
                return

            d = self.dsr_line_df.copy()

            st_col = None
            for cand in ("Station", "LinePoint"):
                if cand in d.columns:
                    st_col = cand
                    break

            if st_col is None:
                self.statusBar().showMessage("No Station column found in DSR data", 5000)
                return

            d[st_col] = pd.to_numeric(d[st_col], errors="coerce")

            x_col = None
            y_col = None
            if "PrimaryEasting" in d.columns and "PrimaryNorthing" in d.columns:
                x_col, y_col = "PrimaryEasting", "PrimaryNorthing"
            elif "SecondaryEasting" in d.columns and "SecondaryNorthing" in d.columns:
                x_col, y_col = "SecondaryEasting", "SecondaryNorthing"

            if x_col is None or y_col is None:
                self.statusBar().showMessage("No map coordinates found in DSR data", 5000)
                return

            d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
            d[y_col] = pd.to_numeric(d[y_col], errors="coerce")

            ds = d.loc[
                d[st_col].round() == sti,
                [x_col, y_col]
            ].dropna()

            if ds.empty:
                self.statusBar().showMessage(
                    f"No coordinates found for Line {line} Station {sti}",
                    5000,
                )
                return

            x = float(ds[x_col].mean())
            y = float(ds[y_col].mean())

            vb = self.tabs.map_plot.getViewBox()
            half_size = max(float(self.rp_circle_radius) * 2.0, 25.0)

            vb.setXRange(x - half_size, x + half_size, padding=0)
            vb.setYRange(y - half_size, y + half_size, padding=0)
            vb.update()

            try:
                self.tabs.map_plot.repaint()
            except Exception:
                pass

            try:
                self.right.set_coordinates(f"{x:,.2f}", f"{y:,.2f}")
                self.right.set_selection(Line=line, Station=sti)
            except Exception:
                pass

            self.statusBar().showMessage(
                f"Focused map on Line {line} Station {sti}",
                5000,
            )

        except Exception as e:
            print("[DSR] central map zoom error:", e)
            self.statusBar().showMessage(str(e), 5000)

        # -------------------------------------------------
        # 4) Move BB red marker to nearest BB timestamp for this station Deploy T
        # -------------------------------------------------
        try:
            if deploy_ts is not None and pd.notna(
                    deploy_ts) and self.bb_line_df is not None and not self.bb_line_df.empty:
                bb = self.bb_line_df.copy()
                bb["TimeStamp_dt"] = pd.to_datetime(bb["TimeStamp"], errors="coerce")
                bb = bb.dropna(subset=["TimeStamp_dt"])

                if not bb.empty:
                    bb["TimeStampNum"] = bb["TimeStamp_dt"].astype("int64") / 1e9
                    arr = bb["TimeStampNum"].to_numpy(dtype=float)

                    if arr.size:
                        target_num = float(deploy_ts.timestamp())
                        idx = int(np.argmin(np.abs(arr - target_num)))
                        self._set_bb_time_marker(float(arr[idx]))
        except Exception as e:
            print("[BB marker by Deploy T] error:", e)

    def _make_station_depth_df(self, dsr_line_df):
        if "Station" in dsr_line_df.columns:
            st_col = "Station"
        elif "LinePoint" in dsr_line_df.columns:
            st_col = "LinePoint"
        else:
            raise ValueError("Neither Station nor LinePoint found")

        out = (
            dsr_line_df.groupby(st_col, as_index=False)[
                [
                    "PrimaryElevation",
                    "SecondaryElevation",
                    "Sigma1",
                    "Sigma2",
                    "DeltaEprimarytosecondary",
                    "DeltaNprimarytosecondary",
                    "Rangeprimarytosecondary",
                    "RangetoPrePlot",
                ]
            ]
            .mean(numeric_only=True)
            .sort_values(st_col)
        )
        return out, st_col

    def _add_bb_track(
        self,
        df,
        x_col,
        y_col,
        layer_key,
        label,
        point_color="white",
        line_color="white",
        point_size=5,
        line_width=1.2,
        heading_col=None,
        compute_heading_if_missing=False,
    ):
        if df is None or df.empty:
            return
        if x_col not in df.columns or y_col not in df.columns:
            return

        meta_cols = [
            "Line",
            "Station",
            "LinePoint",
            "TimeStamp",
            "DateTime",
            "TS",
            "UTC",
            "FileID",
            "BBFileID",
            "Source",
            "VesselHDG",
            "ROV1_HDG",
            "ROV2_HDG",
        ]

        d = df.copy()

        if heading_col and heading_col in d.columns:
            d["VesselHDG"] = pd.to_numeric(d[heading_col], errors="coerce")
        else:
            if "VesselHDG" not in d.columns:
                d["VesselHDG"] = np.nan
            else:
                d["VesselHDG"] = pd.to_numeric(d["VesselHDG"], errors="coerce")

        keep = [x_col, y_col] + [c for c in meta_cols if c in d.columns]
        d = d[keep].copy()

        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
        d = d.dropna(subset=[x_col, y_col])
        if d.empty:
            return

        if compute_heading_if_missing and "VesselHDG" in d.columns:
            if d["VesselHDG"].isna().all():
                dx = d[x_col].diff()
                dy = d[y_col].diff()
                hdg = np.degrees(np.arctan2(dx, dy))
                d["VesselHDG"] = (hdg + 360.0) % 360.0

        old = self.bb_layers.get(layer_key)
        if old:
            if old.get("curve"):
                self.tabs.map_plot.removeItem(old["curve"])
            if old.get("scatter"):
                self.tabs.map_plot.removeItem(old["scatter"])

        layer = PlotFactory.create_scatter_layer(
            d,
            x_col=x_col,
            y_col=y_col,
            connect_points=True,
            plot_name=label,
            plot_id=layer_key,
            point_size=point_size,
            point_color=point_color,
            line_color=line_color,
            line_width=line_width,
            meta_cols=[c for c in meta_cols if c in d.columns],
            meta_mode="dict",
            scatter_max_points=self._quality()["bb_scatter_max_points"],
            interactive_max_points=self._quality()["bb_scatter_max_points"],
        )

        self.bb_layers[layer_key] = layer
        if layer.get("curve"):
            self.tabs.map_plot.addItem(layer["curve"])
        if layer.get("scatter"):
            self.tabs.map_plot.addItem(layer["scatter"])

    def plot_blackbox_tracks(self, bb_df):
        if not hasattr(self, "bb_layers"):
            self.bb_layers = {}

        if bb_df is not None and not bb_df.empty:
            vessel_name = bb_df["vessel_name"].iloc[0]
            rov1_name = bb_df["rov1_name"].iloc[0]
            rov2_name = bb_df["rov2_name"].iloc[0]
        else:
            vessel_name = "Unknown Vessel"
            rov1_name = "ROV1"
            rov2_name = "ROV2"

        self._add_bb_track(
            bb_df,
            x_col="VesselEasting",
            y_col="VesselNorthing",
            layer_key="bb_vessel",
            label=vessel_name,
            point_color="yellow",
            line_color="yellow",
            point_size=4,
            line_width=1.5,
        )

        self._add_bb_track(
            bb_df,
            x_col="ROV1_INS_Easting",
            y_col="ROV1_INS_Northing",
            layer_key="bb_rov1_ins",
            label=f"{rov1_name} INS",
            point_color="cyan",
            line_color="cyan",
            point_size=4,
            line_width=1.2,
        )

        self._add_bb_track(
            bb_df,
            x_col="ROV2_INS_Easting",
            y_col="ROV2_INS_Northing",
            layer_key="bb_rov2_ins",
            label=f"{rov2_name} INS",
            point_color="magenta",
            line_color="magenta",
            point_size=4,
            line_width=1.2,
        )

        self._add_bb_track(
            bb_df,
            x_col="ROV1_USBL_Easting",
            y_col="ROV1_USBL_Northing",
            layer_key="bb_rov1_usbl",
            label=f"{rov1_name} USBL",
            point_color="lime",
            line_color="lime",
            point_size=4,
            line_width=1.0,
        )

        self._add_bb_track(
            bb_df,
            x_col="ROV2_USBL_Easting",
            y_col="ROV2_USBL_Northing",
            layer_key="bb_rov2_usbl",
            label=f"{rov2_name} USBL",
            point_color="orange",
            line_color="orange",
            point_size=4,
            line_width=1.0,
        )

        self._enable_legend_toggle()

    def closeEvent(self, event):
        if hasattr(self, "plot_manager"):
            self.plot_manager.close_all()
        super().closeEvent(event)

    def _set_station_marker_on_windows(self, line: int, station: int):
        for key, w in self._iter_dsr_plot_widgets():
            if not w or not hasattr(w, "items"):
                continue

            try:
                if int(w.state.get("current_line", -1)) != int(line):
                    continue
            except Exception:
                continue

            sel = w.items.get("sel_line")
            if sel is None:
                continue

            sel.setPos(float(station))
            sel.setVisible(True)
            sel.setZValue(10000)
            w.state["selected_station"] = int(station)

            try:
                w.plot.getViewBox().update()
                w.plot.repaint()
            except Exception:
                pass

    def _redraw_rp_circle_layer(self):
        if self.rp_circle_df is None or self.rp_circle_df.empty:
            return

        if self.rp_circle_item is not None:
            try:
                self.tabs.map_plot.removeItem(self.rp_circle_item)
            except Exception:
                pass
            self.rp_circle_item = None

        if getattr(self, "rp_circle_legend_proxy", None) is not None:
            try:
                self.tabs.legend.removeItem(self.rp_circle_legend_proxy.name())
            except Exception:
                pass
            self.rp_circle_legend_proxy = None

        self.rp_circle_item = PlotFactory.create_circle_layer_fast(
            self.rp_circle_df,
            x_col="X",
            y_col="Y",
            radius=self.rp_circle_radius,
            line_color="red",
            fill_color=None,
            line_width=2.0,
            max_circles=200000,
            name=f"R = {self.rp_circle_radius}m",
        )

        self.tabs.map_plot.addItem(self.rp_circle_item)

        proxy_pen = pg.mkPen("red", width=2.0)
        proxy_brush = pg.QtGui.QBrush(pg.QtCore.Qt.NoBrush)

        self.rp_circle_legend_proxy = pg.PlotDataItem(
            [0],
            [0],
            pen=proxy_pen,
            symbol="o",
            symbolPen=proxy_pen,
            symbolBrush=proxy_brush,
            symbolSize=10,
            name=self.rp_circle_item.name(),
        )

        self.tabs.legend.addItem(
            self.rp_circle_legend_proxy,
            self.rp_circle_legend_proxy.name(),
        )

    def _on_rp_radius_changed(self, value: float):
        self.rp_circle_radius = float(value)
        self._redraw_rp_circle_layer()

    def _spot_to_info(self, scatter, spot) -> dict:
        meta = spot.data()
        if meta is None:
            return {}

        if getattr(scatter, "meta_mode", None) == "tuple":
            cols = getattr(scatter, "meta_cols", [])
            return dict(zip(cols, meta))

        if getattr(scatter, "meta_mode", None) == "dict":
            return meta if isinstance(meta, dict) else {}

        return {"data": meta}

    def _nearest_bb_hover_spot(self, x: float, y: float, scene_pos):
        if not hasattr(self, "bb_layers"):
            return None

        vb = self.tabs.map_plot.getViewBox()

        px_tol = 18.0
        p0 = vb.mapSceneToView(scene_pos)
        p1 = vb.mapSceneToView(scene_pos + QtCore.QPointF(px_tol, px_tol))
        tol_x = abs(float(p1.x()) - float(p0.x()))
        tol_y = abs(float(p1.y()) - float(p0.y()))
        tol2 = (tol_x * tol_x) + (tol_y * tol_y)

        best = None
        best_d2 = None

        for layer_key in ("bb_rov1_ins", "bb_rov2_ins", "bb_rov1_usbl", "bb_rov2_usbl", "bb_vessel"):
            layer = self.bb_layers.get(layer_key)
            if not layer:
                continue

            scatter = layer.get("scatter")
            if scatter is None or not scatter.isVisible():
                continue

            try:
                data = scatter.getData()
                if not data or len(data) < 2:
                    continue

                xs, ys = data[0], data[1]
                if xs is None or ys is None:
                    continue

                xs = np.asarray(xs, dtype=float)
                ys = np.asarray(ys, dtype=float)
                if xs.size == 0:
                    continue

                dx = xs - x
                dy = ys - y
                d2 = dx * dx + dy * dy

                idx = int(np.argmin(d2))
                cur_d2 = float(d2[idx])

                if cur_d2 > tol2:
                    continue

                pts = scatter.points()
                if idx >= len(pts):
                    continue

                spot = pts[idx]
                info = self._spot_to_info(scatter, spot)

                if best_d2 is None or cur_d2 < best_d2:
                    best = (layer_key, scatter, spot, info)
                    best_d2 = cur_d2

            except Exception:
                continue

        return best

    def _on_map_mouse_moved(self, evt):
        def _safe_float(v):
            try:
                f = float(v)
                if math.isnan(f):
                    return None
                return f
            except Exception:
                return None

        def _pick(info: dict, keys: list[str]):
            for k in keys:
                if k in info:
                    val = _safe_float(info.get(k))
                    if val is not None:
                        return val
            return None

        pos = evt[0]

        vb = self.tabs.map_plot.getViewBox()
        if not vb.sceneBoundingRect().contains(pos):
            QtWidgets.QToolTip.hideText()
            return

        p = vb.mapSceneToView(pos)
        x = float(p.x())
        y = float(p.y())

        self.statusBar().showMessage(f"Cursor X: {x:,.2f}   Y: {y:,.2f}")
        self.right.set_coordinates(f"{x:,.2f}", f"{y:,.2f}")
        self.right.set_metric("Cursor", f"{x:,.1f}, {y:,.1f}")

        hit = self._nearest_bb_hover_spot(x, y, pos)
        if hit is None:
            QtWidgets.QToolTip.hideText()
            return

        layer_key, scatter, spot, info = hit

        v_hdg = _pick(info, ["VesselHDG", "VesselHeading", "ShipHeading", "Heading"])
        r1_hdg = _pick(info, ["ROV1_HDG", "ROV1Heading", "ROV1_INS_Heading", "ROV1_HDG_DEG"])
        r2_hdg = _pick(info, ["ROV2_HDG", "ROV2Heading", "ROV2_INS_Heading", "ROV2_HDG_DEG"])

        if v_hdg is not None:
            self.right.set_vessel_heading(v_hdg)
        if r1_hdg is not None:
            self.right.set_rov1_heading(r1_hdg)
        if r2_hdg is not None:
            self.right.set_rov2_heading(r2_hdg)

        self.right.set_selection(
            Layer=layer_key,
            Line=info.get("Line"),
            Station=info.get("Station"),
            Node=info.get("Node"),
        )

        msg = "\n".join(f"{k}: {v}" for k, v in info.items())
        QtWidgets.QToolTip.showText(QCursor.pos(), msg, self)

    def _enable_legend_toggle(self):
        legend = self.tabs.legend
        if legend is None:
            return

        for sample, label in legend.items:
            item = sample.item
            name = item.name()
            if not name:
                continue

            def make_toggle(nm):
                def toggle(ev):
                    self._toggle_bb_layer(nm)
                return toggle

            sample.mousePressEvent = make_toggle(name)

    def _toggle_bb_layer(self, name: str):
        if not hasattr(self, "bb_layers"):
            return

        layer = self.bb_layers.get(name)
        if not layer:
            return

        visible = True
        if layer.get("curve"):
            visible = layer["curve"].isVisible()

        new_state = not visible

        if layer.get("curve"):
            layer["curve"].setVisible(new_state)

        if layer.get("scatter"):
            layer["scatter"].setVisible(new_state)

    def on_plot_settings_changed(self, settings: dict):
        if isinstance(settings, dict):
            self.plot_settings = settings

        try:
            self.config_store.save(self.plot_settings)
        except Exception as e:
            print("[Config save] error:", e)

        try:
            rp_ps = int(self.plot_settings.get("rp", {}).get("point_size", 6))
            if self.rp_layer and self.rp_circle_df is not None and not self.rp_circle_df.empty:
                if self.rp_layer.get("curve"):
                    self.tabs.map_plot.removeItem(self.rp_layer["curve"])
                if self.rp_layer.get("scatter"):
                    self.tabs.map_plot.removeItem(self.rp_layer["scatter"])

                self.rp_layer = PlotFactory.create_scatter_layer(
                    self.rp_circle_df,
                    x_col="X",
                    y_col="Y",
                    group_col="Line",
                    order_col="Point",
                    plot_name="RPPreplot",
                    plot_id="rp_layer",
                    point_size=rp_ps,
                    point_color="blue",
                    line_color="blue",
                    line_width=1.5,
                    scatter_max_points=self._quality()["rp_scatter_max_points"],
                    interactive_max_points=self._quality()["interactive_max_points"],
                    interactive=False,
                )
                if self.rp_layer.get("curve"):
                    self.tabs.map_plot.addItem(self.rp_layer["curve"])
                if self.rp_layer.get("scatter"):
                    self.tabs.map_plot.addItem(self.rp_layer["scatter"])

                self.rp_circle_radius = float(self.plot_settings.get("rp", {}).get("circle_radius", 25.0))
                self._redraw_rp_circle_layer()
        except Exception:
            pass

        try:
            if getattr(self, "dsr_line_df", None) is not None:
                self.plot_dsr_primary_secondary(self.dsr_line_df)
        except Exception:
            pass

        try:
            if getattr(self, "bb_line_df", None) is not None:
                self.plot_blackbox_tracks(self.bb_line_df)
                if self.plot_settings.get("bb", {}).get("show_timeseries_window", True):
                    self.show_blackbox_timeseries_window(
                        self.current_dsr_line if hasattr(self, "current_dsr_line") else 0,
                        df_st=getattr(self, "current_dsr_stations_df", None),
                    )
        except Exception:
            pass

    def _get_dsr_mdi_window(self, line: int):
        if self.dsr_mdi_window is None:
            self.dsr_mdi_window = DsrMdiWindow(self)
        self.dsr_mdi_window.set_line_title(line)
        self.dsr_mdi_window.show()
        self.dsr_mdi_window.raise_()
        self.dsr_mdi_window.activateWindow()
        return self.dsr_mdi_window

    def _iter_dsr_plot_widgets(self):
        if self.dsr_mdi_window is None:
            return []

        out = []
        for key in ("depth_vs_station", "sigmas_vs_station", "radial_offset_vs_station"):
            sub = self.dsr_mdi_window.get_subwindow(key)
            if sub is None:
                continue

            try:
                w = sub.widget()
            except Exception:
                w = None

            if w is not None:
                out.append((key, w))

        return out

    def _get_bb_mdi_window(self, line: int):
        if self.bb_mdi_window is None:
            self.bb_mdi_window = BbMdiWindow(self)
        self.bb_mdi_window.set_title(line)
        self.bb_mdi_window.show()
        self.bb_mdi_window.raise_()
        self.bb_mdi_window.activateWindow()
        return self.bb_mdi_window

    def _iter_bb_plot_widgets(self):
        if self.bb_mdi_window is None:
            return []

        keys = (
            "bb_hdg",
            "bb_sog",
            "bb_cog",
            "bb_nos",
            "bb_diffage",
            "bb_fixquality",
            "bb_hdop",
            "bb_depth",
        )

        out = []
        for key in keys:
            sub = self.bb_mdi_window.get_subwindow(key)
            if sub is None:
                continue
            try:
                w = sub.widget()
            except Exception:
                w = None
            if w is not None:
                out.append((key, w))
        return out

    def _set_bb_time_marker(self, ts_num: float):
        for _, w in self._iter_bb_plot_widgets():
            try:
                sel = w.items.get("sel_line")
                if sel is not None:
                    sel.setPos(float(ts_num))
                    sel.setVisible(True)
                    sel.setZValue(10000)
                    w.state["selected_time"] = float(ts_num)
                    if hasattr(w, "plot") and w.plot is not None:
                        w.plot.repaint()
            except Exception:
                pass

    def show_blackbox_timeseries_window(self, line: int, df_st=None):
        if getattr(self, "bb_line_df", None) is None or self.bb_line_df.empty:
            return

        d = self.bb_line_df.copy()

        if "TimeStamp" not in d.columns:
            return

        d["TimeStamp_dt"] = pd.to_datetime(d["TimeStamp"], errors="coerce")
        d = d.dropna(subset=["TimeStamp_dt"]).sort_values("TimeStamp_dt")
        if d.empty:
            return

        # fast numeric x-axis for pyqtgraph
        d["TimeStampNum"] = d["TimeStamp_dt"].astype("int64") / 1e9

        # -------------------------------------------------
        # labels from bb_line_df (legend only, not column names)
        # -------------------------------------------------
        def _first_str(df, col, default):
            try:
                if col in df.columns:
                    vals = df[col].dropna().astype(str).str.strip()
                    vals = vals[vals != ""]
                    if not vals.empty:
                        return vals.iloc[0]
            except Exception:
                pass
            return default

        vessel_name = _first_str(d, "vessel_name", "Vessel")
        rov1_name = _first_str(d, "rov1_name", "ROV1")
        rov2_name = _first_str(d, "rov2_name", "ROV2")
        gnss1_name = _first_str(d, "gnss1_name", "GNSS1")
        gnss2_name = _first_str(d, "gnss2_name", "GNSS2")
        depth1_name = _first_str(d, "Depth1_name", _first_str(d, "depth1_name", "Depth1"))
        depth2_name = _first_str(d, "Depth2_name", _first_str(d, "depth2_name", "Depth2"))

        # -------------------------------------------------
        # settings
        # -------------------------------------------------
        bb_sel = self.plot_settings.get("bb", {})

        # optional light downsampling for long logs
        max_rows = 200_000
        if len(d) > max_rows:
            idx = np.unique(np.rint(np.linspace(0, len(d) - 1, max_rows)).astype(np.int64))
            d = d.iloc[idx].copy()

        mdi = self._get_bb_mdi_window(line)

        created_keys = []
        base_plot = None

        def _add_ts_plot(key: str, title: str, y_label: str, series: list[dict]):
            nonlocal base_plot
            w = PlotWindow(title=title)

            PlotFactory.build_multi_series_vs_time(
                w,
                d,
                time_col="TimeStampNum",
                title=title,
                y_label=y_label,
                series=series,
            )

            mdi.add_plot_window(key, w, title)

            if base_plot is None:
                base_plot = w.plot
            else:
                try:
                    w.plot.setXLink(base_plot)
                except Exception:
                    pass

            created_keys.append(key)
            return w

        # -------------------------------------------------
        # Heading
        # -------------------------------------------------
        if bb_sel.get("ts_hdg", True):
            _add_ts_plot(
                "bb_hdg",
                f"Heading - Line {line}",
                "HDG",
                [
                    {"col": "VesselHDG", "name": vessel_name, "color": "yellow"},
                    {"col": "ROV1_HDG", "name": rov1_name, "color": "cyan"},
                    {"col": "ROV2_HDG", "name": rov2_name, "color": "magenta"},
                ],
            )

        # -------------------------------------------------
        # Speed over ground
        # -------------------------------------------------
        if bb_sel.get("ts_sog", True):
            _add_ts_plot(
                "bb_sog",
                f"Speed over Ground - Line {line}",
                "SOG",
                [
                    {"col": "VesselSOG", "name": vessel_name, "color": "yellow"},
                    {"col": "ROV1_SOG", "name": rov1_name, "color": "cyan"},
                    {"col": "ROV2_SOG", "name": rov2_name, "color": "magenta"},
                ],
            )

        # -------------------------------------------------
        # Course over ground
        # -------------------------------------------------
        if bb_sel.get("ts_cog", True):
            _add_ts_plot(
                "bb_cog",
                f"Course over Ground - Line {line}",
                "COG",
                [
                    {"col": "VesselCOG", "name": vessel_name, "color": "yellow"},
                    {"col": "ROV1_COG", "name": rov1_name, "color": "cyan"},
                    {"col": "ROV2_COG", "name": rov2_name, "color": "magenta"},
                ],
            )

        # -------------------------------------------------
        # Number of satellites
        # -------------------------------------------------
        if bb_sel.get("ts_nos", True):
            _add_ts_plot(
                "bb_nos",
                f"Number of Satellites - Line {line}",
                "NOS",
                [
                    {"col": "GNSS1_NOS", "name": gnss1_name, "color": "lime"},
                    {"col": "GNSS2_NOS", "name": gnss2_name, "color": "orange"},
                ],
            )

        # -------------------------------------------------
        # GPS Diff Age
        # -------------------------------------------------
        if bb_sel.get("ts_diffage", True):
            _add_ts_plot(
                "bb_diffage",
                f"GPS Diff Age - Line {line}",
                "DiffAge",
                [
                    {"col": "GNSS1_DiffAge", "name": gnss1_name, "color": "lime"},
                    {"col": "GNSS2_DiffAge", "name": gnss2_name, "color": "orange"},
                ],
            )

        # -------------------------------------------------
        # GPS FixQuality
        # -------------------------------------------------
        if bb_sel.get("ts_fixquality", True):
            _add_ts_plot(
                "bb_fixquality",
                f"GPS Fix Quality - Line {line}",
                "Fix Quality",
                [
                    {"col": "GNSS1_FixQuality", "name": gnss1_name, "color": "lime"},
                    {"col": "GNSS2_FixQuality", "name": gnss2_name, "color": "orange"},
                ],
            )

        # -------------------------------------------------
        # HDOP
        # -------------------------------------------------
        if bb_sel.get("ts_hdop", True):
            _add_ts_plot(
                "bb_hdop",
                f"HDOP - Line {line}",
                "HDOP",
                [
                    {"col": "GNSS1_HDOP", "name": gnss1_name, "color": "lime"},
                    {"col": "GNSS2_HDOP", "name": gnss2_name, "color": "orange"},
                ],
            )

        # -------------------------------------------------
        # Depth
        # legend = combination of rov name + depth sensor name
        # -------------------------------------------------
        if bb_sel.get("ts_depth", True):
            depth_series = []

            if "ROV1_Depth1" in d.columns:
                depth_series.append({
                    "col": "ROV1_Depth1",
                    "name": f"{rov1_name} {depth1_name}",
                    "color": "cyan",
                })

            if "ROV1_Depth2" in d.columns:
                depth_series.append({
                    "col": "ROV1_Depth2",
                    "name": f"{rov1_name} {depth2_name}",
                    "color": "deepskyblue",
                })

            if "ROV2_Depth1" in d.columns:
                depth_series.append({
                    "col": "ROV2_Depth1",
                    "name": f"{rov2_name} {depth1_name}",
                    "color": "magenta",
                })

            if "ROV2_Depth2" in d.columns:
                depth_series.append({
                    "col": "ROV2_Depth2",
                    "name": f"{rov2_name} {depth2_name}",
                    "color": "violet",
                })

            if depth_series:
                _add_ts_plot(
                    "bb_depth",
                    f"Depth - Line {line}",
                    "Depth",
                    depth_series,
                )

        # -------------------------------------------------
        # arrange mdi windows
        # -------------------------------------------------
        try:
            mdi.mdi.tileSubWindows()
        except Exception:
            pass

        # -------------------------------------------------
        # initial red marker = nearest BB timestamp to first station Deploy T
        # -------------------------------------------------
        try:
            deploy_ts = None

            if df_st is not None and not df_st.empty:
                for cand in ("Deploy T", "DeployT", "DeployTime", "Deploy_Time"):
                    if cand in df_st.columns:
                        vals = pd.to_datetime(df_st[cand], errors="coerce").dropna()
                        if not vals.empty:
                            deploy_ts = vals.iloc[0]
                            break

            arr = d["TimeStampNum"].to_numpy(dtype=float)
            if arr.size:
                if deploy_ts is not None and pd.notna(deploy_ts):
                    target_num = float(deploy_ts.timestamp())
                    idx = int(np.argmin(np.abs(arr - target_num)))
                    self._set_bb_time_marker(float(arr[idx]))
                else:
                    self._set_bb_time_marker(float(arr[0]))
        except Exception as e:
            print("[BB initial marker] error:", e)

    def _show_loading(self, text="Loading..."):
        if getattr(self, "_loading_dialog", None) is not None:
            try:
                self._loading_dialog.close()
            except Exception:
                pass

        dlg = QtWidgets.QProgressDialog(text, None, 0, 0, self)
        dlg.setWindowTitle("Please wait")
        dlg.setWindowModality(QtCore.Qt.WindowModality.ApplicationModal)
        dlg.setCancelButton(None)
        dlg.setMinimumDuration(0)
        dlg.setAutoClose(False)
        dlg.setAutoReset(False)
        dlg.setValue(0)

        # cleaner look
        dlg.setWindowFlags(
            dlg.windowFlags()
            & ~QtCore.Qt.WindowType.WindowContextHelpButtonHint
        )

        self._loading_dialog = dlg

        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.CursorShape.WaitCursor)
        dlg.show()
        QtWidgets.QApplication.processEvents()

    def _hide_loading(self):
        try:
            QtWidgets.QApplication.restoreOverrideCursor()
        except Exception:
            pass

        dlg = getattr(self, "_loading_dialog", None)
        if dlg is not None:
            try:
                dlg.close()
                dlg.deleteLater()
            except Exception:
                pass
            self._loading_dialog = None