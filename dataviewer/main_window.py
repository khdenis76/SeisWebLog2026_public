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
        self.setWindowTitle("SeisWebLog Viewer")
        self.resize(1400, 900)

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
        self._build_ui()
        self._wire()
        self.statusBar().showMessage("Ready")

        # auto-load at start
        self.load_projects()
        self.plot_manager = PlotManager()
        self.depth_windows = {}
        self.plot_settings = {
            "rp": {"point_size": 6},
            "dsr": {"depth": True, "sigmas": True, "radial": True, "primary_point_size": 6, "secondary_point_size": 6},
            "bb": {"bb_vessel": True, "bb_rov1_ins": True, "bb_rov2_ins": True, "bb_rov1_usbl": True,
                   "bb_rov2_usbl": True},
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

    def _build_ui(self):
        # 3-way splitter: Left panel | Center tabs | Right panel
        self.splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        self.setCentralWidget(self.splitter)

        # Create widgets
        self.left = LeftPanel()
        self.tabs = CentralTabs()
        # connect hover
        self._hover_proxy = pg.SignalProxy(
            self.tabs.map_plot.scene().sigMouseMoved,
            rateLimit=30,
            slot=self._on_map_mouse_moved
        )
        self.right = RightPanel()
        QtCore.QTimer.singleShot(1000, self._debug_heading_test)
        # Keep map scale equal (Easting/Northing)
        try:
            self.tabs.map_plot.getViewBox().setAspectLocked(True, ratio=1)
        except Exception:
            pass

        # Add to splitter
        self.splitter.addWidget(self.left)
        self.splitter.addWidget(self.tabs)
        self.splitter.addWidget(self.right)

        # Stretch behavior: center grows, side panels stay reasonable
        self.splitter.setStretchFactor(0, 0)  # left
        self.splitter.setStretchFactor(1, 1)  # center
        self.splitter.setStretchFactor(2, 0)  # right

        # Initial sizes (px)
        self.splitter.setSizes([360, 900, 320])

        self.splitter.setChildrenCollapsible(False)
        # ---- TEST ROTATION ----
        QtCore.QTimer.singleShot(500, lambda: self.right.set_rov1_heading(45))
        QtCore.QTimer.singleShot(500, lambda: self.right.set_rov2_heading(120))

    def _wire(self):
        self.act_exit.triggered.connect(self.close)
        self.act_toggle_left.triggered.connect(self._toggle_left)
        self.act_toggle_right.triggered.connect(self._toggle_right)

        self.right.closeRequested.connect(self._close_right_panel)

        self.left.reloadProjectsClicked.connect(self.load_projects)
        self.left.projectChanged.connect(self.on_project_changed)

        # DSR
        self.left.dsrLineClicked.connect(self.on_dsr_line_clicked)
        self.left.dsrStationClicked.connect(self.on_dsr_station_clicked)
        self.left.dsrSelectionChanged.connect(self.on_dsr_selection_changed)

        # BlackBox
        self.left.blackBoxSelectionChanged.connect(self.on_blackbox_selection_changed)
        self.left.blackBoxRowClicked.connect(self.on_blackbox_row_clicked)
        #Red radius
        self.left.rpRadiusChanged.connect(self._on_rp_radius_changed)
        self.left.plotsSettingsChanged.connect(self.on_plot_settings_changed)

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

            # select first
            self.left.cmb_project.setCurrentIndex(0)
            self.on_project_changed(self.left.cmb_project.currentText())

            # also show preview in table tab
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

        # store current project early
        self.current_project = {
            "id": int(r["id"]),
            "name": str(r["name"]),
            "root_path": root_path,
            "folder_name": folder_name,
            "project_dir": str(project_dir),
        }

        self.left.set_project_dir(str(project_dir))
        """
        self.right.set_text(
            f"Project: {self.current_project['name']}\n"
            f"ID: {self.current_project['id']}\n"
            f"Root: {self.current_project['root_path']}\n"
            f"Folder: {self.current_project['folder_name']}\n"
            f"Dir: {self.current_project['project_dir']}\n"
        )
        """
        # --- RPPreplot plot (optional) ---
        try:
            pdb = ProjectDb(project_dir)
            df = pdb.read_rp_preplot()

            # remove previous RP layer
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
            )
            self.rp_layer = layer
            if layer.get("curve"):
                self.tabs.map_plot.addItem(layer["curve"])
            if layer.get("scatter"):
                self.tabs.map_plot.addItem(layer["scatter"])

            # store df for future redraw + redraw circles using current radius
            self.rp_circle_df = df

            # sync settings UI (LeftPanel), without emitting valueChanged
            try:
                self.left.set_rp_radius(self.rp_circle_radius)
            except Exception:
                pass

            # redraw circle layer + legend proxy
            self._redraw_rp_circle_layer()

        except ProjectDbError as e:
            self.statusBar().showMessage(str(e), 10000)

        # --- Load DSR lines summary into left panel ---
        try:
            pdb = ProjectDb(project_dir)
            df_sum = pdb.read_v_dsr_lines()
            self.left.set_dsr_lines_table(df_sum)
        except ProjectDbError as e:
            self.statusBar().showMessage(str(e), 10000)
            self.left.set_dsr_lines_table(None)

        # --- Load BlackBox files into left panel ---
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

        try:
            pdb = ProjectDb(self.current_project["project_dir"])

            # 1) stations list for this line
            df_st = pdb.read_dsr_stations_for_line(line)
            self.left.set_dsr_stations_table(df_st)

            # 2) load full DSR for plotting
            self.dsr_line_df = pdb.read_dsr_for_line(line)
            depth_df, st_col = self._make_station_depth_df(self.dsr_line_df)
            # BlackBox subset in Date1 window for this line
            try:
                self.bb_line_df = pdb.read_blackbox_for_line_by_date1_window(line)

            except Exception:
                self.bb_line_df = None

            # plot tracks on map (vessel / rov ins / usbl)
            self.plot_blackbox_tracks(self.bb_line_df)

            dsr_sel = self.plot_settings.get("dsr", {})

            # Build only selected windows
            w_depth = None
            w_sig = None
            w_rad = None

            if dsr_sel.get("depth", True):
                key = "depth_vs_station"
                w_depth = self.plot_manager.get_or_create(key, title="Depth vs Station", seq=1)
                w_depth.show();
                w_depth.raise_();
                w_depth.activateWindow()
                w_depth.state["current_line"] = int(line)
                w_depth.state["on_station_selected"] = lambda st: self.on_dsr_station_clicked(line, st)

                PlotFactory.build_two_series_vs_station(
                    w_depth,
                    depth_df,
                    station_col="Station",
                    series1_col="PrimaryElevation",
                    series2_col="SecondaryElevation",
                    y_label="Elevation",
                    title=f"Depth vs Station — Line {line}",
                )

            if dsr_sel.get("sigmas", True):
                key1 = "sigmas_vs_station"
                w_sig = self.plot_manager.get_or_create(key1, title="Sigma vs Station", seq=2)
                w_sig.show();
                w_sig.raise_();
                w_sig.activateWindow()
                w_sig.state["current_line"] = int(line)
                w_sig.state["on_station_selected"] = lambda st: self.on_dsr_station_clicked(line, st)

                PlotFactory.build_two_series_vs_station(
                    w_sig,
                    depth_df,
                    station_col="Station",
                    series1_col="Sigma1",
                    series2_col="Sigma2",
                    y_label="Sigma",
                    title=f"Sigma vs Station — Line {line}",
                )

            if dsr_sel.get("radial", True):
                key2 = "radial_offset_vs_station"
                w_rad = self.plot_manager.get_or_create(key2, title="Radial Offset", seq=3)
                w_rad.show();
                w_rad.raise_();
                w_rad.activateWindow()
                w_rad.state["current_line"] = int(line)
                w_rad.state["on_station_selected"] = lambda st: self.on_dsr_station_clicked(line, st)

                PlotFactory.build_two_series_vs_station(
                    w_rad,
                    depth_df,
                    station_col="Station",
                    series1_col="DeltaEprimarytosecondary",
                    series2_col="DeltaNprimarytosecondary",
                    y_label="Radial Offset",
                    title=f"Radial Offset — Line {line}",
                )

            # Link X only among windows that exist
            base = w_depth or w_sig or w_rad
            if base:
                if w_sig: w_sig.plot.setXLink(base.plot)
                if w_rad: w_rad.plot.setXLink(base.plot)

            # 3) plot primary + secondary on map
            self.plot_dsr_primary_secondary(self.dsr_line_df)

            # ✅ Zoom map to project extent (RPPreplot)
            if self.rp_layer and self.rp_layer.get("curve"):
                bounds = self.rp_layer["curve"].boundingRect()
                vb = self.tabs.map_plot.getViewBox()
                vb.setRange(bounds, padding=0.02)

            self.statusBar().showMessage(
                f"Line {line}: stations {len(df_st)}, dsr rows {len(self.dsr_line_df)}",
                8000
            )

        except ProjectDbError as e:
            self.statusBar().showMessage(str(e), 10000)
            self.left.set_dsr_stations_table(None)
            self.dsr_line_df = None

    def on_dsr_selection_changed(self, lines: list):
        self.statusBar().showMessage(f"Selected lines: {', '.join(map(str, lines))}")

    def on_blackbox_selection_changed(self, file_ids: list):
        self.statusBar().showMessage(f"BlackBox selected files: {file_ids}")

    def on_blackbox_row_clicked(self, file_id: int):
        self.statusBar().showMessage(f"BlackBox file clicked: {file_id}")

    def plot_dsr_primary_secondary(self, df):
        # remove old
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

        # Primary
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
            )
            if self.dsr_primary_layer["scatter"]:
                self.tabs.map_plot.addItem(self.dsr_primary_layer["scatter"])

        # Secondary
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

        for key, w in self.plot_manager.windows.items():
            if not w or not hasattr(w, "items"):
                continue

            # ✅ match by stored current line
            if int(w.state.get("current_line", -1)) != int(line):
                continue

            sel = w.items.get("sel_line")
            if sel is None:
                continue

            sel.setPos(st)
            sel.setVisible(True)
            w.state["selected_station"] = int(station)

    def on_dsr_station_clicked(self, line: int, station: int):
        if not self.current_project:
            return

        try:
            stf = float(station)
            sti = int(float(station))
        except Exception:
            return

        # 1) Update red vertical line in BOTH windows
        for key in ("depth_vs_station", "sigmas_vs_station","radial_offset_vs_station"):
            w = self.plot_manager.windows.get(key)
            if not w or not hasattr(w, "items"):
                continue

            # only update windows that currently display this line
            try:
                if int(w.state.get("current_line", -1)) != int(line):
                    continue
            except Exception:
                continue

            # If sel_line does not exist yet, just store selection and let PlotFactory restore it
            sel = w.items.get("sel_line")
            w.state["selected_station"] = sti

            if sel is not None:
                sel.setPos(stf)
                sel.setVisible(True)
                sel.setZValue(10_000)

                # ✅ Force redraw immediately (this is the key)
                try:
                    w.plot.getViewBox().update()
                except Exception:
                    pass
                w.plot.repaint()

        # Process pending paint events (helps on Windows)
        try:
            QtWidgets.QApplication.processEvents()
        except Exception:
            pass

        # 2) Zoom map to this station
        try:
            pdb = ProjectDb(self.current_project["project_dir"])
            center = pdb.read_dsr_station_center(line, station)
            if not center:
                self.statusBar().showMessage(
                    f"No Primary coords for Line {line} Station {station}", 8000
                )
                return

            # zoom = 2 × circle radius
            half_size = float(self.rp_circle_radius) * 2.0

            self.zoom_map_to_xy(
                center["X"],
                center["Y"],
                half_size_m=half_size
            )
            self.statusBar().showMessage(f"Zoom to Line {line} Station {station} {half_size}")
            print("clicked station:", station)
        except ProjectDbError as e:
            self.statusBar().showMessage(str(e), 10000)

    def _make_station_depth_df(self, dsr_line_df):
        if "Station" in dsr_line_df.columns:
            st_col = "Station"
        elif "LinePoint" in dsr_line_df.columns:
            st_col = "LinePoint"
        else:
            raise ValueError("Neither Station nor LinePoint found")
        out = (
            dsr_line_df.groupby(st_col, as_index=False)[["PrimaryElevation", "SecondaryElevation",
                                                         "Sigma1","Sigma2",
                                                         "DeltaEprimarytosecondary","DeltaNprimarytosecondary",
                                                         "Rangeprimarytosecondary","RangetoPrePlot"]]
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
            compute_heading_if_missing=False,  # <- optional
    ):
        """
        Adds one track (line+scatter) to the map from df[x_col,y_col].
        Stores the layer in self.bb_layers[layer_key] so we can remove/update later.
        """

        if df is None or df.empty:
            return
        if x_col not in df.columns or y_col not in df.columns:
            return

        # keep coords + any metadata we want to see on hover
        meta_cols = [
            "Line", "Station", "LinePoint",
            "TimeStamp", "DateTime", "TS", "UTC",
            "FileID", "BBFileID", "Source",
            # headings
            "VesselHDG", "ROV1_HDG", "ROV2_HDG",
        ]

        d = df.copy()

        # --- normalize heading into a fixed name for hover/UI ---
        # For vessel layer we store heading in VesselHDG (even if source col has different name)
        if heading_col and heading_col in d.columns:
            d["VesselHDG"] = pd.to_numeric(d[heading_col], errors="coerce")
        else:
            # ensure column exists
            if "VesselHDG" not in d.columns:
                d["VesselHDG"] = np.nan
            else:
                d["VesselHDG"] = pd.to_numeric(d["VesselHDG"], errors="coerce")

        # --- choose which columns to keep (IMPORTANT: use d, not df) ---
        keep = [x_col, y_col] + [c for c in meta_cols if c in d.columns]
        d = d[keep].copy()

        # numeric coords
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
        d = d.dropna(subset=[x_col, y_col])
        if d.empty:
            return

        # --- optional: compute heading from XY if missing ---
        if compute_heading_if_missing and "VesselHDG" in d.columns:
            if d["VesselHDG"].isna().all():
                # heading = atan2(East, North) => 0..360 (0=N, 90=E)
                dx = d[x_col].diff()
                dy = d[y_col].diff()
                hdg = np.degrees(np.arctan2(dx, dy))
                d["VesselHDG"] = (hdg + 360.0) % 360.0

        # remove previous
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
            meta_mode="tuple",
        )

        self.bb_layers[layer_key] = layer
        if layer.get("curve"):
            self.tabs.map_plot.addItem(layer["curve"])
        if layer.get("scatter"):
            self.tabs.map_plot.addItem(layer["scatter"])

    def plot_blackbox_tracks(self, bb_df):
        if not hasattr(self, "bb_layers"):
            self.bb_layers = {}

        # Vessel
        self._add_bb_track(
            bb_df,
            x_col="VesselEasting",
            y_col="VesselNorthing",
            layer_key="bb_vessel",
            label="Vessel",
            point_color="yellow",
            line_color="yellow",
            point_size=4,
            line_width=1.5,
        )

        # ROV1 INS
        self._add_bb_track(
            bb_df,
            x_col="ROV1_INS_Easting",
            y_col="ROV1_INS_Northing",
            layer_key="bb_rov1_ins",
            label="ROV1 INS",
            point_color="cyan",
            line_color="cyan",
            point_size=4,
            line_width=1.2,
        )

        # ROV2 INS
        self._add_bb_track(
            bb_df,
            x_col="ROV2_INS_Easting",
            y_col="ROV2_INS_Northing",
            layer_key="bb_rov2_ins",
            label="ROV2 INS",
            point_color="magenta",
            line_color="magenta",
            point_size=4,
            line_width=1.2,
        )

        # ROV1 USBL
        self._add_bb_track(
            bb_df,
            x_col="ROV1_USBL_Easting",
            y_col="ROV1_USBL_Northing",
            layer_key="bb_rov1_usbl",
            label="ROV1 USBL",
            point_color="lime",
            line_color="lime",
            point_size=4,
            line_width=1.0,
        )

        # ROV2 USBL
        self._add_bb_track(
            bb_df,
            x_col="ROV2_USBL_Easting",
            y_col="ROV2_USBL_Northing",
            layer_key="bb_rov2_usbl",
            label="ROV2 USBL",
            point_color="orange",
            line_color="orange",
            point_size=4,
            line_width=1.0,
        )
        self._enable_legend_toggle()

    def closeEvent(self, event):
        # Close all secondary plot windows
        if hasattr(self, "plot_manager"):
            self.plot_manager.close_all()

        super().closeEvent(event)

    def _set_station_marker_on_windows(self, line: int, station: int):
        for key in ("depth_vs_station", "sigmas_vs_station"):
            w = self.plot_manager.windows.get(key)
            if not w or not hasattr(w, "items"):
                continue

            # only update if this window currently displays the same line
            if int(w.state.get("current_line", -1)) != int(line):
                continue

            sel = w.items.get("sel_line")
            if sel is None:
                continue

            sel.setPos(float(station))
            sel.setVisible(True)
            w.state["selected_station"] = int(station)



    def _redraw_rp_circle_layer(self):
        if self.rp_circle_df is None or self.rp_circle_df.empty:
            return

        # remove old circle item
        if self.rp_circle_item is not None:
            try:
                self.tabs.map_plot.removeItem(self.rp_circle_item)
            except Exception:
                pass
            self.rp_circle_item = None

        # remove legend proxy
        if getattr(self, "rp_circle_legend_proxy", None) is not None:
            try:
                self.tabs.legend.removeItem(self.rp_circle_legend_proxy.name())
            except Exception:
                pass
            self.rp_circle_legend_proxy = None

        # create new circle
        self.rp_circle_item = PlotFactory.create_circle_layer_fast(
            self.rp_circle_df,
            x_col="X",
            y_col="Y",
            radius=self.rp_circle_radius,
            line_color="red",
            fill_color=None,
            line_width=2.0,
            max_circles=200000,
            name=f"R = {self.rp_circle_radius}m"
        )

        self.tabs.map_plot.addItem(self.rp_circle_item)

        # rebuild legend proxy
        proxy_pen = pg.mkPen("red", width=2.0)
        proxy_brush = pg.QtGui.QBrush(pg.QtCore.Qt.NoBrush)

        self.rp_circle_legend_proxy = pg.PlotDataItem(
            [0], [0],
            pen=proxy_pen,
            symbol="o",
            symbolPen=proxy_pen,
            symbolBrush=proxy_brush,
            symbolSize=10,
            name=self.rp_circle_item.name()
        )

        self.tabs.legend.addItem(
            self.rp_circle_legend_proxy,
            self.rp_circle_legend_proxy.name()
        )

    def _on_rp_radius_changed(self, value: float):
        self.rp_circle_radius = float(value)
        self._redraw_rp_circle_layer()

    def _on_map_mouse_moved(self, evt):
        def _safe_float(v):
            try:
                f = float(v)
                if math.isnan(f):
                    return None
                return f
            except Exception:
                return None

        pos = evt[0]

        vb = self.tabs.map_plot.getViewBox()
        if not vb.sceneBoundingRect().contains(pos):
            QtWidgets.QToolTip.hideText()
            return

        p = vb.mapSceneToView(pos)
        x = float(p.x())
        y = float(p.y())

        if not hasattr(self, "bb_layers"):
            QtWidgets.QToolTip.hideText()
            return

        # You can keep priority, but it doesn't matter anymore
        for layer_key in ("bb_rov1_ins", "bb_rov2_ins","bb_rov1_usbl", "bb_rov2_usbl","bb_vessel"):
            layer = self.bb_layers.get(layer_key)
            if not layer:
                continue

            scatter = layer.get("scatter")
            if scatter is None:
                continue

            pts = scatter.pointsAt(pg.QtCore.QPointF(x, y))
            if pts is None or len(pts) == 0:
                continue

            spot = pts[0]
            meta = spot.data()
            if meta is None:
                continue

            if getattr(scatter, "meta_mode", None) == "tuple":
                cols = getattr(scatter, "meta_cols", [])
                info = dict(zip(cols, meta))
            elif getattr(scatter, "meta_mode", None) == "dict":
                info = meta
            else:
                info = {"data": meta}

            # ✅ Update all 3 headings from the hovered record
            v_hdg = _safe_float(info.get("VesselHDG"))
            r1_hdg = _safe_float(info.get("ROV1_HDG"))
            r2_hdg = _safe_float(info.get("ROV2_HDG"))

            if v_hdg is not None:
                self.right.set_vessel_heading(v_hdg)
            if r1_hdg is not None:
                self.right.set_rov1_heading(r1_hdg)
            if r2_hdg is not None:
                self.right.set_rov2_heading(r2_hdg)

            msg = "\n".join(f"{k}: {v}" for k, v in info.items())
            QtWidgets.QToolTip.showText(QCursor.pos(), msg, self)
            return

        QtWidgets.QToolTip.hideText()

    def _enable_legend_toggle(self):
        legend = self.tabs.legend
        if legend is None:
            return

        for sample, label in legend.items:
            item = sample.item  # the actual PlotDataItem

            # only handle blackbox layers
            name = item.name()
            if not name:
                continue

            # connect mouse press event
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

        # detect current visibility
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

        # If RP layer exists: rebuild RP scatter with new point size (optional)
        # easiest: just re-run on_project_changed (heavy) OR redraw only RP scatter.
        # We'll do light redraw if data exists.

        # redraw RP scatter (only if we have layer + df)
        try:
            rp_ps = int(self.plot_settings.get("rp", {}).get("point_size", 6))
            if self.rp_layer and self.rp_circle_df is not None and not self.rp_circle_df.empty:
                # remove old
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
                )
                if self.rp_layer.get("curve"):
                    self.tabs.map_plot.addItem(self.rp_layer["curve"])
                if self.rp_layer.get("scatter"):
                    self.tabs.map_plot.addItem(self.rp_layer["scatter"])

                # keep circles on top
                self._redraw_rp_circle_layer()
        except Exception:
            pass

        # redraw DSR primary/secondary if currently plotted
        try:
            if getattr(self, "dsr_line_df", None) is not None:
                self.plot_dsr_primary_secondary(self.dsr_line_df)
        except Exception:
            pass

        # replot blackbox if we already have bb_line_df
        try:
            if getattr(self, "bb_line_df", None) is not None:
                self.plot_blackbox_tracks(self.bb_line_df)
        except Exception:
            pass

    def _debug_heading_test(self):
        from PySide6 import QtWidgets

        try:
            # make sure heading tab is visible
            if hasattr(self.right, "tabs"):
                self.right.tabs.setCurrentIndex(1)

            self.right.set_vessel_heading(30)
            self.right.set_rov1_heading(45)
            self.right.set_rov2_heading(120)

            QtWidgets.QMessageBox.information(self, "Test", "Heading test executed ✅")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Test ERROR", repr(e))