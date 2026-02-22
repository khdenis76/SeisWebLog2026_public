from pathlib import Path

import pandas as pd
import pyqtgraph as pg
from PySide6 import QtCore, QtWidgets
from PySide6.QtGui import QAction

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

        self._build_menu()
        self._build_ui()
        self._wire()
        self.statusBar().showMessage("Ready")

        # auto-load at start
        self.load_projects()
        self.plot_manager = PlotManager()
        self.depth_windows = {}

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
        self.right = RightPanel()

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
        self.right.set_text(
            f"Project: {self.current_project['name']}\n"
            f"ID: {self.current_project['id']}\n"
            f"Root: {self.current_project['root_path']}\n"
            f"Folder: {self.current_project['folder_name']}\n"
            f"Dir: {self.current_project['project_dir']}\n"
        )

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
            if layer["curve"]:
                self.tabs.map_plot.addItem(layer["curve"])
            if layer["scatter"]:
                self.tabs.map_plot.addItem(layer["scatter"])

            # circles (optional)
            if self.rp_circle_item is not None:
                self.tabs.map_plot.removeItem(self.rp_circle_item)

            self.rp_circle_item = PlotFactory.create_circle_layer_fast(
                df,
                x_col="X",
                y_col="Y",
                radius=25.0,
                line_color="red",
                fill_color=None,
                line_width=2.0,
                max_circles=200000,
                name="R = 25m"
            )
            self.tabs.map_plot.addItem(self.rp_circle_item)
            # remove old proxy if exists
            if getattr(self, "rp_circle_legend_proxy", None) is not None:
                try:
                    self.tabs.legend.removeItem(self.rp_circle_legend_proxy.name())
                except Exception:
                    pass
                self.rp_circle_legend_proxy = None

            # build proxy just for legend
            proxy_pen = pg.mkPen("red", width=2.0)
            proxy_brush = pg.QtGui.QBrush(pg.QtCore.Qt.NoBrush)  # since fill_color=None

            self.rp_circle_legend_proxy = pg.PlotDataItem(
                [0], [0],
                pen=proxy_pen,
                symbol="o",
                symbolPen=proxy_pen,
                symbolBrush=proxy_brush,
                symbolSize=10,
                name=self.rp_circle_item.name()
            )

            self.tabs.legend.addItem(self.rp_circle_legend_proxy, self.rp_circle_legend_proxy.name())


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

            # ----------- DEPTH WINDOW -----------
            key = "depth_vs_station"
            w = self.plot_manager.get_or_create(key, title="Depth vs Station", seq=1)

            w.show()
            w.raise_()
            w.activateWindow()

            # IMPORTANT: tag window with current line (for red-line updates)
            w.state["current_line"] = int(line)
            w.state["on_station_selected"] = lambda st: self.on_dsr_station_clicked(line, st)

            # OPTIONAL (uncomment if you want red line cleared when line changes)
            # prev_line = w.state.get("prev_line")
            # if prev_line is not None and int(prev_line) != int(line):
            #     w.state.pop("selected_station", None)
            #     if "sel_line" in w.items:
            #         w.items["sel_line"].setVisible(False)
            # w.state["prev_line"] = int(line)

            PlotFactory.build_two_series_vs_station(
                w,
                depth_df,
                station_col="Station",  # <-- force Station on X axis (if you want)
                series1_col="PrimaryElevation",
                series2_col="SecondaryElevation",
                y_label="Elevation",
                title=f"Depth vs Station — Line {line}",
            )

            # ----------- SIGMA WINDOW -----------
            key1 = "sigmas_vs_station"
            w1 = self.plot_manager.get_or_create(key1, title="Sigma vs Station", seq=2)

            w1.show()
            w1.raise_()
            w1.activateWindow()

            # IMPORTANT: tag window with current line (for red-line updates)
            w1.state["current_line"] = int(line)
            w1.state["on_station_selected"] = lambda st: self.on_dsr_station_clicked(line, st)

            # OPTIONAL (uncomment if you want red line cleared when line changes)
            # prev_line1 = w1.state.get("prev_line")
            # if prev_line1 is not None and int(prev_line1) != int(line):
            #     w1.state.pop("selected_station", None)
            #     if "sel_line" in w1.items:
            #         w1.items["sel_line"].setVisible(False)
            # w1.state["prev_line"] = int(line)

            PlotFactory.build_two_series_vs_station(
                w1,
                depth_df,
                station_col="Station",  # <-- force Station on X axis (if you want)
                series1_col="Sigma1",
                series2_col="Sigma2",
                y_label="Sigma",
                title=f"Sigma vs Station — Line {line}",
            )
            # ----------- DEPTH WINDOW -----------
            key2 = "radial_offset_vs_station"
            w2 = self.plot_manager.get_or_create(key2, title="Radial Offset", seq=3)

            w2.show()
            w2.raise_()
            w2.activateWindow()

            # IMPORTANT: tag window with current line (for red-line updates)
            w2.state["current_line"] = int(line)
            w2.state["on_station_selected"] = lambda st: self.on_dsr_station_clicked(line, st)

            # OPTIONAL (uncomment if you want red line cleared when line changes)
            # prev_line = w.state.get("prev_line")
            # if prev_line is not None and int(prev_line) != int(line):
            #     w.state.pop("selected_station", None)
            #     if "sel_line" in w.items:
            #         w.items["sel_line"].setVisible(False)
            # w.state["prev_line"] = int(line)

            PlotFactory.build_two_series_vs_station(
                w2,
                depth_df,
                station_col="Station",  # <-- force Station on X axis (if you want)
                series1_col="DeltaEprimarytosecondary",
                series2_col="DeltaNprimarytosecondary",
                y_label="Radial Offset",
                title=f"Radial Offset — Line {line}",
            )

            # ✅ Sync X zoom/pan
            w1.plot.setXLink(w.plot)
            w2.plot.setXLink(w.plot)
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
                point_size=6,
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
                point_size=6,
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

            self.zoom_map_to_xy(center["X"], center["Y"], half_size_m=50.0)
            self.statusBar().showMessage(f"Zoom to Line {line} Station {station} (±50 m)")
            print("clicked station:", station)
            print("plot x range:", w.plot.viewRange()[0])
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

    def _add_bb_track(self, df, x_col, y_col, layer_key, label,
                      point_color="white", line_color="white", point_size=5, line_width=1.2):
        """
        Adds one track (line+scatter) to the map from df[x_col,y_col], ordered by TimeStamp.
        Stores the layer in self.bb_layers[layer_key] so we can remove/update later.
        """
        if df is None or df.empty:
            return

        if x_col not in df.columns or y_col not in df.columns:
            return

        d = df[[x_col, y_col]].copy()
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
        d = d.dropna(subset=[x_col, y_col])
        if d.empty:
            return

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
            connect_points=True,  # line + scatter
            plot_name=label,
            plot_id=layer_key,
            point_size=point_size,
            point_color=point_color,
            line_color=line_color,
            line_width=line_width,
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




