import pandas as pd
from PySide6 import QtCore, QtWidgets, QtGui


class StationTableWidget(QtWidgets.QTableWidget):
    stationActivated = QtCore.Signal(int, int)  # (line, station)

    def keyPressEvent(self, event):
        key = event.key()
        if key in (QtCore.Qt.Key.Key_Up, QtCore.Qt.Key.Key_Down):
            super().keyPressEvent(event)

            row = self.currentRow()
            if row < 0:
                return

            line_item = self.item(row, 0)
            st_item = self.item(row, 1)
            if not line_item or not st_item:
                return

            try:
                line = int(float(line_item.text()))
                station = int(float(st_item.text()))
            except ValueError:
                return

            self.stationActivated.emit(line, station)
            return

        super().keyPressEvent(event)


class LeftPanel(QtWidgets.QFrame):
    rpRadiusChanged = QtCore.Signal(float)
    projectChanged = QtCore.Signal(str)
    plotsSettingsChanged = QtCore.Signal(dict)
    reloadProjectsClicked = QtCore.Signal()

    dsrLineClicked = QtCore.Signal(int)
    dsrSelectionChanged = QtCore.Signal(list)
    dsrStationClicked = QtCore.Signal(int, int)

    blackBoxSelectionChanged = QtCore.Signal(list)
    blackBoxRowClicked = QtCore.Signal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameShape(QtWidgets.QFrame.Shape.StyledPanel)
        self.setMinimumWidth(360)

        main_layout = QtWidgets.QVBoxLayout(self)
        main_layout.setContentsMargins(6, 6, 6, 6)

        self.tabs = QtWidgets.QTabWidget()
        self.tabs.setTabPosition(QtWidgets.QTabWidget.TabPosition.North)
        main_layout.addWidget(self.tabs)

        self._build_projects_tab()
        self._build_dsr_tab()
        self._build_blackbox_tab()
        self._build_settings_tab()
        self._wire_settings_signals()

        QtCore.QTimer.singleShot(0, self._emit_plot_settings)

    # ----------------------------------
    # TAB 1 — Projects
    # ----------------------------------
    def _build_projects_tab(self):
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)

        gb = QtWidgets.QGroupBox("Django Projects")
        form = QtWidgets.QFormLayout(gb)

        self.btn_reload_projects = QtWidgets.QPushButton("Load Projects")
        self.cmb_project = QtWidgets.QComboBox()
        self.lbl_project_dir = QtWidgets.QLabel("-")
        self.lbl_project_dir.setTextInteractionFlags(
            QtCore.Qt.TextInteractionFlag.TextSelectableByMouse
        )

        form.addRow("", self.btn_reload_projects)
        form.addRow("Project:", self.cmb_project)
        form.addRow("Project dir:", self.lbl_project_dir)

        layout.addWidget(gb)
        layout.addStretch(1)

        self.tabs.addTab(tab, "Projects")

        self.btn_reload_projects.clicked.connect(self.reloadProjectsClicked.emit)
        self.cmb_project.currentTextChanged.connect(self.projectChanged.emit)

    # ----------------------------------
    # TAB 2 — DSR
    # ----------------------------------
    def _build_dsr_tab(self):
        tab = QtWidgets.QWidget()
        h = QtWidgets.QHBoxLayout(tab)
        h.setContentsMargins(0, 0, 0, 0)

        self.dsr_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)

        self.tbl_dsr_lines = QtWidgets.QTableWidget()
        self.tbl_dsr_lines.setColumnCount(6)
        self.tbl_dsr_lines.setHorizontalHeaderLabels(
            ["", "Line", "Stations", "Vessel", "ROV 1", "ROV 2"]
        )
        self.tbl_dsr_lines.setSelectionBehavior(
            QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows
        )
        self.tbl_dsr_lines.setSelectionMode(
            QtWidgets.QAbstractItemView.SelectionMode.SingleSelection
        )
        self.tbl_dsr_lines.setEditTriggers(
            QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers
        )
        self.tbl_dsr_lines.setColumnWidth(0, 28)
        self.tbl_dsr_lines.cellClicked.connect(self._on_dsr_line_row_clicked)
        self.tbl_dsr_lines.itemChanged.connect(self._on_dsr_checkbox_changed)

        self.tbl_dsr_stations = StationTableWidget()
        self.tbl_dsr_stations.setColumnCount(5)
        self.tbl_dsr_stations.setHorizontalHeaderLabels(
            ["Line", "Station", "Node", "ROV", "Deploy T"]
        )
        self.tbl_dsr_stations.setSelectionBehavior(
            QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows
        )
        self.tbl_dsr_stations.setSelectionMode(
            QtWidgets.QAbstractItemView.SelectionMode.SingleSelection
        )
        self.tbl_dsr_stations.setEditTriggers(
            QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers
        )
        self.tbl_dsr_stations.cellClicked.connect(self._on_dsr_station_row_clicked)
        self.tbl_dsr_stations.stationActivated.connect(self.dsrStationClicked.emit)

        self.dsr_splitter.addWidget(self.tbl_dsr_lines)
        self.dsr_splitter.addWidget(self.tbl_dsr_stations)
        self.dsr_splitter.setStretchFactor(0, 0)
        self.dsr_splitter.setStretchFactor(1, 1)
        self.dsr_splitter.setSizes([320, 520])

        h.addWidget(self.dsr_splitter, 1)
        self.tabs.addTab(tab, "DSR")

    # ----------------------------------
    # TAB 3 — BlackBox
    # ----------------------------------
    def _build_blackbox_tab(self):
        tab = QtWidgets.QWidget()
        v = QtWidgets.QVBoxLayout(tab)

        self.tbl_bb_files = QtWidgets.QTableWidget()
        self.tbl_bb_files.setColumnCount(3)
        self.tbl_bb_files.setHorizontalHeaderLabels(["", "ID", "FileName"])
        self.tbl_bb_files.setSelectionBehavior(
            QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows
        )
        self.tbl_bb_files.setSelectionMode(
            QtWidgets.QAbstractItemView.SelectionMode.SingleSelection
        )
        self.tbl_bb_files.setEditTriggers(
            QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers
        )
        self.tbl_bb_files.setColumnWidth(0, 28)

        v.addWidget(self.tbl_bb_files, 1)
        self.tabs.addTab(tab, "BlackBox")

        self.tbl_bb_files.cellClicked.connect(self._on_bb_row_clicked)
        self.tbl_bb_files.itemChanged.connect(self._on_bb_checkbox_changed)

    # ----------------------------------
    # TAB 4 — Settings
    # ----------------------------------
    def _build_settings_tab(self):
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)

        # ---------- General ----------
        general_box = QtWidgets.QGroupBox("General")
        g = QtWidgets.QVBoxLayout(general_box)

        self.chk_dark_mode = QtWidgets.QCheckBox("Dark Mode")
        self.chk_auto_load = QtWidgets.QCheckBox("Auto-load last project")
        # backward-compatible alias for old code
        self.chk_autoload_last_project = self.chk_auto_load

        g.addWidget(self.chk_dark_mode)
        g.addWidget(self.chk_auto_load)

        # ---------- RP ----------
        rp_box = QtWidgets.QGroupBox("RP")
        rp = QtWidgets.QFormLayout(rp_box)
        rp.setLabelAlignment(QtCore.Qt.AlignmentFlag.AlignLeft)

        self.spn_rp_radius = QtWidgets.QDoubleSpinBox()
        self.spn_rp_radius.setRange(0.0, 100000.0)
        self.spn_rp_radius.setDecimals(1)
        self.spn_rp_radius.setSingleStep(5.0)
        self.spn_rp_radius.setValue(25.0)

        self.spn_rp_point_size = QtWidgets.QSpinBox()
        self.spn_rp_point_size.setRange(1, 50)
        self.spn_rp_point_size.setSingleStep(1)
        self.spn_rp_point_size.setValue(6)

        # backward-compatible aliases
        self.spin_rp_radius = self.spn_rp_radius
        self.spin_rp_point_size = self.spn_rp_point_size

        rp.addRow("Circle radius (m):", self.spn_rp_radius)
        rp.addRow("RPPreplot point size:", self.spn_rp_point_size)

        # ---------- DSR ----------
        dsr_box = QtWidgets.QGroupBox("DSR")
        dsr = QtWidgets.QFormLayout(dsr_box)
        dsr.setLabelAlignment(QtCore.Qt.AlignmentFlag.AlignLeft)

        self.chk_dsr_depth = QtWidgets.QCheckBox("Depth window")
        self.chk_dsr_sigmas = QtWidgets.QCheckBox("Sigmas window")
        self.chk_dsr_radial = QtWidgets.QCheckBox("Radial Offset window")
        self.chk_dsr_depth.setChecked(True)
        self.chk_dsr_sigmas.setChecked(True)
        self.chk_dsr_radial.setChecked(True)

        self.spn_dsr_primary_size = QtWidgets.QSpinBox()
        self.spn_dsr_primary_size.setRange(1, 50)
        self.spn_dsr_primary_size.setValue(6)

        self.spn_dsr_secondary_size = QtWidgets.QSpinBox()
        self.spn_dsr_secondary_size.setRange(1, 50)
        self.spn_dsr_secondary_size.setValue(6)

        # backward-compatible aliases
        self.spin_dsr_primary_size = self.spn_dsr_primary_size
        self.spin_dsr_secondary_size = self.spn_dsr_secondary_size

        dsr.addRow(self.chk_dsr_depth)
        dsr.addRow(self.chk_dsr_sigmas)
        dsr.addRow(self.chk_dsr_radial)
        dsr.addRow("DSR Primary point size:", self.spn_dsr_primary_size)
        dsr.addRow("DSR Secondary point size:", self.spn_dsr_secondary_size)

        # ---------- BlackBox ----------
        bb_box = QtWidgets.QGroupBox("BlackBox")
        bb = QtWidgets.QVBoxLayout(bb_box)

        self.chk_bb_show_tracks = QtWidgets.QCheckBox("Show BlackBox tracks on map")
        self.chk_bb_show_tracks.setChecked(True)

        self.chk_bb_show_timeseries = QtWidgets.QCheckBox("Show BlackBox time-series window")
        self.chk_bb_show_timeseries.setChecked(True)

        bb.addWidget(self.chk_bb_show_tracks)
        bb.addWidget(self.chk_bb_show_timeseries)

        tracks_box = QtWidgets.QGroupBox("BlackBox tracks")
        tracks_layout = QtWidgets.QVBoxLayout(tracks_box)

        self.chk_bb_vessel = QtWidgets.QCheckBox("Vessel")
        self.chk_bb_rov1_ins = QtWidgets.QCheckBox("ROV1 INS")
        self.chk_bb_rov2_ins = QtWidgets.QCheckBox("ROV2 INS")
        self.chk_bb_rov1_usbl = QtWidgets.QCheckBox("ROV1 USBL")
        self.chk_bb_rov2_usbl = QtWidgets.QCheckBox("ROV2 USBL")

        for cb in (
            self.chk_bb_vessel,
            self.chk_bb_rov1_ins,
            self.chk_bb_rov2_ins,
            self.chk_bb_rov1_usbl,
            self.chk_bb_rov2_usbl,
        ):
            cb.setChecked(True)
            tracks_layout.addWidget(cb)

        ts_box = QtWidgets.QGroupBox("BlackBox time-series")
        ts_layout = QtWidgets.QVBoxLayout(ts_box)

        self.chk_ts_hdg = QtWidgets.QCheckBox("Heading graph")
        self.chk_ts_sog = QtWidgets.QCheckBox("SOG graph")
        self.chk_ts_cog = QtWidgets.QCheckBox("COG graph")
        self.chk_ts_nos = QtWidgets.QCheckBox("Number of satellites graph")
        self.chk_ts_diffage = QtWidgets.QCheckBox("GPS Diff Age graph")
        self.chk_ts_fixquality = QtWidgets.QCheckBox("GPS FixQuality graph")
        self.chk_ts_hdop = QtWidgets.QCheckBox("HDOP graph")
        self.chk_ts_depth = QtWidgets.QCheckBox("Depth graph")

        for cb in (
            self.chk_ts_hdg,
            self.chk_ts_sog,
            self.chk_ts_cog,
            self.chk_ts_nos,
            self.chk_ts_diffage,
            self.chk_ts_fixquality,
            self.chk_ts_hdop,
            self.chk_ts_depth,
        ):
            cb.setChecked(True)
            ts_layout.addWidget(cb)

        bb.addWidget(tracks_box)
        bb.addWidget(ts_box)

        layout.addWidget(general_box)
        layout.addWidget(rp_box)
        layout.addWidget(dsr_box)
        layout.addWidget(bb_box)
        layout.addStretch(1)

        self.tabs.addTab(tab, "Settings")

        # rp radius should update immediately
        self.spn_rp_radius.valueChanged.connect(self.rpRadiusChanged.emit)

    # ----------------------------------
    # Settings helpers
    # ----------------------------------
    def _wire_settings_signals(self):
        checkbox_names = [
            "chk_dark_mode",
            "chk_autoload_last_project",
            "chk_dsr_depth",
            "chk_dsr_sigmas",
            "chk_dsr_radial",
            "chk_bb_show_tracks",
            "chk_bb_show_timeseries",
            "chk_bb_vessel",
            "chk_bb_rov1_ins",
            "chk_bb_rov2_ins",
            "chk_bb_rov1_usbl",
            "chk_bb_rov2_usbl",
            "chk_ts_hdg",
            "chk_ts_sog",
            "chk_ts_cog",
            "chk_ts_nos",
            "chk_ts_diffage",
            "chk_ts_fixquality",
            "chk_ts_hdop",
            "chk_ts_depth",
        ]
        for name in checkbox_names:
            w = getattr(self, name, None)
            if w is not None:
                w.toggled.connect(self._emit_plot_settings)

        spin_names = [
            "spn_rp_point_size",
            "spn_dsr_primary_size",
            "spn_dsr_secondary_size",
        ]
        for name in spin_names:
            w = getattr(self, name, None)
            if w is not None:
                w.editingFinished.connect(self._emit_plot_settings)

    def _emit_plot_settings(self):
        self.plotsSettingsChanged.emit(self.get_plot_settings())

    def get_plot_settings(self) -> dict:
        return {
            "general": {
                "dark_mode": self.chk_dark_mode.isChecked(),
                "autoload_last_project": self.chk_autoload_last_project.isChecked(),
            },
            "rp": {
                "circle_radius": float(self.spn_rp_radius.value()),
                "point_size": int(self.spn_rp_point_size.value()),
            },
            "dsr": {
                "depth": self.chk_dsr_depth.isChecked(),
                "sigmas": self.chk_dsr_sigmas.isChecked(),
                "radial": self.chk_dsr_radial.isChecked(),
                "primary_point_size": int(self.spn_dsr_primary_size.value()),
                "secondary_point_size": int(self.spn_dsr_secondary_size.value()),
            },
            "bb": {
                "show_tracks_window": self.chk_bb_show_tracks.isChecked(),
                "show_timeseries_window": self.chk_bb_show_timeseries.isChecked(),
                "bb_vessel": self.chk_bb_vessel.isChecked(),
                "bb_rov1_ins": self.chk_bb_rov1_ins.isChecked(),
                "bb_rov2_ins": self.chk_bb_rov2_ins.isChecked(),
                "bb_rov1_usbl": self.chk_bb_rov1_usbl.isChecked(),
                "bb_rov2_usbl": self.chk_bb_rov2_usbl.isChecked(),
                "ts_hdg": self.chk_ts_hdg.isChecked(),
                "ts_sog": self.chk_ts_sog.isChecked(),
                "ts_cog": self.chk_ts_cog.isChecked(),
                "ts_nos": self.chk_ts_nos.isChecked(),
                "ts_diffage": self.chk_ts_diffage.isChecked(),
                "ts_fixquality": self.chk_ts_fixquality.isChecked(),
                "ts_hdop": self.chk_ts_hdop.isChecked(),
                "ts_depth": self.chk_ts_depth.isChecked(),
            },
        }

    def apply_plot_settings(self, settings: dict):
        general = settings.get("general", {})
        rp = settings.get("rp", {})
        dsr = settings.get("dsr", {})
        bb = settings.get("bb", {})

        self.chk_dark_mode.setChecked(bool(general.get("dark_mode", False)))
        self.chk_autoload_last_project.setChecked(
            bool(general.get("autoload_last_project", False))
        )

        self.spn_rp_radius.setValue(float(rp.get("circle_radius", 25.0)))
        self.spn_rp_point_size.setValue(int(rp.get("point_size", 6)))

        self.chk_dsr_depth.setChecked(bool(dsr.get("depth", True)))
        self.chk_dsr_sigmas.setChecked(bool(dsr.get("sigmas", True)))
        self.chk_dsr_radial.setChecked(bool(dsr.get("radial", True)))
        self.spn_dsr_primary_size.setValue(int(dsr.get("primary_point_size", 6)))
        self.spn_dsr_secondary_size.setValue(int(dsr.get("secondary_point_size", 6)))

        self.chk_bb_show_tracks.setChecked(bool(bb.get("show_tracks_window", True)))
        self.chk_bb_show_timeseries.setChecked(bool(bb.get("show_timeseries_window", True)))

        self.chk_bb_vessel.setChecked(bool(bb.get("bb_vessel", True)))
        self.chk_bb_rov1_ins.setChecked(bool(bb.get("bb_rov1_ins", True)))
        self.chk_bb_rov2_ins.setChecked(bool(bb.get("bb_rov2_ins", True)))
        self.chk_bb_rov1_usbl.setChecked(bool(bb.get("bb_rov1_usbl", True)))
        self.chk_bb_rov2_usbl.setChecked(bool(bb.get("bb_rov2_usbl", True)))

        self.chk_ts_hdg.setChecked(bool(bb.get("ts_hdg", True)))
        self.chk_ts_sog.setChecked(bool(bb.get("ts_sog", True)))
        self.chk_ts_cog.setChecked(bool(bb.get("ts_cog", True)))
        self.chk_ts_nos.setChecked(bool(bb.get("ts_nos", True)))
        self.chk_ts_diffage.setChecked(bool(bb.get("ts_diffage", True)))
        self.chk_ts_fixquality.setChecked(bool(bb.get("ts_fixquality", True)))
        self.chk_ts_hdop.setChecked(bool(bb.get("ts_hdop", True)))
        self.chk_ts_depth.setChecked(bool(bb.get("ts_depth", True)))

    def set_rp_radius(self, value: float):
        self.spn_rp_radius.blockSignals(True)
        self.spn_rp_radius.setValue(float(value))
        self.spn_rp_radius.blockSignals(False)

    # ----------------------------------
    # Helpers
    # ----------------------------------
    def set_projects(self, names):
        self.cmb_project.blockSignals(True)
        self.cmb_project.clear()
        for n in names:
            self.cmb_project.addItem(str(n))
        self.cmb_project.blockSignals(False)

    def set_project_dir(self, path: str):
        self.lbl_project_dir.setText(path or "-")

    def set_dsr_lines_table(self, df):
        self.tbl_dsr_lines.blockSignals(True)
        self.tbl_dsr_lines.setRowCount(0)

        if df is None or df.empty:
            self.tbl_dsr_lines.blockSignals(False)
            return

        stations_col = None
        for c in df.columns:
            if str(c).lower() in ("stations", "sations"):
                stations_col = c
                break

        if "Line" not in df.columns or stations_col is None:
            self.tbl_dsr_lines.blockSignals(False)
            return

        self.tbl_dsr_lines.setRowCount(len(df))

        for r in range(len(df)):
            line_val = df.iloc[r]["Line"]
            st_val = df.iloc[r][stations_col]
            vessel_val = df.iloc[r]["Vessel_name"] if "Vessel_name" in df.columns else ""
            rov1_val = df.iloc[r]["rov1_name"] if "rov1_name" in df.columns else ""
            rov2_val = df.iloc[r]["rov2_name"] if "rov2_name" in df.columns else ""

            chk = QtWidgets.QTableWidgetItem()
            chk.setFlags(
                QtCore.Qt.ItemFlag.ItemIsEnabled
                | QtCore.Qt.ItemFlag.ItemIsUserCheckable
            )
            chk.setCheckState(QtCore.Qt.CheckState.Unchecked)
            self.tbl_dsr_lines.setItem(r, 0, chk)

            self.tbl_dsr_lines.setItem(
                r, 1, QtWidgets.QTableWidgetItem("" if pd.isna(line_val) else str(line_val))
            )
            self.tbl_dsr_lines.setItem(
                r, 2, QtWidgets.QTableWidgetItem("" if pd.isna(st_val) else str(st_val))
            )
            self.tbl_dsr_lines.setItem(
                r, 3, QtWidgets.QTableWidgetItem("" if pd.isna(vessel_val) else str(vessel_val))
            )
            self.tbl_dsr_lines.setItem(
                r, 4, QtWidgets.QTableWidgetItem("" if pd.isna(rov1_val) else str(rov1_val))
            )
            self.tbl_dsr_lines.setItem(
                r, 5, QtWidgets.QTableWidgetItem("" if pd.isna(rov2_val) else str(rov2_val))
            )

        self.tbl_dsr_lines.resizeColumnsToContents()
        self.tbl_dsr_lines.setColumnWidth(0, 28)
        self.tbl_dsr_lines.blockSignals(False)

    def set_dsr_stations_table(self, df):
        self.tbl_dsr_stations.setRowCount(0)

        if df is None or df.empty:
            return

        self.tbl_dsr_stations.setRowCount(len(df))

        for r in range(len(df)):
            line_val = df.iloc[r]["Line"] if "Line" in df.columns else ""
            st_val = df.iloc[r]["Station"] if "Station" in df.columns else ""
            node_val = df.iloc[r]["Node"] if "Node" in df.columns else ""
            rov_val = df.iloc[r]["ROV"] if "ROV" in df.columns else ""
            dep_val = (
                df.iloc[r]["DeployTime"]
                if "DeployTime" in df.columns
                else (df.iloc[r]["Deploy T"] if "Deploy T" in df.columns else "")
            )

            self.tbl_dsr_stations.setItem(r, 0, QtWidgets.QTableWidgetItem(str(line_val)))
            self.tbl_dsr_stations.setItem(r, 1, QtWidgets.QTableWidgetItem(str(st_val)))
            self.tbl_dsr_stations.setItem(r, 2, QtWidgets.QTableWidgetItem(str(node_val)))
            self.tbl_dsr_stations.setItem(r, 3, QtWidgets.QTableWidgetItem(str(rov_val)))
            self.tbl_dsr_stations.setItem(r, 4, QtWidgets.QTableWidgetItem(str(dep_val)))

        self.tbl_dsr_stations.resizeColumnsToContents()
        if self.tbl_dsr_stations.rowCount() > 0:
            self.tbl_dsr_stations.setCurrentCell(0, 0)
            self.tbl_dsr_stations.setFocus()

    def set_blackbox_files_table(self, df):
        self.tbl_bb_files.blockSignals(True)
        self.tbl_bb_files.setRowCount(0)

        if df is None or df.empty:
            self.tbl_bb_files.blockSignals(False)
            return

        id_col = "ID" if "ID" in df.columns else ("id" if "id" in df.columns else None)
        if id_col is None:
            self.tbl_bb_files.blockSignals(False)
            return

        name_col = None
        for cand in ("FileName", "FILE_NAME", "File_Name", "Name", "Filename"):
            if cand in df.columns:
                name_col = cand
                break
        if name_col is None:
            for c in df.columns:
                if df[c].dtype == object:
                    name_col = c
                    break
        if name_col is None:
            name_col = id_col

        self.tbl_bb_files.setRowCount(len(df))

        for r in range(len(df)):
            file_id = df.at[df.index[r], id_col]
            file_name = df.at[df.index[r], name_col]

            chk = QtWidgets.QTableWidgetItem()
            chk.setFlags(
                QtCore.Qt.ItemFlag.ItemIsEnabled
                | QtCore.Qt.ItemFlag.ItemIsUserCheckable
            )
            chk.setCheckState(QtCore.Qt.CheckState.Unchecked)
            self.tbl_bb_files.setItem(r, 0, chk)

            self.tbl_bb_files.setItem(
                r, 1, QtWidgets.QTableWidgetItem("" if pd.isna(file_id) else str(file_id))
            )
            self.tbl_bb_files.setItem(
                r, 2, QtWidgets.QTableWidgetItem("" if pd.isna(file_name) else str(file_name))
            )

        self.tbl_bb_files.resizeColumnsToContents()
        self.tbl_bb_files.setColumnWidth(0, 28)
        self.tbl_bb_files.blockSignals(False)

    def _on_dsr_checkbox_changed(self, item: QtWidgets.QTableWidgetItem):
        if item.column() != 0:
            return

        selected = []
        for r in range(self.tbl_dsr_lines.rowCount()):
            chk = self.tbl_dsr_lines.item(r, 0)
            line_item = self.tbl_dsr_lines.item(r, 1)
            if not chk or not line_item:
                continue
            if chk.checkState() == QtCore.Qt.CheckState.Checked:
                try:
                    selected.append(int(float(line_item.text())))
                except ValueError:
                    pass

        self.dsrSelectionChanged.emit(selected)

    def _on_bb_checkbox_changed(self, item: QtWidgets.QTableWidgetItem):
        if item.column() != 0:
            return

        selected = []
        for r in range(self.tbl_bb_files.rowCount()):
            chk = self.tbl_bb_files.item(r, 0)
            id_item = self.tbl_bb_files.item(r, 1)
            if not chk or not id_item:
                continue
            if chk.checkState() == QtCore.Qt.CheckState.Checked:
                try:
                    selected.append(int(float(id_item.text())))
                except ValueError:
                    pass

        self.blackBoxSelectionChanged.emit(selected)

    def _on_bb_row_clicked(self, row: int, col: int):
        if col == 0:
            return
        item = self.tbl_bb_files.item(row, 1)
        if not item:
            return
        try:
            file_id = int(float(item.text()))
        except ValueError:
            return
        self.blackBoxRowClicked.emit(file_id)

    def _on_dsr_station_row_clicked(self, row: int, col: int):
        line_item = self.tbl_dsr_stations.item(row, 0)
        st_item = self.tbl_dsr_stations.item(row, 1)
        if not line_item or not st_item:
            return
        try:
            line = int(float(line_item.text()))
            station = int(float(st_item.text()))
        except ValueError:
            return

        self.dsrStationClicked.emit(line, station)

    def _activate_current_station(self):
        row = self.tbl_dsr_stations.currentRow()
        if row < 0:
            return

        line_item = self.tbl_dsr_stations.item(row, 0)
        st_item = self.tbl_dsr_stations.item(row, 1)

        if not line_item or not st_item:
            return

        try:
            line = int(float(line_item.text()))
            station = int(float(st_item.text()))
        except ValueError:
            return

        self.dsrStationClicked.emit(line, station)

    def _on_dsr_line_row_clicked(self, row: int, col: int):
        if col == 0:
            return

        item = self.tbl_dsr_lines.item(row, 1)
        if not item:
            return

        try:
            line = int(float(item.text()))
        except ValueError:
            return

        print("LeftPanel emitted line:", line)
        self.dsrLineClicked.emit(line)