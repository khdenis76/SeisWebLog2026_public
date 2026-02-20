import pandas as pd
from PySide6 import QtCore, QtWidgets, QtGui


class LeftPanel(QtWidgets.QFrame):
    projectChanged = QtCore.Signal(str)
    reloadProjectsClicked = QtCore.Signal()
    dsrLineClicked = QtCore.Signal(int)  # row click -> show details
    dsrSelectionChanged = QtCore.Signal(list)  # checkbox selection -> list of lines
    blackBoxSelectionChanged = QtCore.Signal(list)  # list of selected file IDs
    blackBoxRowClicked = QtCore.Signal(int)  # single click -> file ID
    dsrLineClicked = QtCore.Signal(int)  # row click -> single line
    dsrSelectionChanged = QtCore.Signal(list)  # checkbox selection -> list of lines (optional)
    dsrStationClicked = QtCore.Signal(int, int)  # (line, station/linepoint)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameShape(QtWidgets.QFrame.Shape.StyledPanel)
        self.setMinimumWidth(360)

        main_layout = QtWidgets.QVBoxLayout(self)
        main_layout.setContentsMargins(6, 6, 6, 6)

        # Tab widget inside left panel
        self.tabs = QtWidgets.QTabWidget()
        self.tabs.setTabPosition(QtWidgets.QTabWidget.TabPosition.North)

        main_layout.addWidget(self.tabs)

        # Build tabs
        self._build_projects_tab()
        self._build_dsr_tab()
        self._build_blackbox_tab()
        self._build_settings_tab()

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

        # signals
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

        # LEFT: Lines table
        self.tbl_dsr_lines = QtWidgets.QTableWidget()
        self.tbl_dsr_lines.setColumnCount(3)
        self.tbl_dsr_lines.setHorizontalHeaderLabels(["", "Line", "Stations"])
        self.tbl_dsr_lines.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.tbl_dsr_lines.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.tbl_dsr_lines.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.tbl_dsr_lines.setColumnWidth(0, 28)
        self.tbl_dsr_lines.cellClicked.connect(self._on_dsr_line_row_clicked)
        self.tbl_dsr_lines.itemChanged.connect(self._on_dsr_checkbox_changed)

        # RIGHT: Stations table (MUST be StationTableWidget so arrows emit)
        self.tbl_dsr_stations = StationTableWidget()
        self.tbl_dsr_stations.setColumnCount(5)
        self.tbl_dsr_stations.setHorizontalHeaderLabels(["Line", "Station", "Node", "ROV", "Deploy T"])
        self.tbl_dsr_stations.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.tbl_dsr_stations.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.tbl_dsr_stations.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)

        # Mouse click (optional)
        self.tbl_dsr_stations.cellClicked.connect(self._on_dsr_station_row_clicked)

        # Arrow keys (this is the important part)
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

        self.chk_dark_mode = QtWidgets.QCheckBox("Dark Mode")
        self.chk_auto_load = QtWidgets.QCheckBox("Auto-load last project")

        layout.addWidget(self.chk_dark_mode)
        layout.addWidget(self.chk_auto_load)
        layout.addStretch(1)

        self.tabs.addTab(tab, "Settings")

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

        # Stations column can be "Stations" or "Sations"
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

            chk = QtWidgets.QTableWidgetItem()
            chk.setFlags(QtCore.Qt.ItemFlag.ItemIsEnabled | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
            chk.setCheckState(QtCore.Qt.CheckState.Unchecked)
            self.tbl_dsr_lines.setItem(r, 0, chk)

            self.tbl_dsr_lines.setItem(r, 1, QtWidgets.QTableWidgetItem("" if pd.isna(line_val) else str(line_val)))
            self.tbl_dsr_lines.setItem(r, 2, QtWidgets.QTableWidgetItem("" if pd.isna(st_val) else str(st_val)))

        self.tbl_dsr_lines.resizeColumnsToContents()
        self.tbl_dsr_lines.setColumnWidth(0, 28)
        self.tbl_dsr_lines.blockSignals(False)

    def set_blackbox_files_table(self, df):
        self.tbl_bb_files.blockSignals(True)
        self.tbl_bb_files.setRowCount(0)

        if df is None or df.empty:
            self.tbl_bb_files.blockSignals(False)
            return

        # Determine ID column (ID or id)
        id_col = "ID" if "ID" in df.columns else ("id" if "id" in df.columns else None)
        if id_col is None:
            self.tbl_bb_files.blockSignals(False)
            return

        # Determine file name column (FileName/File_Name/Name etc.)
        name_col = None
        for cand in ("FileName", "FILE_NAME", "File_Name", "Name", "Filename"):
            if cand in df.columns:
                name_col = cand
                break
        if name_col is None:
            # fallback: show first text-like column
            for c in df.columns:
                if df[c].dtype == object:
                    name_col = c
                    break
        if name_col is None:
            name_col = id_col  # worst case

        self.tbl_bb_files.setRowCount(len(df))

        for r in range(len(df)):
            file_id = df.at[df.index[r], id_col]
            file_name = df.at[df.index[r], name_col]

            # checkbox
            chk = QtWidgets.QTableWidgetItem()
            chk.setFlags(QtCore.Qt.ItemFlag.ItemIsEnabled | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
            chk.setCheckState(QtCore.Qt.CheckState.Unchecked)
            self.tbl_bb_files.setItem(r, 0, chk)

            # ID
            self.tbl_bb_files.setItem(r, 1, QtWidgets.QTableWidgetItem("" if pd.isna(file_id) else str(file_id)))

            # FileName
            self.tbl_bb_files.setItem(r, 2, QtWidgets.QTableWidgetItem("" if pd.isna(file_name) else str(file_name)))

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
                    selected.append(int(line_item.text()))
                except ValueError:
                    pass

        self.dsrSelectionChanged.emit(selected)

    def set_dsr_stations_table(self, df):
        self.tbl_dsr_stations.setRowCount(0)

        if df is None or df.empty:
            return

        # Expecting columns: Line, LinePoint, Nodes, FirstTime, LastTime
        self.tbl_dsr_stations.setRowCount(len(df))

        for r in range(len(df)):
            self.tbl_dsr_stations.setItem(r, 0, QtWidgets.QTableWidgetItem(str(df.iloc[r]["Line"])))
            self.tbl_dsr_stations.setItem(r, 1, QtWidgets.QTableWidgetItem(str(df.iloc[r]["Station"])))
            self.tbl_dsr_stations.setItem(r, 2, QtWidgets.QTableWidgetItem(str(df.iloc[r]["Node"])))
            self.tbl_dsr_stations.setItem(r, 3, QtWidgets.QTableWidgetItem(str(df.iloc[r]["ROV"])))
            self.tbl_dsr_stations.setItem(r, 4, QtWidgets.QTableWidgetItem(str(df.iloc[r]["DeployTime"])))

        self.tbl_dsr_stations.resizeColumnsToContents()
        self.tbl_dsr_stations.setCurrentCell(0, 0)
        if self.tbl_dsr_stations.rowCount() > 0:
            self.tbl_dsr_stations.setCurrentCell(0, 0)
            self.tbl_dsr_stations.setFocus()

    def _on_bb_row_clicked(self, row: int, col: int):
        if col == 0:
            return
        item = self.tbl_bb_files.item(row, 1)  # ID column
        if not item:
            return
        try:
            file_id = int(item.text())
        except ValueError:
            return
        self.blackBoxRowClicked.emit(file_id)

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
                    selected.append(int(id_item.text()))
                except ValueError:
                    pass

        self.blackBoxSelectionChanged.emit(selected)

    def _on_dsr_station_row_clicked(self, row: int, col: int):
        # expecting stations table columns: Line, Station, ...
        line_item = self.tbl_dsr_stations.item(row, 0)
        st_item = self.tbl_dsr_stations.item(row, 1)
        if not line_item or not st_item:
            return
        try:
            line = int(line_item.text())
            station = int(st_item.text())
        except ValueError:
            return

        def _activate_current_station(self):
            row = self.tbl_dsr_stations.currentRow()
            if row < 0:
                return
            line_item = self.tbl_dsr_stations.item(row, 0)
            st_item = self.tbl_dsr_stations.item(row, 1)
            if not line_item or not st_item:
                return
            try:
                line = int(line_item.text())
                station = int(st_item.text())
            except ValueError:
                return
            self.dsrStationClicked.emit(line, station)

        self.dsrStationClicked.emit(line, station)

    def _activate_current_station(self):
        row = self.tbl_dsr_stations.currentRow()
        if row < 0:
            return

        line_item = self.tbl_dsr_stations.item(row, 0)  # Line
        st_item = self.tbl_dsr_stations.item(row, 1)  # Station

        if not line_item or not st_item:
            return

        try:
            line = int(line_item.text())
            station = int(st_item.text())
        except ValueError:
            return

        self.dsrStationClicked.emit(line, station)

    def _on_dsr_line_row_clicked(self, row: int, col: int):
        if col == 0:
            return  # checkbox col

        item = self.tbl_dsr_lines.item(row, 1)  # column 1 MUST be "Line"
        if not item:
            return

        try:
            line = int(item.text())
        except ValueError:
            return

        print("LeftPanel emitted line:", line)  # DEBUG
        self.dsrLineClicked.emit(line)

    def _on_station_current_row_changed(self, current, previous):
        row = current.row()
        if row < 0:
            return

        line_item = self.tbl_dsr_stations.item(row, 0)  # Line column
        st_item = self.tbl_dsr_stations.item(row, 1)  # Station column
        if not line_item or not st_item:
            return

        try:
            line = int(line_item.text())
            station = int(st_item.text())
        except ValueError:
            return

        self.dsrStationClicked.emit(line, station)


class StationTableWidget(QtWidgets.QTableWidget):
    stationActivated = QtCore.Signal(int, int)  # (line, station)

    def keyPressEvent(self, event):
        key = event.key()
        if key in (QtCore.Qt.Key.Key_Up, QtCore.Qt.Key.Key_Down):
            # let the table move selection first
            super().keyPressEvent(event)

            row = self.currentRow()
            if row < 0:
                return

            line_item = self.item(row, 0)   # Line column
            st_item = self.item(row, 1)     # Station column
            if not line_item or not st_item:
                return

            try:
                line = int(line_item.text())
                station = int(st_item.text())
            except ValueError:
                return

            self.stationActivated.emit(line, station)
            return

        super().keyPressEvent(event)



