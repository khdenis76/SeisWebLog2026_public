from pathlib import Path

import pandas as pd
from PySide6 import QtCore, QtWidgets
from PySide6.QtGui import QAction

from db.django_db import DjangoDb, DjangoDbError
from ui.left_panel import LeftPanel
from ui.central_tabs import CentralTabs


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

        self._build_menu()
        self._build_ui()
        self._wire()
        self.statusBar().showMessage("Ready")

        # auto-load at start
        self.load_projects()

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

    def _build_ui(self):
        self.splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        self.setCentralWidget(self.splitter)

        self.left = LeftPanel()
        self.tabs = CentralTabs()

        self.splitter.addWidget(self.left)
        self.splitter.addWidget(self.tabs)
        self.splitter.setStretchFactor(0, 0)
        self.splitter.setStretchFactor(1, 1)
        self.splitter.setSizes([360, 1040])

    def _wire(self):
        self.act_exit.triggered.connect(self.close)
        self.act_toggle_left.triggered.connect(self._toggle_left)

        self.left.reloadProjectsClicked.connect(self.load_projects)
        self.left.projectChanged.connect(self.on_project_changed)

    def _toggle_left(self):
        self.left.setVisible(self.act_toggle_left.isChecked())

    def load_projects(self):
        try:
            db = DjangoDb("..", "db.sqlite")
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

        self.current_project = {
            "id": int(r["id"]),
            "name": str(r["name"]),
            "root_path": root_path,
            "folder_name": folder_name,
            "project_dir": str(project_dir),
        }
        self.left.set_project_dir(str(project_dir))
        self.statusBar().showMessage(f"Selected project: {self.current_project['name']}")
