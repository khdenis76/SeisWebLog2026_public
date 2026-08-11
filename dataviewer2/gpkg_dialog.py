from __future__ import annotations

from pathlib import Path
from PySide6 import QtCore, QtWidgets


class GeoPackageAttachDialog(QtWidgets.QDialog):
    """Select a GeoPackage and choose the internal vector layers to attach."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Attach GeoPackage")
        self.resize(760, 520)
        self._layers: list[dict] = []

        layout = QtWidgets.QVBoxLayout(self)
        path_row = QtWidgets.QHBoxLayout()
        self.path_edit = QtWidgets.QLineEdit()
        self.path_edit.setPlaceholderText("Select a .gpkg file…")
        browse = QtWidgets.QPushButton("Browse…")
        scan = QtWidgets.QPushButton("Scan layers")
        browse.clicked.connect(self._browse)
        scan.clicked.connect(self._scan)
        path_row.addWidget(self.path_edit, 1)
        path_row.addWidget(browse)
        path_row.addWidget(scan)
        layout.addLayout(path_row)

        self.info_label = QtWidgets.QLabel("Choose a GeoPackage, then select the layers to attach.")
        layout.addWidget(self.info_label)

        self.table = QtWidgets.QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(["Use", "Layer", "Geometry", "Features", "CRS"])
        self.table.horizontalHeader().setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeMode.Stretch)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        layout.addWidget(self.table, 1)

        controls = QtWidgets.QHBoxLayout()
        all_button = QtWidgets.QPushButton("Select all")
        none_button = QtWidgets.QPushButton("Clear")
        all_button.clicked.connect(lambda: self._set_all(True))
        none_button.clicked.connect(lambda: self._set_all(False))
        controls.addWidget(all_button)
        controls.addWidget(none_button)
        controls.addStretch(1)
        layout.addLayout(controls)

        buttons = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.StandardButton.Ok | QtWidgets.QDialogButtonBox.StandardButton.Cancel
        )
        self.ok_button = buttons.button(QtWidgets.QDialogButtonBox.StandardButton.Ok)
        self.ok_button.setText("Attach selected layers")
        self.ok_button.setEnabled(False)
        buttons.accepted.connect(self._accept_checked)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _browse(self) -> None:
        filename, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "Select GeoPackage", str(Path.home()), "GeoPackage (*.gpkg)"
        )
        if filename:
            self.path_edit.setText(filename)
            self._scan()

    def _scan(self) -> None:
        path = Path(self.path_edit.text().strip()).expanduser()
        if not path.is_file() or path.suffix.lower() != ".gpkg":
            QtWidgets.QMessageBox.warning(self, "GeoPackage", "Select a valid .gpkg file.")
            return
        try:
            import pyogrio
        except ImportError:
            QtWidgets.QMessageBox.critical(
                self, "GeoPackage support",
                "GeoPackage support requires pyogrio and pyarrow.\n\nRun:\npython -m pip install pyogrio pyarrow"
            )
            return
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.CursorShape.WaitCursor)
        try:
            rows = []
            for name, geometry_type in pyogrio.list_layers(path):
                try:
                    info = pyogrio.read_info(path, layer=name)
                except Exception:
                    info = {}
                rows.append({
                    "name": str(name),
                    "display_name": str(name),
                    "geometry_type": str(geometry_type or ""),
                    "features": info.get("features"),
                    "crs": str(info.get("crs") or ""),
                })
            self._layers = rows
            self.table.setRowCount(len(rows))
            for row_index, layer in enumerate(rows):
                check = QtWidgets.QTableWidgetItem()
                check.setFlags(check.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
                check.setCheckState(QtCore.Qt.CheckState.Unchecked)
                self.table.setItem(row_index, 0, check)
                self.table.setItem(row_index, 1, QtWidgets.QTableWidgetItem(layer["name"]))
                self.table.setItem(row_index, 2, QtWidgets.QTableWidgetItem(layer["geometry_type"] or "Unknown"))
                count = layer["features"]
                self.table.setItem(row_index, 3, QtWidgets.QTableWidgetItem("—" if count is None else f"{int(count):,}"))
                self.table.setItem(row_index, 4, QtWidgets.QTableWidgetItem(layer["crs"] or "Unknown"))
            self.info_label.setText(f"{len(rows)} vector layer(s) found in {path.name}")
            self.ok_button.setEnabled(bool(rows))
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "GeoPackage", f"Cannot scan GeoPackage:\n{exc}")
        finally:
            QtWidgets.QApplication.restoreOverrideCursor()

    def _set_all(self, checked: bool) -> None:
        state = QtCore.Qt.CheckState.Checked if checked else QtCore.Qt.CheckState.Unchecked
        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            if item is not None:
                item.setCheckState(state)

    def _accept_checked(self) -> None:
        if not self.selected_layers():
            QtWidgets.QMessageBox.information(self, "GeoPackage", "Select at least one layer.")
            return
        self.accept()

    def selected_layers(self) -> list[dict]:
        result = []
        for row, layer in enumerate(self._layers):
            item = self.table.item(row, 0)
            if item is not None and item.checkState() == QtCore.Qt.CheckState.Checked:
                result.append(dict(layer))
        return result

    @property
    def gpkg_path(self) -> Path:
        return Path(self.path_edit.text().strip()).expanduser().resolve()
