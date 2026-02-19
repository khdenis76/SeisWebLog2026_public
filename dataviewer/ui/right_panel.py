from PySide6 import QtCore, QtWidgets


class RightPanel(QtWidgets.QFrame):
    closeRequested = QtCore.Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameShape(QtWidgets.QFrame.Shape.StyledPanel)
        self.setMinimumWidth(280)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        header = QtWidgets.QHBoxLayout()
        self.lbl_title = QtWidgets.QLabel("Inspector")
        self.lbl_title.setStyleSheet("font-weight:600;")
        header.addWidget(self.lbl_title)
        header.addStretch(1)

        self.btn_close = QtWidgets.QToolButton()
        self.btn_close.setText("✕")
        self.btn_close.setToolTip("Close panel")
        self.btn_close.setAutoRaise(True)
        header.addWidget(self.btn_close)

        layout.addLayout(header)

        self.text = QtWidgets.QPlainTextEdit()
        self.text.setReadOnly(True)
        self.text.setPlaceholderText("Details will appear here (clicked point/series, project info, etc.)")
        layout.addWidget(self.text, 1)
        self.tbl_info = QtWidgets.QTableWidget()
        self.tbl_info.setColumnCount(2)
        self.tbl_info.setHorizontalHeaderLabels(["Field", "Value"])
        self.tbl_info.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        layout.addWidget(self.tbl_info, 1)

        self.btn_close.clicked.connect(self.closeRequested.emit)

    def set_text(self, s: str):
        self.text.setPlainText(s or "")

    def set_kv(self, data: dict):
        self.tbl_info.setRowCount(0)
        if not data:
            return

        keys = list(data.keys())
        self.tbl_info.setRowCount(len(keys))

        for i, k in enumerate(keys):
            self.tbl_info.setItem(i, 0, QtWidgets.QTableWidgetItem(str(k)))
            v = data.get(k)
            self.tbl_info.setItem(i, 1, QtWidgets.QTableWidgetItem("" if v is None else str(v)))

        self.tbl_info.resizeColumnsToContents()


