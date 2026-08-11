from __future__ import annotations

from pathlib import Path

from PySide6 import QtCore, QtGui, QtWidgets
from PySide6.QtSvgWidgets import QGraphicsSvgItem


class RotatingSvgView(QtWidgets.QGraphicsView):
    """Fixed wind-rose background with a heading-controlled SVG foreground."""

    def __init__(self, foreground: Path, background: Path | None = None, parent=None) -> None:
        super().__init__(parent)
        self.setRenderHints(
            QtGui.QPainter.RenderHint.Antialiasing
            | QtGui.QPainter.RenderHint.SmoothPixmapTransform
        )
        self.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        self.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.setMinimumHeight(155)
        self._scene = QtWidgets.QGraphicsScene(self)
        self.setScene(self._scene)

        self._background = None
        if background and background.exists():
            self._background = QGraphicsSvgItem(str(background))
            self._background.setZValue(0)
            self._scene.addItem(self._background)

        self._foreground = QGraphicsSvgItem(str(foreground))
        self._foreground.setZValue(10)
        self._scene.addItem(self._foreground)
        self._heading: float | None = None
        self._layout_items()

    def _layout_items(self) -> None:
        items = [item for item in (self._background, self._foreground) if item is not None]
        if not items:
            return
        target = QtCore.QPointF(0.0, 0.0)
        for item in items:
            rect = item.boundingRect()
            item.setTransformOriginPoint(rect.center())
            item.setPos(target - rect.center())
        united = items[0].mapRectToScene(items[0].boundingRect())
        for item in items[1:]:
            united = united.united(item.mapRectToScene(item.boundingRect()))
        self._scene.setSceneRect(united.adjusted(-4, -4, 4, 4))
        self.fitInView(self._scene.sceneRect(), QtCore.Qt.AspectRatioMode.KeepAspectRatio)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        if not self._scene.sceneRect().isEmpty():
            self.fitInView(self._scene.sceneRect(), QtCore.Qt.AspectRatioMode.KeepAspectRatio)

    def set_heading(self, value: float | None) -> None:
        if value is None:
            return
        try:
            angle = float(value) % 360.0
        except (TypeError, ValueError):
            return
        self._foreground.setRotation(angle)
        self._heading = angle
        self.viewport().update()


class HeadingCard(QtWidgets.QGroupBox):
    def __init__(self, title: str, foreground: Path, background: Path | None, parent=None) -> None:
        super().__init__(title, parent)
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(4, 7, 4, 5)
        layout.setSpacing(2)
        self.view = RotatingSvgView(foreground, background, self)
        self.value = QtWidgets.QLabel("Heading: —")
        self.value.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.value.setStyleSheet("font-weight: 700;")
        layout.addWidget(self.view, 1)
        layout.addWidget(self.value)

    def set_heading(self, value: float | None) -> None:
        if value is None:
            self.value.setText("Heading: —")
            return
        try:
            heading = float(value) % 360.0
        except (TypeError, ValueError):
            self.value.setText("Heading: —")
            return
        self.view.set_heading(heading)
        self.value.setText(f"Heading: {heading:.1f}°")


class HeadingPanel(QtWidgets.QWidget):
    """Vessel, ROV1 and ROV2 heading inspector used by BlackBox map tracks."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        resource = Path(__file__).resolve().parent / "resources" / "heading"
        windrose = resource / "windrose_inv.svg"
        vessel = resource / "vessel.svg"
        rov = resource / "rov_red.svg"

        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setSpacing(5)
        self.context = QtWidgets.QLabel("Move the cursor over a BlackBox track.")
        self.context.setWordWrap(True)
        self.context.setTextInteractionFlags(QtCore.Qt.TextInteractionFlag.TextSelectableByMouse)
        layout.addWidget(self.context)

        self.vessel = HeadingCard("Vessel", vessel, windrose)
        self.rov1 = HeadingCard("ROV 1", rov, windrose)
        self.rov2 = HeadingCard("ROV 2", rov, windrose)
        splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        splitter.addWidget(self.vessel)
        splitter.addWidget(self.rov1)
        splitter.addWidget(self.rov2)
        splitter.setSizes([200, 200, 200])
        layout.addWidget(splitter, 1)

    def set_context(self, text: str) -> None:
        self.context.setText(text or "Move the cursor over a BlackBox track.")

    def set_names(self, vessel_name: str | None = None, rov1_name: str | None = None, rov2_name: str | None = None) -> None:
        self.vessel.setTitle(str(vessel_name or "Vessel"))
        self.rov1.setTitle(str(rov1_name or "ROV 1"))
        self.rov2.setTitle(str(rov2_name or "ROV 2"))

    def set_headings(self, vessel=None, rov1=None, rov2=None) -> None:
        self.vessel.set_heading(vessel)
        self.rov1.set_heading(rov1)
        self.rov2.set_heading(rov2)
