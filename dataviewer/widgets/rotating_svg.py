from PySide6 import QtCore, QtGui, QtWidgets
from PySide6.QtSvgWidgets import QGraphicsSvgItem


class RotatingSvgView(QtWidgets.QGraphicsView):
    """
    QGraphicsView with:
      - optional background SVG (wind rose) fixed
      - foreground SVG rotated around its center (vessel/rov)
    """
    def __init__(
        self,
        fg_svg_path: str,
        parent=None,
        bg_svg_path: str | None = None,
        fg_scale: float = 1.0,
        bg_scale: float = 1.0,
    ):
        super().__init__(parent)

        self.setRenderHints(
            QtGui.QPainter.RenderHint.Antialiasing
            | QtGui.QPainter.RenderHint.SmoothPixmapTransform
        )

        self._scene = QtWidgets.QGraphicsScene(self)
        self.setScene(self._scene)
        self.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)

        self._bg_item = None
        if bg_svg_path:
            self._bg_item = QGraphicsSvgItem(bg_svg_path)
            self._bg_item.setZValue(0)
            self._scene.addItem(self._bg_item)

        self._fg_item = QGraphicsSvgItem(fg_svg_path)
        self._fg_item.setZValue(10)
        self._scene.addItem(self._fg_item)

        self._last_angle = None
        self._fg_scale = float(fg_scale)
        self._bg_scale = float(bg_scale)

        self._recenter_and_scale()

    def _recenter_and_scale(self):
        # scale items
        if self._bg_item is not None:
            self._bg_item.setScale(self._bg_scale)

        self._fg_item.setScale(self._fg_scale)

        # compute bounds (union)
        rects = []
        if self._bg_item is not None:
            rects.append(self._bg_item.mapRectToScene(self._bg_item.boundingRect()))
        rects.append(self._fg_item.mapRectToScene(self._fg_item.boundingRect()))

        united = rects[0]
        for r in rects[1:]:
            united = united.united(r)

        self._scene.setSceneRect(united)

        # center background and foreground at (0,0) in scene coordinates by shifting their positions
        # We want both centered on the same point.
        center = united.center()

        if self._bg_item is not None:
            bg_rect = self._bg_item.mapRectToScene(self._bg_item.boundingRect())
            self._bg_item.setPos(self._bg_item.pos() + (center - bg_rect.center()))

        fg_rect = self._fg_item.mapRectToScene(self._fg_item.boundingRect())
        self._fg_item.setPos(self._fg_item.pos() + (center - fg_rect.center()))

        # set rotation origin for foreground (center of its *local* bounds)
        br = self._fg_item.boundingRect()
        self._fg_item.setTransformOriginPoint(br.center())

        # fit view
        self.fitInView(self._scene.sceneRect(), QtCore.Qt.AspectRatioMode.KeepAspectRatio)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.fitInView(self._scene.sceneRect(), QtCore.Qt.AspectRatioMode.KeepAspectRatio)

    def set_heading_deg(self, hdg: float, *, offset_deg: float = 0.0, invert: bool = False):
        if hdg is None:
            return

        try:
            angle = (float(hdg) + float(offset_deg)) % 360.0
        except Exception:
            return

        if invert:
            angle = (-angle) % 360.0

        self._fg_item.setRotation(angle)
        self._last_angle = angle
        self.viewport().update()