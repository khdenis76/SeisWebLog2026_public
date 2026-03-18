from PySide6.QtCore import Qt, QTimer, QPropertyAnimation
from PySide6.QtGui import QPixmap, QGuiApplication
from PySide6.QtWidgets import QLabel, QGraphicsOpacityEffect


class FadeSplash(QLabel):
    def __init__(self, image_path: str, duration_ms: int = 4000, fade_ms: int = 1200, on_finish=None):
        super().__init__()
        self.duration_ms = duration_ms
        self.fade_ms = fade_ms
        self.on_finish = on_finish

        self.setWindowFlags(
            Qt.FramelessWindowHint |
            Qt.WindowStaysOnTopHint |
            Qt.Tool
        )
        self.setAttribute(Qt.WA_TranslucentBackground)

        pix = QPixmap(image_path)
        self.setPixmap(pix)
        self.resize(pix.size())

        # center
        screen = QGuiApplication.primaryScreen().geometry()
        self.move(
            (screen.width() - self.width()) // 2,
            (screen.height() - self.height()) // 2
        )

        # opacity
        self.effect = QGraphicsOpacityEffect(self)
        self.setGraphicsEffect(self.effect)
        self.effect.setOpacity(0)

        self.fade_in = QPropertyAnimation(self.effect, b"opacity")
        self.fade_in.setDuration(self.fade_ms)
        self.fade_in.setStartValue(0)
        self.fade_in.setEndValue(1)

        self.fade_out = QPropertyAnimation(self.effect, b"opacity")
        self.fade_out.setDuration(self.fade_ms)
        self.fade_out.setStartValue(1)
        self.fade_out.setEndValue(0)
        self.fade_out.finished.connect(self.finish)

    def showEvent(self, event):
        super().showEvent(event)

        self.fade_in.start()
        QTimer.singleShot(self.duration_ms - self.fade_ms, self.fade_out.start)

    def finish(self):
        self.close()
        if self.on_finish:
            self.on_finish()