from __future__ import annotations

import traceback
from collections.abc import Callable
from typing import Any

from PySide6 import QtCore


class WorkerSignals(QtCore.QObject):
    completed = QtCore.Signal(object)
    failed = QtCore.Signal(str)


class FunctionWorker(QtCore.QRunnable):
    def __init__(self, function: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    @QtCore.Slot()
    def run(self) -> None:
        try:
            result = self.function(*self.args, **self.kwargs)
        except Exception:
            self.signals.failed.emit(traceback.format_exc())
            return
        self.signals.completed.emit(result)
