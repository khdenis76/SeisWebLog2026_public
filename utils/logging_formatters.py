import logging


class SafeExtraFormatter(logging.Formatter):
    DEFAULTS = {
        "request_id": "-",
        "username": "-",
        "project_name": "-",
        "method": "-",
        "path": "-",
        "status_code": "-",
        "duration_ms": "-",
    }

    def format(self, record):
        for key, value in self.DEFAULTS.items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return super().format(record)