import json
from pathlib import Path


DEFAULT_APP_CONFIG = {
    "general": {
        "dark_mode": False,
        "autoload_last_project": False,
    },
    "rp": {
        "circle_radius": 25.0,
        "point_size": 6,
    },
    "dsr": {
        "depth": True,
        "sigmas": True,
        "radial": True,
        "primary_point_size": 15,
        "secondary_point_size": 6,
    },
    "bb": {
        "show_tracks_window": True,
        "show_timeseries_window": True,
        "bb_vessel": True,
        "bb_rov1_ins": True,
        "bb_rov2_ins": True,
        "bb_rov1_usbl": True,
        "bb_rov2_usbl": True,
        "ts_hdg": True,
        "ts_sog": True,
        "ts_cog": True,
        "ts_nos": True,
        "ts_diffage": True,
        "ts_fixquality": True,
        "ts_hdop": True,
        "ts_depth": True,
    },
}


class ConfigStore:
    def __init__(self, app_name: str = "dataviewer"):
        self.app_name = app_name
        self.config_dir = Path.home() / f".{app_name}"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.config_file = self.config_dir / "settings.json"

    def _deep_merge(self, base: dict, override: dict) -> dict:
        out = dict(base)
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(out.get(k), dict):
                out[k] = self._deep_merge(out[k], v)
            else:
                out[k] = v
        return out

    def load(self) -> dict:
        if not self.config_file.exists():
            return json.loads(json.dumps(DEFAULT_APP_CONFIG))

        try:
            data = json.loads(self.config_file.read_text(encoding="utf-8"))
            return self._deep_merge(
                json.loads(json.dumps(DEFAULT_APP_CONFIG)),
                data if isinstance(data, dict) else {},
            )
        except Exception:
            return json.loads(json.dumps(DEFAULT_APP_CONFIG))

    def save(self, config: dict):
        merged = self._deep_merge(
            json.loads(json.dumps(DEFAULT_APP_CONFIG)),
            config if isinstance(config, dict) else {},
        )
        self.config_file.write_text(
            json.dumps(merged, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )