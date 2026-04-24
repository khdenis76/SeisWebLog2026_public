APP_NAME = "SeisWebLog DataViewer"
APP_VERSION = "2.0.0"
DEFAULT_WINDOW_WIDTH = 1600
DEFAULT_WINDOW_HEIGHT = 980

MAP_SETTINGS = {
    "lock_aspect": True,
    "show_grid": True,
    "clip_to_view": True,
    "downsampling_mode": "peak",
}

PLOT_QUALITY_PRESETS = {
    "Fast": {
        "rp_scatter_max_points": 20000,
        "bb_scatter_max_points": 20000,
        "dsr_scatter_max_points": 30000,
        "interactive_max_points": 8000,
    },
    "Balanced": {
        "rp_scatter_max_points": 80000,
        "bb_scatter_max_points": 60000,
        "dsr_scatter_max_points": 80000,
        "interactive_max_points": 20000,
    },
    "Full": {
        "rp_scatter_max_points": 150000,
        "bb_scatter_max_points": 120000,
        "dsr_scatter_max_points": 120000,
        "interactive_max_points": 40000,
    },
}
