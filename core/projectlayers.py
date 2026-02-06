from dataclasses import dataclass
import os
from pathlib import Path

@dataclass
class ProjectLayer:
    layer_id:int=None
    fill_color: str = "#000000"
    point_style: str = "circle"
    point_size: int = 0
