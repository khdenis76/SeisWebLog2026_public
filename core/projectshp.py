from dataclasses import dataclass
import os
from pathlib import Path


@dataclass
class ProjectShape:
    id:int=0
    full_name:str=""
    is_filled:int=0
    fill_color:str="#000000"
    line_color:str="#000000"
    line_width:int=0
    line_style:str="solid"
    hatch_pattern:str=""

    @property
    def file_name(self)->str:
        fname=os.path.basename(self.full_name)
        return fname
    @property
    def file_check(self)->int:
        file_check = 1 if self.full_name and Path(self.full_name).exists() else 0
        return file_check




