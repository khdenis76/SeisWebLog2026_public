from dataclasses import dataclass
import os

@dataclass
class ProjectShape:
    id:int=0
    full_name:str=""
    is_filled:int=0
    fill_color:str="#000000"
    line_color:str="#000000"
    line_width:int=0
    line_style:str="solid"
    @property
    def file_name(self)->str:
        fname=os.path.basename(self.full_name)
        return fname



