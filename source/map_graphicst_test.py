from bokeh.io import show
from bokeh.layouts import gridplot
from source.source_map_graph import SourceMapGraphics

#from core.projectdb import ProjectDB

project_db_path="D:\\313311_AW1-APEX\\16_SWL_DATA\\AW1\\data\\project.sqlite3"
sm=SourceMapGraphics(project_db_path)
default_epsg =32615
sm.build_daybyday_source_production(production_code ="AWP",
            non_production_code="LRMT",
            kill_code="KX",
            is_show=True,
            json_return = False,
            title = "Day-by-Day Source Production",
            include_other = False,  # set True if you want to see leftover codes
    )
"""
sm.build_source_sunburst(is_show = True,
                         json_return = False,
                         title = "Source Production",
                         drop_zeros = True,
)"""
#sm.build_source_progress_map(is_show=True,default_epsg=default_epsg,use_tiles=True)