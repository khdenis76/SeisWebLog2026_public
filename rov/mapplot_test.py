from bokeh.io import show
from bokeh.layouts import gridplot

from dsr_line_graphics import DSRLineGraphics
from bbox_graphics import BlackBoxGraphics
from dsr_map_graphics import DSRMapPlots
#from core.projectdb import ProjectDB

project_db_path="D:\\04_TEST_DATA\\AW1\\data\\project.sqlite3"

dsr_plot = DSRMapPlots(project_db_path,default_epsg=32615,use_tiles=True)
rp_data = dsr_plot.read_rp_preplot()
dsr_data = dsr_plot.read_dsr()
rec_db_data = dsr_plot.read_recdb()
#dsr_plot.make_map(rp_df=rp_data,dsr_df=dsr_data, is_show=True)
layers = [
    dict(
        name="Deployed by ROV",
        df='dsr',
        x_col="PrimaryEasting",
        y_col="PrimaryNorthing",
        marker="circle",
        size=6,
        alpha=0.9,
        color_col="ROV",                       # categorical color mapping
        where="ROV.notna() and ROV != ''",     # filter: ROV not empty
    ),
dict(
        name="Recovered by ROV",
        df='dsr',
        x_col="PrimaryEasting1",
        y_col="PrimaryNorthing1",
        marker="circle",
        size=6,
        alpha=0.9,
        color_col="ROV1",                       # categorical color mapping
        where="ROV1.notna() and ROV1 != ''",     # filter: ROV not empty
    ),
dict(
        name="Deployment",
        df='dsr',
        x_col="PrimaryEasting",
        y_col="PrimaryNorthing",
        marker="circle",
        size=6,
        alpha=0.9,
        color='blue',
        #color_col="ROV",                       # categorical color mapping
        where="ROV.notna() and ROV1 != ''",     # filter: ROV not empty
    ),
dict(
        name="Recovered Nodes",
        df='dsr',
        x_col="PrimaryEasting1",
        y_col="PrimaryNorthing1",
        marker="circle",
        size=6,
        alpha=0.9,
        color='orange',
        #color_col="ROV",                       # categorical color mapping
        where="ROV1.notna() and ROV1 != ''",     # filter: ROV not empty
    ),
dict(
        name="Processed Nodes",
        df='rec',
        x_col="REC_X",
        y_col="REC_Y",
        marker="circle",
        size=6,
        alpha=0.9,
        color='red',
        #color_col="ROV",                       # categorical color mapping
        where=None,     # filter: ROV not empty
    ),
]

layout = dsr_plot.make_map_multi_layers(
    rp_df=rp_data,              # your RPPreplot dataframe
    dsr_df=dsr_data,            # your DSR dataframe
    rec_db_df=rec_db_data,
    title="PROJECT PROGRESS MAP",
    layers=layers,
    show_preplot=True,
    show_shapes=True,
    show_tiles=True,          # if using mercator tiles
    is_show=True
)