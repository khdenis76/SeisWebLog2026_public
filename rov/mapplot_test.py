from bokeh.io import show
from bokeh.layouts import gridplot

from dsr_line_graphics import DSRLineGraphics
from bbox_graphics import BlackBoxGraphics
from dsr_map_graphics import DSRMapPlots
#from core.projectdb import ProjectDB

project_db_path="D:\\313311_AW1-APEX\\16_SWL_DATA\\AW1\\data\\project.sqlite3"

dsr_plot = DSRMapPlots(project_db_path,default_epsg=32615,use_tiles=True)
rp_data = dsr_plot.read_rp_preplot()
dsr_data = dsr_plot.read_dsr()
dsr_line_data =dsr_plot.read_line_summary()


dsr_plot.build_line_summary_qc_grid(df=dsr_line_data,json_export=False,is_show=True,ncols=3)

"""
dsr_data = dsr_plot.add_inline_xline_offsets(
    dsr_data,rp_data,
    from_xy=("PreplotEasting", "PreplotNorthing"),
    to_xy=("PrimaryEasting", "PrimaryNorthing"),
    out_prefix="Pri"
)
dsr_data = dsr_plot.add_inline_xline_offsets(
    dsr_data,rp_data,
    from_xy=("PreplotEasting", "PreplotNorthing"),
    to_xy=("SecondaryEasting", "SecondaryNorthing"),
    out_prefix="Pri"
)
dsr_plot.build_offsets_histograms_by_rov(
            dsr_data,
            rov_col="ROV",
            inline_col="PriOffInline",
            xline_col="PriOffXline",
            radial_col="RangetoPrePlot",
            bins=40,
            show_mean_line=True,
            title_prefix="Offsets",
            is_show=True,
            json_import=False,
            target_id="dsr_offsets_hist",
            max_offset=25,
    )

dsr_plot.sunburst_prod_3layers_plotly(
            metric="RECStations",
            title="Recovery",
            labels="Rec",
            is_show=True,
            json_return=False,
    )
"""


#dsr_plot.layer_donut_deploy_and_recovery_plotly(is_show=True)


#dsr_plot.layer_donut_deploy_recovery_plotly(is_show=True, json_return=False)
#dsr_plot.donut_rov_summary_plotly(metric="Stations", is_show=True, json_return=False)
#dsr_plot.day_by_day_recovery(is_show =True)

"""
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
"""