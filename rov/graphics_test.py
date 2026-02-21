from bokeh.io import show
from bokeh.layouts import gridplot

from dsr_line_graphics import DSRLineGraphics
from bbox_graphics import BlackBoxGraphics
from dsr_map_graphics import DSRMapPlots

project_db_path="D:\\04_TEST_DATA\\AW1\\data\\project.sqlite3"

dsr_plot = DSRMapPlots(project_db_path)
rp_data = dsr_plot.read_rp_preplot()
dsr_data = dsr_plot.read_dsr()
dsr_plot.make_map(rp_df=rp_data,dsr_data=dsr_data, is_show=True)

#plgr=DSRLineGraphics(project_db_path)
#bbr=BlackBoxGraphics(project_db_path)
#rows=plgr.get_sigmas_deltas(14233)
#plgr.plot_dep_deltas(14233,rows,True)
#plgr.bokeh_scatter_rov_depth1_vs_depth2_qc(is_show=True)
#plgr.bokeh_compare_sensors_rov1(rov_num =1,start_ts="2026-02-05 00:00:00", end_ts="2026-02-07 00:00:00" ,is_show=True,plot="hist")


#data = bbr.load_bbox_data(file_ids=[16])
#dsr_df = bbr.dsr_points_in_bbox_timeframe(data)


#bbr.boke_cog_hdg_timeseries_all(df=data)
#bbr.bokeh_bbox_sog_timeseries(df=data,plot_kind='line',is_show=True)
#p = bbr.bokeh_drift_timeseries(df=data,is_show=False,cog_col="ROV1_COG",hdg_col="ROV1_HDG", vn_col="Rov1Name", return_json=False)
#p = bbr.add_dsr_vertical_lines(p, dsr_df)
#show(p)

        #bbr.bokeh_bbox_sog_timeseries(df=data,plot_kind="scater",is_show=True)
#bbr.bokeh_polar_qc_cog(df =data,is_show=True)
#bbr.bokeh_cog_hdg_drift_rose_qc(df =data,hdg_col='VesselHDG',cog_col='VesselCOG',is_show=True,sog_col='VesselSOG',)
#bbr.bokeh_cog_hdg_drift_rose_qc(df =data,hdg_col='ROV1_HDG',cog_col='ROV1_COG',is_show=True,sog_col='ROV1_SOG',)

#bbr.bokeh_bbox_gnss_hdop_timeseries(df=data,is_show=True)
#bbr.bokeh_bbox_depth12_diff_timeseries(df=data,diff_threshold=10,plot_kind="vbar",is_show=True)
"""
bbr.bokeh_compare_sensors_rov1(rov_num =1, start_ts="2026-02-05 00:00:00", end_ts="2026-02-07 00:00:00" ,is_show=True,plot="bland")

#plgr.bokeh_compare_sensors_rov1(rov_num =2,start_ts="2026-02-05 00:00:00", end_ts="2026-02-07 00:00:00" ,is_show=True,plot="hist")
bbr.bokeh_compare_sensors_rov1(rov_num =2, start_ts="2026-02-05 00:00:00", end_ts="2026-02-07 00:00:00" ,is_show=True,plot="bland")

selected_cfg = bbr.get_bbox_config_names_by_filename('IP_BBLOG-2026-02-08-1200.csv')
bbr.bokeh_gnss_qc_timeseries(file_name='IP_BBLOG-2026-02-08-1200.csv',
                             is_show=True,
                             gnss1_label=selected_cfg['gnss1_name'],
                             gnss2_label=selected_cfg['gnss2_name'],
                             )
"""