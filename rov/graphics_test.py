from bokeh.io import show
from bokeh.layouts import gridplot
from dsr_line_graphics import DSRLineGraphics
from bbox_graphics import BlackBoxGraphics




project_db_path="D:\\313311_AW1-APEX\\16_SWL_DATA\\AW1\\data\\project.sqlite3"
plgr=DSRLineGraphics(project_db_path)
line_df = plgr.read_dsr_for_line(13513)
plgr.bokeh_three_vbar_by_category_shared_x(df=line_df,
                                           rov_col="ROV",
                                           is_show=True,
                                           json_return=False,
                                           reverse_y_if_negative=False,
                                           y1_col="DeltaEprimarytosecondary",
                                           y2_col="DeltaNprimarytosecondary",
                                           y3_col="Rangeprimarytosecondary")

"""
plgr.bokeh_one_series_vbar_vs_station_by_category(df=line_df,
                                              y_col="DeltaEprimarytosecondary",
                                              rov_col="ROV",
                                              is_show=True,
                                              json_return=False,
                                              reverse_y_if_negative=False
                                              )

plgr.bokeh_two_series_vs_station(df=line_df,
                                 series1_col="PrimaryElevation",
                                 series2_col="SecondaryElevation",
                                 series1_label="PRIMARY DEPTH",
                                 series2_label="SECONDARY DEPTH",
                                 rov_col="ROV",
                                 is_show=True,
                                 json_item=False,
                                 reverse_y_if_negative=False)
"""
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