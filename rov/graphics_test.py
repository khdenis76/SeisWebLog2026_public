from dsr_line_graphics import DSRLineGraphics
from bbox_graphics import BlackBoxGraphics
project_db_path="D:\\04_TEST_DATA\\AW1\\data\\project.sqlite3"
plgr=DSRLineGraphics(project_db_path)
bbr=BlackBoxGraphics(project_db_path)
#rows=plgr.get_sigmas_deltas(14233)
#plgr.plot_dep_deltas(14233,rows,True)
#plgr.bokeh_scatter_rov_depth1_vs_depth2_qc(is_show=True)
#plgr.bokeh_compare_sensors_rov1(rov_num =1,start_ts="2026-02-05 00:00:00", end_ts="2026-02-07 00:00:00" ,is_show=True,plot="hist")
#plgr.bokeh_compare_sensors_rov1(rov_num =1, start_ts="2026-02-05 00:00:00", end_ts="2026-02-07 00:00:00" ,is_show=True,plot="bland")

#plgr.bokeh_compare_sensors_rov1(rov_num =2,start_ts="2026-02-05 00:00:00", end_ts="2026-02-07 00:00:00" ,is_show=True,plot="hist")
#plgr.bokeh_compare_sensors_rov1(rov_num =2, start_ts="2026-02-05 00:00:00", end_ts="2026-02-07 00:00:00" ,is_show=True,plot="bland")
selected_cfg = bbr.get_bbox_config_names_by_filename('IP_BBLOG-2026-02-08-1200.csv')
bbr.bokeh_gnss_qc_timeseries(file_name='IP_BBLOG-2026-02-08-1200.csv',
                             is_show=True,
                             gnss1_label=selected_cfg['gnss1_name'],
                             gnss2_label=selected_cfg['gnss2_name'],
                             )