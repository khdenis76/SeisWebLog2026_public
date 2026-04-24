import math

from dsr_line_graphics_matplotlib import DSRLineGraphicsMatplotlib
from rov.dsr_map_graphics import DSRMapPlots

project_db_path="G:\\02_PROJECTS\AW1\\16_SWL_DATA\\AW1\\data\\project.sqlite3"
g=DSRLineGraphicsMatplotlib(project_db_path)
save_dir = "G:\\02_PROJECTS\AW1\\16_SWL_DATA\\AW1\\plots\\"
line=12865 #13513
"""
df = g.read_dsr_for_line(line)
# --- BlackBox ---
bbdata, cfg_row, start_time, end_time = g.load_bbdata_for_dsr_line(
    line=line,
    dsr_table="DSR",
    line_col="Line",
    ts_start_col="TimeStamp",
    ts_end_col="TimeStamp",
    pad_minutes=1,   # можно 0, 5, 10 и т.д.
)
# --- 3. Выбираем 6 станций (одна страница 3x2) ---
stations = (
    df.sort_values("Station")["Station"]
    .dropna()
    .unique()
    .tolist()
)

station_values = stations[:6]   # первые 6 нодов

# --- 4. Строим одну страницу ---

g.plot_one_line_map_page(
    df=df,
    bbdata=bbdata,
    cfg_row=cfg_row,
    station_values=station_values,
    output_path=f"{save_dir}node_page_1.png",
    line=line,
    rows=3,
    cols=2,
    page_size="A4",
    orientation="portrait",
    dpi=180,
    page_title=f"Line {line} | Node Maps",
    draw_kwargs=dict(
        zoom_m=12,
        radius_m=5,
        show_preplot=True,
        show_blackbox=True,
        show_vessel=True,
        show_dsr_deployment=True,
        show_station_primary_secondary=True,
        add_station_label=True,
        add_primary_secondary_distance=True,
        add_deploy_vertical=False,

        bb_linewidth=0.7,
        bb_marker_size=6,
        bb_linestyle_primary="--",
        bb_linestyle_secondary=":",
        vessel_linewidth=0.8,
        vessel_linestyle="-",
    ),
)

pages = g.plot_whole_line_node_pages_fast(
    line=line,
    output_dir=save_dir,
    rows=3,
    cols=2,
    page_size="A4",
    orientation="portrait",
    dpi=140,
    pad_minutes=10,
    bb_stride=3,   # 1 = all BB points, 2/3/5 = faster
    draw_kwargs=dict(
        zoom_m=12,
        radius_m=5,
        show_preplot=True,
        show_blackbox=True,
        show_vessel=True,
        show_dsr_deployment=True,
        show_station_primary_secondary=True,
        add_station_label=True,
        add_primary_secondary_distance=True,
        add_deploy_vertical=False,
        bb_linewidth=0.6,
        bb_marker_size=10,
        vessel_linewidth=0.7,
    ),
)

print("DONE:", pages)
"""
fig, path = g.plot_bbox_gnss_qc_for_line(
    line=line,
    pad_minutes=0,
    is_show=False,
    save_dir=save_dir,
)

print(path)