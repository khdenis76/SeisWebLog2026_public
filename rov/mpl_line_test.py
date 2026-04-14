import math

from dsr_line_graphics_matplotlib import DSRLineGraphicsMatplotlib
from rov.dsr_map_graphics import DSRMapPlots

project_db_path="G:\\02_PROJECTS\AW1\\16_SWL_DATA\\AW1\\data\\project.sqlite3"
g=DSRLineGraphicsMatplotlib(project_db_path)
line=13513
save_dir = "G:\\02_PROJECTS\AW1\\16_SWL_DATA\\AW1\\plots\\"
"""
df = g.read_dsr_for_line(line)
df['Primary_e95']= df['Sigma1']*math.sqrt(5.991)
df['Primary_n95']= df['Sigma2']*math.sqrt(5.991)
df['Primary_z95']= df['Sigma5']*math.sqrt(5.991)
df['Secondary_e95']= df['Sigma3']*math.sqrt(5.991)
df['Secondary_n95']= df['Sigma4']*math.sqrt(5.991)

df['dX_primary'] = df['PreplotEasting']-df['PrimaryEasting']
df['dX_secondary'] = df['PreplotEasting']-df['SecondaryEasting']
df['dX_primary1'] = df['PreplotEasting']-df['PrimaryEasting1']
df['dY_primary1'] = df['PreplotNorthing']-df['PrimaryNorthing1']


df['dY_primary'] = df['PreplotNorthing']-df['PrimaryNorthing']
df['dY_secondary'] = df['PreplotNorthing']-df['SecondaryNorthing']


save_dir = "G:\\02_PROJECTS\AW1\\16_SWL_DATA\\AW1\\plots\\"
dsr_plot = DSRMapPlots(project_db_path,default_epsg=32615,use_tiles=True)

rp_data = dsr_plot.read_rp_preplot()

fig, path = g.plot_project_map(
    rp_df=rp_data,
    dsr_df=df,
    selected_line=line,
    show_station_labels=True,
    save_dir=save_dir,
    suffix="project_map_selected",
    is_show=False,
    source_epsg=32615,
    target_epsg=32615,
    show_shapes=True,
    show_layers=True,
    zebra_frame=True,
    zebra_x_step=50000,
    zebra_y_step=50000,
)
print(path)
fig, path = g.two_series_vs_station_with_diff_bar(
    df=df,
    series1_col="PrimaryElevation",
    series2_col="SecondaryElevation",
    series1_label="PRIMARY DEPTH",
    series2_label="SECONDARY DEPTH",
    is_show=False,
    reverse_y_if_negative=False,
    title=f"Line {line} Water",
    y_label="Depth",
    diff_y_label="Primary - Secondary",
    save_dir=save_dir,
    rov_col="ROV",
    suffix="water_diff"
)
print(path)
fig, path = g.three_vbar_by_category_shared_x(
    df=df,
    rov_col="ROV",
    is_show=False,
    reverse_y_if_negative=False,
    y1_col="DeltaEprimarytosecondary",
    y2_col="DeltaNprimarytosecondary",
    y3_col="Rangeprimarytosecondary",
    title1="Δ Easting Primary(INS) to Secondary(USBL)",
    title2="Δ Northing Primary(INS) to Secondary(USBL)",
    title3="Radial Offset Primary(INS) to Secondary(USBL)",
    y1_label="ΔE",
    y2_label="ΔN",
    y3_label="Rad. Offset",
    y_axis_label="Offset, m",
    save_dir=save_dir,
    suffix="primsec"
)
print(path)
fig, path = g.dxdy_primary_secondary_with_hists(
    df=df,
    dx_p_col="dX_primary",
    dy_p_col="dY_primary",
    dx_s_col="dX_secondary",
    dy_s_col="dY_secondary",
    title="DSR dX/dY (Primary & Secondary)",
    red_radius=20,
    red_is_show=True,
    p_name="Primary",
    s_name="Secondary",
    bins=40,
    padding_ratio=0.10,
    is_show=False,
    save_dir=save_dir,
    suffix="delta"
)
print(path)
fig, path = g.deployment_offsets_vs_preplot(
    df=df,
    line=line,
    line_bearing=0,
    is_show=False,
    save_dir=save_dir,
    suffix="deplpre"
)
print(path)
"""
pages = g.plot_bbox_gnss_qc_for_line_paged(
    line=line,
    hours_per_page=3,
    pad_minutes=0,
    save_dir=save_dir,
    is_show=False,
    close=True,
)

for p in pages:
    print(p)
    print("pages count =", len(pages))