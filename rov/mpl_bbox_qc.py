from dsr_line_graphics_matplotlib import DSRLineGraphicsMatplotlib
import dsr_line_graphics_matplotlib
import os

print("MODULE FILE:", dsr_line_graphics_matplotlib.__file__)

project_db_path = r"G:\02_PROJECTS\AW1\16_SWL_DATA\AW1\data\project.sqlite3"
save_dir = r"G:\02_PROJECTS\AW1\16_SWL_DATA\AW1\plots"
line = 13513

g = DSRLineGraphicsMatplotlib(project_db_path)

pages = g.plot_bbox_motion_qc_for_line_paged_combined(
    line=line,
    hours_per_page=24,
    pad_minutes=0,
    save_dir=save_dir,
    is_show=False,
    close=True,
    bb_stride=20,
)

print("PAGES COUNT:", len(pages))
for p in pages:
    print("PAGE:", p)
"""    
pages = g.plot_bbox_gnss_qc_for_line_paged(
    line=line,
    hours_per_page=48,
    pad_minutes=0,
    save_dir=save_dir,
    is_show=False,
    close=True,
)

print("PAGES COUNT:", len(pages))
for p in pages:
    print("PAGE:", p, "EXISTS:", os.path.exists(p))
"""