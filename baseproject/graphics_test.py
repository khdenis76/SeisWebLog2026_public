from pathlib import Path

from bokeh.io import show
from bokeh.io import output_file, show
from  preplot_graphics import PreplotGraphics
pg = PreplotGraphics(r"D:\04_TEST_DATA\AW1\data\project.sqlite3")  # <-- put your real db here

print("DB path:", pg.db_path)
print("Exists:", pg.db_path.exists())
print("Size bytes:", pg.db_path.stat().st_size if pg.db_path.exists() else None)

p = pg.preplot_map(
    rl_table="RLPreplot",
    sl_table="SLPreplot",
    src_epsg=32615,   # ← change if needed
)
pg.add_scale_bar(p,length_m=10000)
#output_file("preplot_map.html")
show(p)
