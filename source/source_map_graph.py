from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bokeh.embed import json_item
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.plotting import figure
import sqlite3
from bokeh.models.tiles import WMTSTileSource


@dataclass
class SourceMapGraphics:
    """
    Source progress map:
      - SLPreplot segments: planned geometry
      - SLSolution segments: actual/processed geometry (with vessel/sailline/seq for hover)

    Assumptions (adjust SQL if your column names differ):
      SLPreplot:  Line, StartX, StartY, EndX, EndY
      SLSolution: Line, SailLine, Seq, VesselName, StartX, StartY, EndX, EndY, ProdShots
    """

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn


    def build_source_progress_map(
            self,
            production_only: bool = True,
            use_tiles: bool = False,
            is_show: bool = False,
            json_return: bool = False,
    ):
        """
        Plots:
          - SLPreplot: planned (StartX/Y -> EndX/Y)
          - SLSolution: actual  (StartX/Y -> EndX/Y) + hover (VesselName, SailLine, Seq)

        Returns:
          - json_item dict if json_return=True
          - else Bokeh Figure
        """

        sql_preplot = """
            SELECT
                COALESCE(Line, 0)      AS Line,
                COALESCE(StartX, 0.0)  AS x0,
                COALESCE(StartY, 0.0)  AS y0,
                COALESCE(EndX, 0.0)    AS x1,
                COALESCE(EndY, 0.0)    AS y1
            FROM SLPreplot
            WHERE COALESCE(StartX,0) != 0 AND COALESCE(StartY,0) != 0
              AND COALESCE(EndX,0)   != 0 AND COALESCE(EndY,0)   != 0
        """

        sql_solution = """
            SELECT
                COALESCE(Line, 0)            AS Line,
                COALESCE(SailLine, '')       AS SailLine,
                COALESCE(Seq, 0)             AS Seq,
                COALESCE(VesselName, '')     AS VesselName,
                COALESCE(StartX, 0.0)        AS x0,
                COALESCE(StartY, 0.0)        AS y0,
                COALESCE(EndX, 0.0)          AS x1,
                COALESCE(EndY, 0.0)          AS y1,
                COALESCE(ProdShots, 0)       AS ProdShots
            FROM SLSolution
            WHERE COALESCE(StartX,0) != 0 AND COALESCE(StartY,0) != 0
              AND COALESCE(EndX,0)   != 0 AND COALESCE(EndY,0)   != 0
        """
        if production_only:
            sql_solution += " AND COALESCE(ProdShots,0) > 0 "

        with self._connect() as conn:
            preplot_rows = conn.execute(sql_preplot).fetchall()
            sol_rows = conn.execute(sql_solution).fetchall()

        def _cds(rows, keys):
            cols = {k: [] for k in keys}
            for r in rows:
                for i, k in enumerate(keys):
                    cols[k].append(r[i])
            return ColumnDataSource(cols)

        src_preplot = _cds(preplot_rows, ["Line", "x0", "y0", "x1", "y1"])
        src_sol = _cds(sol_rows, ["Line", "SailLine", "Seq", "VesselName", "x0", "y0", "x1", "y1", "ProdShots"])

        p = figure(
            title="Source Progress Map (SLPreplot vs SLSolution)",
            x_axis_label="X",
            y_axis_label="Y",
            tools="pan,wheel_zoom,reset,save",
            active_scroll="wheel_zoom",
            sizing_mode="stretch_both",
            match_aspect=True,
        )

        # ✅ Tiles only make sense if your coordinates are WebMercator (EPSG:3857).
        # If your StartX/StartY are UTM, set use_tiles=False.
        if use_tiles:
            tile_provider = WMTSTileSource(
                url="https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"
            )
            p.add_tile(tile_provider)

        # Planned (SLPreplot)
        p.segment(
            x0="x0", y0="y0", x1="x1", y1="y1",
            source=src_preplot,
            line_width=2,
            line_alpha=0.30,
            line_dash="dashed",
            legend_label="SLPreplot (planned)",
        )

        # Actual (SLSolution) + hover
        r_sol = p.segment(
            x0="x0", y0="y0", x1="x1", y1="y1",
            source=src_sol,
            line_width=3,
            line_alpha=0.95,
            legend_label="SLSolution (actual)",
        )

        p.add_tools(HoverTool(
            renderers=[r_sol],
            tooltips=[
                ("Line", "@Line"),
                ("SailLine", "@SailLine"),
                ("Seq", "@Seq"),
                ("Vessel", "@VesselName"),
                ("ProdShots", "@ProdShots"),
                ("Start", "(@x0, @y0)"),
                ("End", "(@x1, @y1)"),
            ],
        ))

        p.legend.click_policy = "hide"

        if is_show:
            from bokeh.io import show
            show(p)

        if json_return:
            return json_item(p, target="source-progress-map")

        return p