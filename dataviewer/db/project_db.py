import sqlite3
from pathlib import Path
import pandas as pd
import pyqtgraph as pg


class ProjectDbError(Exception):
    pass

class ProjectDb:
    def __init__(self, db_path: str):
        self.db_path = (
                Path(db_path)
                .expanduser()
                .resolve()
                / "data"
                / "project.sqlite3"
        )
    def _connect(self) -> sqlite3.Connection:
        if not self.db_path.exists():
            raise ProjectDbError(f"Project DB not found: {self.db_path}")
        conn = sqlite3.connect(str(self.db_path), timeout=5.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def read_rp_preplot(self) -> pd.DataFrame:
        # Adjust column names if yours differ.
        # Common patterns: Easting/Northing, X/Y, PreplotEasting/PreplotNorthing, etc.
        sql = """
            SELECT
                Line,
                Point,
                LinePoint, 
                X,
                Y
            FROM RPPreplot
            WHERE X IS NOT NULL AND Y IS NOT NULL 
            ORDER BY Line, Point
        """
        with self._connect() as conn:
            # check table exists
            ok = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='RPPreplot' LIMIT 1"
            ).fetchone()
            if not ok:
                raise ProjectDbError("Table 'RPPreplot' not found in project DB.")
            df = pd.read_sql_query(sql, conn)
        return df

    def scatter_from_df(self, scatter, df, x_col="X", y_col="Y", max_points=300_000):
        if df is None or df.empty:
            scatter.setData([], [])
            return

        # keep only valid rows
        d = df[[x_col, y_col]].copy()
        d = d.dropna(subset=[x_col, y_col])

        # to numpy float arrays
        x = d[x_col].astype(float).to_numpy()
        y = d[y_col].astype(float).to_numpy()

        # optional downsample for speed
        if max_points and len(x) > max_points:
            import numpy as np
            idx = np.linspace(0, len(x) - 1, max_points).astype(int)
            x = x[idx]
            y = y[idx]

        scatter.setData(x, y)

    def create_preplot_scatter(self,df,size:int = 5,color="blue")->pg.ScatterPlotItem:
        scatter = pg.ScatterPlotItem(
            size=size,
            pxMode=True,
            brush=pg.mkBrush(color),
            pen=None,
        )
        self.scatter_from_df(scatter,df)
        return scatter

    def read_v_dsr_lines(self):
        view_name = "V_DSR_LineSummary"
        with self._connect() as conn:
            ok = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE (type='view' OR type='table') AND name=? LIMIT 1",
                (view_name,),
            ).fetchone()
            if not ok:
                raise ProjectDbError(f"'{view_name}' not found in project DB.")

            df = pd.read_sql_query(f"SELECT * FROM {view_name}", conn)

        return df

    def read_v_dsr_line_details(self, line: int) -> dict:
        view_name = "V_DSR_LineSummary"
        with self._connect() as conn:
            ok = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE (type='view' OR type='table') AND name=? LIMIT 1",
                (view_name,),
            ).fetchone()
            if not ok:
                raise ProjectDbError(f"'{view_name}' not found in project DB.")

            row = conn.execute(
                f"SELECT * FROM {view_name} WHERE Line = ? LIMIT 1",
                (int(line),),
            ).fetchone()

        if not row:
            return {}

        return dict(row)

    def read_blackbox_files(self) -> pd.DataFrame:
        table_name = "BlackBox_Files"

        with self._connect() as conn:
            ok = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table_name,),
            ).fetchone()
            if not ok:
                raise ProjectDbError(f"Table '{table_name}' not found in project DB.")

            df = pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY ID", conn)

        return df
    def load_bbox_data(
        self,
        # selection
        file_name: str | None = None,
        file_names: list[str] | None = None,
        file_ids: list[int] | None = None,
        config_fk: int | None = None,
        day: str | None = None,
        start_ts: str | None = None,
        end_ts: str | None = None,
        # what to load
        columns: list[str] | None = None,
        use_cache: bool = True,
        force_reload: bool = False,
    ) -> pd.DataFrame:
        """
        Load BlackBox rows once and reuse across many plots.

        Notes:
          - You can pass file_name OR file_names OR file_ids.
          - Returned dataframe includes 'T' datetime if requested as `bb.TimeStamp AS T`.
          - If `columns` is None, a "common package" (GNSS + ROV + config names) is loaded.
        """

        if file_names is None:
            file_names = []
        if file_ids is None:
            file_ids = []
        if file_name:
            file_names = list(file_names) + [file_name]

        if columns is None:
            # common package used by most plots in this file
            columns = [
                "bb.TimeStamp AS T",
                "bb.File_FK AS File_FK",
                "bf.FileName AS FileName",
                "bf.Config_FK AS Config_FK",

                # GNSS
                "bb.GNSS1_NOS", "bb.GNSS1_DiffAge", "bb.GNSS1_FixQuality","bb.GNSS1_HDOP",
                "bb.GNSS2_NOS", "bb.GNSS2_DiffAge", "bb.GNSS2_FixQuality","bb.GNSS2_HDOP",
                "bb.GNSS1_Elevation", "bb.GNSS2_Elevation",

                # ROV depths (both rovs; keep as-is even if null in some configs)
                "bb.ROV1_Depth1", "bb.ROV1_Depth2",
                "bb.ROV2_Depth1", "bb.ROV2_Depth2",
                "bb.ROV1_Depth","bb.ROV1_HDG","bb.ROV1_SOG","bb.ROV1_COG","bb.ROV1_PITCH","bb.ROV1_ROLL",
                "bb.ROV2_Depth", "bb.ROV2_HDG", "bb.ROV2_SOG", "bb.ROV2_COG","bb.ROV2_PITCH","bb.ROV2_ROLL",


                # Vessel parameters
                "bb.VesselHDG","bb.VesselSOG","bb.VesselCOG",


                # names from config
                "cfg.ID AS ConfigID",
                "cfg.rov1_name AS Rov1Name",
                "cfg.rov2_name AS Rov2Name",
                "cfg.gnss1_name AS Gnss1Name",
                "cfg.gnss2_name AS Gnss2Name",
                "cfg.Depth1_name AS Depth1Name",
                "cfg.Depth2_name AS Depth2Name",
                "cfg.Vessel_name AS VesselName",
            ]

        cache_key = self._cache_key(
            file_names=file_names,
            file_ids=file_ids,
            config_fk=config_fk,
            day=day,
            start_ts=start_ts,
            end_ts=end_ts,
            columns=tuple(columns),
        )

        if use_cache and not force_reload and cache_key in self._bbox_cache:
            return self._bbox_cache[cache_key].copy()

        # file_names -> file_ids
        if file_names:
            placeholders = ",".join("?" for _ in file_names)
            q = f"SELECT ID, Config_FK, FileName FROM BlackBox_Files WHERE FileName IN ({placeholders})"
            with self._connect() as conn:
                rows = conn.execute(q, tuple(file_names)).fetchall()

            found = {r["FileName"]: int(r["ID"]) for r in rows} if rows else {}
            missing = [fn for fn in file_names if fn not in found]
            if missing:
                raise ValueError(f"BlackBox_Files: FileName not found: {missing}")

            file_ids = list(set(list(file_ids) + list(found.values())))

        # WHERE
        where = ["bb.TimeStamp IS NOT NULL"]
        params: list = []

        if file_ids:
            placeholders = ",".join("?" for _ in file_ids)
            where.append(f"bb.File_FK IN ({placeholders})")
            params.extend(file_ids)

        if config_fk is not None:
            where.append("bf.Config_FK = ?")
            params.append(int(config_fk))

        if day:
            where.append("bb.TimeStamp >= ? AND bb.TimeStamp < ?")
            params.append(f"{day} 00:00:00")
            params.append(f"{day} 23:59:59")

        if start_ts:
            where.append("bb.TimeStamp >= ?")
            params.append(start_ts)
        if end_ts:
            where.append("bb.TimeStamp <= ?")
            params.append(end_ts)

        sql = f"""
            SELECT
                {", ".join(columns)}
            FROM BlackBox bb
            JOIN BlackBox_Files bf ON bf.ID = bb.File_FK
            LEFT JOIN BBox_Configs_List cfg ON cfg.ID = bf.Config_FK
            WHERE {" AND ".join(where)}
            ORDER BY bb.TimeStamp
        """

        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=tuple(params))

        # normalize timestamps if present
        if "T" in df.columns:
            df["T"] = pd.to_datetime(df["T"], errors="coerce")
            df = df.dropna(subset=["T"])

        if use_cache:
            self._bbox_cache[cache_key] = df.copy()

        return df

    def read_dsr_for_line(self, line: int) -> pd.DataFrame:
        with self._connect() as conn:
            ok = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='DSR' LIMIT 1"
            ).fetchone()
            if not ok:
                raise ProjectDbError("Table 'DSR' not found in project DB.")

            df = pd.read_sql_query(
                "SELECT * FROM DSR WHERE Line = ? ORDER BY LinePoint, TimeStamp",
                conn,
                params=(int(line),),
            )
        return df

    def read_dsr_stations_for_line(self, line: int) -> pd.DataFrame:
        with self._connect() as conn:
            df = pd.read_sql_query(
                """
                SELECT
                    Line,
                    LinePoint,
                    MIN(TimeStamp) AS FirstTime,
                    MAX(TimeStamp) AS LastTime,
                    COUNT(*) AS Nodes
                FROM DSR
                WHERE Line = ?
                GROUP BY Line, LinePoint
                ORDER BY LinePoint
                """,
                conn,
                params=(int(line),),
            )
        return df

    def read_dsr_station_center(self, line: int, linepoint: int) -> dict:
        """
        Returns center coordinates for a station (LinePoint) using Primary coords.
        """
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    AVG(PrimaryEasting)  AS X,
                    AVG(PrimaryNorthing) AS Y,
                    COUNT(*)             AS N
                FROM DSR
                WHERE Line = ?
                  AND LinePoint = ?
                  AND PrimaryEasting IS NOT NULL
                  AND PrimaryNorthing IS NOT NULL
                """,
                (int(line), int(linepoint)),
            ).fetchone()

        if not row or row["X"] is None or row["Y"] is None:
            return {}

        return {"X": float(row["X"]), "Y": float(row["Y"]), "N": int(row["N"])}

    def read_blackbox_for_line_by_date1_window(self, line: int) -> pd.DataFrame:
        """
        Select BlackBox rows where datetime(BlackBox.TimeStamp) is between
        min/max datetime(DSR.Date1) for the selected Line.

        Works when BlackBox.TimeStamp is TEXT (ISO datetime).
        """
        with self._connect() as conn:
            win = conn.execute(
                """
                SELECT
                    MIN(datetime(Date1)) AS TMin,
                    MAX(datetime(Date1)) AS TMax
                FROM DSR
                WHERE Line = ?
                  AND Date1 IS NOT NULL
                """,
                (int(line),),
            ).fetchone()

            if not win or win["TMin"] is None or win["TMax"] is None:
                return pd.DataFrame()

            tmin = win["TMin"]
            tmax = win["TMax"]

            df = pd.read_sql_query(
                """
                SELECT *
                FROM BlackBox
                WHERE datetime(TimeStamp) >= datetime(?)
                  AND datetime(TimeStamp) <= datetime(?)
                ORDER BY datetime(TimeStamp)
                """,
                conn,
                params=(tmin, tmax),
            )

        return df

