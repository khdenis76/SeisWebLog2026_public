# core/projectdb.py
from __future__ import annotations

import math
import io
import os
from dataclasses import dataclass, field
from django.core.files.uploadedfile import UploadedFile
from datetime import date
from pathlib import Path
import sqlite3
from typing import Optional
from .models import SPSRevision
from typing import Literal
DuplicateMode = Literal["add", "keep_first", "keep_last"]
# ======================= DATA CLASSES =======================

@dataclass
class MainSettings:
    """
    Main project-level metadata stored in project_main table.
    """
    name: str = "New project"
    location: str = "N/A"
    area: str = "Gulf of America"
    client: str = "No name"
    contractor: str = "No name"
    project_client_id: str = "PRJ000"
    project_contractor_id: str = "PRJ000"
    epsg: str = "26716"
    line_code: str = "AAAAA"
    # Start date of the project (default: today)
    start_project: str = field(default_factory=lambda: date.today().isoformat())
    # Estimated duration of the project in days
    project_duration: int = 30




@dataclass
class GeometrySettings:
    """
    Geometry parameters of the seismic project.
    """
    rpi: float = 0.0
    rli: float = 0.0
    spi: float = 0.0
    sli: float = 0.0
    rl_heading: float = 360.0
    sl_heading: float = 0.0
    production_code: str = "AP"
    non_production_code: str = "LRMXTK"
    rl_mask: str = "LLLLPPPP"
    sl_mask: str = "LLLLXSSSS"

    @property
    def rec_point_length(self) -> int:
        """Receiver point number length (10^n)."""
        if not self.rl_mask:
            return 0
        num_P = self.rl_mask.count("P")
        return 10 ** (num_P + 1)
    @property
    def rec_line_length(self) -> int:
        """line number length (10^n)."""
        if not self.rl_mask:
            return 0
        num_L = self.rl_mask.count("L")
        return 10 ** (num_L + 1)

    @property
    def sou_line_length(self) -> int:
        """line number length (10^n)."""
        if not self.sl_mask:
            return 0
        num_L = self.rl_mask.count("L")
        return 10 ** (num_L + 1)

    @property
    def rec_linepoint_length(self) -> int:
        """Receiver line-point number length (10^n)."""
        if not self.rl_mask:
            return 0
        num_L = self.rl_mask.count("L")
        return 10 ** (num_L + 1)

    @property
    def sou_point_length(self) -> int:
        """Source point number length (10^n)."""
        if not self.sl_mask:
            return 0
        num_P = self.sl_mask.count("P")
        return 10 ** (num_P + 1)

    @property
    def sou_linepoint_length(self) -> int:
        """Source line-point number length (10^n)."""
        if not self.sl_mask:
            return 0
        num_L = self.sl_mask.count("L")
        return 10 ** (num_L + 1)

    @property
    def sou_attempt_length(self) -> int:
        """Source attempt number length (10^n)."""
        if not self.sl_mask:
            return 0
        num_X = self.sl_mask.count("X")
        return 10 ** num_X
@dataclass
class NodeQCSettings:
    """
    QC tolerances for node-based systems (OBN).
    """
    max_il_offset: float = 0.0
    max_xl_offset: float = 0.0
    max_radial_offset: float = 0.0
    percent_of_depth: float = 0.0
    # 0 = radial, 1 = inline, 2 = crossline
    use_offset: int = 0


@dataclass
class GunQCSettings:
    """
    QC tolerances and configuration for source gun arrays.
    """
    num_of_arrays: int = 3
    num_of_strings: int = 3
    num_of_guns: int = 3
    depth: float = 0.0
    depth_tolerance: float = 5.0
    time_warning: float = 1.0
    time_error: float = 1.5
    pressure: float = 2000.0
    pressure_drop: float = 100.0
    volume: float = 4000.0
    max_il_offset: float = 0.0
    max_xl_offset: float = 0.0
    max_radial_offset: float = 0.0

@dataclass
class PreplotData:
    """
     Class for SPS data import to preplot db
    """
    line_fk: int | None = None
    filе_fk: int | None = None
    line:int =0
    point:int =0
    point_code:str =""
    point_index:int =1
    easting:float=0.0
    northing:float=0.0
    elevation:float=0.0
    line_point:int=0
    tier: int =1
    tier_line:int=0
    line_point_idx:int=0
    tier_line_point:int=0
    tier_line_point_idx:int=0
    line_bearing:float=0
# ======================= PROJECT DB WRAPPER =======================

class ProjectDB:
    """
    Wrapper around project-specific SQLite database.

    Each table has exactly 1 row with id=1.
    """

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    # ======================= INIT DB =======================

    def init_db(self) -> None:
        """
        Create tables if not exist and ensure one default row in each table.
        """
        with self._connect() as conn:
            cur = conn.cursor()

            # -------- project_main --------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS project_main (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    name TEXT NOT NULL,
                    location TEXT NOT NULL,
                    area TEXT NOT NULL,
                    client TEXT NOT NULL,
                    contractor TEXT NOT NULL,
                    project_client_id TEXT NOT NULL,
                    project_contractor_id TEXT NOT NULL,
                    epsg TEXT NOT NULL,
                    line_code TEXT NOT NULL,
                    start_project TEXT NOT NULL,
                    project_duration INTEGER NOT NULL
                );
                """
            )
            cur.execute("SELECT COUNT(*) AS cnt FROM project_main;")
            if cur.fetchone()["cnt"] == 0:
                m = MainSettings()
                cur.execute(
                    """
                    INSERT INTO project_main
                        (id, name, location, area, client, contractor,
                         project_client_id, project_contractor_id,
                         epsg, line_code, start_project, project_duration)
                    VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        m.name, m.location, m.area,
                        m.client, m.contractor,
                        m.project_client_id, m.project_contractor_id,
                        m.epsg, m.line_code,
                        m.start_project, m.project_duration,
                    ),
                )

            # -------- project_geometry --------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS project_geometry (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    rpi REAL NOT NULL,
                    rli REAL NOT NULL,
                    spi REAL NOT NULL,
                    sli REAL NOT NULL,
                    rl_heading REAL NOT NULL,
                    sl_heading REAL NOT NULL,
                    production_code TEXT NOT NULL,
                    non_production_code TEXT NOT NULL,
                    rl_mask TEXT NOT NULL,
                    sl_mask TEXT NOT NULL
                );
                """
            )
            cur.execute("SELECT COUNT(*) AS cnt FROM project_geometry;")
            if cur.fetchone()["cnt"] == 0:
                g = GeometrySettings()
                cur.execute(
                    """
                    INSERT INTO project_geometry
                        (id, rpi, rli, spi, sli,
                         rl_heading, sl_heading,
                         production_code, non_production_code,
                         rl_mask, sl_mask)
                    VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        g.rpi, g.rli, g.spi, g.sli,
                        g.rl_heading, g.sl_heading,
                        g.production_code, g.non_production_code,
                        g.rl_mask, g.sl_mask,
                    ),
                )

            # -------- project_node_qc --------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS project_node_qc (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    max_il_offset REAL NOT NULL,
                    max_xl_offset REAL NOT NULL,
                    max_radial_offset REAL NOT NULL,
                    percent_of_depth REAL NOT NULL,
                    use_offset INTEGER NOT NULL
                );
                """
            )
            cur.execute("SELECT COUNT(*) AS cnt FROM project_node_qc;")
            if cur.fetchone()["cnt"] == 0:
                n = NodeQCSettings()
                cur.execute(
                    """
                    INSERT INTO project_node_qc
                        (id, max_il_offset, max_xl_offset, max_radial_offset,
                         percent_of_depth, use_offset)
                    VALUES (1, ?, ?, ?, ?, ?)
                    """,
                    (
                        n.max_il_offset, n.max_xl_offset, n.max_radial_offset,
                        n.percent_of_depth, n.use_offset,
                    ),
                )

            # -------- project_gun_qc --------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS project_gun_qc (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    num_of_arrays INTEGER NOT NULL,
                    num_of_strings INTEGER NOT NULL,
                    num_of_guns INTEGER NOT NULL,
                    depth REAL NOT NULL,
                    depth_tolerance REAL NOT NULL,
                    time_warning REAL NOT NULL,
                    time_error REAL NOT NULL,
                    pressure REAL NOT NULL,
                    pressure_drop REAL NOT NULL,
                    volume REAL NOT NULL,
                    max_il_offset REAL NOT NULL,
                    max_xl_offset REAL NOT NULL,
                    max_radial_offset REAL NOT NULL
                );
                """
            )
            cur.execute("SELECT COUNT(*) AS cnt FROM project_gun_qc;")
            if cur.fetchone()["cnt"] == 0:
                q = GunQCSettings()
                cur.execute(
                    """
                    INSERT INTO project_gun_qc
                        (id, num_of_arrays, num_of_strings, num_of_guns,
                         depth, depth_tolerance,
                         time_warning, time_error,
                         pressure, pressure_drop, volume,
                         max_il_offset, max_xl_offset, max_radial_offset)
                    VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        q.num_of_arrays, q.num_of_strings, q.num_of_guns,
                        q.depth, q.depth_tolerance,
                        q.time_warning, q.time_error,
                        q.pressure, q.pressure_drop, q.volume,
                        q.max_il_offset, q.max_xl_offset, q.max_radial_offset,
                    ),
                )

            conn.commit()

    # ======================= GETTERS =======================
    def run_sql_file(self, path):
        with open(path, "r", encoding="utf-8") as file:
            sql = file.read()

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.executescript(sql)  # для SQLite
        conn.commit()
        conn.close()
        print("SQL file executed.")
    def get_main(self) -> MainSettings:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM project_main WHERE id = 1;")
            row = cur.fetchone()
            if not row:
                return MainSettings()
            return MainSettings(
                name=row["name"],
                location=row["location"],
                area=row["area"],
                client=row["client"],
                contractor=row["contractor"],
                project_client_id=row["project_client_id"],
                project_contractor_id=row["project_contractor_id"],
                epsg=row["epsg"],
                line_code=row["line_code"],
                start_project=row["start_project"],
                project_duration=row["project_duration"],
            )

    def get_geometry(self) -> GeometrySettings:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM project_geometry WHERE id = 1;")
            row = cur.fetchone()
            if not row:
                return GeometrySettings()
            return GeometrySettings(
                rpi=row["rpi"],
                rli=row["rli"],
                spi=row["spi"],
                sli=row["sli"],
                rl_heading=row["rl_heading"],
                sl_heading=row["sl_heading"],
                production_code=row["production_code"],
                non_production_code=row["non_production_code"],
                rl_mask=row["rl_mask"] or "",
                sl_mask=row["sl_mask"] or "",
            )

    def get_node_qc(self) -> NodeQCSettings:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM project_node_qc WHERE id = 1;")
            row = cur.fetchone()
            if not row:
                return NodeQCSettings()
            return NodeQCSettings(
                max_il_offset=row["max_il_offset"],
                max_xl_offset=row["max_xl_offset"],
                max_radial_offset=row["max_radial_offset"],
                percent_of_depth=row["percent_of_depth"],
                use_offset=row["use_offset"],
            )

    def get_gun_qc(self) -> GunQCSettings:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM project_gun_qc WHERE id = 1;")
            row = cur.fetchone()
            if not row:
                return GunQCSettings()
            return GunQCSettings(
                num_of_arrays=row["num_of_arrays"],
                num_of_strings=row["num_of_strings"],
                num_of_guns=row["num_of_guns"],
                depth=row["depth"],
                depth_tolerance=row["depth_tolerance"],
                time_warning=row["time_warning"],
                time_error=row["time_error"],
                pressure=row["pressure"],
                pressure_drop=row["pressure_drop"],
                volume=row["volume"],
                max_il_offset=row["max_il_offset"],
                max_xl_offset=row["max_xl_offset"],
                max_radial_offset=row["max_radial_offset"],
            )

    # ======================= UPDATERS =======================

    def update_main(self, data: MainSettings) -> None:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE project_main
                SET name = ?, location = ?, area = ?, client = ?, contractor = ?,
                    project_client_id = ?, project_contractor_id = ?,
                    epsg = ?, line_code = ?, start_project = ?, project_duration = ?
                WHERE id = 1;
                """,
                (
                    data.name, data.location, data.area,
                    data.client, data.contractor,
                    data.project_client_id, data.project_contractor_id,
                    data.epsg, data.line_code,
                    data.start_project, data.project_duration,
                ),
            )
            conn.commit()

    def update_geometry(self, data: GeometrySettings) -> None:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE project_geometry
                SET rpi = ?, rli = ?, spi = ?, sli = ?,
                    rl_heading = ?, sl_heading = ?,
                    production_code = ?, non_production_code = ?,
                    rl_mask = ?, sl_mask = ?
                WHERE id = 1;
                """,
                (
                    data.rpi, data.rli, data.spi, data.sli,
                    data.rl_heading, data.sl_heading,
                    data.production_code, data.non_production_code,
                    data.rl_mask, data.sl_mask,
                ),
            )
            conn.commit()

    def update_node_qc(self, data: NodeQCSettings) -> None:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE project_node_qc
                SET max_il_offset = ?, max_xl_offset = ?, max_radial_offset = ?,
                    percent_of_depth = ?, use_offset = ?
                WHERE id = 1;
                """,
                (
                    data.max_il_offset,
                    data.max_xl_offset,
                    data.max_radial_offset,
                    data.percent_of_depth,
                    data.use_offset,
                ),
            )
            conn.commit()

    def update_gun_qc(self, data: GunQCSettings) -> None:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE project_gun_qc
                SET num_of_arrays = ?, num_of_strings = ?, num_of_guns = ?,
                    depth = ?, depth_tolerance = ?,
                    time_warning = ?, time_error = ?,
                    pressure = ?, pressure_drop = ?, volume = ?,
                    max_il_offset = ?, max_xl_offset = ?, max_radial_offset = ?
                WHERE id = 1;
                """,
                (
                    data.num_of_arrays, data.num_of_strings, data.num_of_guns,
                    data.depth, data.depth_tolerance,
                    data.time_warning, data.time_error,
                    data.pressure, data.pressure_drop, data.volume,
                    data.max_il_offset, data.max_xl_offset, data.max_radial_offset,
                ),
            )
            conn.commit()

    def ensure_rlpreplot_lines(self, sps_points: list, file_fk: int | None = None) -> dict[int, int]:
        """
        Гарантирует, что все линии из sps_points существуют в RLPreplot по уникальному TierLine.
        Добавляет отсутствующие (INSERT OR IGNORE).
        Проставляет p.line_fk = RLPreplot.ID.
        Возвращает dict {TierLine: ID}.
        """
        # собрать уникальные TierLine и (Tier, Line)
        conn=self._connect()
        tierline_map: dict[int, tuple[int, int]] = {}
        for p in sps_points:
            tl = getattr(p, "tier_line", None)
            ln = getattr(p, "line", None)
            tr = getattr(p, "tier", None)  # лучше хранить tier в PreplotData
            if tl is None or ln is None:
                continue
            if tr is None:
                tr = 1
            tierline_map.setdefault(int(tl), (int(tr), int(ln)))

        if not tierline_map:
            return {}

        tierlines = sorted(tierline_map.keys())

        conn.execute("BEGIN IMMEDIATE")
        try:
            if file_fk is None:
                conn.executemany(
                    "INSERT OR IGNORE INTO RLPreplot (TierLine, Line, Tier) VALUES (?, ?, ?)",
                    [(tl, tierline_map[tl][1], tierline_map[tl][0]) for tl in tierlines],
                )
            else:
                conn.executemany(
                    "INSERT OR IGNORE INTO RLPreplot (TierLine, Line, Tier, File_FK) VALUES (?, ?, ?, ?)",
                    [(tl, tierline_map[tl][1], tierline_map[tl][0], file_fk) for tl in tierlines],
                )

            placeholders = ",".join("?" for _ in tierlines)
            cur = conn.execute(
                f"SELECT ID, TierLine FROM RLPreplot WHERE TierLine IN ({placeholders})",
                tierlines,
            )
            tl2id = {tierline: _id for _id, tierline in cur.fetchall()}

            # проставить fk в объектах
            for p in sps_points:
                tl = getattr(p, "tier_line", None)
                if tl is not None:
                    p.line_fk = tl2id.get(int(tl))

            conn.commit()
            return tl2id
        except Exception:
            conn.rollback()
            raise

    def recalc_rlpreplot_from_rppreplot(self) -> None:
        """
        Recalculate per-line stats in RLPreplot based on RPPreplot points.
        Assumes:
          - RPPreplot.Line_FK points to RLPreplot.ID
          - RLPreplot has columns: Points, FirstPoint, LastPoint, StartX, StartY, EndX, EndY, LineLength, LineBearing
            (names can be adapted to your schema)
        """

        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            # 1) получить список линий и базовые агрегаты
            cur = conn.execute("""
                       SELECT
                           Line_FK,
                           MIN(Point)  AS FirstPoint,
                           MAX(Point)  AS LastPoint,
                           COUNT(*)    AS Points
                       FROM RPPreplot
                       WHERE Line_FK IS NOT NULL
                       GROUP BY Line_FK
                   """)
            lines = cur.fetchall()

            # 2) для каждой линии найти координаты первой и последней точки
            for row in lines:
                line_fk = row["Line_FK"]
                first_point = row["FirstPoint"]
                last_point = row["LastPoint"]
                points_cnt = row["Points"]

                # первая точка (минимальный Point, затем минимальный PointIndex)
                cur1 = conn.execute("""
                           SELECT X, Y
                           FROM RPPreplot
                           WHERE Line_FK = ?
                           ORDER BY Point ASC, PointIndex ASC
                           LIMIT 1
                       """, (line_fk,))
                r1 = cur1.fetchone()
                if not r1:
                    continue

                # последняя точка (максимальный Point, затем максимальный PointIndex)
                cur2 = conn.execute("""
                           SELECT X, Y
                           FROM RPPreplot
                           WHERE Line_FK = ?
                           ORDER BY Point DESC, PointIndex DESC
                           LIMIT 1
                       """, (line_fk,))
                r2 = cur2.fetchone()
                if not r2:
                    continue

                sx, sy = float(r1["X"] or 0.0), float(r1["Y"] or 0.0)
                ex, ey = float(r2["X"] or 0.0), float(r2["Y"] or 0.0)

                # длина (простейшая: расстояние Start-End)
                line_len = math.hypot(ex - sx, ey - sy)

                # bearing (0..360, где 0 = North). Если не нужен — можно убрать.
                # dx = Easting, dy = Northing
                dx = ex - sx
                dy = ey - sy
                bearing = (math.degrees(math.atan2(dx, dy)) + 360.0) % 360.0

                # 3) обновить RLPreplot
                conn.execute("""
                           UPDATE RLPreplot
                           SET
                               Points     = ?,
                               FirstPoint = ?,
                               LastPoint  = ?,
                               StartX     = ?,
                               StartY     = ?,
                               EndX       = ?,
                               EndY       = ?,
                               LineLength = ?,
                               LineBearing= ?
                           WHERE ID = ?
                       """, (
                    points_cnt,
                    first_point,
                    last_point,
                    sx, sy, ex, ey,
                    line_len,
                    bearing,
                    line_fk
                ))

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def insert_rppreplot_bulk_old(self, sps_points: list, file_fk: int, dup_mode: DuplicateMode = "add"):
        if not sps_points:
            return

        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            if dup_mode == "add":
                # ✅ add all duplicates: PointIndex = MAX+1 per (Tier,Line,Point)
                sql = """
                INSERT INTO RPPreplot (
                    Tier, Line, Point, PointIndex,
                    Line_FK, File_FK,
                    X, Y, Z, PointCode,
                    LinePoint, LinePointIndex,
                    TierLine, TLinePoint, TLinePointIndex,
                    LineBearing
                )
                VALUES (
                    ?, ?, ?,
                    COALESCE(
                        (SELECT MAX(PointIndex) FROM RPPreplot WHERE Tier=? AND Line=? AND Point=?),
                        0
                    ) + 1,
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?
                );
                """
                for p in sps_points:
                    conn.execute(sql, (
                        p.tier, p.line, p.point,
                        p.tier, p.line, p.point,  # for MAX(PointIndex)
                        p.line_fk, file_fk,
                        p.easting, p.northing, p.elevation, (p.point_code or ""),
                        p.line_point, p.line_point_idx,
                        p.tier_line, p.tier_line_point, p.tier_line_point_idx,
                        p.line_bearing
                    ))

            elif dup_mode == "keep_first":
                # ✅ keep first: insert only if (Tier,Line,Point,PointIndex=1) not exists
                # Для этого фиксируем PointIndex=1 и используем INSERT OR IGNORE по ux_rppreplot
                sql = """
                INSERT OR IGNORE INTO RPPreplot (
                    Tier, Line, Point, PointIndex,
                    Line_FK, File_FK,
                    X, Y, Z, PointCode,
                    LinePoint, LinePointIndex,
                    TierLine, TLinePoint, TLinePointIndex,
                    LineBearing
                )
                VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """
                for p in sps_points:
                    conn.execute(sql, (
                        p.tier, p.line, p.point,
                        p.line_fk, file_fk,
                        p.easting, p.northing, p.elevation, (p.point_code or ""),
                        p.line_point, p.line_point_idx,
                        p.tier_line, p.tier_line_point, p.tier_line_point_idx,
                        p.line_bearing
                    ))

            elif dup_mode == "keep_last":
                # ✅ keep last: всегда перезаписать "первую" запись (PointIndex=1)
                # Делаем UPSERT по уникальному индексу ux_rppreplot (Tier,Line,Point,PointIndex)
                sql = """
                INSERT INTO RPPreplot (
                    Tier, Line, Point, PointIndex,
                    Line_FK, File_FK,
                    X, Y, Z, PointCode,
                    LinePoint, LinePointIndex,
                    TierLine, TLinePoint, TLinePointIndex,
                    LineBearing
                )
                VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(Tier, Line, Point, PointIndex) DO UPDATE SET
                    Line_FK=excluded.Line_FK,
                    File_FK=excluded.File_FK,
                    X=excluded.X, Y=excluded.Y, Z=excluded.Z,
                    PointCode=excluded.PointCode,
                    LinePoint=excluded.LinePoint,
                    LinePointIndex=excluded.LinePointIndex,
                    TierLine=excluded.TierLine,
                    TLinePoint=excluded.TLinePoint,
                    TLinePointIndex=excluded.TLinePointIndex,
                    LineBearing=excluded.LineBearing;
                """
                for p in sps_points:
                    conn.execute(sql, (
                        p.tier, p.line, p.point,
                        p.line_fk, file_fk,
                        p.easting, p.northing, p.elevation, (p.point_code or ""),
                        p.line_point, p.line_point_idx,
                        p.tier_line, p.tier_line_point, p.tier_line_point_idx,
                        p.line_bearing
                    ))

            else:
                raise ValueError(f"Unknown dup_mode: {dup_mode}")

            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def get_or_create_file_id(self, file_name: str) -> int:
        """
        Возвращает ID файла из таблицы Files.
        Если файла нет — добавляет его.
        """
        conn=self._connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            # 1) пробуем найти
            cur = conn.execute(
                "SELECT ID FROM Files WHERE FileName = ?",
                (file_name,)
            )
            row = cur.fetchone()
            if row:
                file_id = row[0]
                conn.commit()
                return file_id

            # 2) если нет — вставляем
            cur = conn.execute(
                "INSERT INTO Files (FileName) VALUES (?)",
                (file_name,)
            )
            file_id = cur.lastrowid

            conn.commit()
            return file_id

        except Exception:
            conn.rollback()
            raise
    #=========================== HELPERS ===================

    @staticmethod
    def _detect_text_encoding(sample: bytes) -> str:
        """
            SPS-optimized encoding detection.

                    SPS files are typically ASCII / UTF-8 compatible. We keep detection fast:
                      1) BOM check (utf-8-sig / utf-16)
                      2) strict UTF-8 decode attempt
                      3) fallback to single-byte encoding (cp1252 or cp1251) based on heuristic
                      4) final fallback latin1 (never fails)

                    Returns: encoding name for bytes.decode()
        """

        if not sample:
            return "utf-8"

        # 1) BOM checks (fast and reliable)
        if sample.startswith(b"\xef\xbb\xbf"):
            return "utf-8-sig"
        if sample.startswith(b"\xff\xfe") or sample.startswith(b"\xfe\xff"):
            # Rare for SPS, but handle if it happens
            return "utf-16"

        # 2) Strict UTF-8 test (no errors allowed)
        try:
            sample.decode("utf-8", errors="strict")
            return "utf-8"
        except UnicodeDecodeError:
            pass

        # 3) Heuristic: choose cp1251 if it "looks like" Cyrillic text
        # We decode with cp1251 and count Cyrillic letters.
        try:
            s1251 = sample.decode("cp1251", errors="ignore")
            cyr = sum(1 for ch in s1251 if "\u0400" <= ch <= "\u04FF")  # Cyrillic range
            letters = sum(1 for ch in s1251 if ch.isalpha())

            # If there are some letters and a noticeable share is Cyrillic,
            # it's probably cp1251.
            if letters >= 20 and (cyr / max(letters, 1)) >= 0.2:
                return "cp1251"
        except Exception:
            pass

        # 4) Default single-byte fallback:
        # cp1252 is a good "western" default; latin1 always works too.
        return "cp1252"

    @staticmethod
    def _to_int(s: str, default: Optional[int] = None) -> Optional[int]:
        s = (s or "").strip()
        if not s:
            return default
        try:
            return int(float(s))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _to_float(s: str, default: Optional[float] = None) -> Optional[float]:
        s = (s or "").strip()
        if not s:
            return default
        try:
            return float(s.replace(",", "."))
        except (ValueError, TypeError):
            return default
    @staticmethod
    def decode_sps_string(s:str,
                          sps_revision:SPSRevision,
                          geom:GeometrySettings,
                          default:int|None,
                          tier:int=1,
                          line_bearing:float=0,
                          point_type:str="R")->PreplotData:
        if point_type == "R":
            point_len = geom.rec_point_length
            line_len = geom.rec_line_length
            line_point_len =geom.rec_linepoint_length
        else:
            point_len = geom.sou_point_length
            line_len = geom.sou_line_length
            line_point_len = geom.sou_linepoint_length

        line  = ProjectDB._to_int(s[sps_revision.line_start:sps_revision.line_end],default=default)
        point = ProjectDB._to_int(s[sps_revision.point_start:sps_revision.point_end],default=default)
        easting = ProjectDB._to_float(s[sps_revision.easting_start:sps_revision.easting_end],default=default)
        northing = ProjectDB._to_float(s[sps_revision.northing_start:sps_revision.northing_end],default=default)
        elevation = ProjectDB._to_float(s[sps_revision.elevation_start:sps_revision.elevation_end],default=default)
        point_code =s[sps_revision.point_code_start:sps_revision.point_code_end] or ""
        point_index= ProjectDB._to_int(s[sps_revision.point_idx_start:sps_revision.point_idx_end],default=default)
        if not point_index:
            point_index = 1
        line_point = line*point_len+point
        line_point_idx = line_point*10+point_index
        tier_line=tier*line_len+line
        tier_line_point = tier*line_point_len+line_point
        tier_line_point_idx = tier*(10**(len(str(line_point_idx)))*10)+line_point_idx


        return PreplotData(
            line=line,
            point=point,
            easting=easting,
            northing=northing,
            elevation=elevation,
            point_code=point_code,
            point_index=point_index,
            line_point=line_point,
            line_point_idx=line_point_idx,
            tier_line=tier_line,
            tier_line_point=tier_line_point,
            tier_line_point_idx=tier_line_point_idx,
            line_bearing=line_bearing

        )

    def load_sps_file(
            self,
            file_path: str,
            sps_revision: SPSRevision,
            default: int | None,
            line_bearing:float=0,
            tier: int = 1,
            point_type: str = "R",
    ) -> list[PreplotData]:
        file_name = os.path.basename(file_path)

        # 1) определить кодировку
        with open(file_path, "rb") as sps_file:
            encoding = ProjectDB._detect_text_encoding(sps_file.read(4096))

        # 2) geometry один раз (а не в цикле)
        geom = self.get_geometry()

        # 3) парсинг
        sps_points: list[PreplotData] = []
        with open(file_path, "r", encoding=encoding, errors="replace") as sps_file:
            for text_line in sps_file:
                if not text_line:
                    continue
                if text_line[0] == "H":
                    continue

                sps_point = ProjectDB.decode_sps_string(
                    text_line,
                    sps_revision=sps_revision,
                    geom=geom,
                    default=default,
                    tier=tier,
                    point_type=point_type,
                    line_bearing=line_bearing
                )
                if sps_point is not None:
                    sps_points.append(sps_point)

        # 4) сортировка для стабильного порядка индексов
        sps_points.sort(key=lambda p: (p.line, p.point))

        # 5) Files -> file_id
        file_id = self.get_or_create_file_id(file_name)

        # 6) RLPreplot + проставить line_fk в объектах
        self.ensure_rlpreplot_lines(sps_points, file_fk=file_id)

        # 7) RPPreplot (с File_FK)
        self.insert_rppreplot_bulk(sps_points, file_fk=file_id)


        return sps_points



    def load_sps_uploaded_file(
            self,
            uploaded_file: UploadedFile,
            sps_revision: SPSRevision,
            default: int | None,
            line_bearing: float = 0,
            tier: int = 1,
            point_type: str = "R",dup_mode: DuplicateMode = "add"
    ) -> list[PreplotData]:
        """
        Load SPS directly from Django UploadedFile (request.FILES),
        without saving to disk.
        """

        file_name = uploaded_file.name

        # --------------------------------------------------
        # 1) detect encoding (read small chunk)
        # --------------------------------------------------
        uploaded_file.seek(0)
        sample = uploaded_file.read(4096)
        encoding = ProjectDB._detect_text_encoding(sample)

        # IMPORTANT: reset cursor after read()
        uploaded_file.seek(0)

        # --------------------------------------------------
        # 2) geometry once
        # --------------------------------------------------
        geom = self.get_geometry()

        # --------------------------------------------------
        # 3) parse SPS lines
        # --------------------------------------------------
        sps_points: list[PreplotData] = []

        for raw_line in uploaded_file:
            # raw_line is bytes
            try:
                text_line = raw_line.decode(encoding, errors="replace")
            except Exception:
                continue

            if not text_line:
                continue
            if text_line[0] == "H":
                continue

            p = ProjectDB.decode_sps_string(
                text_line,
                sps_revision=sps_revision,
                geom=geom,
                default=default,
                tier=tier,
                point_type=point_type,
                line_bearing=line_bearing,
            )
            if p is not None:
                sps_points.append(p)

        # --------------------------------------------------
        # 4) stable order
        # --------------------------------------------------
        sps_points.sort(key=lambda p: (p.line, p.point))

        # --------------------------------------------------
        # 5) Files → file_id
        # --------------------------------------------------
        file_id = self.get_or_create_file_id(file_name)

        # --------------------------------------------------
        # 6) RLPreplot (ensure lines + line_fk)
        # --------------------------------------------------
        self.ensure_rlpreplot_lines(sps_points, file_fk=file_id)

        # --------------------------------------------------
        # 7) RPPreplot bulk insert
        # --------------------------------------------------
        self.insert_rppreplot_bulk(sps_points, file_fk=file_id,dup_mode= dup_mode)
        self. recalc_rlpreplot_from_rppreplot()

        return sps_points

    def select_rlpreplot(self) -> list[dict]:
        sql = """
        SELECT
            Tier,
            Line,
            TierLine,
            Points,
            FirstPoint,
            LastPoint,
            LineLength,
            LineBearing
        FROM RLPreplot
        ORDER BY Tier ASC, TierLine ASC
        """
        with self._connect() as conn:
            rows = conn.execute(sql).fetchall()
            return [dict(r) for r in rows]

    def _begin_fast_import(self, conn: sqlite3.Connection) -> None:
        # Максимально ускоряем массовую вставку в SQLite
        conn.execute("PRAGMA synchronous = OFF;")
        conn.execute("PRAGMA journal_mode = MEMORY;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        conn.execute("PRAGMA cache_size = -200000;")  # ~200MB cache (можно меньше)
        conn.execute("PRAGMA foreign_keys = OFF;")
        conn.execute("BEGIN IMMEDIATE;")

    def _end_fast_import(self, conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA foreign_keys = ON;")

    def insert_rppreplot_bulk(self, sps_points: list[PreplotData], file_fk: int, dup_mode: DuplicateMode = "add"):
        if not sps_points:
            return

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")

            if dup_mode == "add":
                # 1) Собираем уникальные ключи (Tier,Line,Point) из batch
                keys = {(p.tier, p.line, p.point) for p in sps_points}

                # 2) Одним запросом получаем текущие MAX(PointIndex) по этим ключам
                # SQLite лимит на количество параметров, поэтому режем чанками
                base_max: dict[tuple[int, int, int], int] = {}
                keys_list = list(keys)
                CHUNK = 300  # 300 * 3 params = 900, безопасно

                for i in range(0, len(keys_list), CHUNK):
                    chunk = keys_list[i:i + CHUNK]
                    placeholders = ",".join(["(?, ?, ?)"] * len(chunk))
                    sql_max = f"""
                        SELECT Tier, Line, Point, MAX(PointIndex) AS mx
                        FROM RPPreplot
                        WHERE (Tier, Line, Point) IN ({placeholders})
                        GROUP BY Tier, Line, Point
                    """
                    params = []
                    for t, l, p in chunk:
                        params.extend([t, l, p])

                    for row in conn.execute(sql_max, params).fetchall():
                        base_max[(row[0], row[1], row[2])] = int(row[3] or 0)

                # 3) Счётчики “сколько дублей добавили в этой пачке”
                added_in_batch: dict[tuple[int, int, int], int] = {}

                # 4) Готовим строки для executemany (PointIndex считаем в Python)
                rows = []
                for p in sps_points:
                    k = (p.tier, p.line, p.point)
                    start = base_max.get(k, 0)
                    inc = added_in_batch.get(k, 0) + 1
                    added_in_batch[k] = inc
                    point_index = start + inc

                    rows.append((
                        p.tier, p.line, p.point, point_index,
                        p.line_fk, file_fk,
                        p.easting, p.northing, p.elevation, (p.point_code or ""),
                        p.line_point, p.line_point_idx,
                        p.tier_line, p.tier_line_point, p.tier_line_point_idx,
                        p.line_bearing
                    ))

                sql_ins = """
                    INSERT INTO RPPreplot (
                        Tier, Line, Point, PointIndex,
                        Line_FK, File_FK,
                        X, Y, Z, PointCode,
                        LinePoint, LinePointIndex,
                        TierLine, TLinePoint, TLinePointIndex,
                        LineBearing
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                conn.executemany(sql_ins, rows)

            elif dup_mode == "keep_first":
                sql = """
                    INSERT OR IGNORE INTO RPPreplot (
                        Tier, Line, Point, PointIndex,
                        Line_FK, File_FK,
                        X, Y, Z, PointCode,
                        LinePoint, LinePointIndex,
                        TierLine, TLinePoint, TLinePointIndex,
                        LineBearing
                    )
                    VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                rows = [(
                    p.tier, p.line, p.point,
                    p.line_fk, file_fk,
                    p.easting, p.northing, p.elevation, (p.point_code or ""),
                    p.line_point, p.line_point_idx,
                    p.tier_line, p.tier_line_point, p.tier_line_point_idx,
                    p.line_bearing
                ) for p in sps_points]
                conn.executemany(sql, rows)

            elif dup_mode == "keep_last":
                sql = """
                    INSERT INTO RPPreplot (
                        Tier, Line, Point, PointIndex,
                        Line_FK, File_FK,
                        X, Y, Z, PointCode,
                        LinePoint, LinePointIndex,
                        TierLine, TLinePoint, TLinePointIndex,
                        LineBearing
                    )
                    VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(Tier, Line, Point, PointIndex) DO UPDATE SET
                        Line_FK=excluded.Line_FK,
                        File_FK=excluded.File_FK,
                        X=excluded.X, Y=excluded.Y, Z=excluded.Z,
                        PointCode=excluded.PointCode,
                        LinePoint=excluded.LinePoint,
                        LinePointIndex=excluded.LinePointIndex,
                        TierLine=excluded.TierLine,
                        TLinePoint=excluded.TLinePoint,
                        TLinePointIndex=excluded.TLinePointIndex,
                        LineBearing=excluded.LineBearing
                """
                rows = [(
                    p.tier, p.line, p.point,
                    p.line_fk, file_fk,
                    p.easting, p.northing, p.elevation, (p.point_code or ""),
                    p.line_point, p.line_point_idx,
                    p.tier_line, p.tier_line_point, p.tier_line_point_idx,
                    p.line_bearing
                ) for p in sps_points]
                conn.executemany(sql, rows)

            else:
                raise ValueError(f"Unknown dup_mode: {dup_mode}")

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()



    def load_sps_uploaded_file_fast(
            self,
            uploaded_file: UploadedFile,
            sps_revision: SPSRevision,
            default: int | None,
            line_bearing: float = 0,
            tier: int = 1,
            point_type: str = "R",
            dup_mode: DuplicateMode = "add",
            batch_size: int = 20000,
    ) -> dict:
        file_name = uploaded_file.name

        # detect encoding
        uploaded_file.seek(0)
        sample = uploaded_file.read(4096)
        encoding = ProjectDB._detect_text_encoding(sample)
        uploaded_file.seek(0)

        geom = self.get_geometry()

        # 1) file id
        file_id = self.get_or_create_file_id(file_name)

        # 2) streaming text reader (вместо raw_line.decode в цикле)
        stream = io.TextIOWrapper(uploaded_file.file, encoding=encoding, errors="replace", newline="")

        conn = self._connect()
        try:
            self._begin_fast_import(conn)

            # line aggregates for RLPreplot (TierLine -> stats)
            # считаем Start/End как min/max по (Point,PointIndex)
            stats: dict[int, dict] = {}

            # batch of PreplotData
            batch: list[PreplotData] = []
            total = 0
            skipped = 0

            for text_line in stream:
                if not text_line:
                    continue
                if text_line[0] == "H":
                    continue

                p = ProjectDB.decode_sps_string(
                    text_line,
                    sps_revision=sps_revision,
                    geom=geom,
                    default=default,
                    tier=tier,
                    point_type=point_type,
                    line_bearing=line_bearing,
                )
                if p is None:
                    skipped += 1
                    continue

                # aggregates per TierLine
                tl = int(p.tier_line)
                st = stats.get(tl)
                # ключ сравнения "первой/последней" точки
                key = (p.point, p.point_index)

                if st is None:
                    stats[tl] = {
                        "tier": int(p.tier),
                        "line": int(p.line),
                        "count": 1,
                        "first_key": key,
                        "last_key": key,
                        "sx": float(p.easting), "sy": float(p.northing),
                        "ex": float(p.easting), "ey": float(p.northing),
                    }
                else:
                    st["count"] += 1
                    if key < st["first_key"]:
                        st["first_key"] = key
                        st["sx"], st["sy"] = float(p.easting), float(p.northing)
                    if key > st["last_key"]:
                        st["last_key"] = key
                        st["ex"], st["ey"] = float(p.easting), float(p.northing)

                batch.append(p)
                total += 1

                if len(batch) >= batch_size:
                    # 1) ensure RL lines for this batch + set p.line_fk
                    self._ensure_rlpreplot_lines_on_conn(conn, batch, file_fk=file_id)
                    # 2) insert RP batch using same conn
                    self._insert_rppreplot_bulk_on_conn(conn, batch, file_fk=file_id, dup_mode=dup_mode)
                    batch.clear()

            if batch:
                self._ensure_rlpreplot_lines_on_conn(conn, batch, file_fk=file_id)
                self._insert_rppreplot_bulk_on_conn(conn, batch, file_fk=file_id, dup_mode=dup_mode)
                batch.clear()

            # bulk update RLPreplot from stats (один проход)
            self._update_rlpreplot_from_stats_on_conn(conn, stats, file_fk=file_id)

            conn.commit()
            self._end_fast_import(conn)

            return {"file_id": file_id, "points": total, "skipped": skipped, "lines": len(stats)}

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_rlpreplot_lines_on_conn(self, conn: sqlite3.Connection, sps_points: list[PreplotData],
                                        file_fk: int | None = None) -> dict[int, int]:
        tierline_map: dict[int, tuple[int, int]] = {}
        for p in sps_points:
            tl = int(p.tier_line)
            tierline_map.setdefault(tl, (int(p.tier), int(p.line)))

        if not tierline_map:
            return {}

        tierlines = sorted(tierline_map.keys())

        if file_fk is None:
            conn.executemany(
                "INSERT OR IGNORE INTO RLPreplot (TierLine, Line, Tier) VALUES (?, ?, ?)",
                [(tl, tierline_map[tl][1], tierline_map[tl][0]) for tl in tierlines],
            )
        else:
            conn.executemany(
                "INSERT OR IGNORE INTO RLPreplot (TierLine, Line, Tier, File_FK) VALUES (?, ?, ?, ?)",
                [(tl, tierline_map[tl][1], tierline_map[tl][0], file_fk) for tl in tierlines],
            )

        placeholders = ",".join("?" for _ in tierlines)
        cur = conn.execute(
            f"SELECT ID, TierLine FROM RLPreplot WHERE TierLine IN ({placeholders})",
            tierlines,
        )
        tl2id = {tierline: _id for _id, tierline in cur.fetchall()}

        for p in sps_points:
            p.line_fk = tl2id.get(int(p.tier_line))

        return tl2id

    def _insert_rppreplot_bulk_on_conn(self, conn: sqlite3.Connection, sps_points: list[PreplotData], file_fk: int,
                                       dup_mode: DuplicateMode):
        # Вставь сюда код из переписанного insert_rppreplot_bulk,
        # только убери conn = self._connect()/commit/rollback/close.
        # (Т.е. пусть работает на переданном conn)
        ...

    def _update_rlpreplot_from_stats_on_conn(self, conn: sqlite3.Connection, stats: dict[int, dict], file_fk: int):
        if not stats:
            return

        rows = []
        for tierline, st in stats.items():
            sx, sy, ex, ey = st["sx"], st["sy"], st["ex"], st["ey"]
            line_len = math.hypot(ex - sx, ey - sy)
            dx = ex - sx
            dy = ey - sy
            bearing = (math.degrees(math.atan2(dx, dy)) + 360.0) % 360.0

            first_point = st["first_key"][0]
            last_point = st["last_key"][0]
            points_cnt = st["count"]

            rows.append((
                points_cnt, first_point, last_point,
                sx, sy, ex, ey,
                line_len, bearing,
                tierline, file_fk
            ))

        conn.executemany("""
            UPDATE RLPreplot
            SET
                Points=?,
                FirstPoint=?,
                LastPoint=?,
                StartX=?,
                StartY=?,
                EndX=?,
                EndY=?,
                LineLength=?,
                LineBearing=?
            WHERE TierLine=? AND File_FK=?
        """, rows)


