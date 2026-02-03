# core/projectdb.py
from __future__ import annotations

import csv
import math
import io
import os
from dataclasses import dataclass, field

import fiona
import pandas as pd
from django.core.files.uploadedfile import UploadedFile
from datetime import date, time, datetime
from pathlib import Path
import sqlite3
from typing import Optional

from shapely import Point, LineString
from shapely.geometry import mapping

from .models import SPSRevision
from typing import Literal
from .projectshp import ProjectShape
from typing import Sequence
from .project_dataclasses import  *
DuplicateMode = Literal["add", "keep_first", "keep_last"]
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
            cur.execute(
                """
                  CREATE TABLE IF NOT EXISTS project_folders (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    shapes_folder TEXT,
                    image_folder TEXT,
                    local_prj_folder TEXT,
                    bb_folder TEXT,                
                    segy_folder TEXT);
                """)
            cur.execute("SELECT COUNT(*) AS cnt FROM project_folders;")
            if cur.fetchone()["cnt"] == 0:
                f = FolderSettings()
                cur.execute(
                    """
                    INSERT INTO project_folders
                        (id, shapes_folder,image_folder,local_prj_folder,bb_folder,segy_folder)
                    VALUES (1, ?, ?, ?, ?, ?)
                    """,
                    (f.shapes_folder,f.image_folder,f.local_prj_folder,f.bb_folder,f.segy_folder),
                )
            sql="""
                   CREATE TABLE IF NOT EXISTS "project_shapes" (
                                "id" INTEGER, 
                                "FullName" TEXT UNIQUE NOT NULL,
                                "FileName" TEXT,
                                "isFilled" INTEGER DEFAULT 0,
                                "FillColor" TEXT DEFAULT '#000000',
                                "LineColor" TEXT DEFAULT '#000000',
                                "LineWidth" INTEGER DEFAULT 1,
                                "LineStyle" TEXT DEFAULT '',
	                            PRIMARY KEY(id,FullName));
            """
            cur.execute(sql)
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
    def get_folders(self)->FolderSettings:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM project_folders WHERE id = 1;")
            row = cur.fetchone()
            if not row:
                return GunQCSettings()
            return FolderSettings(
                shapes_folder=row["shapes_folder"],
                image_folder=row["image_folder"],
                local_prj_folder=row["local_prj_folder"],
                bb_folder=row["bb_folder"],
                segy_folder=row["segy_folder"],

            )
    def get_shapes(self) -> list[ProjectShape]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM project_shapes;")

            return [
                ProjectShape(
                    id=row["id"],
                    full_name=row["FullName"],
                    is_filled=row["IsFilled"],
                    fill_color=row["FillColor"],
                    line_color=row["LineColor"],
                    line_width=row["LineWidth"],
                    line_style=row["LineStyle"],
                )
                for row in cur.fetchall()
            ]
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
    def update_folders(self, data: FolderSettings) -> None:
        with self._connect() as conn:
            cur = conn.cursor()
            sql = f"""UPDATE project_folders SET 
                             shapes_folder = '{data.shapes_folder}', 
                             image_folder = '{data.image_folder}', 
                             local_prj_folder = '{data.local_prj_folder}',  
                             bb_folder = '{data.bb_folder}', 
                             segy_folder = '{data.segy_folder}' 
                        WHERE id = 1;"""
            cur.execute(sql)

            conn.commit()
    def upsert_shape(self, shape: ProjectShape) -> None:
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute(
                """
                INSERT INTO project_shapes (
                    id,
                    FullName,
                    FileName,
                    isFilled,
                    FillColor,
                    LineColor,
                    LineWidth,
                    LineStyle
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(FullName) DO UPDATE SET
                    isFilled   = excluded.isFilled,
                    FillColor = excluded.FillColor,
                    LineColor = excluded.LineColor,
                    LineWidth = excluded.LineWidth,
                    LineStyle = excluded.LineStyle,
                    FileName  = excluded.FileName
                ;
                """,
                (
                    shape.id,
                    shape.full_name,
                    shape.file_name,
                    shape.is_filled,
                    shape.fill_color,
                    shape.line_color,
                    shape.line_width,
                    shape.line_style,
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
                self.farthest_points(point_type='R',line_fk=line_fk)
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

    def select_rlpreplot(self,point_code:str="R") -> list[dict]:
        if point_code =="R":
           line_table_name:str ="RLPreplot"
        elif point_code == "S":
           line_table_name: str = "SLPreplot"

        sql = f"""
        SELECT
            ID,
            Tier,
            Line,
            TierLine,
            Points,
            FirstPoint,
            LastPoint,
            LineLength,
            LineBearing
        FROM {line_table_name}
        ORDER BY Tier ASC, TierLine ASC
        """
        with self._connect() as conn:
            rows = conn.execute(sql).fetchall()
            return [dict(r) for r in rows]

    def _begin_fast_import(self, conn: sqlite3.Connection) -> None:
        # Safer fast-import pragmas for a DB used by a web app (Django)
        # journal_mode should be set ONCE (WAL) when DB is created/opened, not here.

        # Wait a bit instead of failing immediately when another connection is reading
        conn.execute("PRAGMA busy_timeout = 30000;")  # 30s

        # Keep integrity reasonably safe while still fast
        conn.execute("PRAGMA synchronous = NORMAL;")  # better than OFF for web app
        conn.execute("PRAGMA temp_store = MEMORY;")
        conn.execute("PRAGMA cache_size = -200000;")  # ~200MB cache

        # Foreign keys OFF speeds bulk loads; re-enable after + (optionally) check
        conn.execute("PRAGMA foreign_keys = OFF;")

        # Acquire write lock for the import
        conn.execute("BEGIN IMMEDIATE;")

    def _end_fast_import(self, conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA foreign_keys = ON;")

    def insert_rppreplot_bulk(self, sps_points: list[PreplotData], file_fk: int, dup_mode: DuplicateMode = "add",point_code:str="R"):
        if not sps_points:
            return
        if point_code =='R':
           line_table_name="RLPreplot"
           point_table_name="RPPreplot"
        elif point_code =='S':
            line_table_name = "SLPreplot"
            point_table_name = "SPPreplot"
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
                        FROM {point_table_name}
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

                sql_ins = f"""
                    INSERT INTO {point_table_name} (
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
                sql = f"""
                    INSERT OR IGNORE INTO {point_table_name} (
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
                sql = f"""
                    INSERT INTO {point_table_name} (
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
                    self._ensure_rlpreplot_lines_on_conn(conn, batch, file_fk=file_id,point_code=point_type)

                    # 2) insert RP batch using same conn
                    self._insert_rppreplot_bulk_on_conn(
                        conn,
                        batch,
                        file_fk=file_id,
                        dup_mode=dup_mode,
                        point_code=point_type,  # <-- ВАЖНО
                    )

                    batch.clear()

            if batch:
                self._ensure_rlpreplot_lines_on_conn(conn, batch, file_fk=file_id,point_code=point_type)


                self._insert_rppreplot_bulk_on_conn(
                    conn,
                    batch,
                    file_fk=file_id,
                    dup_mode=dup_mode,
                    point_code=point_type,  # <-- ВАЖНО
                )

                batch.clear()

            # bulk update RLPreplot from stats (один проход)
            self._update_rlpreplot_from_stats_on_conn(conn, stats, file_fk=file_id,point_code=point_type)

            conn.commit()
            self._end_fast_import(conn)

            return {"file_id": file_id, "points": total, "skipped": skipped, "lines": len(stats)}

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_rlpreplot_lines_on_conn(self, conn: sqlite3.Connection, sps_points: list[PreplotData],
                                        file_fk: int | None = None,point_code:str ='R') -> dict[int, int]:
        tierline_map: dict[int, tuple[int, int]] = {}
        if point_code =='R':
           line_table_name="RLPreplot"
           point_table_name="RPPreplot"
        elif point_code =='S':
            line_table_name = "SLPreplot"
            point_table_name = "SPPreplot"
        for p in sps_points:
            tl = int(p.tier_line)
            tierline_map.setdefault(tl, (int(p.tier), int(p.line)))

        if not tierline_map:
            return {}

        tierlines = sorted(tierline_map.keys())

        if file_fk is None:
            conn.executemany(
                f"INSERT OR IGNORE INTO {line_table_name} (TierLine, Line, Tier) VALUES (?, ?, ?)",
                [(tl, tierline_map[tl][1], tierline_map[tl][0]) for tl in tierlines],
            )
        else:
            conn.executemany(
                f"INSERT OR IGNORE INTO {line_table_name} (TierLine, Line, Tier, File_FK) VALUES (?, ?, ?, ?)",
                [(tl, tierline_map[tl][1], tierline_map[tl][0], file_fk) for tl in tierlines],
            )

        placeholders = ",".join("?" for _ in tierlines)
        cur = conn.execute(
            f"SELECT ID, TierLine FROM {line_table_name} WHERE TierLine IN ({placeholders})",
            tierlines,
        )
        tl2id = {tierline: _id for _id, tierline in cur.fetchall()}

        for p in sps_points:
            p.line_fk = tl2id.get(int(p.tier_line))

        return tl2id

    def _insert_rppreplot_bulk_on_conn(
            self,
            conn: sqlite3.Connection,
            sps_points: list[PreplotData],
            file_fk: int,
            dup_mode: DuplicateMode,
            point_code: str = "R",
    ):
        if not sps_points:
            return

        if point_code == "R":
            point_table_name = "RPPreplot"
        elif point_code == "S":
            point_table_name = "SPPreplot"
        else:
            raise ValueError(f"Unknown point_code: {point_code}")

        if dup_mode == "add":
            keys = {(p.tier, p.line, p.point) for p in sps_points}
            base_max: dict[tuple[int, int, int], int] = {}
            keys_list = list(keys)
            CHUNK = 300  # 900 params safe

            for i in range(0, len(keys_list), CHUNK):
                chunk = keys_list[i:i + CHUNK]
                placeholders = ",".join(["(?, ?, ?)"] * len(chunk))
                sql_max = f"""
                    SELECT Tier, Line, Point, MAX(PointIndex) AS mx
                    FROM {point_table_name}
                    WHERE (Tier, Line, Point) IN ({placeholders})
                    GROUP BY Tier, Line, Point
                """
                params = []
                for t, l, p in chunk:
                    params.extend([t, l, p])

                for row in conn.execute(sql_max, params).fetchall():
                    base_max[(row[0], row[1], row[2])] = int(row[3] or 0)

            added_in_batch: dict[tuple[int, int, int], int] = {}
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

            sql_ins = f"""
                INSERT INTO {point_table_name} (
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
            sql = f"""
                INSERT OR IGNORE INTO {point_table_name} (
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
            sql = f"""
                INSERT INTO {point_table_name} (
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

    def _update_rlpreplot_from_stats_on_conn(self, conn: sqlite3.Connection, stats: dict[int, dict], file_fk: int,point_code:str="R"):
        if not stats:
            return
        if point_code =='R':
           line_table_name="RLPreplot"
           point_table_name="RPPreplot"
        elif point_code =='S':
            line_table_name = "SLPreplot"
            point_table_name = "SPPreplot"
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

        conn.executemany(f"""
            UPDATE {line_table_name}
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

    def update_line_real_geometry(
            self,
            point_type: str = "R",
            select_by: str = "Line_FK",  # "Line_FK" or "TierLine"
            line_ids: list[int] | None = None,
    ) -> str:
        """
        Update RLPreplot / SLPreplot using two farthest points from RPPreplot / SPPreplot.

        Updates:
          MinPoint, MaxPoint,
          RealStartX, RealStartY,
          RealEndX, RealEndY,
          RealLineLength

        select_by:
          - "Line_FK": points WHERE Line_FK = line.ID   (recommended)
          - "TierLine": points WHERE TierLine = line.TierLine

        Returns:
          "<line_table> | updated=<count>"
        """

        point_type = (point_type or "R").upper()
        if point_type not in ("R", "S"):
            raise ValueError("point_type must be 'R' or 'S'")

        select_by = (select_by or "Line_FK")
        if select_by not in ("Line_FK", "TierLine"):
            raise ValueError("select_by must be 'Line_FK' or 'TierLine'")

        line_table = "RLPreplot" if point_type == "R" else "SLPreplot"
        points_table = "RPPreplot" if point_type == "R" else "SPPreplot"

        def dist2(a, b):
            dx = a[1] - b[1]
            dy = a[2] - b[2]
            return dx * dx + dy * dy

        updated = 0

        with self._connect() as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("BEGIN IMMEDIATE")
            try:
                # ---- get lines to process ----
                if line_ids:
                    ph = ",".join("?" for _ in line_ids)
                    lines = conn.execute(
                        f"SELECT ID, TierLine FROM {line_table} WHERE ID IN ({ph})",
                        tuple(line_ids),
                    ).fetchall()
                else:
                    lines = conn.execute(
                        f"SELECT ID, TierLine FROM {line_table}"
                    ).fetchall()

                # ---- process each line ----
                for line_id, tierline in lines:
                    if select_by == "Line_FK":
                        pts = conn.execute(
                            f"""
                            SELECT Point, X, Y
                            FROM {points_table}
                            WHERE Line_FK = ?
                            ORDER BY Point
                            """,
                            (line_id,),
                        ).fetchall()
                    else:
                        if tierline is None:
                            continue
                        pts = conn.execute(
                            f"""
                            SELECT Point, X, Y
                            FROM {points_table}
                            WHERE TierLine = ?
                            ORDER BY Point
                            """,
                            (tierline,),
                        ).fetchall()

                    pts = [(int(p), float(x), float(y)) for (p, x, y) in pts if x is not None and y is not None]
                    if len(pts) < 2:
                        continue

                    min_point = pts[0][0]
                    max_point = pts[-1][0]

                    max_d2 = -1.0
                    p1 = p2 = None

                    for i in range(len(pts)):
                        for j in range(i + 1, len(pts)):
                            d2 = dist2(pts[i], pts[j])
                            if d2 > max_d2:
                                max_d2 = d2
                                p1, p2 = pts[i], pts[j]

                    if p1[0] > p2[0]:
                        p1, p2 = p2, p1

                    conn.execute(
                        f"""
                        UPDATE {line_table}
                        SET MinPoint = ?,
                            MaxPoint = ?,
                            RealStartX = ?,
                            RealStartY = ?,
                            RealEndX = ?,
                            RealEndY = ?,
                            RealLineLength = ?
                        WHERE ID = ?
                        """,
                        (
                            min_point,
                            max_point,
                            p1[1], p1[2],
                            p2[1], p2[2],
                            max_d2 ** 0.5,
                            line_id,
                        ),
                    )

                    updated += 1

                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return f"{line_table} | updated={updated}"

    def update_line_real_geometry_fast(
            self,
            point_type: str = "R",
            select_by: str = "Line_FK",  # "Line_FK" or "TierLine"
            line_ids: list[int] | None = None,
    ) -> str:
        """
        Exact farthest-pair using convex hull + rotating calipers.
        Much faster than O(n^2) on big lines.
        """

        point_type = (point_type or "R").upper()
        if point_type not in ("R", "S"):
            raise ValueError("point_type must be 'R' or 'S'")

        select_by = (select_by or "Line_FK")
        if select_by not in ("Line_FK", "TierLine"):
            raise ValueError("select_by must be 'Line_FK' or 'TierLine'")

        line_table = "RLPreplot" if point_type == "R" else "SLPreplot"
        points_table = "RPPreplot" if point_type == "R" else "SPPreplot"

        def _cross(o, a, b):
            return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])

        def _dist2_xy(a, b):
            dx = a[0] - b[0]
            dy = a[1] - b[1]
            return dx * dx + dy * dy

        def _convex_hull(points_xy):
            # Monotonic chain. points_xy: list[(x,y)] unique-ish
            pts = sorted(points_xy)
            if len(pts) <= 1:
                return pts

            lower = []
            for p in pts:
                while len(lower) >= 2 and _cross(lower[-2], lower[-1], p) <= 0:
                    lower.pop()
                lower.append(p)

            upper = []
            for p in reversed(pts):
                while len(upper) >= 2 and _cross(upper[-2], upper[-1], p) <= 0:
                    upper.pop()
                upper.append(p)

            return lower[:-1] + upper[:-1]  # no duplicate endpoints

        def _rotating_calipers_diameter(hull):
            # Returns (p1, p2, max_d2). hull is CCW polygon without duplicate endpoint.
            m = len(hull)
            if m == 0:
                return None, None, -1.0
            if m == 1:
                return hull[0], hull[0], 0.0
            if m == 2:
                return hull[0], hull[1], _dist2_xy(hull[0], hull[1])

            def area2(i, j, k):
                # twice triangle area (signed)
                return abs(_cross(hull[i], hull[j], hull[k]))

            j = 1
            best_i = 0
            best_j = 1
            best_d2 = _dist2_xy(hull[0], hull[1])

            for i in range(m):
                ni = (i + 1) % m
                while area2(i, ni, (j + 1) % m) > area2(i, ni, j):
                    j = (j + 1) % m

                d2 = _dist2_xy(hull[i], hull[j])
                if d2 > best_d2:
                    best_d2 = d2
                    best_i, best_j = i, j

                d2 = _dist2_xy(hull[ni], hull[j])
                if d2 > best_d2:
                    best_d2 = d2
                    best_i, best_j = ni, j

            return hull[best_i], hull[best_j], best_d2

        updated = 0

        with self._connect() as conn:
            conn.execute("PRAGMA foreign_keys = ON;")

            # speed pragmas for bulk update (safe enough for a rebuild step)
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA temp_store = MEMORY;")

            conn.execute("BEGIN IMMEDIATE")
            try:
                if line_ids:
                    ph = ",".join("?" for _ in line_ids)
                    lines = conn.execute(
                        f"SELECT ID, TierLine FROM {line_table} WHERE ID IN ({ph})",
                        tuple(line_ids),
                    ).fetchall()
                else:
                    lines = conn.execute(f"SELECT ID, TierLine FROM {line_table}").fetchall()

                for line_id, tierline in lines:
                    if select_by == "Line_FK":
                        # Get min/max point without sorting a huge list in Python
                        mm = conn.execute(
                            f"SELECT MIN(Point), MAX(Point) FROM {points_table} WHERE Line_FK=?",
                            (line_id,),
                        ).fetchone()
                        if not mm or mm[0] is None or mm[1] is None:
                            continue
                        min_point, max_point = int(mm[0]), int(mm[1])

                        pts = conn.execute(
                            f"SELECT X, Y FROM {points_table} WHERE Line_FK=? AND X IS NOT NULL AND Y IS NOT NULL",
                            (line_id,),
                        ).fetchall()
                    else:
                        if tierline is None:
                            continue
                        mm = conn.execute(
                            f"SELECT MIN(Point), MAX(Point) FROM {points_table} WHERE TierLine=?",
                            (tierline,),
                        ).fetchone()
                        if not mm or mm[0] is None or mm[1] is None:
                            continue
                        min_point, max_point = int(mm[0]), int(mm[1])

                        pts = conn.execute(
                            f"SELECT X, Y FROM {points_table} WHERE TierLine=? AND X IS NOT NULL AND Y IS NOT NULL",
                            (tierline,),
                        ).fetchall()

                    if not pts or len(pts) < 2:
                        continue

                    # Build hull on XY only (dedup helps hull performance a lot)
                    xy = [(float(x), float(y)) for (x, y) in pts]
                    if len(xy) < 2:
                        continue

                    # Optional dedup (can reduce hull work massively if repeated points)
                    # Using dict preserves speed; order not important
                    xy = list({p: None for p in xy}.keys())
                    if len(xy) < 2:
                        continue

                    hull = _convex_hull(xy)
                    p1_xy, p2_xy, max_d2 = _rotating_calipers_diameter(hull)
                    if max_d2 < 0:
                        continue

                    # Enforce "start/end" order by Point: we don't know Point for hull endpoints,
                    # so keep "RealStart/RealEnd" as geometric endpoints only (stable for mapping).
                    conn.execute(
                        f"""
                        UPDATE {line_table}
                        SET MinPoint = ?,
                            MaxPoint = ?,
                            RealStartX = ?,
                            RealStartY = ?,
                            RealEndX = ?,
                            RealEndY = ?,
                            RealLineLength = ?
                        WHERE ID = ?
                        """,
                        (
                            min_point,
                            max_point,
                            p1_xy[0], p1_xy[1],
                            p2_xy[0], p2_xy[1],
                            max_d2 ** 0.5,
                            line_id,
                        ),
                    )

                    updated += 1

                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return f"{line_table} | updated={updated}"

    def delete_preplot_lines(self, line_ids: list[int],point_code:str="R") -> int:
        if not line_ids:
            return 0
        if point_code == "R":
           line_table="RLPreplot"
        else:
            line_table = "SLPreplot"
        with self._connect() as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("BEGIN IMMEDIATE")
            try:
                placeholders = ",".join("?" for _ in line_ids)


                #conn.execute(f"DELETE FROM RPPreplot WHERE Line_FK IN ({placeholders})", line_ids)
                cur = conn.execute(f"DELETE FROM {line_table} WHERE ID IN ({placeholders})", line_ids)

                conn.commit()
                return cur.rowcount
            except Exception:
                conn.rollback()
                raise

    def delete_shapes(self, full_names: list[str]) -> int:
        if not full_names:
            return 0

        with self._connect() as conn:
            cur = conn.cursor()
            placeholders = ",".join("?" for _ in full_names)

            cur.execute(
                f"DELETE FROM project_shapes WHERE FullName IN ({placeholders});",
                full_names,
            )

            conn.commit()
            return cur.rowcount

    def export_sol_and_eol_to_csv(
            self,
            csv_path: str,
            *,
            where_sql: str = "",
            params: Sequence = (),
            order_by: str = "Line, TierLine, FirstPoint",
            include_header: bool = True,
            point_type: str = "R",
    ) -> str:
        """
        Export RLPreplot lines to CSV.

        Columns:
        Line, TierLine, FirstPoint, LastPoint,
        StartX, StartY, EndX, EndY
        """
        data_table = "RLPreplot" if point_type == "R" else "SLPreplot"
        columns = (
            "Line",
            "TierLine",
            "FirstPoint",
            "LastPoint",
            "StartX",
            "StartY",
            "EndX",
            "EndY",
        )

        sql = f"""
            SELECT {", ".join(columns)}
            FROM {data_table}
            {("WHERE " + where_sql) if where_sql else ""}
            {("ORDER BY " + order_by) if order_by else ""}
        """

        csv_path = Path(csv_path)

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            data = cur.fetchall()

            with csv_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)

                if include_header:
                    writer.writerow(columns)

                for row in data:
                    writer.writerow(row)

        return str(csv_path)

    def export_preplot_by_line(
            self,
            point_type: str,
            out_dir: str,
            where_sql: str = "",
            params: tuple = (),
            chunk_size: int = 5000,
    ) -> str:
        """
        Export RPPreplot (R) or SPPreplot (S) into multiple CSV files,
        split by TierLine.

        Returns:
            "<out_dir> | files=<count>"
        """

        point_type = (point_type or "R").upper()
        table = "RPPreplot" if point_type == "R" else "SPPreplot"

        if not os.path.isdir(out_dir):
            raise FileNotFoundError(f"Export directory does not exist: {out_dir}")

        sql = f"""
            SELECT TierLine, Line, Point, X, Y, Z
            FROM {table}
            {where_sql}
            ORDER BY TierLine, Point
        """

        files_created = 0
        current_tierline = None
        f = None
        writer = None

        def close_file():
            nonlocal f
            if f:
                f.close()
                f = None

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)

            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break

                for (tierline, line, point, x, y, z) in rows:
                    if tierline != current_tierline:
                        close_file()
                        current_tierline = tierline

                        path = os.path.join(
                            out_dir, f"{point_type}{current_tierline}.csv"
                        )
                        f = open(path, "w", newline="", encoding="utf-8")
                        writer = csv.writer(f)

                        writer.writerow([
                            "TierLine",
                            "Line",
                            "Point",
                            "X",
                            "Y",
                            "Z",
                            "PointType",
                        ])

                        files_created += 1

                    writer.writerow([
                        tierline,
                        line,
                        point,
                        x,
                        y,
                        z,
                        point_type,
                    ])

        close_file()

        return f"{out_dir} | files={files_created}"

    def export_preplot_to_one_csv(
            self,
            point_type: str,
            out_dir: str,
            where_sql: str = "",
            params: tuple = (),
            chunk_size: int = 5000,
    ) -> str:
        """
        Export RPPreplot (R) or SPPreplot (S) into ONE single CSV file.

        Returns:
            "<out_path> | files=1"
        """

        point_type = (point_type or "R").upper()
        table = "RPPreplot" if point_type == "R" else "SPPreplot"


        if not os.path.isdir(out_dir):
            raise FileNotFoundError(f"Export directory does not exist: {out_dir}")
        out_file = Path(out_dir / F'{point_type}_preplot_export.csv')
        sql = f"""
            SELECT TierLine, Line, Point, X, Y, Z
            FROM {table}
            {where_sql}
            ORDER BY TierLine, Point
        """

        with open(out_file , "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "TierLine",
                "Line",
                "Point",
                "X",
                "Y",
                "Z",
                "PointType",
            ])

            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, params)

                while True:
                    rows = cur.fetchmany(chunk_size)
                    if not rows:
                        break

                    for (tierline, line, point, x, y, z) in rows:
                        writer.writerow([tierline, line, point, x, y, z, point_type])

        return f"{out_file } | files=1"

    def export_to_gpkg(
            self,
            tierline: int,
            out_dir: str,
            filename: str,
            point_type: str = "R",
            points_layer: str = "points",
            line_layer: str = "line",
    ) -> str:
        """
        Export one TierLine into ONE GeoPackage file with 2 layers:
          - <points_layer> (Point)
          - <line_layer>   (LineString)

        Creates:
          - <out_dir>/<filename>.gpkg

        Returns:
            "<gpkg_path> | files=1"
        """

        point_type = (point_type or "R").upper()
        table = "RPPreplot" if point_type == "R" else "SPPreplot"

        if not os.path.isdir(out_dir):
            raise FileNotFoundError(f"Output directory does not exist: {out_dir}")

        gpkg_path = os.path.join(out_dir, f"{filename}.gpkg")

        sql = f"""
            SELECT TierLine, Line, Point, X, Y
            FROM {table}
            WHERE TierLine = ?
            ORDER BY Point
        """

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql, (tierline,))
            rows = cur.fetchall()

        if len(rows) < 2:
            raise ValueError(f"TierLine {tierline} has less than 2 points")

        # --- schemas (GPKG supports long field names, but keep them simple) ---
        points_schema = {
            "geometry": "Point",
            "properties": {
                "TierLine": "int",
                "Line": "int",
                "Point": "int",
            },
        }

        line_schema = {
            "geometry": "LineString",
            "properties": {
                "TierLine": "int",
                "Line": "int",
            },
        }

        # If file exists, remove to avoid layer conflicts
        if os.path.exists(gpkg_path):
            os.remove(gpkg_path)

        # ---- write POINTS layer ----
        with fiona.open(
                gpkg_path,
                "w",
                driver="GPKG",
                layer=points_layer,
                schema=points_schema,
        ) as dst:
            for (tl, ln, pt, x, y) in rows:
                dst.write({
                    "geometry": mapping(Point(float(x), float(y))),
                    "properties": {
                        "TierLine": int(tl),
                        "Line": int(ln),
                        "Point": int(pt),
                    },
                })

        # ---- write LINE layer (append to same gpkg) ----
        points_xy = [(float(r[3]), float(r[4])) for r in rows]
        with fiona.open(
                gpkg_path,
                "a",
                driver="GPKG",
                layer=line_layer,
                schema=line_schema,
        ) as dst:
            dst.write({
                "geometry": mapping(LineString(points_xy)),
                "properties": {
                    "TierLine": int(rows[0][0]),
                    "Line": int(rows[0][1]),
                },
            })

        return f"{gpkg_path} | files=1"

    def export_all_lines_to_gpkg(
            self,
            out_dir: str,
            point_type: str = "R",
            epsg: int=32634,
            chunk_size: int = 5000,
            where_sql: str = "",
            params: tuple = (),
    ) -> str:
        """
        Export ALL TierLines from RPPreplot/SPPreplot.
        Creates ONE GeoPackage per TierLine:
            <out_dir>/<table>_TIERLINE_<TierLine>.gpkg

        Returns:
            "<out_dir> | files=<count>"
        """

        point_type = (point_type or "R").upper()
        table = "RPPreplot" if point_type == "R" else "SPPreplot"

        if not os.path.isdir(out_dir):
            raise FileNotFoundError(f"Output directory does not exist: {out_dir}")

        sql = f"""
            SELECT TierLine, Line, Point, X, Y
            FROM {table}
            {where_sql}
            ORDER BY TierLine, Point
        """

        points_schema = {
            "geometry": "Point",
            "properties": {
                "TierLine": "int",
                "Line": "int",
                "Point": "int",
            },
        }

        lines_schema = {
            "geometry": "LineString",
            "properties": {
                "TierLine": "int",
                "Line": "int",
            },
        }

        files_created = 0

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)

            current_tl = None
            current_ln = None
            coords = []
            rows_cache = []

            def flush_tierline(tl, ln, rows, coords):
                nonlocal files_created

                if not rows or len(coords) < 2:
                    return

                gpkg_path = os.path.join(out_dir, f"{point_type}_LINE_{tl}.gpkg")

                if os.path.exists(gpkg_path):
                    os.remove(gpkg_path)
                crs = fiona.crs.from_epsg(int(epsg))
                # --- write POINTS layer ---
                with fiona.open(
                        gpkg_path,
                        "w",
                        driver="GPKG",
                        layer="points",
                        schema=points_schema,
                        crs=crs,
                ) as dst:
                    for (tl_, ln_, pt_, x_, y_) in rows:
                        dst.write({
                            "geometry": mapping(Point(float(x_), float(y_))),
                            "properties": {
                                "TierLine": int(tl_),
                                "Line": int(ln_),
                                "Point": int(pt_),
                            },
                        })

                # --- write LINE layer ---
                with fiona.open(
                        gpkg_path,
                        "a",
                        driver="GPKG",
                        layer="line",
                        schema=lines_schema,
                ) as dst:
                    dst.write({
                        "geometry": mapping(LineString(coords)),
                        "properties": {
                            "TierLine": int(tl),
                            "Line": int(ln),
                        },
                    })

                files_created += 1

            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break

                for (tl, ln, pt, x, y) in rows:
                    if tl != current_tl:
                        flush_tierline(current_tl, current_ln, rows_cache, coords)
                        current_tl = tl
                        current_ln = ln
                        coords = []
                        rows_cache = []

                    rows_cache.append((tl, ln, pt, x, y))
                    coords.append((float(x), float(y)))

            flush_tierline(current_tl, current_ln, rows_cache, coords)

        return f"{out_dir} | files={files_created}"

    def export_all_tierlines_to_one_gpkg(
            self,
            out_dir: str,
            filename: str,
            point_type: str = "R",
            epsg: int = 4326,
            where_sql: str = "",
            params: tuple = (),
            chunk_size: int = 5000,
            overwrite: bool = True,
    ) -> str:
        """
        Export ALL TierLines from RPPreplot/SPPreplot into ONE GeoPackage file.
        Each TierLine becomes TWO layers:
          - <R|S>_TL_<TierLine>_points
          - <R|S>_TL_<TierLine>_line

        Returns:
          "<gpkg_path> | files=1"
        """

        point_type = (point_type or "R").upper()
        table = "RPPreplot" if point_type == "R" else "SPPreplot"

        if not os.path.isdir(out_dir):
            raise FileNotFoundError(f"Output directory does not exist: {out_dir}")

        gpkg_path = os.path.join(out_dir, f"{filename}.gpkg")
        if overwrite and os.path.exists(gpkg_path):
            os.remove(gpkg_path)

        crs = fiona.crs.from_epsg(int(epsg))

        sql = f"""
            SELECT TierLine, Line, Point, X, Y
            FROM {table}
            {where_sql}
            ORDER BY TierLine, Point
        """

        # GPKG supports long field names, but keep simple
        points_schema = {
            "geometry": "Point",
            "properties": {"TierLine": "int", "Line": "int", "Point": "int"},
        }
        line_schema = {
            "geometry": "LineString",
            "properties": {"TierLine": "int", "Line": "int"},
        }

        # We'll buffer one TierLine at a time (points + coords) and then write two layers.
        first_layer_written = False

        def layer_name_points(tl: int) -> str:
            return f"{point_type}_TL_{int(tl)}_points"

        def layer_name_line(tl: int) -> str:
            return f"{point_type}_TL_{int(tl)}_line"

        def write_tierline_layers(tl, ln, rows_buf, coords_buf):
            nonlocal first_layer_written

            # --- validate inputs ---
            if tl is None or ln is None:
                return
            if not rows_buf:
                return

            # Clean coords (avoid GEOS/GDAL weird errors)
            coords = [(float(x), float(y)) for (x, y) in coords_buf if x is not None and y is not None]
            if len(coords) < 2:
                return

            # Layer names: keep short + safe
            tl_int = int(tl)
            layer_pts = f"{point_type}_TL_{tl_int}_pts"
            layer_lin = f"{point_type}_TL_{tl_int}_lin"

            # Avoid super long names (GDAL sometimes hates it)
            layer_pts = layer_pts[:50]
            layer_lin = layer_lin[:50]

            # If file already exists and layer exists, you'll get collision/mismatch errors.
            # If overwrite=True earlier, this should not happen, but guard anyway.
            if os.path.exists(gpkg_path):
                try:
                    existing = set(fiona.listlayers(gpkg_path))
                    if layer_pts in existing or layer_lin in existing:
                        # simplest safe behavior: skip duplicates
                        return
                except Exception:
                    # if listlayers fails, continue and let open raise a clear error
                    pass

            # --- points layer ---
            mode_pts = "w" if not first_layer_written else "a"
            with fiona.open(
                    gpkg_path,
                    mode_pts,
                    driver="GPKG",
                    layer=layer_pts,
                    schema=points_schema,
                    crs=crs,
            ) as dst_points:
                for (tl_, ln_, pt_, x_, y_) in rows_buf:
                    if x_ is None or y_ is None:
                        continue
                    dst_points.write({
                        "geometry": mapping(Point(float(x_), float(y_))),
                        "properties": {"TierLine": int(tl_), "Line": int(ln_), "Point": int(pt_)},
                    })

            first_layer_written = True

            # --- line layer ---
            # IMPORTANT: many GDAL builds require crs= on layer creation even in append mode
            with fiona.open(
                    gpkg_path,
                    "a",
                    driver="GPKG",
                    layer=layer_lin,
                    schema=line_schema,
                    crs=crs,
            ) as dst_line:
                dst_line.write({
                    "geometry": mapping(LineString(coords)),
                    "properties": {"TierLine": tl_int, "Line": int(ln)},
                })

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)

            current_tl = None
            current_ln = None
            rows_buf = []
            coords_buf = []

            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break

                for (tl, ln, pt, x, y) in rows:
                    print(f"{tl}, {ln}, {pt}, {x}, {y}")
                    if tl != current_tl:
                        write_tierline_layers(current_tl, current_ln, rows_buf, coords_buf)
                        current_tl = tl
                        current_ln = ln
                        rows_buf = []
                        coords_buf = []

                    rows_buf.append((tl, ln, pt, x, y))
                    coords_buf.append((float(x), float(y)))

            # flush last TierLine
            write_tierline_layers(current_tl, current_ln, rows_buf, coords_buf)

        return f"{gpkg_path} | files=1"

    def export_preplot_to_shapes(
            self,
            points_dir: str,
            lines_dir: str,
            point_type: str,
            epsg: int | None = None,
            chunk_size: int = 5000,
    ) -> str:
        """
        Export ALL lines from RLPreplot/SLPreplot (chosen by point_type) to shapefiles.

        For each Line -> 2 files:
          - points: <points_dir>/<point_type>_<Line>_pts.shp
          - line:   <lines_dir>/<point_type>_<Line>_lin.shp

        Returns:
          "<points_dir>,<lines_dir> | files=<count>"
        """

        point_type = (point_type or "R").upper()
        if point_type not in ("R", "S"):
            raise ValueError("point_type must be 'R' or 'S'")

        table = "RPPreplot" if point_type == "R" else "SPPreplot"

        if not os.path.isdir(points_dir):
            raise FileNotFoundError(f"points_dir does not exist: {points_dir}")
        if not os.path.isdir(lines_dir):
            raise FileNotFoundError(f"lines_dir does not exist: {lines_dir}")

        crs = fiona.crs.from_epsg(int(epsg)) if epsg is not None else None

        # Shapefile field names <= 10 chars
        points_schema = {
            "geometry": "Point",
            "properties": {"TierLine": "int", "Line": "int", "Point": "int"},
        }
        line_schema = {
            "geometry": "LineString",
            "properties": {"TierLine": "int", "Line": "int"},
        }

        sql = f"""
            SELECT TierLine, Line, Point, X, Y
            FROM {table}
            ORDER BY Line, Point
        """

        files_created = 0
        current_line = None
        current_tierline = None
        points_rows = []
        coords = []

        def flush_line():
            nonlocal files_created, current_line, current_tierline, points_rows, coords

            if current_line is None:
                return

            coords_clean = [(float(x), float(y)) for (x, y) in coords if x is not None and y is not None]
            if len(coords_clean) < 2:
                # skip broken line (and its points) to avoid invalid exports
                current_line = None
                current_tierline = None
                points_rows = []
                coords = []
                return

            base = f"{point_type}_{int(current_line)}"
            points_path = os.path.join(points_dir, f"{base}_pts.shp")
            line_path = os.path.join(lines_dir, f"{base}_lin.shp")

            with fiona.open(points_path, "w", driver="ESRI Shapefile", schema=points_schema, crs=crs) as dst_pts:
                for (tl, ln, pt, x, y) in points_rows:
                    if x is None or y is None:
                        continue
                    dst_pts.write({
                        "geometry": mapping(Point(float(x), float(y))),
                        "properties": {"TierLine": int(tl), "Line": int(ln), "Point": int(pt)},
                    })

            with fiona.open(line_path, "w", driver="ESRI Shapefile", schema=line_schema, crs=crs) as dst_lin:
                dst_lin.write({
                    "geometry": mapping(LineString(coords_clean)),
                    "properties": {"TierLine": int(current_tierline), "Line": int(current_line)},
                })

            files_created += 2

            current_line = None
            current_tierline = None
            points_rows = []
            coords = []

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql)

            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break

                for (tl, ln, pt, x, y) in rows:
                    if ln is None:
                        continue

                    if ln != current_line:
                        flush_line()
                        current_line = ln
                        current_tierline = tl
                        points_rows = []
                        coords = []

                    points_rows.append((tl, ln, pt, x, y))
                    coords.append((x, y))

            flush_line()

        return f"{points_dir},{lines_dir} | files={files_created}"

    def export_splited_sps(
            self,
            out_dir: str,
            point_type: str = "S",
            where_sql: str = "",
            params: tuple = (),
            chunk_size: int = 5000,
    ) -> str:
        """
        Split preplot by TierLine and export each TierLine as separate SPS-like text file.

        Output file name:
          <out_dir>/<point_type><Line>_Preplot.r01

        Line format (matches your example):
          S{Line}                {Point}1S1                {X:.1f} {Y:.1f}     0

        Returns:
          "<out_dir> | files=<count>"
        """

        if not os.path.isdir(out_dir):
            raise FileNotFoundError(f"Output directory does not exist: {out_dir}")

        point_type = (point_type or "S").upper()
        if point_type not in ("R", "S"):
            raise ValueError("point_type must be 'R' or 'S'")

        table = "RPPreplot" if point_type == "R" else "SPPreplot"

        sql = f"""
            SELECT TierLine, Line, Point, X, Y
            FROM {table}
            {where_sql}
            ORDER BY TierLine, Point
        """

        files_created = 0
        current_tl = None
        current_line = None
        out = None

        def close_out():
            nonlocal out
            if out:
                out.close()
                out = None

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)

            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break

                for (tl, line, point, x, y) in rows:
                    if tl is None or line is None or point is None or x is None or y is None:
                        continue

                    if tl != current_tl:
                        close_out()
                        current_tl = tl
                        current_line = line

                        # filename starts with point_type then line
                        path = os.path.join(out_dir, f"{point_type}{int(current_line)}_Preplot.r01")
                        out = open(path, "wt", encoding="utf-8")
                        files_created += 1

                    # write line
                    out.write(
                        f"{point_type}{int(line)}                {int(point)}1{point_type}1                "
                        f"{float(x):.1f} {float(y):.1f}     0         \n"
                    )

        close_out()

        return f"{out_dir} | files={files_created}"

    def upload_csv_layer(
            db_path,
            uploaded_file,  # Django UploadedFile
            layer_name,
            comments,
            col_point,
            col_x,
            col_y,
            col_z,
    ):
        """
        Upload CSV into SQLite tables:
          - CSVLayers
          - CSVpoints

        Returns dict:
          {"layer_id": int, "points_inserted": int}
        """

        # --- read CSV (auto delimiter, headers) ---
        text_stream = io.TextIOWrapper(
            uploaded_file.file,
            encoding="utf-8-sig",
            newline=""
        )

        df = pd.read_csv(
            text_stream,
            sep=None,  # auto-detect delimiter
            engine="python"
        )

        # --- validate mapping ---
        required = [col_point, col_x, col_y, col_z]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns in CSV: {missing}")

        # --- normalize dataframe ---
        data = df[[col_point, col_x, col_y, col_z]].copy()
        data.columns = ["Point", "X", "Y", "Z"]

        data["X"] = pd.to_numeric(data["X"], errors="coerce")
        data["Y"] = pd.to_numeric(data["Y"], errors="coerce")
        data["Z"] = pd.to_numeric(data["Z"], errors="coerce")

        data.dropna(subset=["X", "Y", "Z"], inplace=True)

        points_count = int(len(data))

        # --- insert into SQLite ---
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        try:
            # insert layer
            cur.execute(
                """
                INSERT INTO CSVLayers (Name, Points, Comments)
                VALUES (?, ?, ?)
                """,
                (layer_name, points_count, comments or "")
            )
            layer_id = cur.lastrowid

            # bulk insert points
            cur.executemany(
                """
                INSERT INTO CSVpoints (Layer_FK, Point, X, Y, Z)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        layer_id,
                        r.Point,
                        float(r.X),
                        float(r.Y),
                        float(r.Z),
                    )
                    for r in data.itertuples(index=False)
                ]
            )

            conn.commit()

        except Exception:
            conn.rollback()
            raise

        finally:
            conn.close()

        return {
            "layer_id": layer_id,
            "points_inserted": points_count,
        }

    def get_csv_layers(self):
        """
        Fetch CSVLayers table as list of dicts.
        Safe for JsonResponse and Django templates.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("""
                    SELECT ID,
                           Name,
                           Points,
                           Attr1Name,
                           Attr2Name,
                           Attr3Name,
                           Comments
                    FROM CSVLayers
                    ORDER BY ID DESC
                    """)

        rows = [dict(row) for row in cur.fetchall()]
        conn.close()

        return rows

    def load_csv_layer_from_upload(
            self,
            uploaded_file,
            layer_name: str,
            comments: str,
            pointfield: str,
            xfield: str,
            yfield: str,
            zfield: str,
            attr1_name: str = "",
            attr2_name: str = "",
            attr3_name: str = "",
            attr1_field: str | None = None,
            attr2_field: str | None = None,
            attr3_field: str | None = None,
    ) -> dict:
        """
        Returns: {"layer_id": int, "points_inserted": int}
        """

        # bytes -> text (important for pandas sep=None)
        text_stream = io.TextIOWrapper(uploaded_file.file, encoding="utf-8-sig", newline="")

        df = pd.read_csv(text_stream, sep=None, engine="python")

        # required columns
        required = [pointfield, xfield, yfield, zfield]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # optional attr columns
        opt_fields = []
        if attr1_field:
            opt_fields.append(attr1_field)
        if attr2_field:
            opt_fields.append(attr2_field)
        if attr3_field:
            opt_fields.append(attr3_field)

        opt_missing = [c for c in opt_fields if c not in df.columns]
        if opt_missing:
            raise ValueError(f"Missing attribute columns: {opt_missing}")

        # build normalized frame
        cols = [pointfield, xfield, yfield, zfield]
        rename = {pointfield: "Point", xfield: "X", yfield: "Y", zfield: "Z"}

        if attr1_field:
            cols.append(attr1_field)
            rename[attr1_field] = "Attr1"
        if attr2_field:
            cols.append(attr2_field)
            rename[attr2_field] = "Attr2"
        if attr3_field:
            cols.append(attr3_field)
            rename[attr3_field] = "Attr3"

        data = df[cols].copy().rename(columns=rename)

        # numeric conversions
        data["X"] = pd.to_numeric(data["X"], errors="coerce")
        data["Y"] = pd.to_numeric(data["Y"], errors="coerce")
        data["Z"] = pd.to_numeric(data["Z"], errors="coerce")

        if "Attr1" in data.columns:
            data["Attr1"] = pd.to_numeric(data["Attr1"], errors="coerce")
        if "Attr2" in data.columns:
            data["Attr2"] = pd.to_numeric(data["Attr2"], errors="coerce")
        if "Attr3" in data.columns:
            data["Attr3"] = pd.to_numeric(data["Attr3"], errors="coerce")

        # drop bad coordinate rows
        data.dropna(subset=["X", "Y", "Z"], inplace=True)

        points_count = int(len(data))
        if points_count == 0:
            raise ValueError("No valid rows (X/Y/Z) found after conversion.")

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        try:
            # 1) insert layer
            cur.execute(
                """
                INSERT INTO CSVLayers (Name, Points, Attr1Name, Attr2Name, Attr3Name, Comments)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    layer_name or "CSV Layer",
                    points_count,
                    (attr1_name or "") if attr1_field else "",
                    (attr2_name or "") if attr2_field else "",
                    (attr3_name or "") if attr3_field else "",
                    comments or "",
                )
            )
            layer_id = cur.lastrowid

            # 2) bulk insert points
            has_a1 = "Attr1" in data.columns
            has_a2 = "Attr2" in data.columns
            has_a3 = "Attr3" in data.columns

            rows = []
            for r in data.itertuples(index=False):
                # r has attributes by column order
                d = r._asdict() if hasattr(r, "_asdict") else {
                    "Point": r[0], "X": r[1], "Y": r[2], "Z": r[3]
                }

                rows.append((
                    layer_id,
                    str(d.get("Point", "")),
                    float(d["X"]),
                    float(d["Y"]),
                    float(d["Z"]),
                    float(d["Attr1"]) if has_a1 and d.get("Attr1") is not None else None,
                    float(d["Attr2"]) if has_a2 and d.get("Attr2") is not None else None,
                    float(d["Attr3"]) if has_a3 and d.get("Attr3") is not None else None,
                ))

            cur.executemany(
                """
                INSERT INTO CSVpoints (Layer_FK, Point, X, Y, Z, Attr1, Attr2, Attr3)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows
            )

            conn.commit()
            return {"layer_id": layer_id, "points_inserted": points_count}

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def farthest_points(
            self,
            point_type: str = "R",
            tierline: int | None = None,
            line_fk: int | None = None,
            select_by: str = "TierLine",  # "TierLine" or "Line_FK"
    ):
        """
        Find two points with maximum XY distance within a selected set of points
        from RPPreplot/SPPreplot.

        Selection:
          - select_by="TierLine" -> uses WHERE TierLine = ?
          - select_by="Line_FK"  -> uses WHERE Line_FK = ?

        Args:
            point_type: 'R' -> RPPreplot, 'S' -> SPPreplot
            tierline: TierLine value (required if select_by="TierLine")
            line_fk: parent ID (required if select_by="Line_FK")
            select_by: "TierLine" or "Line_FK"

        Returns:
            {
                "TierLine": int | None,
                "Line_FK": int | None,
                "P1": {"Point": int, "X": float, "Y": float, "Z": float | None},
                "P2": {"Point": int, "X": float, "Y": float, "Z": float | None},
                "Distance": float
            }

        Notes:
          - P1 and P2 are ordered by Point (ascending).
          - Distance is computed in XY.
        """

        point_type = (point_type or "R").upper()
        if point_type not in ("R", "S"):
            raise ValueError("point_type must be 'R' or 'S'")
        table = "RPPreplot" if point_type == "R" else "SPPreplot"

        select_by = (select_by or "TierLine").strip()
        if select_by not in ("TierLine", "Line_FK"):
            raise ValueError("select_by must be 'TierLine' or 'Line_FK'")

        if select_by == "TierLine":
            if tierline is None:
                raise ValueError("tierline is required when select_by='TierLine'")
            where_col = "TierLine"
            where_val = tierline
        else:
            if line_fk is None:
                raise ValueError("line_fk is required when select_by='Line_FK'")
            where_col = "Line_FK"
            where_val = line_fk

        sql = f"""
            SELECT TierLine, Line_FK, Line, Point, X, Y, Z
            FROM {table}
            WHERE {where_col} = ?
            ORDER BY Point
        """

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql, (where_val,))
            rows = cur.fetchall()

        if len(rows) < 2:
            raise ValueError(f"Selection has less than 2 points ({where_col}={where_val})")

        # keep only rows with valid XY
        pts = []
        for (tl, lf, ln, pt, x, y, z) in rows:
            if x is None or y is None:
                continue
            pts.append((int(pt), float(x), float(y), float(z) if z is not None else None, tl, lf))

        if len(pts) < 2:
            raise ValueError(f"Selection has less than 2 valid XY points ({where_col}={where_val})")

        max_d2 = -1.0
        a = b = None

        for i in range(len(pts)):
            _, x1, y1, *_ = pts[i]
            for j in range(i + 1, len(pts)):
                _, x2, y2, *_ = pts[j]
                dx = x1 - x2
                dy = y1 - y2
                d2 = dx * dx + dy * dy
                if d2 > max_d2:
                    max_d2 = d2
                    a = pts[i]
                    b = pts[j]

        # order by Point ascending
        if a[0] > b[0]:
            a, b = b, a

        # Prefer returning real values from the dataset
        tierline_out = a[4] if select_by == "Line_FK" else tierline
        line_fk_out = a[5] if select_by == "TierLine" else line_fk

        return {
            "TierLine": tierline_out,
            "Line_FK": line_fk_out,
            "P1": {"Point": a[0], "X": a[1], "Y": a[2], "Z": a[3]},
            "P2": {"Point": b[0], "X": b[1], "Y": b[2], "Z": b[3]},
            "Distance": max_d2 ** 0.5,
        }

    def get_preplot_summary_allfiles(self) -> dict:
        """
        Returns:
          {
            "RLPreplot": {...row dict...},
            "SLPreplot": {...row dict...}
          }
        Requires SQL view: PreplotSummaryAllFiles
        """
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM PreplotSummaryAllFiles").fetchall()

        out = {}
        for r in rows:
            # sqlite3.Row -> dict
            d = dict(r)
            out[d["TableName"]] = d

        # Ensure keys exist even if one table empty
        out.setdefault("RLPreplot", {})
        out.setdefault("SLPreplot", {})
        return out







