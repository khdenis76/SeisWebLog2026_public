from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3

import numpy as np


@dataclass(slots=True)
class SvpCast:
    profile_id: int
    name: str
    x: float
    y: float
    timestamp: str
    depth: np.ndarray
    velocity: np.ndarray
    temperature: np.ndarray
    salinity: np.ndarray
    conductivity: np.ndarray
    density: np.ndarray


class SvpDataError(RuntimeError):
    pass


class SvpDataRepository:
    PROFILE_TABLES = ("SVP_Profiles", "SVP_Profile", "svp_profiles", "svp_profile")
    DATA_TABLES = ("SVP_Data", "svp_data", "SVP_Points", "svp_points")

    def __init__(self, project_path: str | Path) -> None:
        path = Path(project_path).expanduser().resolve()
        self.db_path = self._resolve_db_path(path)

    @staticmethod
    def _resolve_db_path(path: Path) -> Path:
        """Resolve the active project's SQLite database robustly.

        DataViewer2 may receive either the database file itself, the project
        root, or the project's ``data`` directory.
        """
        if path.is_file() and path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
            return path

        candidates = [
            path / "project.sqlite3",
            path / "data" / "project.sqlite3",
            path / "db.sqlite3",
            path.parent / "data" / "project.sqlite3",
        ]
        for candidate in candidates:
            if candidate.is_file():
                return candidate.resolve()

        checked = "\n  - ".join(str(c) for c in candidates)
        raise SvpDataError(
            "Project database was not found. Paths checked:\n  - " + checked
        )

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_path), timeout=30)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _tables(con: sqlite3.Connection) -> set[str]:
        return {
            str(r[0])
            for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
            )
        }

    @staticmethod
    def _columns(con: sqlite3.Connection, table: str) -> list[str]:
        return [str(r[1]) for r in con.execute(f'PRAGMA table_info("{table}")')]

    @staticmethod
    def _pick(columns: list[str], *aliases: str) -> str | None:
        lookup = {c.lower(): c for c in columns}
        return next((lookup[a.lower()] for a in aliases if a.lower() in lookup), None)

    def _resolve_tables(self, con: sqlite3.Connection) -> tuple[str, str]:
        tables = self._tables(con)
        table_lookup = {table.lower(): table for table in tables}
        p = next((table_lookup.get(candidate.lower()) for candidate in self.PROFILE_TABLES if candidate.lower() in table_lookup), None)
        d = next((table_lookup.get(candidate.lower()) for candidate in self.DATA_TABLES if candidate.lower() in table_lookup), None)
        if not p or not d:
            found = ", ".join(sorted(tables))
            raise SvpDataError(
                "SVP profile/point tables were not found in the opened database. "
                f"Database: {self.db_path}\n"
                "Expected svp_profiles with svp_points (or legacy SVP_Data). "
                f"Tables/views found: {found or 'none'}"
            )
        return p, d

    def load_casts(self) -> list[SvpCast]:
        with self._connect() as con:
            pt, dt = self._resolve_tables(con)
            pc, dc = self._columns(con, pt), self._columns(con, dt)
            pid = self._pick(pc, "ID", "id", "ProfileID")
            name = self._pick(pc, "ProfileName", "Name", "profile_name", "FileName")
            x = self._pick(pc, "Easting", "X", "coord_e", "Longitude")
            y = self._pick(pc, "Northing", "Y", "coord_n", "Latitude")
            ts = self._pick(pc, "StartDateTime", "DateTime", "Timestamp", "StartTime", "CreatedAt")
            if not pid or not x or not y:
                raise SvpDataError(f"SVP profile ID/Easting/Northing columns were not found in {pt}. Columns: {', '.join(pc)}")

            fk = self._pick(dc, "Profile_FK", "ProfileID", "SVP_Profile_FK", "profile_id", "svp_id")
            depth = self._pick(dc, "Depth", "depth_m", "DepthM", "Pressure")
            velocity = self._pick(dc, "Velocity", "SoundVelocity", "sound_velocity_ms", "VelocityMS", "velocity_mps")
            temperature = self._pick(dc, "Temperature", "temperature_c", "Temp")
            salinity = self._pick(dc, "Salinity", "salinity_psu")
            conductivity = self._pick(dc, "Conductivity", "conductivity_mscm", "ConductivityMSCM")
            density = self._pick(dc, "Density", "density_kgm3")
            if not fk or not depth or not velocity:
                raise SvpDataError(f"SVP point profile/depth/velocity columns were not found in {dt}. Columns: {', '.join(dc)}")

            p_select = [f'"{pid}" AS profile_id', f'"{x}" AS x', f'"{y}" AS y']
            p_select.append(f'"{name}" AS name' if name else "'' AS name")
            p_select.append(f'"{ts}" AS timestamp' if ts else "'' AS timestamp")
            profiles = con.execute(f'SELECT {", ".join(p_select)} FROM "{pt}" WHERE "{x}" IS NOT NULL AND "{y}" IS NOT NULL').fetchall()

            fields = {"depth": depth, "velocity": velocity, "temperature": temperature, "salinity": salinity, "conductivity": conductivity, "density": density}
            out: list[SvpCast] = []
            for p in profiles:
                select = [f'CAST("{depth}" AS REAL) AS depth', f'CAST("{velocity}" AS REAL) AS velocity']
                for key in ("temperature", "salinity", "conductivity", "density"):
                    col = fields[key]
                    select.append(f'CAST("{col}" AS REAL) AS {key}' if col else f'NULL AS {key}')
                rows = con.execute(
                    f'SELECT {", ".join(select)} FROM "{dt}" WHERE "{fk}"=? ORDER BY CAST("{depth}" AS REAL)',
                    (p["profile_id"],),
                ).fetchall()
                if not rows:
                    continue
                def arr(key: str) -> np.ndarray:
                    return np.asarray([np.nan if r[key] is None else float(r[key]) for r in rows], dtype=float)
                dep, vel = arr("depth"), arr("velocity")
                valid = np.isfinite(dep) & np.isfinite(vel)
                if valid.sum() < 2:
                    continue
                out.append(SvpCast(
                    int(p["profile_id"]), str(p["name"] or f'SVP {p["profile_id"]}'), float(p["x"]), float(p["y"]), str(p["timestamp"] or ""),
                    dep[valid], vel[valid], arr("temperature")[valid], arr("salinity")[valid], arr("conductivity")[valid], arr("density")[valid],
                ))
        return out

    def load_rp_preplot(self) -> tuple[np.ndarray, np.ndarray]:
        with self._connect() as con:
            if "RPPreplot" not in self._tables(con):
                return np.empty(0), np.empty(0)
            cols = self._columns(con, "RPPreplot")
            x = self._pick(cols, "X", "Easting", "PreplotEasting")
            y = self._pick(cols, "Y", "Northing", "PreplotNorthing")
            if not x or not y:
                return np.empty(0), np.empty(0)
            rows = con.execute(f'SELECT CAST("{x}" AS REAL), CAST("{y}" AS REAL) FROM RPPreplot WHERE "{x}" IS NOT NULL AND "{y}" IS NOT NULL').fetchall()
        return np.asarray([r[0] for r in rows], float), np.asarray([r[1] for r in rows], float)
