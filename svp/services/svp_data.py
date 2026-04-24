from __future__ import annotations

import csv
import io
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from .svp_parser import SVPParser

@dataclass
class SVPProfileRow:
    id: int
    name: str | None
    profile_source: str | None
    file_type: str | None
    location: str | None
    instrument_make: str | None
    instrument_model: str | None
    serial: str | None
    rov: str | None
    timestamp: str | None
    latitude: float | None
    longitude: float | None
    coord_e: float | None
    coord_n: float | None
    casts: str | None
    surface_velocity: float | None
    mean_velocity: float | None
    seabed_velocity: float | None
    bottom_depth: float | None
    mean_density: float | None
    temperature_surface: float | None
    salinity_surface: float | None
    source_file_name: str | None
    notes: str | None
    points_count: int
    created_at: str | None
    updated_at: str | None


class SVPData:
    """
    No-ORM SVP service for SeisWebLog project database.

    Supports:
      - DB schema creation
      - CRUD for profiles + points
      - import from .svp / .000 / .csv / .txt
    """

    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self.ensure_tables()

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------
    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _utcnow_str() -> str:
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None

    @staticmethod
    def _to_int(value: Any) -> int | None:
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None
        try:
            return int(float(s))
        except Exception:
            return None

    @staticmethod
    def _clean_text(value: Any) -> str | None:
        if value is None:
            return None
        s = str(value).strip()
        return s if s else None

    @staticmethod
    def _parse_datetime_flexible(value: str | None) -> str | None:
        if not value:
            return None

        s = value.strip()
        if not s:
            return None

        formats = [
            "%d/%m/%Y %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%Y-%m-%dT%H:%M:%S",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
        return s

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    def ensure_tables(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS svp_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    profile_source TEXT,
                    file_type TEXT,
                    location TEXT,
                    instrument_make TEXT,
                    instrument_model TEXT,
                    serial TEXT,
                    rov TEXT,
                    timestamp TEXT,
                    latitude REAL,
                    longitude REAL,
                    coord_e REAL,
                    coord_n REAL,
                    casts TEXT,
                    surface_velocity REAL,
                    mean_velocity REAL,
                    seabed_velocity REAL,
                    bottom_depth REAL,
                    mean_density REAL,
                    temperature_surface REAL,
                    salinity_surface REAL,
                    source_file_name TEXT,
                    source_file_path TEXT,
                    raw_header TEXT,
                    notes TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS svp_points (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    svp_id INTEGER NOT NULL,
                    point_index INTEGER NOT NULL,
                    depth_m REAL NOT NULL,
                    velocity_mps REAL NOT NULL,
                    temperature_c REAL,
                    salinity_psu REAL,
                    density_kgm3 REAL,
                    source_row_text TEXT,
                    FOREIGN KEY (svp_id) REFERENCES svp_profiles(id) ON DELETE CASCADE
                )
                """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_svp_points_svp_id
                ON svp_points (svp_id)
                """
            )
            conn.execute(
                """
                          CREATE TABLE IF NOT EXISTS svp_format_setups (
                                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                                 name TEXT,
                                 file_ext TEXT,
                                 delimiter TEXT,
                             
                                 header_line_count INTEGER,
                                 data_header_line_index INTEGER,
                                 data_start_line_index INTEGER,
                             
                                 meta_coordinates_key TEXT,
                                 meta_lat_key TEXT,
                                 meta_lon_key TEXT,
                                 meta_rov_key TEXT,
                                 meta_timestamp_key TEXT,
                                 meta_name_key TEXT,
                                 meta_location_key TEXT,
                                 meta_serial_key TEXT,
                                 meta_make_key TEXT,
                                 meta_model_key TEXT,
                             
                                 col_timestamp TEXT,
                                 col_depth TEXT,
                                 col_velocity TEXT,
                                 col_temperature TEXT,
                                 col_salinity TEXT,
                                 col_density TEXT,
                             
                                 sort_by_depth INTEGER,
                                 clamp_negative_depth_to_zero INTEGER,
                                 pressure_is_depth INTEGER,
                             
                                 notes TEXT,
                             
                                 created_at TEXT,
                                 updated_at TEXT
                             )
            """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_svp_profiles_timestamp
                ON svp_profiles (timestamp)
                """
            )

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------
    def list_profiles(self, limit: int | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT
                p.*,
                COALESCE(pc.points_count, 0) AS points_count
            FROM svp_profiles p
            LEFT JOIN (
                SELECT svp_id, COUNT(*) AS points_count
                FROM svp_points
                GROUP BY svp_id
            ) pc ON pc.svp_id = p.id
            ORDER BY
                CASE WHEN p.timestamp IS NULL OR p.timestamp = '' THEN 1 ELSE 0 END,
                p.timestamp DESC,
                p.id DESC
        """
        params: list[Any] = []
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def get_profile(self, svp_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    p.*,
                    COALESCE(pc.points_count, 0) AS points_count
                FROM svp_profiles p
                LEFT JOIN (
                    SELECT svp_id, COUNT(*) AS points_count
                    FROM svp_points
                    GROUP BY svp_id
                ) pc ON pc.svp_id = p.id
                WHERE p.id = ?
                """,
                (svp_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_points(self, svp_id: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    id,
                    svp_id,
                    point_index,
                    depth_m,
                    velocity_mps,
                    temperature_c,
                    salinity_psu,
                    density_kgm3
                FROM svp_points
                WHERE svp_id = ?
                ORDER BY point_index ASC, depth_m ASC
                """,
                (svp_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_full_profile(self, svp_id: int) -> dict[str, Any] | None:
        profile = self.get_profile(svp_id)
        if not profile:
            return None
        profile["points"] = self.get_points(svp_id)
        return profile

    # ------------------------------------------------------------------
    # Create / Update / Delete
    # ------------------------------------------------------------------
    def create_profile(
        self,
        profile: dict[str, Any],
        points: list[dict[str, Any]],
    ) -> int:
        if not points:
            raise ValueError("SVP profile must contain at least one point.")

        now = self._utcnow_str()

        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO svp_profiles (
                    name,
                    profile_source,
                    file_type,
                    location,
                    instrument_make,
                    instrument_model,
                    serial,
                    rov,
                    timestamp,
                    latitude,
                    longitude,
                    coord_e,
                    coord_n,
                    casts,
                    surface_velocity,
                    mean_velocity,
                    seabed_velocity,
                    bottom_depth,
                    mean_density,
                    temperature_surface,
                    salinity_surface,
                    source_file_name,
                    source_file_path,
                    raw_header,
                    notes,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self._clean_text(profile.get("name")),
                    self._clean_text(profile.get("profile_source")),
                    self._clean_text(profile.get("file_type")),
                    self._clean_text(profile.get("location")),
                    self._clean_text(profile.get("instrument_make")),
                    self._clean_text(profile.get("instrument_model")),
                    self._clean_text(profile.get("serial")),
                    self._clean_text(profile.get("rov")),
                    self._parse_datetime_flexible(profile.get("timestamp")),
                    self._to_float(profile.get("latitude")),
                    self._to_float(profile.get("longitude")),
                    self._to_float(profile.get("coord_e")),
                    self._to_float(profile.get("coord_n")),
                    self._clean_text(profile.get("casts")),
                    self._to_float(profile.get("surface_velocity")),
                    self._to_float(profile.get("mean_velocity")),
                    self._to_float(profile.get("seabed_velocity")),
                    self._to_float(profile.get("bottom_depth")),
                    self._to_float(profile.get("mean_density")),
                    self._to_float(profile.get("temperature_surface")),
                    self._to_float(profile.get("salinity_surface")),
                    self._clean_text(profile.get("source_file_name")),
                    self._clean_text(profile.get("source_file_path")),
                    profile.get("raw_header"),
                    self._clean_text(profile.get("notes")),
                    now,
                    now,
                ),
            )
            svp_id = int(cur.lastrowid)
            self._insert_points(conn, svp_id, points)
            return svp_id

    def update_profile(
        self,
        svp_id: int,
        profile: dict[str, Any],
        points: list[dict[str, Any]] | None = None,
        replace_points: bool = False,
    ) -> None:
        now = self._utcnow_str()

        with self._connect() as conn:
            conn.execute(
                """
                UPDATE svp_profiles
                SET
                    name = ?,
                    profile_source = ?,
                    file_type = ?,
                    location = ?,
                    instrument_make = ?,
                    instrument_model = ?,
                    serial = ?,
                    rov = ?,
                    timestamp = ?,
                    latitude = ?,
                    longitude = ?,
                    coord_e = ?,
                    coord_n = ?,
                    casts = ?,
                    surface_velocity = ?,
                    mean_velocity = ?,
                    seabed_velocity = ?,
                    bottom_depth = ?,
                    mean_density = ?,
                    temperature_surface = ?,
                    salinity_surface = ?,
                    source_file_name = ?,
                    source_file_path = ?,
                    raw_header = ?,
                    notes = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    self._clean_text(profile.get("name")),
                    self._clean_text(profile.get("profile_source")),
                    self._clean_text(profile.get("file_type")),
                    self._clean_text(profile.get("location")),
                    self._clean_text(profile.get("instrument_make")),
                    self._clean_text(profile.get("instrument_model")),
                    self._clean_text(profile.get("serial")),
                    self._clean_text(profile.get("rov")),
                    self._parse_datetime_flexible(profile.get("timestamp")),
                    self._to_float(profile.get("latitude")),
                    self._to_float(profile.get("longitude")),
                    self._to_float(profile.get("coord_e")),
                    self._to_float(profile.get("coord_n")),
                    self._clean_text(profile.get("casts")),
                    self._to_float(profile.get("surface_velocity")),
                    self._to_float(profile.get("mean_velocity")),
                    self._to_float(profile.get("seabed_velocity")),
                    self._to_float(profile.get("bottom_depth")),
                    self._to_float(profile.get("mean_density")),
                    self._to_float(profile.get("temperature_surface")),
                    self._to_float(profile.get("salinity_surface")),
                    self._clean_text(profile.get("source_file_name")),
                    self._clean_text(profile.get("source_file_path")),
                    profile.get("raw_header"),
                    self._clean_text(profile.get("notes")),
                    now,
                    svp_id,
                ),
            )

            if replace_points:
                conn.execute("DELETE FROM svp_points WHERE svp_id = ?", (svp_id,))
                if points:
                    self._insert_points(conn, svp_id, points)

    def delete_profile(self, svp_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM svp_points WHERE svp_id = ?", (svp_id,))
            conn.execute("DELETE FROM svp_profiles WHERE id = ?", (svp_id,))

    def clone_profile(self, svp_id: int, new_name: str | None = None) -> int:
        full = self.get_full_profile(svp_id)
        if not full:
            raise ValueError(f"SVP profile id={svp_id} not found.")

        points = full.pop("points", [])
        full.pop("id", None)
        full.pop("points_count", None)
        full.pop("created_at", None)
        full.pop("updated_at", None)

        if new_name:
            full["name"] = new_name
        else:
            full["name"] = f"{full.get('name') or 'SVP'} (copy)"

        return self.create_profile(profile=full, points=points)

    def _insert_points(self, conn: sqlite3.Connection, svp_id: int, points: list[dict[str, Any]]) -> None:
        rows = []
        for idx, p in enumerate(points):
            depth = self._to_float(p.get("depth_m"))
            velocity = self._to_float(p.get("velocity_mps"))
            if depth is None or velocity is None:
                continue

            rows.append(
                (
                    svp_id,
                    idx,
                    depth,
                    velocity,
                    self._to_float(p.get("temperature_c")),
                    self._to_float(p.get("salinity_psu")),
                    self._to_float(p.get("density_kgm3")),
                    p.get("source_row_text"),
                )
            )

        if not rows:
            raise ValueError("No valid SVP points to insert.")

        conn.executemany(
            """
            INSERT INTO svp_points (
                svp_id,
                point_index,
                depth_m,
                velocity_mps,
                temperature_c,
                salinity_psu,
                density_kgm3,
                source_row_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    # ------------------------------------------------------------------
    # Import helpers
    # ------------------------------------------------------------------
    def import_file(
        self,
        file_path: str | Path,
        *,
        name: str | None = None,
        notes: str | None = None,
    ) -> int:
        path = Path(file_path)
        ext = path.suffix.lower()

        if ext == ".svp":
            parsed = self.parse_svp_text(path.read_text(encoding="utf-8", errors="ignore"))
        elif ext == ".000":
            parsed = self.parse_000_text(path.read_text(encoding="utf-8", errors="ignore"))
        elif ext in {".csv", ".txt"}:
            parsed = self.parse_csv_text(path.read_text(encoding="utf-8", errors="ignore"))
        else:
            raise ValueError(f"Unsupported SVP file type: {ext}")

        profile = parsed["profile"]
        points = parsed["points"]

        if name:
            profile["name"] = name
        if notes:
            profile["notes"] = notes

        profile["source_file_name"] = path.name
        profile["source_file_path"] = str(path)

        return self.create_profile(profile=profile, points=points)


    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------
    def parse_svp_text(self, text: str) -> dict[str, Any]:
        """
        Parse common SVP file format like your MIDAS SVX2 .svp sample.
        """
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        header_map: dict[str, str] = {}
        points: list[dict[str, Any]] = []

        header_line_idx: int | None = None

        for i, line in enumerate(lines):
            if line.startswith("[") and line.endswith("]") and "=" in line:
                inner = line[1:-1]
                key, value = inner.split("=", 1)
                header_map[key.strip()] = value.strip()
                continue

            if "Depth:Meter" in line and "Calculated Sound Velocity" in line:
                header_line_idx = i
                break

        if header_line_idx is None:
            raise ValueError("SVP file header row not found.")

        csv_header = [h.strip() for h in lines[header_line_idx].split(",")]

        for raw_line in lines[header_line_idx + 1 :]:
            parts = [p.strip() for p in raw_line.split(",")]
            if len(parts) < 2:
                continue

            row = dict(zip(csv_header, parts))
            depth = self._to_float(row.get("Depth:Meter"))
            vel = self._to_float(row.get("Calculated Sound Velocity:m/sec"))

            if depth is None or vel is None:
                continue

            points.append(
                {
                    "depth_m": depth,
                    "velocity_mps": vel,
                    "temperature_c": self._to_float(row.get("Temperature:C")),
                    "salinity_psu": self._to_float(row.get("Salinity:PSU")),
                    "density_kgm3": self._to_float(row.get("Density:kg/m^3")),
                    "source_row_text": raw_line,
                }
            )

        if not points:
            raise ValueError("No valid SVP points found in .svp file.")

        first = points[0]
        last = points[-1]

        velocities = [p["velocity_mps"] for p in points if p.get("velocity_mps") is not None]
        densities = [p["density_kgm3"] for p in points if p.get("density_kgm3") is not None]

        coord_e, coord_n = self._parse_coordinates_en(header_map.get("Coordinates"))

        profile = {
            "name": header_map.get("Name"),
            "profile_source": header_map.get("Profile Source"),
            "file_type": "svp",
            "location": header_map.get("Location"),
            "instrument_make": header_map.get("Instrument:Make"),
            "instrument_model": header_map.get("Instrument:Model"),
            "serial": header_map.get("Serial"),
            "rov": header_map.get("ROV"),
            "timestamp": None,
            "latitude": self._to_float(header_map.get("Latitude")),
            "longitude": self._to_float(header_map.get("Longitude")),
            "coord_e": coord_e,
            "coord_n": coord_n,
            "casts": header_map.get("Casts"),
            "surface_velocity": first.get("velocity_mps"),
            "mean_velocity": (sum(velocities) / len(velocities)) if velocities else None,
            "seabed_velocity": last.get("velocity_mps"),
            "bottom_depth": last.get("depth_m"),
            "mean_density": (sum(densities) / len(densities)) if densities else None,
            "temperature_surface": first.get("temperature_c"),
            "salinity_surface": first.get("salinity_psu"),
            "raw_header": "\n".join(lines[: header_line_idx + 1]),
            "notes": None,
        }

        return {"profile": profile, "points": points}

    def parse_000_text(self, text: str) -> dict[str, Any]:
        """
        Parse MIDAS .000 raw log.
        Uses Calc. SOUND VELOCITY and PRESSURE/DEPTH.
        """
        lines = [ln.rstrip("\n") for ln in text.splitlines() if ln.strip()]
        meta: dict[str, str] = {}

        table_header_idx: int | None = None
        table_header: list[str] = []

        for i, line in enumerate(lines):
            if "\t" in line and not line.startswith("Date / Time"):
                key, value = line.split("\t", 1)
                meta[key.strip(" :")] = value.strip()
                continue

            if line.startswith("Date / Time"):
                table_header_idx = i
                table_header = [c.strip() for c in line.split("\t")]
                break

        if table_header_idx is None:
            raise ValueError("No data table found in .000 file.")

        points: list[dict[str, Any]] = []
        timestamp_first: str | None = None

        for raw_line in lines[table_header_idx + 1 :]:
            parts = [c.strip() for c in raw_line.split("\t")]
            if len(parts) != len(table_header):
                continue

            row = dict(zip(table_header, parts))

            if timestamp_first is None:
                timestamp_first = row.get("Date / Time")

            depth = self._to_float(row.get("PRESSURE;M"))
            velocity = self._to_float(row.get("Calc. SOUND VELOCITY;M/SEC"))

            if depth is None or velocity is None:
                continue

            if depth < 0:
                depth = 0.0

            points.append(
                {
                    "depth_m": depth,
                    "velocity_mps": velocity,
                    "temperature_c": self._to_float(row.get("TEMPERATURE;C")),
                    "salinity_psu": self._to_float(row.get("Calc. SALINITY;PSU")),
                    "density_kgm3": self._to_float(row.get("Calc. DENSITY;KG/M3")),
                    "source_row_text": raw_line,
                }
            )

        if not points:
            raise ValueError("No valid points found in .000 file.")

        points.sort(key=lambda x: (x["depth_m"], x["velocity_mps"]))

        first = points[0]
        last = points[-1]
        velocities = [p["velocity_mps"] for p in points if p.get("velocity_mps") is not None]
        densities = [p["density_kgm3"] for p in points if p.get("density_kgm3") is not None]

        profile = {
            "name": meta.get("Site Information") or meta.get("File Name"),
            "profile_source": "Raw",
            "file_type": "000",
            "location": meta.get("Site Information"),
            "instrument_make": None,
            "instrument_model": meta.get("Model Name"),
            "serial": meta.get("Serial No."),
            "rov": None,
            "timestamp": self._parse_datetime_flexible(meta.get("Time Stamp") or timestamp_first),
            "latitude": None,
            "longitude": None,
            "coord_e": None,
            "coord_n": None,
            "casts": None,
            "surface_velocity": first.get("velocity_mps"),
            "mean_velocity": (sum(velocities) / len(velocities)) if velocities else None,
            "seabed_velocity": last.get("velocity_mps"),
            "bottom_depth": last.get("depth_m"),
            "mean_density": (sum(densities) / len(densities)) if densities else None,
            "temperature_surface": first.get("temperature_c"),
            "salinity_surface": first.get("salinity_psu"),
            "raw_header": "\n".join(lines[: table_header_idx + 1]),
            "notes": None,
        }

        return {"profile": profile, "points": points}

    def parse_csv_text(self, text: str) -> dict[str, Any]:
        """
        Generic CSV/TXT parser.
        Accepts headers like:
          depth, velocity
          depth_m, sound_velocity
          z, sv
        """
        stream = io.StringIO(text)
        sample = stream.read(4096)
        stream.seek(0)

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except Exception:
            dialect = csv.excel

        reader = csv.DictReader(stream, dialect=dialect)
        if not reader.fieldnames:
            raise ValueError("CSV header not found.")

        field_map = {name.strip().lower(): name for name in reader.fieldnames}

        depth_key = self._find_any(
            field_map,
            ["depth", "depth_m", "z", "dep", "pressure", "pressure_m"],
        )
        vel_key = self._find_any(
            field_map,
            ["velocity", "velocity_mps", "soundvelocity", "sound_velocity", "sv", "vel"],
        )
        temp_key = self._find_any(field_map, ["temperature", "temperature_c", "temp", "t"])
        sal_key = self._find_any(field_map, ["salinity", "salinity_psu", "psu"])
        dens_key = self._find_any(field_map, ["density", "density_kgm3"])

        if not depth_key or not vel_key:
            raise ValueError("CSV must contain depth and velocity columns.")

        points: list[dict[str, Any]] = []

        for row in reader:
            depth = self._to_float(row.get(depth_key))
            velocity = self._to_float(row.get(vel_key))
            if depth is None or velocity is None:
                continue

            points.append(
                {
                    "depth_m": depth,
                    "velocity_mps": velocity,
                    "temperature_c": self._to_float(row.get(temp_key)) if temp_key else None,
                    "salinity_psu": self._to_float(row.get(sal_key)) if sal_key else None,
                    "density_kgm3": self._to_float(row.get(dens_key)) if dens_key else None,
                    "source_row_text": str(row),
                }
            )

        if not points:
            raise ValueError("No valid points found in CSV/TXT.")

        points.sort(key=lambda x: x["depth_m"])
        first = points[0]
        last = points[-1]
        velocities = [p["velocity_mps"] for p in points]
        densities = [p["density_kgm3"] for p in points if p.get("density_kgm3") is not None]

        profile = {
            "name": "Imported CSV SVP",
            "profile_source": "Imported",
            "file_type": "csv",
            "location": None,
            "instrument_make": None,
            "instrument_model": None,
            "serial": None,
            "rov": None,
            "timestamp": None,
            "latitude": None,
            "longitude": None,
            "coord_e": None,
            "coord_n": None,
            "casts": None,
            "surface_velocity": first.get("velocity_mps"),
            "mean_velocity": (sum(velocities) / len(velocities)) if velocities else None,
            "seabed_velocity": last.get("velocity_mps"),
            "bottom_depth": last.get("depth_m"),
            "mean_density": (sum(densities) / len(densities)) if densities else None,
            "temperature_surface": first.get("temperature_c"),
            "salinity_surface": first.get("salinity_psu"),
            "raw_header": ",".join(reader.fieldnames),
            "notes": None,
        }

        return {"profile": profile, "points": points}

    @staticmethod
    def _find_any(field_map: dict[str, str], candidates: list[str]) -> str | None:
        for c in candidates:
            if c in field_map:
                return field_map[c]
        return None

    @staticmethod
    def _parse_coordinates_en(value: str | None) -> tuple[float | None, float | None]:
        """
        Parses strings like:
            'E 368720.33, N 3038790.54'
        """
        if not value:
            return None, None

        try:
            cleaned = value.replace(",", " ").replace("E", "").replace("N", "").split()
            nums = [float(x) for x in cleaned if x.replace(".", "", 1).replace("-", "", 1).isdigit()]
            if len(nums) >= 2:
                return nums[0], nums[1]
        except Exception:
            pass
        return None, None

    # ------------------------------------------------------------------
    # Optional helpers
    # ------------------------------------------------------------------
    def find_profile_for_timestamp(self, ts: str) -> dict[str, Any] | None:
        """
        Returns latest SVP profile at or before given timestamp.
        """
        ts_norm = self._parse_datetime_flexible(ts)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    p.*,
                    COALESCE(pc.points_count, 0) AS points_count
                FROM svp_profiles p
                LEFT JOIN (
                    SELECT svp_id, COUNT(*) AS points_count
                    FROM svp_points
                    GROUP BY svp_id
                ) pc ON pc.svp_id = p.id
                WHERE p.timestamp IS NOT NULL
                  AND p.timestamp <= ?
                ORDER BY p.timestamp DESC
                LIMIT 1
                """,
                (ts_norm,),
            ).fetchone()
            return dict(row) if row else None

    def delete_all_profiles(self) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM svp_points")
            conn.execute("DELETE FROM svp_profiles")

    def import_uploaded_file(self, file_obj, *, file_name=None, name=None, notes=None, config_id=None) -> int:
        raw = file_obj.read()
        text = raw.decode("utf-8", errors="ignore") if isinstance(raw, bytes) else str(raw)

        detected_name = file_name or getattr(file_obj, "name", None) or "uploaded_file"

        if config_id:
            cfg = self.get_format_config(int(config_id))
            if not cfg:
                raise ValueError(f"Config id={config_id} not found.")

            from .svp_format_setup import SVPFormatSetup
            from .svp_parser import SVPParser

            setup = SVPFormatSetup(
                format_name=cfg.get("name") or "saved_config",
                file_ext=cfg.get("file_ext"),
                delimiter=cfg.get("delimiter"),
                header_line_count=cfg.get("header_line_count"),
                data_header_line_index=cfg.get("data_header_line_index"),
                data_start_line_index=cfg.get("data_start_line_index"),
                meta_coordinates_key=cfg.get("meta_coordinates_key"),
                meta_lat_key=cfg.get("meta_lat_key"),
                meta_lon_key=cfg.get("meta_lon_key"),
                meta_rov_key=cfg.get("meta_rov_key"),
                meta_timestamp_key=cfg.get("meta_timestamp_key"),
                meta_name_key=cfg.get("meta_name_key"),
                meta_location_key=cfg.get("meta_location_key"),
                meta_serial_key=cfg.get("meta_serial_key"),
                meta_make_key=cfg.get("meta_make_key"),
                meta_model_key=cfg.get("meta_model_key"),
                col_timestamp=cfg.get("col_timestamp"),
                col_depth=cfg.get("col_depth"),
                col_velocity=cfg.get("col_velocity"),
                col_temperature=cfg.get("col_temperature"),
                col_salinity=cfg.get("col_salinity"),
                col_density=cfg.get("col_density"),
                sort_by_depth=bool(cfg.get("sort_by_depth")),
                clamp_negative_depth_to_zero=bool(cfg.get("clamp_negative_depth_to_zero")),
                pressure_is_depth=bool(cfg.get("pressure_is_depth")),
            )

            parsed = SVPParser.parse(text, setup)
        else:
            from .svp_parser import SVPParser
            setup = SVPParser.detect_setup(text, detected_name)
            parsed = SVPParser.parse(text, setup)

        profile = parsed["profile"]
        points = parsed["points"]

        if name:
            profile["name"] = name
        if notes:
            profile["notes"] = notes

        profile["source_file_name"] = detected_name

        return self.create_profile(profile=profile, points=points)



    def list_format_configs(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM svp_format_setups
                ORDER BY id DESC
                """
            ).fetchall()
            return [dict(r) for r in rows]

    def export_format_config_to_json(self, config_id: int) -> str:
        row = self.get_format_config(config_id)
        if not row:
            raise ValueError(f"SVP config id={config_id} not found.")

        row.pop("id", None)
        row.pop("created_at", None)
        row.pop("updated_at", None)

        payload = {
            "config_name": row.get("name"),
            "file_ext": row.get("file_ext"),
            "delimiter": row.get("delimiter"),
            "header_line_count": row.get("header_line_count"),
            "data_header_line_index": row.get("data_header_line_index"),
            "data_start_line_index": row.get("data_start_line_index"),
            "meta_coordinates_key": row.get("meta_coordinates_key"),
            "meta_lat_key": row.get("meta_lat_key"),
            "meta_lon_key": row.get("meta_lon_key"),
            "meta_rov_key": row.get("meta_rov_key"),
            "meta_timestamp_key": row.get("meta_timestamp_key"),
            "meta_name_key": row.get("meta_name_key"),
            "meta_location_key": row.get("meta_location_key"),
            "meta_serial_key": row.get("meta_serial_key"),
            "meta_make_key": row.get("meta_make_key"),
            "meta_model_key": row.get("meta_model_key"),
            "col_timestamp": row.get("col_timestamp"),
            "col_depth": row.get("col_depth"),
            "col_velocity": row.get("col_velocity"),
            "col_temperature": row.get("col_temperature"),
            "col_salinity": row.get("col_salinity"),
            "col_density": row.get("col_density"),
            "sort_by_depth": bool(row.get("sort_by_depth")),
            "clamp_negative_depth_to_zero": bool(row.get("clamp_negative_depth_to_zero")),
            "pressure_is_depth": bool(row.get("pressure_is_depth")),
            "notes": row.get("notes"),
        }

        return json.dumps(payload, indent=2, ensure_ascii=False)

    def import_format_config_from_json(self, json_text: str) -> int:
        try:
            payload = json.loads(json_text)
        except Exception as exc:
            raise ValueError(f"Invalid JSON: {exc}")

        if not isinstance(payload, dict):
            raise ValueError("JSON root must be an object.")

        return self.save_format_config(payload)

    def import_format_config_uploaded_file(self, file_obj) -> int:
        raw = file_obj.read()
        text = raw.decode("utf-8", errors="ignore") if isinstance(raw, bytes) else str(raw)
        return self.import_format_config_from_json(text)

    def save_format_config(self, cfg: dict) -> int:
        now = self._utcnow_str()

        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO svp_format_setups (
                    name,
                    file_ext,
                    delimiter,
                    header_line_count,
                    data_header_line_index,
                    data_start_line_index,
                    meta_coordinates_key,
                    meta_lat_key,
                    meta_lon_key,
                    meta_rov_key,
                    meta_timestamp_key,
                    meta_name_key,
                    meta_location_key,
                    meta_serial_key,
                    meta_make_key,
                    meta_model_key,
                    col_timestamp,
                    col_depth,
                    col_velocity,
                    col_temperature,
                    col_salinity,
                    col_density,
                    sort_by_depth,
                    clamp_negative_depth_to_zero,
                    pressure_is_depth,
                    notes,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cfg.get("config_name") or cfg.get("name"),
                    cfg.get("file_ext"),
                    cfg.get("delimiter"),
                    self._to_int(cfg.get("header_line_count")),
                    self._to_int(cfg.get("data_header_line_index")),
                    self._to_int(cfg.get("data_start_line_index")),
                    cfg.get("meta_coordinates_key"),
                    cfg.get("meta_lat_key"),
                    cfg.get("meta_lon_key"),
                    cfg.get("meta_rov_key"),
                    cfg.get("meta_timestamp_key"),
                    cfg.get("meta_name_key"),
                    cfg.get("meta_location_key"),
                    cfg.get("meta_serial_key"),
                    cfg.get("meta_make_key"),
                    cfg.get("meta_model_key"),
                    cfg.get("col_timestamp"),
                    cfg.get("col_depth"),
                    cfg.get("col_velocity"),
                    cfg.get("col_temperature"),
                    cfg.get("col_salinity"),
                    cfg.get("col_density"),
                    int(bool(cfg.get("sort_by_depth"))),
                    int(bool(cfg.get("clamp_negative_depth_to_zero"))),
                    int(bool(cfg.get("pressure_is_depth"))),
                    cfg.get("notes"),
                    now,
                    now,
                ),
            )
            return int(cur.lastrowid)

    def get_format_config(self, config_id: int) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM svp_format_setups
                WHERE id = ?
                """,
                (config_id,),
            ).fetchone()
            return dict(row) if row else None

    def delete_format_config(self, config_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM svp_format_setups
                WHERE id = ?
                """,
                (config_id,),
            )

    def export_format_config_to_json(self, config_id: int) -> str:
        row = self.get_format_config(config_id)
        if not row:
            raise ValueError(f"Config id={config_id} not found.")

        # remove DB-only fields
        row.pop("id", None)
        row.pop("created_at", None)
        row.pop("updated_at", None)

        # convert DB structure → clean JSON structure
        payload = {
            "config_name": row.get("name"),
            "file_ext": row.get("file_ext"),
            "delimiter": row.get("delimiter"),
            "header_line_count": row.get("header_line_count"),
            "data_header_line_index": row.get("data_header_line_index"),
            "data_start_line_index": row.get("data_start_line_index"),

            "meta_coordinates_key": row.get("meta_coordinates_key"),
            "meta_lat_key": row.get("meta_lat_key"),
            "meta_lon_key": row.get("meta_lon_key"),
            "meta_rov_key": row.get("meta_rov_key"),
            "meta_timestamp_key": row.get("meta_timestamp_key"),
            "meta_name_key": row.get("meta_name_key"),
            "meta_location_key": row.get("meta_location_key"),
            "meta_serial_key": row.get("meta_serial_key"),
            "meta_make_key": row.get("meta_make_key"),
            "meta_model_key": row.get("meta_model_key"),

            "col_timestamp": row.get("col_timestamp"),
            "col_depth": row.get("col_depth"),
            "col_velocity": row.get("col_velocity"),
            "col_temperature": row.get("col_temperature"),
            "col_salinity": row.get("col_salinity"),
            "col_density": row.get("col_density"),

            "sort_by_depth": bool(row.get("sort_by_depth")),
            "clamp_negative_depth_to_zero": bool(row.get("clamp_negative_depth_to_zero")),
            "pressure_is_depth": bool(row.get("pressure_is_depth")),

            "notes": row.get("notes"),
        }

        return json.dumps(payload, indent=2, ensure_ascii=False)