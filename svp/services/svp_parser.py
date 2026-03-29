from __future__ import annotations

import csv
import io
from pathlib import Path

from .svp_format_setup import SVPFormatSetup
from datetime import datetime

class SVPParser:
    # ... detect_setup methods above ...
    @staticmethod
    def detect_setup(text: str, file_name: str | None = None) -> SVPFormatSetup:
        lines = [ln.rstrip("\n") for ln in text.splitlines()]

        ext = Path(file_name or "").suffix.lower()

        if ext == ".svp":
            return SVPParser._detect_svp_setup(lines)

        if ext == ".000":
            return SVPParser._detect_000_setup(lines)

        if ext in {".csv", ".txt"}:
            return SVPParser._detect_csv_setup(lines, ext)

        return SVPFormatSetup(format_name="unknown", file_ext=ext or None)

    @staticmethod
    def _detect_svp_setup(lines: list[str]) -> SVPFormatSetup:
        data_header_idx = None
        for i, line in enumerate(lines):
            if "Depth:Meter" in line and "Calculated Sound Velocity" in line:
                data_header_idx = i
                break

        header_count = data_header_idx if data_header_idx is not None else 0

        return SVPFormatSetup(
            format_name="svx2_svp",
            file_ext=".svp",
            header_line_count=header_count,
            data_header_line_index=data_header_idx,
            data_start_line_index=(data_header_idx + 1) if data_header_idx is not None else None,
            delimiter=",",
            meta_name_key="Name",
            meta_location_key="Location",
            meta_rov_key="ROV",
            meta_serial_key="Serial",
            meta_make_key="Instrument:Make",
            meta_model_key="Instrument:Model",
            meta_lat_key="Latitude",
            meta_coordinates_key="Coordinates",
            col_depth="Depth:Meter",
            col_velocity="Calculated Sound Velocity:m/sec",
            col_temperature="Temperature:C",
            col_salinity="Salinity:PSU",
            col_density="Density:kg/m^3",
        )

    @staticmethod
    def _detect_000_setup(lines: list[str]) -> SVPFormatSetup:
        data_header_idx = None
        for i, line in enumerate(lines):
            if line.startswith("Date / Time"):
                data_header_idx = i
                break

        header_count = data_header_idx if data_header_idx is not None else 0

        return SVPFormatSetup(
            format_name="svx2_000",
            file_ext=".000",
            header_line_count=header_count,
            data_header_line_index=data_header_idx,
            data_start_line_index=(data_header_idx + 1) if data_header_idx is not None else None,
            delimiter="\t",
            meta_location_key="Site Information",
            meta_timestamp_key="Time Stamp",
            meta_serial_key="Serial No.",
            meta_model_key="Model Name",
            col_timestamp="Date / Time",
            col_depth="PRESSURE;M",
            col_velocity="Calc. SOUND VELOCITY;M/SEC",
            col_temperature="TEMPERATURE;C",
            col_salinity="Calc. SALINITY;PSU",
            col_density="Calc. DENSITY;KG/M3",
        )

    @staticmethod
    def _detect_csv_setup(lines: list[str], ext: str) -> SVPFormatSetup:
        sample = "\n".join(lines[:5])

        delimiter = ","
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
            delimiter = dialect.delimiter
        except Exception:
            pass

        header_line = lines[0] if lines else ""
        headers = [h.strip() for h in header_line.split(delimiter)]

        lower_map = {h.lower(): h for h in headers}

        def pick(*names):
            for n in names:
                if n in lower_map:
                    return lower_map[n]
            return None

        return SVPFormatSetup(
            format_name="generic_csv",
            file_ext=ext,
            header_line_count=1,
            data_header_line_index=0,
            data_start_line_index=1,
            delimiter=delimiter,
            col_depth=pick("depth", "depth_m", "z", "dep", "pressure", "pressure_m"),
            col_velocity=pick("velocity", "velocity_mps", "sound_velocity", "soundvelocity", "sv", "vel"),
            col_temperature=pick("temperature", "temperature_c", "temp"),
            col_salinity=pick("salinity", "salinity_psu", "psu"),
            col_density=pick("density", "density_kgm3"),
        )
    @staticmethod
    @staticmethod
    def parse(text: str, setup: SVPFormatSetup) -> dict:
        parser = (getattr(setup, "parser_type", None) or "").lower()
        ext = (setup.file_ext or "").lower()

        if parser:
            if parser == "svx2_svp":
                return SVPParser._parse_svp(text, setup)
            if parser == "svx2_000":
                return SVPParser._parse_000(text, setup)
            if parser == "generic_csv":
                return SVPParser._parse_csv(text, setup)

        # fallback by extension
        if ext == ".svp":
            return SVPParser._parse_svp(text, setup)
        if ext == ".000":
            return SVPParser._parse_000(text, setup)

        return SVPParser._parse_csv(text, setup)

    @staticmethod
    def _parse_header_pairs(lines: list[str]) -> dict[str, str]:
        meta = {}
        for line in lines:
            s = line.strip()
            if not s:
                continue

            if s.startswith("[") and s.endswith("]") and "=" in s:
                inner = s[1:-1]
                key, value = inner.split("=", 1)
                meta[key.strip()] = value.strip()
                continue

            if "\t" in s and ":" in s:
                key, value = s.split("\t", 1)
                meta[key.replace(" :", "").strip()] = value.strip()
                continue

        return meta

    @staticmethod
    def _parse_svp(text: str, setup: SVPFormatSetup) -> dict:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        header_lines = lines[: setup.header_line_count or 0]
        meta = SVPParser._parse_header_pairs(header_lines)

        header = [h.strip() for h in lines[setup.data_header_line_index].split(setup.delimiter)]
        points = []

        for raw in lines[setup.data_start_line_index :]:
            vals = [v.strip() for v in raw.split(setup.delimiter)]
            if len(vals) != len(header):
                continue

            row = dict(zip(header, vals))
            depth = _to_float(row.get(setup.col_depth))
            velocity = _to_float(row.get(setup.col_velocity))
            if depth is None or velocity is None:
                continue

            points.append({
                "depth_m": depth,
                "velocity_mps": velocity,
                "temperature_c": _to_float(row.get(setup.col_temperature)),
                "salinity_psu": _to_float(row.get(setup.col_salinity)),
                "density_kgm3": _to_float(row.get(setup.col_density)),
                "source_row_text": raw,
            })

        e, n = _parse_en(meta.get(setup.meta_coordinates_key))
        lat = _to_float(meta.get(setup.meta_lat_key))

        profile = {
            "name": meta.get(setup.meta_name_key),
            "location": meta.get(setup.meta_location_key),
            "rov": meta.get(setup.meta_rov_key),
            "serial": meta.get(setup.meta_serial_key),
            "instrument_make": meta.get(setup.meta_make_key),
            "instrument_model": meta.get(setup.meta_model_key),
            "latitude": lat,
            "longitude": None,
            "coord_e": e,
            "coord_n": n,
            "profile_source": "Processed",
            "file_type": "svp",
            "raw_header": "\n".join(header_lines),
        }
        return {"profile": _finalize_profile(profile, points), "points": points, "setup": setup.to_dict()}

    @staticmethod
    def _parse_000(text: str, setup: SVPFormatSetup) -> dict:
        lines = [ln.rstrip("\n") for ln in text.splitlines() if ln.strip()]
        header_lines = lines[: setup.header_line_count or 0]
        meta = SVPParser._parse_header_pairs(header_lines)

        header = [h.strip() for h in lines[setup.data_header_line_index].split(setup.delimiter)]
        points = []
        first_ts = None

        for raw in lines[setup.data_start_line_index :]:
            vals = [v.strip() for v in raw.split(setup.delimiter)]
            if len(vals) != len(header):
                continue

            row = dict(zip(header, vals))
            if first_ts is None:
                first_ts = row.get(setup.col_timestamp)

            depth = _to_float(row.get(setup.col_depth))
            velocity = _to_float(row.get(setup.col_velocity))
            if depth is None or velocity is None:
                continue

            if setup.clamp_negative_depth_to_zero and depth < 0:
                depth = 0.0

            points.append({
                "depth_m": depth,
                "velocity_mps": velocity,
                "temperature_c": _to_float(row.get(setup.col_temperature)),
                "salinity_psu": _to_float(row.get(setup.col_salinity)),
                "density_kgm3": _to_float(row.get(setup.col_density)),
                "source_row_text": raw,
            })

        if setup.sort_by_depth:
            points.sort(key=lambda x: x["depth_m"])

        profile = {
            "name": meta.get("Site Information") or meta.get("File Name"),
            "location": meta.get(setup.meta_location_key),
            "rov": None,
            "serial": meta.get(setup.meta_serial_key),
            "instrument_make": None,
            "instrument_model": meta.get(setup.meta_model_key),
            "timestamp": _normalize_dt(meta.get(setup.meta_timestamp_key) or first_ts),
            "profile_source": "Raw",
            "file_type": "000",
            "raw_header": "\n".join(header_lines),
        }
        return {"profile": _finalize_profile(profile, points), "points": points, "setup": setup.to_dict()}

    @staticmethod
    def _parse_csv(text: str, setup: SVPFormatSetup) -> dict:
        stream = io.StringIO(text)
        reader = csv.DictReader(stream, delimiter=setup.delimiter or ",")

        points = []
        for row in reader:
            depth = _to_float(row.get(setup.col_depth))
            velocity = _to_float(row.get(setup.col_velocity))
            if depth is None or velocity is None:
                continue

            points.append({
                "depth_m": depth,
                "velocity_mps": velocity,
                "temperature_c": _to_float(row.get(setup.col_temperature)),
                "salinity_psu": _to_float(row.get(setup.col_salinity)),
                "density_kgm3": _to_float(row.get(setup.col_density)),
                "source_row_text": str(row),
            })

        if setup.sort_by_depth:
            points.sort(key=lambda x: x["depth_m"])

        profile = {
            "name": "Imported CSV SVP",
            "profile_source": "Imported",
            "file_type": "csv",
            "raw_header": ",".join(reader.fieldnames or []),
        }
        return {"profile": _finalize_profile(profile, points), "points": points, "setup": setup.to_dict()}


def _to_float(value):
        try:
            if value is None:
                return None
            s = str(value).strip()
            if not s:
                return None
            return float(s)
        except Exception:
            return None
def _normalize_dt(value):
        if not value:
            return None
        s = str(value).strip()
        for fmt in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        return s
def _parse_en(value):
        if not value:
            return None, None
        s = value.replace(",", " ").replace("E", "").replace("N", "")
        nums = []
        for part in s.split():
            try:
                nums.append(float(part))
            except Exception:
                pass
        if len(nums) >= 2:
            return nums[0], nums[1]
        return None, None
def _finalize_profile(profile: dict, points: list[dict]) -> dict:
        if not points:
            raise ValueError("No valid SVP points found.")

        first = points[0]
        last = points[-1]
        velocities = [p["velocity_mps"] for p in points if p.get("velocity_mps") is not None]
        densities = [p["density_kgm3"] for p in points if p.get("density_kgm3") is not None]

        profile["surface_velocity"] = first.get("velocity_mps")
        profile["seabed_velocity"] = last.get("velocity_mps")
        profile["bottom_depth"] = last.get("depth_m")
        profile["temperature_surface"] = first.get("temperature_c")
        profile["salinity_surface"] = first.get("salinity_psu")
        profile["mean_velocity"] = sum(velocities) / len(velocities) if velocities else None
        profile["mean_density"] = sum(densities) / len(densities) if densities else None
        return profile