from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from bokeh.embed import components
from bokeh.models import ColorBar, ColumnDataSource, HoverTool, LinearAxis, LinearColorMapper, Range1d, Span
from bokeh.palettes import Category10, Turbo256
from bokeh.plotting import figure
from bokeh.resources import INLINE


COMPARISONS = {
    "dep_primary_preplot": ("Deployment Primary vs RPPreplot", "PrimaryEasting", "PrimaryNorthing", "PrimaryElevation", "RPPreplotEasting", "RPPreplotNorthing", "RPPreplotElevation"),
    "dep_secondary_preplot": ("Deployment Secondary vs RPPreplot", "SecondaryEasting", "SecondaryNorthing", "SecondaryElevation", "RPPreplotEasting", "RPPreplotNorthing", "RPPreplotElevation"),
    "dep_primary_secondary": ("Deployment Primary → Secondary", "SecondaryEasting", "SecondaryNorthing", "SecondaryElevation", "PrimaryEasting", "PrimaryNorthing", "PrimaryElevation"),
    "rec_primary_preplot": ("Recovery Primary vs RPPreplot", "PrimaryEasting1", "PrimaryNorthing1", "PrimaryElevation1", "RPPreplotEasting", "RPPreplotNorthing", "RPPreplotElevation"),
    "rec_secondary_preplot": ("Recovery Secondary vs RPPreplot", "SecondaryEasting1", "SecondaryNorthing1", "SecondaryElevation1", "RPPreplotEasting", "RPPreplotNorthing", "RPPreplotElevation"),
    "rec_primary_secondary": ("Recovery Primary → Secondary", "SecondaryEasting1", "SecondaryNorthing1", "SecondaryElevation1", "PrimaryEasting1", "PrimaryNorthing1", "PrimaryElevation1"),
    "dep_primary_rec_primary": ("Deployment Primary → Recovery Primary", "PrimaryEasting1", "PrimaryNorthing1", "PrimaryElevation1", "PrimaryEasting", "PrimaryNorthing", "PrimaryElevation"),
    "dep_secondary_rec_secondary": ("Deployment Secondary → Recovery Secondary", "SecondaryEasting1", "SecondaryNorthing1", "SecondaryElevation1", "SecondaryEasting", "SecondaryNorthing", "SecondaryElevation"),
    "recdb_preplot": ("REC_DB vs RPPreplot", "REC_X", "REC_Y", "REC_Z", "RPPreplotEasting", "RPPreplotNorthing", "RPPreplotElevation"),
    "recdb_dep_primary": ("REC_DB → Deployment Primary", "REC_X", "REC_Y", "REC_Z", "PrimaryEasting", "PrimaryNorthing", "PrimaryElevation"),
    "recdb_dep_secondary": ("REC_DB → Deployment Secondary", "REC_X", "REC_Y", "REC_Z", "SecondaryEasting", "SecondaryNorthing", "SecondaryElevation"),
    "recdb_rec_primary": ("REC_DB → Recovery Primary", "REC_X", "REC_Y", "REC_Z", "PrimaryEasting1", "PrimaryNorthing1", "PrimaryElevation1"),
    "recdb_rec_secondary": ("REC_DB → Recovery Secondary", "REC_X", "REC_Y", "REC_Z", "SecondaryEasting1", "SecondaryNorthing1", "SecondaryElevation1"),
}

COMPARISON_CONTEXT = {
    "dep_primary_preplot": {"phase": "Deployment", "rov": "ROV", "sigma_e": "Sigma", "sigma_n": "Sigma1"},
    "dep_secondary_preplot": {"phase": "Deployment", "rov": "ROV", "sigma_e": "Sigma2", "sigma_n": "Sigma3"},
    "dep_primary_secondary": {"phase": "Deployment", "rov": "ROV", "sigma_e": None, "sigma_n": None},
    "rec_primary_preplot": {"phase": "Recovery", "rov": "ROV1", "sigma_e": "Sigma6", "sigma_n": "Sigma7"},
    "rec_secondary_preplot": {"phase": "Recovery", "rov": "ROV1", "sigma_e": "Sigma8", "sigma_n": "Sigma9"},
    "rec_primary_secondary": {"phase": "Recovery", "rov": "ROV1", "sigma_e": None, "sigma_n": None},
    "dep_primary_rec_primary": {"phase": "Deployment vs Recovery", "rov": None, "sigma_e": None, "sigma_n": None},
    "dep_secondary_rec_secondary": {"phase": "Deployment vs Recovery", "rov": None, "sigma_e": None, "sigma_n": None},
    "recdb_preplot": {"phase": "REC_DB", "rov": None, "sigma_e": None, "sigma_n": None},
    "recdb_dep_primary": {"phase": "REC_DB vs Deployment", "rov": "ROV", "sigma_e": None, "sigma_n": None},
    "recdb_dep_secondary": {"phase": "REC_DB vs Deployment", "rov": "ROV", "sigma_e": None, "sigma_n": None},
    "recdb_rec_primary": {"phase": "REC_DB vs Recovery", "rov": "ROV1", "sigma_e": None, "sigma_n": None},
    "recdb_rec_secondary": {"phase": "REC_DB vs Recovery", "rov": "ROV1", "sigma_e": None, "sigma_n": None},
}


def _num(series):
    return pd.to_numeric(series, errors="coerce")


def _json_number(value, digits=3):
    if value is None or not np.isfinite(value):
        return None
    return round(float(value), digits)


def _dominant_sector(bearings, width=45):
    values = pd.Series(bearings).dropna()
    if values.empty:
        return "—"
    edges = np.arange(0, 360 + width, width)
    counts, _ = np.histogram(values % 360, bins=edges)
    start = int(edges[int(np.argmax(counts))])
    end = (start + width) % 360
    names = {0: "N", 45: "NE", 90: "E", 135: "SE", 180: "S", 225: "SW", 270: "W", 315: "NW"}
    return f"{names.get(start, '')} ({start}°–{end}°)"


@dataclass
class ReceiverStatistics:
    db_path: Path | str
    filters: dict

    @staticmethod
    def available_lines(db_path):
        try:
            with sqlite3.connect(str(db_path)) as conn:
                return [row[0] for row in conn.execute("SELECT DISTINCT Line FROM DSR WHERE Line IS NOT NULL ORDER BY Line")]
        except sqlite3.Error:
            return []

    def _tables(self):
        with sqlite3.connect(str(self.db_path)) as conn:
            names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
        missing = {"DSR", "REC_DB", "RPPreplot"} - names
        if missing:
            raise ValueError("Missing project table(s): " + ", ".join(sorted(missing)))
        return names

    def load(self):
        self._tables()
        clauses, params = ["1=1"], []
        f = self.filters
        lines = [int(line) for line in f.get("lines", [])]
        if lines:
            clauses.append("d.Line IN (" + ",".join("?" for _ in lines) + ")")
            params.extend(lines)
        for key, expression, op in (
            ("station_from", "d.Station", ">="), ("station_to", "d.Station", "<="),
        ):
            if f.get(key) is not None:
                clauses.append(f"{expression} {op} ?")
                params.append(f[key])
        if f.get("deployment_rov"):
            clauses.append("TRIM(d.ROV) = TRIM(?)")
            params.append(f["deployment_rov"])
        if f.get("recovery_rov"):
            clauses.append("TRIM(d.ROV1) = TRIM(?)")
            params.append(f["recovery_rov"])

        basis = f.get("time_basis") or "either"
        phase_fields = [("d.Day", "d.Week", "d.Month", "d.Year", "d.TimeStamp")] if basis == "deployment" else [("d.Day1", "d.Week1", "d.Month1", "d.Year1", "d.TimeStamp1")] if basis == "recovery" else [("d.Day", "d.Week", "d.Month", "d.Year", "d.TimeStamp"), ("d.Day1", "d.Week1", "d.Month1", "d.Year1", "d.TimeStamp1")]
        period_type = f.get("period_type") or "all"
        period_tests = []
        for day_col, week_col, month_col, year_col, timestamp_col in phase_fields:
            if period_type == "day" and f.get("day"):
                period_tests.append(f"date(COALESCE(NULLIF({day_col}, ''), {timestamp_col})) = date(?)")
                params.append(str(f["day"]))
            elif period_type == "week" and f.get("week"):
                year, week = str(f["week"]).split("-W")
                period_tests.append(f"CAST({year_col} AS INTEGER)=? AND CAST({week_col} AS INTEGER)=?")
                params.extend([int(year), int(week)])
            elif period_type == "month" and f.get("month"):
                year, month = str(f["month"]).split("-")
                period_tests.append(f"CAST({year_col} AS INTEGER)=? AND CAST({month_col} AS INTEGER)=?")
                params.extend([int(year), int(month)])
            elif period_type == "period" and f.get("date_from") and f.get("date_to"):
                period_tests.append(f"date(COALESCE(NULLIF({day_col}, ''), {timestamp_col})) BETWEEN date(?) AND date(?)")
                params.extend([str(f["date_from"]), str(f["date_to"])])
        if period_tests:
            clauses.append("(" + " OR ".join(f"({test})" for test in period_tests) + ")")

        sql = f"""
            SELECT d.*, rp.ID AS RPPreplot_ID,
                   rp.X AS RPPreplotEasting, rp.Y AS RPPreplotNorthing,
                   rp.Z AS RPPreplotElevation, rp.LineBearing AS ReceiverLineBearing,
                   r.ID AS REC_DB_ID, r.REC_ID, r.NODE_ID AS REC_NODE_ID,
                   r.REC_X, r.REC_Y, r.REC_Z, r.RFIELD_X, r.RFIELD_Y, r.RFIELD_Z
              FROM DSR d
              LEFT JOIN RPPreplot rp ON rp.Line = d.Line AND rp.Point = d.Station
              LEFT JOIN REC_DB r
                ON r.ID = (
                    SELECT MAX(r2.ID) FROM REC_DB r2
                     WHERE r2.LinePointIdx = d.LinePointIdx
                )
             WHERE {' AND '.join(clauses)}
             ORDER BY d.Line, d.Station
        """
        with sqlite3.connect(str(self.db_path)) as conn:
            df = pd.read_sql_query(sql, conn, params=params)
            df["_is_dsr"] = True

            # REC_DB contains a small number of legitimate processing records whose
            # LinePointIdx does not occur in DSR. Include those in whole-database
            # processing/RPPreplot statistics without treating them as DSR records.
            append_params = []
            append_clauses = ["1=1"]
            if lines:
                append_clauses.append("r.Line IN (" + ",".join("?" for _ in lines) + ")")
                append_params.extend(lines)
            if f.get("station_from") is not None:
                append_clauses.append("r.Point >= ?")
                append_params.append(f["station_from"])
            if f.get("station_to") is not None:
                append_clauses.append("r.Point <= ?")
                append_params.append(f["station_to"])
            if period_type == "all" and not f.get("deployment_rov") and not f.get("recovery_rov"):
                orphan_sql = f"""
                    SELECT r.Line, r.Point AS Station,
                           rp.ID AS RPPreplot_ID, rp.X AS RPPreplotEasting,
                           rp.Y AS RPPreplotNorthing, rp.Z AS RPPreplotElevation,
                           rp.LineBearing AS ReceiverLineBearing,
                           r.ID AS REC_DB_ID, r.REC_ID, r.NODE_ID AS REC_NODE_ID,
                           r.REC_X, r.REC_Y, r.REC_Z,
                           r.RFIELD_X, r.RFIELD_Y, r.RFIELD_Z
                      FROM REC_DB r
                      LEFT JOIN RPPreplot rp ON rp.Line=r.Line AND rp.Point=r.Point
                     WHERE {' AND '.join(append_clauses)}
                       AND NOT EXISTS (
                           SELECT 1 FROM DSR d WHERE d.LinePointIdx=r.LinePointIdx
                       )
                """
                orphan_rec = pd.read_sql_query(orphan_sql, conn, params=append_params)
                if not orphan_rec.empty:
                    orphan_rec["_is_dsr"] = False
                    df = pd.concat([df, orphan_rec], ignore_index=True, sort=False)

            # Add planned points absent from DSR so the progress map represents the
            # complete RPPreplot plan. They do not affect DSR production totals.
            rp_params = []
            rp_clauses = ["1=1"]
            if lines:
                rp_clauses.append("rp.Line IN (" + ",".join("?" for _ in lines) + ")")
                rp_params.extend(lines)
            if f.get("station_from") is not None:
                rp_clauses.append("rp.Point >= ?")
                rp_params.append(f["station_from"])
            if f.get("station_to") is not None:
                rp_clauses.append("rp.Point <= ?")
                rp_params.append(f["station_to"])
            rp_only_sql = f"""
                SELECT rp.Line, rp.Point AS Station, rp.ID AS RPPreplot_ID,
                       rp.X AS RPPreplotEasting, rp.Y AS RPPreplotNorthing,
                       rp.Z AS RPPreplotElevation, rp.LineBearing AS ReceiverLineBearing
                  FROM RPPreplot rp
                 WHERE {' AND '.join(rp_clauses)}
                   AND NOT EXISTS (
                       SELECT 1 FROM DSR d WHERE d.Line=rp.Line AND d.Station=rp.Point
                   )
            """
            rp_only = pd.read_sql_query(rp_only_sql, conn, params=rp_params)
            if not rp_only.empty:
                rp_only["_is_dsr"] = False
                df = pd.concat([df, rp_only], ignore_index=True, sort=False)
            qc = pd.read_sql_query("SELECT * FROM project_node_qc WHERE id=1", conn) if self._has_table(conn, "project_node_qc") else pd.DataFrame()
            project = pd.read_sql_query("SELECT * FROM project_main WHERE id=1", conn) if self._has_table(conn, "project_main") else pd.DataFrame()
        return df, qc, project

    @staticmethod
    def _has_table(conn, name):
        return conn.execute("SELECT 1 FROM sqlite_master WHERE name=?", [name]).fetchone() is not None

    def _with_offsets(self, df):
        result = df.copy()
        headings = _num(result.get("ReceiverLineBearing", pd.Series(index=result.index, dtype=float)))
        headings = headings.where(headings.abs() > 1e-9)
        for _, indices in result.groupby("Line").groups.items():
            if headings.loc[indices].notna().any():
                continue
            points = result.loc[indices].sort_values("Station")
            if len(points) > 1:
                valid = points.dropna(subset=["RPPreplotEasting", "RPPreplotNorthing"])
                if len(valid) < 2:
                    continue
                de = float(_num(valid["RPPreplotEasting"]).iloc[-1] - _num(valid["RPPreplotEasting"]).iloc[0])
                dn = float(_num(valid["RPPreplotNorthing"]).iloc[-1] - _num(valid["RPPreplotNorthing"]).iloc[0])
                headings.loc[indices] = (math.degrees(math.atan2(de, dn)) + 360) % 360
        result["ReceiverLineBearing"] = headings.fillna(0)
        heading_rad = np.radians(result["ReceiverLineBearing"])
        for key, (_, x1, y1, z1, x0, y0, z0) in COMPARISONS.items():
            if not {x1, y1, x0, y0}.issubset(result.columns):
                result[f"{key}_offset"] = np.nan
                result[f"{key}_bearing"] = np.nan
                result[f"{key}_dx"] = np.nan
                result[f"{key}_dy"] = np.nan
                result[f"{key}_dz"] = np.nan
                result[f"{key}_inline"] = np.nan
                result[f"{key}_crossline"] = np.nan
                continue
            dx = _num(result[x1]) - _num(result[x0])
            dy = _num(result[y1]) - _num(result[y0])
            result[f"{key}_dx"] = dx
            result[f"{key}_dy"] = dy
            result[f"{key}_offset"] = np.hypot(dx, dy)
            result[f"{key}_bearing"] = (np.degrees(np.arctan2(dx, dy)) + 360) % 360
            result[f"{key}_inline"] = dx * np.sin(heading_rad) + dy * np.cos(heading_rad)
            result[f"{key}_crossline"] = dx * np.cos(heading_rad) - dy * np.sin(heading_rad)
            result[f"{key}_dz"] = _num(result[z1]) - _num(result[z0]) if z0 and z1 in result and z0 in result else np.nan
        return result

    def _one_stat_row(self, df, key, name, limit, rov="All"):
        valid = df[_num(df[f"{key}_offset"]).notna()].copy()
        values = _num(valid[f"{key}_offset"])
        bearings = _num(valid[f"{key}_bearing"]).dropna()
        dx = _num(valid[f"{key}_dx"]).dropna()
        dy = _num(valid[f"{key}_dy"]).dropna()
        inline = _num(valid[f"{key}_inline"]).dropna()
        crossline = _num(valid[f"{key}_crossline"]).dropna()
        context = COMPARISON_CONTEXT[key]
        sigma_e = _num(valid[context["sigma_e"]]).dropna() if context["sigma_e"] and context["sigma_e"] in valid else pd.Series(dtype=float)
        sigma_n = _num(valid[context["sigma_n"]]).dropna() if context["sigma_n"] and context["sigma_n"] in valid else pd.Series(dtype=float)
        row = {"key": key, "name": name, "phase": context["phase"], "rov": rov, "count": int(values.size), "limit": limit}
        if values.empty:
            row.update({k: None for k in ("mean", "median", "min", "max", "std", "rms", "p50", "p90", "p95", "p99", "cep50", "cep90", "cep95", "cep99", "bias_de", "bias_dn", "bias_2d", "inline_mean", "inline_std", "crossline_mean", "crossline_std", "sigma_e95_mean", "sigma_n95_mean", "within_1", "within_2", "within_5", "within_10", "out_count", "out_pct")})
            row["dominant_sector"] = "—"
            return row
        bias_de = float(dx.mean()) if not dx.empty else np.nan
        bias_dn = float(dy.mean()) if not dy.empty else np.nan
        row.update({
            "mean": _json_number(values.mean()), "median": _json_number(values.median()),
            "min": _json_number(values.min()), "max": _json_number(values.max()),
            "std": _json_number(values.std(ddof=1) if len(values) > 1 else 0),
            "rms": _json_number(math.sqrt(float(np.mean(np.square(values))))),
            "p50": _json_number(values.quantile(.50)), "p90": _json_number(values.quantile(.90)),
            "p95": _json_number(values.quantile(.95)), "p99": _json_number(values.quantile(.99)),
            "cep50": _json_number(values.quantile(.50)), "cep90": _json_number(values.quantile(.90)),
            "cep95": _json_number(values.quantile(.95)), "cep99": _json_number(values.quantile(.99)),
            "bias_de": _json_number(bias_de), "bias_dn": _json_number(bias_dn),
            "bias_2d": _json_number(math.hypot(bias_de, bias_dn)),
            "inline_mean": _json_number(inline.mean()), "inline_std": _json_number(inline.std(ddof=1) if len(inline) > 1 else 0),
            "crossline_mean": _json_number(crossline.mean()), "crossline_std": _json_number(crossline.std(ddof=1) if len(crossline) > 1 else 0),
            "sigma_e95_mean": _json_number(sigma_e.mean()) if not sigma_e.empty else None,
            "sigma_n95_mean": _json_number(sigma_n.mean()) if not sigma_n.empty else None,
            "within_1": _json_number((values <= 1).mean() * 100, 1),
            "within_2": _json_number((values <= 2).mean() * 100, 1),
            "within_5": _json_number((values <= 5).mean() * 100, 1),
            "within_10": _json_number((values <= 10).mean() * 100, 1),
            "out_count": int((values > limit).sum()), "out_pct": _json_number((values > limit).mean() * 100, 1),
            "dominant_sector": _dominant_sector(bearings),
        })
        return row

    def _comparison_stats(self, df, qc):
        limit = 10.0
        if not qc.empty and "max_radial_offset" in qc:
            limit = float(qc.iloc[0]["max_radial_offset"] or limit)
        output = []
        for key, definition in COMPARISONS.items():
            output.append(self._one_stat_row(df, key, definition[0], limit))
            rov_column = COMPARISON_CONTEXT[key]["rov"]
            if rov_column and rov_column in df:
                for rov, subset in df[df[rov_column].notna()].groupby(rov_column):
                    output.append(self._one_stat_row(subset, key, definition[0], limit, str(rov)))
        return output

    def _summary(self, df):
        dsr = df[df["_is_dsr"].fillna(False)] if "_is_dsr" in df else df
        dep_time = pd.to_datetime(dsr.get("TimeStamp"), errors="coerce")
        rec_time = pd.to_datetime(dsr.get("TimeStamp1"), errors="coerce")
        dep_days = dep_time.dropna().dt.date.nunique()
        rec_days = rec_time.dropna().dt.date.nunique()
        basis = self.filters.get("time_basis") or "either"
        selected_days = dep_days if basis == "deployment" else rec_days if basis == "recovery" else pd.concat([dep_time, rec_time]).dropna().dt.date.nunique()
        deployed = int(dep_time.notna().sum())
        recovered = int(rec_time.notna().sum())
        processed = int(df.get("REC_DB_ID", pd.Series(dtype=float)).notna().sum())
        processed_matched = int(dsr.get("REC_DB_ID", pd.Series(dtype=float)).notna().sum())
        return {
            "records": int(len(dsr)), "days": int(selected_days),
            "deployment_days": int(dep_days), "recovery_days": int(rec_days),
            "lines": int(dsr["Line"].nunique()),
            "stations": int(dsr[["Line", "Station"]].dropna().drop_duplicates().shape[0]),
            "nodes": int(dsr["Node"].nunique()), "deployed": deployed, "recovered": recovered,
            "planned_preplot": int(df.get("RPPreplot_ID", pd.Series(dtype=float)).dropna().nunique()),
            "dsr_without_rppreplot": int(dsr.get("RPPreplot_ID", pd.Series(dtype=float)).isna().sum()),
            "processed": processed, "processed_matched_to_dsr": processed_matched,
            "recdb_without_dsr": int(processed - processed_matched),
            "recovered_unprocessed": int(((rec_time.notna()) & dsr["REC_DB_ID"].isna()).sum()),
            "deployed_unrecovered": int((dep_time.notna() & rec_time.isna()).sum()),
            "recovery_pct": round(100 * recovered / deployed, 1) if deployed else 0,
            "processing_pct": round(100 * processed / recovered, 1) if recovered else 0,
            "first_deployment": dep_time.min().strftime("%Y-%m-%d %H:%M") if dep_time.notna().any() else "—",
            "last_deployment": dep_time.max().strftime("%Y-%m-%d %H:%M") if dep_time.notna().any() else "—",
            "first_recovery": rec_time.min().strftime("%Y-%m-%d %H:%M") if rec_time.notna().any() else "—",
            "last_recovery": rec_time.max().strftime("%Y-%m-%d %H:%M") if rec_time.notna().any() else "—",
        }

    def _grouped(self, df):
        df = df[df["_is_dsr"].fillna(False)] if "_is_dsr" in df else df
        group = self.filters.get("grouping") or "day"
        dep = pd.DataFrame({"time": pd.to_datetime(df["TimeStamp"], errors="coerce"), "type": "Deployed", "ROV": df["ROV"], "Line": df["Line"]})
        rec = pd.DataFrame({"time": pd.to_datetime(df["TimeStamp1"], errors="coerce"), "type": "Recovered", "ROV": df["ROV1"], "Line": df["Line"]})
        events = pd.concat([dep, rec], ignore_index=True).dropna(subset=["time"])
        if events.empty:
            return []
        if group == "week":
            iso = events["time"].dt.isocalendar()
            events["group"] = iso.year.astype(str) + "-W" + iso.week.astype(str).str.zfill(2)
        elif group == "month":
            events["group"] = events["time"].dt.strftime("%Y-%m")
        elif group == "line":
            events["group"] = events["Line"].astype("Int64").astype(str)
        elif group == "rov":
            events["group"] = events["ROV"].fillna("Unknown").astype(str)
        else:
            events["group"] = events["time"].dt.strftime("%Y-%m-%d")
        table = events.groupby(["group", "type"]).size().unstack(fill_value=0).reset_index()
        for name in ("Deployed", "Recovered"):
            if name not in table:
                table[name] = 0
        return table[["group", "Deployed", "Recovered"]].to_dict("records")

    def _production_statistics(self, df):
        dsr = df[df["_is_dsr"].fillna(False)] if "_is_dsr" in df else df
        planned = int(df.get("RPPreplot_ID", pd.Series(dtype=float)).dropna().nunique())
        dep_time = pd.to_datetime(dsr.get("TimeStamp"), errors="coerce")
        rec_time = pd.to_datetime(dsr.get("TimeStamp1"), errors="coerce")
        counts = {
            "Planned": planned,
            "Deployed": int(dep_time.notna().sum()),
            "Recovered": int(rec_time.notna().sum()),
            "Processed": int(df.get("REC_DB_ID", pd.Series(dtype=float)).notna().sum()),
        }
        status = [{"phase": name, "nodes": value, "percent": round(100 * value / planned, 1) if planned else 0}
                  for name, value in counts.items()]
        daily_rows, prediction = [], []
        processed_time = rec_time.where(dsr.get("REC_DB_ID", pd.Series(index=dsr.index, dtype=float)).notna())
        for phase, times, rov_values, completed in (
            ("Deployment", dep_time, dsr.get("ROV"), counts["Deployed"]),
            ("Recovery", rec_time, dsr.get("ROV1"), counts["Recovered"]),
            ("Processing", processed_time, dsr.get("ROV1"), counts["Processed"]),
        ):
            temp = pd.DataFrame({"date": times.dt.date, "rov": rov_values.fillna("Unknown").astype(str)}).dropna(subset=["date"])
            daily = temp.groupby(["date", "rov"]).size().reset_index(name="nodes")
            totals = daily.groupby("date")["nodes"].transform("sum") if not daily.empty else pd.Series(dtype=float)
            if not daily.empty:
                daily["percent"] = (daily["nodes"] / totals * 100).round(1)
                daily["phase"] = phase
                daily_rows.extend(daily[["phase", "date", "rov", "nodes", "percent"]].to_dict("records"))
            active_days = int(temp.date.nunique())
            average = completed / active_days if active_days else 0
            remaining = max(0, planned - completed)
            days_left = math.ceil(remaining / average) if average else None
            last_date = pd.Timestamp(max(temp.date)) if not temp.empty else None
            predicted = (last_date + pd.Timedelta(days=days_left)).date().isoformat() if last_date is not None and days_left is not None else "—"
            prediction.append({"phase": phase, "completed": completed, "planned": planned,
                               "average_nodes_day": round(average, 1), "remaining": remaining,
                               "days_left": days_left, "predicted_completion": predicted})
        return {"status": status, "daily": daily_rows, "prediction": prediction}

    @staticmethod
    def _phase_polar_data(df):
        rows = []
        for phase, key, rov_field in (("Deployment", "dep_primary_preplot", "ROV"), ("Recovery", "rec_primary_preplot", "ROV1")):
            temp = pd.DataFrame({"bearing": _num(df[f"{key}_bearing"]), "rov": df[rov_field].fillna("Unknown")}).dropna(subset=["bearing"])
            temp["sector"] = (temp.bearing // 22.5 * 22.5)
            grouped = temp.groupby(["rov", "sector"]).size().reset_index(name="nodes")
            if not grouped.empty:
                grouped["percent"] = grouped.nodes / grouped.groupby("rov").nodes.transform("sum") * 100
                grouped["phase"] = phase
                rows.extend(grouped[["phase", "rov", "sector", "nodes", "percent"]].to_dict("records"))
        return rows

    @staticmethod
    def _ecdf_series_data(df):
        rows = []
        definitions = (("Deployment", "dep_primary_preplot", "ROV"),
                       ("Recovery", "rec_primary_preplot", "ROV1"),
                       ("REC_DB", "recdb_preplot", "ROV1"))
        for phase, key, rov_field in definitions:
            temp = pd.DataFrame({"offset": _num(df[f"{key}_offset"]), "rov": df[rov_field].fillna("Unknown")}).dropna(subset=["offset"])
            for rov, subset in temp.groupby("rov"):
                ordered = np.sort(subset.offset.to_numpy())
                if not len(ordered):
                    continue
                indices = np.unique(np.linspace(0, len(ordered) - 1, min(301, len(ordered))).astype(int))
                for index in indices:
                    rows.append({"phase": phase, "rov": str(rov), "offset": float(ordered[index]),
                                 "percent": round((index + 1) / len(ordered) * 100, 3)})
        return rows

    def build(self):
        raw, qc, project = self.load()
        df = self._with_offsets(raw)
        selected = self.filters.get("comparison") or "dep_primary_preplot"
        if selected not in COMPARISONS:
            selected = "dep_primary_preplot"
        stats = self._comparison_stats(df, qc)
        selected_context = COMPARISON_CONTEXT[selected]
        detail_columns = [c for c in [
            "Line", "Station", "Node", "ROV", "TimeStamp", "ROV1", "TimeStamp1",
            "RPPreplotEasting", "RPPreplotNorthing", "RPPreplotElevation", "PrimaryEasting", "PrimaryNorthing", "PrimaryElevation",
            "SecondaryEasting", "SecondaryNorthing", "SecondaryElevation", "PrimaryEasting1", "PrimaryNorthing1",
            "PrimaryElevation1", "SecondaryEasting1", "SecondaryNorthing1", "SecondaryElevation1",
            "REC_X", "REC_Y", "REC_Z", f"{selected}_dx", f"{selected}_dy", f"{selected}_dz",
            f"{selected}_inline", f"{selected}_crossline", f"{selected}_offset", f"{selected}_bearing", selected_context["sigma_e"], selected_context["sigma_n"]
        ] if c in df.columns]
        detail = df[detail_columns].copy()
        rename = {c: c.replace(f"{selected}_", "") for c in detail.columns}
        if selected_context["sigma_e"]:
            rename[selected_context["sigma_e"]] = "sigma_e_95"
        if selected_context["sigma_n"]:
            rename[selected_context["sigma_n"]] = "sigma_n_95"
        detail = detail.rename(columns=rename)
        detail = detail.replace({np.nan: None})
        project_name = str(project.iloc[0].get("name", "")) if not project.empty else ""
        return {
            "project_name": project_name, "summary": self._summary(df), "comparisons": stats,
            "comparison_choices": [(key, value[0]) for key, value in COMPARISONS.items()],
            "selected_comparison": selected, "selected_name": COMPARISONS[selected][0],
            "selected_rov_field": selected_context["rov"],
            "grouped": self._grouped(df), "production": self._production_statistics(df),
            "phase_polar": self._phase_polar_data(df), "ecdf_series": self._ecdf_series_data(df),
            "detail_columns": list(detail.columns),
            "detail": detail.to_dict("records"), "plots": self._plots(df, selected, qc),
        }

    def _plots(self, df, selected, qc):
        rov_column = COMPARISON_CONTEXT[selected]["rov"]
        plot_df = pd.DataFrame({
            "Offset": _num(df[f"{selected}_offset"]), "Bearing": _num(df[f"{selected}_bearing"]),
            "DE": _num(df[f"{selected}_dx"]), "DN": _num(df[f"{selected}_dy"]),
            "Inline": _num(df[f"{selected}_inline"]), "Crossline": _num(df[f"{selected}_crossline"]),
            "Line": df["Line"], "Station": df["Station"],
            "ROV": df.get(rov_column, pd.Series("All", index=df.index)) if rov_column else pd.Series("All", index=df.index),
        }).dropna(subset=["Offset"])
        if plot_df.empty:
            return {"plotly": [], "bokeh_script": "", "bokeh_divs": {}, "bokeh_resources": ""}
        plot_df["ROV"] = plot_df["ROV"].fillna("Unknown").astype(str)
        rovs = sorted(plot_df["ROV"].unique())
        colors = {rov: Category10[10][i % 10] for i, rov in enumerate(rovs)}
        tools = "pan,wheel_zoom,box_zoom,reset,save"

        def finish(p):
            p.legend.click_policy = "hide"
            p.legend.location = "top_right"
            p.toolbar.logo = None
            return p

        def histogram(field, title, bin_size=.5):
            signed = field != "Offset"
            clipped = plot_df[(plot_df[field] >= (-10 if signed else 0)) & (plot_df[field] <= 10)]
            values = clipped[field].dropna().to_numpy()
            if not len(values):
                values = np.array([0.0])
            if bin_size:
                lo = -10 if signed else 0
                hi = 10 + bin_size
                edges = np.arange(lo, hi, bin_size)
            else:
                edges = np.histogram_bin_edges(values, bins="auto")
            p = figure(title=title + " (0.5 m bins; display range ±10 m)", x_axis_label="Offset (m)", y_axis_label="Nodes", height=350, sizing_mode="stretch_width", tools=tools)
            for rov in rovs:
                vals = clipped.loc[clipped.ROV == rov, field].dropna().to_numpy()
                counts, _ = np.histogram(vals, bins=edges)
                percent = counts / max(1, len(vals)) * 100
                center = (edges[:-1] + edges[1:]) / 2
                source = ColumnDataSource({"left": edges[:-1], "right": edges[1:], "center": center, "nodes": counts, "percent": percent})
                p.quad(top="nodes", bottom=0, left="left", right="right", source=source, fill_color=colors[rov], line_color=colors[rov], alpha=.28, legend_label=f"{rov} nodes")
                p.add_tools(HoverTool(renderers=[p.renderers[-1]], tooltips=[("ROV", rov), ("Bin", "@left{0.0} to @right{0.0} m"), ("Nodes", "@nodes"), ("Percent", "@percent{0.0}%")]))
                if len(vals) > 3 and float(np.std(vals)) > 0:
                    try:
                        from scipy.stats import gaussian_kde
                        x = np.linspace(edges[0], edges[-1], 240)
                        kde_nodes = gaussian_kde(vals)(x) * len(vals) * bin_size
                        p.line(x, kde_nodes, color=colors[rov], line_width=3, legend_label=f"{rov} KDE · STD {np.std(vals, ddof=1):.2f} m")
                    except Exception:
                        pass
            return finish(p)

        bokeh_figures = {
            "hist_de": histogram("DE", "Delta Easting distribution"),
            "hist_dn": histogram("DN", "Delta Northing distribution"),
            "hist_radial": histogram("Offset", "Radial offset distribution", .5),
        }

        bull = figure(title="Bullseye: DE vs DN", x_axis_label="DE / cross-east (m)", y_axis_label="DN / north (m)", height=560, sizing_mode="stretch_width", match_aspect=True, tools=tools)
        limit = 10.0
        if not qc.empty and "max_radial_offset" in qc:
            limit = float(qc.iloc[0]["max_radial_offset"] or limit)
        theta = np.linspace(0, 2 * np.pi, 240)
        for radius, dash in ((1, "dotted"), (2, "dotted"), (5, "dashed"), (limit, "solid")):
            bull.line(radius * np.cos(theta), radius * np.sin(theta), color="#d62728" if radius == limit else "#aab5c0", line_dash=dash, line_width=2 if radius == limit else 1)
        marker_types = ["circle", "square", "triangle", "diamond", "inverted_triangle", "hex"]
        mapper = LinearColorMapper(palette=Turbo256, low=0, high=10, nan_color="#d9dde3")
        for i, rov in enumerate(rovs):
            subset = plot_df[plot_df.ROV == rov]
            src = ColumnDataSource(subset)
            bull.scatter("DE", "DN", source=src, size=8, alpha=.72, marker=marker_types[i % len(marker_types)],
                         fill_color={"field": "Offset", "transform": mapper}, line_color=colors[rov], line_width=1.4, legend_label=rov)
            if len(subset):
                p95 = float(subset.Offset.quantile(.95)); bx=float(subset.DE.mean()); by=float(subset.DN.mean())
                bull.line(bx + p95*np.cos(theta), by + p95*np.sin(theta), color=colors[rov], line_width=2.5, line_dash="dashed", legend_label=f"{rov} 95% ({p95:.2f} m)")
        bull.add_layout(ColorBar(color_mapper=mapper, title="Radial offset (m)"), "right")
        bull.add_layout(Span(location=0, dimension="height", line_dash="dashed", line_color="#777"))
        bull.add_layout(Span(location=0, dimension="width", line_dash="dashed", line_color="#777"))
        bull.add_tools(HoverTool(tooltips=[("ROV", "@ROV"), ("Line", "@Line"), ("Station", "@Station"), ("DE", "@DE{0.000}"), ("DN", "@DN{0.000}"), ("Radial", "@Offset{0.000}")]))
        bokeh_figures["bullseye"] = finish(bull)

        for phase, key, rov_field, output_key in (
            ("Deployment Primary", "dep_primary_preplot", "ROV", "ecdf_deployment"),
            ("Recovery Primary", "rec_primary_preplot", "ROV1", "ecdf_recovery"),
            ("REC_DB", "recdb_preplot", "ROV1", "ecdf_recdb"),
        ):
            cdf = figure(title=f"{phase} vs RPPreplot — radial ECDF", x_axis_label="Radial offset (m)",
                         y_axis_label="Cumulative probability (%)", x_range=(0, 10), y_range=(0, 101),
                         height=350, sizing_mode="stretch_width", tools=tools)
            phase_rovs = df[rov_field].fillna("Unknown").astype(str)
            for i, rov in enumerate(sorted(phase_rovs.unique())):
                values = _num(df.loc[phase_rovs == rov, f"{key}_offset"]).dropna().to_numpy()
                if not len(values):
                    continue
                ordered = np.sort(values)
                cdf.line(ordered, np.arange(1, len(ordered) + 1) / len(ordered) * 100,
                         color=Category10[10][i % 10], line_width=3, legend_label=f"{rov} (N={len(ordered)})")
            bokeh_figures[output_key] = finish(cdf)

        series = figure(title="In-line, cross-line and radial offset series", x_axis_label="Station", y_axis_label="Offset (m)", height=390, sizing_mode="stretch_width", tools=tools)
        metric_styles = {"Inline": "solid", "Crossline": "dashed", "Offset": "dotdash"}
        for rov in rovs:
            subset = plot_df[plot_df.ROV == rov].sort_values("Station")
            for metric, dash in metric_styles.items():
                series.line(subset.Station, subset[metric], color=colors[rov], line_dash=dash, line_width=2, alpha=.9, legend_label=f"{rov} {metric}")
        bokeh_figures["offset_series"] = finish(series)

        progress = figure(title="Progress map - click legend to hide layers", x_axis_label="Easting", y_axis_label="Northing", height=560, sizing_mode="stretch_width", match_aspect=True, tools=tools)
        map_layers = [
            ("RPPreplot", "RPPreplotEasting", "RPPreplotNorthing", "#6c757d", "cross"),
            ("Deployment", "PrimaryEasting", "PrimaryNorthing", "#1677ff", "square"),
            ("Recovery", "PrimaryEasting1", "PrimaryNorthing1", "#20a44b", "triangle"),
            ("REC_DB", "REC_X", "REC_Y", "#ff8c1a", "circle"),
        ]
        for label, xcol, ycol, color, marker in map_layers:
            if {xcol, ycol}.issubset(df.columns):
                layer = pd.DataFrame({"x": _num(df[xcol]), "y": _num(df[ycol]), "Line": df.Line, "Station": df.Station}).dropna(subset=["x", "y"])
                progress.scatter("x", "y", source=ColumnDataSource(layer), marker=marker, size=7, alpha=.72, color=color, legend_label=label)
        progress.add_tools(HoverTool(tooltips=[("Line", "@Line"), ("Station", "@Station"), ("E", "@x{0.00}"), ("N", "@y{0.00}")]))
        bokeh_figures["progress_map"] = finish(progress)

        production = self._production_statistics(df)
        prediction_by_phase = {row["phase"]: row for row in production["prediction"]}
        for phase, output_key in (("Deployment", "production_deployment"), ("Recovery", "production_recovery")):
            daily = pd.DataFrame([row for row in production["daily"] if row["phase"] == phase])
            if daily.empty:
                p = figure(title=f"{phase} by day", height=380, sizing_mode="stretch_width", tools=tools)
                bokeh_figures[output_key] = p
                continue
            daily["date"] = daily.date.astype(str)
            pivot = daily.pivot_table(index="date", columns="rov", values="nodes", aggfunc="sum", fill_value=0).sort_index()
            groups = list(pivot.index); phase_rovs = [str(c) for c in pivot.columns]
            source_data = {"date": groups}
            for rov in phase_rovs:
                source_data[rov] = pivot[rov].astype(int).tolist()
            total_by_day = pivot.sum(axis=1)
            planned = max(1, int(production["status"][0]["nodes"]))
            source_data["progress"] = (total_by_day.cumsum() / planned * 100).tolist()
            pred = prediction_by_phase[phase]
            title = (f"{phase} day by day — {pred['completed']:,}/{pred['planned']:,} nodes | "
                     f"average {pred['average_nodes_day']:.1f} nodes/day | predicted {pred['predicted_completion']}")
            p = figure(title=title, x_range=groups, x_axis_label="Day", y_axis_label="Nodes",
                       height=400, sizing_mode="stretch_width", tools=tools)
            source = ColumnDataSource(source_data)
            p.vbar_stack(phase_rovs, x="date", width=.82, source=source,
                         color=[Category10[10][i % 10] for i in range(len(phase_rovs))],
                         legend_label=[f"{rov} ({int(pivot[rov].sum()):,} nodes)" for rov in phase_rovs])
            p.extra_y_ranges = {"progress": Range1d(start=0, end=100)}
            p.add_layout(LinearAxis(y_range_name="progress", axis_label="Progress (% of RPPreplot)"), "right")
            p.line("date", "progress", source=source, y_range_name="progress", color="#111827", line_width=2.5, legend_label=f"{phase} progress")
            p.scatter("date", "progress", source=source, y_range_name="progress", color="#111827", size=5, legend_label=f"{phase} progress")
            p.xaxis.major_label_orientation = math.pi / 2
            bokeh_figures[output_key] = finish(p)

        script, divs = components(bokeh_figures)

        polar_figures = []
        for phase, key, rov_field in (("Deployment", "dep_primary_preplot", "ROV"), ("Recovery", "rec_primary_preplot", "ROV1")):
            phase_data = pd.DataFrame({"Bearing": _num(df[f"{key}_bearing"]), "ROV": df[rov_field].fillna("Unknown")}).dropna(subset=["Bearing"])
            for rov, subset in phase_data.groupby("ROV"):
                polar_data = subset.assign(Sector=(subset.Bearing // 22.5 * 22.5)).groupby("Sector", as_index=False).size().rename(columns={"size": "Nodes"})
                polar_data["Percent"] = polar_data.Nodes / polar_data.Nodes.sum() * 100
                fig = px.bar_polar(polar_data, r="Percent", theta="Sector", hover_data={"Nodes": True, "Percent": ":.1f"}, title=f"{phase} offset direction — {rov}")
                fig.update_traces(name=str(rov), showlegend=True)
                fig.update_layout(template="plotly_white", polar_bgcolor="white", paper_bgcolor="white", font_color="#243447", legend={"itemclick": "toggle", "itemdoubleclick": "toggleothers"})
                polar_figures.append(fig)
        selected_stat = self._one_stat_row(df, selected, COMPARISONS[selected][0], 10)
        inside = max(0, selected_stat["count"] - (selected_stat["out_count"] or 0))
        donut = go.Figure(go.Pie(labels=["Within specification", "Out of specification"], values=[inside, selected_stat["out_count"] or 0], hole=.68, marker_colors=["#20a44b", "#dc3545"]))
        donut.update_layout(title="QC compliance", template="plotly_white", paper_bgcolor="white", font_color="#243447", legend={"itemclick": "toggle", "itemdoubleclick": "toggleothers"})
        plotly_html = [fig.to_html(full_html=False, include_plotlyjs=False, config={"responsive": True, "displaylogo": False}) for fig in (*polar_figures, donut)]
        return {"plotly": plotly_html, "bokeh_script": script, "bokeh_divs": divs, "bokeh_resources": INLINE.render()}
