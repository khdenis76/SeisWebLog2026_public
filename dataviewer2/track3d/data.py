from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import sqlite3
import numpy as np


class Track3DDataError(RuntimeError):
    pass


@dataclass(slots=True)
class DsrLine3D:
    line: int
    station: np.ndarray
    x: np.ndarray
    y: np.ndarray
    z: np.ndarray
    node: np.ndarray
    label: str


@dataclass(slots=True)
class BBoxFile3D:
    file_id: int
    label: str
    row_count: int


@dataclass(slots=True)
class BBoxTrack3D:
    name: str
    x: np.ndarray
    y: np.ndarray
    z: np.ndarray
    time_labels: np.ndarray
    z_source: str


class Track3DRepository:
    def __init__(self, project_path: str | Path) -> None:
        path = Path(project_path).expanduser().resolve()
        self.db_path = self._resolve_db_path(path)

    @staticmethod
    def _resolve_db_path(path: Path) -> Path:
        if path.is_file() and path.suffix.lower() in {'.db', '.sqlite', '.sqlite3'}:
            return path
        candidates = [
            path / 'project.sqlite3',
            path / 'data' / 'project.sqlite3',
            path / 'db.sqlite3',
            path.parent / 'data' / 'project.sqlite3',
        ]
        for candidate in candidates:
            if candidate.is_file():
                return candidate.resolve()
        raise Track3DDataError('Project database was not found.\nChecked:\n' + '\n'.join(str(c) for c in candidates))

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_path), timeout=30)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _tables(con: sqlite3.Connection) -> dict[str, str]:
        return {str(r[0]).lower(): str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}

    @staticmethod
    def _columns(con: sqlite3.Connection, table: str) -> list[str]:
        return [str(r[1]) for r in con.execute(f'PRAGMA table_info("{table}")')]

    @staticmethod
    def _pick(columns: list[str], aliases: tuple[str, ...]) -> str | None:
        lookup = {c.lower(): c for c in columns}
        return next((lookup[a.lower()] for a in aliases if a.lower() in lookup), None)

    @staticmethod
    def _num(value) -> float:
        try:
            result = float(value)
            return result if np.isfinite(result) and abs(result) < 1e12 else np.nan
        except (TypeError, ValueError):
            return np.nan

    def dsr_lines(self) -> list[int]:
        with self._connect() as con:
            table = self._tables(con).get('dsr')
            if not table:
                return []
            cols = self._columns(con, table)
            line = self._pick(cols, ('Line', 'RLine', 'ReceiverLine'))
            if not line:
                return []
            rows = con.execute(f'SELECT DISTINCT CAST("{line}" AS INTEGER) FROM "{table}" WHERE "{line}" IS NOT NULL ORDER BY 1').fetchall()
        return [int(r[0]) for r in rows if r[0] is not None]

    def dsr_position_options(self) -> list[tuple[str, str, str, tuple[str, ...]]]:
        return [
            ('Primary deployment', 'PrimaryEasting', 'PrimaryNorthing', ('PrimaryElevation', 'PrimaryDepth', 'Elevation', 'Depth')),
            ('Secondary deployment', 'SecondaryEasting', 'SecondaryNorthing', ('SecondaryElevation', 'SecondaryDepth', 'Elevation', 'Depth')),
            ('Primary recovery', 'PrimaryEasting1', 'PrimaryNorthing1', ('PrimaryElevation1', 'PrimaryDepth1', 'Elevation1', 'Depth1')),
            ('Secondary recovery', 'SecondaryEasting1', 'SecondaryNorthing1', ('SecondaryElevation1', 'SecondaryDepth1', 'Elevation1', 'Depth1')),
            ('Preplot', 'PreplotEasting', 'PreplotNorthing', ('PreplotElevation', 'Elevation', 'WaterDepth', 'Depth')),
        ]

    def load_dsr_line(self, line_value: int, option_label: str) -> DsrLine3D:
        with self._connect() as con:
            table = self._tables(con).get('dsr')
            if not table:
                raise Track3DDataError('DSR table was not found.')
            cols = self._columns(con, table)
            line_col = self._pick(cols, ('Line', 'RLine', 'ReceiverLine'))
            station_col = self._pick(cols, ('Station', 'LinePoint', 'Point'))
            node_col = self._pick(cols, ('Node', 'NODE_HEX_ID', 'RemoteUnit'))
            option = next((o for o in self.dsr_position_options() if o[0] == option_label), self.dsr_position_options()[0])
            x_col = self._pick(cols, (option[1],))
            y_col = self._pick(cols, (option[2],))
            z_col = self._pick(cols, option[3])
            if not line_col or not station_col or not x_col or not y_col:
                raise Track3DDataError(f'{option_label} coordinate columns were not found in DSR.\nAvailable columns: {", ".join(cols)}')
            select = [f'"{station_col}" AS station', f'"{x_col}" AS x', f'"{y_col}" AS y']
            select.append(f'"{z_col}" AS z' if z_col else '0.0 AS z')
            select.append(f'"{node_col}" AS node' if node_col else "'' AS node")
            rows = con.execute(
                f'SELECT {", ".join(select)} FROM "{table}" WHERE "{line_col}"=? ORDER BY CAST("{station_col}" AS REAL)',
                (line_value,),
            ).fetchall()
        if not rows:
            raise Track3DDataError(f'No DSR records were found for line {line_value}.')
        station = np.asarray([self._num(r['station']) for r in rows], float)
        x = np.asarray([self._num(r['x']) for r in rows], float)
        y = np.asarray([self._num(r['y']) for r in rows], float)
        z = np.asarray([self._num(r['z']) for r in rows], float)
        z[~np.isfinite(z)] = 0.0
        node = np.asarray([str(r['node'] or '') for r in rows], object)
        valid = np.isfinite(station) & np.isfinite(x) & np.isfinite(y)
        if valid.sum() < 1:
            raise Track3DDataError(f'Line {line_value} has no valid {option_label.lower()} coordinates.')
        return DsrLine3D(line_value, station[valid], x[valid], y[valid], z[valid], node[valid], option_label)

    def _find_bbox_tables(self, con: sqlite3.Connection) -> tuple[str | None, str | None]:
        tables = self._tables(con)
        file_table = next((tables.get(n) for n in ('blackbox_files', 'blackboxfiles', 'bbox_files')), None)
        data_table = next((tables.get(n) for n in ('blackbox', 'blackbox_data', 'bbox', 'bbox_data')), None)
        return file_table, data_table

    def bbox_files(self) -> list[BBoxFile3D]:
        with self._connect() as con:
            file_table, data_table = self._find_bbox_tables(con)
            if not data_table:
                return []
            dcols = self._columns(con, data_table)
            fk = self._pick(dcols, ('File_FK', 'FileID', 'FileId', 'file_id', 'BlackBoxFile_FK'))
            if file_table:
                fcols = self._columns(con, file_table)
                fid = self._pick(fcols, ('ID', 'id', 'FileID'))
                name = self._pick(fcols, ('FileName', 'Name', 'filename', 'source_file_name'))
                if fid:
                    name_expr = f'"{name}"' if name else "''"
                    sql = (
                        f'SELECT "{fid}" AS id, {name_expr} AS name '
                        f'FROM "{file_table}" ORDER BY "{fid}" DESC'
                    )
                    rows = con.execute(sql).fetchall()
                    result = []
                    for row in rows:
                        count = con.execute(f'SELECT COUNT(*) FROM "{data_table}" WHERE "{fk}"=?', (row['id'],)).fetchone()[0] if fk else 0
                        result.append(BBoxFile3D(int(row['id']), str(row['name'] or f'BlackBox {row["id"]}'), int(count)))
                    return result
            if fk:
                rows = con.execute(f'SELECT "{fk}" AS id, COUNT(*) AS n FROM "{data_table}" GROUP BY "{fk}" ORDER BY "{fk}" DESC').fetchall()
                return [BBoxFile3D(int(r['id']), f'BlackBox {r["id"]}', int(r['n'])) for r in rows]
            count = int(con.execute(f'SELECT COUNT(*) FROM "{data_table}"').fetchone()[0])
            return [BBoxFile3D(0, data_table, count)]

    @staticmethod
    def _coord_prefix(column: str, suffixes: tuple[str, ...]) -> str | None:
        suffix = '|'.join(re.escape(s) for s in suffixes)
        m = re.match(rf'^(.*?)[ _-]*({suffix})$', column, flags=re.IGNORECASE)
        if not m:
            return None
        prefix = re.sub(r'[ _-]+$', '', m.group(1)).strip()
        return prefix.lower() if prefix else None

    def load_bbox_tracks(self, file_id: int) -> list[BBoxTrack3D]:
        with self._connect() as con:
            _, table = self._find_bbox_tables(con)
            if not table:
                raise Track3DDataError('BlackBox data table was not found.')
            cols = self._columns(con, table)
            fk = self._pick(cols, ('File_FK', 'FileID', 'FileId', 'file_id', 'BlackBoxFile_FK'))
            time_col = self._pick(cols, ('TimeStamp', 'Timestamp', 'DateTime', 'datetime', 'Time', 'time'))
            sql = f'SELECT * FROM "{table}"'
            params: tuple = ()
            if fk and file_id:
                sql += f' WHERE "{fk}"=?'; params = (file_id,)
            if time_col:
                sql += f' ORDER BY "{time_col}"'
            rows = con.execute(sql, params).fetchall()
        if not rows:
            return []
        xmap: dict[str, str] = {}
        ymap: dict[str, str] = {}
        zmap: dict[str, str] = {}
        for c in cols:
            p = self._coord_prefix(c, ('Easting', 'East', 'X'))
            if p: xmap.setdefault(p, c)
            p = self._coord_prefix(c, ('Northing', 'North', 'Y'))
            if p: ymap.setdefault(p, c)
            p = self._coord_prefix(c, ('Elevation', 'Height', 'Depth', 'Altitude', 'Z'))
            if p: zmap.setdefault(p, c)
        preferred = ['gnss1', 'gnss2', 'vessel', 'rov1', 'rov2', 'ins', 'usbl', 'tms']
        prefixes = [p for p in preferred if p in xmap and p in ymap]
        prefixes += sorted(p for p in (xmap.keys() & ymap.keys()) if p not in prefixes)
        labels = np.asarray([str(r[time_col]) if time_col and r[time_col] is not None else str(i) for i, r in enumerate(rows)], object)
        canonical = {'gnss1':'GNSS1','gnss2':'GNSS2','vessel':'Vessel','rov1':'ROV1','rov2':'ROV2','ins':'INS','usbl':'USBL','tms':'TMS'}
        tracks: list[BBoxTrack3D] = []
        for prefix in prefixes:
            xc, yc, zc = xmap[prefix], ymap[prefix], zmap.get(prefix)
            x = np.asarray([self._num(r[xc]) for r in rows], float)
            y = np.asarray([self._num(r[yc]) for r in rows], float)
            z = np.asarray([self._num(r[zc]) for r in rows], float) if zc else np.zeros(len(rows), float)
            z[~np.isfinite(z)] = 0.0
            valid = np.isfinite(x) & np.isfinite(y)
            if valid.sum() < 2:
                continue
            tracks.append(BBoxTrack3D(canonical.get(prefix, prefix.upper()), x[valid], y[valid], z[valid], labels[valid], zc or 'Z = 0'))
        return tracks
