"""
Raw data loading helpers for SeisWebLog reports.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Dict
import pandas as pd


@dataclass
class ReportQueries:
    """Small query helper bound to one project DB path."""
    db_path: str

    def _read_sql(self, sql: str, params=None) -> pd.DataFrame:
        params = params or []
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query(sql, conn, params=params)
        except Exception:
            return pd.DataFrame()

    def get_dsr(self, start_date, end_date) -> pd.DataFrame:
        return self._read_sql(
            """
            SELECT *
            FROM DSR
            WHERE DATE(COALESCE(Timestamp, Timestamp1, '')) BETWEEN DATE(?) AND DATE(?)
            """,
            [str(start_date), str(end_date)],
        )

    def get_shots(self, start_date, end_date) -> pd.DataFrame:
        return self._read_sql(
            """
            SELECT *
            FROM SHOT_TABLE
            WHERE DATE(COALESCE(shot_time, shot_datetime, date_time, created_at, '')) BETWEEN DATE(?) AND DATE(?)
            """,
            [str(start_date), str(end_date)],
        )

    def get_blackbox(self, start_date, end_date) -> pd.DataFrame:
        return self._read_sql(
            """
            SELECT *
            FROM BlackBox
            WHERE DATE(COALESCE(Timestamp, timestamp, '')) BETWEEN DATE(?) AND DATE(?)
            """,
            [str(start_date), str(end_date)],
        )

    def get_fleet(self) -> pd.DataFrame:
        return self._read_sql("SELECT * FROM project_fleet")

    def get_project_info(self) -> pd.DataFrame:
        return self._read_sql("SELECT * FROM project_geometry LIMIT 1")

    def collect_all(self, start_date, end_date) -> Dict[str, pd.DataFrame]:
        return {
            "dsr": self.get_dsr(start_date, end_date),
            "shots": self.get_shots(start_date, end_date),
            "blackbox": self.get_blackbox(start_date, end_date),
            "fleet": self.get_fleet(),
            "project_info": self.get_project_info(),
        }
