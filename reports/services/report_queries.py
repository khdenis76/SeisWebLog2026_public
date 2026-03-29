"""
Raw data extraction for report generation.

This module is responsible for reading the active project database and
returning data frames that will later be used for metrics, charts, and
maps.

Design goal:
- if a table or view is missing, do not crash the whole report
- return empty data frames when data is unavailable
- keep SQL access centralized in one place
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Dict, List

import pandas as pd


@dataclass
class QueryConfig:
    """
    Query configuration describing the selected report period.
    """

    start_date: str
    end_date: str


class ReportQueries:
    """
    Read report source data from the project SQLite database.

    The class accepts a path to the project database file. It then checks
    for the existence of tables/views before issuing queries.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        """
        Open SQLite connection.
        """
        return sqlite3.connect(self.db_path)

    def _list_tables(self, conn: sqlite3.Connection) -> List[str]:
        """
        Return a list of existing tables/views in the database.
        """
        sql = "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
        return pd.read_sql_query(sql, conn)["name"].tolist()

    def _safe_query(self, conn: sqlite3.Connection, sql: str) -> pd.DataFrame:
        """
        Execute query safely and return an empty data frame on failure.

        Reporting should be resilient. A missing optional table should not
        break the entire report build.
        """
        try:
            return pd.read_sql_query(sql, conn)
        except Exception:
            return pd.DataFrame()

    def collect_all(self, start_date, end_date) -> Dict[str, pd.DataFrame]:
        """
        Collect all raw data needed by the report engine.

        Notes:
        - Dates are interpolated as plain text because project databases in
          SeisWebLog often store date/timestamp fields as TEXT.
        - You may later replace these queries with project-specific views
          such as V_DSR_LineSummary, Daily_Recovery, Daily_Deployment, etc.
        """
        start_date = str(start_date)
        end_date = str(end_date)

        with self._connect() as conn:
            existing = set(self._list_tables(conn))

            data = {
                "dsr": self._get_dsr(conn, existing, start_date, end_date),
                "shots": self._get_shots(conn, existing, start_date, end_date),
                "blackbox": self._get_blackbox(conn, existing, start_date, end_date),
                "fleet": self._get_fleet(conn, existing),
                "v_dsr_line_summary": self._get_view(conn, existing, "V_DSR_LineSummary"),
                "v_shot_line_summary": self._get_view(conn, existing, "V_SHOT_LineSummary"),
                "daily_deployment": self._get_view(conn, existing, "Daily_Deployment"),
                "daily_recovery": self._get_view(conn, existing, "Daily_Recovery"),
                "vessel_activity_summary": self._get_view(conn, existing, "VesselActivitySummary"),
            }

        return data

    def _get_view(self, conn, existing, view_name: str) -> pd.DataFrame:
        """
        Read a whole SQL view when it exists.
        """
        if view_name not in existing:
            return pd.DataFrame()
        return self._safe_query(conn, f"SELECT * FROM {view_name}")

    def _get_dsr(self, conn, existing, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Read DSR rows for the selected time interval.

        This query tries a few likely timestamp fields used in SeisWebLog.
        """
        if "DSR" not in existing:
            return pd.DataFrame()

        sql = f"""
            SELECT *
            FROM DSR
            WHERE (
                COALESCE(dsr_timestamp, '') BETWEEN '{start_date}' AND '{end_date} 23:59:59'
                OR COALESCE(dsr_timestamp1, '') BETWEEN '{start_date}' AND '{end_date} 23:59:59'
                OR COALESCE(Timestamp, '') BETWEEN '{start_date}' AND '{end_date} 23:59:59'
            )
        """
        return self._safe_query(conn, sql)

    def _get_shots(self, conn, existing, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Read shot records for the selected time interval.
        """
        if "SHOT_TABLE" not in existing:
            return pd.DataFrame()

        sql = f"""
            SELECT *
            FROM SHOT_TABLE
            WHERE (
                COALESCE(shot_time, '') BETWEEN '{start_date}' AND '{end_date} 23:59:59'
                OR COALESCE(source_time, '') BETWEEN '{start_date}' AND '{end_date} 23:59:59'
                OR COALESCE(Timestamp, '') BETWEEN '{start_date}' AND '{end_date} 23:59:59'
            )
        """
        return self._safe_query(conn, sql)

    def _get_blackbox(self, conn, existing, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Read BlackBox navigation / timing data for the selected period.
        """
        if "BlackBox" not in existing:
            return pd.DataFrame()

        sql = f"""
            SELECT *
            FROM BlackBox
            WHERE COALESCE(Timestamp, '') BETWEEN '{start_date}' AND '{end_date} 23:59:59'
        """
        return self._safe_query(conn, sql)

    def _get_fleet(self, conn, existing) -> pd.DataFrame:
        """
        Read project fleet table if available.
        """
        for candidate in ("project_fleet", "fleet_vessel", "fleet"):
            if candidate in existing:
                return self._safe_query(conn, f"SELECT * FROM {candidate}")
        return pd.DataFrame()
