"""
project_template_db.py

Database helper class for the SeisWebLog baseproject application.

Purpose
-------
This class manages the custom SQLite table `project_template` inside the
ACTIVE PROJECT database, not Django's default db.sqlite3.

The table is intended to store project planning/template data imported from
Excel files, for example:

    Start SL  -> FirstSL
    End SL    -> LastSL
    # Lines   -> LNum
    Rx        -> RLine
    Tier      -> Tier

It also supports optional vessel links:

    deployed_by_vessel  -> project_fleet.ID
    recovered_by_vessel -> project_fleet.ID

This class is designed to be reused by:
    - Excel import views
    - JSON API endpoints
    - Bootstrap tables
    - Bokeh plots
    - Matplotlib/report generation

Coding style follows SeisWebLog conventions:
    - imports at top
    - direct SQLite helper class for project database work
    - small reusable methods
    - Bokeh-friendly export helpers
"""

import sqlite3
from pathlib import Path

from django.template.loader import render_to_string


class ProjectTemplateDB:
    """
    Helper class for managing the `project_template` table in a project SQLite DB.

    Parameters
    ----------
    db_path : str | Path
        Full path to the active project SQLite database.

    Example
    -------
    ptdb = ProjectTemplateDB(project.db_path)
    ptdb.ensure_schema()
    rows = ptdb.list_rows()
    """

    def __init__(self, db_path):
        """
        Store the database path.

        Nothing is created here. The actual database connection is opened only
        when one of the methods needs it. This keeps the class lightweight and
        avoids holding SQLite locks longer than necessary.
        """
        self.db_path = Path(db_path)

    def _connect(self):
        """
        Open a SQLite connection to the active project database.

        Returns
        -------
        sqlite3.Connection
            SQLite connection with:
                - row_factory = sqlite3.Row, so query rows behave like dicts
                - foreign_keys enabled
                - busy_timeout set to reduce 'database is locked' errors

        Notes
        -----
        Every public method opens and closes its own connection. This is safer
        for Django views and lazy-loaded Bokeh endpoints because it avoids long
        running open database handles.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 60000")
        return conn

    def ensure_schema(self):
        """
        Create required tables for project template workflows.
        """

        conn = self._connect()

        try:
            cur = conn.cursor()

            # =========================================================
            # MAIN TEMPLATE TABLE
            # =========================================================

            cur.execute("""
                CREATE TABLE IF NOT EXISTS project_template (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,

                    FirstSL INTEGER,
                    LastSL INTEGER,

                    LNum INTEGER,
                    RLine INTEGER,

                    Tier INTEGER,

                    deployed_by_vessel INTEGER,
                    recovered_by_vessel INTEGER,

                    FOREIGN KEY(deployed_by_vessel)
                        REFERENCES project_fleet(ID),

                    FOREIGN KEY(recovered_by_vessel)
                        REFERENCES project_fleet(ID)
                )
            """)

            # =========================================================
            # SOURCE LINE GROUPS
            # =========================================================

            cur.execute("""
                CREATE TABLE IF NOT EXISTS project_template_sl_groups (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,

                    group_no INTEGER NOT NULL,

                    start_line INTEGER NOT NULL,
                    end_line INTEGER NOT NULL,

                    direction TEXT NOT NULL DEFAULT 'asc',

                    is_active INTEGER NOT NULL DEFAULT 1
                )
            """)

            # =========================================================
            # INDEXES
            # =========================================================

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_project_template_rline
                ON project_template(RLine)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_project_template_firstsl
                ON project_template(FirstSL)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_project_template_lastsl
                ON project_template(LastSL)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_project_template_sl_groups_no
                ON project_template_sl_groups(group_no)
            """)

            conn.commit()

        finally:
            conn.close()

    def clear_all(self):
        """
        Delete all rows from `project_template`.

        Returns
        -------
        int
            Number of deleted rows.

        Use case
        --------
        Useful when the import modal has a 'Replace existing template' option.
        """
        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM project_template")
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

    def save_sl_groups(self, groups, replace=True):
        """
        Save source-line group rules used to expand FirstSL -> LastSL.

        Example:
            Group 1: 10 -> 1 DESC
            Group 2: 20 -> 11 DESC

            FirstSL=5, LastSL=14 gives:
            5,4,3,2,1,14,13,12,11
        """
        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS project_template_sl_groups (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_no INTEGER NOT NULL,
                    start_line INTEGER NOT NULL,
                    end_line INTEGER NOT NULL,
                    direction TEXT NOT NULL DEFAULT 'asc',
                    is_active INTEGER NOT NULL DEFAULT 1
                )
            """)

            if replace:
                cur.execute("DELETE FROM project_template_sl_groups")

            values = []
            for g in groups:
                values.append((
                    int(g["group_no"]),
                    int(g["start_line"]),
                    int(g["end_line"]),
                    g.get("direction", "asc"),
                    1,
                ))

            cur.executemany("""
                INSERT INTO project_template_sl_groups (
                    group_no,
                    start_line,
                    end_line,
                    direction,
                    is_active
                )
                VALUES (?, ?, ?, ?, ?)
            """, values)

            conn.commit()
            return len(values)

        finally:
            conn.close()

    def insert_rows(self, rows, replace=False):
        """
        Insert many project template rows.

        Parameters
        ----------
        rows : list[dict]
            List of dictionaries. Expected keys:
                - FirstSL
                - LastSL
                - LNum
                - RLine
                - Tier
                - deployed_by_vessel
                - recovered_by_vessel

        replace : bool, default False
            If True, all existing rows are deleted before insert.
            If False, rows are appended.

        Returns
        -------
        int
            Number of inserted rows.

        Example
        -------
        ptdb.insert_rows([
            {
                "FirstSL": 1001,
                "LastSL": 1010,
                "LNum": 10,
                "RLine": 4517,
                "Tier": 1,
                "deployed_by_vessel": 1,
                "recovered_by_vessel": 2,
            }
        ])
        """
        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()

            if replace:
                cur.execute("DELETE FROM project_template")

            values = []
            for row in rows:
                values.append((
                    row.get("FirstSL"),
                    row.get("LastSL"),
                    row.get("LNum"),
                    row.get("RLine"),
                    row.get("Tier"),
                    row.get("deployed_by_vessel"),
                    row.get("recovered_by_vessel"),
                ))

            cur.executemany("""
                INSERT INTO project_template (
                    FirstSL,
                    LastSL,
                    LNum,
                    RLine,
                    Tier,
                    deployed_by_vessel,
                    recovered_by_vessel
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, values)

            conn.commit()
            return len(values)
        finally:
            conn.close()

    def list_rows(self, limit=None, offset=0):
        """
        Return project template rows with vessel names.

        Parameters
        ----------
        limit : int | None
            Optional maximum number of rows to return.

        offset : int, default 0
            Offset for pagination.

        Returns
        -------
        list[dict]
            Rows as dictionaries, including:
                - project_template columns
                - deployed_vessel_name
                - recovered_vessel_name

        Notes
        -----
        This method is useful for Bootstrap tables and JSON API endpoints.
        """
        self.ensure_schema()

        sql = """
            SELECT
                pt.ID,
                pt.FirstSL,
                pt.LastSL,
                pt.LNum,
                pt.RLine,
                pt.Tier,
                pt.deployed_by_vessel,
                pt.recovered_by_vessel,
                dv.vessel_name AS deployed_vessel_name,
                rv.vessel_name AS recovered_vessel_name
            FROM project_template pt
            LEFT JOIN project_fleet dv
                ON dv.ID = pt.deployed_by_vessel
            LEFT JOIN project_fleet rv
                ON rv.ID = pt.recovered_by_vessel
            ORDER BY pt.FirstSL, pt.LastSL, pt.RLine, pt.ID
        """

        params = []
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params.extend([int(limit), int(offset)])

        conn = self._connect()
        try:
            cur = conn.cursor()
            return [dict(row) for row in cur.execute(sql, params).fetchall()]
        finally:
            conn.close()

    def get_by_id(self, template_id):
        """
        Return one project template row by ID.

        Parameters
        ----------
        template_id : int
            project_template.ID value.

        Returns
        -------
        dict | None
            Row dictionary if found, otherwise None.
        """
        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()
            row = cur.execute("""
                SELECT *
                FROM project_template
                WHERE ID = ?
            """, (int(template_id),)).fetchone()

            return dict(row) if row else None
        finally:
            conn.close()

    def delete_by_ids(self, ids):
        """
        Delete selected project template rows.

        Parameters
        ----------
        ids : list[int | str]
            List of project_template.ID values.

        Returns
        -------
        int
            Number of deleted rows.

        Use case
        --------
        Useful for a Bootstrap table with checkboxes:
            - select rows
            - delete selected
        """
        self.ensure_schema()

        ids = [int(x) for x in ids if str(x).strip()]
        if not ids:
            return 0

        placeholders = ",".join("?" for _ in ids)

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                f"DELETE FROM project_template WHERE ID IN ({placeholders})",
                ids,
            )
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

    def update_row(self, template_id, data):
        """
        Update one project template row.

        Parameters
        ----------
        template_id : int
            project_template.ID value.

        data : dict
            Fields to update. Allowed keys:
                - FirstSL
                - LastSL
                - LNum
                - RLine
                - Tier
                - deployed_by_vessel
                - recovered_by_vessel

        Returns
        -------
        int
            Number of updated rows. Usually 0 or 1.

        Notes
        -----
        Unknown keys are ignored. This prevents accidental SQL updates from
        uncontrolled request data.
        """
        self.ensure_schema()

        allowed = {
            "FirstSL",
            "LastSL",
            "LNum",
            "RLine",
            "Tier",
            "deployed_by_vessel",
            "recovered_by_vessel",
        }

        fields = []
        values = []

        for key, value in data.items():
            if key in allowed:
                fields.append(f"{key} = ?")
                values.append(value)

        if not fields:
            return 0

        values.append(int(template_id))

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(f"""
                UPDATE project_template
                SET {", ".join(fields)}
                WHERE ID = ?
            """, values)

            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

    def summary_by_tier(self):
        """
        Summarize project template rows by Tier.

        Returns
        -------
        list[dict]
            One row per Tier with:
                - Tier
                - TemplateRows
                - TotalLines
                - FirstSL
                - LastSL

        Use case
        --------
        Useful for summary cards, Bootstrap tables, or Bokeh bar plots.
        """
        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()
            return [dict(row) for row in cur.execute("""
                SELECT
                    Tier,
                    COUNT(*) AS TemplateRows,
                    SUM(COALESCE(LNum, 0)) AS TotalLines,
                    MIN(FirstSL) AS FirstSL,
                    MAX(LastSL) AS LastSL
                FROM project_template
                GROUP BY Tier
                ORDER BY Tier
            """).fetchall()]
        finally:
            conn.close()

    def summary_by_rline(self):
        """
        Summarize project template rows by receiver line and tier.

        Returns
        -------
        list[dict]
            Rows grouped by RLine and Tier.

        Use case
        --------
        Useful for checking how many source line ranges belong to each receiver
        line and tier.
        """
        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()
            return [dict(row) for row in cur.execute("""
                SELECT
                    RLine,
                    Tier,
                    COUNT(*) AS TemplateRows,
                    SUM(COALESCE(LNum, 0)) AS TotalLines,
                    MIN(FirstSL) AS FirstSL,
                    MAX(LastSL) AS LastSL
                FROM project_template
                GROUP BY RLine, Tier
                ORDER BY RLine, Tier
            """).fetchall()]
        finally:
            conn.close()

    def find_by_sail_line(self, sail_line):
        """
        Find template rows where a sail line falls inside FirstSL/LastSL range.

        Parameters
        ----------
        sail_line : int
            Sail line number to search.

        Returns
        -------
        list[dict]
            Matching template rows.

        Example
        -------
        find_by_sail_line(17156) returns all rows where:
            FirstSL <= 17156 <= LastSL
        """
        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()
            return [dict(row) for row in cur.execute("""
                SELECT *
                FROM project_template
                WHERE FirstSL <= ?
                  AND LastSL >= ?
                ORDER BY Tier, RLine, FirstSL
            """, (int(sail_line), int(sail_line))).fetchall()]
        finally:
            conn.close()

    def to_table_rows(self):
        """
        Return rows in a format ready for table display.

        Returns
        -------
        list[dict]
            Same output as list_rows().

        Notes
        -----
        This wrapper exists so views can call a semantic method name when they
        are building an HTML table or JSON table endpoint.
        """
        return self.list_rows()

    def to_bokeh_source_rows(self):
        """
        Return data in ColumnDataSource-friendly dictionary format.

        Returns
        -------
        dict[str, list]
            Dictionary where each key is a column name and each value is a list.

        Use case
        --------
        Bokeh ColumnDataSource expects this format:

            source = ColumnDataSource(ptdb.to_bokeh_source_rows())

        This is useful for future interactive plots such as:
            - FirstSL/LastSL range plot
            - LNum by RLine bar plot
            - Tier color-coded template plot
        """
        rows = self.list_rows()

        return {
            "ID": [r["ID"] for r in rows],
            "FirstSL": [r["FirstSL"] for r in rows],
            "LastSL": [r["LastSL"] for r in rows],
            "LNum": [r["LNum"] for r in rows],
            "RLine": [r["RLine"] for r in rows],
            "Tier": [r["Tier"] for r in rows],
            "deployed_by_vessel": [r["deployed_by_vessel"] for r in rows],
            "recovered_by_vessel": [r["recovered_by_vessel"] for r in rows],
            "deployed_vessel_name": [r["deployed_vessel_name"] or "" for r in rows],
            "recovered_vessel_name": [r["recovered_vessel_name"] or "" for r in rows],
        }

    def count_rows(self):
        """
        Count rows in project_template.

        Returns
        -------
        int
            Total number of rows.
        """
        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()
            row = cur.execute("SELECT COUNT(*) AS cnt FROM project_template").fetchone()
            return int(row["cnt"] or 0)
        finally:
            conn.close()

    def render_table_body(self):
        """
        Render project_template rows into HTML using Django template.

        Returns:
            str: HTML string for table body
        """
        rows = self.list_rows()

        html = render_to_string(
            "baseproject/partials/project_template_body.html",
            {
                "rows": rows
            }
        )

        return html

    def visual_offset_table_data(self):
        """
        Build matrix data for Visual Offset Identifier HTML table.

        Columns:
            SLPreplot.Line

        Rows:
            project_template.RLine

        Blue cell:
            SLPreplot.Line between project_template.FirstSL and project_template.LastSL
        """
        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()

            sl_lines = [
                int(row["Line"])
                for row in cur.execute("""
                       SELECT DISTINCT Line
                       FROM SLPreplot
                       WHERE Line IS NOT NULL
                       ORDER BY Line
                   """).fetchall()
            ]

            template_rows = [
                dict(row)
                for row in cur.execute("""
                       SELECT
                           ID,
                           FirstSL,
                           LastSL,
                           LNum,
                           RLine,
                           Tier
                       FROM project_template
                       WHERE FirstSL IS NOT NULL
                         AND LastSL IS NOT NULL
                         AND RLine IS NOT NULL
                       ORDER BY RLine
                   """).fetchall()
            ]

            table_rows = []

            for r in template_rows:
                first_sl = int(r["FirstSL"])
                last_sl = int(r["LastSL"])
                rline = int(r["RLine"])

                lo = min(first_sl, last_sl)
                hi = max(first_sl, last_sl)

                cells = []

                for sl in sl_lines:
                    is_active = lo <= sl <= hi

                    cells.append({
                        "sl": sl,
                        "active": is_active,
                        "label": "---" if is_active else "",
                    })

                table_rows.append({
                    "id": r["ID"],
                    "rline": rline,
                    "first_sl": first_sl,
                    "last_sl": last_sl,
                    "lnum": r["LNum"],
                    "tier": r["Tier"],
                    "cells": cells,
                })

            return {
                "sl_lines": sl_lines,
                "rows": table_rows,
            }

        finally:
            conn.close()

    def visual_offset_status_table_data(self):
        """
        Build matrix data for Template Matrix / Production Status.

        Important group logic:
            Source groups define real display order.

            Example:
                Group 1: 10 -> 1 DESC
                Group 2: 20 -> 11 DESC

                FirstSL = 5
                LastSL  = 14

                Result:
                5,4,3,2,1,14,13,12,11

            If FirstSL and LastSL are inside SAME group:
                Group 2: 48143 -> 39567 DESC
                FirstSL = 48143
                LastSL  = 43375

                Result:
                48143,48111,48079,...,43375
        """

        self.ensure_schema()

        conn = self._connect()
        try:
            cur = conn.cursor()

            # ------------------------------------------------------------
            # Source lines from SLPreplot
            # ------------------------------------------------------------
            raw_sl_lines = [
                int(row["Line"])
                for row in cur.execute("""
                    SELECT DISTINCT Line
                    FROM SLPreplot
                    WHERE Line IS NOT NULL
                """).fetchall()
            ]

            raw_sl_set = set(raw_sl_lines)

            # ------------------------------------------------------------
            # Load SL groups
            # ------------------------------------------------------------
            group_rows = [
                dict(row)
                for row in cur.execute("""
                    SELECT
                        group_no,
                        start_line,
                        end_line,
                        direction
                    FROM project_template_sl_groups
                    WHERE is_active = 1
                    ORDER BY group_no
                """).fetchall()
            ]

            if not group_rows and raw_sl_lines:
                group_rows = [{
                    "group_no": 1,
                    "start_line": min(raw_sl_lines),
                    "end_line": max(raw_sl_lines),
                    "direction": "asc",
                }]

            groups = []

            for idx, g in enumerate(group_rows, start=1):
                group_no = int(g.get("group_no") or idx)
                start_line = int(g["start_line"])
                end_line = int(g["end_line"])
                direction = (g.get("direction") or "asc").lower()

                lo = min(start_line, end_line)
                hi = max(start_line, end_line)

                group_lines = [
                    sl for sl in raw_sl_set
                    if lo <= sl <= hi
                ]

                group_lines = sorted(
                    group_lines,
                    reverse=(direction == "desc"),
                )

                groups.append({
                    "group_no": group_no,
                    "start_line": start_line,
                    "end_line": end_line,
                    "direction": direction,
                    "lines": group_lines,
                })

            groups.sort(key=lambda x: x["group_no"])

            # ------------------------------------------------------------
            # Global SL header order
            # ------------------------------------------------------------
            sl_lines = []
            used_sl = set()

            for g in groups:
                for sl in g["lines"]:
                    if sl not in used_sl:
                        sl_lines.append(sl)
                        used_sl.add(sl)

            for sl in sorted(raw_sl_set):
                if sl not in used_sl:
                    sl_lines.append(sl)

            def find_group(line):
                line = int(line)
                for g in groups:
                    if line in g["lines"]:
                        return g
                return None

            def build_sl_list_by_groups(first_sl, last_sl):
                """
                Expand FirstSL -> LastSL using configured SL groups.

                Rules:
                    - If FirstSL and LastSL are in same group:
                        take FirstSL -> LastSL in group order

                    - First group:
                        take FirstSL -> end of that group

                    - Middle groups:
                        take full group

                    - Last group:
                        take start of that group -> LastSL

                Example:
                    Group 1 = [24217, ..., 17721]
                    Group 2 = [48143, ..., 39567]

                    FirstSL = 20217
                    LastSL  = 45903

                    Result:
                        20217 ... 17721
                        48143 ... 45903
                """

                first_sl = int(first_sl)
                last_sl = int(last_sl)

                first_group = find_group(first_sl)
                last_group = find_group(last_sl)

                if not first_group or not last_group:
                    return []

                first_group_no = int(first_group["group_no"])
                last_group_no = int(last_group["group_no"])

                output = []
                used = set()

                for g in groups:
                    group_no = int(g["group_no"])
                    lines = g["lines"]

                    if group_no < first_group_no or group_no > last_group_no:
                        continue

                    selected = []

                    # FirstSL and LastSL are in the same group
                    if group_no == first_group_no and group_no == last_group_no:
                        if first_sl in lines and last_sl in lines:
                            i1 = lines.index(first_sl)
                            i2 = lines.index(last_sl)

                            if i1 <= i2:
                                selected = lines[i1:i2 + 1]
                            else:
                                selected = lines[i2:i1 + 1]

                    # First group: FirstSL -> end of group
                    elif group_no == first_group_no:
                        if first_sl in lines:
                            i1 = lines.index(first_sl)
                            selected = lines[i1:]

                    # Last group: start of group -> LastSL
                    elif group_no == last_group_no:
                        if last_sl in lines:
                            i2 = lines.index(last_sl)
                            selected = lines[:i2 + 1]

                    # Middle groups: full group
                    else:
                        selected = lines

                    for sl in selected:
                        if sl not in used:
                            output.append(sl)
                            used.add(sl)

                return output

            # ------------------------------------------------------------
            # Template rows
            # ------------------------------------------------------------
            template_rows = [
                dict(row)
                for row in cur.execute("""
                    SELECT
                        ID,
                        FirstSL,
                        LastSL,
                        LNum,
                        RLine,
                        Tier
                    FROM project_template
                    WHERE FirstSL IS NOT NULL
                      AND LastSL IS NOT NULL
                      AND RLine IS NOT NULL
                    ORDER BY RLine
                """).fetchall()
            ]

            # ------------------------------------------------------------
            # Completed source lines
            # ------------------------------------------------------------
            completed_sl_lines = {
                int(row["Line"])
                for row in cur.execute("""
                    SELECT DISTINCT Line
                    FROM SLSolution
                    WHERE Line IS NOT NULL
                """).fetchall()
            }

            # ------------------------------------------------------------
            # SL vessel + production seq list
            # ------------------------------------------------------------
            sl_vessel_map = {}
            sl_seq_values = {}

            prod_rules = [
                dict(row)
                for row in cur.execute("""
                    SELECT
                        seq_first,
                        seq_last,
                        vessel_id
                    FROM sequence_vessel_assignment
                    WHERE is_active = 1
                      AND purpose_id = 1
                """).fetchall()
            ]

            for row in cur.execute("""
                SELECT
                    s.Line,
                    s.Seq,
                    s.Vessel_FK,
                    f.vessel_name AS VesselName
                FROM SLSolution s
                LEFT JOIN project_fleet f
                    ON f.ID = s.Vessel_FK
                WHERE s.Line IS NOT NULL
            """).fetchall():
                line = int(row["Line"])

                if line not in sl_vessel_map:
                    sl_vessel_map[line] = row["VesselName"] or ""

                seq = row["Seq"]
                vessel_fk = row["Vessel_FK"]

                if seq is None or vessel_fk is None:
                    continue

                seq = int(seq)

                for rule in prod_rules:
                    if (
                            int(rule["vessel_id"]) == int(vessel_fk)
                            and int(rule["seq_first"]) <= seq <= int(rule["seq_last"])
                    ):
                        sl_seq_values.setdefault(line, set()).add(seq)
                        break

            sl_seq_map = {
                line: ",".join(str(s) for s in sorted(seq_set))
                for line, seq_set in sl_seq_values.items()
            }

            # ------------------------------------------------------------
            # Receiver line status
            # ------------------------------------------------------------
            rline_status = {}
            rline_vessel_map = {}

            for row in cur.execute("""
                SELECT
                    Line,
                    COALESCE(DeployedPct, 0) AS DeployedPct,
                    COALESCE(RetrievedPct, 0) AS RetrievedPct,
                    Vessel_name
                FROM DSR_LineSummary
                WHERE Line IS NOT NULL
            """).fetchall():
                line = int(row["Line"])
                deployed_pct = float(row["DeployedPct"] or 0)
                retrieved_pct = float(row["RetrievedPct"] or 0)

                rline_status[line] = {
                    "deployed_pct": deployed_pct,
                    "retrieved_pct": retrieved_pct,
                    "is_deployed": deployed_pct >= 100,
                    "is_recovered": retrieved_pct >= 100,
                }

                rline_vessel_map[line] = row["Vessel_name"] or ""

            deployed_r_lines = {
                line
                for line, status in rline_status.items()
                if status.get("is_deployed")
            }

            # ------------------------------------------------------------
            # Build matrix rows
            # ------------------------------------------------------------
            sl_required_rlines = {sl: set() for sl in sl_lines}
            table_rows = []

            for r in template_rows:
                first_sl = int(r["FirstSL"])
                last_sl = int(r["LastSL"])
                rline = int(r["RLine"])

                active_sl_list = build_sl_list_by_groups(first_sl, last_sl)
                active_sl_set = set(active_sl_list)

                planned_count = len(active_sl_set)
                completed_count = len(active_sl_set.intersection(completed_sl_lines))
                all_completed = planned_count > 0 and planned_count == completed_count

                cells = []

                for sl in sl_lines:
                    in_range = sl in active_sl_set
                    completed = sl in completed_sl_lines

                    if in_range:
                        sl_required_rlines[sl].add(rline)

                    cells.append({
                        "sl": sl,
                        "in_range": in_range,
                        "completed": completed,
                        "label": "---" if in_range else "",
                    })

                status = rline_status.get(rline, {})
                rline_deployed = bool(status.get("is_deployed"))
                rline_recovered = bool(status.get("is_recovered"))

                if rline_recovered:
                    rline_status_class = "vo-rline-recovered"
                elif all_completed:
                    rline_status_class = "vo-rline-ready-recovery"
                elif rline_deployed:
                    rline_status_class = "vo-rline-deployed"
                else:
                    rline_status_class = "vo-rline-preplot"

                vessel_name = rline_vessel_map.get(rline, "")

                vessel_class = (
                        "vo-vessel-" +
                        vessel_name.lower()
                        .replace(" ", "-")
                        .replace("_", "-")
                        .replace("/", "-")
                ) if vessel_name else "vo-vessel-empty"

                table_rows.append({
                    "id": r["ID"],
                    "rline": rline,
                    "vessel": vessel_name,
                    "vessel_class": vessel_class,
                    "first_sl": first_sl,
                    "last_sl": last_sl,
                    "lnum": r["LNum"],
                    "tier": r["Tier"],
                    "planned_count": planned_count,
                    "completed_count": completed_count,
                    "all_completed": all_completed,
                    "rline_deployed": rline_deployed,
                    "rline_recovered": rline_recovered,
                    "rline_status_class": rline_status_class,
                    "deployed_pct": status.get("deployed_pct", 0),
                    "retrieved_pct": status.get("retrieved_pct", 0),
                    "cells": cells,
                })

            # ------------------------------------------------------------
            # Build SL headers
            # ------------------------------------------------------------
            sl_headers = []
            prev_group_no = None

            for sl in sl_lines:
                required_rlines = sl_required_rlines.get(sl, set())

                deployed_required_rlines = {
                    rline for rline in required_rlines
                    if rline in deployed_r_lines
                }

                all_receivers_deployed = (
                        len(required_rlines) > 0
                        and len(required_rlines) == len(deployed_required_rlines)
                )

                source_completed = sl in completed_sl_lines

                current_group_no = None
                for g in groups:
                    if sl in g["lines"]:
                        current_group_no = g["group_no"]
                        break

                is_group_start = (
                        current_group_no is not None
                        and current_group_no != prev_group_no
                )
                prev_group_no = current_group_no

                sl_headers.append({
                    "line": sl,
                    "vessel": sl_vessel_map.get(sl, ""),
                    "seq": sl_seq_map.get(sl, ""),
                    "required_rline_count": len(required_rlines),
                    "deployed_rline_count": len(deployed_required_rlines),
                    "all_receivers_deployed": all_receivers_deployed,
                    "source_completed": source_completed,
                    "group_no": current_group_no,
                    "is_group_start": is_group_start,
                })

            return {
                "sl_headers": sl_headers,
                "rows": table_rows,
                "sl_count": len(sl_lines),
                "rline_count": len(table_rows),
            }

        finally:
            conn.close()
