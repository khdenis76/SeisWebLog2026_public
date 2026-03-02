import sqlite3
from pathlib import Path

ALLOWED_SEQUENCE_PURPOSES = (
    "Production",                #1
    "Non-Production",            #2
    "Non-Production-Infill",     #3
    "Production-Infill",         #4
    "Test",                      #5
    "Other",                     #6
)

# id → label mapping
PURPOSE_ID_TO_LABEL = {
    i + 1: label for i, label in enumerate(ALLOWED_SEQUENCE_PURPOSES)
}

# label → id mapping
PURPOSE_LABEL_TO_ID = {
    label: i + 1 for i, label in enumerate(ALLOWED_SEQUENCE_PURPOSES)
}


class VesselSeqRelations:

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    # -----------------------------
    # TABLE
    # -----------------------------

    def ensure_sequence_vessel_tables(self):
        con = self._connect()
        cur = con.cursor()

        cur.executescript("""
        CREATE TABLE IF NOT EXISTS sequence_vessel_assignment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            seq_first INTEGER NOT NULL,
            seq_last  INTEGER NOT NULL,

            vessel_id INTEGER NOT NULL,

            purpose_id INTEGER NOT NULL DEFAULT 1,

            comments TEXT,
            is_active INTEGER DEFAULT 1,

            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT,

            FOREIGN KEY (vessel_id) REFERENCES project_fleet(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_seq_vessel_range
        ON sequence_vessel_assignment(seq_first, seq_last);

        CREATE INDEX IF NOT EXISTS idx_seq_vessel_vessel_id
        ON sequence_vessel_assignment(vessel_id);
        """)

        con.commit()
        con.close()

    # -----------------------------
    # LIST
    # -----------------------------

    def list_sequence_vessel_assignments(self, active_only=False):
        con = self._connect()
        cur = con.cursor()

        where = "WHERE a.is_active = 1" if active_only else ""

        cur.execute(f"""
            SELECT
                a.*,
                COALESCE(pf.vessel_name, '') AS vessel_name
            FROM sequence_vessel_assignment a
            LEFT JOIN project_fleet pf ON pf.id = a.vessel_id
            {where}
            ORDER BY a.seq_first, a.seq_last
        """)

        rows = []
        for r in cur.fetchall():
            d = dict(r)
            d["purpose_label"] = PURPOSE_ID_TO_LABEL.get(d["purpose_id"], "Unknown")
            rows.append(d)

        con.close()
        return rows

    # -----------------------------
    # VALIDATION
    # -----------------------------

    def _validate_seq_range(self, seq_first, seq_last):
        try:
            a = int(seq_first)
            b = int(seq_last)
        except Exception:
            return False, "Sequence first/last must be integers"

        if a <= 0 or b <= 0:
            return False, "Sequence numbers must be > 0"

        if a > b:
            return False, "seq_first must be <= seq_last"

        return True, ""

    def _validate_purpose_id(self, purpose_id):
        try:
            pid = int(purpose_id)
        except Exception:
            return False, "Invalid purpose_id"

        if pid not in PURPOSE_ID_TO_LABEL:
            return False, "Invalid purpose_id"

        return True, pid

    # -----------------------------
    # ADD
    # -----------------------------

    def add_sequence_vessel_assignment(
            self,
            seq_first,
            seq_last,
            vessel_id,
            purpose_id,
            comments="",
            is_active=1,
            allow_overlap=False
    ):

        ok, msg = self._validate_seq_range(seq_first, seq_last)
        if not ok:
            return {"ok": False, "error": msg}

        okp, pid = self._validate_purpose_id(purpose_id)
        if not okp:
            return {"ok": False, "error": pid}

        con = self._connect()
        cur = con.cursor()

        cur.execute("""
            INSERT INTO sequence_vessel_assignment
            (seq_first, seq_last, vessel_id, purpose_id, comments, is_active, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            int(seq_first),
            int(seq_last),
            int(vessel_id),
            pid,
            (comments or "").strip(),
            1 if is_active else 0
        ))

        new_id = cur.lastrowid
        con.commit()
        con.close()

        return {"ok": True, "id": new_id}

    # -----------------------------
    # UPDATE
    # -----------------------------

    def update_sequence_vessel_assignment(
            self,
            row_id,
            seq_first,
            seq_last,
            vessel_id,
            purpose_id,
            comments="",
            is_active=1,
            allow_overlap=False
    ):

        ok, msg = self._validate_seq_range(seq_first, seq_last)
        if not ok:
            return {"ok": False, "error": msg}

        okp, pid = self._validate_purpose_id(purpose_id)
        if not okp:
            return {"ok": False, "error": pid}

        con = self._connect()
        cur = con.cursor()

        cur.execute("""
            UPDATE sequence_vessel_assignment
            SET seq_first=?,
                seq_last=?,
                vessel_id=?,
                purpose_id=?,
                comments=?,
                is_active=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, (
            int(seq_first),
            int(seq_last),
            int(vessel_id),
            pid,
            (comments or "").strip(),
            1 if is_active else 0,
            int(row_id)
        ))

        con.commit()
        con.close()

        return {"ok": True}

    def list_project_fleet_simple(self, active_only=True):
        con = self._connect()
        cur = con.cursor()
        where = "WHERE is_active = 1" if active_only else ""
        cur.execute(f"""
            SELECT id, vessel_name
            FROM project_fleet
            {where}
            ORDER BY vessel_name
        """)
        rows = [dict(r) for r in cur.fetchall()]
        con.close()
        return rows