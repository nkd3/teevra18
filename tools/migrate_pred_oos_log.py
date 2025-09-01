import os, sqlite3
from pathlib import Path

DB = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

REQUIRED_COLS = [
    ("instrument",  "TEXT"),
    ("ts_utc",      "TEXT"),
    ("prob_up",     "REAL"),
    ("exp_move_abs","REAL"),
    ("rr_est",      "REAL"),
    ("sl_per_lot",  "REAL"),
    ("signal_id",   "INTEGER"),
    ("label",       "INTEGER"),
    ("realized_at", "TEXT"),
    ("notes",       "TEXT")
]

def table_cols(conn, table):
    cols = {}
    for cid, name, ctype, notnull, dflt, pk in conn.execute(f"PRAGMA table_info({table});"):
        cols[name] = ctype.upper()
    return cols

with sqlite3.connect(DB) as conn:
    # Ensure table exists; if not, create full schema now.
    conn.execute("""
    CREATE TABLE IF NOT EXISTS pred_oos_log (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at    TEXT NOT NULL
    );""")
    conn.commit()

    existing = table_cols(conn, "pred_oos_log")
    added = []
    for col, ctype in REQUIRED_COLS:
        if col not in existing:
            conn.execute(f"ALTER TABLE pred_oos_log ADD COLUMN {col} {ctype};")
            added.append(col)
    conn.commit()
    print("[OK] pred_oos_log migration complete. Added:", ", ".join(added) if added else "(none)")
