import os, sqlite3
from pathlib import Path

DB = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

REQUIRED = [
    ("created_at",  "TEXT"),
    ("instrument",  "TEXT"),
    ("ts_utc",      "TEXT"),
    ("prob_up",     "REAL"),
    ("exp_move_abs","REAL"),
    ("rr_est",      "REAL"),
    ("sl_per_lot",  "REAL"),
    ("signal_id",   "INTEGER"),
    ("label",       "INTEGER"),
    ("realized_at", "TEXT"),
    ("notes",       "TEXT"),
]

def cols(conn, table):
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()}

with sqlite3.connect(DB) as conn:
    # Ensure table exists (minimal)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS pred_oos_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT
    );
    """)
    existing = cols(conn, "pred_oos_log")
    added = []
    for name, ctype in REQUIRED:
        if name not in existing:
            conn.execute(f"ALTER TABLE pred_oos_log ADD COLUMN {name} {ctype};")
            added.append(name)
    conn.commit()
    print("[OK] pred_oos_log migration complete. Added:", ", ".join(added) if added else "(none)")
