# C:\teevra18\tools\repair_signals_rr_columns.py
import sqlite3
from pathlib import Path
import os

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

REQUIRED_COLS = {
    "sl_price": "REAL",
    "tp_price": "REAL",
    "rr_ratio": "REAL",
    "rr_validated": "INTEGER",       # 1=valid, 0=reject, NULL=pending
    "rr_reject_reason": "TEXT"
}

# Helpful, but only added if missing:
OPTIONAL_COLS = {
    "direction": "TEXT",
    "entry_price": "REAL",
    "lot_size": "REAL"
}

def get_existing_cols(cur, table):
    cur.execute(f"PRAGMA table_info({table});")
    return {row[1] for row in cur.fetchall()}

def add_column_if_missing(cur, table, col, coltype):
    cur.execute(f"PRAGMA table_info({table});")
    cols = {row[1] for row in cur.fetchall()}
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype};")
        print(f"[OK] Added column {col} {coltype}")
    else:
        print(f"[SKIP] Column {col} already exists")

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found at {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Check that signals exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signals';")
    if not cur.fetchone():
        conn.close()
        raise SystemExit("Table 'signals' not found. Run M7 first.")

    # Add required columns
    for c, t in REQUIRED_COLS.items():
        add_column_if_missing(cur, "signals", c, t)

    # Add optional columns if they’re missing (no data harm)
    for c, t in OPTIONAL_COLS.items():
        add_column_if_missing(cur, "signals", c, t)

    conn.commit()
    conn.close()
    print("Schema repair complete.")

if __name__ == "__main__":
    main()
