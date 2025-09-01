# C:\teevra18\tools\repair_universe_tables.py
import os, sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

def column_exists(cur, table, col):
    cur.execute(f"PRAGMA table_info({table});")
    return any(r[1] == col for r in cur.fetchall())

def table_exists(cur, table):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,))
    return cur.fetchone() is not None

def ensure_universe_derivatives(cur):
    # Create if missing (minimal)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS universe_derivatives(
        symbol TEXT,
        lot_size REAL
    );
    """)
    # Add ts_utc if missing
    if not column_exists(cur, "universe_derivatives", "ts_utc"):
        cur.execute("ALTER TABLE universe_derivatives ADD COLUMN ts_utc TEXT;")
        print("[OK] Added ts_utc to universe_derivatives")
    else:
        print("[SKIP] ts_utc already exists on universe_derivatives")

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if not table_exists(cur, "universe_derivatives"):
        print("[INFO] universe_derivatives did not exist; creating…")
    ensure_universe_derivatives(cur)
    conn.commit()
    conn.close()
    print("Repair complete.")

if __name__ == "__main__":
    main()
