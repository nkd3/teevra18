# C:\teevra18\tools\repair_quote_snap.py
import os, sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

REQUIRED = [
    ("symbol", "TEXT"),
    ("ltp",    "REAL"),
    ("bid",    "REAL"),
    ("ask",    "REAL"),
    ("ts_utc", "TEXT"),
]

def get_cols(cur, table):
    cur.execute(f"PRAGMA table_info({table});")
    rows = cur.fetchall()
    return {r[1]: (r[2] or "").upper() for r in rows}  # name -> type

def table_exists(cur, table):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,))
    return cur.fetchone() is not None

def ensure_table(conn):
    cur = conn.cursor()
    if not table_exists(cur, "quote_snap"):
        cur.execute("""
        CREATE TABLE quote_snap(
            symbol TEXT,
            ltp    REAL,
            bid    REAL,
            ask    REAL,
            ts_utc TEXT
        );
        """)
        conn.commit()
        print("[OK] Created quote_snap (fresh).")
        return

    # Table exists; add any missing columns.
    cols = get_cols(cur, "quote_snap")
    added_any = False
    for name, ctype in REQUIRED:
        if name not in cols:
            cur.execute(f"ALTER TABLE quote_snap ADD COLUMN {name} {ctype};")
            print(f"[OK] Added column {name} {ctype}")
            added_any = True

    if not added_any:
        print("[SKIP] quote_snap already has required columns.")

    conn.commit()

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)
    conn.close()
    print("quote_snap repair complete.")

if __name__ == "__main__":
    main()
