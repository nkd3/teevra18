# C:\teevra18\scripts\fix_universe_watchlist.py
import os, sqlite3, shutil
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

REQUIRED_COLS = ["id", "exchange_segment", "security_id", "is_active", "is_hot_option"]

CREATE_TARGET = """
CREATE TABLE IF NOT EXISTS universe_watchlist__new (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  exchange_segment INTEGER NOT NULL,
  security_id INTEGER NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  is_hot_option INTEGER NOT NULL DEFAULT 0
);
"""

def table_exists(conn, name):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return cur.fetchone() is not None

def table_cols(conn, name):
    try:
        cur = conn.execute(f"PRAGMA table_info({name});")
        return [r[1] for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return None

def backup_db():
    if DB_PATH.exists():
        bak = DB_PATH.with_suffix(".bak_watchlist")
        shutil.copy2(DB_PATH, bak)
        print(f"[BACKUP] {bak}")

def migrate_watchlist():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        if not table_exists(conn, "universe_watchlist"):
            # Fresh create with final schema
            conn.executescript(CREATE_TARGET.replace("__new", ""))
            print("[CREATE] universe_watchlist created with target schema.")
            return

        existing_cols = table_cols(conn, "universe_watchlist") or []
        missing = [c for c in REQUIRED_COLS if c not in existing_cols]
        if not missing:
            print("[OK] universe_watchlist already has the required columns:", existing_cols)
            return

        print(f"[MIGRATE] universe_watchlist missing columns: {missing}. Performing safe swap.")
        # Create new table with correct schema
        conn.executescript(CREATE_TARGET)

        # Best-effort copy: if legacy table has exchange_segment/security_id columns, copy them; else default values
        # Build SELECT list
        sel_exchange_segment = "exchange_segment" if "exchange_segment" in existing_cols else "2 AS exchange_segment"
        sel_security_id = "security_id" if "security_id" in existing_cols else "NULL AS security_id"
        sel_is_active = "is_active" if "is_active" in existing_cols else "1 AS is_active"
        sel_is_hot_option = "is_hot_option" if "is_hot_option" in existing_cols else "0 AS is_hot_option"

        try:
            conn.execute(f"""
                INSERT INTO universe_watchlist__new(exchange_segment, security_id, is_active, is_hot_option)
                SELECT {sel_exchange_segment}, {sel_security_id}, {sel_is_active}, {sel_is_hot_option}
                FROM universe_watchlist;
            """)
            print("[MIGRATE] Copied rows from legacy universe_watchlist where possible.")
        except Exception as e:
            print("[MIGRATE] Copy step skipped due to:", e)

        # Swap tables
        conn.execute("DROP TABLE universe_watchlist;")
        conn.execute("ALTER TABLE universe_watchlist__new RENAME TO universe_watchlist;")
        print("[SWAP] Replaced old universe_watchlist with target schema.")

if __name__ == "__main__":
    backup_db()
    migrate_watchlist()
    print("[DONE] universe_watchlist schema ensured.")
