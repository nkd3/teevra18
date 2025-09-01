# C:\teevra18\scripts\m9_migrate_ops_log_idempotent.py
import sqlite3

DB = r"C:\teevra18\data\teevra18.db"

NEEDED = [
    ("id",        "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("ts_utc",    "TEXT NOT NULL DEFAULT (datetime('now'))"),
    ("source",    "TEXT"),
    ("level",     "TEXT"),
    ("event",     "TEXT"),
    ("ref_table", "TEXT"),
    ("ref_id",    "INTEGER"),
    ("message",   "TEXT"),
]

def table_exists(conn, name):
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())

def get_cols(conn, table):
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()}

def create_table(conn):
    ddl = """
    CREATE TABLE IF NOT EXISTS ops_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts_utc TEXT NOT NULL DEFAULT (datetime('now')),
      source TEXT,
      level TEXT,
      event TEXT,
      ref_table TEXT,
      ref_id INTEGER,
      message TEXT
    );
    """
    conn.executescript(ddl)

def add_missing(conn):
    cols = get_cols(conn, "ops_log")
    alters = []
    for col, decl in NEEDED:
        if col not in cols:
            # Only id & ts_utc are special; others can be nullable so inserts supply values.
            if col == "id":
                # Can't add a PK col after the fact; skip if missing (means table already has a PK named differently).
                continue
            alters.append(f"ALTER TABLE ops_log ADD COLUMN {col} {decl};")
    if alters:
        conn.executescript("\n".join(alters))

def main():
    conn = sqlite3.connect(DB)
    try:
        if not table_exists(conn, "ops_log"):
            create_table(conn)
            conn.commit()
            print("[M9] Created ops_log with modern schema.")
            return
        add_missing(conn)
        conn.commit()
        print("[M9] ops_log migrated idempotently.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
