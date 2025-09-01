import sqlite3, os
from pathlib import Path

DB = Path(os.getenv('DB_PATH', r'C:\teevra18\data\teevra18.db'))

REQUIRED = [
    ('ts_utc',    'TEXT'),     # NOT NULL is okay if existing rows already have it; we won't re-assert constraints here
    ('component', 'TEXT'),
    ('status',    'TEXT'),
    ('rows',      'INTEGER'),
    ('warns',     'TEXT'),
    ('extra',     'TEXT'),
]

with sqlite3.connect(DB) as c:
    # Ensure table exists at least with ts_utc
    c.execute("CREATE TABLE IF NOT EXISTS ops_log (id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT)")
    # Discover current columns
    cols = { r[1] for r in c.execute("PRAGMA table_info(ops_log)") }
    # Add any missing columns
    for name, typ in REQUIRED:
        if name not in cols:
            c.execute(f"ALTER TABLE ops_log ADD COLUMN {name} {typ}")
    # Helpful index
    c.execute("CREATE INDEX IF NOT EXISTS idx_opslog_ts ON ops_log(ts_utc)")
    c.commit()

    # Show final shape
    print("ops_log columns now:")
    for r in c.execute("PRAGMA table_info(ops_log)"):
        print(" ", r)
