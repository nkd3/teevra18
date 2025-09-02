import sqlite3
from pathlib import Path

DB_PATH = r"C:\teevra18\data\teevra18.db"
TABLE = "strategies_catalog"
COLUMN = "source_mode"
DDL = f"ALTER TABLE {TABLE} ADD COLUMN {COLUMN} TEXT DEFAULT 'graphical'"

db = Path(DB_PATH)
if not db.exists():
    raise SystemExit(f"[ERR] DB not found at {DB_PATH}")

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# check existing columns
cur.execute(f"PRAGMA table_info({TABLE})")
cols = [row[1] for row in cur.fetchall()]

if COLUMN in cols:
    print(f"[OK] Column '{COLUMN}' already exists in '{TABLE}'. Nothing to do.")
else:
    cur.execute(DDL)
    con.commit()
    print(f"[OK] Added column '{COLUMN}' to '{TABLE}' with DEFAULT 'graphical'.")

con.close()
