import sqlite3, sys, pathlib

DB_PATH = r"C:\teevra18\data\teevra18.db"
SQL_FILE = r"C:\teevra18\data\universe_seed.sql"

if not pathlib.Path(DB_PATH).exists():
    print(f"[ERR] Database not found: {DB_PATH}")
    sys.exit(1)
if not pathlib.Path(SQL_FILE).exists():
    print(f"[ERR] SQL file not found: {SQL_FILE}")
    sys.exit(1)

with open(SQL_FILE, "r", encoding="utf-8") as f:
    sql = f.read()

con = sqlite3.connect(DB_PATH)
cur = con.cursor()
cur.executescript(sql)
con.commit()
con.close()

print(f"[OK] Applied {SQL_FILE} into {DB_PATH}")
