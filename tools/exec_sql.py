# C:\teevra18\tools\exec_sql.py
import os, sys, sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

if __name__ == "__main__":
    sql = " ".join(sys.argv[1:]).strip()
    if not sql:
        print("Usage: exec_sql.py \"<SQL>\"")
        raise SystemExit(1)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute(sql)
    conn.commit()
    conn.close()
    print("[OK] Executed on", DB_PATH)
