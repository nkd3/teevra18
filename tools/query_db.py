# C:\teevra18\tools\query_db.py
import os, sys, sqlite3, pandas as pd
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python query_db.py \"<SQL>\"")
        raise SystemExit(1)
    sql = " ".join(sys.argv[1:]).strip()
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql(sql, conn)
    with pd.option_context("display.max_rows", 200, "display.width", 180):
        print(df)
