# C:\teevra18\scripts\peek_universe.py
import os, sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

with sqlite3.connect(DB_PATH) as conn:
    cur = conn.execute("SELECT * FROM universe_option_underlyings LIMIT 20;")
    for row in cur.fetchall():
        print(row)
