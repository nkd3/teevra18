@'
# -*- coding: utf-8 -*-
import json, sqlite3
from pathlib import Path

CFG = json.loads(Path(r"C:\teevra18\teevra18.config.json").read_text(encoding="utf-8"))
DB = CFG["db_path"]

def show_table(cur, name):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    if not cur.fetchone():
        print(f"[MISSING] {name}")
        return
    print(f"\n== {name} ==")
    cur.execute(f"PRAGMA table_info({name})")
    cols = [r[1] for r in cur.fetchall()]
    print("Columns:", cols)
    try:
        cur.execute(f"SELECT * FROM {name} LIMIT 5")
        rows = cur.fetchall()
        if rows:
            print("Sample rows:")
            for row in rows:
                print(tuple(row))
        else:
            print("No rows.")
    except Exception as e:
        print("Read error:", e)
    # Show distinct config_id if present
    if "config_id" in cols:
        try:
            cur.execute(f"SELECT DISTINCT config_id, COUNT(*) FROM {name} GROUP BY config_id ORDER BY 2 DESC")
            print("config_id distribution:")
            for r in cur.fetchall():
                print(r)
        except Exception as e:
            print("config_id scan error:", e)

con = sqlite3.connect(DB)
cur = con.cursor()
for t in ("backtest_orders","paper_orders"):
    show_table(cur, t)
con.close()
'@ | Set-Content -Encoding UTF8 C:\teevra18\scripts\peek_orders.py

& C:\teevra18\.venv\Scripts\Activate.ps1
python C:\teevra18\scripts\peek_orders.py
