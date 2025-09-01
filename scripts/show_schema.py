# -*- coding: utf-8 -*-
import json, sqlite3
from pathlib import Path

CFG = json.loads(Path(r"C:\teevra18\teevra18.config.json").read_text(encoding="utf-8"))
DB = CFG["db_path"]
tables = ["backtest_orders","paper_orders","exec_trades","kpi_summary","strategy_configs"]

con = sqlite3.connect(DB)
cur = con.cursor()
for t in tables:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (t,))
    if not cur.fetchone():
        print(f"[MISSING] {t}")
        continue
    print(f"\n== {t} ==")
    cur.execute(f"PRAGMA table_info({t})")
    for _, name, typ, nn, dflt, pk in cur.fetchall():
        print(f"- {name} ({typ}){' PK' if pk else ''}")
con.close()
