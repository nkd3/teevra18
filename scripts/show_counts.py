# -*- coding: utf-8 -*-
import json, sqlite3
from pathlib import Path
cfg = json.loads(Path(r"C:\teevra18\teevra18.config.json").read_text(encoding="utf-8"))
con = sqlite3.connect(cfg["db_path"]); cur = con.cursor()
for t in ("backtest_orders","paper_orders"):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (t,))
    if not cur.fetchone():
        print(f"[MISSING] {t}"); continue
    cur.execute(f"SELECT COUNT(*) FROM {t}"); cnt = cur.fetchone()[0]
    print(f"{t}: {cnt} rows")
con.close()
