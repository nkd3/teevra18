# -*- coding: utf-8 -*-
import json, sqlite3
from pathlib import Path
cfg = json.loads(Path(r"C:\teevra18\teevra18.config.json").read_text(encoding="utf-8"))
con = sqlite3.connect(cfg["db_path"]); cur = con.cursor()
cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_exec_trade_unique ON exec_trades(stage,config_id,trade_id)")
con.commit(); con.close()
print("OK: unique index ux_exec_trade_unique ensured.")
