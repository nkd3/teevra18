# -*- coding: utf-8 -*-
import json, sqlite3
from pathlib import Path
CFG=json.loads(Path(r"C:\teevra18\teevra18.config.json").read_text(encoding="utf-8"))
con=sqlite3.connect(CFG["db_path"]); cur=con.cursor()
cur.execute("SELECT trade_id, side, qty_lots, entry_price, exit_price, fees, pnl FROM exec_trades WHERE stage='Paper' AND config_id=2 ORDER BY rowid DESC LIMIT 3")
for r in cur.fetchall(): print(r)
con.close()
