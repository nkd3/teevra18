# -*- coding: utf-8 -*-
import json, sqlite3
from pathlib import Path
cfg = json.loads(Path(r"C:\teevra18\teevra18.config.json").read_text(encoding="utf-8"))
con = sqlite3.connect(cfg["db_path"]); cur = con.cursor()
print("\nTop 5 paper_orders (key fields):")
cur.execute("""SELECT id, filled_ts_utc, closed_ts_utc, lots, lot_size,
                      fill_price, exit_price, charges_at_fill, charges_at_exit,
                      pnl_gross, pnl_net
               FROM paper_orders ORDER BY COALESCE(closed_ts_utc, filled_ts_utc) DESC LIMIT 5""")
for r in cur.fetchall(): print(r)
print("\nLast 5 exec_trades for Paper:")
cur.execute("""SELECT trade_id, qty_lots, entry_time, exit_time, entry_price, exit_price, fees, pnl
               FROM exec_trades WHERE stage='Paper' ORDER BY rowid DESC LIMIT 5""")
for r in cur.fetchall(): print(r)
con.close()
