# -*- coding: utf-8 -*-
from pathlib import Path
import sqlite3, datetime

DB = Path(r"C:\teevra18\data\teevra18.db")

with sqlite3.connect(DB) as conn:
    conn.execute("""
      INSERT INTO backtest_orders (run_id, symbol, ts, side, qty, price, pnl, notes)
      VALUES (?,?,?,?,?,?,?,?)
    """, (1, "NIFTY", datetime.datetime.now().isoformat(timespec="seconds"), "buy", 50, 23250.0, 125.0, "seed row"))
    conn.commit()

print("Inserted 1 demo row into backtest_orders.")
