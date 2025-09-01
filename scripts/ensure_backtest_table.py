# -*- coding: utf-8 -*-
from pathlib import Path
import sqlite3

DB = Path(r"C:\teevra18\data\teevra18.db")
DB.parent.mkdir(parents=True, exist_ok=True)

DDL = """
CREATE TABLE IF NOT EXISTS backtest_orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER,                -- links to runs.id (optional)
  symbol TEXT NOT NULL,
  ts TEXT,                       -- ISO datetime string (optional)
  side TEXT,                     -- buy/sell
  qty REAL,
  price REAL,
  pnl REAL,
  notes TEXT
);
"""

with sqlite3.connect(DB) as conn:
    conn.execute(DDL)
    conn.commit()

print("OK: backtest_orders table ensured at", DB)
