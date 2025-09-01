# C:\teevra18\scripts\db_migrate_kpis_m12.py
import json, sqlite3, os
from pathlib import Path

CFG = Path(r"C:\teevra18\teevra18.config.json")
cfg = json.loads(CFG.read_text(encoding="utf-8"))
DB = cfg["db_path"]

DDL = [
# Universal executed trades table (you can load Backtest or Paper fills here)
"""
CREATE TABLE IF NOT EXISTS exec_trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  stage TEXT NOT NULL CHECK(stage IN ('Backtest','Paper','Live-Ready')),
  config_id INTEGER NOT NULL,
  trade_id TEXT NOT NULL,
  side TEXT NOT NULL CHECK(side IN ('LONG','SHORT')),
  qty_lots INTEGER NOT NULL,
  entry_time TEXT NOT NULL,
  exit_time TEXT NOT NULL,
  entry_price REAL NOT NULL,
  exit_price REAL NOT NULL,
  fees REAL DEFAULT 0.0,
  pnl REAL NOT NULL,
  FOREIGN KEY(config_id) REFERENCES strategy_configs(id)
);
""",
# KPI summary per config + stage + batch label
"""
CREATE TABLE IF NOT EXISTS kpi_summary (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  stage TEXT NOT NULL CHECK(stage IN ('Backtest','Paper','Live-Ready')),
  config_id INTEGER NOT NULL,
  label TEXT NOT NULL,
  trades_count INTEGER NOT NULL,
  win_rate REAL NOT NULL,
  profit_factor REAL NOT NULL,
  avg_trade REAL NOT NULL,
  expectancy REAL NOT NULL,
  max_drawdown_pct REAL NOT NULL,
  gross_profit REAL NOT NULL,
  gross_loss REAL NOT NULL,
  net_pnl REAL NOT NULL,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY(config_id) REFERENCES strategy_configs(id)
);
"""
]

def main():
  os.makedirs(os.path.dirname(DB), exist_ok=True)
  con = sqlite3.connect(DB)
  try:
    cur = con.cursor()
    for ddl in DDL:
      cur.execute(ddl)
    con.commit()
    print("KPI migration complete.")
  finally:
    con.close()

if __name__ == "__main__":
  main()
