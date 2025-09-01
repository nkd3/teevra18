# C:\teevra18\scripts\db_migrate_m12.py
import json, sqlite3, os
from pathlib import Path

CFG = Path(r"C:\teevra18\teevra18.config.json")
cfg = json.loads(Path(CFG).read_text(encoding="utf-8"))
DB = cfg["db_path"]

DDL = [
"""
CREATE TABLE IF NOT EXISTS strategy_configs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  stage TEXT NOT NULL CHECK(stage IN ('Backtest','Paper','Live-Ready')),
  version INTEGER NOT NULL DEFAULT 1,
  is_active INTEGER NOT NULL DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now')),
  notes TEXT DEFAULT ''
);
""",
"""
CREATE TABLE IF NOT EXISTS strategy_params (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  config_id INTEGER NOT NULL,
  param_key TEXT NOT NULL,
  param_value TEXT NOT NULL,
  FOREIGN KEY(config_id) REFERENCES strategy_configs(id)
);
""",
"""
CREATE TABLE IF NOT EXISTS risk_policies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  config_id INTEGER NOT NULL,
  capital_mode TEXT NOT NULL CHECK(capital_mode IN ('Fixed','Dynamic')),
  fixed_capital REAL DEFAULT 150000,
  risk_per_trade_pct REAL DEFAULT 1.0,
  max_trades_per_day INTEGER DEFAULT 5,
  rr_min REAL DEFAULT 2.0,
  sl_max_per_lot REAL DEFAULT 1000.0,
  daily_loss_limit REAL DEFAULT 0.0,
  group_exposure_cap_pct REAL DEFAULT 100.0,
  breaker_threshold REAL DEFAULT 0.0,
  trading_windows TEXT DEFAULT '09:20-15:20',
  FOREIGN KEY(config_id) REFERENCES strategy_configs(id)
);
""",
"""
CREATE TABLE IF NOT EXISTS liquidity_filters (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  config_id INTEGER NOT NULL,
  min_oi INTEGER DEFAULT 0,
  min_volume INTEGER DEFAULT 0,
  max_spread_paisa INTEGER DEFAULT 50,
  slippage_bps INTEGER DEFAULT 5,
  fees_per_lot REAL DEFAULT 30.0,
  FOREIGN KEY(config_id) REFERENCES strategy_configs(id)
);
""",
"""
CREATE TABLE IF NOT EXISTS notif_settings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  config_id INTEGER NOT NULL,
  telegram_enabled INTEGER DEFAULT 1,
  t_bot_token TEXT,
  t_chat_id TEXT,
  eod_summary INTEGER DEFAULT 1,
  FOREIGN KEY(config_id) REFERENCES strategy_configs(id)
);
""",
"""
CREATE TABLE IF NOT EXISTS config_versions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  config_id INTEGER NOT NULL,
  snapshot_json TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now')),
  label TEXT DEFAULT '',
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
    print("M12 migration complete.")
  finally:
    con.close()

if __name__ == "__main__":
  main()
