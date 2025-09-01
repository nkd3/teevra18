import os, sqlite3, json, time
from datetime import datetime
from pathlib import Path

# Load .env (lightweight manual parse to avoid extra deps during early bootstrap)
ENV_PATH = Path(r"C:\teevra18\config\.env")
env = {}
if ENV_PATH.exists():
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line=line.strip()
        if line and not line.startswith("#") and "=" in line:
            k,v=line.split("=",1)
            env[k.strip()]=v.strip()

DB_PATH = env.get("DB_PATH", r"C:\teevra18\data\teevra18.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

DDL = [
# --- Pragmas (set after connect) ---
# --- Core ops/meta ---
"""
CREATE TABLE IF NOT EXISTS config_meta (
  key TEXT PRIMARY KEY,
  value TEXT,
  updated_at TEXT DEFAULT (datetime('now'))
);
""",
"""
CREATE TABLE IF NOT EXISTS token_status (
  provider TEXT PRIMARY KEY,
  ok INTEGER NOT NULL DEFAULT 0,
  last_ok_ts TEXT,
  extra_json TEXT
);
""",
"""
CREATE TABLE IF NOT EXISTS ops_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL DEFAULT (datetime('now')),
  level TEXT NOT NULL,               -- INFO/WARN/ERROR
  service TEXT NOT NULL,
  event TEXT NOT NULL,
  data_json TEXT
);
""",
"""
CREATE TABLE IF NOT EXISTS health (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL DEFAULT (datetime('now')),
  service TEXT NOT NULL,
  ok INTEGER NOT NULL DEFAULT 1,
  detail TEXT
);
""",
"CREATE INDEX IF NOT EXISTS idx_health_service_ts ON health(service, ts DESC);",
"""
CREATE TABLE IF NOT EXISTS breaker_state (
  id INTEGER PRIMARY KEY CHECK (id=1),
  state TEXT NOT NULL CHECK (state IN ('RUNNING','PAUSED','PANIC')),
  reason TEXT,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
""",

# --- Universe & policies ---
"""
CREATE TABLE IF NOT EXISTS universe_underlyings (
  symbol TEXT PRIMARY KEY,           -- e.g., NIFTY, BANKNIFTY, RELIANCE
  name TEXT,
  exchange TEXT,                     -- NSE
  segment TEXT,                      -- INDEX/CASH
  lot_size INTEGER,
  tick_size REAL,
  isin TEXT,
  enabled INTEGER NOT NULL DEFAULT 1
);
""",
"""
CREATE TABLE IF NOT EXISTS universe_derivatives (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  underlying_symbol TEXT NOT NULL,
  instrument_type TEXT NOT NULL CHECK (instrument_type IN ('FUT','OPT')),
  expiry DATE NOT NULL,
  strike REAL,                       -- NULL for futures
  option_type TEXT CHECK (option_type IN ('CE','PE','')), -- '' for FUT
  symbol TEXT UNIQUE,                -- broker symbol/token if available
  tradingsymbol TEXT,                -- display symbol
  exchange TEXT,
  lot_size INTEGER,
  enabled INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY (underlying_symbol) REFERENCES universe_underlyings(symbol)
);
""",
"""
CREATE TABLE IF NOT EXISTS universe_watchlist (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 5,
  notes TEXT,
  active INTEGER NOT NULL DEFAULT 1,
  UNIQUE(symbol)
);
""",
"""
CREATE TABLE IF NOT EXISTS policies_group (
  group_id TEXT PRIMARY KEY,         -- e.g., 'INDEX', 'NIFTY50', 'OPTIONS'
  description TEXT,
  max_trades_per_day INTEGER,
  rr_min REAL,                       -- minimum risk:reward
  sl_cap_per_lot REAL                -- rupees
);
""",
"""
CREATE TABLE IF NOT EXISTS policies_instrument (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  group_id TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  overrides_json TEXT,
  UNIQUE(symbol),
  FOREIGN KEY (group_id) REFERENCES policies_group(group_id)
);
""",

# --- Strategies & RR ---
"""
CREATE TABLE IF NOT EXISTS strategies_catalog (
  strategy_id TEXT PRIMARY KEY,      -- e.g., 'ema_vwap_atr'
  name TEXT,
  version TEXT,
  params_json TEXT,
  enabled INTEGER NOT NULL DEFAULT 1
);
""",
"""
CREATE TABLE IF NOT EXISTS rr_profiles (
  profile_name TEXT PRIMARY KEY,     -- e.g., 'BASELINE'
  rr_min REAL NOT NULL,              -- >= 2.0
  sl_cap_per_lot REAL NOT NULL,      -- <= 1000
  sl_method TEXT,                    -- e.g., 'ATR'
  tp_method TEXT,                    -- e.g., 'RR'
  tp_factor REAL,                    -- e.g., 2.0
  spread_buffer_ticks REAL,
  min_liquidity_lots REAL
);
""",
"""
CREATE TABLE IF NOT EXISTS rr_overrides (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  profile_name TEXT NOT NULL,
  start_ts TEXT,
  end_ts TEXT,
  reason TEXT,
  FOREIGN KEY (profile_name) REFERENCES rr_profiles(profile_name)
);
""",

# --- Market data (live) ---
"""
CREATE TABLE IF NOT EXISTS ticks_raw (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,                  -- market timestamp
  symbol TEXT NOT NULL,
  ltp REAL NOT NULL,                 -- last traded price
  volume INTEGER,                    -- cumulative vol if provided
  bid REAL, ask REAL,
  oi INTEGER,                        -- open interest (if provided)
  source TEXT,                       -- e.g., 'DHAN_WS'
  ingest_ts TEXT NOT NULL DEFAULT (datetime('now'))
);
""",
"CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks_raw(symbol, ts);",
"""
CREATE TABLE IF NOT EXISTS depth20_snap (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  symbol TEXT NOT NULL,
  bids_json TEXT NOT NULL,           -- top 20 levels
  asks_json TEXT NOT NULL,
  best_bid REAL, best_ask REAL,
  bid_sz INTEGER, ask_sz INTEGER
);
"""
,
"CREATE INDEX IF NOT EXISTS idx_depth20_symbol_ts ON depth20_snap(symbol, ts DESC);",
"""
CREATE TABLE IF NOT EXISTS option_chain_snap (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  underlying_symbol TEXT NOT NULL,
  expiry DATE NOT NULL,
  chain_json TEXT NOT NULL,          -- compact per strike per side
  iv_mean REAL,                      -- optional quick stat
  put_call_oi_ratio REAL
);
""",
"CREATE INDEX IF NOT EXISTS idx_chain_underlying_ts ON option_chain_snap(underlying_symbol, ts DESC);",

# --- Quote Snap (M2) ---
"""
CREATE TABLE IF NOT EXISTS quote_snap (
  ts TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ltp REAL,
  open REAL, high REAL, low REAL, prev_close REAL,
  volume INTEGER,
  PRIMARY KEY (symbol, ts)
);
""",

# --- Candles (M4) ---
"""
CREATE TABLE IF NOT EXISTS candles_1m (
  ts_start TEXT NOT NULL,
  symbol TEXT NOT NULL,
  open REAL, high REAL, low REAL, close REAL,
  volume INTEGER, oi INTEGER,
  PRIMARY KEY (symbol, ts_start)
);
""",
"CREATE INDEX IF NOT EXISTS idx_c1_symbol_ts ON candles_1m(symbol, ts_start);",
"""
CREATE TABLE IF NOT EXISTS candles_5m (
  ts_start TEXT NOT NULL,
  symbol TEXT NOT NULL,
  open REAL, high REAL, low REAL, close REAL,
  volume INTEGER, oi INTEGER,
  PRIMARY KEY (symbol, ts_start)
);
""",
"CREATE INDEX IF NOT EXISTS idx_c5_symbol_ts ON candles_5m(symbol, ts_start);",
"""
CREATE TABLE IF NOT EXISTS candles_15m (
  ts_start TEXT NOT NULL,
  symbol TEXT NOT NULL,
  open REAL, high REAL, low REAL, close REAL,
  volume INTEGER, oi INTEGER,
  PRIMARY KEY (symbol, ts_start)
);
""",
"CREATE INDEX IF NOT EXISTS idx_c15_symbol_ts ON candles_15m(symbol, ts_start);",
"""
CREATE TABLE IF NOT EXISTS candles_60m (
  ts_start TEXT NOT NULL,
  symbol TEXT NOT NULL,
  open REAL, high REAL, low REAL, close REAL,
  volume INTEGER, oi INTEGER,
  PRIMARY KEY (symbol, ts_start)
);
""",
"CREATE INDEX IF NOT EXISTS idx_c60_symbol_ts ON candles_60m(symbol, ts_start);",

# --- Signals & execution (M7–M10) ---
"""
CREATE TABLE IF NOT EXISTS signals (
  id TEXT PRIMARY KEY,               -- nonce/uuid for idempotency
  ts TEXT NOT NULL,
  symbol TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
  entry REAL NOT NULL,
  sl REAL NOT NULL,
  tp REAL NOT NULL,
  rr REAL NOT NULL,                  -- computed RR
  profile_name TEXT,                 -- RR profile used
  rr_ok INTEGER NOT NULL DEFAULT 0,  -- 1 if rr>=min and sl<=cap
  status TEXT NOT NULL DEFAULT 'NEW' CHECK (status IN ('NEW','CANCELLED','FILLED','REJECTED')),
  reason TEXT,
  extra_json TEXT
);
""",
"CREATE INDEX IF NOT EXISTS idx_signals_symbol_ts ON signals(symbol, ts);",
"CREATE INDEX IF NOT EXISTS idx_signals_strategy_ts ON signals(strategy_id, ts);",
"""
CREATE TABLE IF NOT EXISTS paper_orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  signal_id TEXT NOT NULL,
  ts_signal TEXT NOT NULL,
  ts_fill TEXT,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
  qty INTEGER NOT NULL DEFAULT 1,
  entry REAL, sl REAL, tp REAL,
  status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','SL_HIT','TP_HIT','CANCELLED','EXPIRED','CLOSED')),
  pnl REAL,
  notes TEXT,
  UNIQUE(signal_id),
  FOREIGN KEY (signal_id) REFERENCES signals(id)
);
""",
"""
CREATE TABLE IF NOT EXISTS backtest_orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
  ts_entry TEXT, ts_exit TEXT,
  entry REAL, exit REAL,
  qty INTEGER NOT NULL DEFAULT 1,
  pnl REAL, rr REAL, sl REAL, tp REAL,
  tags TEXT
);
""",
"CREATE INDEX IF NOT EXISTS idx_bt_run ON backtest_orders(run_id);",
"CREATE INDEX IF NOT EXISTS idx_bt_symbol_entry ON backtest_orders(symbol, ts_entry);",

# --- Analytics ---
"""
CREATE TABLE IF NOT EXISTS predictions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  symbol TEXT NOT NULL,
  horizon_min INTEGER NOT NULL,
  prob_up REAL,                      -- model probability
  expected_move REAL,                -- abs pct or points
  model_id TEXT,
  features_json TEXT
);
""",
"CREATE INDEX IF NOT EXISTS idx_pred_symbol_ts ON predictions(symbol, ts DESC);",
"""
CREATE TABLE IF NOT EXISTS kpi_daily (
  date TEXT PRIMARY KEY,             -- YYYY-MM-DD
  total_signals INTEGER,
  total_trades INTEGER,
  win_rate REAL,
  gross_pnl REAL,
  net_pnl REAL,
  avg_rr REAL,
  notes TEXT
);
""",

# --- Journaling ---
"""
CREATE TABLE IF NOT EXISTS live_journal (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL DEFAULT (datetime('now')),
  text TEXT,
  tag TEXT
);
"""
]

def main():
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        # Pragmas
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA foreign_keys=ON;")
        # DDLs
        for stmt in DDL:
            cur.executescript(stmt)
        con.commit()

        # Seed breaker_state if missing
        cur.execute("SELECT state FROM breaker_state WHERE id=1;")
        row = cur.fetchone()
        if not row:
            cur.execute("INSERT INTO breaker_state(id, state, reason) VALUES (1, 'RUNNING', 'bootstrap');")

        # Seed baseline RR profile
        cur.execute("SELECT profile_name FROM rr_profiles WHERE profile_name='BASELINE';")
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO rr_profiles(profile_name, rr_min, sl_cap_per_lot, sl_method, tp_method, tp_factor, spread_buffer_ticks, min_liquidity_lots)
                VALUES ('BASELINE', 2.0, 1000, 'ATR', 'RR', 2.0, 1.0, 1.0);
            """)

        # Seed strategies catalog example (disabled by default)
        cur.execute("SELECT strategy_id FROM strategies_catalog WHERE strategy_id='ema_vwap_atr';")
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO strategies_catalog(strategy_id, name, version, params_json, enabled)
                VALUES ('ema_vwap_atr','EMA(9/21)+VWAP+ATR','1.0', '{"ema_fast":9,"ema_slow":21,"atr_len":14,"min_ticks":5,"vwap_filter":true}', 0);
            """)

        # First health heartbeat
        cur.execute("""
            INSERT INTO health(service, ok, detail)
            VALUES ('bootstrap', 1, 'DB created with WAL and baseline seeds');
        """)

        # Meta
        cur.execute("""
            INSERT OR REPLACE INTO config_meta(key, value, updated_at)
            VALUES ('schema_version','m0.1', datetime('now'));
        """)
        con.commit()
        print(f"[OK] Database initialized at: {DB_PATH}")
    finally:
        con.close()

if __name__ == "__main__":
    main()
