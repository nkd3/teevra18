import sqlite3, pathlib

DB = r"C:\teevra18\data\teevra18.db"
pathlib.Path(DB).parent.mkdir(parents=True, exist_ok=True)

DDL = """
-- universe_watchlist (already fixed earlier, but safe)
CREATE TABLE IF NOT EXISTS universe_watchlist(
  symbol TEXT PRIMARY KEY
);

-- Minimal candles table (1m) if not present
CREATE TABLE IF NOT EXISTS candles_1m(
  symbol TEXT,
  ts TEXT,            -- ISO8601
  o REAL, h REAL, l REAL, c REAL, v REAL,
  PRIMARY KEY(symbol, ts)
);

-- Option chain snapshot (optional; used for filters if present)
CREATE TABLE IF NOT EXISTS option_chain_snap(
  underlying TEXT,
  ts TEXT,
  strike REAL,
  option_type TEXT,   -- CE/PE
  ltp REAL,
  iv REAL,
  oi INTEGER,
  PRIMARY KEY(underlying, ts, strike, option_type)
);

-- Signals output
CREATE TABLE IF NOT EXISTS signals(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT,              -- when signal generated
  symbol TEXT,          -- target tradable (e.g., NIFTY-SEP25-24000-CE or NIFTY_FUT)
  driver TEXT,          -- driver context (NIFTY/BANKNIFTY or EQUITY symbol)
  action TEXT,          -- BUY/SELL
  rr REAL,              -- risk:reward at entry
  sl REAL,              -- absolute ₹ per lot (cap ≤ 1000)
  tp REAL,              -- absolute ₹ per lot (≥ 2x SL)
  reason TEXT,          -- textual reason / rule id
  state TEXT DEFAULT 'NEW'  -- NEW->SENT->FILLED (paper)
);
"""

with sqlite3.connect(DB) as conn:
    conn.executescript(DDL)
print("[OK] Core schema ensured.")
