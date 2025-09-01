# C:\teevra18\scripts\init_chain_schema.py
import os, sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

DDL = r"""
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS universe_option_underlyings (
    underlying TEXT PRIMARY KEY,
    underlying_scrip INTEGER NOT NULL,
    underlying_seg TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS option_chain_snap (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_fetch_utc TEXT NOT NULL,
    underlying TEXT NOT NULL,
    underlying_scrip INTEGER NOT NULL,
    underlying_seg TEXT NOT NULL,
    expiry TEXT NOT NULL,
    last_price REAL,
    strike REAL NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('CE','PE')),
    implied_volatility REAL,
    ltp REAL,
    oi INTEGER,
    previous_oi INTEGER,
    volume INTEGER,
    previous_volume INTEGER,
    previous_close_price REAL,
    top_ask_price REAL,
    top_ask_quantity INTEGER,
    top_bid_price REAL,
    top_bid_quantity INTEGER,
    delta REAL,
    theta REAL,
    gamma REAL,
    vega REAL,
    src_status TEXT,
    UNIQUE(underlying, expiry, strike, side, ts_fetch_utc)
);
"""

print(f"Using DB: {DB_PATH}")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
with sqlite3.connect(DB_PATH) as conn:
    conn.executescript(DDL)
    conn.commit()
print("Schema ensured: universe_option_underlyings, option_chain_snap")
