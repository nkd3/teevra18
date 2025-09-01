# C:\teevra18\teevra\db.py
import sqlite3, os, time
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS ticks_raw (
  ts_utc            TEXT NOT NULL,
  exchange_segment  INTEGER NOT NULL,      -- see annexure (e.g., NSE_FNO=2)
  security_id       INTEGER NOT NULL,
  mode              TEXT NOT NULL,         -- 'Q' (quote) or 'F' (full) or 'T' (ticker)
  ltt_epoch         INTEGER,               -- Dhan 'Last Trade Time' (int32) from feed
  ltp               REAL,
  atp               REAL,
  last_qty          INTEGER,
  volume            INTEGER,
  buy_qty_total     INTEGER,
  sell_qty_total    INTEGER,
  oi                INTEGER,
  day_open          REAL,
  day_high          REAL,
  day_low           REAL,
  day_close         REAL,
  prev_close        REAL,
  recv_ts_utc       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ticks_sid_ts ON ticks_raw(security_id, ts_utc);

CREATE TABLE IF NOT EXISTS instrument_master (
  security_id       INTEGER PRIMARY KEY,
  exchange_segment  INTEGER,
  display_name      TEXT,
  underlying_id     INTEGER,
  lot_size          INTEGER
);

CREATE TABLE IF NOT EXISTS universe_watchlist (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  exchange_segment INTEGER NOT NULL,
  security_id INTEGER NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  is_hot_option INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ops_log (
  ts_utc TEXT NOT NULL,
  level  TEXT NOT NULL,
  area   TEXT NOT NULL,
  msg    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS health (
  key   TEXT PRIMARY KEY,
  value TEXT,
  ts_utc TEXT
);
"""

def connect():
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def ensure_schema():
    conn = connect()
    for stmt in filter(None, DDL.split(";")):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.close()

def put_health(key, value):
    conn = connect()
    conn.execute("INSERT INTO health(key,value,ts_utc) VALUES(?,?,datetime('now')) "
                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value, ts_utc=datetime('now');",
                 (key, value))
    conn.close()

def log(level, area, msg):
    conn = connect()
    conn.execute("INSERT INTO ops_log(ts_utc,level,area,msg) VALUES(datetime('now'),?,?,?)",
                 (level, area, msg))
    conn.close()
