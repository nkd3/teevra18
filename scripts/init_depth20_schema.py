import os, sqlite3

db = r"C:\teevra18\data\teevra18.db"
os.makedirs(os.path.dirname(db), exist_ok=True)
con = sqlite3.connect(db)
cur = con.cursor()

cur.executescript("""
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS depth20_levels (
  ts_recv_utc   TEXT    NOT NULL,        -- ISO8601 receive time
  security_id   INTEGER NOT NULL,        -- Dhan SecurityId
  exchange_seg  TEXT    NOT NULL,        -- 'NSE_EQ' or 'NSE_FNO'
  side          TEXT    NOT NULL,        -- 'BID' or 'ASK'
  level         INTEGER NOT NULL,        -- 1..20 (1 = best)
  price         REAL    NOT NULL,
  qty           INTEGER NOT NULL,
  orders        INTEGER NOT NULL,
  -- derived metrics, repeated on all 20 rows for the snapshot tick (fast filtering)
  top5_bid_qty  INTEGER,
  top5_ask_qty  INTEGER,
  top10_bid_qty INTEGER,
  top10_ask_qty INTEGER,
  pressure_1_5  REAL,    -- (bid5 - ask5) / (bid5 + ask5)
  pressure_1_10 REAL,    -- (bid10 - ask10) / (bid10 + ask10)
  latency_ms    REAL,    -- recv->parsed insert delta
  PRIMARY KEY (ts_recv_utc, security_id, side, level)
);

CREATE INDEX IF NOT EXISTS idx_d20_sid_time ON depth20_levels(security_id, ts_recv_utc);
CREATE INDEX IF NOT EXISTS idx_d20_side_lvl ON depth20_levels(side, level);
""")

con.commit()
con.close()
print("depth20_levels ready.")
