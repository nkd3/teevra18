import sqlite3, os
from pathlib import Path

DB = Path(os.getenv('DB_PATH', r'C:\teevra18\data\teevra18.db'))

DDL = '''
CREATE TABLE IF NOT EXISTS option_chain_features (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_fetch_utc TEXT NOT NULL,
  underlying    TEXT NOT NULL,
  expiry        TEXT NOT NULL,
  last_price    REAL,
  atm_strike    REAL,
  window_n      INTEGER,
  ce_oi_sum     INTEGER,
  pe_oi_sum     INTEGER,
  pcr_oi        REAL,
  ce_vol_sum    INTEGER,
  pe_vol_sum    INTEGER,
  iv_atm_ce     REAL,
  iv_atm_pe     REAL,
  iv_skew       REAL,
  atm_delta_ce  REAL,
  atm_delta_pe  REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_och_feat ON option_chain_features(ts_fetch_utc, underlying, expiry);
CREATE INDEX IF NOT EXISTS ix_och_feat_und_exp ON option_chain_features(underlying, expiry);
'''
with sqlite3.connect(DB) as c:
    c.executescript(DDL)
print('OK: option_chain_features schema ensured at', DB)
