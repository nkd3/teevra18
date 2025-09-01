import sqlite3, os
from pathlib import Path
DB_PATH = Path(os.getenv('DB_PATH', r'C:\teevra18\data\teevra18.db'))
sql = '''
CREATE VIEW IF NOT EXISTS option_chain_latest AS
SELECT o.*
FROM option_chain_snap o
JOIN (
  SELECT underlying, expiry, strike, side, MAX(ts_fetch_utc) AS ts_fetch_utc
  FROM option_chain_snap
  GROUP BY underlying, expiry, strike, side
) mx
ON  o.underlying = mx.underlying
AND o.expiry     = mx.expiry
AND o.strike     = mx.strike
AND o.side       = mx.side
AND o.ts_fetch_utc = mx.ts_fetch_utc;
'''
with sqlite3.connect(DB_PATH) as c:
    c.executescript(sql)
print('OK: view option_chain_latest created at', DB_PATH)
