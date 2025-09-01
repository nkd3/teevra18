import sqlite3, os
from pathlib import Path
DB = Path(os.getenv('DB_PATH', r'C:\teevra18\data\teevra18.db'))
with sqlite3.connect(DB) as c:
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS uq_option_chain_snap ON option_chain_snap(ts_fetch_utc, underlying, expiry, strike, side)')
    c.commit()
print('OK: uq_option_chain_snap ensured at', DB)
