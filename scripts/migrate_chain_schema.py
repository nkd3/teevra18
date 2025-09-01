import os, sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv('DB_PATH', r'C:\teevra18\data\teevra18.db'))

DDL = r'''
CREATE TABLE IF NOT EXISTS option_chain_snap (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_fetch_utc TEXT,
    underlying TEXT,
    underlying_scrip INTEGER,
    underlying_seg TEXT,
    expiry TEXT,
    last_price REAL,
    strike REAL,
    side TEXT,
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
    src_status TEXT
);
'''

EXPECTED = {
 'ts_fetch_utc':'TEXT','underlying':'TEXT','underlying_scrip':'INTEGER','underlying_seg':'TEXT',
 'expiry':'TEXT','last_price':'REAL','strike':'REAL','side':'TEXT','implied_volatility':'REAL',
 'ltp':'REAL','oi':'INTEGER','previous_oi':'INTEGER','volume':'INTEGER','previous_volume':'INTEGER',
 'previous_close_price':'REAL','top_ask_price':'REAL','top_ask_quantity':'INTEGER','top_bid_price':'REAL',
 'top_bid_quantity':'INTEGER','delta':'REAL','theta':'REAL','gamma':'REAL','vega':'REAL','src_status':'TEXT'
}

def ensure():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(DDL)
        cols = {r[1]: r[2] for r in conn.execute('PRAGMA table_info(option_chain_snap)').fetchall()}
        for name, typ in EXPECTED.items():
            if name not in cols:
                conn.execute(f'ALTER TABLE option_chain_snap ADD COLUMN {name} {typ}')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_ocs_fetch ON option_chain_snap(ts_fetch_utc)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_ocs_ux ON option_chain_snap(underlying, expiry, strike, side, ts_fetch_utc)')
        conn.commit()
    print(f'OK: Schema ensured at {DB_PATH}')

if __name__ == '__main__':
    ensure()
