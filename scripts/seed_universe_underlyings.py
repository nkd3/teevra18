# C:\teevra18\scripts\seed_universe_underlyings.py
import os, sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

rows = [
    # TODO: Replace <NIFTY_SCRIP_ID> and <BANKNIFTY_SCRIP_ID> with real Security IDs when known
    ("NIFTY",      -1, "IDX_I", 1),  # use -1 as placeholder for now
    ("BANKNIFTY",  -1, "IDX_I", 1),
]

with sqlite3.connect(DB_PATH) as conn:
    conn.executemany("""
        INSERT INTO universe_option_underlyings(underlying,underlying_scrip,underlying_seg,enabled)
        VALUES(?,?,?,?)
        ON CONFLICT(underlying) DO UPDATE SET
            underlying_scrip=excluded.underlying_scrip,
            underlying_seg=excluded.underlying_seg,
            enabled=excluded.enabled
    """, rows)
    conn.commit()
print("Seeded universe_option_underlyings (placeholders).")
