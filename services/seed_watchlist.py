# C:\teevra18\services\seed_watchlist.py
import sys, os
if r"C:\teevra18" not in sys.path:
    sys.path.append(r"C:\teevra18")

from teevra.db import ensure_schema, connect, log

# Demo seeds — replace with real Dhan security_ids
NSE_FNO = 2
SEEDS = [
    (NSE_FNO, 26000, 0),  # NIFTY Fut (dummy)
    (NSE_FNO, 52175, 1),  # NIFTY PE (dummy)
    (NSE_FNO, 52176, 1),  # NIFTY CE (dummy)
]

def main():
    ensure_schema()
    with connect() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS universe_watchlist (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          exchange_segment INTEGER NOT NULL,
          security_id INTEGER NOT NULL,
          is_active INTEGER NOT NULL DEFAULT 1,
          is_hot_option INTEGER NOT NULL DEFAULT 0
        );
        """)
        c.execute("DELETE FROM universe_watchlist;")
        c.executemany("""
            INSERT INTO universe_watchlist(exchange_segment, security_id, is_hot_option)
            VALUES(?,?,?)
        """, SEEDS)
    log("INFO","watchlist",f"seeded demo watchlist ({len(SEEDS)} rows)")
    print("Seeded universe_watchlist (replace security_ids with real ones).")

if __name__ == "__main__":
    main()
