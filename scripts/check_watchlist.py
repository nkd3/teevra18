import sqlite3

DB = r"C:\teevra18\data\teevra18.db"
with sqlite3.connect(DB) as conn:
    c = conn.cursor()
    c.execute("PRAGMA table_info(universe_watchlist);")
    print("[Schema]", [(r[1], r[2]) for r in c.fetchall()])
    c.execute("SELECT COUNT(*) FROM universe_watchlist;")
    print("[Total symbols]", c.fetchone()[0])
    c.execute("SELECT symbol FROM universe_watchlist ORDER BY symbol LIMIT 20;")
    print("[Peek]", [r[0] for r in c.fetchall()])
