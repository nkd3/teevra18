import sqlite3, datetime

DB = r"C:\teevra18\data\teevra18.db"

with sqlite3.connect(DB) as conn:
    c = conn.cursor()
    # Ensure canonical table exists
    c.execute("""
    CREATE TABLE IF NOT EXISTS candles_1m_std(
      symbol TEXT,
      ts TEXT,
      o REAL, h REAL, l REAL, c REAL, v REAL,
      PRIMARY KEY(symbol, ts)
    );
    """)

    base = 22000.0
    ts0 = datetime.datetime.utcnow().replace(second=0, microsecond=0)
    rows = []
    for i in range(40):
        t = ts0 - datetime.timedelta(minutes=39 - i)
        px = base + (i - 20) * 5  # linear slope to force an EMA cross around the middle
        rows.append(("NIFTY", t.strftime("%Y-%m-%d %H:%M:%S"), px-5, px+5, px-7, px, 100000))

    c.executemany(
        "INSERT OR REPLACE INTO candles_1m_std(symbol, ts, o, h, l, c, v) VALUES (?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()

print("[OK] Seeded demo candles into candles_1m_std for NIFTY.")
