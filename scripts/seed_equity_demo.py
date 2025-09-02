import sqlite3, datetime

DB = r"C:\teevra18\data\teevra18.db"
equities = [("RELIANCE", 2900.0), ("TCS", 4000.0)]

with sqlite3.connect(DB) as conn:
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS candles_1m_std(
      symbol TEXT,
      ts TEXT,
      o REAL, h REAL, l REAL, c REAL, v REAL,
      PRIMARY KEY(symbol, ts)
    );
    """)
    ts0 = datetime.datetime.utcnow().replace(second=0, microsecond=0)
    for sym, base in equities:
        # 40 bars with a forced mini wiggle for a possible cross
        closes = [base - (38 - i) * 0.5 for i in range(38)]
        closes += [closes[-1] - 8, closes[-1] + 60]
        rows = []
        for i, px in enumerate(closes):
            t = ts0 - datetime.timedelta(minutes=(len(closes) - 1 - i))
            rows.append((sym, t.strftime("%Y-%m-%d %H:%M:%S"), px-2, px+2, px-3, px, 200000))
        c.executemany(
            "INSERT OR REPLACE INTO candles_1m_std(symbol, ts, o,h,l,c,v) VALUES (?,?,?,?,?,?,?)",
            rows
        )
    conn.commit()
print("[OK] Seeded equity candles for RELIANCE & TCS.")
