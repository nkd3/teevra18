import sqlite3, datetime

DB = r"C:\teevra18\data\teevra18.db"

def build_strict_lastbar_buy(base: float):
    vals = []
    # 39 bars gently DOWN so EMA9 stays <= EMA21
    for i in range(39):
        vals.append(base - (39 - i) * 2.0)   # monotonic down
    # last bar big POP to force fresh cross on the last point
    vals.append(vals[-1] + 120.0)
    return vals

with sqlite3.connect(DB) as conn:
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS candles_1m_std(
      symbol TEXT, ts TEXT, o REAL, h REAL, l REAL, c REAL, v REAL,
      PRIMARY KEY(symbol, ts)
    );
    """)
    ts0 = datetime.datetime.utcnow().replace(second=0, microsecond=0)

    for sym, base in [("NIFTY", 22000.0), ("BANKNIFTY", 48000.0)]:
        closes = build_strict_lastbar_buy(base)
        rows = []
        for i, px in enumerate(closes):
            t = ts0 - datetime.timedelta(minutes=(len(closes)-1 - i))
            rows.append((sym, t.strftime("%Y-%m-%d %H:%M:%S"), px-5, px+5, px-7, px, 150000))
        c.executemany(
            "INSERT OR REPLACE INTO candles_1m_std(symbol, ts, o,h,l,c,v) VALUES (?,?,?,?,?,?,?)",
            rows
        )
    conn.commit()

print("[OK] Seeded strict last-bar BUY-cross series for NIFTY & BANKNIFTY.")
