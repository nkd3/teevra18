import sqlite3, datetime

DB = r"C:\teevra18\data\teevra18.db"
SYMS = ["NIFTY", "BANKNIFTY"]

def make_series(base):
    # 38 bars gently rising -> EMA9 slightly below EMA21
    # 39th dips, 40th pops -> cross BUY on last bar
    vals = []
    for i in range(38):
        vals.append(base + i*2)
    vals.append(vals[-1] - 15)
    vals.append(vals[-1] + 30)
    return vals

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
    for sym in SYMS:
        base = 22000.0 if sym=="NIFTY" else 48000.0
        closes = make_series(base)
        rows = []
        for i, px in enumerate(closes):
            t = ts0 - datetime.timedelta(minutes=(len(closes)-1 - i))
            rows.append((sym, t.strftime("%Y-%m-%d %H:%M:%S"), px-5, px+5, px-7, px, 100000))
        c.executemany(
            "INSERT OR REPLACE INTO candles_1m_std(symbol, ts, o,h,l,c,v) VALUES (?,?,?,?,?,?,?)",
            rows
        )
    conn.commit()
print("[OK] Seeded force-cross candles for NIFTY & BANKNIFTY.")
