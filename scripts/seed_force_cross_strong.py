import sqlite3, datetime
DB = r"C:\teevra18\data\teevra18.db"

def series_for_buy(base: float):
    vals = []
    # 38 bars nearly flat (slight down) so EMA9 <= EMA21
    for i in range(38):
        vals.append(base - (38-i)*0.5)  # gentle drift down
    vals.append(vals[-1] - 10)          # extra dip
    vals.append(vals[-1] + 100)         # big pop = cross up on last bar
    return vals

with sqlite3.connect(DB) as conn:
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS candles_1m_std(
      symbol TEXT, ts TEXT, o REAL,h REAL,l REAL,c REAL,v REAL,
      PRIMARY KEY(symbol, ts)
    );
    """)
    ts0 = datetime.datetime.utcnow().replace(second=0, microsecond=0)
    for sym, base in [("NIFTY", 22000.0), ("BANKNIFTY", 48000.0)]:
        closes = series_for_buy(base)
        rows=[]
        for i, px in enumerate(closes):
            t = ts0 - datetime.timedelta(minutes=(len(closes)-1 - i))
            rows.append((sym, t.strftime("%Y-%m-%d %H:%M:%S"), px-5, px+5, px-7, px, 150000))
        c.executemany(
            "INSERT OR REPLACE INTO candles_1m_std(symbol, ts, o,h,l,c,v) VALUES (?,?,?,?,?,?,?)",
            rows
        )
    conn.commit()
print("[OK] Strong BUY-cross candles written for NIFTY & BANKNIFTY.")
