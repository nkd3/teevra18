import sqlite3, datetime, math

DB = r"C:\teevra18\data\teevra18.db"

def ema(series, n):
    if len(series) < n: return []
    k = 2/(n+1)
    out = []
    ema_val = sum(series[:n])/n
    out.extend([None]*(n-1))
    out.append(ema_val)
    for px in series[n:]:
        ema_val = px*k + ema_val*(1-k)
        out.append(ema_val)
    return out

def build_series_force_lastbar_buy(base: float):
    """
    Construct 40 closes so that:
      - on bar 39 (prev): EMA9 <= EMA21
      - on bar 40 (last): EMA9 > EMA21
    We iteratively adjust the last two closes until satisfied.
    """
    # Start from a gentle down drift to bias EMA9 under EMA21
    closes = [base - (39 - i) * 1.0 for i in range(38)]  # 38 bars
    # Initial guesses for last two bars
    prev = closes[-1] - 20   # push EMAs downward
    last = prev + 200        # strong pop to cross up
    for _ in range(100):     # iterate to satisfy condition
        trial = closes + [prev, last]
        e9  = ema(trial, 9)
        e21 = ema(trial, 21)
        if not e9 or not e21 or len(e9) < 2 or len(e21) < 2:
            prev -= 5; last += 10; continue
        a9_prev, a21_prev = e9[-2], e21[-2]
        a9_now,  a21_now  = e9[-1], e21[-1]
        # Check condition
        if a9_prev <= a21_prev and a9_now > a21_now:
            return trial
        # If prev is still above, push prev lower
        if a9_prev > a21_prev:
            prev -= 10
        # If last not above, pull last higher
        if a9_now <= a21_now:
            last += 10
    # Fallback (shouldn't happen): return whatever we have
    return closes + [prev, last]

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
        closes = build_series_force_lastbar_buy(base)
        rows=[]
        for i, px in enumerate(closes):
            t = ts0 - datetime.timedelta(minutes=(len(closes)-1 - i))
            rows.append((sym, t.strftime("%Y-%m-%d %H:%M:%S"), px-5, px+5, px-7, px, 200000))
        c.executemany(
            "INSERT OR REPLACE INTO candles_1m_std(symbol, ts, o,h,l,c,v) VALUES (?,?,?,?,?,?,?)",
            rows
        )
    conn.commit()

print("[OK] Seeded EXACT last-bar BUY-cross series for NIFTY & BANKNIFTY.")
