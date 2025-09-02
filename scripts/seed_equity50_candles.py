import sqlite3, random, time
from datetime import datetime, timedelta, timezone

DB = r"C:\teevra18\data\teevra18.db"

# Start with a reasonable subset; add more tickers as you need
EQUITIES = [
    "RELIANCE","TCS","HDFCBANK","ICICIBANK","SBIN","INFY","ITC",
    "AXISBANK","LT","HINDUNILVR","BHARTIARTL","KOTAKBANK","BAJFINANCE",
    "ASIANPAINT","MARUTI","SUNPHARMA","TITAN","ULTRACEMCO","WIPRO","NTPC"
]

# Fallback base prices for first-time seeding
BASE_PX = {
    "RELIANCE": 2950.0, "TCS": 4050.0, "HDFCBANK": 1600.0, "ICICIBANK": 1050.0,
    "SBIN": 850.0, "INFY": 1650.0, "ITC": 450.0, "AXISBANK": 1150.0, "LT": 3600.0,
    "HINDUNILVR": 2600.0, "BHARTIARTL": 1400.0, "KOTAKBANK": 1850.0, "BAJFINANCE": 7000.0,
    "ASIANPAINT": 3100.0, "MARUTI": 12000.0, "SUNPHARMA": 1200.0, "TITAN": 3500.0,
    "ULTRACEMCO": 11000.0, "WIPRO": 550.0, "NTPC": 350.0
}

def iso_ts(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

with sqlite3.connect(DB) as conn:
    cur = conn.cursor()

    # Ensure target table exists (standardized candle table)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS candles_1m_std(
        symbol TEXT NOT NULL,
        ts     TEXT NOT NULL,
        o      REAL NOT NULL,
        h      REAL NOT NULL,
        l      REAL NOT NULL,
        c      REAL NOT NULL,
        v      REAL NOT NULL,
        PRIMARY KEY(symbol, ts)
    );
    """)

    # Helper to get last close if any
    def last_close(sym: str):
        cur.execute("SELECT c FROM candles_1m_std WHERE symbol=? ORDER BY ts DESC LIMIT 1;", (sym,))
        row = cur.fetchone()
        return float(row[0]) if row else None

    now = datetime.utcnow()

    total_rows = 0
    for sym in EQUITIES:
        start_px = last_close(sym) or BASE_PX.get(sym, 1000.0)
        px = float(start_px)

        # Generate last 40 minutes of bars (oldest -> newest)
        bars = []
        for i in range(40, 0, -1):
            ts = iso_ts(now - timedelta(minutes=i))
            # small random walk
            drift = random.uniform(-0.003, 0.003)  # ±0.3%
            new_px = max(1.0, px * (1.0 + drift))
            o = px
            c_ = new_px
            hi = max(o, c_) * (1.0 + random.uniform(0.000, 0.0015))
            lo = min(o, c_) * (1.0 - random.uniform(0.000, 0.0015))
            v = random.randint(10000, 200000)
            bars.append((sym, ts, o, hi, lo, c_, v))
            px = new_px

        # Upsert (so re-running is safe)
        cur.executemany("""
            INSERT INTO candles_1m_std(symbol, ts, o, h, l, c, v)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, ts) DO UPDATE SET
              o=excluded.o, h=excluded.h, l=excluded.l, c=excluded.c, v=excluded.v;
        """, bars)
        total_rows += len(bars)

    conn.commit()

print(f"[OK] Seeded/Upserted {total_rows} candles across {len(EQUITIES)} equities into candles_1m_std.")
