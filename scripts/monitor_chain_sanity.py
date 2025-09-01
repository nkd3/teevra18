# C:\teevra18\scripts\monitor_chain_sanity.py
import os, sqlite3, datetime
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

def main():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        # 1) Latest fetch timestamp
        cur.execute("SELECT MAX(ts_fetch_utc) FROM option_chain_snap")
        latest = cur.fetchone()[0]
        print(f"Latest ts_fetch_utc: {latest}")

        # 2) Row counts by underlying/expiry
        print("\nCounts by underlying & expiry (latest 5 fetches):")
        cur.execute("""
            WITH ranked AS (
                SELECT ts_fetch_utc, underlying, expiry, COUNT(*) AS n,
                       ROW_NUMBER() OVER (ORDER BY ts_fetch_utc DESC) AS rk
                FROM option_chain_snap
                GROUP BY ts_fetch_utc, underlying, expiry
            )
            SELECT ts_fetch_utc, underlying, expiry, n
            FROM ranked
            WHERE rk <= 5
            ORDER BY ts_fetch_utc DESC, underlying, expiry;
        """)
        for row in cur.fetchall():
            print("  ", row)

        # 3) Basic sanity: IV range, delta range, bid<=ask etc (sample)
        print("\nSanity checks (sample 10 rows):")
        cur.execute("""
            SELECT underlying, expiry, strike, side, implied_volatility, delta, gamma, top_bid_price, top_ask_price
            FROM option_chain_snap
            WHERE ts_fetch_utc = (SELECT MAX(ts_fetch_utc) FROM option_chain_snap)
            LIMIT 10;
        """)
        rows = cur.fetchall()
        for u, e, k, s, iv, dlt, gma, bid, ask in rows:
            iv_ok = (iv is None) or (0.5 <= iv <= 200.0)
            d_ok  = (dlt is None) or (-1.05 <= dlt <= 1.05)
            g_ok  = (gma is None) or (gma >= -0.005)  # gamma rarely negative in practice
            sp_ok = (bid is None or ask is None) or (bid <= ask)
            status = "OK" if (iv_ok and d_ok and g_ok and sp_ok) else "CHECK"
            print(f"{status} {u} {e} {k} {s} | IV={iv} Δ={dlt} Γ={gma} bid={bid} ask={ask}")

if __name__ == "__main__":
    main()
