# C:\teevra18\services\ltp_feeder\ltp_feeder_mock.py
"""
Minimal LTP ingestor for paper trading.
- Every tick (default 2s), look at FILLED but not CLOSED orders.
- For each, write an LTP that moves toward TP (or SL) depending on mode.
- This is a MOCK/random-walk style feeder; replace the price_compute() with Dhan/Zerodha reads later.

Run:
  python C:\teevra18\services\ltp_feeder\ltp_feeder_mock.py --mode drift_to_tp
  python C:\teevra18\services\ltp_feeder\ltp_feeder_mock.py --mode random --vol 0.02

Then run your M9 worker loop/once in parallel.
"""
import sqlite3, argparse, time, random
from datetime import datetime

DB = r"C:\teevra18\data\teevra18.db"

def price_compute(po, mode: str, vol: float):
    """
    Return next LTP for this order.
    - BUY: drift up for TP, SELL: drift down for TP (if mode='drift_to_tp')
    - random: add +/- vol% noise around last price
    """
    last = po["fill_price"] if po["last_price"] is None else po["last_price"]
    last = float(last)
    entry = float(po["entry_price"])
    sl    = float(po["sl_price"])
    tp    = float(po["tp_price"])
    side  = (po["side"] or "").upper()

    if mode == "drift_to_tp":
        # Push gently toward TP (0.5% of distance per tick), with tiny noise
        target = tp if side == "BUY" else tp  # TP is target for both; its relative location differs
        step = (target - last) * 0.05  # 5% of remaining distance per tick
        noise = (random.random() - 0.5) * 0.01 * max(1.0, abs(last))
        next_px = last + step + noise
    else:
        # random walk: +/- vol% of last
        noise = (random.random() - 0.5) * 2 * vol * max(1.0, abs(last))
        next_px = last + noise

    # Keep premium >= 0
    if next_px < 0:
        next_px = 0.0
    return round(next_px, 2)

def ensure_ltp_table(conn):
    conn.execute("""
      CREATE TABLE IF NOT EXISTS ltp_cache(
        option_symbol TEXT,
        ts_utc TEXT,
        ltp REAL
      );
    """)

def fetch_active_orders(conn):
    # FILLED orders that are not CLOSED yet (state is 'FILLED' until TP/SL hit)
    rows = conn.execute("""
      SELECT
        po.id, po.option_symbol, po.side, po.entry_price, po.fill_price,
        po.sl_price, po.tp_price,
        -- last pushed LTP if any for context
        (SELECT ltp FROM ltp_cache lc
           WHERE lc.option_symbol = po.option_symbol
           ORDER BY lc.ts_utc DESC LIMIT 1) AS last_price
      FROM paper_orders po
      WHERE po.state='FILLED'
      ORDER BY po.id ASC
      LIMIT 50
    """).fetchall()
    return rows

def push_ltp(conn, symbol: str, ltp: float):
    conn.execute("INSERT INTO ltp_cache(option_symbol, ts_utc, ltp) VALUES(?, datetime('now'), ?)", (symbol, ltp))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB)
    ap.add_argument("--tick", type=float, default=2.0, help="seconds between pushes")
    ap.add_argument("--mode", choices=["drift_to_tp", "random"], default="drift_to_tp")
    ap.add_argument("--vol", type=float, default=0.02, help="random mode volatility (fraction of price)")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    ensure_ltp_table(conn); conn.commit()

    print(f"[LTP] feeder started mode={args.mode}, tick={args.tick}s")
    try:
        while True:
            orders = fetch_active_orders(conn)
            if not orders:
                time.sleep(args.tick)
                continue
            for po in orders:
                nxt = price_compute(po, args.mode, args.vol)
                push_ltp(conn, po["option_symbol"], nxt)
            conn.commit()
            time.sleep(args.tick)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
