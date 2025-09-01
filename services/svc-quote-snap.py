# C:\teevra18\services\svc-quote-snap.py
import os, sys, time, json, sqlite3, logging, datetime
from pathlib import Path

import requests  # pip install requests

DB_PATH  = Path(os.getenv("DB_PATH",  r"C:\teevra18\data\teevra18.db"))
LOG_DIR  = Path(os.getenv("LOG_DIR",  r"C:\teevra18\logs"))
LOG_FILE = LOG_DIR / "svc-quote-snap.log"

DHAN_BASE   = os.getenv("DHAN_REST_BASE", "https://api.dhan.co")
DHAN_APIKEY = os.getenv("DHAN_API_KEY", "")
DHAN_TOKEN  = os.getenv("DHAN_ACCESS_TOKEN", "")

LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def ensure_tables(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS quote_snap(
        symbol TEXT,
        ltp    REAL,
        bid    REAL,
        ask    REAL,
        ts_utc TEXT
    );
    """)
    conn.commit()

def get_watchlist(conn):
    """
    Prefer a user table if present; else read simple file; else default list.
    - Try table: universe_watchlist(symbol)
    - Else fallback file: C:\teevra18\data\watchlist.txt (one symbol per line)
    - Else default: ['NIFTY', 'RELIANCE']
    """
    cur = conn.cursor()
    try:
        cur.execute("SELECT symbol FROM universe_watchlist WHERE symbol IS NOT NULL")
        rows = [r[0] for r in cur.fetchall()]
        if rows:
            return sorted(list(set(rows)))
    except Exception:
        pass

    file_fallback = Path(r"C:\teevra18\data\watchlist.txt")
    if file_fallback.exists():
        syms = []
        for line in file_fallback.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s:
                syms.append(s)
        if syms:
            return sorted(list(set(syms)))

    return ["NIFTY", "RELIANCE"]

def fetch_quote_rest(symbol):
    """
    Placeholder for Dhan REST.
    Implement your actual endpoint & params here.
    Must return dict with keys: ltp, bid, ask (any missing -> None).
    """
    if not DHAN_APIKEY or not DHAN_TOKEN:
        # Credentials not present -> skip REST
        return None

    try:
        # Example placeholder (adjust to real Dhan endpoint when available):
        url = f"{DHAN_BASE}/placeholder/quotes"  # CHANGE THIS when you wire the real endpoint
        headers = {
            "access-token": DHAN_TOKEN,
            "Content-Type": "application/json",
            "client-id": DHAN_APIKEY
        }
        # Change payload/params per Dhan docs
        resp = requests.get(url, headers=headers, timeout=3)
        if resp.status_code == 200:
            # parse your real JSON here
            data = resp.json()
            # This is mocked mapping; replace with real fields from Dhan:
            return {
                "ltp": float(data.get("ltp", 0)) if "ltp" in data else None,
                "bid": float(data.get("bid", 0)) if "bid" in data else None,
                "ask": float(data.get("ask", 0)) if "ask" in data else None,
            }
        logging.warning(f"REST quote failed {symbol}: {resp.status_code} {resp.text[:120]}")
    except Exception as e:
        logging.warning(f"REST quote exception {symbol}: {e}")

    return None

def fetch_quote_fallback_from_candles(conn, symbol):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT close FROM candles_1m
            WHERE symbol = ?
            ORDER BY ts_utc DESC
            LIMIT 1
        """, (symbol,))
        r = cur.fetchone()
        if r and r[0] is not None:
            return {"ltp": float(r[0]), "bid": None, "ask": None}
    except Exception:
        pass
    return None

def upsert_snap(conn, symbol, snap):
    cur = conn.cursor()
    ts = datetime.datetime.utcnow().isoformat(timespec="seconds")
    cur.execute("INSERT INTO quote_snap(symbol, ltp, bid, ask, ts_utc) VALUES(?,?,?,?,?)",
                (symbol, snap.get("ltp"), snap.get("bid"), snap.get("ask"), ts))
    conn.commit()

def run_once():
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    syms = get_watchlist(conn)

    got = 0
    for s in syms:
        snap = fetch_quote_rest(s)
        if snap is None:
            snap = fetch_quote_fallback_from_candles(conn, s)
        if snap is None:
            logging.info(f"No quote for {s} (REST+fallback failed)")
            continue
        upsert_snap(conn, s, snap)
        got += 1
        logging.info(f"SNAP {s}: ltp={snap.get('ltp')}")

    conn.close()
    print(f"quote-snap: captured {got} snapshots.")

def run_follow(loop_ms=2000):
    while True:
        run_once()
        time.sleep(max(0.1, loop_ms/1000.0))

def main():
    mode = (sys.argv[1] if len(sys.argv) >= 2 else "once").lower()
    if mode == "once":
        run_once()
    elif mode == "follow":
        loop_ms = int(sys.argv[2]) if len(sys.argv) >= 3 else 2000
        run_follow(loop_ms)
    else:
        print("Usage:\n  python C:\\teevra18\\services\\svc-quote-snap.py once\n  python C:\\teevra18\\services\\svc-quote-snap.py follow 2000")

if __name__ == "__main__":
    main()
