# C:\teevra18\services\ltp_feeder\build_atm_ladder.py
import csv, os, sqlite3, sys
from datetime import datetime

DB  = r"C:\teevra18\data\teevra18.db"
CSV = r"C:\teevra18\data\dhan_instruments.csv"

# Strike step assumptions
STEP = {"NIFTY": 50, "BANKNIFTY": 100}
K = 10  # ATM ±10 strikes

TEMPLATE = """security_id,tradingsymbol,underlying,expiry,strike,option_type,exchange_segment,lot_size
# Example rows (replace with real SecurityIds)
# 123456789,BANKNIFTY24SEP49500PE,BANKNIFTY,2025-09-25,49500,PE,NSE_FNO,35
# 987654321,NIFTY24SEP24500CE,NIFTY,2025-09-25,24500,CE,NSE_FNO,75
"""

def ensure_csv_exists_with_template():
    os.makedirs(os.path.dirname(CSV), exist_ok=True)
    if not os.path.exists(CSV):
        with open(CSV, "w", encoding="utf-8", newline="") as f:
            f.write(TEMPLATE)
        print(f"[INFO] Created CSV template at {CSV}. Fill real rows & re-run.")
        return False
    return True

def read_master():
    rows = []
    try:
        with open(CSV, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader((line for line in f if not line.strip().startswith("#")))
            for r in rdr:
                if (r.get("exchange_segment") or "").strip() != "NSE_FNO":
                    continue
                if (r.get("option_type") or "").strip() not in ("CE","PE"):
                    continue
                if not r.get("security_id") or not r.get("tradingsymbol") or not r.get("underlying") or not r.get("expiry") or not r.get("strike"):
                    continue
                rows.append({k.strip(): (v.strip() if isinstance(v, str) else v) for k,v in r.items()})
    except FileNotFoundError:
        return []
    return rows

def nearest_expiry(rows, underlying):
    today = datetime.utcnow().date()
    exp = sorted({datetime.fromisoformat(r["expiry"]).date()
                  for r in rows
                  if r.get("underlying")==underlying and r.get("expiry")}, key=lambda d: d)
    for e in exp:
        if e >= today:
            return e.isoformat()
    return None

def round_to_step(x, step): return int(round(x / step) * step)

def guess_spot_from_options(rows, underlying):
    return {"NIFTY": 24000, "BANKNIFTY": 50000}.get(underlying, 24000)

def pick_symbols(rows, underlying, spot):
    step = STEP.get(underlying)
    if not step:
        print(f"[WARN] No STEP defined for {underlying}; skipping.")
        return []

    atm  = round_to_step(spot, step)
    exp  = nearest_expiry(rows, underlying)

    idx = {}
    for r in rows:
        if r.get("underlying") != underlying: continue
        try: strike = int(float(r.get("strike","0")))
        except: continue
        key = (strike, r.get("option_type"))
        idx.setdefault(key, []).append(r)

    targets = []
    for d in range(-K, K+1):
        strike = atm + d*step
        for opt in ("CE","PE"):
            candidates = idx.get((strike,opt), [])
            if not candidates: continue
            chosen = None
            if exp:
                chosen = next((r for r in candidates if r.get("expiry")==exp), None)
            if not chosen:  # fallback: nearest future expiry or just first
                fut = []
                today = datetime.utcnow().date()
                for r in candidates:
                    try:
                        e = datetime.fromisoformat(r["expiry"]).date()
                        if e >= today: fut.append((e,r))
                    except: pass
                chosen = fut[0][1] if fut else candidates[0]
            if chosen:
                targets.append((chosen["tradingsymbol"], chosen["security_id"]))

    if not targets:
        print(f"[SAFE] No matches for {underlying}. Relaxing — nothing upserted.")
    elif exp is None:
        print(f"[WARN] No expiry found for {underlying}; used best-effort strikes.")
    return sorted(set(targets))

def upsert_subscriptions(pairs):
    conn = sqlite3.connect(DB)
    with conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS ltp_subscriptions(
            option_symbol TEXT PRIMARY KEY, broker TEXT, token TEXT, exchange TEXT)""")
        for sym, sec in pairs:
            conn.execute("INSERT OR REPLACE INTO ltp_subscriptions VALUES(?,?,?,?)",
                         (sym, "DHAN", sec, "NFO"))
    conn.close()

def load_spot_from_db_or_fallback(underlying, rows):
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    q = conn.execute("""SELECT ltp FROM ltp_cache
        WHERE option_symbol IN ('NIFTY','BANKNIFTY','NIFTY50','BANKNIFTY_INDEX')
        ORDER BY ts_utc DESC LIMIT 1""").fetchone()
    conn.close()
    if q:
        try: return float(q["ltp"])
        except: pass
    return guess_spot_from_options(rows, underlying)

def main():
    if not ensure_csv_exists_with_template(): sys.exit(0)
    rows = read_master()
    if not rows:
        print(f"[WARN] {CSV} has no valid rows yet. Fill real values & re-run.")
        sys.exit(0)

    total = 0
    for root in ("NIFTY","BANKNIFTY"):
        spot = load_spot_from_db_or_fallback(root, rows)
        pairs = pick_symbols(rows, root, spot)
        if pairs:
            upsert_subscriptions(pairs)
            total += len(pairs)
            print(f"[ATM LADDER] {root}: upserted {len(pairs)} symbols (spot≈{spot}).")
        else:
            print(f"[INFO] {root}: nothing selected. Safe skip.")
    print(f"[ATM LADDER] Done. Total upserts: {total}")

if __name__ == "__main__":
    main()
