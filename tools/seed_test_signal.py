import os, sqlite3, datetime, uuid, hashlib
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

SYMBOL = "RELIANCE"   # change if needed
SIDE   = "LONG"       # MUST be exactly 'LONG' or 'SHORT'
RR_MIN = 2.0
SL_PER_LOT_MAX = 1000.0  # your rule

def latest_ltp(conn, symbol):
    row = conn.execute(
        "SELECT ltp FROM quote_snap WHERE symbol=? ORDER BY ts_utc DESC LIMIT 1", (symbol,)
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None

def lot_size(conn, symbol):
    row = conn.execute(
        "SELECT lot_size FROM universe_derivatives WHERE symbol=? ORDER BY ts_utc DESC LIMIT 1", (symbol,)
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else 1.0

def main():
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    entry = latest_ltp(conn, SYMBOL)
    if entry is None:
        print("No LTP found in quote_snap; seed quotes first.")
        return
    lots  = lot_size(conn, SYMBOL)

    price_risk = SL_PER_LOT_MAX / max(1.0, lots)
    if SIDE == "LONG":
        stop   = entry - price_risk
        target = entry + (price_risk * RR_MIN)
    elif SIDE == "SHORT":
        stop   = entry + price_risk
        target = entry - (price_risk * RR_MIN)
    else:
        print("SIDE must be 'LONG' or 'SHORT'")
        return

    denom = (entry - stop)
    rr_val = abs((target - entry) / denom) if denom != 0 else RR_MIN

    ts_utc = datetime.datetime.utcnow().isoformat(timespec="seconds")
    created_at_utc = ts_utc  # <<< FIX: satisfy NOT NULL created_at_utc
    version = "m8-seed-v1"
    strategy_id = "test_rr"
    group_name  = "TESTS"
    security_id = SYMBOL  # fallback; map to your real token if needed

    hsrc = f"{SYMBOL}|{SIDE}|{entry}|{stop}|{target}|{strategy_id}|{group_name}|{ts_utc}"
    deterministic_hash = hashlib.sha1(hsrc.encode("utf-8")).hexdigest()
    run_id = str(uuid.uuid4())

    # get actual table columns in order
    cols_info = cur.execute("PRAGMA table_info(signals)").fetchall()
    col_names = [r[1] for r in cols_info]

    # build payload covering all NOT NULLs + useful optionals
    payload = {
        # base requireds
        "ts_utc": ts_utc,
        "created_at_utc": created_at_utc,   # <<< FIX
        "security_id": security_id,
        "group_name": group_name,
        "strategy_id": strategy_id,
        "side": SIDE,                       # CHECK ('LONG','SHORT')
        "entry": entry,
        "stop": stop,
        "target": target,
        "rr": rr_val,
        "sl_per_lot": SL_PER_LOT_MAX,
        "version": version,
        "state": "PENDING",
        "deterministic_hash": deterministic_hash,
        "run_id": run_id,

        # nice-to-have mirrors for M8/M9
        "symbol": SYMBOL,
        "direction": SIDE,
        "entry_price": entry,
        "lot_size": lots,
    }

    # IMPORTANT: don’t force NULLs into NOT NULL columns with defaults.
    # We’ll only include the columns we have values for; others keep their table defaults.
    insert_cols = [c for c in col_names if c in payload]
    placeholders = ",".join(["?"] * len(insert_cols))
    sql = f"INSERT INTO signals({','.join(insert_cols)}) VALUES({placeholders})"
    vals = [payload[c] for c in insert_cols]

    cur.execute(sql, vals)
    conn.commit()
    conn.close()
    print(f"[OK] seeded {SYMBOL} {SIDE} entry={entry} stop={stop} target={target} rr={rr_val:.2f} lot={lots}")

if __name__ == "__main__":
    main()
