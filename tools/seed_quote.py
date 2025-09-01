# C:\teevra18\tools\seed_quote.py
import os, sqlite3, datetime
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

# Dummy LTPs just to unblock M8 end-to-end
SEED_QUOTES = [
    ("NIFTY",      24500.00),
    ("BANKNIFTY",  52300.00),
    ("RELIANCE",    3020.50),
]

# Defaults to satisfy common NOT NULL columns seen in broker schemas
DEFAULTS = {
    "exchange": "NSE",
    "exchange_segment": "NSE",     # <-- your DB requires this (NOT NULL)
    "tradingsymbol": None,         # will fallback to symbol if NOT NULL
    "instrument_token": None,
    "symbol_token": None,
    "bid": None,
    "ask": None,
}

def table_cols_info(cur, table):
    cur.execute(f"PRAGMA table_info({table});")
    # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
    rows = cur.fetchall()
    cols = [r[1] for r in rows]
    notnull = {r[1]: bool(r[3]) for r in rows}
    return cols, notnull

def ensure_table_min(cur):
    # Create minimal table if it doesn't exist (won't override your existing)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS quote_snap(
        symbol  TEXT,
        ltp     REAL,
        bid     REAL,
        ask     REAL,
        ts_utc  TEXT
    );
    """)

def value_for(col, symbol, ltp, notnull):
    # Always provide symbol/ltp/ts_utc if present
    if col == "symbol":          return symbol
    if col == "ltp":             return float(ltp)
    if col == "ts_utc":          return datetime.datetime.utcnow().isoformat(timespec="seconds")

    # Bid/Ask defaults
    if col == "bid":             return DEFAULTS["bid"]
    if col == "ask":             return DEFAULTS["ask"]

    # Exchange-ish fields
    if col == "exchange_segment": return DEFAULTS["exchange_segment"]
    if col == "exchange":         return DEFAULTS["exchange"]

    # Symbol-ish fallbacks
    if col == "tradingsymbol":    return symbol if notnull.get(col, False) else (DEFAULTS["tradingsymbol"] or symbol)
    if col == "instrument_token": return DEFAULTS["instrument_token"]
    if col == "symbol_token":     return DEFAULTS["symbol_token"]

    # Unknown column: return None unless NOT NULL, then try something reasonable
    if notnull.get(col, False):
        # last-resort fallback for NOT NULL unknowns: try symbol or 0
        # numeric-ish?
        return symbol
    return None

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    ensure_table_min(cur)
    conn.commit()

    # Introspect actual table schema (including extra NOT NULL columns)
    cols, notnull = table_cols_info(cur, "quote_snap")

    # Guarantee we always include core columns if present
    # Preserve existing order from PRAGMA to avoid surprises
    insert_cols = cols.copy()

    placeholders = ",".join(["?"] * len(insert_cols))
    insert_sql = f"INSERT INTO quote_snap({','.join(insert_cols)}) VALUES({placeholders})"

    inserted = 0
    for sym, ltp in SEED_QUOTES:
        values = [value_for(c, sym, ltp, notnull) for c in insert_cols]
        cur.execute(insert_sql, values)
        inserted += 1
        print(f"[OK] quote {sym} ltp={ltp}")

    conn.commit()
    conn.close()
    print(f"seed quote_snap: done. rows={inserted}")

if __name__ == "__main__":
    main()
