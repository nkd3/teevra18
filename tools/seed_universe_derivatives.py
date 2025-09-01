# C:\teevra18\tools\seed_universe_derivatives.py
import os, sqlite3, datetime
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

# You can extend/change these freely.
# Format: (symbol, lot_size, instrument_type, exchange)
SEED = [
    ("NIFTY",      50,  "FUT", "NSE"),
    ("BANKNIFTY",  15,  "FUT", "NSE"),
    ("RELIANCE",  505,  "FUT", "NSE"),
    # Add more as needed...
]

DEFAULTS = {
    "expiry": "2099-12-31",   # placeholder date to satisfy NOT NULL
    "enabled": 1,
    # nullable by design for FUT:
    "option_type": None,
    "strike": None,
}

def get_cols(cur, table):
    cur.execute(f"PRAGMA table_info({table});")
    rows = cur.fetchall()
    cols = [r[1] for r in rows]               # column names
    notnull = {r[1]: bool(r[3]) for r in rows}  # name -> notnull flag
    return cols, notnull

def ensure_table_min(cur):
    # Do NOT overwrite your real schema; just ensure table exists
    cur.execute("""
    CREATE TABLE IF NOT EXISTS universe_derivatives(
        id INTEGER PRIMARY KEY,
        symbol TEXT,
        lot_size INTEGER
    );
    """)

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    ensure_table_min(cur)
    conn.commit()

    cols, notnull = get_cols(cur, "universe_derivatives")
    now_ts = datetime.datetime.utcnow().isoformat(timespec="seconds")

    # Build a safe, dynamic column set for INSERT based on what's present
    insert_cols = []
    def add_col(name): 
        if name in cols and name not in insert_cols:
            insert_cols.append(name)

    # Common columns many schemas use
    for c in ["symbol","underlying_symbol","instrument_type","expiry","strike","option_type",
              "tradingsymbol","exchange","lot_size","enabled","ts_utc"]:
        add_col(c)

    # Build SQL dynamically
    placeholders = ",".join(["?"] * len(insert_cols))
    insert_sql = f"INSERT INTO universe_derivatives({','.join(insert_cols)}) VALUES({placeholders})"

    # We don't have a unique key; do UPDATE-if-exists (by symbol) else INSERT.
    # If your schema uses a different uniqueness rule, adjust the WHERE.
    for sym, lot, inst_type, exch in SEED:
        row = {c: None for c in insert_cols}

        # Fill row with safe defaults conditioned on column presence:
        if "symbol" in row:               row["symbol"] = sym
        if "underlying_symbol" in row:    row["underlying_symbol"] = sym  # same as symbol by default
        if "instrument_type" in row:      row["instrument_type"] = inst_type or "FUT"
        if "expiry" in row:               row["expiry"] = DEFAULTS["expiry"]
        if "strike" in row:               row["strike"] = DEFAULTS["strike"]
        if "option_type" in row:          row["option_type"] = DEFAULTS["option_type"]
        if "tradingsymbol" in row:        row["tradingsymbol"] = sym
        if "exchange" in row:             row["exchange"] = exch or "NSE"
        if "lot_size" in row:             row["lot_size"] = int(lot)
        if "enabled" in row:              row["enabled"] = DEFAULTS["enabled"]
        if "ts_utc" in row:               row["ts_utc"] = now_ts

        # UPDATE first (by symbol), covering columns that exist in your schema
        set_parts = []
        params_upd = []
        for c in insert_cols:
            if c == "symbol":
                continue
            set_parts.append(f"{c}=?")
            params_upd.append(row[c])
        params_upd.append(sym)

        if set_parts:
            upd_sql = f"UPDATE universe_derivatives SET {', '.join(set_parts)} WHERE symbol=?"
            cur.execute(upd_sql, params_upd)
            updated = cur.rowcount
        else:
            updated = 0

        if updated == 0:
            # INSERT new
            params_ins = [row[c] for c in insert_cols]
            cur.execute(insert_sql, params_ins)
            print(f"[OK][INS] {sym} lot_size={lot} inst={row.get('instrument_type')} expiry={row.get('expiry')} exch={row.get('exchange')}")
        else:
            print(f"[OK][UPD] {sym} lot_size={lot} inst={inst_type} exch={exch}")

    conn.commit()
    conn.close()
    print("seed universe_derivatives: done.")

if __name__ == "__main__":
    main()
