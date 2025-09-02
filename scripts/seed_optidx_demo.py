# C:\teevra18\scripts\seed_optidx_demo.py
import sqlite3

DB = r"C:\teevra18\data\teevra18.db"
EXP = "2099-12-31"

with sqlite3.connect(DB) as conn:
    c = conn.cursor()

    # Ensure table exists (matches your schema + CHECK constraint FUT/OPT)
    c.execute("""
    CREATE TABLE IF NOT EXISTS universe_derivatives(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      underlying_symbol TEXT,
      instrument_type TEXT CHECK(instrument_type IN ('FUT','OPT')),
      expiry DATE,
      strike REAL,
      option_type TEXT,
      symbol TEXT,
      tradingsymbol TEXT,
      exchange TEXT,
      lot_size INTEGER,
      enabled INTEGER,
      ts_utc TEXT
    );
    """)

    # Ensure we have a unique index on symbol (some DBs already have it)
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_univ_der_symbol ON universe_derivatives(symbol);")

    def last_close(sym):
        c.execute("SELECT c FROM candles_1m_std WHERE symbol=? ORDER BY ts DESC LIMIT 1;", (sym,))
        r = c.fetchone()
        return r[0] if r else None

    upsert_sql = """
        INSERT INTO universe_derivatives
        (underlying_symbol, instrument_type, expiry, strike, option_type,
         symbol, tradingsymbol, exchange, lot_size, enabled, ts_utc)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol) DO UPDATE SET
            underlying_symbol=excluded.underlying_symbol,
            instrument_type=excluded.instrument_type,
            expiry=excluded.expiry,
            strike=excluded.strike,
            option_type=excluded.option_type,
            tradingsymbol=excluded.tradingsymbol,
            exchange=excluded.exchange,
            lot_size=excluded.lot_size,
            enabled=excluded.enabled,
            ts_utc=excluded.ts_utc
    """

    for idx in ("NIFTY", "BANKNIFTY"):
        px = last_close(idx) or (22000.0 if idx == "NIFTY" else 48000.0)
        step = 50 if idx == "NIFTY" else 100
        lot  = 50 if idx == "NIFTY" else 15
        atm = int(round(px / step) * step)

        rows = [
            (
                idx, "OPT", EXP, atm, "CE",
                f"{idx}{EXP}CE{atm}", f"{idx} {atm} CE",
                "NSE", lot, 1, None
            ),
            (
                idx, "OPT", EXP, atm, "PE",
                f"{idx}{EXP}PE{atm}", f"{idx} {atm} PE",
                "NSE", lot, 1, None
            )
        ]

        # UPSERT each row so re-running this script won't error
        for r in rows:
            c.execute(upsert_sql, r)

    conn.commit()
print("[OK] Upserted demo OPT (index) CE/PE for NIFTY & BANKNIFTY.")
