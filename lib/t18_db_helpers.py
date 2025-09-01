# C:\teevra18\lib\t18_db_helpers.py
import sqlite3

def t18_fetch_lot_size(conn: sqlite3.Connection, symbol: str, default_ls: float = 1.0) -> float:
    """
    Tolerant lot-size fetcher:
    - Tries symbol match
    - Falls back to underlying_symbol
    - Then tradingsymbol
    - Orders by ts_utc if present
    Returns default_ls if nothing found.
    """
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(universe_derivatives);")
        cols = [r[1] for r in cur.fetchall()]
        has_ts   = "ts_utc" in cols
        order = " ORDER BY ts_utc DESC LIMIT 1" if has_ts else " LIMIT 1"

        # 1) Exact symbol
        cur.execute(f"SELECT lot_size FROM universe_derivatives WHERE symbol=?{order}", (symbol,))
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])

        # 2) underlying_symbol
        if "underlying_symbol" in cols:
            cur.execute(f"SELECT lot_size FROM universe_derivatives WHERE underlying_symbol=?{order}", (symbol,))
            row = cur.fetchone()
            if row and row[0] is not None:
                return float(row[0])

        # 3) tradingsymbol
        if "tradingsymbol" in cols:
            cur.execute(f"SELECT lot_size FROM universe_derivatives WHERE tradingsymbol=?{order}", (symbol,))
            row = cur.fetchone()
            if row and row[0] is not None:
                return float(row[0])

    except Exception:
        # swallow and return default
        pass
    return float(default_ls)
