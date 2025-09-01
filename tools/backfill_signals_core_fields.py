# C:\teevra18\tools\backfill_signals_core_fields.py
import os
import sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

# --- Defaults (edit if you like) ---
DEFAULT_DIRECTION = "LONG"   # used only if missing and not inferable
DEFAULT_LOT_SIZE  = 1.0      # used only if missing and not inferable

def get_table_columns(cur, table):
    cur.execute(f"PRAGMA table_info({table});")
    # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    rows = cur.fetchall()
    cols = [r[1] for r in rows]
    pks = [r[1] for r in rows if r[5] == 1]  # pk flag
    return cols, pks

def has_rowid(cur, table):
    # Without-rowid tables won’t have a working rowid/oid/_rowid_.
    # We can test by selecting one.
    try:
        cur.execute(f"SELECT rowid FROM {table} LIMIT 1;")
        cur.fetchone()
        return True
    except sqlite3.Error:
        return False

def detect_pk_column(cur, table="signals"):
    cols, pks = get_table_columns(cur, table)
    # 1) If declared PK exists, prefer the first one (assume single-column PK)
    if pks:
        return pks[0], False  # (column_name, is_rowid)
    # 2) Try common PK names
    candidates = ["signal_id", "id", "sig_id", "uuid", "guid", "nonce"]
    for c in candidates:
        if c in cols:
            return c, False
    # 3) Fallback to rowid if available
    if has_rowid(cur, table):
        return "rowid", True
    # 4) No PK & no rowid — last resort: use first column
    return cols[0], False if cols else (None, False)

def get_latest_entry_price(cur, symbol):
    # Try quote_snap.ltp first
    try:
        cur.execute("""
            SELECT ltp
            FROM quote_snap
            WHERE symbol = ?
            ORDER BY ts_utc DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass

    # Fallback: candles_1m.close
    try:
        cur.execute("""
            SELECT close
            FROM candles_1m
            WHERE symbol = ?
            ORDER BY ts_utc DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass

    return None

def get_lot_size(cur, symbol):
    # Try universe_derivatives.lot_size
    try:
        cur.execute("""
            SELECT lot_size
            FROM universe_derivatives
            WHERE symbol = ?
            ORDER BY ts_utc DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass
    return None

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Confirm table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signals';")
    if not cur.fetchone():
        conn.close()
        raise SystemExit("Table 'signals' not found. Run earlier stages first.")

    pk_col, is_rowid = detect_pk_column(cur, "signals")

    if pk_col is None:
        conn.close()
        raise SystemExit("Could not detect any columns in 'signals'.")

    # Build SELECT list ensuring we always fetch a usable primary key alias 'pk'
    if is_rowid:
        select_pk = "rowid AS pk"
        where_pk = "rowid"
    else:
        select_pk = f"{pk_col} AS pk"
        where_pk = pk_col

    # Pull candidates: missing any of the three fields
    query = f"""
        SELECT {select_pk}, *
        FROM signals
        WHERE (direction IS NULL OR entry_price IS NULL OR lot_size IS NULL)
        ORDER BY COALESCE(ts_utc, CURRENT_TIMESTAMP) DESC
        LIMIT 500
    """
    cur.execute(query)
    rows = cur.fetchall()

    if not rows:
        print("No signals need backfill.")
        conn.close()
        return

    updated = 0
    for r in rows:
        pk = r["pk"]  # guaranteed by our SELECT alias
        symbol = r["symbol"] if "symbol" in r.keys() else None

        direction = r["direction"] if "direction" in r.keys() else None
        entry_price = r["entry_price"] if "entry_price" in r.keys() else None
        lot_size = r["lot_size"] if "lot_size" in r.keys() else None

        # Backfill direction (simple default)
        if direction is None:
            direction = DEFAULT_DIRECTION

        # Backfill entry_price from quote_snap or candles_1m
        if entry_price is None and symbol:
            entry_price = get_latest_entry_price(cur, symbol)

        # Backfill lot_size from universe_derivatives (or default)
        if lot_size is None and symbol:
            lot_size = get_lot_size(cur, symbol)

        if lot_size is None:
            lot_size = DEFAULT_LOT_SIZE

        # If still missing entry_price (no symbol data), skip this row
        if entry_price is None:
            print(f"[SKIP] {where_pk}={pk} — Could not infer entry_price (no quote/candle).")
            continue

        # Apply updates
        cur.execute(f"""
            UPDATE signals
            SET direction = ?, entry_price = ?, lot_size = ?, rr_validated = NULL, rr_reject_reason = NULL
            WHERE {where_pk} = ?
        """, (direction, float(entry_price), float(lot_size), pk))
        updated += 1
        print(f"[OK] {where_pk}={pk} -> direction={direction}, entry_price={entry_price}, lot_size={lot_size}")

    conn.commit()
    conn.close()
    print(f"Backfill complete. Updated rows: {updated}")

if __name__ == "__main__":
    main()
