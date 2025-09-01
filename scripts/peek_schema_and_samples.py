# -*- coding: utf-8 -*-
"""
Print schemas + top 5 rows for M0..M5 tables and suggest best-fit column mappings.
Use this once to see your REAL column names and confirm the auto-mapper logic.
"""

import os, sqlite3, textwrap
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

TABLES = [
    ("health", ["breaker_state","state","mode"],  # breaker
               ["updated_at","last_seen","heartbeat_ts","updated_on","ts","time","asof","created_at","timestamp"]),
    ("ticks_raw", ["symbol","tradingsymbol","scrip","token","instrument"], 
                  ["ts","timestamp","event_time","ts_epoch","created_at","time","asof","updated_at","ltt","ltp_time"]),
    ("quote_snap", ["symbol","tradingsymbol","scrip","token","instrument"],
                   ["ts","timestamp","asof","snapshot_ts","updated_at","time","created_at"]),
    ("depth20_snap", ["symbol","tradingsymbol","scrip","token","instrument"],
                     ["ts","timestamp","asof","updated_at","time","created_at","snapshot_ts"]),
    ("candles_1m", [],  # OHLC assumed as open/high/low/close already present
                   ["ts_close","bar_close","end_ts","close_ts","ts","timestamp","bar_time","bar_end_time","asof"]),
    ("option_chain_snap", ["symbol","tradingsymbol","scrip","token","instrument"],
                          ["ts","timestamp","asof","updated_at","snapshot_ts","time","created_at"]),
]

def table_exists(conn, name):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name=?;", (name,))
    return bool(cur.fetchone())

def suggest(cols, candidates):
    cols_low = [c.lower() for c in cols]
    # 1) exact (case-insensitive)
    for cand in candidates:
        if cand.lower() in cols_low:
            return cols[cols_low.index(cand.lower())], "exact"
    # 2) fuzzy (substring)
    for cand in candidates:
        for c in cols:
            if cand.lower() in c.lower():
                return c, f"fuzzy:{cand}"
    # 3) known generic fallbacks
    generics = ["ts","time","timestamp","created_at","updated_at","asof","close_ts","bar_close","end_ts","bar_time","bar_end_time"]
    for g in generics:
        if g.lower() in cols_low:
            return cols[cols_low.index(g.lower())], "generic"
    return None, "none"

def main():
    conn = sqlite3.connect(DB_PATH.as_posix())

    print(f"DB: {DB_PATH}")
    for base, sym_cands, ts_cands in TABLES:
        print("\n" + "="*72)
        print(f"TABLE: {base}")
        if not table_exists(conn, base):
            print("  (missing)")
            continue
        # schema
        info = conn.execute(f"PRAGMA table_info({base});").fetchall()
        cols = [r[1] for r in info]
        print("COLUMNS:")
        print("  " + ", ".join(cols) if cols else "  (none)")
        # samples
        rows = conn.execute(f"SELECT * FROM {base} LIMIT 5;").fetchall()
        if rows:
            print("SAMPLE ROWS (up to 5):")
            for i, row in enumerate(rows, 1):
                print(f"  {i}: " + ", ".join(f"{c}={repr(v)}" for c,v in zip(cols,row)))
        else:
            print("SAMPLE ROWS: (none)")

        # suggestions
        sym = None
        if sym_cands:
            sym, why = suggest(cols, sym_cands)
            print(f"SUGGESTED symbol-like: {sym} ({why})")
        ts, why = suggest(cols, ts_cands)
        print(f"SUGGESTED ts-like: {ts} ({why})")

    conn.close()
    print("\nDone. Use these suggestions to verify/adjust the view builder.")
if __name__ == "__main__":
    main()
