import sqlite3

DB = r"C:\teevra18\data\teevra18.db"
CANDLE_TABLES = ["candles_1m","candles_5m","candles_15m","candles_60m"]

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    instrument_id TEXT NOT NULL,
    t_start       INTEGER NOT NULL,
    open          REAL NOT NULL,
    high          REAL NOT NULL,
    low           REAL NOT NULL,
    close         REAL NOT NULL,
    volume        REAL NOT NULL,
    trades        INTEGER NOT NULL,
    vwap          REAL,
    PRIMARY KEY (instrument_id, t_start)
);
"""

def table_exists(con, table):
    r = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return r is not None

def has_column(con, table, col):
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        return any(r[1] == col for r in rows)
    except Exception:
        return False

def is_empty(con, table):
    try:
        c = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return (c == 0)
    except Exception:
        return True  # treat unreadable as empty to allow recreate

def main():
    con = sqlite3.connect(DB, timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")

    for t in CANDLE_TABLES:
        if table_exists(con, t):
            if not has_column(con, t, "t_start"):
                # Broken schema → drop if empty, else warn
                if is_empty(con, t):
                    print(f"[FIX] Dropping empty broken table: {t}")
                    con.execute(f"DROP TABLE {t}")
                else:
                    print(f"[WARN] {t} has no t_start and is NOT empty. Skipping drop for safety.")
                    print("       Please export or move data, then rerun this tool.")
                    continue

        # (Re)create with correct schema (no-op if already correct)
        con.execute(SCHEMA_SQL.format(table=t))
        # Create index if not present
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_{t}_t ON {t}(t_start);")
        print(f"[OK] Schema guaranteed for {t}")

    con.close()
    print("[DONE] Candles schema repair/check complete.")

if __name__ == "__main__":
    main()
