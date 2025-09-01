# C:\teevra18\scripts\patch_schema_m1.py
import os, sqlite3, shutil
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

REQUIRED_TICKS_RAW_COLS = [
    "ts_utc","exchange_segment","security_id","mode","ltt_epoch","ltp","atp","last_qty",
    "volume","buy_qty_total","sell_qty_total","oi","day_open","day_high","day_low",
    "day_close","prev_close","recv_ts_utc"
]

CREATE_TICKS_RAW = """
CREATE TABLE IF NOT EXISTS ticks_raw (
  ts_utc            TEXT NOT NULL,
  exchange_segment  INTEGER NOT NULL,
  security_id       INTEGER NOT NULL,
  mode              TEXT NOT NULL,
  ltt_epoch         INTEGER,
  ltp               REAL,
  atp               REAL,
  last_qty          INTEGER,
  volume            INTEGER,
  buy_qty_total     INTEGER,
  sell_qty_total    INTEGER,
  oi                INTEGER,
  day_open          REAL,
  day_high          REAL,
  day_low           REAL,
  day_close         REAL,
  prev_close        REAL,
  recv_ts_utc       TEXT NOT NULL
);
"""

CREATE_IDX_TICKS = "CREATE INDEX IF NOT EXISTS idx_ticks_sid_ts ON ticks_raw(security_id, ts_utc);"

CREATE_UNIVERSE_WATCHLIST = """
CREATE TABLE IF NOT EXISTS universe_watchlist (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  exchange_segment INTEGER NOT NULL,
  security_id INTEGER NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  is_hot_option INTEGER NOT NULL DEFAULT 0
);
"""

def _cols(c, table):
    cur = c.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]

def _count(c, table):
    try:
        return c.execute(f"SELECT COUNT(1) FROM {table}").fetchone()[0]
    except sqlite3.OperationalError:
        return None

def ensure_universe_watchlist(c):
    c.executescript(CREATE_UNIVERSE_WATCHLIST)

def ensure_ticks_raw(c):
    # If table doesn't exist, create from scratch + index
    try:
        existing = _cols(c, "ticks_raw")
    except sqlite3.OperationalError:
        existing = None

    if existing is None:
        c.executescript(CREATE_TICKS_RAW)
        c.execute(CREATE_IDX_TICKS)
        return

    # Table exists: verify columns
    missing = [col for col in REQUIRED_TICKS_RAW_COLS if col not in existing]
    if not missing:
        # Ensure index exists
        c.execute(CREATE_IDX_TICKS)
        return

    # Need migration
    rowcount = _count(c, "ticks_raw") or 0
    print(f"[MIGRATE] ticks_raw missing columns: {missing}; rows={rowcount}")

    # Create new table with correct schema
    c.executescript("""
    CREATE TABLE IF NOT EXISTS ticks_raw__new (
      ts_utc            TEXT NOT NULL,
      exchange_segment  INTEGER NOT NULL,
      security_id       INTEGER NOT NULL,
      mode              TEXT NOT NULL,
      ltt_epoch         INTEGER,
      ltp               REAL,
      atp               REAL,
      last_qty          INTEGER,
      volume            INTEGER,
      buy_qty_total     INTEGER,
      sell_qty_total    INTEGER,
      oi                INTEGER,
      day_open          REAL,
      day_high          REAL,
      day_low           REAL,
      day_close         REAL,
      prev_close        REAL,
      recv_ts_utc       TEXT NOT NULL
    );
    """)

    if rowcount > 0:
        # Migrate known columns; set missing ones to NULL
        select_cols = []
        for col in REQUIRED_TICKS_RAW_COLS:
            if col in existing:
                select_cols.append(col)
            else:
                select_cols.append("NULL AS " + col)
        sel = ", ".join(select_cols)
        c.execute(f"INSERT INTO ticks_raw__new ({', '.join(REQUIRED_TICKS_RAW_COLS)}) SELECT {sel} FROM ticks_raw")
        print(f"[MIGRATE] Copied {rowcount} rows into ticks_raw__new")

    # Swap tables
    c.execute("DROP TABLE ticks_raw")
    c.execute("ALTER TABLE ticks_raw__new RENAME TO ticks_raw")
    c.execute(CREATE_IDX_TICKS)
    print("[MIGRATE] ticks_raw migrated & index created.")

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    # optional safety backup
    if DB_PATH.exists():
        bak = DB_PATH.with_suffix(".bak")
        shutil.copy2(DB_PATH, bak)
        print(f"[BACKUP] {bak}")
    with sqlite3.connect(DB_PATH) as c:
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")
        ensure_ticks_raw(c)
        ensure_universe_watchlist(c)
    print("[OK] Schema ensured for M1: ticks_raw + universe_watchlist (+index)")

if __name__ == "__main__":
    main()
