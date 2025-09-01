# C:\teevra18\scripts\migrate_m7.py  — self-healing (rebuild if needed)
import sqlite3, datetime
from pathlib import Path

DB = Path(r"C:\teevra18\data\teevra18.db")

FULL_CREATE = r"""
CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_utc TEXT NOT NULL,
  security_id TEXT NOT NULL,
  symbol TEXT,
  group_name TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  side TEXT NOT NULL CHECK(side IN ('LONG','SHORT')),
  entry REAL NOT NULL,
  stop REAL NOT NULL,
  target REAL NOT NULL,
  rr REAL NOT NULL,
  sl_per_lot REAL NOT NULL,
  reason TEXT,
  version TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'PENDING',
  deterministic_hash TEXT NOT NULL,
  run_id TEXT NOT NULL,
  created_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

HEALTH_CREATE = r"""
CREATE TABLE IF NOT EXISTS health (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  ts_utc TEXT NOT NULL
);
"""

INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_signals_day ON signals(substr(created_at_utc,1,10));",
    "CREATE INDEX IF NOT EXISTS idx_signals_state ON signals(state);",
    "CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_signals_key ON signals(ts_utc, security_id, strategy_id, side, deterministic_hash);"
]

# Columns that MUST exist for M7
REQUIRED_CORE = [
    "ts_utc","security_id","symbol","group_name","strategy_id","side",
    "entry","stop","target","rr","sl_per_lot","reason","version",
    "state","deterministic_hash","run_id","created_at_utc"
]

# If signals exists but lacks any REQUIRED_CORE, we rebuild with FULL schema.
def table_exists(conn, name):
    r = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,)).fetchone()
    return bool(r)

def colset(conn, table):
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()}

def needs_rebuild(conn):
    if not table_exists(conn, "signals"):
        return False  # will create fresh later
    existing = colset(conn, "signals")
    missing = [c for c in REQUIRED_CORE if c not in existing]
    return len(missing) > 0

def seed_breaker(conn):
    cur = conn.execute("SELECT 1 FROM health WHERE key='m7_breaker' LIMIT 1").fetchone()
    if not cur:
        ts = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
        conn.execute("INSERT INTO health(key,value,ts_utc) VALUES(?,?,?)", ("m7_breaker","RUNNING",ts))

def create_indexes(conn):
    for sql in INDEX_SQL:
        conn.execute(sql)

def create_fresh(conn):
    conn.executescript(FULL_CREATE)
    create_indexes(conn)

def rebuild_signals(conn):
    # Build a new table with the full schema
    conn.execute("DROP TABLE IF EXISTS signals_new;")
    conn.execute(FULL_CREATE.replace("IF NOT EXISTS signals", "IF NOT EXISTS signals_new").replace(" signals ", " signals_new "))

    # Determine columns present in old 'signals'
    old_cols = colset(conn, "signals")

    # Prepare a SELECT that maps old columns and fills defaults for missing ones
    # Defaults chosen to keep constraints happy and make sense for history
    def has(c): return c in old_cols
    select_parts = []
    # Order must match REQUIRED_CORE & schema insert list below
    # id is auto; we do not copy it to avoid conflicts
    mappings = {
        "ts_utc":           "ts_utc"            if has("ts_utc")           else "datetime('now')",
        "security_id":      "security_id"       if has("security_id")      else "''",
        "symbol":           "symbol"            if has("symbol")           else "NULL",
        "group_name":       "group_name"        if has("group_name")       else "''",
        "strategy_id":      "strategy_id"       if has("strategy_id")      else "''",
        "side":             "side"              if has("side")             else "'LONG'",
        "entry":            "entry"             if has("entry")            else "0.0",
        "stop":             "stop"              if has("stop")             else "0.0",
        "target":           "target"            if has("target")           else "0.0",
        "rr":               "rr"                if has("rr")               else "0.0",
        "sl_per_lot":       "sl_per_lot"        if has("sl_per_lot")       else "0.0",
        "reason":           "reason"            if has("reason")           else "NULL",
        "version":          "version"           if has("version")          else "'m7-core-1.0'",
        "state":            "state"             if has("state")            else "'PENDING'",
        "deterministic_hash":"deterministic_hash" if has("deterministic_hash") else "hex(randomblob(8))",
        "run_id":           "run_id"            if has("run_id")           else "'migrate'",
        "created_at_utc":   "created_at_utc"    if has("created_at_utc")   else "datetime('now')"
    }

    for k in ["ts_utc","security_id","symbol","group_name","strategy_id","side",
              "entry","stop","target","rr","sl_per_lot","reason","version",
              "state","deterministic_hash","run_id","created_at_utc"]:
        select_parts.append(mappings[k])

    select_sql = "SELECT " + ", ".join(select_parts) + " FROM signals"

    # Insert data into new table
    conn.execute("""
        INSERT INTO signals_new
        (ts_utc,security_id,symbol,group_name,strategy_id,side,entry,stop,target,rr,sl_per_lot,reason,version,state,deterministic_hash,run_id,created_at_utc)
        """ + select_sql + ";")

    # Swap tables
    conn.execute("DROP TABLE IF EXISTS signals;")
    conn.execute("ALTER TABLE signals_new RENAME TO signals;")

    # Ensure indexes on the new table
    create_indexes(conn)

def main():
    DB.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")

        # Health table + breaker
        conn.executescript(HEALTH_CREATE)
        seed_breaker(conn)

        if not table_exists(conn, "signals"):
            # Fresh create
            create_fresh(conn)
            conn.commit()
            print("M7 migration: signals created fresh with full schema + indexes; breaker seeded.")
            return

        if needs_rebuild(conn):
            rebuild_signals(conn)
            conn.commit()
            print("M7 migration: signals rebuilt to full schema; indexes ensured; breaker seeded.")
            return

        # Already ok — ensure indexes (idempotent)
        create_indexes(conn)
        conn.commit()
        print("M7 migration: signals schema verified; indexes ensured; breaker seeded.")

if __name__ == "__main__":
    main()
