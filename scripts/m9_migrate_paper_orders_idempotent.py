import sqlite3, sys

DB = r"C:\teevra18\data\teevra18.db"

PAPER_ORDERS_COLUMNS = [
    ("signal_row_id", "INTEGER"),
    ("signal_id", "TEXT"),
    ("option_symbol", "TEXT"),
    ("underlying_root", "TEXT"),
    ("side", "TEXT"),
    ("lots", "INTEGER"),
    ("qty", "INTEGER"),
    ("lot_size", "INTEGER"),
    ("state", "TEXT"),
    ("entry_price", "REAL"),
    ("fill_price", "REAL"),
    ("sl_price", "REAL"),
    ("tp_price", "REAL"),
    ("exit_price", "REAL"),
    ("created_ts_utc", "TEXT DEFAULT (datetime('now'))"),
    ("delayed_fill_at", "TEXT"),
    ("filled_ts_utc", "TEXT"),
    ("closed_ts_utc", "TEXT"),
    ("charges_at_fill", "REAL DEFAULT 0"),
    ("charges_at_exit", "REAL DEFAULT 0"),
    ("pnl_gross", "REAL"),
    ("pnl_net", "REAL"),
    ("rr_metrics_json", "TEXT"),
    ("extra_json", "TEXT"),
]

INDEXES = [
    ("idx_paper_orders_signal", "CREATE INDEX idx_paper_orders_signal ON paper_orders(signal_row_id)"),
    ("idx_paper_orders_state",  "CREATE INDEX idx_paper_orders_state  ON paper_orders(state)"),
]

def table_exists(conn, name):
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return bool(row)

def get_columns(conn, table):
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()}

def index_exists(conn, name):
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (name,)).fetchone()
    return bool(row)

def create_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS paper_orders (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      signal_row_id     INTEGER NOT NULL,
      signal_id         TEXT    NOT NULL,
      option_symbol     TEXT    NOT NULL,
      underlying_root   TEXT    NOT NULL,
      side              TEXT    NOT NULL,
      lots              INTEGER NOT NULL,
      qty               INTEGER NOT NULL,
      lot_size          INTEGER NOT NULL,
      state             TEXT    NOT NULL,
      entry_price       REAL    NOT NULL,
      fill_price        REAL,
      sl_price          REAL    NOT NULL,
      tp_price          REAL    NOT NULL,
      exit_price        REAL,
      created_ts_utc    TEXT    NOT NULL DEFAULT (datetime('now')),
      delayed_fill_at   TEXT,
      filled_ts_utc     TEXT,
      closed_ts_utc     TEXT,
      charges_at_fill   REAL DEFAULT 0,
      charges_at_exit   REAL DEFAULT 0,
      pnl_gross         REAL,
      pnl_net           REAL,
      rr_metrics_json   TEXT,
      extra_json        TEXT
    );
    """
    conn.executescript(sql)

def add_missing_columns(conn):
    cols = get_columns(conn, "paper_orders")
    alters = []
    for col, decl in PAPER_ORDERS_COLUMNS:
        if col not in cols:
            alters.append(f"ALTER TABLE paper_orders ADD COLUMN {col} {decl};")
    if alters:
        conn.executescript("\n".join(alters))

def create_indexes(conn):
    # only create an index if referenced column exists
    cols = get_columns(conn, "paper_orders")
    for name, ddl in INDEXES:
        if not index_exists(conn, name):
            # quick column presence check
            if name == "idx_paper_orders_signal" and "signal_row_id" not in cols:
                continue
            if name == "idx_paper_orders_state" and "state" not in cols:
                continue
            conn.execute(ddl)

def main():
    conn = sqlite3.connect(DB)
    try:
        if not table_exists(conn, "paper_orders"):
            create_table(conn)
            create_indexes(conn)
            conn.commit()
            print("[M9] Created paper_orders with indexes.")
            return

        # table exists -> add missing cols and indexes
        add_missing_columns(conn)
        create_indexes(conn)
        conn.commit()
        print("[M9] paper_orders migrated idempotently.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
