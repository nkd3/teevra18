# C:\teevra18\tools\migrate_kpi_daily.py
import os, sqlite3, datetime as dt

DB_PATH = os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db")

TARGET_DDL = """
CREATE TABLE IF NOT EXISTS kpi_daily_new (
  trade_date TEXT NOT NULL,
  group_name TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  trades_total INTEGER,
  wins INTEGER,
  losses INTEGER,
  win_rate REAL,
  avg_rr REAL,
  gross_pnl REAL,
  fees REAL,
  net_pnl REAL,
  max_drawdown REAL,
  avg_trade_duration_sec REAL,
  kpi_json TEXT,
  created_at_utc TEXT NOT NULL,
  PRIMARY KEY (trade_date, group_name, strategy_id)
);
"""

TARGET_COLS = [
  "trade_date","group_name","strategy_id","trades_total","wins","losses","win_rate",
  "avg_rr","gross_pnl","fees","net_pnl","max_drawdown","avg_trade_duration_sec",
  "kpi_json","created_at_utc"
]

def table_exists(conn, name: str) -> bool:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return cur.fetchone() is not None

def list_cols(conn, table):
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()]

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")

    if not table_exists(conn, "kpi_daily"):
        print("[INFO] kpi_daily not found. Creating fresh target schema.")
        conn.executescript(TARGET_DDL.replace("kpi_daily_new", "kpi_daily"))
        conn.commit()
        print("[OK] kpi_daily created to target schema.")
        return

    print("[INFO] kpi_daily exists. Preparing migration to target schema...")
    conn.executescript("DROP TABLE IF EXISTS kpi_daily_new;")
    conn.executescript(TARGET_DDL)

    old_cols = set(list_cols(conn, "kpi_daily"))
    print(f"[INFO] Existing columns: {sorted(old_cols)}")

    # Build SELECT list for INSERT using best-available source columns.
    # IMPORTANT: never reference a column that doesn't exist in old table.
    now_utc = dt.datetime.utcnow().isoformat()

    def has(col): return col in old_cols

    # trade_date: prefer existing 'trade_date' then legacy 'date', else today()
    if has("trade_date"):
        trade_date_expr = "trade_date"
    elif has("date"):
        trade_date_expr = "date"
    else:
        trade_date_expr = "date('now')"  # SQLite current date (UTC)

    # group_name / strategy_id did not exist before -> defaults
    group_name_expr = "'ALL'"
    strategy_id_expr = "'ALL'"

    # trades_total: prefer 'trades_total', then legacy 'total_trades', else 0
    if has("trades_total"):
        trades_total_expr = "trades_total"
    elif has("total_trades"):
        trades_total_expr = "total_trades"
    else:
        trades_total_expr = "0"

    # wins / losses unknown in old -> 0
    wins_expr = "0"
    losses_expr = "0"

    # win_rate
    win_rate_expr = "win_rate" if has("win_rate") else "0.0"

    # avg_rr
    avg_rr_expr = "avg_rr" if has("avg_rr") else "0.0"

    # gross_pnl / net_pnl
    gross_pnl_expr = "gross_pnl" if has("gross_pnl") else "0.0"
    net_pnl_expr   = "net_pnl"   if has("net_pnl")   else "0.0"

    # fees, max_drawdown, avg_trade_duration_sec absent -> 0
    fees_expr = "0.0"
    mdd_expr = "0.0"
    avgdur_expr = "0.0"

    # kpi_json absent -> NULL
    kpi_json_expr = "NULL"

    # created_at_utc -> now
    created_expr = f"'{now_utc}'"

    select_exprs = [
        f"{trade_date_expr} AS trade_date",
        f"{group_name_expr} AS group_name",
        f"{strategy_id_expr} AS strategy_id",
        f"{trades_total_expr} AS trades_total",
        f"{wins_expr} AS wins",
        f"{losses_expr} AS losses",
        f"{win_rate_expr} AS win_rate",
        f"{avg_rr_expr} AS avg_rr",
        f"{gross_pnl_expr} AS gross_pnl",
        f"{fees_expr} AS fees",
        f"{net_pnl_expr} AS net_pnl",
        f"{mdd_expr} AS max_drawdown",
        f"{avgdur_expr} AS avg_trade_duration_sec",
        f"{kpi_json_expr} AS kpi_json",
        f"{created_expr} AS created_at_utc",
    ]

    insert_sql = f"""
    INSERT OR IGNORE INTO kpi_daily_new ({",".join(TARGET_COLS)})
    SELECT {", ".join(select_exprs)} FROM kpi_daily;
    """
    conn.executescript(insert_sql)

    # Swap tables; keep old as backup
    conn.executescript("""
    ALTER TABLE kpi_daily RENAME TO kpi_daily_old;
    ALTER TABLE kpi_daily_new RENAME TO kpi_daily;
    """)
    conn.commit()
    print("[OK] Migration complete. Old table kept as kpi_daily_old (you may drop later).")

if __name__ == "__main__":
    main()
