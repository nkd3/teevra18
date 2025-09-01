import sqlite3, json, sys

DB_PATH = r"C:\teevra18\data\teevra18.db"

# --- Expected schema (Stage M0) ---
EXPECTED_TABLES = [
    "ops_log",
    "health",
    "breaker_state",
    "universe_underlyings",
    "universe_derivatives",
    "universe_watchlist",
    "policies_group",
    "policies_instrument",
    "strategies_catalog",
    "rr_profiles",
    "rr_overrides",
    "ticks_raw",
    "depth20_snap",
    "option_chain_snap",
    "quote_snap",
    "candles_1m",
    "candles_5m",
    "candles_15m",
    "candles_60m",
    "signals",
    "paper_orders",
    "backtest_orders",
    "predictions",
    "kpi_daily",
    "live_journal",
    "config_meta",
    "token_status",
]

EXPECTED_INDEXES = [
    "idx_health_service_ts",
    "idx_ticks_symbol_ts",
    "idx_depth20_symbol_ts",
    "idx_chain_underlying_ts",
    "idx_c1_symbol_ts",
    "idx_c5_symbol_ts",
    "idx_c15_symbol_ts",
    "idx_c60_symbol_ts",
    "idx_signals_symbol_ts",
    "idx_signals_strategy_ts",
    "idx_bt_run",
    "idx_bt_symbol_entry",
]

def fetch_set(cur, sql, col=0):
    cur.execute(sql)
    return {row[col] for row in cur.fetchall()}

def main():
    results = {"pass": True, "details": {}}

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # 1) WAL mode
    cur.execute("PRAGMA journal_mode;")
    wal_mode = cur.fetchone()[0]
    results["details"]["wal_mode"] = wal_mode
    if wal_mode.lower() != "wal":
        results["pass"] = False

    # 2) Tables present
    actual_tables = fetch_set(cur, "SELECT name FROM sqlite_master WHERE type='table';")
    missing_tables = sorted([t for t in EXPECTED_TABLES if t not in actual_tables])
    extra_tables = sorted(list(actual_tables - set(EXPECTED_TABLES)))
    results["details"]["tables"] = {
        "missing": missing_tables,
        "extra": extra_tables,
        "count": len(actual_tables)
    }
    if missing_tables:
        results["pass"] = False

    # 3) Indexes present (ignore SQLite auto indexes)
    actual_indexes = fetch_set(cur, "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_autoindex%';")
    missing_indexes = sorted([i for i in EXPECTED_INDEXES if i not in actual_indexes])
    results["details"]["indexes"] = {
        "missing": missing_indexes,
        "present_count": len(actual_indexes)
    }
    if missing_indexes:
        results["pass"] = False

    # 4) Seed checks
    # breaker_state
    cur.execute("SELECT state, updated_at FROM breaker_state WHERE id=1;")
    br = cur.fetchone()
    results["details"]["breaker_state"] = br[0] if br else None
    if not br or br[0] not in ("RUNNING", "PAUSED", "PANIC"):
        results["pass"] = False

    # health row count
    cur.execute("SELECT COUNT(*) FROM health;")
    health_rows = cur.fetchone()[0]
    results["details"]["health_rows"] = health_rows
    if health_rows < 1:
        results["pass"] = False

    # rr_profiles baseline
    cur.execute("SELECT COUNT(*) FROM rr_profiles WHERE profile_name='BASELINE';")
    results["details"]["rr_baseline_rows"] = cur.fetchone()[0]
    if results["details"]["rr_baseline_rows"] < 1:
        results["pass"] = False

    # strategies_catalog seed
    cur.execute("SELECT COUNT(*) FROM strategies_catalog WHERE strategy_id='ema_vwap_atr';")
    results["details"]["strategies_seed_rows"] = cur.fetchone()[0]
    if results["details"]["strategies_seed_rows"] < 1:
        results["pass"] = False

    con.close()

    # Pretty print + exit code for CI friendliness
    print(json.dumps(results, indent=2))
    sys.exit(0 if results["pass"] else 1)

if __name__ == "__main__":
    main()
