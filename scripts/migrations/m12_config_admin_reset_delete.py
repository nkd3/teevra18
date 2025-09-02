# scripts/migrations/m12_config_admin_reset_delete.py
import os, sqlite3, json, datetime

def resolve_db_path():
    # Try config file first
    cfg_candidates = [
        os.path.join(os.getcwd(), "teevra18.config.json"),
        os.path.join(os.getcwd(), "configs", "teevra18.config.json"),
    ]
    for p in cfg_candidates:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                dbp = (cfg.get("paths", {}) or {}).get("sqlite")
                if dbp and os.path.exists(dbp):
                    return dbp
            except Exception:
                pass
    # Fallback default
    default_path = r"C:\teevra18\data\teevra18.db"
    return default_path

def safe_exec(cur, sql):
    try:
        cur.execute(sql)
    except sqlite3.OperationalError as e:
        # Usually "duplicate column" or "already exists"
        print(f"[skip] {e} for SQL: {sql!r}")

def main():
    db_path = resolve_db_path()
    print(f"[info] Using DB: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()

    # 1) Add soft-delete columns to strategy_config
    safe_exec(cur, "ALTER TABLE strategy_config ADD COLUMN status TEXT DEFAULT 'active';")
    safe_exec(cur, "ALTER TABLE strategy_config ADD COLUMN deleted_at TEXT;")
    safe_exec(cur, "CREATE INDEX IF NOT EXISTS idx_strategy_config_status ON strategy_config(status);")

    # 2) Audit table for admin actions
    safe_exec(cur, """
    CREATE TABLE IF NOT EXISTS config_admin_audit(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      config_id TEXT NOT NULL,
      action TEXT NOT NULL,         -- 'soft_delete' | 'reset'
      reason TEXT,                  -- optional
      actor TEXT,                   -- UI user or system
      ts_utc TEXT NOT NULL          -- ISO ts
    );
    """)

    conn.commit()
    conn.close()
    print("[done] Migration applied.")

if __name__ == "__main__":
    main()
