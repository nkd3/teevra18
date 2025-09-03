# scripts/db/bootstrap_m12_config_admin.py
import os, json, sqlite3, datetime

DEFAULT_DB = r"C:\teevra18\data\teevra18.db"

def _resolve_db_path():
    # Try config file(s) if present
    for p in [
        os.path.join(os.getcwd(), "teevra18.config.json"),
        os.path.join(os.getcwd(), "configs", "teevra18.config.json"),
    ]:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                candidate = (cfg.get("paths", {}) or {}).get("sqlite")
                if candidate:
                    # Ensure parent dir exists
                    os.makedirs(os.path.dirname(candidate), exist_ok=True)
                    return candidate
            except Exception:
                pass
    # Fallback
    os.makedirs(os.path.dirname(DEFAULT_DB), exist_ok=True)
    return DEFAULT_DB

def _conn(db_path):
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def _table_exists(cur, name):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return cur.fetchone() is not None

def _col_exists(cur, table, col):
    cur.execute(f"PRAGMA table_info({table});")
    return any(r[1] == col for r in cur.fetchall())

def main():
    db_path = _resolve_db_path()
    print(f"[info] DB: {db_path}")
    with _conn(db_path) as con:
        cur = con.cursor()

        # 1) strategy_config (master)
        if not _table_exists(cur, "strategy_config"):
            cur.execute("""
                CREATE TABLE strategy_config(
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TEXT,
                    updated_at TEXT
                );
            """)
            print("[create] strategy_config")

        # Add soft-delete columns if missing
        if not _col_exists(cur, "strategy_config", "status"):
            cur.execute("ALTER TABLE strategy_config ADD COLUMN status TEXT DEFAULT 'active';")
            print("[alter] strategy_config.status")
        if not _col_exists(cur, "strategy_config", "deleted_at"):
            cur.execute("ALTER TABLE strategy_config ADD COLUMN deleted_at TEXT;")
            print("[alter] strategy_config.deleted_at")
        # Helpful index
        cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_config_status ON strategy_config(status);")

        # 2) children tables (params, policies, liquidity, notifications)
        children = {
            "strategy_params": """
                CREATE TABLE strategy_params(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    config_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT,
                    FOREIGN KEY(config_id) REFERENCES strategy_config(id) ON DELETE CASCADE
                );
            """,
            "strategy_policies": """
                CREATE TABLE strategy_policies(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    config_id TEXT NOT NULL,
                    policy_key TEXT NOT NULL,
                    policy_value TEXT,
                    FOREIGN KEY(config_id) REFERENCES strategy_config(id) ON DELETE CASCADE
                );
            """,
            "strategy_liquidity": """
                CREATE TABLE strategy_liquidity(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    config_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    filters TEXT,
                    FOREIGN KEY(config_id) REFERENCES strategy_config(id) ON DELETE CASCADE
                );
            """,
            "strategy_notifications": """
                CREATE TABLE strategy_notifications(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    config_id TEXT NOT NULL,
                    channel TEXT NOT NULL,  -- e.g., telegram/eod
                    settings TEXT,
                    FOREIGN KEY(config_id) REFERENCES strategy_config(id) ON DELETE CASCADE
                );
            """,
        }
        for t, ddl in children.items():
            if not _table_exists(cur, t):
                cur.execute(ddl)
                print(f"[create] {t}")
            # Helpful index
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{t}_config_id ON {t}(config_id);")

        # 3) audit table
        if not _table_exists(cur, "config_admin_audit"):
            cur.execute("""
                CREATE TABLE config_admin_audit(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  config_id TEXT NOT NULL,
                  action TEXT NOT NULL,   -- 'soft_delete' | 'reset'
                  reason TEXT,
                  actor TEXT,
                  ts_utc TEXT NOT NULL,
                  FOREIGN KEY(config_id) REFERENCES strategy_config(id) ON DELETE CASCADE
                );
            """)
            print("[create] config_admin_audit")

        con.commit()
    print("[done] bootstrap/migration complete.")

if __name__ == "__main__":
    main()
