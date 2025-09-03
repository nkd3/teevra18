# services/config_admin.py
import os, sqlite3, json, datetime
from contextlib import closing

# --- Optional auto-bootstrap (safe to keep)
# If your bootstrap script exists, this will create the tables once.
try:
    from scripts.db.bootstrap_m12_config_admin import main as _bootstrap
    _bootstrap()
except Exception:
    # If import fails due to relative paths, we simply skip; explicit bootstrap covers it.
    pass


# ---------- Internals ----------

def _now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _resolve_db_path():
    """
    Single source of truth: teevra18.config.json (root or /configs).
    Falls back to C:\teevra18\data\teevra18.db and ensures the folder exists.
    """
    for p in [
        os.path.join(os.getcwd(), "teevra18.config.json"),
        os.path.join(os.getcwd(), "configs", "teevra18.config.json"),
    ]:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                dbp = (cfg.get("paths", {}) or {}).get("sqlite")
                if dbp:
                    os.makedirs(os.path.dirname(dbp), exist_ok=True)
                    return dbp
            except Exception:
                pass
    fallback = r"C:\teevra18\data\teevra18.db"
    os.makedirs(os.path.dirname(fallback), exist_ok=True)
    return fallback

def _connect():
    conn = sqlite3.connect(_resolve_db_path(), timeout=10, isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    _ensure_schema(conn)
    return conn

def _ensure_schema(conn: sqlite3.Connection):
    """
    Idempotent schema guard so UI never crashes if migration was skipped.
    """
    cur = conn.cursor()
    # Master table used by Admin
    cur.execute("""
        CREATE TABLE IF NOT EXISTS strategy_config(
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT,
            status TEXT DEFAULT 'active',
            deleted_at TEXT
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_config_status ON strategy_config(status);")

    # Optional audit table (best-effort)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS config_admin_audit(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          config_id TEXT NOT NULL,
          action TEXT NOT NULL,        -- 'soft_delete' | 'reset' | 'restore'
          reason TEXT,
          actor TEXT,
          ts_utc TEXT NOT NULL,
          FOREIGN KEY(config_id) REFERENCES strategy_config(id) ON DELETE CASCADE
        );
    """)

    # Children (optional) – deletes in reset() are guarded with try/except
    cur.execute("""
        CREATE TABLE IF NOT EXISTS strategy_params(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            config_id TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT,
            FOREIGN KEY(config_id) REFERENCES strategy_config(id) ON DELETE CASCADE
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS strategy_policies(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            config_id TEXT NOT NULL,
            policy_key TEXT NOT NULL,
            policy_value TEXT,
            FOREIGN KEY(config_id) REFERENCES strategy_config(id) ON DELETE CASCADE
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS strategy_liquidity(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            config_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            filters TEXT,
            FOREIGN KEY(config_id) REFERENCES strategy_config(id) ON DELETE CASCADE
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS strategy_notifications(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            config_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            settings TEXT,
            FOREIGN KEY(config_id) REFERENCES strategy_config(id) ON DELETE CASCADE
        );
    """)
    conn.commit()


# ---------- Public API used by Streamlit pages ----------

def list_configs(include_deleted: bool = False):
    """
    Return (id, name, status) rows for Admin grid.
    """
    q = "SELECT id, name, COALESCE(status,'active') as status FROM strategy_config"
    if not include_deleted:
        q += " WHERE COALESCE(status,'active') <> 'deleted'"
    q += " ORDER BY name COLLATE NOCASE;"
    with closing(_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(q)
        return cur.fetchall()

def soft_delete_config(config_id: str, reason: str = "", actor: str = "ui"):
    """
    Mark as deleted (soft delete). Keeps the row, sets deleted_at.
    """
    with closing(_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute("BEGIN;")
        cur.execute(
            "UPDATE strategy_config SET status='deleted', deleted_at=?, updated_at=? WHERE id=?;",
            (_now_iso(), _now_iso(), config_id),
        )
        # Audit best-effort
        try:
            cur.execute(
                "INSERT INTO config_admin_audit(config_id, action, reason, actor, ts_utc) VALUES (?,?,?,?,?);",
                (config_id, "soft_delete", reason, actor, _now_iso()),
            )
        except Exception:
            pass
        conn.commit()

def reset_config(config_id: str, actor: str = "ui"):
    """
    Purge children (params/policies/liquidity/notifications) but keep the same id in strategy_config.
    """
    with closing(_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute("BEGIN;")
        for table in ["strategy_params", "strategy_policies", "strategy_liquidity", "strategy_notifications"]:
            try:
                cur.execute(f"DELETE FROM {table} WHERE config_id=?;", (config_id,))
            except Exception:
                # Child table may not exist yet; ignore to keep operation resilient
                pass
        # Optional: also bump updated_at so Admin shows recency
        cur.execute("UPDATE strategy_config SET updated_at=? WHERE id=?;", (_now_iso(), config_id))
        try:
            cur.execute(
                "INSERT INTO config_admin_audit(config_id, action, reason, actor, ts_utc) VALUES (?,?,?,?,?);",
                (config_id, "reset", "purge children keep id", actor, _now_iso()),
            )
        except Exception:
            pass
        conn.commit()

def restore_config(config_id: str, actor: str = "ui"):
    """
    Restore (undelete): status='active', deleted_at=NULL.
    """
    with closing(_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute("BEGIN;")
        cur.execute(
            "UPDATE strategy_config SET status='active', deleted_at=NULL, updated_at=? WHERE id=?;",
            (_now_iso(), config_id),
        )
        try:
            cur.execute(
                "INSERT INTO config_admin_audit(config_id, action, reason, actor, ts_utc) VALUES (?,?,?,?,?);",
                (config_id, "restore", "undelete", actor, _now_iso()),
            )
        except Exception:
            pass
        conn.commit()

def upsert_config(config_id: str, name: str, actor: str = "ui"):
    """
    General-purpose upsert so other pages (e.g., Strategy Lab) can register/update a config.
    Ensures it is 'active' if previously soft-deleted.
    """
    with closing(_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute("BEGIN;")
        cur.execute("""
            INSERT INTO strategy_config(id, name, created_at, updated_at, status)
            VALUES(?, ?, ?, ?, 'active')
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                updated_at=excluded.updated_at,
                status='active';
        """, (config_id, name, _now_iso(), _now_iso()))
        try:
            cur.execute(
                "INSERT INTO config_admin_audit(config_id, action, reason, actor, ts_utc) VALUES (?,?,?,?,?);",
                (config_id, "upsert", "auto-register from UI", actor, _now_iso()),
            )
        except Exception:
            pass
        conn.commit()
