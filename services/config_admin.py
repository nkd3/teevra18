# services/config_admin.py
import os, sqlite3, json, datetime
from contextlib import closing

def _now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _resolve_db_path():
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
    return r"C:\teevra18\data\teevra18.db"

def _connect():
    conn = sqlite3.connect(_resolve_db_path(), timeout=10, isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON;")
    # Enable WAL for fewer locks on UI usage
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn

def list_configs(include_deleted=False):
    q = "SELECT id, name, COALESCE(status,'active') as status FROM strategy_config"
    if not include_deleted:
        q += " WHERE COALESCE(status,'active') <> 'deleted'"
    q += " ORDER BY name COLLATE NOCASE;"
    with closing(_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(q)
        return cur.fetchall()

def soft_delete_config(config_id: str, reason: str = "", actor: str = "ui"):
    with closing(_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute("BEGIN;")
        cur.execute(
            "UPDATE strategy_config SET status='deleted', deleted_at=? WHERE id=?;",
            (_now_iso(), config_id),
        )
        cur.execute(
            "INSERT INTO config_admin_audit(config_id, action, reason, actor, ts_utc) VALUES (?,?,?,?,?);",
            (config_id, "soft_delete", reason, actor, _now_iso()),
        )
        conn.commit()

def reset_config(config_id: str, actor: str = "ui"):
    with closing(_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute("BEGIN;")
        # Purge children (adjust table names if different)
        for table in [
            "strategy_params", 
            "strategy_policies", 
            "strategy_liquidity", 
            "strategy_notifications"
        ]:
            cur.execute(f"DELETE FROM {table} WHERE config_id=?;", (config_id,))
        # Optional: Reset some base fields on strategy_config itself (not status/id)
        # e.g., clear versioning fields:
        # cur.execute("UPDATE strategy_config SET version=NULL, updated_at=? WHERE id=?;", (_now_iso(), config_id))
        cur.execute(
            "INSERT INTO config_admin_audit(config_id, action, reason, actor, ts_utc) VALUES (?,?,?,?,?);",
            (config_id, "reset", "purge children keep id", actor, _now_iso()),
        )
        conn.commit()
