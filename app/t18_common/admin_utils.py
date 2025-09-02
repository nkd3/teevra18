from typing import Dict, Any
from t18_common.db import get_conn, table_exists

def _ensure_table(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS alerts_settings(
        key TEXT PRIMARY KEY,
        value TEXT,
        enabled INTEGER DEFAULT 1,
        updated_at TEXT DEFAULT (datetime('now','localtime'))
    )""")
    con.commit()

def get_alerts_map(con=None) -> Dict[str, Dict[str, Any]]:
    close_after = False
    own = False
    if con is None:
        con = get_conn()
        own = True
    try:
        _ensure_table(con)
        cur = con.execute("SELECT key, value, enabled FROM alerts_settings")
        return {r["key"]: {"value": r["value"], "enabled": bool(r["enabled"])} for r in cur.fetchall()}
    finally:
        if own: con.close()

def upsert_alert_setting(con, key: str, value: str, enabled: bool = True) -> None:
    _ensure_table(con)
    con.execute("""
        INSERT INTO alerts_settings(key, value, enabled, updated_at)
        VALUES(?, ?, ?, datetime('now','localtime'))
        ON CONFLICT(key) DO UPDATE SET
            value=excluded.value,
            enabled=excluded.enabled,
            updated_at=datetime('now','localtime')
    """, (key, value, 1 if enabled else 0))
    con.commit()
