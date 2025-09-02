# C:\teevra18\app\t18_common\admin_utils.py
from typing import Dict, Any, Optional, Tuple
from t18_common.db import get_conn, table_exists, columns

def _ensure_table(con):
    # Works with either a simple schema (key,value,enabled,updated_at)
    # or an extended schema that includes a 'channel' column.
    if not table_exists(con, "alerts_settings"):
        con.execute("""
        CREATE TABLE IF NOT EXISTS alerts_settings(
            key TEXT PRIMARY KEY,
            value TEXT,
            enabled INTEGER DEFAULT 1,
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        )""")
        con.commit()

def _has_channel(con) -> bool:
    if not table_exists(con, "alerts_settings"):
        return False
    return "channel" in set(columns(con, "alerts_settings"))

def get_alerts_map(con=None, channel: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    """
    Backward-compatible:
      - called as get_alerts_map() or get_alerts_map(con)
      - your pages call get_alerts_map(conn, "telegram")
    If a 'channel' column exists, filter by that.
    Otherwise, filter by key prefix:  f"{channel}:<name>"
    """
    own = False
    if con is None:
        con = get_conn(); own = True
    try:
        _ensure_table(con)
        use_channel = _has_channel(con)
        if channel:
            if use_channel:
                cur = con.execute("SELECT key, value, enabled FROM alerts_settings WHERE channel=?", (channel,))
                rows = cur.fetchall()
            else:
                cur = con.execute("SELECT key, value, enabled FROM alerts_settings WHERE key LIKE ?", (f"{channel}:%",))
                rows = cur.fetchall()
        else:
            cur = con.execute("SELECT key, value, enabled FROM alerts_settings")
            rows = cur.fetchall()

        result: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            k = r["key"]
            v = {"value": r["value"], "enabled": bool(r["enabled"])}
            if channel and not use_channel and k.startswith(f"{channel}:"):
                # strip "channel:" prefix in result keys
                k = k.split(":", 1)[1]
            result[k] = v
        return result
    finally:
        if own: con.close()

def upsert_alert_setting(con, *args, **kwargs) -> None:
    """
    Flexible signature so existing pages keep working.

    Accepts either:
      (con, key, value, enabled=True)
    or:
      (con, channel, key, value, enabled=True)

    If the DB has a 'channel' column we store it separately.
    Else we prefix keys as "channel:key".
    """
    _ensure_table(con)
    use_channel = _has_channel(con)
    enabled = True

    if len(args) >= 4:
        # (con, channel, key, value, [enabled])
        channel, key, value = args[0], args[1], args[2]
        if len(args) >= 5:
            enabled = bool(args[3])
        if use_channel:
            con.execute("""
                INSERT INTO alerts_settings(key, value, enabled, updated_at, channel)
                VALUES(?, ?, ?, datetime('now','localtime'), ?)
                ON CONFLICT(key) DO UPDATE SET
                   value=excluded.value, enabled=excluded.enabled,
                   updated_at=datetime('now','localtime'), channel=excluded.channel
            """, (key, value, 1 if enabled else 0, channel))
        else:
            full_key = f"{channel}:{key}"
            con.execute("""
                INSERT INTO alerts_settings(key, value, enabled, updated_at)
                VALUES(?, ?, ?, datetime('now','localtime'))
                ON CONFLICT(key) DO UPDATE SET
                   value=excluded.value, enabled=excluded.enabled,
                   updated_at=datetime('now','localtime')
            """, (full_key, value, 1 if enabled else 0))
    elif len(args) >= 3:
        # (con, key, value, [enabled])
        key, value = args[0], args[1]
        if len(args) >= 4:
            enabled = bool(args[2])
        con.execute("""
            INSERT INTO alerts_settings(key, value, enabled, updated_at)
            VALUES(?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value, enabled=excluded.enabled,
              updated_at=datetime('now','localtime')
        """, (key, value, 1 if enabled else 0))
    else:
        raise TypeError("upsert_alert_setting: expected (con, key, value [,enabled]) or (con, channel, key, value [,enabled])")
    con.commit()
