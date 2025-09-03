# services/config_registry.py
import os, json, sqlite3, datetime
from contextlib import closing

def _resolve_db_path():
    # Single source of truth: teevra18.config.json
    for p in [
        os.path.join(os.getcwd(), "teevra18.config.json"),
        os.path.join(os.getcwd(), "configs", "teevra18.config.json"),
    ]:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            dbp = (cfg.get("paths", {}) or {}).get("sqlite")
            if dbp:
                os.makedirs(os.path.dirname(dbp), exist_ok=True)
                return dbp
    # Fallback for dev
    return r"C:\teevra18\data\teevra18.db"

def _now():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def upsert_strategy_config(cfg_id: str, name: str):
    db = _resolve_db_path()
    with closing(sqlite3.connect(db, timeout=10)) as con, closing(con.cursor()) as cur:
        con.execute("PRAGMA foreign_keys = ON;")
        cur.execute("""
            INSERT INTO strategy_config(id, name, created_at, updated_at, status)
            VALUES(?, ?, ?, ?, 'active')
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                updated_at=excluded.updated_at,
                status='active'; -- in case it was soft-deleted earlier
        """, (cfg_id, name, _now(), _now()))
        con.commit()
