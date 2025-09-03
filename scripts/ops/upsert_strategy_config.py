import os, json, sqlite3, argparse, datetime

DEFAULT_DB = r"C:\teevra18\data\teevra18.db"

def resolve_db_path():
    for p in [
        os.path.join(os.getcwd(), "teevra18.config.json"),
        os.path.join(os.getcwd(), "configs", "teevra18.config.json"),
    ]:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                db = (cfg.get("paths", {}) or {}).get("sqlite")
                if db:
                    os.makedirs(os.path.dirname(db), exist_ok=True)
                    return db
            except Exception:
                pass
    os.makedirs(os.path.dirname(DEFAULT_DB), exist_ok=True)
    return DEFAULT_DB

def ensure_table(conn: sqlite3.Connection):
    cur = conn.cursor()
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
    conn.commit()

def upsert(conn: sqlite3.Connection, cfg_id: str, name: str):
    cur = conn.cursor()
    now = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    cur.execute("""
        INSERT INTO strategy_config(id, name, created_at, updated_at, status)
        VALUES(?, ?, ?, ?, 'active')
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            updated_at=excluded.updated_at,
            status='active';
    """, (cfg_id, name, now, now))
    conn.commit()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True, help="strategy config id (e.g., lab_ed1c8e66)")
    ap.add_argument("--name", required=True, help="strategy name (e.g., Test Strategy 1)")
    args = ap.parse_args()

    db = resolve_db_path()
    conn = sqlite3.connect(db, timeout=10)
    conn.execute("PRAGMA foreign_keys = ON;")
    ensure_table(conn)
    upsert(conn, args.id, args.name)
    conn.close()
    print(f"Upserted config: {args.id} → {args.name} in {db}")

if __name__ == "__main__":
    main()
