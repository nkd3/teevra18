import os, json, sqlite3

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
                    return db
            except Exception:
                pass
    return DEFAULT_DB

db = resolve_db_path()
con = sqlite3.connect(db); cur = con.cursor()
cur.execute("SELECT id, name, COALESCE(status,'active') FROM strategy_config ORDER BY updated_at DESC;")
rows = cur.fetchall()
for r in rows:
    print(r)
con.close()
