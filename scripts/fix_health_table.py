# C:\teevra18\scripts\fix_health_table.py
import os, sqlite3, shutil
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

TARGET_COLS = ["key", "value", "ts_utc"]
CREATE_TARGET = """
CREATE TABLE IF NOT EXISTS health__new (
  key    TEXT PRIMARY KEY,
  value  TEXT NOT NULL,
  ts_utc TEXT NOT NULL
);
"""

def table_exists(conn, name):
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,)
    ).fetchone() is not None

def cols(conn, name):
    try:
        return [r[1] for r in conn.execute(f"PRAGMA table_info({name});").fetchall()]
    except sqlite3.OperationalError:
        return []

def backup(db: Path):
    if db.exists():
        bak = db.with_suffix(".bak_health")
        shutil.copy2(db, bak)
        print(f"[BACKUP] {bak}")

def migrate():
    if not DB_PATH.parent.exists():
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as c:
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")

        if not table_exists(c, "health"):
            # Fresh create with target schema
            c.executescript(CREATE_TARGET.replace("__new", ""))
            print("[CREATE] health created with target schema.")
            return

        existing = cols(c, "health")
        if existing == TARGET_COLS:
            print("[OK] health already matches:", existing)
            return

        print(f"[MIGRATE] health columns found: {existing} -> creating health__new")
        c.executescript(CREATE_TARGET)

        # Build a best-effort SELECT mapping from legacy columns to target.
        # We try common legacy names; else fall back to literals.
        key_expr   = "key"   if "key"   in existing else ("k" if "k" in existing else ("name" if "name" in existing else "'unknown_key'"))
        value_expr = "value" if "value" in existing else ("v" if "v" in existing else ("val"  if "val"  in existing else "''"))
        ts_expr    = "ts_utc" if "ts_utc" in existing else ("ts" if "ts" in existing else ("updated_at" if "updated_at" in existing else "datetime('now')"))

        try:
            c.execute(f"""
                INSERT INTO health__new(key, value, ts_utc)
                SELECT {key_expr} AS key, {value_expr} AS value, {ts_expr} AS ts_utc
                FROM health;
            """)
            moved = c.total_changes
            print(f"[MIGRATE] Copied ~{moved} existing rows into health__new.")
        except Exception as e:
            print("[MIGRATE] Copy skipped or partial:", e)

        c.execute("DROP TABLE health;")
        c.execute("ALTER TABLE health__new RENAME TO health;")
        print("[SWAP] Replaced old health with target schema.")

if __name__ == "__main__":
    backup(DB_PATH)
    migrate()
    print("[DONE] health schema ensured (key,value,ts_utc).")
