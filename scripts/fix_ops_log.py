# C:\teevra18\scripts\fix_ops_log.py
import os, sqlite3, shutil
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

TARGET_COLS = ["ts_utc", "level", "area", "msg"]

CREATE_TARGET = """
CREATE TABLE IF NOT EXISTS ops_log__new (
  ts_utc TEXT NOT NULL,
  level  TEXT NOT NULL,
  area   TEXT NOT NULL,
  msg    TEXT NOT NULL
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

def backup():
    if DB_PATH.exists():
        bak = DB_PATH.with_suffix(".bak_opslog")
        shutil.copy2(DB_PATH, bak)
        print(f"[BACKUP] {bak}")

def migrate():
    with sqlite3.connect(DB_PATH) as c:
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")

        if not table_exists(c, "ops_log"):
            # Fresh create with target schema
            c.executescript(CREATE_TARGET.replace("__new", ""))
            print("[CREATE] ops_log created with target schema.")
            return

        existing = cols(c, "ops_log")
        missing = [x for x in TARGET_COLS if x not in existing]
        if not missing:
            print("[OK] ops_log already has:", existing)
            return

        print(f"[MIGRATE] ops_log missing columns: {missing} -> creating ops_log__new")
        c.executescript(CREATE_TARGET)

        # Best-effort copy from legacy table
        sel_ts = "ts_utc" if "ts_utc" in existing else "datetime('now') AS ts_utc"
        sel_level = "level" if "level" in existing else "'INFO' AS level"
        sel_area = "area" if "area" in existing else "'legacy' AS area"
        sel_msg = "msg" if "msg" in existing else "COALESCE(message,'') AS msg"

        try:
            c.execute(f"""
                INSERT INTO ops_log__new(ts_utc, level, area, msg)
                SELECT {sel_ts}, {sel_level}, {sel_area}, {sel_msg}
                FROM ops_log;
            """)
            print("[MIGRATE] Copied legacy rows into ops_log__new.")
        except Exception as e:
            print("[MIGRATE] Copy skipped:", e)

        c.execute("DROP TABLE ops_log;")
        c.execute("ALTER TABLE ops_log__new RENAME TO ops_log;")
        print("[SWAP] Replaced old ops_log with target schema.")

if __name__ == "__main__":
    backup()
    migrate()
    print("[DONE] ops_log schema ensured.")
