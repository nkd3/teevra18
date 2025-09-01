import os, sqlite3
from pathlib import Path

DB = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
with sqlite3.connect(DB) as conn:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(pred_oos_log);")}
    if "pred_id" not in cols:
        conn.execute("ALTER TABLE pred_oos_log ADD COLUMN pred_id INTEGER;")
        conn.commit()
        print("[OK] Added pred_id to pred_oos_log")
    else:
        print("[OK] pred_id already exists")
