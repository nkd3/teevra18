# C:\teevra18\services\m11\health_m11.py
import sqlite3, os
from pathlib import Path
from datetime import datetime, timezone

DB = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

with sqlite3.connect(DB) as conn:
    cur = conn.cursor()
    def count(q): return conn.execute(q).fetchone()[0]

    checks = {
        "predictions_24h": count("SELECT COUNT(*) FROM predictions_m11 WHERE created_at >= datetime('now','-24 hours');"),
        "signals_24h":     count("SELECT COUNT(*) FROM signals_m11     WHERE created_at >= datetime('now','-24 hours');"),
        "oos_24h":         count("SELECT COUNT(*) FROM pred_oos_log    WHERE created_at >= datetime('now','-24 hours');"),
    }
    msg = f"[HEALTH] {now} UTC :: " + " | ".join(f"{k}={v}" for k,v in checks.items())
    print(msg)
    try:
        conn.execute("INSERT INTO ops_log(ts_utc, level, message) VALUES (strftime('%Y-%m-%d %H:%M:%S','now'), 'INFO', ?)", (msg,))
        conn.commit()
    except Exception:
        pass
