import sqlite3, os
from pathlib import Path
DB = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
with sqlite3.connect(DB) as conn:
    # delete old predictions (30d)
    conn.execute("DELETE FROM predictions_m11 WHERE created_at < datetime('now','-30 days');")
    # delete old signals (90d)
    conn.execute("DELETE FROM signals_m11 WHERE created_at < datetime('now','-90 days');")
    # delete old oos (90d)
    conn.execute("DELETE FROM pred_oos_log WHERE created_at < datetime('now','-90 days');")
    conn.commit()
    try:
        conn.execute("VACUUM;")
        conn.execute("ANALYZE;")
    except Exception:
        pass
print('[OK] Retention + vacuum done.')
