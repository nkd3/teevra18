# C:\teevra18\services\m11\latency_check_m11.py
import time, sqlite3, os
from pathlib import Path
from datetime import datetime, timezone
import subprocess, sys

DB = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

def run(cmd):
    t0 = time.perf_counter()
    p = subprocess.run([sys.executable, cmd], capture_output=True, text=True)
    dt_ms = (time.perf_counter() - t0)*1000.0
    return dt_ms, p.returncode, p.stdout.strip(), p.stderr.strip()

def log_ops(conn, msg):
    try:
        conn.execute("INSERT INTO ops_log(ts_utc, level, message) VALUES (strftime('%Y-%m-%d %H:%M:%S','now'), 'INFO', ?)", (msg,))
        conn.commit()
    except Exception:
        pass

with sqlite3.connect(DB) as conn:
    t_infer, rc1, _, _ = run(r"C:\teevra18\services\m11\infer_m11.py")
    t_gate,  rc2, _, _ = run(r"C:\teevra18\services\m11\gate_alerts_m11.py")
    total = t_infer + t_gate
    summary = f"[LATENCY] infer={t_infer:.1f}ms gate={t_gate:.1f}ms total={total:.1f}ms (p99 goal ≤100ms)"
    print(summary)
    log_ops(conn, summary)
