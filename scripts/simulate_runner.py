import sqlite3, time, sys
from datetime import datetime

DB = r"C:\teevra18\data\teevra18.db"
RUNNER = sys.argv[1] if len(sys.argv) > 1 else "sim-runner"

def breaker_state():
    con = sqlite3.connect(DB); cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS breaker_state(
        state TEXT NOT NULL DEFAULT 'RUNNING',
        updated_at TEXT DEFAULT (datetime('now'))
    )""")
    con.commit()
    cur.execute("SELECT state FROM breaker_state LIMIT 1")
    row = cur.fetchone()
    con.close()
    return row[0] if row else "RUNNING"

def heartbeat(state, info=""):
    con = sqlite3.connect(DB); cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS runner_heartbeat(
        runner TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        info TEXT DEFAULT '',
        updated_at TEXT DEFAULT (datetime('now'))
    )""")
    cur.execute("""
        INSERT INTO runner_heartbeat(runner,state,info,updated_at)
        VALUES(?,?,?,datetime('now'))
        ON CONFLICT(runner) DO UPDATE SET
          state=excluded.state,
          info=excluded.info,
          updated_at=excluded.updated_at
    """, (RUNNER, state, info))
    con.commit(); con.close()

print(f"[{RUNNER}] starting… DB={DB}")
while True:
    st = breaker_state()
    if st == "PANIC":
        heartbeat("PANIC","exiting")
        print(f"[{RUNNER}] PANIC -> exiting at {datetime.now().strftime('%H:%M:%S')}")
        break
    elif st == "PAUSED":
        heartbeat("PAUSED","idling")
        time.sleep(1.0)
        continue
    else:
        # RUNNING: pretend to do work
        heartbeat("RUNNING","tick")
        time.sleep(0.5)
