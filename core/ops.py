# C:\teevra18\core\ops.py
"""
Ops helpers for Teevra18 runners:
- _ensure_tables(db): idempotently creates breaker + heartbeat tables
- breaker_state(db): returns 'RUNNING' | 'PAUSED' | 'PANIC'
- heartbeat(db, runner, state, info): upserts runner status each loop
- should_continue(db, runner, idle_sleep): obeys breaker; returns False on PANIC
"""

import sqlite3
import time

def _ensure_tables(db: str) -> None:
    """Create required tables if missing. Safe to call often."""
    con = sqlite3.connect(db); cur = con.cursor()
    # single-row breaker state
    cur.execute("""
        CREATE TABLE IF NOT EXISTS breaker_state(
            state TEXT NOT NULL DEFAULT 'RUNNING',
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    # audit (UI also writes here; runners usually don't)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS breaker_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            new_state TEXT NOT NULL,
            who TEXT DEFAULT 'runner',
            note TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    # per-runner heartbeat
    cur.execute("""
        CREATE TABLE IF NOT EXISTS runner_heartbeat(
            runner TEXT PRIMARY KEY,
            state  TEXT NOT NULL,
            info   TEXT DEFAULT '',
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    con.commit()
    con.close()

def breaker_state(db: str) -> str:
    """Read current breaker state; defaults to RUNNING if table empty."""
    _ensure_tables(db)
    con = sqlite3.connect(db); cur = con.cursor()
    cur.execute("SELECT state FROM breaker_state LIMIT 1")
    row = cur.fetchone()
    con.close()
    return row[0] if row else "RUNNING"

def heartbeat(db: str, runner: str, state: str, info: str = "") -> None:
    """Upsert a heartbeat row for this runner with current state/info."""
    _ensure_tables(db)
    con = sqlite3.connect(db); cur = con.cursor()
    cur.execute(
        """
        INSERT INTO runner_heartbeat(runner,state,info,updated_at)
        VALUES(?,?,?,datetime('now'))
        ON CONFLICT(runner) DO UPDATE SET
          state=excluded.state,
          info=excluded.info,
          updated_at=excluded.updated_at
        """,
        (runner, state, info),
    )
    con.commit()
    con.close()

def should_continue(db: str, runner: str, idle_sleep: float = 1.0) -> bool:
    """
    Call once per loop iteration.

    Returns:
      - False  -> breaker = PANIC (caller should exit loop)
      - True   -> breaker = RUNNING (do work) OR PAUSED (idle + continue)

    Side effects:
      - Writes a heartbeat row each call.
      - Sleeps idle_sleep seconds when PAUSED.
    """
    st = breaker_state(db)
    if st == "PANIC":
        heartbeat(db, runner, "PANIC", "exiting")
        return False
    elif st == "PAUSED":
        heartbeat(db, runner, "PAUSED", "idling")
        time.sleep(idle_sleep)
        return True
    else:
        # RUNNING
        heartbeat(db, runner, "RUNNING", "tick")
        return True
