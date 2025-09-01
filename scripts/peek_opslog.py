import os, sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv('DB_PATH', r'C:\teevra18\data\teevra18.db'))

with sqlite3.connect(DB_PATH) as con:
    cur = con.cursor()
    # Check table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ops_log'")
    if not cur.fetchone():
        print(f'ops_log table not found in {DB_PATH}')
        raise SystemExit(0)

    rows = list(cur.execute(
        "SELECT ts_utc, component, status, rows, substr(warns,1,120) "
        "FROM ops_log "
        "WHERE component IS NOT NULL OR status IS NOT NULL "
        "ORDER BY rowid DESC LIMIT 10"
    ))
    if rows:
        for r in rows:
            print(r)
    else:
        print('No ops_log rows yet with component/status set.')
