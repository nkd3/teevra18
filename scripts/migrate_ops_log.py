import sqlite3, os
from pathlib import Path
DB = Path(os.getenv('DB_PATH', r'C:\teevra18\data\teevra18.db'))
with sqlite3.connect(DB) as c:
    c.executescript('''
    CREATE TABLE IF NOT EXISTS ops_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts_utc TEXT NOT NULL,
      component TEXT NOT NULL,
      status TEXT NOT NULL,
      rows INTEGER DEFAULT 0,
      warns TEXT,
      extra TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_opslog_ts ON ops_log(ts_utc);
    ''')
print('OK: ops_log ready at', DB)
