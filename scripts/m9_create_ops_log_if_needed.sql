CREATE TABLE IF NOT EXISTS ops_log (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_utc          TEXT    NOT NULL DEFAULT (datetime('now')),
  source          TEXT    NOT NULL,
  level           TEXT    NOT NULL,
  event           TEXT    NOT NULL,
  ref_table       TEXT,
  ref_id          INTEGER,
  message         TEXT
);
