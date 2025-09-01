# -*- coding: utf-8 -*-
from pathlib import Path
import sqlite3, json, datetime

DB = Path(r"C:\teevra18\data\teevra18.db")
DB.parent.mkdir(parents=True, exist_ok=True)

DDL = [
"""
CREATE TABLE IF NOT EXISTS strategy_lab (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  notes TEXT,
  indicators_json TEXT NOT NULL,     -- JSON blob of indicators + params
  settings_json   TEXT NOT NULL,     -- JSON blob for strategy/global settings
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
""",
"""
CREATE TABLE IF NOT EXISTS policy_configs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  policy_json TEXT NOT NULL,         -- JSON: caps, rr, sl_per_lot, exposure, etc.
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
""",
"""
CREATE TABLE IF NOT EXISTS strategy_promotions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  lab_id INTEGER NOT NULL,
  target_env TEXT NOT NULL CHECK(target_env IN ('paper','live_ready')),
  promoted_by TEXT,
  promoted_at TEXT NOT NULL DEFAULT (datetime('now')),
  notes TEXT,
  FOREIGN KEY(lab_id) REFERENCES strategy_lab(id)
);
""",
"""
CREATE TABLE IF NOT EXISTS alerts_config (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL,             -- 'telegram' | 'eod'
  key TEXT NOT NULL,                 -- e.g. BOT_TOKEN, CHAT_ID, ENABLED
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""
]

with sqlite3.connect(DB) as conn:
    for stmt in DDL:
        conn.execute(stmt)
    conn.commit()

print("OK: M12 tables ensured:", DB)
