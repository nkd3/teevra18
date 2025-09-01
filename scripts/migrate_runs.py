# -*- coding: utf-8 -*-
from pathlib import Path
import sqlite3

DB = Path(r"C:\teevra18\data\teevra18.db")
DB.parent.mkdir(parents=True, exist_ok=True)

DDL = """
CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL CHECK(kind IN ('backtest','paper','live')),
  status TEXT NOT NULL DEFAULT 'created',   -- created | running | stopped | done | failed
  lab_id INTEGER,                           -- FK to strategy_lab (nullable)
  params_json TEXT,                         -- JSON: symbols, dates, notes, etc.
  started_by TEXT,
  started_at TEXT DEFAULT (datetime('now')),
  ended_at TEXT,
  notes TEXT
);
"""

with sqlite3.connect(DB) as conn:
    conn.execute(DDL)
    conn.commit()

print("OK: runs table ensured.")
