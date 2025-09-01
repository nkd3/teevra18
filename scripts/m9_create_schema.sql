-- Paper orders lifecycle
-- States: PENDING_DELAY -> FILLED -> (TP_HIT | SL_HIT | EXPIRED) -> CLOSED

CREATE TABLE IF NOT EXISTS paper_orders (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  signal_row_id     INTEGER NOT NULL,            -- FK to signals.id
  signal_id         TEXT    NOT NULL,            -- human key
  option_symbol     TEXT    NOT NULL,
  underlying_root   TEXT    NOT NULL,
  side              TEXT    NOT NULL,            -- LONG only for now
  lots              INTEGER NOT NULL,
  qty               INTEGER NOT NULL,            -- lots * lot_size
  lot_size          INTEGER NOT NULL,
  state             TEXT    NOT NULL,            -- PENDING_DELAY/FILLED/TP_HIT/SL_HIT/EXPIRED/CLOSED

  -- prices
  entry_price       REAL    NOT NULL,            -- option premium at decision time
  fill_price        REAL,                        -- premium at +7s (or best available)
  sl_price          REAL    NOT NULL,            -- entry - sl_points (long)
  tp_price          REAL    NOT NULL,            -- entry + tp_points (long)
  exit_price        REAL,                        -- price at close

  -- timestamps
  created_ts_utc    TEXT    NOT NULL DEFAULT (datetime('now')),
  delayed_fill_at   TEXT,                        -- scheduled fill ts (signal ts + 7s)
  filled_ts_utc     TEXT,
  closed_ts_utc     TEXT,

  -- P&L
  charges_at_fill   REAL DEFAULT 0,              -- roundtrip est. at stop; informative
  charges_at_exit   REAL DEFAULT 0,
  pnl_gross         REAL,
  pnl_net           REAL,

  -- debug/audit
  rr_metrics_json   TEXT,
  extra_json        TEXT
);

CREATE INDEX IF NOT EXISTS idx_paper_orders_signal ON paper_orders(signal_row_id);
CREATE INDEX IF NOT EXISTS idx_paper_orders_state  ON paper_orders(state);

-- Simple ops log
CREATE TABLE IF NOT EXISTS ops_log (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_utc          TEXT    NOT NULL DEFAULT (datetime('now')),
  source          TEXT    NOT NULL,               -- 'M9'
  level           TEXT    NOT NULL,               -- INFO/WARN/ERR
  event           TEXT    NOT NULL,               -- e.g., 'CREATE','FILL','SL_HIT','TP_HIT','CLOSE'
  ref_table       TEXT,
  ref_id          INTEGER,
  message         TEXT
);
