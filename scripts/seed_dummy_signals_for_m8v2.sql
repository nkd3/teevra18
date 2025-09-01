-- Dummy signals for M8 v2 validation that satisfy NOT NULL + CHECK(side IN ('LONG','SHORT'))
-- UNIQUE-ish identifiers so you can re-run by changing suffixes if needed.

-- NIFTY TEST — may REJECT on rr_eff or risk>1500 after charges
INSERT INTO signals (
  ts_utc,
  security_id,
  group_name,
  strategy_id,
  side,              -- MUST be LONG/SHORT per your table constraint
  entry,
  stop,
  target,
  rr,
  sl_per_lot,
  version,
  deterministic_hash,
  run_id,
  -- helper fields used by M8 v2 (all nullable in your schema)
  signal_id,
  option_symbol,
  underlying_root,
  entry_price,
  sl_points,
  tp_points,
  lot_size,
  lots
)
VALUES (
  datetime('now'),
  '999001',
  'TESTS',
  'ema_vwap_atr',
  'LONG',            -- <-- FIXED
  120.0,
  100.0,
  165.0,
  2.25,
  1500.0,           -- 20 pts * 75 (NIFTY lot)
  '1.0',
  'TESTS_NIFTY_1',
  'TESTS_RUN_1',

  'TST-NIFTY-1',
  'NIFTY24SEP24500CE',
  'NIFTY',
  120.0,
  20.0,
  45.0,
  75,
  1
);

-- BANKNIFTY TEST — tune tp_points if rr_eff<2 after charges
INSERT INTO signals (
  ts_utc,
  security_id,
  group_name,
  strategy_id,
  side,
  entry,
  stop,
  target,
  rr,
  sl_per_lot,
  version,
  deterministic_hash,
  run_id,
  signal_id,
  option_symbol,
  underlying_root,
  entry_price,
  sl_points,
  tp_points,
  lot_size,
  lots
)
VALUES (
  datetime('now'),
  '999002',
  'TESTS',
  'ema_vwap_atr',
  'LONG',            -- <-- FIXED
  300.0,
  260.0,
  390.0,
  2.25,
  1400.0,            -- 40 pts * 35 (BANKNIFTY lot)
  '1.0',
  'TESTS_BN_1',
  'TESTS_RUN_1',

  'TST-BN-1',
  'BANKNIFTY24SEP49500PE',
  'BANKNIFTY',
  300.0,
  40.0,
  90.0,              -- raise to 90 to help rr_eff ≥ 2 after charges
  35,
  1
);
