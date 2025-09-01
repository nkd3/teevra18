DROP VIEW IF EXISTS v_signals_ready_for_m9;

CREATE VIEW v_signals_ready_for_m9 AS
SELECT
  id,
  ts_utc,
  security_id,
  group_name,
  strategy_id,
  side,
  entry, stop, target, rr,
  entry_price,
  sl_points, tp_points,
  lot_size, lots,
  option_symbol,
  underlying_root,
  signal_id,
  rr_validated,
  rr_metrics_json,
  state
FROM signals
WHERE
  state='PENDING'
  AND rr_validated=1
  AND signal_id IS NOT NULL
  AND option_symbol IS NOT NULL
  AND underlying_root IS NOT NULL
  AND entry_price IS NOT NULL
  AND sl_points IS NOT NULL
  AND tp_points IS NOT NULL
  AND lots IS NOT NULL
  AND rr_metrics_json IS NOT NULL;
