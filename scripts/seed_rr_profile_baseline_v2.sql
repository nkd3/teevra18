-- Seed/Upsert BASELINE_V2 with BOTH per-lot (legacy) and per-trade caps.
INSERT INTO rr_profiles (
  profile_name,
  rr_min,
  sl_cap_per_lot,         -- <-- REQUIRED by your table (NOT NULL)
  sl_cap_per_trade,       -- <-- new per-trade cap (incl. charges)
  include_charges,
  charges_broker,
  sl_method,
  tp_method,
  tp_factor,
  spread_buffer_ticks,
  min_liquidity_lots,
  charges_overrides_json
)
VALUES (
  'BASELINE_V2',
  2.0,
  1000,                   -- legacy per-lot cap (kept for backward compatibility)
  1500,                   -- per-trade cap (incl. charges) used by M8 v2
  1,                      -- include charges
  'ZERODHA',              -- default broker tag (you can switch to 'DHAN')
  'ATR',
  'RR',
  2.0,
  1.0,
  1,
  '{"NIFTY":{"brokerage_per_order":20,"gst_rate":0.18,"stt_sell_rate":0.001,"exch_txn_rate":0.0003503,"sebi_rate":0.000001,"stamp_buy_rate":0.00003},"BANKNIFTY":{"brokerage_per_order":20,"gst_rate":0.18,"stt_sell_rate":0.001,"exch_txn_rate":0.0003503,"sebi_rate":0.000001,"stamp_buy_rate":0.00003}}'
)
ON CONFLICT(profile_name) DO UPDATE SET
  rr_min=excluded.rr_min,
  sl_cap_per_lot=excluded.sl_cap_per_lot,
  sl_cap_per_trade=excluded.sl_cap_per_trade,
  include_charges=excluded.include_charges,
  charges_broker=excluded.charges_broker,
  sl_method=excluded.sl_method,
  tp_method=excluded.tp_method,
  tp_factor=excluded.tp_factor,
  spread_buffer_ticks=excluded.spread_buffer_ticks,
  min_liquidity_lots=excluded.min_liquidity_lots,
  charges_overrides_json=excluded.charges_overrides_json;
