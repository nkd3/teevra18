-- M8 V2 migration: add per-trade risk + charges flags
ALTER TABLE rr_profiles ADD COLUMN sl_cap_per_trade REAL DEFAULT 1500;
ALTER TABLE rr_profiles ADD COLUMN include_charges INTEGER DEFAULT 1;  -- 1=true
ALTER TABLE rr_profiles ADD COLUMN charges_broker TEXT DEFAULT 'ZERODHA'; -- 'ZERODHA'|'DHAN'
ALTER TABLE rr_profiles ADD COLUMN charges_overrides_json TEXT DEFAULT NULL;
