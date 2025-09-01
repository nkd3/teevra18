DROP VIEW IF EXISTS v_ops_log_with_id;

-- Expose rowid as id so queries like "ORDER BY id DESC" work everywhere.
CREATE VIEW v_ops_log_with_id AS
SELECT
  rowid AS id,
  ts_utc,
  level,
  area,
  msg,
  source,
  event,
  ref_table,
  ref_id,
  message
FROM ops_log;
