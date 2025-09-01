PRAGMA foreign_keys=OFF;

CREATE TABLE IF NOT EXISTS predictions_m11_new AS
SELECT
  ROW_NUMBER() OVER ()        AS id,
  instrument,
  ts_utc,
  prob_up,
  exp_move_abs,
  created_at
FROM predictions_m11;

DROP TABLE predictions_m11;
ALTER TABLE predictions_m11_new RENAME TO predictions_m11;
CREATE UNIQUE INDEX IF NOT EXISTS ux_predictions_instr_ts ON predictions_m11(instrument, ts_utc);
