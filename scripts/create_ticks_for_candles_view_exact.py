import sqlite3

DB = r"C:\teevra18\data\teevra18.db"

VIEW_SQL = r"""
DROP VIEW IF EXISTS ticks_for_candles;
CREATE VIEW ticks_for_candles AS
SELECT
  CAST(security_id AS TEXT)                                AS instrument_id,
  CAST(strftime('%s', ts_utc || 'Z') AS INTEGER) * 1000    AS ts_event_ms,
  ltp                                                      AS price,
  COALESCE(last_qty, 1)                                    AS qty
FROM ticks_raw
WHERE ltp IS NOT NULL;
"""

def main():
    con = sqlite3.connect(DB, timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.executescript(VIEW_SQL)
    con.close()
    print("[OK] ticks_for_candles view created with columns: instrument_id, ts_event_ms, price, qty")

if __name__ == "__main__":
    main()
