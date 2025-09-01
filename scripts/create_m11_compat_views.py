# C:\teevra18\scripts\create_m11_compat_views.py
import sqlite3, os

DB = r"C:\teevra18\data\ops.db"

VIEW_SQL = {
"candles_1m_view": """
CREATE VIEW IF NOT EXISTS candles_1m_view AS
SELECT
  ts_utc,
  COALESCE(instrument, symbol) AS instrument,
  close,
  COALESCE(ema, ema_20, ema21, ema_fast) AS ema,
  COALESCE(vwap, vwap_price) AS vwap,
  COALESCE(atr, atr_14, atr14) AS atr
FROM candles_1m;
""",
"depth20_snap_view": """
CREATE VIEW IF NOT EXISTS depth20_snap_view AS
SELECT
  ts_utc,
  COALESCE(instrument, symbol) AS instrument,
  COALESCE(l20_imbalance, imbalance, book_imbalance) AS l20_imbalance
FROM depth20_snap;
""",
"option_chain_view": """
CREATE VIEW IF NOT EXISTS option_chain_view AS
SELECT
  ts_utc,
  COALESCE(instrument, underlying, symbol) AS instrument,
  NULL AS iv_move
FROM option_chain;
""",
"key_levels_view": """
CREATE VIEW IF NOT EXISTS key_levels_view AS
SELECT
  ts_utc,
  COALESCE(instrument, symbol) AS instrument,
  cpr, pdh, pdl, pivot, r1, s1, round_level
FROM key_levels;
"""
}

def table_exists(cur, name):
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return cur.fetchone() is not None

def view_exists(cur, name):
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='view' AND name=?;", (name,))
    return cur.fetchone() is not None

def safe_create_view(cur, name, sql):
    cur.execute(f"SELECT sql FROM sqlite_master WHERE type='view' AND name=?;", (name,))
    row = cur.fetchone()
    if row is None:
        cur.execute(sql)

def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # Create candles view only if base table exists
    if table_exists(cur, "candles_1m"):
        safe_create_view(cur, "candles_1m_view", VIEW_SQL["candles_1m_view"])

    if table_exists(cur, "depth20_snap"):
        safe_create_view(cur, "depth20_snap_view", VIEW_SQL["depth20_snap_view"])

    if table_exists(cur, "option_chain"):
        safe_create_view(cur, "option_chain_view", VIEW_SQL["option_chain_view"])

    if table_exists(cur, "key_levels"):
        safe_create_view(cur, "key_levels_view", VIEW_SQL["key_levels_view"])

    conn.commit()
    conn.close()
    print("[OK] M11 compatibility views ensured (where base tables exist).")

if __name__ == "__main__":
    main()
