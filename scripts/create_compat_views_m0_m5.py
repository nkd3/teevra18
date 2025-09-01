# -*- coding: utf-8 -*-
"""
create_compat_views_m0_m5.py — SMART VIEWS (Your Schema)
- Builds stable views the checker can rely on, using your actual columns.
- Handles your K/V health table, ticks/quotes with security_id fallback, candles t_start, etc.
- Prints the final SQL used so it's transparent.

Views created:
  health_view(updated_at, breaker_state)
  ticks_raw_view(ts, symbol)
  quote_snap_view(ts, symbol)
  depth20_snap_view(ts, bid_qty_total, ask_qty_total, imbalance)
  candles_1m_view(ts_close, open, high, low, close)
  option_chain_snap_view(ts, iv, oi, delta)
"""

import os, sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

def exists(conn, name):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name=?;", (name,))
    return bool(cur.fetchone())

def cols(conn, table):
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()]

def run(conn, sql):
    conn.execute(sql)
    print("OK:", sql.replace("\n", " "))

def main():
    conn = sqlite3.connect(DB_PATH.as_posix())
    conn.execute("PRAGMA journal_mode=WAL;")

    # -------- M0: health (key/value/ts_utc) -> health_view(updated_at, breaker_state)
    if exists(conn, "health"):
        # updated_at = latest ts_utc
        # breaker_state = last state from keys (breaker_state/m0_breaker/m1_breaker), else 'RUNNING'
        conn.execute("DROP VIEW IF EXISTS health_view;")
        sql = r"""
        CREATE VIEW health_view AS
        WITH latest AS (
          SELECT MAX(ts_utc) AS updated_at FROM health
        ),
        last_state AS (
          SELECT h.value AS breaker_state_val
          FROM health h
          WHERE h.key IN ('breaker_state','m0_breaker','m1_breaker')
          ORDER BY h.ts_utc DESC
          LIMIT 1
        )
        SELECT
          latest.updated_at AS updated_at,
          COALESCE((SELECT breaker_state_val FROM last_state), 'RUNNING') AS breaker_state
        FROM latest;
        """
        run(conn, sql.strip())
    else:
        print("SKIP: health table not found")

    # -------- M1: ticks_raw -> ticks_raw_view(ts, symbol)
    if exists(conn, "ticks_raw"):
        # ts = ts_utc; symbol fallback = CAST(security_id AS TEXT)
        conn.execute("DROP VIEW IF EXISTS ticks_raw_view;")
        # prefer a human symbol column if it exists
        c = set(cols(conn, "ticks_raw"))
        symbol_expr = "CAST(security_id AS TEXT)"
        for name in ("symbol","tradingsymbol","scrip","instrument"):
            if name in c:
                symbol_expr = name
                break
        sql = f"""
        CREATE VIEW ticks_raw_view AS
        SELECT
          ts_utc AS ts,
          {symbol_expr} AS symbol
        FROM ticks_raw;
        """
        run(conn, sql.strip())
    else:
        print("SKIP: ticks_raw table not found")

    # -------- M2: quote_snap -> quote_snap_view(ts, symbol)
    # Table name confirmed: quote_snap (from your output)
    if exists(conn, "quote_snap"):
        conn.execute("DROP VIEW IF EXISTS quote_snap_view;")
        cq = set(cols(conn, "quote_snap"))
        symbol_expr = "CAST(security_id AS TEXT)"
        for name in ("symbol","tradingsymbol","scrip","instrument"):
            if name in cq:
                symbol_expr = name
                break
        sql = f"""
        CREATE VIEW quote_snap_view AS
        SELECT
          ts_utc AS ts,
          {symbol_expr} AS symbol
        FROM quote_snap;
        """
        run(conn, sql.strip())
    else:
        print("SKIP: quote_snap table not found")

    # -------- M3: depth20_snap -> depth20_snap_view(ts, bid_qty_total, ask_qty_total, imbalance)
    if exists(conn, "depth20_snap"):
        conn.execute("DROP VIEW IF EXISTS depth20_snap_view;")
        c = set(cols(conn, "depth20_snap"))
        # We don't parse JSON here; expose placeholders if totals not present
        bid_expr = "NULL"
        ask_expr = "NULL"
        if "bid_sz" in c: bid_expr = "bid_sz"
        if "ask_sz" in c: ask_expr = "ask_sz"
        # imbalance simple proxy if both sizes present
        imb_expr = "NULL" if (bid_expr=="NULL" or ask_expr=="NULL") else f"CASE WHEN ({bid_expr}+{ask_expr})>0 THEN (CAST({bid_expr} AS REAL)-CAST({ask_expr} AS REAL))/(CAST({bid_expr} AS REAL)+CAST({ask_expr} AS REAL)) ELSE 0 END"
        sql = f"""
        CREATE VIEW depth20_snap_view AS
        SELECT
          ts,
          {bid_expr} AS bid_qty_total,
          {ask_expr} AS ask_qty_total,
          {imb_expr} AS imbalance
        FROM depth20_snap;
        """
        run(conn, sql.strip())
    else:
        print("SKIP: depth20_snap table not found")

    # -------- M4: candles_1m -> candles_1m_view(ts_close, open, high, low, close)
    if exists(conn, "candles_1m"):
        # Your columns: instrument_id, t_start (epoch seconds), open, high, low, close, ...
        # We treat bar close time = t_start + 60 seconds.
        conn.execute("DROP VIEW IF EXISTS candles_1m_view;")
        c = set(cols(conn, "candles_1m"))
        if "t_start" in c:
            ts_close_expr = "t_start + 60"  # epoch seconds of close
        else:
            # As a fallback, if someone renamed, we try 'ts_utc' or 'ts'
            ts_close_expr = "ts_utc"
            if "ts" in c: ts_close_expr = "ts"
        sql = f"""
        CREATE VIEW candles_1m_view AS
        SELECT
          {ts_close_expr} AS ts_close,
          open, high, low, close
        FROM candles_1m;
        """
        run(conn, sql.strip())
    else:
        print("SKIP: candles_1m table not found")

    # -------- M5: option_chain_snap -> option_chain_snap_view(ts, iv, oi, delta)
    if exists(conn, "option_chain_snap"):
        conn.execute("DROP VIEW IF EXISTS option_chain_snap_view;")
        c = set(cols(conn, "option_chain_snap"))
        iv_expr = "implied_volatility" if "implied_volatility" in c else "iv"
        sql = f"""
        CREATE VIEW option_chain_snap_view AS
        SELECT
          ts,
          {iv_expr} AS iv,
          oi,
          delta
        FROM option_chain_snap;
        """
        run(conn, sql.strip())
    else:
        print("SKIP: option_chain_snap table not found")

    conn.commit()
    conn.close()
    print("Compatibility views created/refreshed successfully.")

if __name__ == "__main__":
    main()
