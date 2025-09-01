# -*- coding: utf-8 -*-
from datetime import datetime
import pandas as pd
from .db import table_exists, columns, read_df, first_existing

def get_today_pl(conn):
    if not table_exists(conn, "paper_orders"):
        return None
    cols = columns(conn, "paper_orders")
    pnl_col = first_existing(cols, ["realized_pnl","pnl_realized","pnl","pl"])
    time_col = first_existing(cols, ["order_time","ts","created_at","time"])
    if not pnl_col:
        return None
    if time_col:
        df = read_df(conn, f"""
            SELECT {pnl_col} AS pnl, date({time_col}) AS d
            FROM paper_orders
            WHERE date({time_col}, 'localtime') = date('now','localtime')
        """)
    else:
        df = read_df(conn, f"SELECT {pnl_col} AS pnl FROM paper_orders")
    if df.empty:
        return 0.0
    return float(df["pnl"].fillna(0).sum())

def get_open_risk(conn):
    if not table_exists(conn, "paper_orders"):
        return None
    cols = columns(conn, "paper_orders")
    status_col = first_existing(cols, ["status","state"])
    qty_col    = first_existing(cols, ["qty","quantity","filled_qty"])
    entry_col  = first_existing(cols, ["avg_price","entry_price","price"])
    sl_col     = first_existing(cols, ["stop","sl","stop_price","stoploss"])
    if "open_risk" in cols:
        df = read_df(conn, "SELECT open_risk FROM paper_orders WHERE open_risk IS NOT NULL")
        return float(df["open_risk"].sum()) if not df.empty else 0.0
    if not (status_col and qty_col and entry_col and sl_col):
        return None
    df = read_df(conn, f"""
        SELECT {qty_col} AS qty, {entry_col} AS entry, {sl_col} AS sl, {status_col} AS st
        FROM paper_orders
        WHERE UPPER({status_col}) IN ('OPEN','PARTIAL','ACTIVE','RUNNING')
    """)
    if df.empty:
        return 0.0
    df = df.dropna(subset=["qty","entry","sl"])
    df["risk"] = (df["qty"].astype(float).abs() * (df["entry"].astype(float) - df["sl"].astype(float)).abs())
    return float(df["risk"].sum())

def get_signal_chips(conn):
    if not table_exists(conn, "signals"):
        return {"green": 0, "amber": 0, "red": 0}
    cols = columns(conn, "signals")
    label_col = first_existing(cols, ["label","status","state","signal","class"])
    if not label_col:
        return {"green": 0, "amber": 0, "red": 0}
    df = read_df(conn, f"SELECT {label_col} AS L FROM signals WHERE date(ts, 'localtime') = date('now', 'localtime')") \
         if "ts" in cols else read_df(conn, f"SELECT {label_col} AS L FROM signals")
    if df.empty:
        return {"green": 0, "amber": 0, "red": 0}
    def bucket(x:str):
        if x is None: return "amber"
        s = str(x).strip().lower()
        if s in ("buy","long","bull","green","g","pos","positive","strong_buy"): return "green"
        if s in ("sell","short","bear","red","r","neg","negative","strong_sell"): return "red"
        if s in ("flat","neutral","hold","amber","orange","a"): return "amber"
        return "amber"
    cts = df["L"].map(bucket).value_counts().to_dict()
    return {"green": int(cts.get("green",0)), "amber": int(cts.get("amber",0)), "red": int(cts.get("red",0))}
