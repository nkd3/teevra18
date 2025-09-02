# -*- coding: utf-8 -*-
import sys
from pathlib import Path
import pandas as pd
import streamlit as st

from t18_common.db import get_conn, table_exists, columns, read_df  # (no first_existing import)
from t18_common.metrics import get_today_pl, get_open_risk, get_signal_chips
from t18_common.policy import get_active_policy_row
from ui_compat import show_image_auto, metric_row

# (optional) path bootstrap
APP_DIR = Path(__file__).resolve().parents[2]
UI_DIR  = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path: sys.path.append(str(APP_DIR))
if str(UI_DIR)  not in sys.path: sys.path.append(str(UI_DIR))

st.set_page_config(page_title="Trader • TeeVra18", page_icon="📈", layout="wide")

user = st.session_state.get("user")
if not user:
    st.error("Please sign in from the landing page.")
    st.stop()

with st.sidebar:
    st.subheader("Trader")
    st.caption(f"Logged in as: **{user.get('username','?')}**")
    symbol_filter = st.text_input("Filter by Symbol (optional)", "")

st.title("Trader Dashboard")

def first_existing(cols_list, candidates):
    """Return first item from candidates that exists in cols_list."""
    s = set(cols_list)
    for c in candidates:
        if c in s:
            return c
    return None

# --- Active Policy banner (read-only)
with get_conn() as conn:
    row, policy = get_active_policy_row(conn)

if row is not None and policy:
    st.success(
        f"**Active Policy:** {row.get('name','(unnamed)')}  •  "
        f"Max Trades/Day: {int(policy.get('max_trades_per_day',5))}  •  "
        f"SL ≤ ₹{int(policy.get('stoploss_rupees_per_lot',1000))}/lot  •  "
        f"Min R:R ≥ {float(policy.get('min_rr',2.0))}  •  "
        f"Daily Loss Cap: ₹{int(policy.get('daily_loss_limit_rupees',5000))}"
    )
else:
    st.info("No active policy found — set one in **Risk Policies** (Admin).")

# --- Top metrics
with get_conn() as conn:
    pl = get_today_pl(conn)
    risk = get_open_risk(conn)
    chips_raw = get_signal_chips(conn)

# normalize chips to dict(green/amber/red)
chips = {"green":0, "amber":0, "red":0}
if isinstance(chips_raw, dict):
    chips.update({k: int(v) for k,v in chips_raw.items() if k in chips})
elif isinstance(chips_raw, list):
    for r in chips_raw:
        s = str(r.get("signal","")).upper()
        if s in ("GREEN","BUY","LONG","BULL","UP"):
            chips["green"] += 1
        elif s in ("AMBER","NEUTRAL","HOLD","FLAT"):
            chips["amber"] += 1
        elif s in ("RED","SELL","SHORT","BEAR","DOWN"):
            chips["red"] += 1

chips_str = f"🟢 {chips['green']} · 🟠 {chips['amber']} · 🔴 {chips['red']}"

metric_row([
    {"label": "Today P/L", "value": ("₹ " + f"{pl:,.0f}") if isinstance(pl,(int,float)) else "n/a"},
    {"label": "Open Risk", "value": ("₹ " + f"{risk:,.0f}") if isinstance(risk,(int,float)) else "n/a"},
    {"label": "Summary", "value": chips_str},
])

st.divider()

# Tabs …
tab_overview, tab_orders, tab_signals, tab_health = st.tabs(["Overview", "Orders", "Signals", "Health & Ops"])

with tab_overview:
    with get_conn() as conn:
        if table_exists(conn, "signals"):
            sig_cols = columns(conn, "signals")
            base_sql = "SELECT * FROM signals"
            if "ts" in sig_cols:
                base_sql += " WHERE date(ts, 'localtime') = date('now','localtime')"
            order_sql = " ORDER BY ts DESC LIMIT 100" if "ts" in sig_cols else ""
            df_sig = read_df(conn, base_sql + order_sql)
            if not df_sig.empty and symbol_filter:
                for c in ["symbol","SYMBOL","underlying","UNDERLYING_SYMBOL","display_name","DISPLAY_NAME"]:
                    if c in df_sig.columns:
                        df_sig = df_sig[df_sig[c].astype(str).str.contains(symbol_filter, case=False, na=False)]
                        break
            st.subheader("Recent Signals")
            if df_sig.empty:
                st.info("No signals to show yet.")
            else:
                st.dataframe(df_sig, use_container_width=True, height=260)
        else:
            st.warning("Table `signals` not found.")

        st.markdown(" ")

        if table_exists(conn, "paper_orders"):
            po_cols = columns(conn, "paper_orders")
            st.subheader("Open Positions")
            status_col = first_existing(po_cols, ["status","state"])
            sql_open = "SELECT * FROM paper_orders"
            if status_col:
                sql_open += f" WHERE UPPER({status_col}) IN ('OPEN','PARTIAL','ACTIVE','RUNNING')"
            df_open = read_df(conn, sql_open + " ORDER BY ROWID DESC LIMIT 200")
            if not df_open.empty and symbol_filter:
                for c in ["symbol","SYMBOL","underlying","UNDERLYING_SYMBOL","display_name","DISPLAY_NAME"]:
                    if c in df_open.columns:
                        df_open = df_open[df_open[c].astype(str).str.contains(symbol_filter, case=False, na=False)]
                        break
            if df_open.empty:
                st.info("No open positions.")
            else:
                st.dataframe(df_open, use_container_width=True, height=240)
        else:
            st.warning("Table `paper_orders` not found.")

with tab_orders:
    with get_conn() as conn:
        if table_exists(conn, "paper_orders"):
            po_cols = columns(conn, "paper_orders")
            tcol = first_existing(po_cols, ["order_time","ts","created_at","time"])
            sql = "SELECT * FROM paper_orders"
            if tcol:
                sql += f" WHERE date({tcol}, 'localtime') = date('now','localtime') ORDER BY {tcol} DESC"
            df_orders = read_df(conn, sql)
            if not df_orders.empty and symbol_filter:
                for c in ["symbol","SYMBOL","underlying","UNDERLYING_SYMBOL","display_name","DISPLAY_NAME"]:
                    if c in df_orders.columns:
                        df_orders = df_orders[df_orders[c].astype(str).str.contains(symbol_filter, case=False, na=False)]
                        break
            st.subheader("Today’s Orders (Paper)")
            if df_orders.empty:
                st.info("No orders today.")
            else:
                st.dataframe(df_orders, use_container_width=True, height=400)
        else:
            st.warning("Table `paper_orders` not found.")

with tab_signals:
    with get_conn() as conn:
        if table_exists(conn, "signals"):
            sig_cols = columns(conn, "signals")
            sql = "SELECT * FROM signals"
            if "ts" in sig_cols:
                sql += " ORDER BY ts DESC"
            df_all = read_df(conn, sql + " LIMIT 300")
            if not df_all.empty and symbol_filter:
                for c in ["symbol","SYMBOL","underlying","UNDERLYING_SYMBOL","display_name","DISPLAY_NAME"]:
                    if c in df_all.columns:
                        df_all = df_all[df_all[c].astype(str).str.contains(symbol_filter, case=False, na=False)]
                        break
            st.subheader("All Signals (latest)")
            if df_all.empty:
                st.info("No signals yet.")
            else:
                st.dataframe(df_all, use_container_width=True, height=500)
        else:
            st.warning("Table `signals` not found.")

with tab_health:
    with get_conn() as conn:
        st.subheader("Health")
        if table_exists(conn, "health"):
            df_h = read_df(conn, "SELECT * FROM health ORDER BY ROWID DESC LIMIT 200")
            if df_h.empty:
                st.info("No health rows yet.")
            else:
                st.dataframe(df_h, use_container_width=True, height=220)
        else:
            st.info("No `health` table yet.")

        st.subheader("Ops Log")
        if table_exists(conn, "ops_log"):
            df_o = read_df(conn, "SELECT * FROM ops_log ORDER BY ROWID DESC LIMIT 200")
            if df_o.empty:
                st.info("No ops log rows yet.")
            else:
                st.dataframe(df_o, use_container_width=True, height=240)
        else:
            st.info("No `ops_log` table yet.")

