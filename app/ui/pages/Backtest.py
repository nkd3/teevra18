# -*- coding: utf-8 -*-
import sys
from pathlib import Path
import streamlit as st
import pandas as pd
import sqlite3

# Make app imports available if you want later (not required here)
APP_DIR = Path(__file__).resolve().parents[2]
if str(APP_DIR) not in sys.path: sys.path.append(str(APP_DIR))

DB_PATH = Path(r"C:\teevra18\data\teevra18.db")

st.set_page_config(page_title="Backtest • TeeVra18", page_icon="🧪", layout="wide")
user = st.session_state.get("user")
if not user:
    st.error("Please sign in.")
    st.stop()

st.title("Backtest")
st.caption("Run offline backtests; this page lists orders/results if present.")

if not DB_PATH.exists():
    st.error(f"Database not found at: {DB_PATH}")
    st.stop()

def table_exists(conn, name: str) -> bool:
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (name,))
        return cur.fetchone() is not None
    except Exception:
        return False

def get_columns(conn, table: str):
    try:
        cur = conn.execute(f"PRAGMA table_info({table});")
        return [r[1] for r in cur.fetchall()]
    except Exception:
        return []

with sqlite3.connect(DB_PATH) as conn:
    if not table_exists(conn, "backtest_orders"):
        st.info("Table `backtest_orders` does not exist yet. Create a Backtest run or write results to this table.")
        st.stop()

    cols = get_columns(conn, "backtest_orders")
    if not cols:
        st.info("`backtest_orders` has no columns yet.")
        st.stop()

    # Try to find a time-like column to order by
    time_candidates = ["ts", "time", "timestamp", "created_at"]
    tcol = next((c for c in time_candidates if c in cols), None)

    order_clause = f"ORDER BY {tcol} DESC" if tcol else "ORDER BY ROWID DESC"
    q = f"SELECT * FROM backtest_orders {order_clause} LIMIT 100"

    try:
        df = pd.read_sql_query(q, conn)
    except Exception as e:
        st.warning(f"No backtest data yet. ({e})")
        df = pd.DataFrame(columns=cols)

st.subheader("Recent Backtest Orders")
if df.empty:
    st.info("No rows in `backtest_orders` yet. Create a backtest run to populate results.")
else:
    st.dataframe(df, use_container_width=True, height=420)




