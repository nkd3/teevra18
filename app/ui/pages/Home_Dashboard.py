# -*- coding: utf-8 -*-
# C:\teevra18\app\ui\pages\Home_Dashboard.py

import streamlit as st
st.set_page_config(
    page_title="Teevra18 | Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------- after set_page_config is OK ----------------
from pathlib import Path
import sys

# optional path bootstrap (no st.* here)
THIS_FILE = Path(__file__).resolve()
APP_DIR   = THIS_FILE.parents[2]   # C:\teevra18\app
UI_DIR    = THIS_FILE.parents[1]   # C:\teevra18\app\ui
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
if str(UI_DIR) not in sys.path:
    sys.path.insert(0, str(UI_DIR))

from ui.components.shell import render_topbar, render_subnav_choice, fetch_kpis, fetch_orders_df, fetch_signals_df, fetch_positions_df

# ---- Top bar (must be after set_page_config) ----
render_topbar(active_primary="DASHBOARD")

# ---- Page body (example, keep your original content here) ----
st.title("Dashboard")
sub = render_subnav_choice(default="Overview")

if sub == "Overview":
    k = fetch_kpis()
    c1, c2, c3 = st.columns(3)
    with c1: st.metric("P/L Today", k.get("pl_today", "₹0"))
    with c2: st.metric("Open Risk", k.get("open_risk", "₹0"))
    with c3: st.metric("Signals Today", k.get("signals_today", "0"))

    st.divider()
    st.write("Welcome to the dashboard. Add your Overview widgets here.")

elif sub == "Orders":
    st.subheader("Recent Orders")
    df = fetch_orders_df(50)
    st.dataframe(df) if df is not None else st.info("No orders yet.")

elif sub == "Signals":
    st.subheader("Recent Signals")
    df = fetch_signals_df(50)
    st.dataframe(df) if df is not None else st.info("No signals yet.")

elif sub == "Health and Ops":
    st.subheader("Ops & Health")
    st.write("Place health widgets or logs here.")
