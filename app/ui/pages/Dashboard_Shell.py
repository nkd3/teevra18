# -*- coding: utf-8 -*-
import streamlit as st
from components.shell import (
    render_topbar, render_subnav_choice,
    fetch_orders_df, fetch_signals_df, fetch_positions_df, fetch_kpis
)

st.set_page_config(page_title="Teevra18 | Dashboard", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")

# Auth guard
if "auth_user" not in st.session_state and "t18_auth_user" not in st.session_state:
    st.error("You are not logged in. Please return to Login."); st.stop()
if "auth_user" not in st.session_state and "t18_auth_user" in st.session_state:
    st.session_state["auth_user"] = {"name": st.session_state["t18_auth_user"]}

# Header
render_topbar(active_primary="DASHBOARD")

# Title
st.markdown("### TRADER DASHBOARD")

# Sub-nav (radio → same page)
active_sub = render_subnav_choice(default="Overview")

# KPI bubbles
k = fetch_kpis()
st.markdown('<div class="t18-section"><div class="t18-kpi-grid">', unsafe_allow_html=True)
cols = st.columns(6, gap="medium")
items = [
    ("Today P/L", k["pl_today"]),
    ("Open Risk", k["open_risk"]),
    ("Net Positions", k["net_positions"]),
    ("Signals (Today)", k["signals_today"]),
    ("Hit Rate (7d)", k["hit_rate_7d"]),
    ("Max DD (30d)", k["max_dd_30d"]),
]
for i,(t,v) in enumerate(items):
    with cols[i]:
        st.markdown(f"<div class='t18-kpi'><h5>{t}</h5><div class='v'>{v}</div></div>", unsafe_allow_html=True)
st.markdown('</div></div>', unsafe_allow_html=True)

# Workspaces (70:30) + Full width
st.markdown('<div class="t18-section t18-row-7030">', unsafe_allow_html=True)
left, right = st.columns([7,3], gap="large")

with left:
    if active_sub == "Overview":
        st.markdown("<div class='t18-card t18-wsA'><b>Market Overview</b>", unsafe_allow_html=True)
        df = fetch_positions_df()
        if df is not None and not df.empty: st.dataframe(df, use_container_width=True, hide_index=True)
        else: st.info("No positions data available.")
        st.markdown("</div>", unsafe_allow_html=True)

    elif active_sub == "Orders":
        st.markdown("<div class='t18-card t18-wsA'><b>Recent Orders</b>", unsafe_allow_html=True)
        odf = fetch_orders_df(50)
        if odf is not None and not odf.empty: st.dataframe(odf, use_container_width=True, hide_index=True)
        else: st.info("No orders found.")
        st.markdown("</div>", unsafe_allow_html=True)

    elif active_sub == "Signals":
        st.markdown("<div class='t18-card t18-wsA'><b>Signals Stream</b>", unsafe_allow_html=True)
        sdf = fetch_signals_df(50)
        if sdf is not None and not sdf.empty: st.dataframe(sdf, use_container_width=True, hide_index=True)
        else: st.info("No signals yet.")
        st.markdown("</div>", unsafe_allow_html=True)

    else:  # Health and Ops
        st.markdown("<div class='t18-card t18-wsA'><b>Health & Ops</b>", unsafe_allow_html=True)
        st.caption("Show heartbeats, runner states, token expiry, last error, etc. (hook to ops_state table).")
        st.markdown("</div>", unsafe_allow_html=True)

with right:
    st.markdown("<div class='t18-card t18-wsB'><b>Activity</b>", unsafe_allow_html=True)
    if active_sub in ("Overview","Signals"):
        sdf = fetch_signals_df(15); st.write("Recent Signals:")
        if sdf is not None and not sdf.empty: st.dataframe(sdf, use_container_width=True, hide_index=True)
        else: st.caption("—")
    elif active_sub == "Orders":
        odf = fetch_orders_df(15); st.write("Recent Orders:")
        if odf is not None and not odf.empty: st.dataframe(odf, use_container_width=True, hide_index=True)
        else: st.caption("—")
    else:
        st.write("System Notices:"); st.caption("No notices.")
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("</div>", unsafe_allow_html=True)  # /row

st.markdown("<div class='t18-section'><div class='t18-card t18-wsFull'><b>Positions & Risk (Full Width)</b>", unsafe_allow_html=True)
pdf = fetch_positions_df()
if pdf is not None and not pdf.empty: st.dataframe(pdf, use_container_width=True, hide_index=True)
else: st.info("No open positions to show.")
st.markdown("</div></div>", unsafe_allow_html=True)
