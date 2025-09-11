# -*- coding: utf-8 -*-
from t18_common.db import get_conn, table_exists, columns, read_df, first_existing
import sys
from pathlib import Path
import streamlit as st
import pandas as pd

APP_DIR = Path(__file__).resolve().parents[2]   # ...\app
UI_DIR  = Path(__file__).resolve().parents[1]   # ...\app\ui
if str(APP_DIR) not in sys.path: sys.path.append(str(APP_DIR))
if str(UI_DIR)  not in sys.path: sys.path.append(str(UI_DIR))

from t18_common.db import get_conn, table_exists, columns, read_df
from ui_compat import show_image_auto, metric_row

st.set_page_config(page_title="Admin • TeeVra18", page_icon="🛡️", layout="wide")

user = st.session_state.get("user")
if not user or user.get("role") != "admin":
    st.error("Access denied. Admins only.")
    st.stop()

with st.sidebar:
    st.subheader("Admin")
    st.caption(f"Logged in as: **{user.get('username','?')}**")

st.title("Admin Dashboard")

tab_users, tab_configs, tab_health = st.tabs(["Users", "Strategy Configs", "Health & Ops"])

with tab_users:
    with get_conn() as conn:
        if table_exists(conn, "users"):
            df_u = read_df(conn, "SELECT id, username, role, is_active, created_at FROM users ORDER BY id ASC")
            st.subheader("User Accounts")
            st.dataframe(df_u, use_container_width=True, height=360)
        else:
            st.info("No `users` table found. (Create via scripts/add_user.py.)")

with tab_configs:
    st.subheader("Strategy Configs (read-only preview)")
    st.caption("Admin can promote configs from Strategy Lab later. For now, this is a read-only view if tables exist.")
    with get_conn() as conn:
        shown = False
        for t in ["strategy_configs","lab_configs","policy_configs","strategy_lab"]:
            if table_exists(conn, t):
                df = read_df(conn, f"SELECT * FROM {t} ORDER BY ROWID DESC LIMIT 200")
                st.write(f"**{t}**")
                st.dataframe(df, use_container_width=True, height=320)
                shown = True
        if not shown:
            st.info("No known config tables yet. Use your M12 Strategy Lab later to create them.")

with tab_health:
    with get_conn() as conn:
        st.subheader("Health")
        if table_exists(conn, "health"):
            df_h = read_df(conn, "SELECT * FROM health ORDER BY ROWID DESC LIMIT 200")
            st.dataframe(df_h, use_container_width=True, height=220)
        else:
            st.info("No `health` data yet.")

        st.subheader("Ops Log")
        if table_exists(conn, "ops_log"):
            df_o = read_df(conn, "SELECT * FROM ops_log ORDER BY ROWID DESC LIMIT 200")
            st.dataframe(df_o, use_container_width=True, height=240)
        else:
            st.info("No `ops_log` yet.")
