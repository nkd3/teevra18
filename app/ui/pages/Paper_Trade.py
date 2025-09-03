# -*- coding: utf-8 -*-
import sys, json
from pathlib import Path
import streamlit as st

from t18_common.db import get_conn, table_exists, read_df

# (optional) path bootstrap
APP_DIR = Path(__file__).resolve().parents[2]
UI_DIR  = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path: sys.path.append(str(APP_DIR))
if str(UI_DIR)  not in sys.path: sys.path.append(str(UI_DIR))

st.set_page_config(page_title="Paper Trade • TeeVra18", page_icon="📝", layout="wide")
user = st.session_state.get("user")
if not user:
    st.error("Please sign in.")
    st.stop()

st.title("Paper Trade")
st.caption("Auto paper execution (~7s latency).")

with get_conn() as conn:
    if not table_exists(conn, "strategy_lab"):
        st.error("No `strategy_lab` table. Create a Lab config first.")
        st.stop()

    df_lab = read_df(conn, "SELECT id, name, updated_at FROM strategy_lab ORDER BY updated_at DESC")
    lab_id = None
    if not df_lab.empty:
        display = [f"{row.id} — {row.name}" for row in df_lab.itertuples()]
        idx = st.selectbox("Lab Config", options=list(range(len(display))), format_func=lambda i: display[i])
        if idx is not None:
            lab_id = int(df_lab.iloc[idx]["id"])

    st.subheader("Session Control")
    start_disabled = (lab_id is None)

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Start Paper Session", disabled=start_disabled):
            params = {"latency_seconds": 7}
            conn.execute(
                """INSERT INTO runs (kind,status,lab_id,params_json,started_by,notes)
                   VALUES ('paper','running',?,?,?,'Paper session started')""",
                (lab_id, json.dumps(params), user.get("username","user"))
            )
            conn.commit()
            st.success("Paper session started (status=running).")
            st.rerun()
    with c2:
        if st.button("Stop Paper Session"):
            conn.execute(
                """UPDATE runs SET status='stopped', ended_at=datetime('now')
                   WHERE kind='paper' AND status='running'"""
            )
            conn.commit()
            st.warning("Paper session stopped.")
            st.rerun()

    st.markdown("---")
    st.subheader("Recent Paper Runs")
    df_runs = read_df(conn,
        "SELECT id, kind, status, lab_id, started_by, started_at, ended_at, notes "
        "FROM runs WHERE kind='paper' ORDER BY id DESC LIMIT 50"
    )
    st.dataframe(df_runs, use_container_width=True, height=360)


