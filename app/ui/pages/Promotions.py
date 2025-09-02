# -*- coding: utf-8 -*-
from t18_common.db import get_conn, table_exists, columns, read_df, first_existing
import sys
from pathlib import Path
import streamlit as st

APP_DIR = Path(__file__).resolve().parents[2]
UI_DIR  = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path: sys.path.append(str(APP_DIR))
if str(UI_DIR)  not in sys.path: sys.path.append(str(UI_DIR))

from t18_common.db import get_conn, table_exists, read_df

st.set_page_config(page_title="Promotions • TeeVra18", page_icon="🚀", layout="wide")

user = st.session_state.get("user")
if not user or user.get("role") != "admin":
    st.error("Access denied. Admins only.")
    st.stop()

st.title("Promotions")
st.caption("Promote a Lab config to Paper or Live-Ready (audit logged).")

with get_conn() as conn:
    if not table_exists(conn, "strategy_lab"):
        st.warning("No `strategy_lab` table. Run migrate_m12.py")
        st.stop()
    if not table_exists(conn, "strategy_promotions"):
        st.warning("No `strategy_promotions` table. Run migrate_m12.py")
        st.stop()

    df = read_df(conn, "SELECT id, name, notes, updated_at FROM strategy_lab ORDER BY updated_at DESC")
    st.subheader("Pick a Lab Config")
    if df.empty:
        st.info("No Lab configs to promote.")
        st.stop()
    st.dataframe(df, use_container_width=True, height=240)

    st.markdown("---")
    lab_id = st.selectbox("Lab ID", df["id"].tolist())
    target_env = st.radio("Promote to", ["paper","live_ready"], horizontal=True)
    notes = st.text_area("Notes (optional)", value="M12 promotion.")

    if st.button("Promote"):
        conn.execute("""
          INSERT INTO strategy_promotions (lab_id, target_env, promoted_by, notes)
          VALUES (?, ?, ?, ?)""", (int(lab_id), target_env, user.get("username","admin"), notes))
        conn.commit()
        st.success(f"Promoted Lab ID {lab_id} -> {target_env}")

    st.markdown("---")
    st.subheader("Promotion History")
    dfp = read_df(conn, """
      SELECT p.id, p.lab_id, l.name AS lab_name, p.target_env, p.promoted_by, p.promoted_at, p.notes
      FROM strategy_promotions p
      LEFT JOIN strategy_lab l ON l.id = p.lab_id
      ORDER BY p.promoted_at DESC
      LIMIT 200
    """)
    if dfp.empty:
        st.info("No promotions yet.")
    else:
        st.dataframe(dfp, use_container_width=True, height=300)




