# C:\teevra18\services\m12\app.py
import os, sqlite3, pandas as pd
from pathlib import Path
import streamlit as st
from datetime import datetime, timezone

DB = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

@st.cache_data(ttl=5)
def q(sql, params=None):
    with sqlite3.connect(DB) as conn:
        return pd.read_sql(sql, conn, params=params or [])

def exec_sql(sql, params=None):
    with sqlite3.connect(DB) as conn:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        conn.commit()

st.set_page_config(page_title="Teevra18 — M12 Control Panel", layout="wide")

st.title("Teevra18 — M12 Control Panel")
st.caption(f"DB: {DB} • Now(UTC): {now_utc()}")

# --- Sidebar Controls ---
st.sidebar.header("Gating Overrides (Session Only)")
prob_thr = st.sidebar.slider("Min Probability", 0.50, 0.99, 0.85, 0.01)
max_trades = st.sidebar.number_input("Max Trades (cap)", 1, 50, 5, 1)
pre_mins   = st.sidebar.number_input("Pre-alert Minutes", 1, 10, 3, 1)

st.sidebar.header("Actions")
if st.sidebar.button("Run Gate Now (override thresholds)"):
    # Use your existing gate script with overrides
    os.system(
        f'python "C:\\teevra18\\services\\m11\\gate_alerts_m11.py" --min-prob {prob_thr} --max-trades {max_trades} --pre-minutes {pre_mins}'
    )
    st.sidebar.success("Gate executed. Refresh tables below.")

# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["Pre-Alerts (PENDING)", "Predictions", "OOS Explorer"])

with tab1:
    st.subheader("Signals Due / Pending")
    df_sig = q("""
        SELECT id, instrument, ts_utc, prob_up, exp_move_abs, pre_alert_at, status, created_at
        FROM signals_m11
        WHERE status='PENDING'
        ORDER BY pre_alert_at ASC, prob_up DESC
        LIMIT 200;
    """)
    st.dataframe(df_sig, use_container_width=True)

    # Bulk actions
    cols = st.columns(3)
    with cols[0]:
        if st.button("Mark selected as ALERTED (simulate send)"):
            st.info("Select rows using the filter/search above, then rerun. (Use SQL if you need bulk ops)")
    with cols[1]:
        if st.button("Refresh"):
            st.experimental_rerun()

with tab2:
    st.subheader("Latest Predictions per Instrument")
    df_pred = q("""
        WITH last AS (
          SELECT instrument, MAX(ts_utc) AS m
          FROM predictions_m11
          GROUP BY instrument
        )
        SELECT p.instrument, p.ts_utc, p.prob_up, p.exp_move_abs, p.created_at
        FROM predictions_m11 p
        JOIN last l ON l.instrument=p.instrument AND l.m=p.ts_utc
        ORDER BY p.prob_up DESC
        LIMIT 500;
    """)
    st.dataframe(df_pred, use_container_width=True)

with tab3:
    st.subheader("OOS Explorer")
    df_oos = q("""
        SELECT id, signal_id, pred_id, instrument, ts_utc, prob_up, label, realized_at, notes
        FROM pred_oos_log
        ORDER BY id DESC
        LIMIT 500;
    """)
    st.dataframe(df_oos, use_container_width=True)

    st.markdown("**Manual Label (quick test while market closed):**")
    colA, colB, colC = st.columns([2,1,1])
    with colA:
        row_id = st.text_input("pred_oos_log.id to label", "")
    with colB:
        lab = st.selectbox("label", ["", "0", "1"])
    with colC:
        if st.button("Apply Label") and row_id.strip().isdigit() and lab in ("0","1"):
            exec_sql("UPDATE pred_oos_log SET label=?, realized_at=? WHERE id=?",
                     (int(lab), now_utc(), int(row_id)))
            st.success(f"Labeled id={row_id} as {lab}.")
            st.experimental_rerun()
