# -*- coding: utf-8 -*-
import sys, json
from pathlib import Path
import streamlit as st

from t18_common.db import get_conn, read_df
from t18_common.admin_utils import get_alerts_map

# (optional) path bootstrap
APP_DIR = Path(__file__).resolve().parents[2]
UI_DIR  = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path: sys.path.append(str(APP_DIR))
if str(UI_DIR)  not in sys.path: sys.path.append(str(UI_DIR))

st.set_page_config(page_title="Live Trading • TeeVra18", page_icon="🟢", layout="wide")

user = st.session_state.get("user")
if not user:
    st.error("Please sign in.")
    st.stop()

st.title("Live Trading")
st.caption("Live = Manual Zerodha after alert (no auto order placement).")

def _get_str(d, key, default=""):
    v = d.get(key, {})
    return str(v.get("value", default)) if isinstance(v, dict) else str(v) if v is not None else default

with get_conn() as conn:
    # Telegram sanity
    tele = get_alerts_map(conn, "telegram")
    bot_token = _get_str(tele, "BOT_TOKEN")
    chat_id   = _get_str(tele, "CHAT_ID")
    enabled_v = _get_str(tele, "ENABLED", "0").strip().lower()
    ok = bool(bot_token and chat_id and (enabled_v in ("1","true","yes","on")))
    if ok:
        st.success("Telegram alerts: ENABLED ✓")
    else:
        st.warning("Telegram alerts not fully configured. Set BOT_TOKEN / CHAT_ID / ENABLED=1 in Alerts Settings.")

    st.subheader("Session Control")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Start Live Session (Manual Zerodha)"):
            params = {"mode":"manual_zerodha_after_alert"}
            conn.execute(
                """INSERT INTO runs (kind,status,lab_id,params_json,started_by,notes)
                   VALUES ('live','running',NULL,?,?, 'Live manual session started')""",
                (json.dumps(params), user.get("username","user"))
            )
            conn.commit()
            st.success("Live session started (status=running).")
            st.rerun()
    with c2:
        if st.button("Stop Live Session"):
            conn.execute(
                """UPDATE runs SET status='stopped', ended_at=datetime('now')
                   WHERE kind='live' AND status='running'"""
            )
            conn.commit()
            st.warning("Live session stopped.")
            st.rerun()

    st.markdown("---")
    st.subheader("Recent Live Sessions")
    df_runs = read_df(conn,
        "SELECT id, kind, status, started_by, started_at, ended_at, notes "
        "FROM runs WHERE kind='live' ORDER BY id DESC LIMIT 50"
    )
    st.dataframe(df_runs, use_container_width=True, height=360)

    st.info("During Live sessions, alerts fire (Telegram) and you place orders in Zerodha manually. This page logs session state only.")


