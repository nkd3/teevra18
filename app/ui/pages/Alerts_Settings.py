# -*- coding: utf-8 -*-
import sys
from pathlib import Path
import streamlit as st

APP_DIR = Path(__file__).resolve().parents[2]
UI_DIR  = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path: sys.path.append(str(APP_DIR))
if str(UI_DIR)  not in sys.path: sys.path.append(str(UI_DIR))

from common.db import get_conn, table_exists, read_df
from common.admin_utils import upsert_alert_setting, get_alerts_map

st.set_page_config(page_title="Alerts Settings • TeeVra18", page_icon="🔔", layout="wide")

user = st.session_state.get("user")
if not user or user.get("role") != "admin":
    st.error("Access denied. Admins only.")
    st.stop()

st.title("Alerts Settings")
st.caption("Configure Telegram / EOD alert settings (stored locally in SQLite).")

with get_conn() as conn:
    if not table_exists(conn, "alerts_config"):
        st.warning("No `alerts_config` table. Run migrate_m12.py")
        st.stop()

    tmap = get_alerts_map(conn, "telegram")

    st.subheader("Telegram")
    bot_token = st.text_input("BOT_TOKEN", value=tmap.get("BOT_TOKEN",""))
    chat_id   = st.text_input("CHAT_ID", value=tmap.get("CHAT_ID",""))
    enabled   = st.checkbox("Enabled", value=tmap.get("ENABLED","0")=="1")

    if st.button("Save Telegram Settings"):
        upsert_alert_setting(conn, "telegram", "BOT_TOKEN", bot_token)
        upsert_alert_setting(conn, "telegram", "CHAT_ID", chat_id)
        upsert_alert_setting(conn, "telegram", "ENABLED", "1" if enabled else "0")
        st.success("Saved Telegram settings.")
