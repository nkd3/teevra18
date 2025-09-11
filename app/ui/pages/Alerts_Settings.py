# -*- coding: utf-8 -*-
import sys
from pathlib import Path
import streamlit as st

from t18_common.db import get_conn, table_exists
from t18_common.admin_utils import upsert_alert_setting, get_alerts_map

# (optional) path bootstrap for safety if you move files later
APP_DIR = Path(__file__).resolve().parents[2]
UI_DIR  = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path: sys.path.append(str(APP_DIR))
if str(UI_DIR)  not in sys.path: sys.path.append(str(UI_DIR))

st.set_page_config(page_title="Alerts Settings • TeeVra18", page_icon="🔔", layout="wide")

user = st.session_state.get("user")
if not user or user.get("role") != "admin":
    st.error("Access denied. Admins only.")
    st.stop()

st.title("Alerts Settings")
st.caption("Configure Telegram / EOD alert settings (stored locally in SQLite).")

def _get_str(d, key, default=""):
    v = d.get(key, {})
    return str(v.get("value", default)) if isinstance(v, dict) else str(v) if v is not None else default

def _get_bool(d, key, default=False):
    v = _get_str(d, key, "1" if default else "0").strip().lower()
    return v in ("1", "true", "yes", "on")

with get_conn() as conn:
    # Ensure table will exist (admin_utils handles creation)
    _ = table_exists(conn, "alerts_settings")

    tmap = get_alerts_map(conn, "telegram")  # channel-aware

    st.subheader("Telegram")
    bot_token = st.text_input("BOT_TOKEN", value=_get_str(tmap, "BOT_TOKEN"))
    chat_id   = st.text_input("CHAT_ID",   value=_get_str(tmap, "CHAT_ID"))
    enabled   = st.checkbox("Enabled",     value=_get_bool(tmap, "ENABLED", default=False))

    if st.button("Save Telegram Settings"):
        upsert_alert_setting(conn, "telegram", "BOT_TOKEN", bot_token)
        upsert_alert_setting(conn, "telegram", "CHAT_ID", chat_id)
        upsert_alert_setting(conn, "telegram", "ENABLED", "1" if enabled else "0")
        st.success("Saved Telegram settings.")
