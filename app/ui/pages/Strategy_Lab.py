# -*- coding: utf-8 -*-
import sys, json
from pathlib import Path
import streamlit as st

APP_DIR = Path(__file__).resolve().parents[2]
UI_DIR  = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path: sys.path.append(str(APP_DIR))
if str(UI_DIR)  not in sys.path: sys.path.append(str(UI_DIR))

from common.db import get_conn, table_exists, read_df

st.set_page_config(page_title="Strategy Lab • TeeVra18", page_icon="🧪", layout="wide")

user = st.session_state.get("user")
if not user or user.get("role") != "admin":
    st.error("Access denied. Admins only.")
    st.stop()

st.title("Strategy Lab")
st.caption("Create/modify indicator stacks and strategy settings, then save as Lab configs.")

with get_conn() as conn:
    if not table_exists(conn, "strategy_lab"):
        st.warning("Table `strategy_lab` not found (run migrate_m12.py).")
    else:
        # list existing lab configs
        df = read_df(conn, "SELECT id, name, notes, created_at, updated_at FROM strategy_lab ORDER BY updated_at DESC")
        st.subheader("Existing Lab Configs")
        if df.empty:
            st.info("No Lab configs yet. Create one below.")
        else:
            st.dataframe(df, use_container_width=True, height=240)

        st.divider()
        st.subheader("Create / Update")

        with st.form("lab_form", clear_on_submit=False):
            mode = st.radio("Mode", ["Create New", "Update Existing"], horizontal=True)
            if mode == "Update Existing" and not df.empty:
                sel_id = st.selectbox("Select Lab ID to update", df["id"].tolist())
            else:
                sel_id = None

            name  = st.text_input("Config Name", value="T18 Base Setup")
            notes = st.text_area("Notes", value="M12 base indicators and settings.")

            st.markdown("**Indicators JSON** (example prefilled)")
            indicators_default = {
                "EMA": {"fast": 9, "slow": 21},         # EMA = Exponential Moving Average
                "RSI": {"period": 14, "ob": 70, "os":30}, # RSI = Relative Strength Index
                "VWAP": {"session": "day"},             # VWAP = Volume Weighted Average Price
                "ADX": {"period":14, "threshold":20},
                "ATR": {"period":14, "mult":1.5}
            }
            indicators_json = st.text_area("Indicators (JSON)", value=json.dumps(indicators_default, indent=2), height=200)

            st.markdown("**Strategy Settings JSON** (example prefilled)")
            settings_default = {
                "lookahead_minutes": 2,
                "max_trades_per_day": 5,
                "require_rr_at_least": 2.0,
                "stoploss_rupees_per_lot": 1000,
                "consider": ["OI","Volume","StrategyDelta","KeyLevels","DarkPools"]
            }
            settings_json = st.text_area("Strategy Settings (JSON)", value=json.dumps(settings_default, indent=2), height=200)

            submitted = st.form_submit_button("Save Lab Config")
            if submitted:
                try:
                    ind = json.loads(indicators_json)
                    stg = json.loads(settings_json)
                except Exception as e:
                    st.error(f"Invalid JSON: {e}")
                else:
                    if sel_id:
                        conn.execute("""
                          UPDATE strategy_lab
                          SET name=?, notes=?, indicators_json=?, settings_json=?, updated_at=datetime('now')
                          WHERE id=?""", (name, notes, json.dumps(ind), json.dumps(stg), int(sel_id)))
                        conn.commit()
                        st.success(f"Updated Lab ID {sel_id}")
                    else:
                        conn.execute("""
                          INSERT INTO strategy_lab (name, notes, indicators_json, settings_json)
                          VALUES (?, ?, ?, ?)""", (name, notes, json.dumps(ind), json.dumps(stg)))
                        conn.commit()
                        st.success("Created new Lab config.")
                    st.rerun()
