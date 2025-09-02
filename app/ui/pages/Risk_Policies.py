# -*- coding: utf-8 -*-
from t18_common.db import get_conn, table_exists, columns, read_df, first_existing
import sys, json
from pathlib import Path
import streamlit as st

APP_DIR = Path(__file__).resolve().parents[2]
UI_DIR  = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path: sys.path.append(str(APP_DIR))
if str(UI_DIR)  not in sys.path: sys.path.append(str(UI_DIR))

from t18_common.db import get_conn, table_exists, read_df

st.set_page_config(page_title="Risk Policies • TeeVra18", page_icon="⚖️", layout="wide")

user = st.session_state.get("user")
if not user or user.get("role") != "admin":
    st.error("Access denied. Admins only.")
    st.stop()

st.title("Risk Policies")
st.caption("Set daily caps, max 5 trades/day, SL ≤ ₹1000/lot, R:R ≥ 1:2, exposure caps, etc.")

DEFAULT_POLICY = {
    "capital_mode": "fixed",        # 'fixed' or 'dynamic'
    "fixed_capital": 150000,
    "risk_per_trade_pct": 1.0,
    "max_trades_per_day": 5,
    "daily_loss_limit_rupees": 5000,
    "stoploss_rupees_per_lot": 1000,
    "min_rr": 2.0,
    "exposure_per_group_pct": 20,
    "trading_window": {"start":"09:20","end":"15:20"},
    "liquidity_filters": {"min_oi": 10000, "min_volume": 10000},
    "spread_buffer": 0.5,
    "slippage_bps": 5,
    "fees_per_order": 20
}

def ensure_default_policy(conn):
    # create table via migrate script separately; this function only inserts a default row if table is empty
    df = read_df(conn, "SELECT COUNT(1) AS c FROM policy_configs")
    if df.empty or int(df.iloc[0]["c"]) == 0:
        conn.execute("""INSERT INTO policy_configs (name, active, policy_json) VALUES (?,?,?)""",
                     ("Default Policy", 1, json.dumps(DEFAULT_POLICY)))
        conn.commit()

with get_conn() as conn:
    if not table_exists(conn, "policy_configs"):
        st.error("Table `policy_configs` not found. Please run:  python C:\\teevra18\\scripts\\migrate_m12.py")
        st.stop()

    # self-heal if the table is empty
    ensure_default_policy(conn)

    # load all policies
    df = read_df(conn, "SELECT id, name, active, policy_json, updated_at FROM policy_configs ORDER BY updated_at DESC")

    st.subheader("Existing Policies")
    if df.empty:
        st.warning("No policies exist. A default policy should have been created. Try refreshing the page.")
        st.stop()
    else:
        st.dataframe(df.drop(columns=["policy_json"]), use_container_width=True, height=240)

    st.markdown("---")
    st.subheader("Edit Policy")

    # choose which policy to edit; prefer active, else first row
    active_ids = df[df.get("active", 0) == 1]["id"].tolist() if "active" in df.columns else []
    default_id = int(active_ids[0]) if active_ids else int(df.iloc[0]["id"])
    all_ids = [int(x) for x in df["id"].tolist()]
    sel_id = st.selectbox("Policy ID", options=all_ids, index=all_ids.index(default_id))

    # fetch selected row safely
    row = df[df["id"] == sel_id]
    if row.empty:
        st.error("Selected policy not found. Please refresh.")
        st.stop()
    row = row.iloc[0]
    policy = {}
    try:
        policy = json.loads(row["policy_json"]) if row.get("policy_json") else {}
    except Exception:
        policy = {}

    # UI fields with current values
    c1, c2, c3 = st.columns(3)
    with c1:
        capital_mode = st.selectbox(
            "Capital Mode", ["fixed","dynamic"],
            index=0 if policy.get("capital_mode","fixed")=="fixed" else 1
        )
        fixed_capital = st.number_input(
            "Fixed Capital (₹)", min_value=0,
            value=int(policy.get("fixed_capital", DEFAULT_POLICY["fixed_capital"])),
            step=1000
        )
        risk_pct = st.number_input(
            "Risk per Trade (%)", min_value=0.1, max_value=5.0, step=0.1,
            value=float(policy.get("risk_per_trade_pct", DEFAULT_POLICY["risk_per_trade_pct"]))
        )
    with c2:
        max_trades = st.number_input(
            "Max Trades / Day", min_value=1, max_value=5,
            value=int(policy.get("max_trades_per_day", DEFAULT_POLICY["max_trades_per_day"]))
        )
        daily_loss = st.number_input(
            "Daily Loss Limit (₹)", min_value=0,
            value=int(policy.get("daily_loss_limit_rupees", DEFAULT_POLICY["daily_loss_limit_rupees"])),
            step=100
        )
        stoploss_rupees = st.number_input(
            "SL ≤ ₹ per lot", min_value=100,
            value=int(policy.get("stoploss_rupees_per_lot", DEFAULT_POLICY["stoploss_rupees_per_lot"])),
            step=50
        )
    with c3:
        min_rr = st.number_input(
            "Min R:R", min_value=1.0, max_value=5.0, step=0.1,
            value=float(policy.get("min_rr", DEFAULT_POLICY["min_rr"]))
        )
        exposure = st.number_input(
            "Exposure per Group (%)", min_value=5, max_value=100, step=5,
            value=int(policy.get("exposure_per_group_pct", DEFAULT_POLICY["exposure_per_group_pct"]))
        )
        spread_buffer = st.number_input(
            "Spread Buffer (₹)", min_value=0.0, step=0.1,
            value=float(policy.get("spread_buffer", DEFAULT_POLICY["spread_buffer"]))
        )

    colA, colB, colC = st.columns(3)
    with colA:
        if st.button("Save Policy"):
            new_policy = {
                "capital_mode": capital_mode,
                "fixed_capital": int(fixed_capital),
                "risk_per_trade_pct": float(risk_pct),
                "max_trades_per_day": int(max_trades),
                "daily_loss_limit_rupees": int(daily_loss),
                "stoploss_rupees_per_lot": int(stoploss_rupees),
                "min_rr": float(min_rr),
                "exposure_per_group_pct": int(exposure),
                "trading_window": policy.get("trading_window", {"start":"09:20","end":"15:20"}),
                "liquidity_filters": policy.get("liquidity_filters", {"min_oi": 10000, "min_volume": 10000}),
                "spread_buffer": float(spread_buffer),
                "slippage_bps": policy.get("slippage_bps", 5),
                "fees_per_order": policy.get("fees_per_order", 20)
            }
            conn.execute("""UPDATE policy_configs SET policy_json=?, updated_at=datetime('now') WHERE id=?""",
                         (json.dumps(new_policy), int(sel_id)))
            conn.commit()
            st.success("Policy saved.")
            st.rerun()

    with colB:
        # set selected policy as active
        if st.button("Set Selected As Active"):
            conn.execute("UPDATE policy_configs SET active=0")
            conn.execute("UPDATE policy_configs SET active=1, updated_at=datetime('now') WHERE id=?", (int(sel_id),))
            conn.commit()
            st.success(f"Policy {sel_id} set as Active.")
            st.rerun()

    with colC:
        # create new policy from template (inactive by default)
        if st.button("Create New Policy from Template"):
            conn.execute("""INSERT INTO policy_configs (name, active, policy_json)
                            VALUES (?,?,?)""", (f"Policy {st.session_state.get('user',{}).get('username','admin')}", 0, json.dumps(DEFAULT_POLICY)))
            conn.commit()
            st.success("New policy created from template.")
            st.rerun()




