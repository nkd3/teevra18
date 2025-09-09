# -*- coding: utf-8 -*-
# C:\teevra18\app\ui\pages\Control_Panel.py

import streamlit as st
st.set_page_config(
    page_title="Control Panel • TeeVra18",
    page_icon="🧭",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# --------------------------------------------------------------------
# Bootstrapping (paths) — no Streamlit calls above this line
# --------------------------------------------------------------------
from pathlib import Path
import sys, time

THIS_FILE = Path(__file__).resolve()
APP_DIR   = THIS_FILE.parents[2]   # C:\teevra18\app
UI_DIR    = THIS_FILE.parents[1]   # C:\teevra18\app\ui

if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
if str(UI_DIR) not in sys.path:
    sys.path.insert(0, str(UI_DIR))

# --------------------------------------------------------------------
# Project imports
# --------------------------------------------------------------------
from ui.components.shell import render_topbar
from t18_common.db import get_conn
from t18_common.metrics import get_today_pl, get_open_risk, get_signal_chips
from t18_common.policy import get_active_policy_row
from ui_compat import metric_row
from ui.components.ops_bar import render_ops_bar  # if present in your project

# --------------------------------------------------------------------
# Top bar (must be AFTER set_page_config)
# --------------------------------------------------------------------
render_topbar(active_primary="CONTROL PANEL")

# --------------------------------------------------------------------
# Auth gate
# --------------------------------------------------------------------
if "user" not in st.session_state:
    st.session_state.user = None

user = st.session_state.get("user")
if not user:
    st.error("Please sign in from the Landing page (Home_Landing).")
    with st.expander("Go to Login"):
        for pth in ("Home_Landing.py", "../Home_Landing.py"):
            try:
                st.page_link(pth, label="Open Landing / Login", icon="🏠")
            except Exception:
                pass
    st.stop()

# Optional ops bar (keep your layout untouched)
try:
    render_ops_bar()
except Exception:
    pass

st.caption("M12 — System Status, Summary KPIs, Quick Links")
st.divider()

# --------------------------------------------------------------------
# Row: System Status | Summary | Refresh
# --------------------------------------------------------------------
c1, c2, c3 = st.columns([3, 4, 1])

# --- SYSTEM STATUS (health checks)
with c1:
    st.subheader("System Status")
    statuses = {
        "Database": False,
        "DhanHQ": False,    # TODO: wire real check
        "Telegram": False,  # TODO: wire real check
        "Parquet": False,   # TODO: wire real check
    }
    try:
        with get_conn() as _:
            statuses["Database"] = True
    except Exception:
        statuses["Database"] = False

    ok, no = "✅", "❌"
    st.write(f"Database: {ok if statuses['Database'] else no}")
    st.write(f"DhanHQ:   {ok if statuses['DhanHQ'] else no}")
    st.write(f"Telegram: {ok if statuses['Telegram'] else no}")
    st.write(f"Parquet:  {ok if statuses['Parquet'] else no}")

# --- SUMMARY (KPIs + chips)
with c2:
    st.subheader("Summary")

    pl = "n/a"
    risk = "n/a"
    chips = {"green": 0, "amber": 0, "red": 0}
    row, policy = None, None

    try:
        with get_conn() as conn:
            pl     = get_today_pl(conn)
            risk   = get_open_risk(conn)
            _chips = get_signal_chips(conn) or {}
            chips  = {
                "green": int(_chips.get("green", 0)),
                "amber": int(_chips.get("amber", 0)),
                "red":   int(_chips.get("red", 0)),
            }
            row, policy = get_active_policy_row(conn)
    except Exception as e:
        st.warning(f"Data not ready yet: {e}")

    def _fmt_rupees(v):
        return ("₹ " + f"{v:,.0f}") if isinstance(v, (int, float)) else "n/a"

    chips_str = f"🟢 {chips.get('green', 0)} · 🟠 {chips.get('amber', 0)} · 🔴 {chips.get('red', 0)}"

    metric_row([
        {"label": "Today P/L",     "value": _fmt_rupees(pl)},
        {"label": "Open Risk",     "value": _fmt_rupees(risk)},
        {"label": "Summary Chips", "value": chips_str},
    ])

    if row is not None and policy:
        try:
            st.info(
                f"**Active Policy:** {row['name']}  •  "
                f"Max Trades/Day: {int(policy.get('max_trades_per_day', 5))}  •  "
                f"SL ≤ ₹{int(policy.get('stoploss_rupees_per_lot', 1000))}/lot  •  "
                f"Min R:R ≥ {float(policy.get('min_rr', 2.0))}  •  "
                f"Daily Loss Cap: ₹{int(policy.get('daily_loss_limit_rupees', 5000))}"
            )
        except Exception:
            st.info("Active policy loaded, but some fields were missing. Review **Risk Policies**.")
    else:
        st.info("No active policy found — set one in **Risk Policies** (Admin).")

# --- REFRESH button
with c3:
    st.subheader(" ")
    if st.button("Refresh", use_container_width=True):
        time.sleep(0.1)
        st.rerun()

st.divider()

# --------------------------------------------------------------------
# Quick Links grid
# --------------------------------------------------------------------
st.subheader("Quick Links")

quick_links = [
    ("📈 Trader Dashboard", "pages/Trader_Dashboard.py"),
    ("🧪 Backtest",         "pages/Backtest.py"),
    ("📝 Paper Trade",      "pages/Paper_Trade.py"),
    ("🟢 Live Trading",     "pages/Live_Trading.py"),
    ("🧪 Strategy Lab",     "pages/Strategy_Lab.py"),
    ("⚖️ Risk Policies",    "pages/Risk_Policies.py"),
    ("🔔 Alerts Settings",  "pages/Alerts_Settings.py"),
    ("👥 User Accounts",    "pages/Account_Users.py"),
    ("🛠️ Admin Config",     "pages/Admin_Config.py"),
    ("👤 Account Users",    "pages/Account_Users.py"),
    ("📊 Admin Dashboard",  "pages/Admin_Dashboard.py"),
    ("🚀 Promotion",        "pages/Promotion.py"),
]

cols = st.columns(3)
for i, (label, target) in enumerate(quick_links):
    with cols[i % 3]:
        try:
            st.page_link(target, label=label)
        except Exception:
            st.button(label, disabled=True)
