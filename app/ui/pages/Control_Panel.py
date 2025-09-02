# -*- coding: utf-8 -*-
from t18_common.db import get_conn, table_exists, columns, read_df, first_existing
from pathlib import Path
import sys
import streamlit as st

# --------------------------------------------------------------------
# Path bootstrap so imports work regardless of launch directory
# This file lives at: C:\teevra18\app\ui\pages\Control_Panel.py
# parents[0] = pages, [1] = ui, [2] = app, [3] = teevra18
# --------------------------------------------------------------------
THIS_FILE = Path(__file__).resolve()
APP_DIR   = THIS_FILE.parents[2]   # C:\teevra18\app
UI_DIR    = THIS_FILE.parents[1]   # C:\teevra18\app\ui

if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
if str(UI_DIR) not in sys.path:
    sys.path.insert(0, str(UI_DIR))

# --------------------------------------------------------------------
# Streamlit page config MUST be before any other Streamlit calls
# --------------------------------------------------------------------
st.set_page_config(
    page_title="Control Panel • TeeVra18",
    page_icon="🧭",
    layout="wide"
)

# Ensure session key exists (so we can gate content cleanly)
if "user" not in st.session_state:
    st.session_state.user = None

# --------------------------------------------------------------------
# Imports (now that sys.path is set up)
# --------------------------------------------------------------------
from t18_common.db import get_conn
from t18_common.metrics import get_today_pl, get_open_risk, get_signal_chips
from t18_common.policy import get_active_policy_row
from ui_compat import metric_row
from ui.components.ops_bar import render_ops_bar  # <— fixed import

# --------------------------------------------------------------------
# Header + Ops Bar
# --------------------------------------------------------------------
st.title("Teevra18 — Control Panel")
render_ops_bar()

# --------------------------------------------------------------------
# Auth (simple gate based on session state)
# --------------------------------------------------------------------
user = st.session_state.get("user")
if not user:
    st.error("Please sign in from the Landing page (Home_Landing).")
    # Helpful links (one of these will work depending on how you launch)
    with st.expander("Go to Login"):
        # Try main entry
        try:
            st.page_link("Home_Landing.py", label="Open Landing / Login", icon="🏠")
        except Exception:
            pass
        # Try parent path form (some Streamlit versions prefer this)
        try:
            st.page_link("../Home_Landing.py", label="Open Landing / Login (alt)", icon="🏠")
        except Exception:
            pass
    st.stop()

st.caption("M12 — Summary, shortcuts, health.")

# --------------------------------------------------------------------
# KPIs (Today P/L, Open Risk, Signal Chips) + Active Policy banner
# --------------------------------------------------------------------
pl = "n/a"
risk = "n/a"
chips = {"green": 0, "amber": 0, "red": 0}
row, policy = None, None

# Be resilient if DB isn't ready yet
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

# Active Policy banner
if row is not None and policy:
    try:
        st.success(
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

# Chips + metrics
def _fmt_rupees(v):
    return ("₹ " + f"{v:,.0f}") if isinstance(v, (int, float)) else "n/a"

chips_str = f"🟢 {chips.get('green', 0)} · 🟠 {chips.get('amber', 0)} · 🔴 {chips.get('red', 0)}"
metric_row([
    {"label": "Today P/L",     "value": _fmt_rupees(pl)},
    {"label": "Open Risk",     "value": _fmt_rupees(risk)},
    {"label": "Summary Chips", "value": chips_str},
])

st.divider()

# --------------------------------------------------------------------
# Quick links (Streamlit multipage navigation)
# NOTE: Your entry file is inside 'pages', and these links also point to files in 'pages'.
# Keeping the "pages/..." form to match your structure.
# --------------------------------------------------------------------
c1, c2, c3 = st.columns(3)
with c1:
    st.page_link("pages/Trader_Dashboard.py", label="Trader Dashboard", icon="📈")
    st.page_link("pages/Backtest.py",         label="Backtest",         icon="🧪")
with c2:
    st.page_link("pages/Paper_Trade.py",      label="Paper Trade",      icon="📝")
    st.page_link("pages/Live_Trading.py",     label="Live Trading",     icon="🟢")
with c3:
    st.page_link("pages/Strategy_Lab.py",     label="Strategy Lab",     icon="🧪")
    st.page_link("pages/Risk_Policies.py",    label="Risk Policies",    icon="⚖️")
    st.page_link("pages/Alerts_Settings.py",  label="Alerts Settings",  icon="🔔")
    st.page_link("pages/Account_Users.py",    label="User Accounts",    icon="👥")




