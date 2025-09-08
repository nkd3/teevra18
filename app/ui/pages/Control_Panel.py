# -*- coding: utf-8 -*-
import streamlit as st
st.set_page_config(page_title="Control Panel • TeeVra18", page_icon="🧭", layout="wide", initial_sidebar_state="collapsed")

from ui.components.shell import render_topbar
render_topbar(active_primary="CONTROL PANEL")

# ...rest of your Control Panel page...

# -*- coding: utf-8 -*-
from pathlib import Path
import sys
import time
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
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --------------------------------------------------------------------
# Imports (after sys.path bootstrap)
# --------------------------------------------------------------------
from t18_common.db import get_conn
from t18_common.metrics import get_today_pl, get_open_risk, get_signal_chips
from t18_common.policy import get_active_policy_row
from ui_compat import metric_row
from ui.components.ops_bar import render_ops_bar  # Already exists in your project

# --------------------------------------------------------------------
# Auth gate
# --------------------------------------------------------------------
if "user" not in st.session_state:
    st.session_state.user = None

user = st.session_state.get("user")
if not user:
    st.error("Please sign in from the Landing page (Home_Landing).")
    with st.expander("Go to Login"):
        try:
            st.page_link("Home_Landing.py", label="Open Landing / Login", icon="🏠")
        except Exception:
            pass
        try:
            st.page_link("../Home_Landing.py", label="Open Landing / Login (alt)", icon="🏠")
        except Exception:
            pass
    st.stop()

# --------------------------------------------------------------------
# Header row: Logo | CONTROL PANEL | User Name  (wireframe)
# --------------------------------------------------------------------
from ui.components.top_nav import _safe_logo  # reuse helper
logo_path = None
# If you set your logo path in top_nav.config.json, you can read it similarly;
# keeping a simple safe default here:
candidate = Path("C:/teevra18/assets/Teevra18_Logo.png")
logo_path = str(candidate) if candidate.exists() else None

h1, h2, h3 = st.columns([1, 6, 2], vertical_alignment="center")
with h1:
    if logo_path:
        st.image(logo_path, width=40)
    else:
        st.markdown("<div style='font-size:28px'>🧭</div>", unsafe_allow_html=True)
with h2:
    st.markdown("<div style='font-size:24px;font-weight:800;letter-spacing:0.8px'>CONTROL PANEL</div>", unsafe_allow_html=True)
with h3:
    st.markdown(f"<div style='text-align:right;font-size:14px'>👤 {user}</div>", unsafe_allow_html=True)

# Optional ops bar (kept minimal to avoid altering layout feel)
render_ops_bar()

st.caption("M12 — System Status, Summary KPIs, Quick Links")

st.divider()

# --------------------------------------------------------------------
# Row: System Status | Summary | Refresh (wireframe)
# --------------------------------------------------------------------
c1, c2, c3 = st.columns([3, 4, 1])

# --- SYSTEM STATUS (health checks)
with c1:
    st.subheader("System Status")
    # Quick checks; keep resilient if DB not ready
    statuses = {
        "Telegram": False,
        "DhanHQ": False,
        "Database": False,
        "Parquet": False,
    }

    try:
        with get_conn() as _:
            statuses["Database"] = True
    except Exception:
        statuses["Database"] = False

    # You may have real functions like check_dhan(), check_telegram(), check_parquet()
    # For now, keep placeholders = safest
    # TODO: wire to your real health functions
    # statuses["DhanHQ"]   = check_dhan()
    # statuses["Telegram"] = check_telegram()
    # statuses["Parquet"]  = check_parquet()

    ok = "✅"
    no = "❌"
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
# Quick Links grid (wireframe labels)
# --------------------------------------------------------------------
st.subheader("Quick Links")

L = [
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
    ("🚀 Promotion",        "pages/Promotion.py")
]

# Render as 3 columns grid
cols = st.columns(3)
for i, (label, target) in enumerate(L):
    with cols[i % 3]:
        try:
            st.page_link(target, label=label)
        except Exception:
            st.button(label, disabled=True)
