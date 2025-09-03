# -*- coding: utf-8 -*-
import streamlit as st
from pathlib import Path
from auth import verify_credentials, require_role
from ui_compat import show_image_auto
import sys
import re
import json
import os
import sqlite3
import tempfile
from typing import Dict, Tuple
import requests

# ---- CONFIG ----
APP_ROOT = Path(r"C:\teevra18\app")
UI_DIR = APP_ROOT / "ui"
PAGES_UI = UI_DIR / "pages"        # try here first (when ui/ is main)
PAGES_ROOT = APP_ROOT / "pages"    # fallback here (classic layout)
CONFIG_PATH = Path(r"C:\teevra18\teevra18.config.json")

# Route -> filename mapping (filenames only; we resolve real path per click)
PAGE_FILES = {
    "status_api": "Status_API_Connectivity.py",
    "control_panel": "Control_Panel.py",
    "backtest": "Backtest.py",
    "paper_trade": "Paper_Trade.py",
    "live_trading": "Live_Trading.py",
    "strategy_lab": "Strategy_Lab.py",
    "risk_policies": "Risk_Policies.py",
    "user_accounts": "Account_Users.py",
    "trader_dashboard": "Trader_Dashboard.py",
}

_SET_PAGE_CONFIG_REGEX = re.compile(r"(?s)\bst\.set_page_config\s*\(.*?\)\s*")

# ---------- Config helpers ----------
def _load_cfg(path: Path = CONFIG_PATH) -> Dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

CFG = _load_cfg()

def _cfg(path: str, default=None):
    node = CFG
    for part in path.split("."):
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            return default
    return node

def _timeout_s() -> int:
    try:
        return int(_cfg("network.timeout_seconds", 6))
    except Exception:
        return 6

# ---------- Status checks (inline acceptance chips) ----------
def _ok(msg: str):    return "🟢", msg
def _warn(msg: str):  return "🟠", msg
def _bad(msg: str):   return "🔴", msg

def _check_sqlite() -> Tuple[str, str]:
    db_path = _cfg("db_path", r"C:\teevra18\data\teevra18.db")
    try:
        p = Path(db_path)
        if not p.exists():
            return _bad(f"DB not found: {db_path}")
        con = sqlite3.connect(db_path, timeout=3)
        try:
            cur = con.cursor()
            cur.execute("SELECT 1;")
            cur.fetchone()
        finally:
            con.close()
        return _ok(f"DB OK")
    except Exception as e:
        return _bad(f"DB error")

def _check_parquet() -> Tuple[str, str]:
    pdir = Path(_cfg("parquet_dir", r"C:\teevra18\data\parquet"))
    try:
        if not pdir.exists():
            return _bad("Parquet missing")
        try:
            fd, tmp = tempfile.mkstemp(prefix="t18_", suffix=".tmp", dir=str(pdir))
            os.close(fd)
            os.remove(tmp)
        except PermissionError:
            return _bad("Parquet no write")
        except Exception:
            return _warn("Parquet write warn")
        return _ok("Parquet OK")
    except Exception:
        return _bad("Parquet err")

def _check_telegram() -> Tuple[str, str]:
    token = _cfg("telegram.bot_token", "")
    if not token or token.startswith("<"):
        return _warn("Telegram token?")
    try:
        r = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=_timeout_s())
        if r.status_code == 200 and r.json().get("ok") is True:
            return _ok("Telegram OK")
        return _bad("Telegram auth")
    except requests.exceptions.ConnectTimeout:
        return _bad("Telegram timeout")
    except requests.exceptions.RequestException:
        return _bad("Telegram err")

def _check_dhan() -> Tuple[str, str]:
    base = (_cfg("dhan.rest_base", "https://api.dhan.co") or "https://api.dhan.co").rstrip("/")
    token = _cfg("dhan.access_token", "")
    if not token or token.startswith("<"):
        # Show warn (reachable unknown) — still acceptance-visible
        try:
            r = requests.get(f"{base}/status", timeout=_timeout_s())
        except Exception:
            pass
        return _warn("Dhan token?")
    try:
        r = requests.get(f"{base}/orders", headers={"access-token": token}, timeout=_timeout_s())
        if r.status_code == 200:
            return _ok("Dhan OK")
        elif r.status_code in (401, 403):
            return _bad("Dhan auth")
        else:
            return _bad("Dhan HTTP")
    except requests.exceptions.ConnectTimeout:
        return _bad("Dhan timeout")
    except requests.exceptions.RequestException:
        return _bad("Dhan err")

def _status_bar():
    # Compact acceptance chips row + refresh button
    c1, c2, c3, c4, c5 = st.columns([1,1,1,1,0.6])
    tg_i, tg_m = _check_telegram()
    dh_i, dh_m = _check_dhan()
    db_i, db_m = _check_sqlite()
    pq_i, pq_m = _check_parquet()
    with c1: st.metric(label="Telegram", value=tg_i, help=tg_m)
    with c2: st.metric(label="DhanHQ", value=dh_i, help=dh_m)
    with c3: st.metric(label="Database", value=db_i, help=db_m)
    with c4: st.metric(label="Parquet", value=pq_i, help=pq_m)
    with c5:
        if st.button("🔄 Refresh", key="btn_status_refresh", use_container_width=True):
            st.experimental_rerun()

# ---------- Router helpers (no st.page_link) ----------
def _resolve_page_path(route_key: str) -> Path | None:
    fname = PAGE_FILES.get(route_key)
    if not fname:
        return None
    p1 = PAGES_UI / fname
    if p1.exists(): return p1
    p2 = PAGES_ROOT / fname
    if p2.exists(): return p2
    return None

def _read_code_bom_safe(page_path: Path) -> str:
    raw = page_path.read_bytes()
    code = raw.decode("utf-8-sig", errors="replace").replace("\ufeff", "")
    code = _SET_PAGE_CONFIG_REGEX.sub("", code)  # remove whole st.set_page_config(...)
    return code

def _run_external_page(page_path: Path):
    try:
        code = _read_code_bom_safe(page_path)
        page_dir = page_path.parent
        if str(page_dir) not in sys.path:
            sys.path.insert(0, str(page_dir))
        sandbox = {"st": st, "__name__": "__embedded_page__", "__file__": str(page_path)}
        exec(compile(code, str(page_path), "exec"), sandbox, None)
    except Exception as e:
        st.exception(e)

def _goto(route_key: str | None):
    st.session_state["_route"] = route_key
    st.experimental_rerun()

# ---------- UI: Sidebar + Tiles ----------
def _nav_button(label: str, route_key: str, key: str, *, container=True):
    if st.button(label, use_container_width=container, key=key):
        _goto(route_key)

def _home_button_inline():
    cols = st.columns([0.22, 0.78])
    with cols[0]:
        if st.button("← Home (Landing)", key="btn_home_top", use_container_width=True):
            _goto(None)
    with cols[1]:
        st.markdown("")

def _sidebar_quick_links():
    st.markdown("---")
    st.subheader("Quick Links")
    if st.button("🏠 Home (Landing)", key="side_home", use_container_width=True):
        _goto(None)

    # Make Status the first obvious action
    _nav_button("Status — API Connectivity ✅", "status_api", "side_status")

    col1, col2 = st.columns(2)
    with col1:
        _nav_button("Control Panel 🧭", "control_panel", "side_ctrl")
        _nav_button("Backtest 🧪", "backtest", "side_bt")
    with col2:
        _nav_button("Paper Trade 📝", "paper_trade", "side_paper")
        _nav_button("Live Trading 🟢", "live_trading", "side_live")
        _nav_button("Strategy Lab 🧪", "strategy_lab", "side_lab")

def _workspace_tiles_for_role(role: str):
    # Always show a Status tile at the top
    st.button("Status — API Connectivity ✅", key="tile_status", use_container_width=True, on_click=_goto, args=("status_api",))
    st.button("🏠 Home (Landing)", key="tile_home", use_container_width=True, on_click=_goto, args=(None,))
    st.divider()

    if role == "admin":
        c1, c2, c3 = st.columns(3)
        with c1:
            _nav_button("Control Panel 🧭", "control_panel", "adm_ctrl")
            _nav_button("Backtest 🧪", "backtest", "adm_bt")
        with c2:
            _nav_button("Paper Trade 📝", "paper_trade", "adm_paper")
            _nav_button("Live Trading 🟢", "live_trading", "adm_live")
        with c3:
            _nav_button("Strategy Lab 🧪", "strategy_lab", "adm_lab")
            _nav_button("Risk Policies ⚖️", "risk_policies", "adm_risk")
            _nav_button("User Accounts 👥", "user_accounts", "adm_users")
            _nav_button("Trader Dashboard 📈", "trader_dashboard", "adm_dash")

    elif role == "trader":
        c1, c2 = st.columns(2)
        with c1:
            _nav_button("Control Panel 🧭", "control_panel", "trd_ctrl")
            _nav_button("Backtest 🧪", "backtest", "trd_bt")
        with c2:
            _nav_button("Paper Trade 📝", "paper_trade", "trd_paper")
            _nav_button("Live Trading 🟢", "live_trading", "trd_live")
            _nav_button("Trader Dashboard 📈", "trader_dashboard", "trd_dash")
            _nav_button("Strategy Lab 🧪", "strategy_lab", "trd_lab")

# ---------- Main ----------
def main():
    st.set_page_config(page_title="Teevra18 • Control Panel", page_icon="🦁", layout="wide")

    # Sidebar
    logo_path = Path(r"C:\teevra18\assets\Teevra18_Logo.png")
    with st.sidebar:
        st.markdown("### TeeVra 18")
        if logo_path.exists():
            show_image_auto(st, str(logo_path))
        st.caption("Secure • Fast • Focused")
        _sidebar_quick_links()

    # Session
    if "user" not in st.session_state:
        st.session_state.user = None
    if "_route" not in st.session_state:
        st.session_state["_route"] = None  # landing by default

    active_route = st.session_state.get("_route")

    # ALWAYS show the acceptance chips at top
    st.markdown("#### System Status")
    _status_bar()

    # If a page is open, show breadcrumb and embed it
    if active_route:
        _home_button_inline()
        page_path = _resolve_page_path(active_route)
        if page_path is None:
            fname = PAGE_FILES.get(active_route, "?")
            st.error(
                "Page not found for route "
                f"`{active_route}`.\n\nTried:\n- {PAGES_UI / fname}\n- {PAGES_ROOT / fname}\n\n"
                "➤ Fix: Ensure the file exists."
            )
            st.session_state["_route"] = None
        else:
            _run_external_page(page_path)
        return

    # ---- Landing (Login + Workspace) ----
    st.title("Welcome to TeeVra18 Control Panel")
    st.subheader("Login")

    if st.session_state.user is None:
        with st.form("login_form", clear_on_submit=False):
            col1, col2 = st.columns(2)
            with col1:
                username = st.text_input("Username")
            with col2:
                password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Sign In")

        if submitted:
            user = verify_credentials(username, password)
            if user:
                st.session_state.user = user
                st.success(f"Signed in as **{user['username']}** ({user['role']})")
                st.experimental_rerun()
            else:
                st.error("Invalid credentials or inactive user.")
    else:
        user = st.session_state.user
        st.success(f"You are signed in as **{user['username']}** · Role: **{user['role']}**")

        st.divider()
        st.subheader("Choose a workspace")
        _workspace_tiles_for_role(user["role"])

        st.divider()
        if st.button("Sign Out", key="btn_signout"):
            st.session_state.user = None
            st.experimental_rerun()

# Streamlit entry
if __name__ == "__main__":
    main()
