# -*- coding: utf-8 -*-
import streamlit as st
from pathlib import Path
from auth import verify_credentials, require_role
from ui_compat import show_image_auto

st.set_page_config(page_title="Teevra18 • Control Panel", page_icon="🦁", layout="wide")

# Sidebar with logo + branding
logo_path = Path(r"C:\teevra18\assets\Teevra18_Logo.png")
with st.sidebar:
    st.markdown("### TeeVra 18")
    if logo_path.exists():
        show_image_auto(st, str(logo_path))
    st.caption("Secure • Fast • Focused")

# Initialise session user
if "user" not in st.session_state:
    st.session_state.user = None

st.title("Welcome to TeeVra18 Control Panel")
st.subheader("Login")

# Login form
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
            st.rerun()
        else:
            st.error("Invalid credentials or inactive user.")

# If already logged in
else:
    user = st.session_state.user
    st.success(f"You are signed in as **{user['username']}** · Role: **{user['role']}**")

    st.divider()
    st.subheader("Choose a workspace")

    # Admin role gets more links
    if user["role"] == "admin":
        c1, c2, c3 = st.columns(3)
        with c1:
            st.page_link("pages/Control_Panel.py", label="Control Panel", icon="🧭")
            st.page_link("pages/Backtest.py", label="Backtest", icon="🧪")
        with c2:
            st.page_link("pages/Paper_Trade.py", label="Paper Trade", icon="📝")
            st.page_link("pages/Live_Trading.py", label="Live Trading", icon="🟢")
        with c3:
            st.page_link("pages/Strategy_Lab.py", label="Strategy Lab", icon="🧪")
            st.page_link("pages/Risk_Policies.py", label="Risk Policies", icon="⚖️")
            st.page_link("pages/Account_Users.py", label="User Accounts", icon="👥")
            st.page_link("pages/Trader_Dashboard.py", label="Trader Dashboard", icon="📈")

    # Trader role gets fewer links
    elif user["role"] == "trader":
        c1, c2 = st.columns(2)
        with c1:
            st.page_link("pages/Control_Panel.py", label="Control Panel", icon="🧭")
            st.page_link("pages/Backtest.py", label="Backtest", icon="🧪")
        with c2:
            st.page_link("pages/Paper_Trade.py", label="Paper Trade", icon="📝")
            st.page_link("pages/Live_Trading.py", label="Live Trading", icon="🟢")
            st.page_link("pages/Trader_Dashboard.py", label="Trader Dashboard", icon="📈")

    st.divider()
    if st.button("Sign Out"):
        st.session_state.user = None
        st.rerun()
