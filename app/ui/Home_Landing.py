# -*- coding: utf-8 -*-
import streamlit as st
from pathlib import Path
from auth import verify_credentials, require_role
from ui_compat import show_image_auto

def _pages_location_mode():
    """
    Returns:
      "ui_pages"    -> pages are under app/ui/pages   (safe to use st.page_link("pages/..."))
      "root_pages"  -> pages are under app/pages      (must run launcher app/Home_Landing.py)
      "missing"     -> no pages folder found
    """
    here = Path(__file__).resolve().parent              # C:\teevra18\app\ui
    ui_pages = here / "pages"
    root_pages = here.parent / "pages"                  # C:\teevra18\app\pages

    if ui_pages.exists() and ui_pages.is_dir():
        return "ui_pages"
    if root_pages.exists() and root_pages.is_dir():
        return "root_pages"
    return "missing"

def _sidebar_quick_links(mode: str):
    """
    Render Quick Links safely. Only call st.page_link() if we are in 'ui_pages' mode.
    Otherwise, show a routing fix helper so the app never crashes.
    """
    st.markdown("---")
    st.subheader("Quick Links")

    if mode == "ui_pages":
        # Safe: pages live under app/ui/pages
        st.page_link("pages/Status_API_Connectivity.py", label="Status — API Connectivity", icon="✅")
    elif mode == "root_pages":
        # Pages are at app/pages; st.page_link will fail from ui/.
        # Show a routing fix box with the correct run command.
        st.info(
            "⚠️ Page links are disabled because Streamlit expects **ui/pages/** when running from **app/ui**.\n\n"
            "Use the launcher so Streamlit sees **app/pages/**:\n\n"
            "```powershell\n"
            "C:\\teevra18\\.venv\\Scripts\\Activate.ps1\n"
            "streamlit run C:\\teevra18\\app\\Home_Landing.py\n"
            "```\n"
            "Alternatively, copy/move your pages folder to **C:\\teevra18\\app\\ui\\pages**."
        )
        # Render non-clickable placeholders so layout stays consistent
        st.button("Status — API Connectivity (routing fix required)", disabled=True)
    else:
        st.warning("No `pages/` folder found. Create one under either app/ui/pages or app/pages.")
        st.button("Status — API Connectivity (missing pages/)", disabled=True)

def main():
    st.set_page_config(page_title="Teevra18 • Control Panel", page_icon="🦁", layout="wide")

    mode = _pages_location_mode()

    # Sidebar with logo + branding
    logo_path = Path(r"C:\teevra18\assets\Teevra18_Logo.png")
    with st.sidebar:
        st.markdown("### TeeVra 18")
        if logo_path.exists():
            show_image_auto(st, str(logo_path))
        st.caption("Secure • Fast • Focused")

        # Quick Links (always visible in sidebar) — SAFE
        _sidebar_quick_links(mode)

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
                if mode == "ui_pages":
                    st.page_link("pages/Control_Panel.py", label="Control Panel", icon="🧭")
                    st.page_link("pages/Backtest.py", label="Backtest", icon="🧪")
                elif mode == "root_pages":
                    st.button("Control Panel (routing fix required)", disabled=True)
                    st.button("Backtest (routing fix required)", disabled=True)
                else:
                    st.button("Control Panel (missing pages/)", disabled=True)
                    st.button("Backtest (missing pages/)", disabled=True)

            with c2:
                if mode == "ui_pages":
                    st.page_link("pages/Paper_Trade.py", label="Paper Trade", icon="📝")
                    st.page_link("pages/Live_Trading.py", label="Live Trading", icon="🟢")
                elif mode == "root_pages":
                    st.button("Paper Trade (routing fix required)", disabled=True)
                    st.button("Live Trading (routing fix required)", disabled=True)
                else:
                    st.button("Paper Trade (missing pages/)", disabled=True)
                    st.button("Live Trading (missing pages/)", disabled=True)

            with c3:
                if mode == "ui_pages":
                    st.page_link("pages/Strategy_Lab.py", label="Strategy Lab", icon="🧪")
                    st.page_link("pages/Risk_Policies.py", label="Risk Policies", icon="⚖️")
                    st.page_link("pages/Account_Users.py", label="User Accounts", icon="👥")
                    st.page_link("pages/Trader_Dashboard.py", label="Trader Dashboard", icon="📈")
                elif mode == "root_pages":
                    st.button("Strategy Lab (routing fix required)", disabled=True)
                    st.button("Risk Policies (routing fix required)", disabled=True)
                    st.button("User Accounts (routing fix required)", disabled=True)
                    st.button("Trader Dashboard (routing fix required)", disabled=True)
                else:
                    st.button("Strategy Lab (missing pages/)", disabled=True)
                    st.button("Risk Policies (missing pages/)", disabled=True)
                    st.button("User Accounts (missing pages/)", disabled=True)
                    st.button("Trader Dashboard (missing pages/)", disabled=True)

        # Trader role gets fewer links
        elif user["role"] == "trader":
            c1, c2 = st.columns(2)
            with c1:
                if mode == "ui_pages":
                    st.page_link("pages/Control_Panel.py", label="Control Panel", icon="🧭")
                    st.page_link("pages/Backtest.py", label="Backtest", icon="🧪")
                elif mode == "root_pages":
                    st.button("Control Panel (routing fix required)", disabled=True)
                    st.button("Backtest (routing fix required)", disabled=True)
                else:
                    st.button("Control Panel (missing pages/)", disabled=True)
                    st.button("Backtest (missing pages/)", disabled=True)

            with c2:
                if mode == "ui_pages":
                    st.page_link("pages/Paper_Trade.py", label="Paper Trade", icon="📝")
                    st.page_link("pages/Live_Trading.py", label="Live Trading", icon="🟢")
                    st.page_link("pages/Trader_Dashboard.py", label="Trader Dashboard", icon="📈")
                    st.page_link("pages/Strategy_Lab.py", label="Strategy Lab", icon="🧪")
                elif mode == "root_pages":
                    st.button("Paper Trade (routing fix required)", disabled=True)
                    st.button("Live Trading (routing fix required)", disabled=True)
                    st.button("Trader Dashboard (routing fix required)", disabled=True)
                    st.button("Strategy Lab (routing fix required)", disabled=True)
                else:
                    st.button("Paper Trade (missing pages/)", disabled=True)
                    st.button("Live Trading (missing pages/)", disabled=True)
                    st.button("Trader Dashboard (missing pages/)", disabled=True)
                    st.button("Strategy Lab (missing pages/)", disabled=True)

        st.divider()
        if st.button("Sign Out"):
            st.session_state.user = None
            st.rerun()

# Streamlit entry
if __name__ == "__main__":
    main()
