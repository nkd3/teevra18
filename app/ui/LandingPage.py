# -*- coding: utf-8 -*-
# Teevra18 LandingPage — minimal login screen (no sidebar, no pill, no top gap)

import base64
from pathlib import Path
import streamlit as st
from auth import verify_credentials   # <- your existing auth.py

# --------------------------------------------------------
# 1) Streamlit Config — MUST be first
# --------------------------------------------------------
st.set_page_config(
    page_title="Teevra18 | Login",
    page_icon="🔒",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# --------------------------------------------------------
# 2) Kill sidebar arrow + top decoration/pill
# --------------------------------------------------------
st.markdown("""
<style>
/* Hide sidebar toggle arrow */
[data-testid="collapsedControl"] {display: none;}
/* Remove any extra padding/margin at the top */
.block-container {padding-top: 1rem;}
/* Center the logo */
.t18-logo {
  display: block;
  margin-left: auto;
  margin-right: auto;
  margin-bottom: 12px;
  width: 280px;
  height: auto;
}
</style>
<script>
(function() {
  function killTopPill() {
    const root = document.querySelector('.main .block-container');
    if (!root) return;
    const kids = Array.from(root.children);
    for (const el of kids) {
      const r = el.getBoundingClientRect();
      const cs = window.getComputedStyle(el);
      const br = parseFloat(cs.borderRadius || "0");
      const bg = cs.backgroundImage || "";
      const isTopish = r.top < 100;
      const isShort  = r.height <= 70;
      const isPill   = br >= 8 || bg.includes('gradient');
      if (isTopish && isShort && isPill) el.style.display = 'none';
    }
  }
  killTopPill();
  const mo = new MutationObserver(killTopPill);
  mo.observe(document.body, { childList: true, subtree: true });
})();
</script>
""", unsafe_allow_html=True)

# --------------------------------------------------------
# 3) Helper: embed logo as base64 <img>
# --------------------------------------------------------
def _logo_html() -> str:
    for p in [
        Path(r"C:\teevra18\assets\Teevra18_Logo.png"),
        Path(r"C:\teevra18\assets\Teevra18_Logo.ico")
    ]:
        if p.exists():
            b64 = base64.b64encode(p.read_bytes()).decode("ascii")
            mime = "png" if p.suffix.lower()==".png" else "x-icon"
            return f'<img class="t18-logo" src="data:image/{mime};base64,{b64}" alt="Teevra18"/>'
    return '<h2 style="text-align:center;">Teevra18</h2>'

# --------------------------------------------------------
# 4) UI — Logo + Login form
# --------------------------------------------------------
st.markdown('<div style="text-align:center;margin-top:2rem;">', unsafe_allow_html=True)
st.markdown(_logo_html(), unsafe_allow_html=True)
st.markdown('<h3 style="margin-top:1rem;">Login</h3>', unsafe_allow_html=True)
st.caption("Welcome to Teevra18 — please sign in to continue.")

username = st.text_input("Username", key="t18_user")
password = st.text_input("Password", type="password", key="t18_pass")

# Ensure 'user' always exists (prevents NameError on reruns)
user = None

if st.button("Sign In", type="primary"):
    try:
        user = verify_credentials(username, password)
    except Exception as e:
        st.error(f"Auth error: {e}")
        user = None

    if user:
        # Normalise and persist session identity for all pages
        username_norm = (user.get("username") or username or "").strip()
        st.session_state["t18_auth_user"] = username_norm
        st.session_state["auth_user"] = {"name": username_norm}
        st.session_state.user = user  # includes 'role' from auth.verify_credentials

        # Route to Dashboard in pages/
        st.switch_page("pages/Dashboard_Shell.py")
    else:
        st.error("Invalid credentials or inactive user.")

st.markdown('<p style="margin-top:3rem;font-size:0.8rem;color:gray;">© Teevra18 • Local-only • Secure by Design</p>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)
