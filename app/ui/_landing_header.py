# ===== Teevra18 minimal header (do not duplicate) =====
import base64, io, os
from pathlib import Path
import streamlit as st

# Call once, and suppress if already called elsewhere
try:
    st.set_page_config(page_title="Teevra18 | Login", page_icon="🔒", layout="centered", initial_sidebar_state="collapsed")
except Exception:
    pass  # already called in your original code; that is fine

# Kill sidebar, header, footer, and default top padding (removes the big empty area)
st.markdown("""
<style>
[data-testid="stSidebar"], header, footer, #MainMenu { display:none !important; }
html, body, .stApp { background:#0b0f14 !important; color:#d6dee7 !important; }
.main .block-container { padding-top:0 !important; padding-bottom:0 !important; margin-top:0 !important; }
.t18-center { min-height:100vh; display:flex; align-items:center; justify-content:center; }
.t18-card { width:520px; max-width:92vw; background:linear-gradient(180deg,#111826,#0e1521);
            border:1px solid rgba(255,255,255,.08); border-radius:18px; padding:28px 26px 22px; box-shadow:0 10px 28px rgba(0,0,0,.45);}
.t18-logo { display:block; margin:0 auto 12px auto; width:86px; height:auto; border-radius:10px; }
.t18-title { text-align:center; margin:2px 0 14px 0; }
.t18-title h2 { margin:0; font-weight:900; letter-spacing:.3px; }
.t18-sub { color:#9aa7b3; font-size:12.5px; text-align:center; margin:-4px 0 14px 0; }
.t18-foot { margin-top:12px; color:#9aa7b3; font-size:12px; text-align:center; border-top:1px dashed rgba(255,255,255,.08); padding-top:10px;}
</style>
""", unsafe_allow_html=True)

# Base64 logo helper (works even if file URLs are blocked)
def t18_logo_html():
    for p in [Path(r"C:\teevra18\assets\Teevra18_Logo.png"), Path(r"C:\teevra18\assets\Teevra18_Logo.ico")]:
        if p.exists():
            b64 = p.read_bytes().__len__() and __import__("base64").b64encode(p.read_bytes()).decode("ascii")
            mime = "png" if p.suffix.lower()==".png" else "x-icon"
            return f'<img class="t18-logo" src="data:image/{mime};base64,{b64}" alt="Teevra18"/>'
    return ""

# Open centered shell; your original code continues after this header
st.markdown('<div class="t18-center"><div class="t18-card">', unsafe_allow_html=True)
# ===== End minimal header =====
