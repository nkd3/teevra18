# -*- coding: utf-8 -*-
import base64, json
from pathlib import Path
import streamlit as st

APP_ROOT = Path(r"C:\teevra18")
DATA_DIR = APP_ROOT / "data"
ASSETS_DIR = APP_ROOT / "assets"
PROFILE_JSON = DATA_DIR / "profiles.json"

st.set_page_config(page_title="Teevra18 | My Profile", page_icon="👤", layout="centered", initial_sidebar_state="collapsed")

auth = st.session_state.get("auth_user") or {"name": st.session_state.get("t18_auth_user")}
if not auth or not (auth.get("name") or st.session_state.get("t18_auth_user")):
    st.error("Not logged in."); st.stop()
username = (auth.get("name") or st.session_state.get("t18_auth_user")).strip().lower()

def _profiles_load() -> dict:
    try:
        if PROFILE_JSON.exists(): return json.loads(PROFILE_JSON.read_text(encoding="utf-8"))
    except Exception: pass
    return {}
def _profiles_save(p: dict):
    PROFILE_JSON.parent.mkdir(parents=True, exist_ok=True)
    PROFILE_JSON.write_text(json.dumps(p, indent=2), encoding="utf-8")

profiles = _profiles_load()
mine = profiles.get(username, {"display_name": username, "avatar_path": None})

st.title("My Profile")

def _show_avatar(path: str|None):
    if path and Path(path).exists():
        b64 = base64.b64encode(Path(path).read_bytes()).decode("ascii")
        st.markdown(f"<img src='data:image/png;base64,{b64}' style='width:120px;height:120px;border-radius:50%;border:1px solid #2a2f3a;object-fit:cover;'/>", unsafe_allow_html=True)
    else:
        st.markdown("<div style='width:120px;height:120px;border-radius:50%;border:1px solid #2a2f3a;background:#0d111a;'></div>", unsafe_allow_html=True)

with st.form("profile_form", clear_on_submit=False):
    st.subheader("Account")
    new_display = st.text_input("Username (display name)", value=mine.get("display_name", username), max_chars=40)

    st.subheader("Profile Picture")
    _show_avatar(mine.get("avatar_path"))
    up = st.file_uploader("Upload a square image (PNG/JPG)", type=["png","jpg","jpeg"])

    st.subheader("Change Password")
    cur = st.text_input("Current Password", type="password")
    new = st.text_input("New Password", type="password")
    con = st.text_input("Confirm New Password", type="password")

    colA, colB = st.columns([1,1])
    save = colA.form_submit_button("Save", type="primary", use_container_width=True)
    discard = colB.form_submit_button("Discard", use_container_width=True)

if discard: st.rerun()

if save:
    # Save avatar
    avatar_path = mine.get("avatar_path")
    if up is not None:
        ASSETS_DIR.mkdir(parents=True, exist_ok=True)
        ext = ".png" if up.name.lower().endswith("png") else ".jpg"
        avatar_path = str(ASSETS_DIR / f"profile_{username}{ext}")
        with open(avatar_path, "wb") as f:
            f.write(up.getbuffer())

    # Store display name & avatar
    profiles[username] = {"display_name": new_display.strip() or username, "avatar_path": avatar_path}
    _profiles_save(profiles)

    # Change password using auth module
    pwd_msg = ""
    if new or con or cur:
        if not (new and con and cur): pwd_msg = "• Password fields incomplete; unchanged."
        elif new != con:              pwd_msg = "• New password and confirm do not match; unchanged."
        else:
            try:
                from auth import change_password
                ok = change_password(username, cur, new)
                pwd_msg = "• Password updated." if ok else "• Current password incorrect; not updated."
            except Exception as e:
                pwd_msg = f"• Password change not available: {e}"

    # refresh header username
    st.session_state["auth_user"] = {"name": profiles[username]["display_name"]}
    st.success(f"Profile saved. {pwd_msg}")
    st.rerun()

st.divider()
col1, col2 = st.columns([1,1])
if col1.button("Back to Dashboard", use_container_width=True):
    try: st.switch_page("pages/Dashboard_Shell.py")
    except Exception: st.query_params["_go"]="Dashboard_Shell.py"; st.rerun()
if col2.button("Logout", use_container_width=True):
    for k in ["auth_user","t18_auth_user","user"]:
        if k in st.session_state: del st.session_state[k]
    st.markdown("<script>window.location.replace(window.location.origin + window.location.pathname);</script>", unsafe_allow_html=True)
    st.stop()
