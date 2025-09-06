# -*- coding: utf-8 -*-
# Teevra18 LandingPage — keeps your existing login logic; only adds CSS + centered container.
import base64
from pathlib import Path
import streamlit as st

st.set_page_config(page_title="Teevra18 | Login", page_icon="🔒", layout="centered", initial_sidebar_state="collapsed")

# Inject dark CSS
_css = Path(r"C:\teevra18\app\static\theme_login_dark.css")
if _css.exists():
    st.markdown(f"<style>{_css.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

# Helper: show centered logo (base64 — works even if file URLs are blocked)
def _logo_tag():
    for p in [Path(r"C:\teevra18\assets\Teevra18_Logo.png"), Path(r"C:\teevra18\assets\Teevra18_Logo.ico")]:
        if p.exists():
            b64 = base64.b64encode(p.read_bytes()).decode("ascii")
            mime = "png" if p.suffix.lower()==".png" else "x-icon"
            return f'<img class="t18-logo" src="data:image/{mime};base64,{b64}" alt="Teevra18" />'
    return ""

# Open centered shell (your existing UI can live inside this)
st.markdown('<div class="t18-center"><div class="t18-card">', unsafe_allow_html=True)
# ———————————— your original code starts below ————————————# -*- coding: utf-8 -*-
"""
Teevra18 Landing (Login) — Wireframe-accurate
- No sidebar or Streamlit chrome
- Centered logo (base64 embedded), then "Login", then Username/Password + Sign In
- Uses existing credentials from SQLite (read-only, auto-detect table/columns)
- On success: switch_page to Home_Dashboard.py
"""
import base64, hashlib, sqlite3, os
from pathlib import Path
import streamlit as st

st.set_page_config(
    page_title="Teevra18 | Login",
    page_icon="🔒",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# Inject dark CSS and hide sidebar/menu
css_path = Path(r"C:\teevra18\app\static\theme_login_dark.css")
if css_path.exists():
    st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

DB_PATH = Path(r"C:\teevra18\data\teevra18.db")

# ---------- UTIL: logo (base64) ----------
def _load_logo_bytes() -> bytes | None:
    # Try common locations; add more if you keep assets elsewhere
    candidates = [
        Path(r"C:\teevra18\assets\Teevra18_Logo.png"),
        Path("assets/Teevra18_Logo.png"),
        Path("Teevra18_Logo.png"),
    ]
    for p in candidates:
        if p.exists():
            try:
                return p.read_bytes()
            except Exception:
                pass
    return None

def _logo_html() -> str:
    raw = _load_logo_bytes()
    if not raw:
        # fallback: minimal dot to preserve spacing
        return '<div style="height:10px"></div>'
    b64 = base64.b64encode(raw).decode("ascii")
    return f'<img class="t18-logo" src="data:image/png;base64,{b64}" alt="Teevra18" />'

# ---------- UTIL: auth autodetect (READ-ONLY) ----------
TABLE_CANDIDATES = ["app_users", "users", "account_users", "user", "accounts"]
USER_COLS = ["username", "user_name", "email", "login"]
PASS_COLS = ["password_hash", "password", "pass_bcrypt", "pass_sha256", "pass_md5"]

def _sha256(s: str) -> str: return hashlib.sha256(s.encode("utf-8")).hexdigest()
def _md5(s: str) -> str: return hashlib.md5(s.encode("utf-8")).hexdigest()

def _get_row_for_user(con: sqlite3.Connection, table: str, user_col: str, username: str):
    q = f'SELECT * FROM {table} WHERE LOWER({user_col}) = LOWER(?) LIMIT 1'
    try:
        con.row_factory = sqlite3.Row
        return con.execute(q, (username.strip(),)).fetchone()
    except Exception:
        return None

def _detect_table_and_columns(con: sqlite3.Connection):
    # Return (table, user_col, pass_col) if found, else None
    cur = con.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
    all_tables = {r[0].lower(): r[0] for r in cur.fetchall()}
    for t in TABLE_CANDIDATES:
        if t.lower() in all_tables:
            # Inspect columns
            cols = [r[1].lower() for r in con.execute(f"PRAGMA table_info('{all_tables[t.lower()]}')").fetchall()]
            # r[1] is name; pragma row format: cid, name, type, notnull, dflt_value, pk
            colnames = [r[1] for r in con.execute(f"PRAGMA table_info('{all_tables[t.lower()]}')")]
            colnames = [c for _, c, *_ in con.execute(f"PRAGMA table_info('{all_tables[t.lower()]}')")]
            # Make a quick lowercase set for checks
            cols_lower = set([c.lower() for c in colnames])
            user_match = next((c for c in colnames if c.lower() in USER_COLS), None)
            pass_match = next((c for c in colnames if c.lower() in PASS_COLS), None)
            if user_match and pass_match:
                return all_tables[t.lower()], user_match, pass_match
    # last resort: try any table with a 'username' and a plausible password column
    for tbl_lower, tbl_real in all_tables.items():
        cols = [c for _, c, *_ in con.execute(f"PRAGMA table_info('{tbl_real}')")]
        cols_lower = [c.lower() for c in cols]
        user_match = next((c for c in cols if c.lower() in USER_COLS), None)
        pass_match = next((c for c in cols if c.lower() in PASS_COLS), None)
        if user_match and pass_match:
            return tbl_real, user_match, pass_match
    return None

def _check_password(row, pass_col: str, password: str) -> bool:
    # Try hash types based on column hint
    val = row[pass_col]
    if val is None: return False
    col = pass_col.lower()
    if col in ("password_hash", "pass_sha256"):
        return (str(val) == _sha256(password))
    if col == "pass_md5":
        return (str(val) == _md5(password))
    if col == "pass_bcrypt":
        try:
            import bcrypt
            return bcrypt.checkpw(password.encode("utf-8"), str(val).encode("utf-8"))
        except Exception:
            # bcrypt not installed; we can't verify
            return False
    # plain fallback
    if col == "password":
        return (str(val) == password)
    # Unknown naming—try best-effort cascade
    if str(val) == _sha256(password): return True
    if str(val) == password: return True
    if str(val) == _md5(password): return True
    return False

def verify_existing_credentials(username: str, password: str) -> bool:
    if not DB_PATH.exists(): return False
    try:
        with sqlite3.connect(str(DB_PATH)) as con:
            detected = _detect_table_and_columns(con)
            if not detected:
                return False
            table, user_col, pass_col = detected
            row = _get_row_for_user(con, table, user_col, username)
            if not row:
                return False
            return _check_password(row, pass_col, password)
    except Exception:
        return False

# ---------- UI (pure center, no sidebar) ----------
st.markdown('<div class="t18-center"><div class="t18-card">', unsafe_allow_html=True)

# Centered logo (base64)
st.markdown(_logo_html(), unsafe_allow_html=True)

# Title + subtext
st.markdown('<div class="t18-title"><h2>Login</h2></div>', unsafe_allow_html=True)
st.markdown('<div class="t18-sub">Welcome to Teevra18 — please sign in to continue.</div>', unsafe_allow_html=True)

# Inputs
username = st.text_input("Username", key="t18_user", label_visibility="visible")
password = st.text_input("Password", type="password", key="t18_pass", label_visibility="visible")

# Sign In
if st.button("Sign In", type="primary"):
    if verify_existing_credentials(username, password):
        st.session_state["t18_auth_user"] = username.strip().lower()
        st.rerun()
    else:
        st.error("Invalid username or password.")

# Footer
st.markdown('<div class="t18-foot">© Teevra18 • Local-only • Secure by Design</div>', unsafe_allow_html=True)
st.markdown('</div></div>', unsafe_allow_html=True)

# If already authed, jump to dashboard
if "t18_auth_user" in st.session_state:
    try:
        st.switch_page("C:/teevra18/app/ui/Home_Dashboard.py")
    except Exception:
        st.switch_page("app/ui/Home_Dashboard.py")
# ———————————— your original code ends above ————————————
st.markdown('</div></div>', unsafe_allow_html=True)
