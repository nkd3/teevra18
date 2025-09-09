# -*- coding: utf-8 -*-
r"""
Teevra18 UI Shell (profile avatar + dropdown, in-page sub-nav)
Place at: C:\teevra18\app\ui\components\shell.py
"""

from pathlib import Path
import base64, json, sqlite3
from datetime import datetime
import streamlit as st

__all__ = [
    "render_topbar",
    "render_subnav_choice",
    "fetch_kpis",
    "fetch_orders_df",
    "fetch_signals_df",
    "fetch_positions_df",
]

# ---------------- Back-compat import alias ----------------
import sys as _sys
if "components.shell" not in _sys.modules:
    _sys.modules["components.shell"] = _sys.modules[__name__]

# ========== Paths ==========
THIS_FILE = Path(__file__).resolve()
UI_ROOT   = THIS_FILE.parents[1]          # C:\teevra18\app\ui
APP_ROOT  = UI_ROOT.parents[1]            # C:\teevra18
DATA_DIR  = APP_ROOT / "data"
ASSETS_DIR= APP_ROOT / "assets"
PROFILE_JSON = DATA_DIR / "profiles.json"

# ========= Styles =========
_CSS = """
<style>
:root { color-scheme: dark; --t18-toolbar-offset: 3.8rem; }
body, .stApp { background:#0E1117; }
.block-container { padding-top: calc(var(--t18-toolbar-offset) + .5rem) !important; }

/* Top bar skeleton */
.t18-topbar{position:sticky;top:var(--t18-toolbar-offset);z-index:1000;background:#0E1117;
  border-bottom:1px solid #22262E;padding:.5rem .6rem;}
/* Grid: Logo | NAV (flex-grow) | Health | User  */
.t18-topgrid{display:grid;grid-template-columns:200px 1fr 460px 210px;gap:.6rem;align-items:center;}
@media(max-width:1400px){ .t18-topgrid{grid-template-columns:160px 1fr 420px 200px;} }
@media(max-width:1280px){ .t18-topgrid{grid-template-columns:140px 1fr 380px 180px;} }

/* Logo */
.t18-logo{display:inline-flex;align-items:center;gap:.5rem;}
.t18-logo img{width:44px;height:44px;border-radius:50%;border:1px solid #2a2f3a;object-fit:cover;}

/* PRIMARY NAV — column row (guaranteed horizontal) */
.t18-nav{display:block; white-space:nowrap; overflow-x:auto; overflow-y:hidden; }
.t18-pill-active{
  display:inline-flex;align-items:center;justify-content:center;
  padding:.26rem .60rem;border-radius:999px;margin-right:.3rem;
  border:1px solid #3b82f6; box-shadow:inset 0 0 0 1px #3b82f6; color:#fff; background:#121621;
  font-size:.78rem;
}
/* page_link wrappers inside the nav row */
.t18-nav [data-testid="stPageLink"]{
  display:inline-flex !important;
  width:auto !important;
  margin:0 .3rem 0 0 !important;
  padding:0 !important;
  vertical-align:middle;
}
.t18-nav [data-testid="stPageLink"] a{
  display:inline-flex;align-items:center;justify-content:center;
  padding:.26rem .60rem;border-radius:999px;
  border:1px solid #283042; background:#121621; color:#cdd3df; font-size:.78rem;
  text-decoration:none; white-space:nowrap;
}
.t18-nav [data-testid="stPageLink"] a:hover{border-color:#334059;background:#151b27;color:#ffffff;}
.t18-pill-disabled{
  display:inline-flex;align-items:center;justify-content:center;
  padding:.26rem .60rem;border-radius:999px;margin-right:.3rem;
  border:1px dashed #283042; color:#a5adbd; background:#121621; opacity:.6;
  font-size:.78rem;
}

/* INLINE Health cluster (System + Summary on same line) */
.t18-health{display:flex;align-items:center;gap:.55rem;white-space:nowrap;justify-content:flex-end;}
.t18-kv{color:#a7b0bf;font-size:.8rem;}
.t18-chip{border:1px solid #283042;background:#121621;color:#d7dbe6;padding:.12rem .48rem;border-radius:6px;font-size:.8rem;}
.t18-refresh{padding:.12rem .55rem;border-radius:6px;border:1px solid #334059;background:#0f1521;color:#cfe1ff;cursor:pointer;font-size:.8rem;}
.t18-light{width:9px;height:9px;border-radius:50%;border:1px solid #2a2f3a;}
.t18-ok{background:#22c55e;} .t18-bad{background:#ef4444;} .t18-amb{background:#f59e0b;}

/* Secondary sub-nav */
.t18-subnav-wrap{margin-top:.6rem;}
.t18-subnav .stRadio > div[role="radiogroup"]{display:flex;gap:.35rem;flex-wrap:wrap;justify-content:flex-start;}
.t18-subnav .stRadio div[role="radio"]{border:1px solid #283042;background:#121621;color:#cdd3df;
  padding:.28rem .7rem;border-radius:8px;cursor:pointer;}
.t18-subnav .stRadio div[role="radio"][aria-checked="true"]{color:#fff;border-color:#a16207;box-shadow:inset 0 0 0 1px #a16207;}
.t18-subnav .stRadio svg{display:none;} .t18-subnav .stRadio input{display:none !important;} .t18-subnav .stRadio label{margin:0;}

/* Misc */
.t18-section{margin-top:.8rem;}
.t18-card{border:1px solid #283042;background:#0f1420;border-radius:10px;padding:.9rem;}
.t18-kpi-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:.8rem;}
@media(max-width:1600px){.t18-kpi-grid{grid-template-columns:repeat(3,1fr);} }
@media(max-width:1100px){.t18-kpi-grid{grid-template-columns:repeat(2,1fr);} }
.t18-kpi{height:110px;border:1px solid #283042;background:#101827;border-radius:12px;padding:.8rem;display:flex;flex-direction:column;justify-content:space-between;}
.t18-kpi h5{margin:0;color:#cfd6e6;font-weight:600;font-size:.9rem;}
.t18-kpi .v{font-size:1.6rem;color:#ffffff;font-weight:800;}

.t18-row-7030{display:grid;grid-template-columns:7fr 3fr;gap:.9rem;}
@media(max-width:1280px){.t18-row-7030{grid-template-columns:1fr;}}
.t18-wsA,.t18-wsB{min-height:320px;}
.t18-wsFull{min-height:280px;}

.t18-userwrap{display:flex;justify-content:flex-end;align-items:center;gap:.55rem;}
.t18-ava{width:38px;height:38px;border-radius:50%;border:1px solid #2a2f3a;background:#0d111a;overflow:hidden;}
.t18-ava img{width:100%;height:100%;object-fit:cover;}
.t18-username{color:#e7eaf3;font-weight:600;}
</style>

<script>
/* keep toolbar offset correct */
(function(){
  function upd(){
    try{
      const tb=document.querySelector('[data-testid="stToolbar"]')||document.querySelector('header[tabindex="-1"]');
      const h=tb?Math.max(60,tb.getBoundingClientRect().height):60;
      document.documentElement.style.setProperty('--t18-toolbar-offset',(h+6)+'px');
    }catch(e){}
  }
  upd(); new MutationObserver(upd).observe(document.body,{childList:true,subtree:true});
  addEventListener('resize',upd);
})();
</script>
"""

# ========= Config / DB helpers =========
def _read_config():
    for p in [APP_ROOT / "teevra18.config.json", Path("teevra18.config.json")]:
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                return {}
    return {}

def _db_path():
    cfg = _read_config()
    return Path(cfg.get("ops_db_path", str(DATA_DIR / "teevra18.db")))

def _conn():
    p = _db_path()
    return sqlite3.connect(str(p)) if p.exists() else None

def get_system_health():
    cfg = _read_config()
    checks = {
        "Database": _db_path().exists(),
        "DhanHQ": bool((cfg.get("dhan", {}) or {}).get("access_token", "")),
        "Telegram": bool((cfg.get("telegram", {}) or {}).get("bot_token", "")),
        "Parquet": Path(cfg.get("parquet_dir", str(APP_ROOT / "parquet"))).exists(),
    }
    return {"checks": checks, "summary": {"green": sum(checks.values()), "amber": 0, "red": list(checks.values()).count(False)}}

# ========= Profile helpers =========
def _profiles_load() -> dict:
    try:
        if PROFILE_JSON.exists():
            return json.loads(PROFILE_JSON.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _profiles_save(profiles: dict) -> None:
    PROFILE_JSON.parent.mkdir(parents=True, exist_ok=True)
    PROFILE_JSON.write_text(json.dumps(profiles, indent=2), encoding="utf-8")

def get_display_name_and_avatar(username: str):
    prof = _profiles_load().get((username or "").lower().strip(), {})
    disp = prof.get("display_name", username or "<user>")
    av   = prof.get("avatar_path")
    return disp, av

def _avatar_img_tag(path: Path | None) -> str:
    if path and Path(path).exists():
        b64 = base64.b64encode(Path(path).read_bytes()).decode("ascii")
        mime = "png" if str(path).lower().endswith(".png") else "jpeg"
        return f'<img src="data:image/{mime};base64,{b64}" alt="avatar"/>'
    return ""

def _logo_img() -> str:
    for p in [ASSETS_DIR / "Teevra18_Logo.png", ASSETS_DIR / "Teevra18_Logo.ico"]:
        if p.exists():
            b64 = base64.b64encode(p.read_bytes()).decode("ascii")
            mime = "png" if p.suffix.lower() == ".png" else "x-icon"
            return f'<img src="data:image/{mime};base64,{b64}" alt="logo"/>'
    return '<div style="width:44px;height:44px;border:1px solid #2a2f3a;border-radius:50%;"></div>'

# ========= Data fetchers =========
def _df(conn, q, args=()):
    import pandas as pd
    cur = conn.cursor(); cur.execute(q, args)
    cols = [d[0] for d in cur.description]; rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

def fetch_kpis():
    today = datetime.now().date()
    k = {"pl_today":"₹0","open_risk":"₹0","net_positions":"0","signals_today":"0","hit_rate_7d":"0%","max_dd_30d":"₹0"}
    conn = _conn()
    if not conn: return k
    try:
        try:
            row = _df(conn, """
                SELECT COALESCE(pl_today,0), COALESCE(open_risk,0), COALESCE(net_positions,0),
                       COALESCE(hit_rate_7d,0), COALESCE(max_dd_30d,0)
                FROM kpi_daily WHERE date=? LIMIT 1
            """, (str(today),)).iloc[0]
            k["pl_today"]=f"₹{int(row[0])}"; k["open_risk"]=f"₹{int(row[1])}"
            k["net_positions"]=str(int(row[2])); k["hit_rate_7d"]=f"{float(row[3]):.1f}%"
            k["max_dd_30d"]=f"₹{int(row[4])}"
        except Exception: pass
        try:
            cnt = _df(conn, "SELECT COUNT(*) AS c FROM signals WHERE date(created_at)=date(?)", (str(today),)).iloc[0,0]
            k["signals_today"]=str(int(cnt))
        except Exception: pass
    finally:
        conn.close()
    return k

def fetch_orders_df(limit=25):
    conn=_conn()
    if not conn: return None
    try:
        return _df(conn, """
          SELECT time(created_at) AS Time, symbol AS Symbol, side AS Side, qty AS Qty, price As Price, status As Status
          FROM paper_orders ORDER BY created_at DESC LIMIT ?
        """,(limit,))
    except Exception: return None
    finally: conn.close()

def fetch_signals_df(limit=25):
    conn=_conn()
    if not conn: return None
    try:
        return _df(conn, """
          SELECT time(created_at) AS Time, symbol AS Symbol, action As Action, reason As Reason
          FROM signals ORDER BY created_at DESC LIMIT ?
        """,(limit,))
    except Exception: return None
    finally: conn.close()

def fetch_positions_df():
    conn=_conn()
    if not conn: return None
    try:
        return _df(conn, """
          SELECT symbol AS Symbol, qty_open AS Qty, avg_price As AvgPrice, mtM AS MtM, risk As Risk
          FROM positions ORDER BY abs(mtM) DESC
        """)
    except Exception: return None
    finally: conn.close()

# ========= Page resolver for primary nav =========
def _first_existing_page(candidates:list[str]) -> str | None:
    """Return the first relative path under UI_ROOT that exists; else None."""
    for rel in candidates:
        if (UI_ROOT / rel).exists():
            return rel
    return None

def _nav_targets() -> list[tuple[str, str | None]]:
    """Return (LABEL, rel_path or None). Prefers Dashboard_Shell explicitly."""
    return [
        ("DASHBOARD",     _first_existing_page([
            "pages/Dashboard_Shell.py",  # <-- prefer this
            "Dashboard_Shell.py",
            "pages/Home_Dashboard.py",
            "Home_Dashboard.py",
        ])),
        ("STRATEGY LAB",  _first_existing_page([
            "pages/Strategy_Lab.py",
            "Strategy_Lab.py",
        ])),
        ("TRADING",       _first_existing_page([
            "pages/Live_Trading.py",
            "pages/Trading.py",
            "Live_Trading.py",
            "Trading.py",
        ])),
        ("PORTFOLIO",     _first_existing_page([
            "pages/Portfolio.py",
            "Portfolio.py",
        ])),
        ("REPORTS",       _first_existing_page([
            "pages/Reports.py",
            "Reports.py",
        ])),
        ("CONTROL PANEL", _first_existing_page([
            "pages/Control_Panel.py",
        ])),
    ]

# ========= Renderers =========
def render_topbar(active_primary: str):
    """
    Call this AFTER your page's st.set_page_config(...).
    Primary nav uses native st.page_link (same-tab). We render them in a single 6-column row,
    and also force inline via CSS so they never stack vertically.
    """
    # Logout redirect
    if st.session_state.pop("__t18_do_logout", False):
        for k in ["auth_user","t18_auth_user","user"]:
            if k in st.session_state: del st.session_state[k]
        try:
            st.switch_page("ui/LandingPage.py")
        except Exception:
            st.markdown("<script>window.location.replace('/');</script>", unsafe_allow_html=True)
        st.stop()

    st.markdown(_CSS, unsafe_allow_html=True)

    h = get_system_health()
    checks, summary = h["checks"], h["summary"]

    auth = st.session_state.get("auth_user") or {"name": st.session_state.get("t18_auth_user","<user>")}
    username = (auth.get("name") or "<user>")
    display_name, avatar_path = get_display_name_and_avatar(username)

    with st.container(border=False):
        st.markdown('<div class="t18-topbar"><div class="t18-topgrid">', unsafe_allow_html=True)
        c1,c2,c3,c4 = st.columns([0.8,3.8,2.0,1.2], gap="small")

        # Logo
        with c1:
            st.markdown(f'<div class="t18-logo">{_logo_img()}</div>', unsafe_allow_html=True)

        # Primary nav — one row of 6 equal columns (no wrap)
        with c2:
            nav = _nav_targets()
            st.markdown('<div class="t18-nav">', unsafe_allow_html=True)
            cols = st.columns(len(nav), gap="small")
            for i, (label, target) in enumerate(nav):
                with cols[i]:
                    if label == active_primary:
                        st.markdown(f"<span class='t18-pill-active'>{label}</span>", unsafe_allow_html=True)
                    else:
                        if target:
                            st.page_link(target, label=label)
                        else:
                            st.markdown(f"<span class='t18-pill-disabled'>{label}</span>", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # System + Summary inline (unchanged)
        with c3:
            st.markdown(f"""
            <div class="t18-health">
                <span class="t18-kv">System</span>
                <span class="t18-light {'t18-ok' if checks['Database'] else 't18-bad'}" title="Database"></span>
                <span class="t18-light {'t18-ok' if checks['DhanHQ'] else 't18-bad'}" title="DhanHQ"></span>
                <span class="t18-light {'t18-ok' if checks['Telegram'] else 't18-bad'}" title="Telegram"></span>
                <span class="t18-light {'t18-ok' if checks['Parquet'] else 't18-bad'}" title="Parquet"></span>
                <button class="t18-refresh" onclick="window.location.reload()">Refresh</button>
                <span class="t18-kv">Summary</span>
                <span class="t18-chip">🟢 {summary['green']} · 🟠 {summary['amber']} · 🔴 {summary['red']}</span>
            </div>
            """, unsafe_allow_html=True)

        # User area
        with c4:
            st.markdown('<div class="t18-userwrap">', unsafe_allow_html=True)
            st.markdown(f'<div class="t18-ava">{_avatar_img_tag(avatar_path)}</div>', unsafe_allow_html=True)

            if hasattr(st,"popover"):
                with st.popover(display_name or "<user>"):
                    if st.button("My Profile", use_container_width=True, key="t18_profile"):
                        try:
                            st.switch_page("pages/My_Profile.py")
                        except Exception:
                            st.query_params["_go"]="pages/My_Profile.py"; st.rerun()
                    if st.button("Logout", use_container_width=True, key="t18_logout"):
                        st.session_state["__t18_do_logout"]=True
            else:
                col1,col2=st.columns([1,1])
                if col1.button("My Profile", key="t18_profile2"):
                    try:
                        st.switch_page("pages/My_Profile.py")
                    except Exception:
                        st.query_params["_go"]="pages/My_Profile.py"; st.rerun()
                if col2.button("Logout", key="t18_logout2"):
                    st.session_state["__t18_do_logout"]=True
            st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('</div></div>', unsafe_allow_html=True)

    # Router is kept (harmless) for other flows
    tgt = st.query_params.get("_go", None)
    if isinstance(tgt, list):
        tgt = tgt[0] if tgt else None
    if tgt:
        try:
            st.switch_page(tgt if tgt.startswith("pages/") else f"pages/{tgt}")
        except Exception:
            try:
                st.switch_page(tgt)
            except Exception:
                pass

def render_subnav_choice(default="Overview"):
    """Horizontal radio that looks like pills; updates SAME page; persists to ?sub=."""
    labels = ["Overview","Orders","Signals","Health and Ops"]
    qp = st.query_params.get("sub", default)
    if isinstance(qp, list): qp = qp[0]
    if qp not in labels: qp = default
    sel_index = labels.index(qp)

    st.markdown('<div class="t18-subnav-wrap"><div class="t18-subnav">', unsafe_allow_html=True)
    selection = st.radio(
        "Sub navigation",
        options=labels,
        index=sel_index,
        horizontal=True,
        label_visibility="collapsed",
        key="t18_sub_radio"
    )
    st.markdown('</div></div>', unsafe_allow_html=True)

    if st.query_params.get("sub") != selection:
        st.query_params["sub"] = selection
    return selection
