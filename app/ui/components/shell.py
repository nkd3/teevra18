# -*- coding: utf-8 -*-
"""
Teevra18 UI Shell (avatar-only dropdown; CSS-only toggle; same-tab Profile/Logout; logout -> LandingPage)
"""

from pathlib import Path
import base64, json, sqlite3
from datetime import datetime
import streamlit as st

APP_ROOT = Path(r"C:\teevra18")
DATA_DIR = APP_ROOT / "data"
ASSETS_DIR = APP_ROOT / "assets"
PROFILE_JSON = DATA_DIR / "profiles.json"  # {"<username>":{"display_name":"...", "avatar_path":"..."}}

# ========== CSS ==========
_CSS = """
<style>
:root { color-scheme: dark; --t18-toolbar-offset: 3.8rem; }
body, .stApp { background:#0E1117; }
.block-container { padding-top: calc(var(--t18-toolbar-offset) + .5rem) !important; }

/* Top bar BELOW Streamlit toolbar */
.t18-topbar{position:sticky;top:var(--t18-toolbar-offset);z-index:1000;background:#0E1117;
  border-bottom:1px solid #22262E;padding:.5rem .6rem;}
.t18-topgrid{display:grid;grid-template-columns:200px 1fr 420px 220px;gap:.6rem;align-items:center;}
@media(max-width:1280px){ .t18-topgrid{grid-template-columns:160px 1fr;row-gap:.6rem;} .t18-status{justify-items:start;} }

/* Logo-only */
.t18-logo{display:inline-flex;align-items:center;gap:.5rem;}
.t18-logo img{width:44px;height:44px;border-radius:50%;border:1px solid #2a2f3a;object-fit:cover;}

/* Primary nav */
.t18-nav{display:inline-flex;gap:.6rem;flex-wrap:wrap;}
.t18-pill{padding:.38rem .8rem;border-radius:999px;border:1px solid #283042;background:#121621;color:#cdd3df;font-size:.88rem;cursor:pointer;}
.t18-pill:hover{border-color:#334059;background:#151b27;}
.t18-pill-active{color:#fff;border-color:#3b82f6;box-shadow:inset 0 0 0 1px #3b82f6;}

/* Status cluster */
.t18-status{display:grid;gap:.25rem;justify-items:end;}
.t18-row{display:inline-flex;gap:.5rem;align-items:center;}
.t18-kv{color:#a7b0bf;font-size:.82rem;}
.t18-chip{border:1px solid #283042;background:#121621;color:#d7dbe6;padding:.1rem .5rem;border-radius:6px;font-size:.8rem;}
.t18-refresh{padding:.12rem .55rem;border-radius:6px;border:1px solid #334059;background:#0f1521;color:#cfe1ff;cursor:pointer;}
.t18-light{width:10px;height:10px;border-radius:50%;border:1px solid #2a2f3a;}
.t18-ok{background:#22c55e;} .t18-bad{background:#ef4444;} .t18-amb{background:#f59e0b;}

/* Secondary sub-nav (radio -> tight pills) */
.t18-subnav-wrap{margin-top:.6rem;}
.t18-subnav .stRadio > div[role="radiogroup"]{display:flex;gap:.35rem;flex-wrap:wrap;justify-content:flex-start;}
.t18-subnav .stRadio div[role="radio"]{border:1px solid #283042;background:#121621;color:#cdd3df;
  padding:.28rem .7rem;border-radius:8px;cursor:pointer;}
.t18-subnav .stRadio div[role="radio"][aria-checked="true"]{color:#fff;border-color:#a16207;box-shadow:inset 0 0 0 1px #a16207;}
.t18-subnav .stRadio svg{display:none;} .t18-subnav .stRadio input{display:none !important;} .t18-subnav .stRadio label{margin:0;}

/* Generic cards + KPI bubbles */
.t18-section{margin-top:.8rem;}
.t18-card{border:1px solid #283042;background:#0f1420;border-radius:10px;padding:.9rem;}
.t18-kpi-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:.8rem;}
@media(max-width:1600px){.t18-kpi-grid{grid-template-columns:repeat(3,1fr);} }
@media(max-width:1100px){.t18-kpi-grid{grid-template-columns:repeat(2,1fr);} }
.t18-kpi{height:110px;border:1px solid #283042;background:#101827;border-radius:12px;padding:.8rem;display:flex;flex-direction:column;justify-content:space-between;}
.t18-kpi h5{margin:0;color:#cfd6e6;font-weight:600;font-size:.9rem;}
.t18-kpi .v{font-size:1.6rem;color:#ffffff;font-weight:800;}

/* Workspaces */
.t18-row-7030{display:grid;grid-template-columns:7fr 3fr;gap:.9rem;}
@media(max-width:1280px){.t18-row-7030{grid-template-columns:1fr;}}
.t18-wsA,.t18-wsB{min-height:320px;}
.t18-wsFull{min-height:280px;}

/* User area (avatar dropdown) */
.t18-userwrap{display:flex;justify-content:flex-end;align-items:center;width:100%;}
.t18-ava{position:relative;display:inline-block;}
.t18-avaimg{
  width:38px;height:38px;border-radius:50%;border:1px solid #2a2f3a;overflow:hidden;cursor:pointer;
  background-size:cover;background-position:center;background-repeat:no-repeat;
  display:flex;align-items:center;justify-content:center;
}
.t18-avaimg.noimg{
  background:radial-gradient(100% 100% at 50% 0%, #1e293b 0%, #0b1220 100%);
  color:#cfe1ff;font-weight:700;font-size:14px;
}
/* checkbox toggle */
.t18-ava .ava-toggle{display:none;}
.t18-menu{
  position:absolute; right:0; top:46px; min-width:210px;
  background:#0F1522; border:1px solid #2b3140; border-radius:10px;
  box-shadow:0 8px 20px rgba(0,0,0,.35); padding:.45rem; display:none; z-index:10000;
  font-size:10.5px; color:#d7dbe6;
}
.t18-menu .hd{opacity:.85; padding:.2rem .35rem .45rem .35rem;}
.t18-menu .item{padding:.35rem .45rem;border-radius:7px;cursor:pointer;}
.t18-menu .item:hover{background:#162032;}
/* show menu when checked */
.t18-ava .ava-toggle:checked ~ .t18-menu{display:block;}
</style>

<script>
/* Keep spacing below Streamlit toolbar (no dropdown JS here) */
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

# ---------- config/db ----------
def _read_config():
    for p in [APP_ROOT / "teevra18.config.json", Path("teevra18.config.json")]:
        if p.exists():
            try: return json.loads(p.read_text(encoding="utf-8"))
            except Exception: return {}
    return {}

def _db_path():
    cfg=_read_config()
    return Path(cfg.get("ops_db_path", str(DATA_DIR / "teevra18.db")))

def _conn():
    p=_db_path()
    return sqlite3.connect(str(p)) if p.exists() else None

def get_system_health():
    cfg=_read_config()
    checks={
        "Database": _db_path().exists(),
        "DhanHQ": bool((cfg.get("dhan",{}) or {}).get("access_token","")),
        "Telegram": bool((cfg.get("telegram",{}) or {}).get("bot_token","")),
        "Parquet": Path(cfg.get("parquet_dir", str(APP_ROOT / "parquet"))).exists(),
    }
    return {"checks":checks,"summary":{"green":sum(checks.values()),"amber":0,"red":list(checks.values()).count(False)}}

# ---------- profiles ----------
def _profiles_load() -> dict:
    try:
        if PROFILE_JSON.exists():
            return json.loads(PROFILE_JSON.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def get_display_name_and_avatar(username: str):
    prof = _profiles_load().get((username or "").strip().lower(), {})
    disp = prof.get("display_name", username or "")
    av   = prof.get("avatar_path")
    return disp, av

def _avatar_b64_css_bg(path: Path|str|None) -> str|None:
    p = Path(path) if path else None
    if p and p.exists():
        b64 = base64.b64encode(p.read_bytes()).decode("ascii")
        mime = "png" if p.suffix.lower()==".png" else "jpeg"
        return f"url('data:image/{mime};base64,{b64}')"
    return None

def _logo_img() -> str:
    for p in [ASSETS_DIR / "Teevra18_Logo.png", ASSETS_DIR / "Teevra18_Logo.ico"]:
        if p.exists():
            b64 = base64.b64encode(p.read_bytes()).decode("ascii")
            mime = "png" if p.suffix.lower()==".png" else "x-icon"
            return f'<img src="data:image/{mime};base64,{b64}" alt="logo"/>'
    return '<div style="width:44px;height:44px;border-radius:50%;border:1px solid #2a2f3a;"></div>'

# ---------- data fetchers ----------
def _df(conn, q, args=()):
    import pandas as pd
    cur=conn.cursor(); cur.execute(q, args)
    cols=[d[0] for d in cur.description]; rows=cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

def fetch_kpis():
    today=datetime.now().date()
    k={"pl_today":"₹0","open_risk":"₹0","net_positions":"0","signals_today":"0","hit_rate_7d":"0%","max_dd_30d":"₹0"}
    conn=_conn()
    if not conn: return k
    try:
        try:
            row=_df(conn, """
                SELECT COALESCE(pl_today,0), COALESCE(open_risk,0), COALESCE(net_positions,0),
                       COALESCE(hit_rate_7d,0), COALESCE(max_dd_30d,0)
                FROM kpi_daily WHERE date=? LIMIT 1
            """,(str(today),)).iloc[0]
            k["pl_today"]=f"₹{int(row[0])}"; k["open_risk"]=f"₹{int(row[1])}"
            k["net_positions"]=str(int(row[2])); k["hit_rate_7d"]=f"{float(row[3]):.1f}%"
            k["max_dd_30d"]=f"₹{int(row[4])}"
        except Exception: pass
        try:
            cnt=_df(conn,"SELECT COUNT(*) AS c FROM signals WHERE date(created_at)=date(?)",(str(today),)).iloc[0,0]
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
          SELECT time(created_at) AS Time, symbol AS Symbol, side AS Side, qty AS Qty, price AS Price, status AS Status
          FROM paper_orders ORDER BY created_at DESC LIMIT ?
        """,(limit,))
    except Exception: return None
    finally:
        conn.close()

def fetch_signals_df(limit=25):
    conn=_conn()
    if not conn: return None
    try:
        return _df(conn, """
          SELECT time(created_at) AS Time, symbol AS Symbol, action AS Action, reason AS Reason
          FROM signals ORDER BY created_at DESC LIMIT ?
        """,(limit,))
    except Exception: return None
    finally:
        conn.close()

def fetch_positions_df():
    conn=_conn()
    if not conn: return None
    try:
        return _df(conn, """
          SELECT symbol AS Symbol, qty_open AS Qty, avg_price AS AvgPrice, mtM AS MtM, risk AS Risk
          FROM positions ORDER BY abs(mtM) DESC
        """)
    except Exception: return None
    finally:
        conn.close()

# ---------- UI ----------
def render_topbar(active_primary:str):
    st.markdown(_CSS, unsafe_allow_html=True)

    # --- helper: clear the action flag so it doesn't "follow" to the next page ---
    def _clear_action_param():
        try:
            if "t18_action" in st.query_params:
                del st.query_params["t18_action"]
        except Exception:
            pass

    # Handle avatar menu actions via query params (NO layout changes below)
    act = st.query_params.get("t18_action")
    if isinstance(act, list):
        act = act[0]

    if act == "profile":
        _clear_action_param()
        # close menu (if you track it in session elsewhere)
        st.session_state["t18_menu_open"] = False
        try:
            st.switch_page("pages/My_Profile.py")
        except Exception:
            # Fallback: push a go param and rerun (same tab)
            st.query_params["_go"] = "My_Profile.py"
            st.rerun()
        st.stop()

    elif act == "logout":
        _clear_action_param()
        st.session_state["t18_menu_open"] = False
        for k in ["auth_user","t18_auth_user","user"]:
            if k in st.session_state:
                del st.session_state[k]
        # Prefer server-side nav to avoid blank new tabs
        try:
            st.switch_page("ui/LandingPage.py")
        except Exception:
            # As a safe fallback, clear all params and rerun to app root
            try:
                st.query_params.clear()
            except Exception:
                pass
            st.rerun()
        st.stop()

    h=get_system_health(); checks,summary=h["checks"],h["summary"]

    auth = st.session_state.get("auth_user") or {"name": st.session_state.get("t18_auth_user","")}
    username = (auth.get("name") or "").strip()
    display_name, avatar_path = get_display_name_and_avatar(username)

    # Build avatar node (image bg vs initial)
    bg = _avatar_b64_css_bg(avatar_path)
    if bg:
        ava_label = f'<label for="t18avacb" class="t18-avaimg" style="background-image:{bg}"></label>'
    else:
        initial = (username[:1].upper() or "U") if username else "U"
        ava_label = f'<label for="t18avacb" class="t18-avaimg noimg">{initial}</label>'

    with st.container(border=False):
        st.markdown('<div class="t18-topbar"><div class="t18-topgrid">', unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns([0.8, 3.2, 2.2, 1.2], gap="small")

        with c1:
            st.markdown(f'<div class="t18-logo">{_logo_img()}</div>', unsafe_allow_html=True)

        with c2:
            nav=[("DASHBOARD","Dashboard_Shell.py"),("STRATEGY LAB","Strategy_Lab.py"),
                 ("TRADING","Trading.py"),("PORTFOLIO","Portfolio.py"),
                 ("REPORTS","Reports.py"),("CONTROL PANEL","ControlPanel.py")]
            html=['<div class="t18-nav">']
            for label,target in nav:
                cls="t18-pill t18-pill-active" if label==active_primary else "t18-pill"
                html.append(f"<span class='{cls}' onclick=\"window.location.search='?_go={target}'\">{label}</span>")
            html.append("</div>")
            st.markdown("".join(html), unsafe_allow_html=True)

        with c3:
            st.markdown(f"""
            <div class="t18-status">
              <div class="t18-row">
                <div class="t18-kv">System Status</div>
                <div class="t18-light {'t18-ok' if checks['Database'] else 't18-bad'}" title="Database"></div>
                <div class="t18-light {'t18-ok' if checks['DhanHQ'] else 't18-bad'}" title="DhanHQ"></div>
                <div class="t18-light {'t18-ok' if checks['Telegram'] else 't18-bad'}" title="Telegram"></div>
                <div class="t18-light {'t18-ok' if checks['Parquet'] else 't18-bad'}" title="Parquet"></div>
                <button class="t18-refresh" onclick="window.location.reload()">Refresh</button>
              </div>
              <div class="t18-row"><div class="t18-kv">Summary</div>
                <div class="t18-chip">🟢 {summary['green']} · 🟠 {summary['amber']} · 🔴 {summary['red']}</div>
              </div>
            </div>
            """, unsafe_allow_html=True)

        with c4:
            # CSS-only dropdown: hidden checkbox + label (avatar) + menu.
            # NOTE: unchanged — still sets t18_action via URL param in same tab.
            avatar_html = """
<div class="t18-userwrap">
  <div class="t18-ava">
    <input id="t18avacb" class="ava-toggle" type="checkbox" />
    __AVA_LABEL__
    <div class="t18-menu">
      <div class="hd">You're logged in as <b>__USERNAME__</b></div>
      <div class="item" onclick="(function(){
        const p=new URLSearchParams(window.location.search);
        p.set('t18_action','profile');
        window.location.search=p.toString();
      })()">My Profile</div>
      <div class="item" onclick="(function(){
        const p=new URLSearchParams(window.location.search);
        p.set('t18_action','logout');
        window.location.search=p.toString();
      })()">Logout</div>
    </div>
  </div>
</div>
"""
            avatar_html = (avatar_html
                           .replace("__AVA_LABEL__", ava_label)
                           .replace("__USERNAME__", (username or "—")))
            st.markdown(avatar_html, unsafe_allow_html=True)

        st.markdown('</div></div>', unsafe_allow_html=True)

    # Primary nav routing (query param → switch_page) — unchanged
    tgt = st.query_params.get("_go", None)
    if isinstance(tgt, list):
        tgt = tgt[0]
    if tgt:
        try:
            st.switch_page(f"pages/{tgt}")
        except Exception:
            pass

def render_subnav_choice(default="Overview"):
    labels = ["Overview","Orders","Signals","Health and Ops"]
    qp = st.query_params.get("sub", default)
    if isinstance(qp, list): qp = qp[0]
    if qp not in labels: qp = default
    sel_index = labels.index(qp)
    st.markdown('<div class="t18-subnav-wrap"><div class="t18-subnav">', unsafe_allow_html=True)
    selection = st.radio("", options=labels, index=sel_index, horizontal=True,
                         label_visibility="collapsed", key="t18_sub_radio")
    st.markdown('</div></div>', unsafe_allow_html=True)
    if st.query_params.get("sub") != selection:
        st.query_params["sub"] = selection
    return selection
