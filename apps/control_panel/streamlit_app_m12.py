# --- BOOTSTRAP: ensure project root is on sys.path ---
import sys
from pathlib import Path
PROJECT_ROOT = Path(r"C:\teevra18")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# -----------------------------------------------------

import json
from pathlib import Path as P
import streamlit as st
import pandas as pd
import sqlite3

from core.config_store import ConfigStore
from core.policies import enforce_core_limits

# ---------- Robust config loader (BOM-safe, empty-safe, error-shows-in-UI) ----------
def load_cfg_safe(path: str):
    p = P(path)
    if not p.exists():
        st.error(f"Config not found at {path}. Create it and reload.")
        st.stop()
    text = p.read_text(encoding="utf-8-sig")  # tolerates BOM; fine for plain UTF-8
    if not text.strip():
        st.error(f"Config at {path} is empty. Please rewrite it.")
        st.stop()
    try:
        return json.loads(text)
    except Exception as e:
        st.error(f"Config JSON invalid: {e}\n\nFirst 200 chars:\n{text[:200]}")
        st.stop()

# Load main config (safe)
CFG = load_cfg_safe(r"C:\teevra18\teevra18.config.json")
STORE = ConfigStore(CFG["db_path"])

st.set_page_config(page_title="Teevra18 — M12 Control Panel", page_icon="⚙️", layout="wide")

# Header with logo (safe default if config key missing)
logo_path = CFG.get("logo_png", r"C:\teevra18\assets\Teevra18_Logo.png")
cols = st.columns([1, 6])
with cols[0]:
    if logo_path and P(logo_path).exists():
        st.image(logo_path, use_column_width=True)
with cols[1]:
    st.title("Teevra18 — M12 Control Panel")
    st.caption("Strategy Lab • Policies • Promotion • Alerts • Health • Export/Import")

st.divider()

# -------------------------- OPS BAR: breaker + KPI tiles + quick links --------------------------
def _table_exists(db_path: str, name: str) -> bool:
    con = sqlite3.connect(db_path); cur = con.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,))
    ok = cur.fetchone() is not None
    con.close()
    return ok

def _ensure_breaker(db_path: str) -> str:
    con = sqlite3.connect(db_path); cur = con.cursor()
    # breaker state
    cur.execute("""CREATE TABLE IF NOT EXISTS breaker_state(
        state TEXT NOT NULL DEFAULT 'RUNNING',
        updated_at TEXT DEFAULT (datetime('now'))
    )""")
    # breaker audit log
    cur.execute("""CREATE TABLE IF NOT EXISTS breaker_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        new_state TEXT NOT NULL,
        who TEXT DEFAULT 'ui',
        note TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now'))
    )""")
    # runner heartbeat
    cur.execute("""CREATE TABLE IF NOT EXISTS runner_heartbeat(
        runner TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        info TEXT DEFAULT '',
        updated_at TEXT DEFAULT (datetime('now'))
    )""")
    con.commit()
    cur.execute("SELECT state FROM breaker_state LIMIT 1")
    row = cur.fetchone()
    if not row:
        cur.execute("INSERT INTO breaker_state(state) VALUES('RUNNING')")
        con.commit()
        state = "RUNNING"
    else:
        state = row[0]
    con.close()
    return state

def _set_breaker(db_path: str, state: str, who: str = "ui", note: str = ""):
    con = sqlite3.connect(db_path); cur = con.cursor()
    # Update or insert state
    cur.execute("UPDATE breaker_state SET state=?, updated_at=datetime('now')", (state,))
    if cur.rowcount == 0:
        cur.execute("INSERT INTO breaker_state(state) VALUES(?)", (state,))
    # Log the change
    cur.execute("INSERT INTO breaker_log(new_state, who, note) VALUES (?, ?, ?)", (state, who, note))
    con.commit(); con.close()


# Breaker row + links
c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 2])
with c1:
    current_state = _ensure_breaker(CFG["db_path"])
    st.metric("Breaker", current_state)
with c2:
    if st.button("Start/Resume", use_container_width=True):
        _set_breaker(CFG["db_path"], "RUNNING"); st.rerun()
with c3:
    if st.button("Pause", use_container_width=True):
        _set_breaker(CFG["db_path"], "PAUSED"); st.rerun()
with c4:
    if st.button("Panic", use_container_width=True):
        _set_breaker(CFG["db_path"], "PANIC"); st.rerun()
with c5:
    links = CFG.get("links", {})
    if links.get("notion_url"):
        st.link_button("Notion", links["notion_url"])
with st.expander("Breaker — Verify Now / Audit & Runner Heartbeats", expanded=False):
    # Show the state read straight from DB
    st.write(f"**DB says breaker is:** `{_ensure_breaker(CFG['db_path'])}`")

    # Refresh button
    if st.button("Refresh Audit/Heartbeat"):
        st.rerun()

    # Breaker audit log (last 15)
    try:
        con = sqlite3.connect(CFG["db_path"]); cur = con.cursor()
        cur.execute("SELECT id,new_state,who,note,created_at FROM breaker_log ORDER BY id DESC LIMIT 15")
        rows = cur.fetchall()
        con.close()
        cols = ["id","new_state","who","note","created_at"]
        df_log = pd.DataFrame(rows, columns=cols)
        # Make Arrow happy
        df_log = df_log.astype({"id":"Int64"})
        for c in ["new_state","who","note","created_at"]:
            df_log[c] = df_log[c].astype("string")
        st.markdown("**Recent Breaker Events:**")
        st.dataframe(df_log, use_container_width=True)
    except Exception as e:
        st.warning(f"Breaker log unavailable: {e}")

    # Runner heartbeats
    try:
        con = sqlite3.connect(CFG["db_path"]); cur = con.cursor()
        cur.execute("SELECT runner,state,info,updated_at FROM runner_heartbeat ORDER BY runner")
        rows = cur.fetchall()
        con.close()
        df_hb = pd.DataFrame(rows, columns=["runner","state","info","updated_at"])
        # compute age in seconds
        def _age_s(ts):
            try:
                t = pd.to_datetime(ts)
                return int((pd.Timestamp.now(tz=None) - t).total_seconds())
            except Exception:
                return None
        df_hb["age_s"] = df_hb["updated_at"].apply(_age_s)
        # Arrow-friendly types
        df_hb["runner"] = df_hb["runner"].astype("string")
        df_hb["state"] = df_hb["state"].astype("string")
        df_hb["info"] = df_hb["info"].astype("string")
        df_hb["updated_at"] = df_hb["updated_at"].astype("string")
        df_hb["age_s"] = df_hb["age_s"].astype("Int64")
        st.markdown("**Runner Heartbeats (lower age = fresher):**")
        st.dataframe(df_hb, use_container_width=True)
        if len(df_hb) == 0:
            st.info("No runners have checked in yet. Use the simulator below or wire your real runners.")
    except Exception as e:
        st.warning(f"Runner heartbeat unavailable: {e}")

    st.caption("Tip: RUNNING = do work; PAUSED = stay alive but idle; PANIC = stop the runner.")


# -------------------------- KPI tiles (timestamp-aware & schema-agnostic) --------------------------
from typing import Optional
import math

SCHEMA = CFG.get("schema", {})

def _get_columns(db_path: str, table: str):
    con = sqlite3.connect(db_path); cur = con.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    con.close()
    return cols

def _override_time_col(table: str) -> Optional[str]:
    # Look for explicit overrides in config: e.g., schema.signals_time_col
    key = f"{table}_time_col"
    return SCHEMA.get(key)

def _best_time_col(db_path: str, table: str) -> Optional[str]:
    if not _table_exists(db_path, table):
        return None
    # 1) explicit override
    over = _override_time_col(table)
    if over: return over
    # 2) guess common names
    cols = _get_columns(db_path, table)
    for cand in ["created_at", "timestamp", "ts", "time", "datetime", "dt", "created"]:
        if cand in cols: return cand
    return None

def _count_today(db_path: str, table: str, tcol: str) -> Optional[int]:
    """Try multiple strategies to count 'today' rows regardless of text/epoch."""
    con = sqlite3.connect(db_path); cur = con.cursor()
    try:
        # Strategy A: assume text-like date, e.g., '2025-08-31 10:45:00'
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE date({tcol}) = date('now','localtime')")
            return int(cur.fetchone()[0])
        except Exception:
            pass
        # Strategy B: epoch seconds
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE date(datetime({tcol}, 'unixepoch', 'localtime')) = date('now','localtime')")
            return int(cur.fetchone()[0])
        except Exception:
            pass
        # Strategy C: epoch milliseconds
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE date(datetime({tcol}/1000, 'unixepoch', 'localtime')) = date('now','localtime')")
            return int(cur.fetchone()[0])
        except Exception:
            pass
        return None
    finally:
        con.close()

def _latest_time(db_path: str, table: str, tcol: str) -> Optional[str]:
    """Return latest time formatted as 'YYYY-MM-DD HH:MM:SS' if possible."""
    con = sqlite3.connect(db_path); cur = con.cursor()
    try:
        # Try text-like
        try:
            cur.execute(f"SELECT MAX({tcol}) FROM {table}")
            val = cur.fetchone()[0]
            if val not in (None, ""):
                ts = pd.to_datetime(val, errors="coerce")
                if pd.notna(ts):
                    return ts.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        # Try epoch seconds
        try:
            cur.execute(f"SELECT MAX({tcol}) FROM {table}")
            val = cur.fetchone()[0]
            if val is not None:
                # integer/float seconds?
                try:
                    sec = float(val)
                    if sec > 1000000000 and sec < 100000000000:  # heuristic: if it's in seconds (10^9..10^11)
                        ts = pd.to_datetime(sec, unit="s", errors="coerce")
                        if pd.notna(ts):
                            return ts.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass
                # maybe milliseconds
                try:
                    ms = float(val)
                    if ms > 100000000000:  # > 10^11 -> probably ms
                        ts = pd.to_datetime(ms, unit="ms", errors="coerce")
                        if pd.notna(ts):
                            return ts.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass
        except Exception:
            pass
        return None
    finally:
        con.close()

def _count_today_or_total_smart(db_path: str, table: str) -> int:
    if not _table_exists(db_path, table):
        return 0
    tcol = _best_time_col(db_path, table)
    if tcol:
        val = _count_today(db_path, table, tcol)
        if val is not None:
            return val
    # fallback: total rows
    con = sqlite3.connect(db_path); cur = con.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return int(cur.fetchone()[0])
    finally:
        con.close()

def _last_time_smart(db_path: str, table: str) -> str:
    if not _table_exists(db_path, table):
        return "—"
    tcol = _best_time_col(db_path, table)
    if tcol:
        ts = _latest_time(db_path, table, tcol)
        if ts: return ts
    # No time column or unreadable -> show em dash
    return "—"

sig_today = _count_today_or_total_smart(CFG["db_path"], "signals")
po_today  = _count_today_or_total_smart(CFG["db_path"], "paper_orders")
last_sig  = _last_time_smart(CFG["db_path"], "signals")

k1, k2, k3 = st.columns(3)
k1.metric("Signals Today", f"{sig_today}")
k2.metric("Paper Fills Today", f"{po_today}")
k3.metric("Last Signal At", last_sig)
st.divider()
# ---------------------------------------------------------------------------------
# -------------------------- RUNNER LIGHTS (🟢/🟠/🔴) --------------------------
def _compute_runner_statuses(db_path: str):
    ops_cfg = CFG.get("ops", {}) or {}
    fresh_s = int(ops_cfg.get("hb_fresh_s", 5))
    stale_s = int(ops_cfg.get("hb_stale_s", 20))
    expected = ops_cfg.get("runners", []) or []

    try:
        con = sqlite3.connect(db_path); cur = con.cursor()
        cur.execute("SELECT runner,state,updated_at FROM runner_heartbeat")
        rows = cur.fetchall()
        con.close()
    except Exception:
        rows = []

    # Pick the latest record per runner
    now = pd.Timestamp.now(tz=None)
    latest = {}
    for r, s, ts in rows:
        # Keep the latest updated_at per runner
        prev = latest.get(r)
        this_t = pd.to_datetime(ts, errors="coerce")
        if prev is None:
            latest[r] = {"runner": r, "state": s or "RUNNING", "updated_at": ts, "ts": this_t}
        else:
            prev_t = prev["ts"]
            if (this_t is not pd.NaT) and (prev_t is pd.NaT or this_t > prev_t):
                latest[r] = {"runner": r, "state": s or "RUNNING", "updated_at": ts, "ts": this_t}

    # Names we will show: config "expected" list; if empty, auto-discover from DB
    names = expected if expected else list(latest.keys())

    statuses = []
    for name in names:
        rec = latest.get(name)
        if not rec or rec["ts"] is pd.NaT:
            statuses.append({"runner": name, "state": "—", "age_s": None, "color": "red", "label": "🔴 dead"})
            continue
        age = int((now - rec["ts"]).total_seconds())
        if age <= fresh_s:
            color, label = "green", "🟢 fresh"
        elif age <= stale_s:
            color, label = "orange", "🟠 stale"
        else:
            color, label = "red", "🔴 dead"
        statuses.append({"runner": name, "state": rec["state"], "age_s": age, "color": color, "label": label})

    return statuses, fresh_s, stale_s

def _render_runner_lights(statuses, fresh_s: int, stale_s: int):
    st.markdown("### Runner Lights")
    if not statuses:
        st.info("No runners to display yet. Start a runner or add names to ops.runners in config.")
        return
    # color map
    cmap = {"green": "#34c759", "orange": "#ff9500", "red": "#ff3b30"}

    cols = st.columns(len(statuses))
    for i, s in enumerate(statuses):
        dot = cmap[s["color"]]
        age = "—" if s["age_s"] is None else f'{s["age_s"]}s'
        html = f"""
        <div style="border:1px solid #e6e6e6;border-radius:12px;padding:8px 12px;text-align:center;">
          <div style="display:flex;align-items:center;justify-content:center;gap:8px;">
            <span style="width:10px;height:10px;border-radius:50%;background:{dot};display:inline-block;"></span>
            <strong>{s['runner']}</strong>
          </div>
          <div style="font-size:12px;opacity:0.8;">{s['state']} · {age}</div>
        </div>
        """
        with cols[i]:
            st.markdown(html, unsafe_allow_html=True)
    st.caption(f"🟢 ≤{fresh_s}s fresh · 🟠 ≤{stale_s}s stale · 🔴 >{stale_s}s dead")

# Compute + render
_runner_statuses, _fresh, _stale = _compute_runner_statuses(CFG["db_path"])
_render_runner_lights(_runner_statuses, _fresh, _stale)
st.divider()
# ---------------------------------------------------------------------------------


# Sidebar: Stage and Config selector
stage = st.sidebar.selectbox("Stage", ["Backtest", "Paper", "Live-Ready"], index=0)
configs = STORE.list_configs(stage=stage)

cfg_labels, cfg_ids = [], []
for c in configs:
    # c = (id, name, stage, version, is_active, notes)
    label = f"[{c[0]}] {c[1]} v{c[3]}{' • active' if c[4] else ''}"
    cfg_labels.append(label)
    cfg_ids.append(c[0])

sel_label = st.sidebar.selectbox("Select Config", cfg_labels + ["<Create New>"])

def load_bundle(cid: int):
    return STORE.get_config_bundle(cid)

def df_params(bundle):
    import json as _json
    if not bundle:
        return pd.DataFrame(columns=["param","value"])
    items = []
    for k, v in (bundle.get("params") or {}).items():
        # stringify to avoid mixed dtype (int/float/bool) that triggers Arrow warnings
        if isinstance(v, (dict, list)):
            v_str = _json.dumps(v, ensure_ascii=False)
        else:
            v_str = str(v)
        items.append({"param": str(k), "value": v_str})
    df = pd.DataFrame(items, columns=["param","value"])
    # ensure explicit string dtype for Arrow
    df["param"] = df["param"].astype("string")
    df["value"] = df["value"].astype("string")
    return df


# Create new config
if sel_label == "<Create New>":
    st.subheader("Create New Strategy Config")
    new_name = st.text_input("Config Name", value="MyStrategy")
    notes = st.text_area("Notes", value="M12 new config.")
    if st.button("Create Backtest Config"):
        cid = STORE.create_config(new_name, "Backtest", notes)
        STORE.add_params(cid, {
            "ema_fast": 9, "ema_slow": 21, "rsi_len": 14, "rsi_buy": 40, "rsi_sell": 60,
            "entry_delay_secs": 1, "exit_on_reverse": True
        })
        STORE.set_policies(cid, {
            "capital_mode": "Fixed",
            "fixed_capital": 150000, "risk_per_trade_pct": 1.0,
            "max_trades_per_day": 5, "rr_min": 2.0, "sl_max_per_lot": 1000.0,
            "daily_loss_limit": 0.0, "group_exposure_cap_pct": 100.0,
            "breaker_threshold": 0.0, "trading_windows": "09:20-15:20"
        })
        STORE.set_liquidity(cid, {
            "min_oi": 0, "min_volume": 0, "max_spread_paisa": 50, "slippage_bps": 5, "fees_per_lot": 30.0
        })
        tcfg = CFG.get("telegram", {})
        STORE.set_notif(cid, {
            "telegram_enabled": tcfg.get("enabled", False),
            "t_bot_token": tcfg.get("bot_token", ""),
            "t_chat_id": tcfg.get("chat_id", ""),
            "eod_summary": True
        })
        st.success(f"Created config id {cid}. Switch stage to Backtest and select it.")
    st.stop()

# Existing config: map selection back to id
cid = None
if sel_label != "<Create New>":
    idx = cfg_labels.index(sel_label) if sel_label in cfg_labels else -1
    if idx >= 0:
        cid = cfg_ids[idx]

if cid is None:
    st.error("No config selected. Choose an existing config or create a new one.")
    st.stop()

bundle = load_bundle(cid)
if not bundle:
    st.error("Failed to load config bundle. Please check your database.")
    st.stop()

tabs = st.tabs([
    "Strategy Lab (F17)",
    "Policies (F18/F20)",
    "Liquidity (F21)",
    "Promotion (F19)",
    "Alerts & EOD (F22)",
    "Health & Ops (F23)",
    "Export/Import (F24)"
])

with tabs[0]:
    st.subheader("Strategy Lab — Indicators & Rules (F17)")
    df = df_params(bundle).copy()
    st.dataframe(df, use_container_width=True)
    st.markdown("**Quick Edit**")
    p = (bundle or {}).get("params") or {}
    ema_fast = st.number_input("EMA Fast", min_value=2, max_value=200, value=int(p.get("ema_fast", 9)))
    ema_slow = st.number_input("EMA Slow", min_value=3, max_value=400, value=int(p.get("ema_slow", 21)))
    rsi_len = st.number_input("RSI Length", min_value=2, max_value=100, value=int(p.get("rsi_len", 14)))
    rsi_buy = st.number_input("RSI Buy Threshold", min_value=1, max_value=99, value=int(p.get("rsi_buy", 40)))
    rsi_sell = st.number_input("RSI Sell Threshold", min_value=1, max_value=99, value=int(p.get("rsi_sell", 60)))
    entry_delay_secs = st.number_input("Entry Delay (secs)", min_value=0, max_value=30, value=int(p.get("entry_delay_secs", 1)))
    exit_on_reverse = st.checkbox("Exit on Reverse Signal", value=bool(p.get("exit_on_reverse", True)))
    if st.button("Save Strategy Params"):
        STORE.add_params(cid, {
            "ema_fast": ema_fast, "ema_slow": ema_slow, "rsi_len": rsi_len,
            "rsi_buy": rsi_buy, "rsi_sell": rsi_sell,
            "entry_delay_secs": entry_delay_secs, "exit_on_reverse": exit_on_reverse
        })
        st.success("Params saved. (A new row is appended; latest key wins when read.)")

with tabs[1]:
    st.subheader("Dynamic Policies & Core Limits (F18/F20)")
    pol = (bundle or {}).get("policies") or {}
    capital_mode = st.selectbox("Capital Mode", ["Fixed", "Dynamic"], index=0 if pol.get("capital_mode", "Fixed") == "Fixed" else 1)
    fixed_capital = st.number_input("Fixed Capital (₹)", min_value=10000.0, step=1000.0, value=float(pol.get("fixed_capital", 150000)))
    risk_pct = st.number_input("Risk per Trade (%)", min_value=0.1, step=0.1, value=float(pol.get("risk_per_trade_pct", 1.0)))
    max_trades = st.number_input("Max Trades/Day (≤5)", min_value=0, max_value=5, value=int(pol.get("max_trades_per_day", 5)))
    rr_min = st.number_input("Min R:R (≥2.0)", min_value=1.0, step=0.1, value=float(pol.get("rr_min", 2.0)))
    sl_max = st.number_input("SL ≤ ₹1000 per lot", min_value=100.0, step=50.0, value=float(pol.get("sl_max_per_lot", 1000.0)))
    daily_loss = st.number_input("Daily Loss Limit (₹)", min_value=0.0, step=100.0, value=float(pol.get("daily_loss_limit", 0.0)))
    exposure_cap = st.number_input("Per-Group Exposure Cap (%)", min_value=1.0, step=1.0, value=float(pol.get("group_exposure_cap_pct", 100.0)))
    breaker = st.number_input("Breaker Threshold (vol spike etc.)", min_value=0.0, step=0.5, value=float(pol.get("breaker_threshold", 0.0)))
    twindows = st.text_input("Trading Windows (HH:MM-HH:MM; comma-separated for multiple)", value=pol.get("trading_windows", "09:20-15:20"))

    if st.button("Save Policies"):
        new_pol = {
            "capital_mode": capital_mode,
            "fixed_capital": fixed_capital,
            "risk_per_trade_pct": risk_pct,
            "max_trades_per_day": max_trades,
            "rr_min": rr_min,
            "sl_max_per_lot": sl_max,
            "daily_loss_limit": daily_loss,
            "group_exposure_cap_pct": exposure_cap,
            "breaker_threshold": breaker,
            "trading_windows": twindows
        }
        issues = enforce_core_limits(new_pol)
        if issues:
            st.error("Policy violations:\n- " + "\n- ".join(issues))
        else:
            STORE.set_policies(cid, new_pol)
            st.success("Policies saved and compliant with core limits.")

with tabs[2]:
    st.subheader("Liquidity / Fees / Slippage (F21)")
    lf = (bundle or {}).get("liquidity") or {}
    min_oi = st.number_input("Min Open Interest", min_value=0, step=100, value=int(lf.get("min_oi", 0)))
    min_vol = st.number_input("Min Volume", min_value=0, step=100, value=int(lf.get("min_volume", 0)))
    max_spread = st.number_input("Max Spread (paise)", min_value=0, step=1, value=int(lf.get("max_spread_paisa", 50)))
    slip_bps = st.number_input("Slippage (bps)", min_value=0, step=1, value=int(lf.get("slippage_bps", 5)))
    fees_lot = st.number_input("Fees per lot (₹)", min_value=0.0, step=1.0, value=float(lf.get("fees_per_lot", 30.0)))
    if st.button("Save Liquidity"):
        STORE.set_liquidity(cid, {
            "min_oi": min_oi, "min_volume": min_vol, "max_spread_paisa": max_spread,
            "slippage_bps": slip_bps, "fees_per_lot": fees_lot
        })
        st.success("Liquidity settings saved.")

with tabs[3]:
    st.subheader("Promotion Pipeline (F19) — KPI Gated")
    st.write("Promotion requires KPIs to meet thresholds. Compute KPIs first, then promote.")
    target = st.selectbox("Target Stage", ["Backtest", "Paper", "Live-Ready"], index=1)
    label = st.text_input("Promotion Label", value="M12 promotion")

    # Thresholds (tweakable)
    st.markdown("**Thresholds (edit as needed):**")
    colA, colB, colC, colD = st.columns(4)
    thr_wr = colA.number_input("Min Win-Rate (%)", min_value=0.0, max_value=100.0, value=48.0, step=0.5)
    thr_pf = colB.number_input("Min Profit Factor", min_value=0.0, value=1.30, step=0.05)
    thr_mdd = colC.number_input("Max Drawdown (%)", min_value=0.0, value=5.0, step=0.5)
    thr_min_trades = colD.number_input("Min Trades", min_value=1, value=30, step=1)

    # Load latest KPI summary (safe if table missing)
    kpi_ok = _table_exists(CFG["db_path"], "kpi_summary")
    exec_ok = _table_exists(CFG["db_path"], "exec_trades")

    if kpi_ok:
        con = sqlite3.connect(CFG["db_path"]); cur = con.cursor()
        cur.execute("""SELECT label,trades_count,win_rate,profit_factor,avg_trade,expectancy,max_drawdown_pct,
                              gross_profit,gross_loss,net_pnl,created_at
                       FROM kpi_summary
                       WHERE stage=? AND config_id=?
                       ORDER BY id DESC LIMIT 1""", (stage, cid))
        row = cur.fetchone()
        # Fixed capital (for MDD% calc if needed)
        pcur = con.cursor()
        pcur.execute("SELECT fixed_capital FROM risk_policies WHERE config_id=?", (cid,))
        ppol = pcur.fetchone()
        con.close()
    else:
        row = None
        ppol = (150000.0,)  # default fixed capital fallback

    fixed_capital = float(ppol[0]) if ppol and ppol[0] else 150000.0

    if row:
        k_label, ntr, wr, pf, avg, exp, mdd_pct_stored, gp, gl, net, created = row

        # Estimate MDD% from equity curve if exec_trades present
        if exec_ok:
            con = sqlite3.connect(CFG["db_path"]); cur2 = con.cursor()
            cur2.execute("SELECT pnl FROM exec_trades WHERE stage=? AND config_id=? ORDER BY id", (stage, cid))
            pnls = [r[0] for r in cur2.fetchall()]
            con.close()
            cum = 0.0
            peak = 0.0
            max_dd_abs = 0.0
            for pval in pnls:
                cum += (pval or 0.0)
                if cum > peak: peak = cum
                dd = peak - cum
                if dd > max_dd_abs: max_dd_abs = dd
            mdd_pct = (max_dd_abs / fixed_capital) * 100.0 if fixed_capital > 0 else 0.0
        else:
            mdd_pct = mdd_pct_stored or 0.0

        st.markdown("**Latest KPIs (from kpi_summary):**")
        st.write(f"- Label: `{k_label}` (created {created})")
        st.write(f"- Trades: **{ntr}**")
        st.write(f"- Win-Rate: **{wr:.2f}%**")
        st.write(f"- Profit Factor: **{pf:.2f}**")
        st.write(f"- Avg Trade: **₹{avg:.2f}** | Expectancy: **₹{exp:.2f}**")
        st.write(f"- Net PnL: **₹{net:.2f}** | Gross Profit: **₹{gp:.2f}**, Gross Loss: **₹{gl:.2f}**")
        st.write(f"- Max Drawdown (est): **{mdd_pct:.2f}%** (capital ₹{fixed_capital:,.0f})")

        gate_pass = (ntr >= thr_min_trades) and (wr >= thr_wr) and (pf >= thr_pf) and (mdd_pct <= thr_mdd)

        if gate_pass:
            st.success("KPI Gate: PASS — you may promote.")
            promo_cmd = f'python C:\\teevra18\\scripts\\promote_config.py {cid} "{target}" "{label}"'
            st.code(promo_cmd, language="powershell")
            st.info("Copy & run the above command in PowerShell to perform promotion.")
        else:
            st.error("KPI Gate: FAIL — adjust strategy/policies or add more trades before promoting.")
            st.caption("Tip: compute KPIs again after adding more trades.")
    else:
        st.warning("No KPI summary found for this config and stage. Compute KPIs first.")
        st.markdown("**Compute KPIs (copy-paste):**")
        st.code(f'python C:\\teevra18\\scripts\\compute_kpis.py {stage} {cid} "{stage.upper()}_{label}"', language="powershell")

with tabs[4]:
    st.subheader("Telegram & EOD Settings (F22)")
    ns = (bundle or {}).get("notif") or {}
    tel_enabled = st.checkbox("Telegram Enabled", value=bool(ns.get("telegram_enabled", True)))
    t_token = st.text_input("Bot Token", value=ns.get("t_bot_token", ""))
    t_chat = st.text_input("Chat ID", value=ns.get("t_chat_id", ""))
    eod = st.checkbox("Send EOD Summary", value=bool(ns.get("eod_summary", True)))
    if st.button("Save Notifications"):
        STORE.set_notif(cid, {
            "telegram_enabled": tel_enabled,
            "t_bot_token": t_token,
            "t_chat_id": t_chat,
            "eod_summary": eod
        })
        st.success("Notification settings saved.")
    st.markdown("**Test Alert (copy-paste):**")
    st.code('python -c "from core.alerts import telegram_send; print(telegram_send(\'Teevra18 test alert\'))"', language="powershell")

with tabs[5]:
    st.subheader("Health & Ops (F23)")
    dbp = P(CFG["db_path"])
    size_mb = dbp.stat().st_size / 1024 / 1024 if dbp.exists() else 0
    st.metric("DB Size (MB)", f"{size_mb:.2f}")
    st.write("Configs (by stage):")
    data = []
for stg in ["Backtest", "Paper", "Live-Ready"]:
    rows = STORE.list_configs(stage=stg)
    data.append({"stage": stg, "count": int(len(rows))})
df_health = pd.DataFrame(data, columns=["stage","count"])
df_health["stage"] = df_health["stage"].astype("string")
df_health["count"] = df_health["count"].astype("Int64")  # nullable integer
st.dataframe(df_health, use_container_width=True)

with tabs[6]:
    st.subheader("Export / Import (F24)")
    st.write("**Export current config to JSON file**")
    if st.button("Export Snapshot"):
        bundle = STORE.get_config_bundle(cid)
        st.download_button("Download JSON", data=json.dumps(bundle, indent=2).encode("utf-8"), file_name=f"teevra18_config_{cid}.json")
    st.write("---")
    st.write("**Import snapshot as new config**")
    upload = st.file_uploader("Choose JSON snapshot", type=["json"])
    new_stage = st.selectbox("Import Stage", ["Backtest", "Paper", "Live-Ready"], index=0)
    new_name = st.text_input("New Config Name", value="ImportedConfig")
    if upload and st.button("Import Snapshot"):
        snap = json.loads(upload.read().decode("utf-8"))
        new_id = STORE.import_snapshot(stage=new_stage, name=new_name, snapshot=snap, notes="Imported via UI")
        st.success(f"Imported as config id {new_id}")
