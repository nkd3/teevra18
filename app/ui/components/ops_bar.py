# -*- coding: utf-8 -*-
import json
import sqlite3
import subprocess
from pathlib import Path
import streamlit as st

# --------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------
BASE = Path(r"C:\teevra18")
VENV_PY   = BASE / r".venv\Scripts\python.exe"
PROC_CTRL = BASE / r"scripts\proc_controller.py"
PID_FILE  = BASE / r"run\pids.json"
DB_PATH   = BASE / r"data\teevra18.db"

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def _run_controller(args):
    """Run controller command and return stdout (string). Never raises."""
    cmd = [str(VENV_PY), str(PROC_CTRL)] + list(args)
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="ignore")
        return out or ""
    except subprocess.CalledProcessError as e:
        # Return combined output so we can still show it in the debug box
        return (e.output or "").strip()
    except Exception as e:
        return f"[ERROR] Controller call failed: {e}"

def _safe_status_dict(obj):
    """Coerce any value into a safe status dict {services:{}, streamlit:{...}}."""
    if isinstance(obj, dict):
        # streamlit can be None in some controller versions
        if obj.get("streamlit") is None:
            obj["streamlit"] = {"pid": None, "alive": False}
        if obj.get("services") is None:
            obj["services"] = {}
        return obj
    return {"services": {}, "streamlit": {"pid": None, "alive": False}}

def _read_status():
    """Ask controller for status and return a safe dict."""
    raw = _run_controller(["status"]).strip()
    try:
        data = json.loads(raw) if raw.startswith("{") else {}
    except Exception:
        data = {}
    return _safe_status_dict(data)

def _last_tick_ts():
    """Read last tick time from DB (non-fatal on errors)."""
    try:
        con = sqlite3.connect(str(DB_PATH))
        cur = con.cursor()
        cur.execute("SELECT value FROM health WHERE key='last_tick_ts' ORDER BY ts DESC LIMIT 1;")
        row = cur.fetchone()
        con.close()
        return row[0] if row else "—"
    except Exception:
        return "—"

# Preferred display order for tiles
SERVICE_ORDER = [
    "svc_ingest_dhan",
    "svc_quote_snap",
    "svc_candles",
    "svc_chain_snap",
    "svc_strategy_core",
    "svc_rr_builder",      # oneshot – may not appear if not tracked
    "svc_kpi_eod",         # oneshot – may not appear if not tracked
]

# --------------------------------------------------------------------
# UI
# --------------------------------------------------------------------
def render_ops_bar():
    st.subheader("Ops")

    c1, c2, c3 = st.columns([1,1,1])

    # Start All (services only; UI stays)
    with c1:
        if st.button("▶️ Start All", type="primary", use_container_width=True):
            out = _run_controller(["start_noui"])
            st.session_state["_ops_out"] = out
            st.toast("Start All triggered.")

    # Stop All (services only; keeps UI alive)
    with c2:
        if st.button("⏹ Stop All", use_container_width=True):
            # use stop_services to avoid killing Streamlit
            out = _run_controller(["stop_services"])
            st.session_state["_ops_out"] = out
            st.toast("Stop All triggered.")

    # Status refresh
    with c3:
        if st.button("ℹ️ Status", use_container_width=True):
            out = _run_controller(["status"])
            st.session_state["_ops_out"] = out

    # Debug expander like your older config
    if "_ops_out" in st.session_state:
        with st.expander("Controller Output (debug)", expanded=True):
            st.code(st.session_state["_ops_out"])

    st.divider()

    # Health line
    st.caption(f"Dhan Live: Last Tick = {_last_tick_ts()}")

    # Status tiles
    data = _read_status()
    services = data.get("services", {}) or {}

    # Show UI PID line (safe)
    ui_alive = bool((data.get("streamlit") or {}).get("alive"))
    ui_pid   = (data.get("streamlit") or {}).get("pid", "—") or "—"

    # Render service tiles in preferred order first, then any extras
    names_in_order = [n for n in SERVICE_ORDER if n in services]
    extras = sorted([n for n in services.keys() if n not in SERVICE_ORDER])
    all_names = names_in_order + extras

    if not all_names:
        st.info("No daemon services tracked yet. Click **Start All** to launch core daemons.")
    else:
        cols = st.columns(4)
        for idx, name in enumerate(all_names):
            meta = services.get(name, {}) or {}
            alive = bool(meta.get("alive"))
            pid   = meta.get("pid", "—")
            with cols[idx % 4]:
                st.metric(
                    label=f"{'🟢' if alive else '🔴'} {name}",
                    value=f"PID {pid}" if alive else "stopped"
                )

    st.caption(f"UI Process: {'🟢' if ui_alive else '🔴'}  PID {ui_pid}")
