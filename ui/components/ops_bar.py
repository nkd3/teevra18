import json, sqlite3, subprocess, sys, time
from pathlib import Path
import streamlit as st

BASE = Path(r"C:\teevra18")
VENV_PY = BASE / ".venv" / "Scripts" / "python.exe"
PROC_CTRL = BASE / "scripts" / "proc_controller.py"
PID_FILE = BASE / "run" / "pids.json"
DB_PATH = BASE / "data" / "teevra18.db"

def run_controller(args):
    cmd = [str(VENV_PY), str(PROC_CTRL)] + args
    # capture output but keep it quiet in UI except for errors
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        return out
    except subprocess.CalledProcessError as e:
        return e.output

def read_status():
    try:
        out = run_controller(["status"])
        data = json.loads(out.strip() if out.strip().startswith("{") else "{}")
        return data
    except Exception:
        return {"services": {}, "streamlit": None}

def last_tick_ts():
    # Optional: show last market tick time from health table
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        # Expect a row like: key='last_tick_ts', value='2025-09-01T09:15:23+05:30'
        cur.execute("SELECT value FROM health WHERE key='last_tick_ts' ORDER BY ts DESC LIMIT 1;")
        row = cur.fetchone()
        con.close()
        return row[0] if row else "—"
    except Exception:
        return "—"

def render_ops_bar():
    st.subheader("Ops Controls")

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("▶️ Start All", type="primary"):
            # From UI we don't relaunch Streamlit; start services only
            out = run_controller(["start_noui"])
            st.toast("Start All triggered.")
            st.session_state["_ops_out"] = out
    with c2:
        if st.button("⏹ Stop All"):
            out = run_controller(["stop"])
            st.toast("Stop All triggered.")
            st.session_state["_ops_out"] = out
    with c3:
        if st.button("ℹ️ Status"):
            out = run_controller(["status"])
            st.session_state["_ops_out"] = out

    if "_ops_out" in st.session_state:
        with st.expander("Controller Output (debug)"):
            st.code(st.session_state["_ops_out"])

    st.divider()
    st.caption(f"Dhan Live: Last Tick = {last_tick_ts()}")
    data = read_status()

    # Small status grid
    cols = st.columns(4)
    i = 0
    for name, meta in sorted(data.get("services", {}).items()):
        alive = meta.get("alive", False)
        pid = meta.get("pid", "—")
        with cols[i % 4]:
            st.metric(
                label=f"{'🟢' if alive else '🔴'} {name}",
                value=f"PID {pid}" if alive else "stopped",
                delta=None
            )
        i += 1

    # Streamlit status shown for completeness (mostly 'alive' because we're here)
    st.caption(f"UI Process: {'🟢' if data.get('streamlit',{}).get('alive') else '🔴'} "
               f"PID {data.get('streamlit',{}).get('pid', '—')}")
