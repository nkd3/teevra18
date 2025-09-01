# -*- coding: utf-8 -*-
import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import psutil

# -----------------------------------------------------------------------------
# Hard-pin repo roots onto sys.path BEFORE any local imports
# -----------------------------------------------------------------------------
BASE = Path(r"C:\teevra18")
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
if str(BASE / "app") not in sys.path:
    sys.path.insert(0, str(BASE / "app"))

# -----------------------------------------------------------------------------
# Safe env loader with built-in fallback (so it NEVER fails)
# -----------------------------------------------------------------------------
def _parse_env_file(path: Path) -> dict:
    if not path.exists():
        return {}
    out = {}
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out

def _safe_load_environment():
    """
    Try common.env.load_environment(); if that import fails,
    fallback: parse C:\teevra18\.env + C:\teevra18\config\.env (config wins),
    then apply DHAN key compatibility (CLIENT_ID <-> API_KEY).
    """
    try:
        from common.env import load_environment  # type: ignore
        load_environment()
        return
    except Exception:
        pass  # fallback below

    root_env = _parse_env_file(BASE / ".env")
    cfg_env  = _parse_env_file(BASE / "config" / ".env")
    merged = {**root_env, **cfg_env}
    for k, v in merged.items():
        os.environ[k] = v

    cid = os.environ.get("DHAN_CLIENT_ID") or os.environ.get("DHAN_API_KEY")
    if cid:
        os.environ["DHAN_CLIENT_ID"] = cid
        os.environ["DHAN_API_KEY"]   = cid

VENV_PY = BASE / ".venv" / "Scripts" / "python.exe"
RUN_DIR = BASE / "run"; RUN_DIR.mkdir(parents=True, exist_ok=True)
PID_FILE = RUN_DIR / "pids.json"

STREAMLIT_ENTRY = BASE / r"app\ui\Home_Landing.py"

SCAN_DIRS = [
    BASE / "services",
    BASE / "scripts",
    BASE / r"app\services",
    BASE / r"app\scripts",
    BASE / r"app\runners",
    BASE,
]

REQUIRED = {
    # Daemons (should stay running)
    "svc_ingest_dhan.py":       {"autostart": True,  "args": [],              "oneshot": False},  # M1
    "svc_quote_snap.py":        {"autostart": True,  "args": [],              "oneshot": False},  # M2
    "svc_depth20.py":           {"autostart": False, "args": [],              "oneshot": False},  # M3
    "svc_candles.py":           {"autostart": True,  "args": ["follow"],      "oneshot": False},  # M4
    "svc_chain_snap.py":        {"autostart": True,  "args": ["follow"],      "oneshot": False},  # M5

    # Strategy runs once per trigger (don’t track as daemon)
    "svc_strategy_core.py":     {"autostart": True,  "args": ["generate"],    "oneshot": True},   # M7

    # One-shot batchers
    "svc_rr_builder.py":        {"autostart": True,  "args": [],              "oneshot": True},   # M8
    "svc_kpi_eod.py":           {"autostart": True,  "args": [],              "oneshot": True},   # M10

    # Optional/manual
    "svc_paper_pm.py":          {"autostart": False, "args": [],              "oneshot": False},  # M9
    "svc_historical_loader.py": {"autostart": False, "args": [],              "oneshot": True},   # M6
    "svc_forecast.py":          {"autostart": False, "args": [],              "oneshot": False},  # M11
}

# ---------------- Core helpers ----------------
def load_pids():
    if PID_FILE.exists():
        try:
            return json.loads(PID_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_pids(pids):
    PID_FILE.write_text(json.dumps(pids, indent=2), encoding="utf-8")

def is_alive(pid: int) -> bool:
    try:
        p = psutil.Process(pid)
        return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
    except Exception:
        return False

def discover_services():
    found = {}
    targets = set(REQUIRED.keys())
    for root in SCAN_DIRS:
        if not root.exists():
            continue
        for p in root.rglob("*.py"):
            fname = p.name
            if fname in targets and fname not in found:
                found[fname] = p  # full path to the script
            if len(found) == len(targets):
                break
        if len(found) == len(targets):
            break
    return found

def _find_running_pid_for_script(script_path: Path) -> int | None:
    """
    Scan processes; if a python process is running this script, return its PID.
    Matches either the full path OR just the basename (Windows can vary).
    """
    sp_full = str(script_path).lower().replace("\\", "/")
    sp_base = script_path.name.lower()

    for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
        try:
            cmd = proc.info.get("cmdline") or []
            if not cmd:
                continue
            joined = " ".join(cmd).lower().replace("\\", "/")
            if "python" not in joined:
                continue

            # accept either full path or just the filename in the command line
            if sp_full in joined or f" {sp_base}" in joined or joined.endswith(sp_base):
                return int(proc.info["pid"])
        except Exception:
            continue
    return None


def start_process(pyfile: Path, extra_args=None, name_for_log: str = None, oneshot: bool = False):
    extra_args = extra_args or []
    if not pyfile.exists():
        return None, f"Missing script: {pyfile}"

    LOG_DIR = BASE / "logs"
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    svc_name = name_for_log or pyfile.stem
    log_path = LOG_DIR / f"{svc_name}.log"

    env = os.environ.copy()
    log_fh = open(log_path, "a", encoding="utf-8", errors="ignore")

    proc = subprocess.Popen(
        [str(VENV_PY), "-u", str(pyfile)] + list(extra_args),
        stdout=log_fh,
        stderr=log_fh,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
        env=env
    )
    return proc.pid, None

def start_streamlit():
    if not STREAMLIT_ENTRY.exists():
        return None, f"Missing Streamlit file: {STREAMLIT_ENTRY}"
    env = os.environ.copy()
    proc = subprocess.Popen(
        [str(VENV_PY), "-m", "streamlit", "run", str(STREAMLIT_ENTRY)],
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
        env=env
    )
    return proc.pid, None

def stop_pid(pid: int, graceful_timeout=5):
    try:
        p = psutil.Process(pid)
        try:
            if hasattr(signal, "CTRL_BREAK_EVENT"):
                p.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                p.terminate()
        except Exception:
            p.terminate()
        gone, alive = psutil.wait_procs([p], timeout=graceful_timeout)
        if alive:
            for a in alive:
                a.kill()
        return True
    except Exception:
        return False

# ---------------- Main orchestrators ----------------
def start_all(include_streamlit=True):
    _safe_load_environment()

    if not os.environ.get("DHAN_CLIENT_ID") or not os.environ.get("DHAN_ACCESS_TOKEN"):
        print("[WARN] DHAN credentials missing. Set in C:\\teevra18\\config\\.env")

    pids = load_pids()
    pids.setdefault("services", {})
    found = discover_services()

    for fname, meta in REQUIRED.items():
        if not meta["autostart"]:
            continue
        path = found.get(fname)
        if not path:
            print(f"[WARN] {fname}: not found under scan dirs.")
            continue

        name = fname.replace(".py", "")

        # If we already track a live PID, don't spawn a duplicate
        existing_pid = pids["services"].get(name)
        if existing_pid and is_alive(existing_pid):
            print(f"[INFO] {name} already running (PID {existing_pid}).")
            continue

        pid, err = start_process(path, meta.get("args", []), name_for_log=name, oneshot=meta.get("oneshot", False))
        if err:
            print(f"[WARN] {name}: {err}")
            continue
        args_str = " " + " ".join(meta.get("args", [])) if meta.get("args") else ""
        print(f"[OK] Started {name}{args_str} (PID {pid})")

        # Track only daemons; one-shot jobs will exit normally and shouldn't show as red
        if not meta.get("oneshot", False):
            pids["services"][name] = pid

    if include_streamlit:
        if "streamlit" in pids and is_alive(pids["streamlit"]):
            print("[INFO] Streamlit already running.")
        else:
            pid, err = start_streamlit()
            if err:
                print(f"[WARN] Streamlit: {err}")
            else:
                pids["streamlit"] = pid
                print(f"[OK] Started Streamlit (PID {pid})")

    save_pids(pids)
    return pids

def _repair_tracked_pids(pids: dict):
    """
    If a tracked PID is dead but the service is actually running,
    reattach to the real PID (found by scanning processes).
    """
    found = discover_services()
    changed = False
    pids.setdefault("services", {})

    for fname, meta in REQUIRED.items():
        if meta.get("oneshot", False):
            continue  # don't track one-shots

        name = fname.replace(".py", "")
        script_path = found.get(fname)
        if not script_path:
            continue

        tracked = pids["services"].get(name)
        if tracked and is_alive(tracked):
            continue  # good

        running_pid = _find_running_pid_for_script(script_path)
        if running_pid and is_alive(running_pid):
            pids["services"][name] = running_pid
            changed = True

    if changed:
        save_pids(pids)
    return pids

def stop_services():
    """Stop only daemon services; keep Streamlit alive. Also kill by script match if PID stale."""
    pids = load_pids()
    pids.setdefault("services", {})
    found = discover_services()

    # First try by tracked PIDs
    for name, pid in list(pids["services"].items()):
        ok = False
        if is_alive(pid):
            ok = stop_pid(pid)
        else:
            # PID stale: try to locate by script path and kill
            fname = f"{name}.py"
            script_path = found.get(fname)
            if script_path:
                running_pid = _find_running_pid_for_script(script_path)
                if running_pid and is_alive(running_pid):
                    ok = stop_pid(running_pid)
        print(f"[STOP] {name} PID {pid}: {'OK' if ok else 'FAILED'}")
        pids["services"].pop(name, None)

    save_pids(pids)
    return pids

def stop_all():
    """Stop services and Streamlit."""
    stop_services()
    pids = load_pids()
    if "streamlit" in pids:
        pid = pids["streamlit"]
        if is_alive(pid):
            ok = stop_pid(pid)
            print(f"[STOP] streamlit PID {pid}: {'OK' if ok else 'FAILED'}")
        pids.pop("streamlit", None)
    save_pids(pids)
    return pids

def status():
    # Load and repair before reporting
    p = load_pids()
    p.setdefault("services", {})
    p = _repair_tracked_pids(p)

    out = {"services": {}, "streamlit": None}
    for name, pid in p.get("services", {}).items():
        out["services"][name] = {"pid": pid, "alive": is_alive(pid)}
    if "streamlit" in p:
        out["streamlit"] = {"pid": p["streamlit"], "alive": is_alive(p["streamlit"])}
    print(json.dumps(out, indent=2))
    return out

def main():
    if len(sys.argv) < 2:
        print("Usage: python proc_controller.py start|start_noui|stop|stop_services|status")
        sys.exit(1)
    cmd = sys.argv[1].lower()
    if cmd == "start":
        start_all(include_streamlit=True)
    elif cmd == "start_noui":
        start_all(include_streamlit=False)
    elif cmd == "stop":
        stop_all()
    elif cmd == "stop_services":
        stop_services()
    elif cmd == "status":
        status()
    else:
        print("Unknown command. Use start|start_noui|stop|stop_services|status")
        sys.exit(1)

if __name__ == "__main__":
    main()
