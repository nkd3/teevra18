# C:\teevra18\app\pages\Status_API_Connectivity.py
# M12 Control Panel: Status Window — API Connectivity
# Requires: streamlit, requests, pydantic (optional but recommended)

import json
import os
import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Dict, Tuple

import requests
import streamlit as st

# -----------------------------
# Config helpers
# -----------------------------
DEFAULT_CONFIG_PATH = Path(r"C:\teevra18\teevra18.config.json")

def load_config(path: Path = DEFAULT_CONFIG_PATH) -> Dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

CFG = load_config()

def get_cfg(path: str, default=None):
    # simple dotted-path reader, e.g. get_cfg("telegram.bot_token")
    node = CFG
    for part in path.split("."):
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            return default
    return node

# -----------------------------
# Status check utilities
# -----------------------------
def status_bad(message: str) -> Tuple[str, str]:
    return "🔴", message

def status_warn(message: str) -> Tuple[str, str]:
    return "🟠", message

def status_ok(message: str) -> Tuple[str, str]:
    return "🟢", message

def with_timeout():
    return int(get_cfg("network.timeout_seconds", 6))

# -----------------------------
# Checks
# -----------------------------
def check_sqlite() -> Tuple[str, str]:
    db_path = get_cfg("db_path", r"C:\teevra18\data\teevra18.db")
    try:
        if not Path(db_path).exists():
            return status_bad(f"DB file not found: {db_path}")
        con = sqlite3.connect(db_path, timeout=3)
        try:
            cur = con.cursor()
            cur.execute("SELECT 1;")
            cur.fetchone()
        finally:
            con.close()
        return status_ok(f"Connected OK → {db_path}")
    except Exception as e:
        return status_bad(f"DB error: {e}")

def check_parquet_dir() -> Tuple[str, str]:
    pdir = Path(get_cfg("parquet_dir", r"C:\teevra18\history"))
    try:
        if not pdir.exists():
            return status_bad(f"Parquet dir missing: {pdir}")
        # Try write test (create + delete a temp file)
        tmp = None
        try:
            fd, tmp = tempfile.mkstemp(prefix="t18_", suffix=".tmp", dir=str(pdir))
            os.close(fd)
            os.remove(tmp)
        except PermissionError:
            return status_bad(f"No write permission: {pdir}")
        except Exception as e:
            return status_warn(f"Write test issue ({pdir}): {e}")
        return status_ok(f"Exists & writable → {pdir}")
    except Exception as e:
        return status_bad(f"Parquet check error: {e}")

def check_telegram() -> Tuple[str, str]:
    token = get_cfg("telegram.bot_token", "")
    if not token or token.startswith("<"):
        return status_warn("No/placeholder Telegram token in config.")
    try:
        url = f"https://api.telegram.org/bot{token}/getMe"
        r = requests.get(url, timeout=with_timeout())
        if r.status_code == 200 and r.json().get("ok") is True:
            return status_ok("Bot reachable (getMe ok).")
        return status_bad(f"getMe failed: HTTP {r.status_code} → {r.text[:120]}")
    except requests.exceptions.ConnectTimeout:
        return status_bad("Telegram timeout (network?).")
    except requests.exceptions.RequestException as e:
        return status_bad(f"Telegram error: {e}")

def check_dhan() -> Tuple[str, str]:
    # Non-destructive endpoint. We’ll call a safe GET and treat 200 as OK; 401/403 as auth error (still shows host reachable).
    base = get_cfg("dhan.rest_base", "https://api.dhan.co").rstrip("/")
    token = get_cfg("dhan.access_token", "")
    if not token or token.startswith("<"):
        return status_warn("No/placeholder Dhan access_token in config.")
    try:
        # A read-only order list call; if unauthorized, we’ll still confirm connectivity
        url = f"{base}/orders"
        r = requests.get(url, headers={"access-token": token}, timeout=with_timeout())
        if r.status_code == 200:
            return status_ok("DhanHQ reachable (auth OK).")
        elif r.status_code in (401, 403):
            return status_bad(f"DhanHQ reachable but auth failed (HTTP {r.status_code}).")
        else:
            return status_bad(f"DhanHQ unexpected HTTP {r.status_code}: {r.text[:120]}")
    except requests.exceptions.ConnectTimeout:
        return status_bad("DhanHQ timeout (network?).")
    except requests.exceptions.RequestException as e:
        return status_bad(f"DhanHQ error: {e}")

# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Teevra18 — Status", page_icon="✅", layout="wide")
st.title("M12 • Status Window: API Connectivity")

with st.expander("Config Snapshot", expanded=False):
    st.write("Using config:", str(DEFAULT_CONFIG_PATH))
    st.json({
        "db_path": get_cfg("db_path"),
        "parquet_dir": get_cfg("parquet_dir"),
        "telegram": {"bot_token_set": bool(get_cfg("telegram.bot_token"))},
        "dhan": {
            "rest_base": get_cfg("dhan.rest_base"),
            "access_token_set": bool(get_cfg("dhan.access_token"))
        },
        "network": {"timeout_seconds": get_cfg("network.timeout_seconds", 6)}
    })

colA, colB, colC, colD = st.columns(4)

# Re-check button forces rerun
if st.button("🔄 Re-check"):
    st.experimental_rerun()

# Run checks
tg_icon, tg_msg = check_telegram()
dh_icon, dh_msg = check_dhan()
db_icon, db_msg = check_sqlite()
pq_icon, pq_msg = check_parquet_dir()

with colA:
    st.subheader("Telegram")
    st.markdown(f"{tg_icon} {tg_msg}")

with colB:
    st.subheader("DhanHQ")
    st.markdown(f"{dh_icon} {dh_msg}")

with colC:
    st.subheader("Database")
    st.markdown(f"{db_icon} {db_msg}")

with colD:
    st.subheader("Parquet Path")
    st.markdown(f"{pq_icon} {pq_msg}")

st.caption("Tip: Use the 🔄 Re-check button after fixing tokens or paths to refresh statuses.")
