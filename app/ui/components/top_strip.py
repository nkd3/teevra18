# -*- coding: utf-8 -*-
import json
from pathlib import Path
import streamlit as st

STATUS_PATH_DEFAULT = Path(r"C:\teevra18_runtime\status.json")

def _status_class(v: str) -> str:
    v = (v or "").lower()
    if v == "ok": return "ok"
    if v in ("warn","warning","degraded"): return "warn"
    return "err"

@st.cache_data(ttl=5.0)
def load_status(p: Path) -> dict:
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "telegram":"err","dhanhq":"err","sqlite":"err","parquet":"err",
            "latency_ms": None, "last_check": None
        }

def render_top_strip(status_file: str = None, logo_path: str = None):
    status = load_status(Path(status_file) if status_file else STATUS_PATH_DEFAULT)
    tel_class = _status_class(status.get("telegram"))
    dhn_class = _status_class(status.get("dhanhq"))
    db_class  = _status_class(status.get("sqlite"))
    pq_class  = _status_class(status.get("parquet"))

    logo_path = logo_path or r"C:\teevra18\assets\Teevra18_Logo.png"

    st.markdown("""
    <div class="t18-top-strip">
      <img src="app/assets/Teevra18_Logo.png" alt="logo" style="height:28px;opacity:.95;border-radius:6px;" onerror="this.style.display='none';"/>
      <div class="t18-chip {tel_class}"><span class="t18-dot"></span>Telegram</div>
      <div class="t18-chip {dhn_class}"><span class="t18-dot"></span>DhanHQ</div>
      <div class="t18-chip {db_class}"><span class="t18-dot"></span>Database</div>
      <div class="t18-chip {pq_class}"><span class="t18-dot"></span>Parquet</div>
      <div style="flex:1;"></div>
      <div class="t18-chip"><b>Latency</b>&nbsp;{lat} ms</div>
      <button onclick="window.location.reload()" class="t18-chip" style="cursor:pointer;">Re-check</button>
    </div>
    """.format(
        tel_class=tel_class, dhn_class=dhn_class, db_class=db_class, pq_class=pq_class,
        lat=(status.get("latency_ms") or "—")
    ), unsafe_allow_html=True)
