# -*- coding: utf-8 -*-
import json
import sqlite3
from pathlib import Path
from uuid import uuid4
import streamlit as st
import os

try:
    import yaml  # pip install pyyaml
except Exception:
    yaml = None

# --- Single Source of Truth: resolve DB from teevra18.config.json (fallback to C:\teevra18\data\teevra18.db)
def _resolve_db_path():
    # Try both root and configs/ locations
    for p in [
        os.path.join(os.getcwd(), "teevra18.config.json"),
        os.path.join(os.getcwd(), "configs", "teevra18.config.json"),
    ]:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                dbp = (cfg.get("paths", {}) or {}).get("sqlite")
                if dbp:
                    os.makedirs(os.path.dirname(dbp), exist_ok=True)
                    return dbp
            except Exception:
                pass
    # Fallback dev path
    return r"C:\teevra18\data\teevra18.db"

DB_PATH = _resolve_db_path()

CATALOG_PATH = Path(r"C:\teevra18\data\dhan_indicators_full.json")
FALLBACK_PATH = Path(r"C:\teevra18\data\dhan_indicators.json")

st.title("🧪 Strategy Lab: All DhanHQ Indicators")

@st.cache_data(show_spinner=False)
def load_catalog():
    if CATALOG_PATH.exists():
        data = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
        inds = data.get("indicators", {})
        aliases = data.get("meta", {}).get("aliases", {})
        meta = data.get("meta", {})
        meta.setdefault("count", len(inds))
        meta.setdefault("source", "pandas_ta")
        return inds, aliases, meta
    if FALLBACK_PATH.exists():
        inds = json.loads(FALLBACK_PATH.read_text(encoding="utf-8"))
        return inds, {}, {"source": "fallback", "count": len(inds)}
    return {
        "EMA": {"params": {"length": 9}},
        "SMA": {"params": {"length": 20}},
        "RSI": {"params": {"length": 14}},
        "MACD": {"params": {"fast": 12, "slow": 26, "signal": 9}},
        "ATR": {"params": {"length": 14}},
        "BBANDS": {"params": {"length": 20, "stdev": 2.0}},
        "SUPERTREND": {"params": {"length": 10, "multiplier": 3.0}},
    }, {}, {"source": "minimal", "count": 7}

INDICATORS, ALIASES, META = load_catalog()

def normalize_key(name: str) -> str:
    k = name.strip().upper().replace("-", " ").replace("_", " ")
    return ALIASES.get(k, k.replace(" ", "_"))

# --- Ensure required tables exist (idempotent)
def _ensure_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    # strategies_catalog – your existing Lab table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS strategies_catalog(
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            params_json TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            source_mode TEXT NOT NULL
        );
    """)
    # strategy_config – Admin’s master list (used by Admin_Config)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS strategy_config(
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT,
            status TEXT DEFAULT 'active',
            deleted_at TEXT
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_config_status ON strategy_config(status);")
    conn.commit()

# --- Upsert into strategy_config so Admin page sees Lab saves
def _upsert_strategy_config(conn: sqlite3.Connection, cfg_id: str, name: str):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO strategy_config(id, name, created_at, updated_at, status)
        VALUES(?, ?, datetime('now'), datetime('now'), 'active')
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            updated_at=excluded.updated_at,
            status='active';
    """, (cfg_id, name))
    conn.commit()

mode = st.radio("Choose Mode", ["Graphical", "Script"], horizontal=True)

conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA foreign_keys = ON;")
_ensure_schema(conn)
cur = conn.cursor()

if mode == "Graphical":
    st.caption(f"Catalog source: {META.get('source','pandas_ta')} · Indicators: {META.get('count', len(INDICATORS))}")
    q = st.text_input("Search indicators", placeholder="e.g., rsi, ema, band, trend…", key="slab_q").strip().upper()
    all_names = sorted(INDICATORS.keys())
    names = [q] if (q and (q in all_names)) else ([n for n in all_names if q in n] if q else all_names)

    if "slab_ms" not in st.session_state:
        st.session_state.slab_ms = []
    col_btn, col_ms = st.columns([1, 3])
    with col_btn:
        if st.button("Select all (filtered)", use_container_width=True) and names:
            st.session_state.slab_ms = names
            st.rerun()
    with col_ms:
        st.multiselect(
            "Select indicators",
            options=names,
            default=[x for x in st.session_state.slab_ms if x in names],
            key="slab_ms",
            placeholder="Pick one or more…",
        )
    selected = st.session_state.slab_ms
    params = {}
    for ind in selected:
        pdef = INDICATORS.get(ind, {}).get("params", {}) or {}
        with st.expander(ind.replace("_", " "), expanded=False):
            params[ind] = {}
            for p, default in pdef.items():
                if isinstance(default, bool):
                    val = st.checkbox(f"{p}", value=default, key=f"p_{ind}_{p}")
                elif isinstance(default, int) or (isinstance(default, float) and float(default).is_integer()):
                    val = st.number_input(f"{p}", value=int(default), step=1, key=f"p_{ind}_{p}")
                elif isinstance(default, (int, float)):
                    val = st.number_input(f"{p}", value=float(default), step=0.1, format="%.4f", key=f"p_{ind}_{p}")
                else:
                    val = st.text_input(f"{p}", value=str(default), key=f"p_{ind}_{p}")
                params[ind][p] = val
    st.divider()
    strat_name = st.text_input("Strategy name", value="Custom Strategy", key="slab_name")
    save_ok = bool(selected) and bool(strat_name.strip())
    if st.button("Save Strategy", type="primary", disabled=not save_ok):
        try:
            payload = {ind: {"params": params.get(ind, {})} for ind in selected}
            strategy_id = f"lab_{uuid4().hex[:8]}"

            # 1) Save as before (Lab’s own catalog)
            cur.execute(
                "INSERT INTO strategies_catalog(strategy_id,name,params_json,enabled,source_mode) VALUES (?,?,?,?,?)",
                (strategy_id, strat_name.strip(), json.dumps(payload), 1, "graphical"),
            )
            conn.commit()

            # 2) NEW: Upsert into Admin’s master list so it appears in /Admin_Config
            _upsert_strategy_config(conn, strategy_id, strat_name.strip())

            st.success(f"Saved {len(selected)} indicators as '{strat_name.strip()}' (id={strategy_id})")
        except Exception as e:
            st.error(f"Save failed: {e}")

else:
    st.caption("Paste YAML or JSON mapping of indicator -> {params: {...}}. Names like ‘Bollinger Bands’ normalize to BBANDS.")
    example = """# YAML example
EMA:
  params:
    length: 9
Bollinger Bands:
  params:
    length: 20
    stdev: 2.0
"""
    script_text = st.text_area("Paste YAML or JSON config", height=240, value="", placeholder=example, key="slab_script")
    strat_name_script = st.text_input("Strategy name", value="Script Strategy", key="slab_script_name")
    if st.button("Validate & Save", type="primary", disabled=(not script_text.strip() or not strat_name_script.strip())):
        try:
            cfg = None
            try:
                cfg = json.loads(script_text)
            except Exception:
                if yaml is None:
                    raise RuntimeError("YAML not available. Run: pip install pyyaml")
                cfg = yaml.safe_load(script_text)
            if not isinstance(cfg, dict) or not cfg:
                raise ValueError("Config must be a non-empty mapping of indicators to params.")
            norm_cfg = {}
            for k, v in cfg.items():
                k2u = k.strip().upper().replace("-", " ").replace("_", " ")
                k2n = ALIASES.get(k2u, k2u.replace(" ", "_"))
                norm_cfg[k2n] = v

            strategy_id = f"lab_{uuid4().hex[:8]}"

            # 1) Save in Lab catalog
            cur.execute(
                "INSERT INTO strategies_catalog(strategy_id,name,params_json,enabled,source_mode) VALUES (?,?,?,?,?)",
                (strategy_id, strat_name_script.strip(), json.dumps(norm_cfg), 1, "script"),
            )
            conn.commit()

            # 2) Upsert into Admin’s master list
            _upsert_strategy_config(conn, strategy_id, strat_name_script.strip())

            st.success(f"Imported & saved '{strat_name_script.strip()}' (id={strategy_id})")
        except Exception as e:
            st.error(f"Validation failed: {e}")

st.divider()
st.subheader("Recently saved strategies")
try:
    rows = cur.execute(
        "SELECT rowid, strategy_id, name, source_mode FROM strategies_catalog ORDER BY rowid DESC LIMIT 15"
    ).fetchall()
    if rows:
        for rowid, sid, name, src in rows:
            st.write(f"• **{name}**  —  id: `{sid}` · mode: `{src}`")
    else:
        st.caption("No strategies saved yet.")
except Exception as e:
    st.caption(f"(Could not list strategies: {e})")

conn.close()
