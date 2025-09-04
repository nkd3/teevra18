# -*- coding: utf-8 -*-
"""
Strategy_Lab.py
- M12 / #tm12 Item 9: "Indicator/Universe persistence & import/export"
- Acceptance: Export includes universe + indicators; Import restores them exactly.
- Repo context: #t18repo https://github.com/nkd3/teevra18  #t18sync

Environment: Windows + Python 3.11 + Streamlit + SQLite
"""

import json
import sqlite3
from pathlib import Path
from uuid import uuid4
import streamlit as st
import os

try:
    import yaml  # pip install pyyaml (optional for YAML import/export)
except Exception:
    yaml = None


# --- Resolve DB from teevra18.config.json (fallback to C:\teevra18\data\teevra18.db)
def _resolve_db_path():
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
    return r"C:\teevra18\data\teevra18.db"


DB_PATH = _resolve_db_path()
EXPORT_DIR = Path(r"C:\teevra18\data\exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# Catalog sources for All DhanHQ Indicators UI
CATALOG_PATH = Path(r"C:\teevra18\data\dhan_indicators_full.json")
FALLBACK_PATH = Path(r"C:\teevra18\data\dhan_indicators.json")

# --- Universe & Indicators tables included in persistence (export/import)
#     Extended with rr_profiles and rr_overrides as requested.
PERSIST_TABLES = [
    "universe_underlyings",
    "universe_derivatives",
    "universe_watchlist",
    "policies_group",
    "policies_instrument",
    "strategies_catalog",   # stores saved indicator sets from Lab
    "rr_profiles",
    "rr_overrides",
]

# --- Import order (parents first), delete order (children first) for FK safety
IMPORT_ORDER = [
    "universe_underlyings",
    "rr_profiles",
    "strategies_catalog",
    "policies_group",
    "policies_instrument",
    "universe_derivatives",
    "rr_overrides",
    "universe_watchlist",
]
DELETE_ORDER = list(reversed(IMPORT_ORDER))

st.title("🧪 Strategy Lab: All DhanHQ Indicators")


@st.cache_data(show_spinner=False)
def load_catalog():
    """Load indicator catalog for the Graphical mode."""
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
    # Minimal built-ins
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


# --- Schema helpers ---------------------------------------------------------
def _ensure_schema(conn: sqlite3.Connection):
    """Ensure required tables exist; add columns if needed to avoid import errors."""
    cur = conn.cursor()
    # strategies_catalog – Lab history of saved indicator sets
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS strategies_catalog(
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            params_json TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            source_mode TEXT NOT NULL
        );
        """
    )
    # Ensure optional 'version' column exists (your export includes it)
    _ensure_column(conn, "strategies_catalog", "version", "TEXT")

    # strategy_config – Admin master list (so Admin can "see" Lab saves)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_config(
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT,
            status TEXT DEFAULT 'active',
            deleted_at TEXT
        );
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_strategy_config_status ON strategy_config(status);"
    )

    # RR tables (lightweight guards if not present)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS rr_profiles(
            profile_name TEXT PRIMARY KEY,
            rr_min REAL,
            sl_cap_per_lot REAL,
            sl_method TEXT,
            tp_method TEXT,
            tp_factor REAL,
            spread_buffer_ticks REAL,
            min_liquidity_lots REAL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS rr_overrides(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_name TEXT,
            symbol TEXT,
            override_json TEXT,
            FOREIGN KEY(profile_name) REFERENCES rr_profiles(profile_name)
        );
        """
    )
    conn.commit()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, coltype: str):
    cur = conn.cursor()
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
            conn.commit()
        except Exception:
            # ignore if ALTER not allowed in some edge cases
            pass


# --- Upsert into strategy_config so Admin page sees Lab saves
def _upsert_strategy_config(conn: sqlite3.Connection, cfg_id: str, name: str):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO strategy_config(id, name, created_at, updated_at, status)
        VALUES(?, ?, datetime('now'), datetime('now'), 'active')
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            updated_at=excluded.updated_at,
            status='active';
        """,
        (cfg_id, name),
    )
    conn.commit()


# --- Export / Import helpers (Item 9 acceptance) ---------------------------
def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    r = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,)
    ).fetchone()
    return r is not None


def export_universe_and_indicators(fmt: str = "json") -> Path:
    """Export PERSIST_TABLES to a single file (json or yaml)."""
    conn = sqlite3.connect(DB_PATH)
    data = {}
    try:
        for t in PERSIST_TABLES:
            if not _table_exists(conn, t):
                data[t] = []
                continue
            rows = conn.execute(f"SELECT * FROM {t}").fetchall()
            cols = [c[1] for c in conn.execute(f"PRAGMA table_info({t})")]
            data[t] = [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()

    outpath = EXPORT_DIR / f"universe_export.{fmt.lower()}"
    if fmt.lower() == "json":
        outpath.write_text(json.dumps(data, indent=2), encoding="utf-8")
    elif fmt.lower() in ("yaml", "yml"):
        if yaml is None:
            raise RuntimeError("PyYAML not installed. Run: pip install pyyaml")
        outpath.write_text(yaml.dump(data, sort_keys=False), encoding="utf-8")
    else:
        raise ValueError("Unsupported format. Use 'json' or 'yaml'.")

    return outpath


def _auto_fill_missing_underlyings(conn: sqlite3.Connection, payload: dict):
    """
    Guardrail:
    If universe_derivatives reference underlying_symbol not present in universe_underlyings,
    create minimal stub rows so future FK checks pass.
    """
    if "universe_derivatives" not in payload:
        return
    cur = conn.cursor()
    existing = set()
    for r in cur.execute("SELECT DISTINCT symbol FROM universe_underlyings").fetchall():
        existing.add(r[0])
    needed = {d.get("underlying_symbol") for d in payload["universe_derivatives"] or []}
    missing = [s for s in needed if s and s not in existing]
    if missing:
        # Create minimal schema if table empty
        cols = [c[1] for c in cur.execute("PRAGMA table_info(universe_underlyings)")]
        if not cols:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS universe_underlyings(
                    symbol TEXT PRIMARY KEY,
                    exchange TEXT,
                    lot_size INTEGER,
                    enabled INTEGER DEFAULT 1,
                    ts_utc TEXT
                );
                """
            )
        for sym in missing:
            cur.execute(
                """
                INSERT OR IGNORE INTO universe_underlyings(symbol, exchange, lot_size, enabled, ts_utc)
                VALUES (?, 'NSE', NULL, 1, datetime('now'))
                """,
                (sym,),
            )
        conn.commit()


def import_universe_and_indicators(file_path: Path):
    """Import (restore) PERSIST_TABLES exactly from a given file. Overwrites rows."""
    if not file_path.exists():
        raise FileNotFoundError(str(file_path))

    # Detect format by extension
    ext = file_path.suffix.lower()
    if ext == ".json":
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    elif ext in (".yaml", ".yml"):
        if yaml is None:
            raise RuntimeError("PyYAML not installed. Run: pip install pyyaml")
        payload = yaml.safe_load(file_path.read_text(encoding="utf-8"))
    else:
        raise ValueError("Unsupported file type. Use .json or .yaml")

    if not isinstance(payload, dict) or not payload:
        raise ValueError("Import file must contain a non-empty object at top level.")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = OFF;")  # disable FKs during bulk restore
    try:
        _ensure_schema(conn)  # make sure tables/columns exist first
        _auto_fill_missing_underlyings(conn, payload)

        cur = conn.cursor()

        # DELETE children first (based on DELETE_ORDER)
        for t in DELETE_ORDER:
            if t in payload and _table_exists(conn, t):
                cur.execute(f"DELETE FROM {t}")

        # INSERT parents first (based on IMPORT_ORDER)
        for t in IMPORT_ORDER:
            rows = payload.get(t, [])
            if not rows:
                continue
            # Align columns with destination table (drop unknown keys, add missing nulls)
            dest_cols = [c[1] for c in cur.execute(f"PRAGMA table_info({t})")]
            # If import contains columns DB doesn't have but we can add safely, try add them
            for k in rows[0].keys():
                if k not in dest_cols:
                    try:
                        cur.execute(f"ALTER TABLE {t} ADD COLUMN {k} TEXT")
                        dest_cols.append(k)
                    except Exception:
                        # ignore if cannot add; we'll drop unknown keys
                        pass

            insert_cols = [c for c in rows[0].keys() if c in dest_cols]
            placeholders = ",".join("?" * len(insert_cols))
            sql = f"INSERT INTO {t} ({','.join(insert_cols)}) VALUES ({placeholders})"
            for r in rows:
                values = tuple((r.get(c) if c in r else None) for c in insert_cols)
                cur.execute(sql, values)

        conn.commit()
    finally:
        # Re-enable FK checks (no pragma to "validate" in SQLite, but we can run a simple pass)
        try:
            conn.execute("PRAGMA foreign_keys = ON;")
        except Exception:
            pass
        conn.close()


# --- Main UI ---------------------------------------------------------------
mode = st.radio("Choose Mode", ["Graphical", "Script"], horizontal=True)

conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA foreign_keys = ON;")
_ensure_schema(conn)
cur = conn.cursor()

# ------------------------
# Graphical indicator picker
# ------------------------
if mode == "Graphical":
    st.caption(
        f"Catalog source: {META.get('source','pandas_ta')} · Indicators: {META.get('count', len(INDICATORS))}"
    )
    q = st.text_input(
        "Search indicators", placeholder="e.g., rsi, ema, band, trend…", key="slab_q"
    ).strip().upper()
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

            # 1) Save in Lab catalog
            cur.execute(
                "INSERT INTO strategies_catalog(strategy_id,name,params_json,enabled,source_mode) VALUES (?,?,?,?,?)",
                (strategy_id, strat_name.strip(), json.dumps(payload), 1, "graphical"),
            )
            conn.commit()

            # 2) Auto-upsert into Admin master list
            _upsert_strategy_config(conn, strategy_id, strat_name.strip())

            st.success(f"Saved {len(selected)} indicators as '{strat_name.strip()}' (id={strategy_id})")
        except Exception as e:
            st.error(f"Save failed: {e}")

# ------------------------
# Script (YAML/JSON) mode
# ------------------------
else:
    st.caption(
        "Paste YAML or JSON mapping of indicator -> {params: {...}}. "
        "Names like ‘Bollinger Bands’ normalize to BBANDS."
    )
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

            # 2) Auto-upsert into Admin master list
            _upsert_strategy_config(conn, strategy_id, strat_name_script.strip())

            st.success(f"Imported & saved '{strat_name_script.strip()}' (id={strategy_id})")
        except Exception as e:
            st.error(f"Validation failed: {e}")

st.divider()

# ------------------------
# Persistence: Universe + Indicators + RR (Export / Import)
# ------------------------
st.subheader("📦 Universe, Indicators & RR Persistence")
col_exp, col_fmt = st.columns([2, 1])
with col_exp:
    if st.button("Export Universe + Indicators + RR", use_container_width=True):
        try:
            out = export_universe_and_indicators(fmt="json")  # lossless default
            st.success(f"Exported to: {out}")
            st.caption("Tip: You can also export YAML by changing fmt='yaml' in code if PyYAML is installed.")
        except Exception as e:
            st.error(f"Export failed: {e}")

with col_fmt:
    st.caption("Exports to C:\\teevra18\\data\\exports")

uploaded = st.file_uploader("Import Snapshot (.json or .yaml)", type=["json", "yaml", "yml"])
if uploaded is not None:
    try:
        tmp_path = EXPORT_DIR / uploaded.name
        tmp_path.write_bytes(uploaded.getvalue())
        import_universe_and_indicators(tmp_path)
        st.success("Import complete. State restored exactly. Consider restarting services/UI.")
    except Exception as e:
        st.error(f"Import failed: {e}")

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
