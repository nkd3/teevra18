import streamlit as st
if "t18_auth_user" not in st.session_state:
    st.switch_page("C:/teevra18/app/ui/Home_Landing.py")

# -*- coding: utf-8 -*-
"""
Teevra18 Dark Dashboard (M12)
- Top strip with runner lights
- 4–6 metric cards
- P/L bar chart
- Positions + Alerts mini-panels
"""
import os, sqlite3, json
from pathlib import Path
import pandas as pd
import streamlit as st
from datetime import datetime, date, timedelta

# Components
from ui.components.top_strip import render_top_strip
from ui.components.cards import metric_card
from ui.components.charts import pnl_bar

# --- THEME + PAGE CONFIG ---
st.set_page_config(
    page_title="Teevra18 | Dashboard",
    page_icon="🟣",
    layout="wide",
)
# Inject CSS
css_path = Path(r"C:\teevra18\app\static\theme_dark.css")
if css_path.exists():
    st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

# Robust logo
logo_file = Path(r"C:\teevra18\assets\Teevra18_Logo.png")
if logo_file.exists():
    st.image(str(logo_file), width=110)

# --- CONFIG / PATHS ---
DB_PATH = Path(r"C:\teevra18\data\teevra18.db")
STATUS_JSON = Path(r"C:\teevra18_runtime\status.json")
PARQUET_ROOT = Path(r"C:\teevra18\data\history")

# --- DATA LOADERS ---
@st.cache_data(ttl=3.0)
def load_status():
    try:
        with open(STATUS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

@st.cache_data(ttl=3.0)
def df_positions() -> pd.DataFrame:
    if not DB_PATH.exists(): return pd.DataFrame()
    try:
        with sqlite3.connect(DB_PATH) as con:
            return pd.read_sql_query("""
            SELECT symbol, qty, avg_price, ltp, 
                   ROUND((ltp-avg_price)*qty,2) AS mtm
            FROM positions_live
            ORDER BY ABS((ltp-avg_price)*qty) DESC
            LIMIT 8
            """, con)
    except Exception:
        # Placeholder
        return pd.DataFrame([
            {"symbol":"NIFTY24SEP17800CE","qty":50,"avg_price":102.5,"ltp":115.2,"mtm":635.0},
            {"symbol":"BANKNIFTY24SEP45000PE","qty":25,"avg_price":210.0,"ltp":187.5,"mtm":-562.5},
        ])

@st.cache_data(ttl=3.0)
def df_alerts_recent() -> pd.DataFrame:
    if not DB_PATH.exists(): return pd.DataFrame()
    try:
        with sqlite3.connect(DB_PATH) as con:
            return pd.read_sql_query("""
            SELECT ts, level, message, symbol
            FROM alerts_log
            WHERE ts >= datetime('now','-1 hour')
            ORDER BY ts DESC
            LIMIT 10
            """, con)
    except Exception:
        return pd.DataFrame([
            {"ts": datetime.now().isoformat(timespec='seconds'),
             "level":"INFO","message":"RSI<30 watch","symbol":"NIFTY"},
            {"ts": (datetime.now()-timedelta(minutes=4)).isoformat(timespec='seconds'),
             "level":"WARN","message":"Spread wide","symbol":"BANKNIFTY"},
        ])

@st.cache_data(ttl=3.0)
def df_pnl_today() -> pd.DataFrame:
    # Try from a precomputed intraday pnl table
    if DB_PATH.exists():
        try:
            with sqlite3.connect(DB_PATH) as con:
                df = pd.read_sql_query("""
                SELECT ts, pnl
                FROM pnl_intraday_bars
                WHERE DATE(ts) = DATE('now','localtime')
                ORDER BY ts ASC
                """, con)
                if not df.empty: return df
        except Exception:
            pass
    # Fallback: simulate small series (keeps chart alive)
    now = datetime.now().replace(second=0, microsecond=0)
    times = [now - timedelta(minutes=m) for m in range(30)][::-1]
    vals = pd.Series(range(len(times))).apply(lambda i: (i%5-2)*50).cumsum()
    return pd.DataFrame({"ts": times, "pnl": vals})

@st.cache_data(ttl=3.0)
def today_summary(df_pnl: pd.DataFrame, df_pos: pd.DataFrame, alerts_df: pd.DataFrame) -> dict:
    today_pl = float(df_pnl["pnl"].iloc[-1]) if not df_pnl.empty else 0.0
    open_positions = int(df_pos.shape[0])
    open_risk = float(df_pos["mtm"].clip(lower=0).sum()*0.0 + df_pos["avg_price"].sum()*0.0)  # Placeholder: wire your risk calc
    alerts_count = int(alerts_df.shape[0])
    status = load_status()
    latency_ms = status.get("latency_ms", None)
    total_pl_session = today_pl  # adapt if you store cumulative
    return {
        "today_pl": today_pl,
        "total_pl": total_pl_session,
        "open_positions": open_positions,
        "open_risk": open_risk,
        "alerts": alerts_count,
        "latency_ms": latency_ms or 0
    }

# --- RENDER ---
# Top strip (Runner Lights moved here)
render_top_strip(status_file=str(STATUS_JSON))

st.markdown("### Dashboard")

# Data
pnl_df = df_pnl_today()
pos_df = df_positions()
alerts_df = df_alerts_recent()
summary = today_summary(pnl_df, pos_df, alerts_df)

# Cards row (4–6)
c1, c2, c3, c4, c5, c6 = st.columns(6)
with c1: metric_card("Today's P/L", f"₹{summary['today_pl']:.0f}", "As of now", positive=summary['today_pl']>=0)
with c2: metric_card("Total P/L (session)", f"₹{summary['total_pl']:.0f}", positive=summary['total_pl']>=0)
with c3: metric_card("Open Positions", str(summary['open_positions']), sub="count", positive=True)
with c4: metric_card("Open Risk", f"₹{summary['open_risk']:.0f}", sub="model", positive=False)
with c5: metric_card("Alerts (1h)", str(summary['alerts']), sub="recent", positive=summary['alerts']==0)
with c6: metric_card("Latency", f"{summary['latency_ms']} ms", sub="last check", positive=True)

st.markdown('<div class="t18-chart">', unsafe_allow_html=True)
st.subheader("P/L — Today")
pnl_bar(pnl_df)
st.markdown('</div>', unsafe_allow_html=True)

# Mini-panels (Positions + Alerts)
left, right = st.columns([1.3,1])
with left:
    st.markdown('<div class="t18-mini">', unsafe_allow_html=True)
    st.markdown("#### Positions (compact)")
    if pos_df.empty:
        st.info("No positions.")
    else:
        st.dataframe(pos_df, use_container_width=True, hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)

with right:
    st.markdown('<div class="t18-mini">', unsafe_allow_html=True)
    st.markdown("#### Alerts (last 1h)")
    if alerts_df.empty:
        st.info("No recent alerts.")
    else:
        st.dataframe(alerts_df, use_container_width=True, hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)

# Footer nav to align with SampleWireframe sections
st.markdown("---")
colA, colB, colC, colD = st.columns(4)
with colA:
    if st.button("Strategy Lab"):
        st.switch_page("pages/Strategy_Lab.py")  # ensure this exists
with colB:
    if st.button("Trading"):
        st.switch_page("pages/Trading.py")       # ensure this exists
with colC:
    if st.button("Portfolio"):
        st.switch_page("pages/Portfolio.py")     # ensure this exists
with colD:
    if st.button("Reports"):
        st.switch_page("pages/Reports.py")       # ensure this exists
