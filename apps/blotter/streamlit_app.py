import sqlite3, pandas as pd, streamlit as st

DB = r"C:\teevra18\data\teevra18.db"

@st.cache_data(ttl=5.0)
def load_df(sql):
    with sqlite3.connect(DB) as conn:
        return pd.read_sql(sql, conn)

st.set_page_config(page_title="Teevra18 Blotter", layout="wide")

st.title("📈 TeeVra18 Paper Trading Blotter")

col1, col2 = st.columns(2)
with col1:
    st.subheader("paper_orders (latest 50)")
    df_o = load_df("""
        SELECT id, signal_id, symbol, side, status, state,
               entry, sl, tp, entry_price, fill_price, exit_price,
               pnl_gross, pnl_net, delayed_fill_at, filled_ts_utc, closed_ts_utc
        FROM paper_orders
        ORDER BY id DESC LIMIT 50
    """)
    st.dataframe(df_o, width='stretch')

with col2:
    st.subheader("ops_log (latest 100)")
    # use the view so we have an id to sort on
    df_l = load_df("""
        SELECT id, ts_utc, level, area, msg, source, event, ref_table, ref_id, message
        FROM v_ops_log_with_id
        ORDER BY id DESC LIMIT 100
    """)
    st.dataframe(df_l, width='stretch')

st.caption("Auto-refresh every ~5s via cached ttl")
