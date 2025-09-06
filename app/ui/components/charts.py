# -*- coding: utf-8 -*-
import pandas as pd
import streamlit as st
import altair as alt

def pnl_bar(df: pd.DataFrame, ts_col="ts", val_col="pnl", height=220):
    if df.empty:
        st.info("No P/L data for today yet.")
        return
    df_plot = df.copy()
    df_plot[ts_col] = pd.to_datetime(df_plot[ts_col])
    df_plot["sign"] = (df_plot[val_col] >= 0).astype(int)
    c = alt.Chart(df_plot).mark_bar().encode(
        x=alt.X(f"{ts_col}:T", axis=alt.Axis(title="Time")),
        y=alt.Y(f"{val_col}:Q", axis=alt.Axis(title="P/L")),
        tooltip=[ts_col, val_col]
    ).properties(height=height)
    st.altair_chart(c, use_container_width=True)
