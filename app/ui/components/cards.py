# -*- coding: utf-8 -*-
import streamlit as st

def metric_card(title: str, value: str, sub: str = None, positive: bool = True):
    color = "#22c55e" if positive else "#ef4444"
    sub = sub or ""
    st.markdown(f"""
    <div class="t18-card">
      <h4>{title}</h4>
      <div class="big" style="color:{color}">{value}</div>
      <div style="color:#9aa7b3;font-size:12px;margin-top:2px;">{sub}</div>
    </div>
    """, unsafe_allow_html=True)
