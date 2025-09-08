# -*- coding: utf-8 -*-
from __future__ import annotations
import json
from pathlib import Path
import streamlit as st

CONFIG_FILE = Path(__file__).with_name("top_nav.config.json")

def _safe_logo(logo_path: str | None):
    if not logo_path:
        return None
    p = Path(logo_path)
    if p.exists() and p.is_file():
        return str(p)
    return None

def render_top_nav(user_name: str | None = None):
    try:
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception:
        cfg = {"brand": {"title": "Teevra18", "icon": "🧭", "logo_path": None}, "items": []}

    brand = cfg.get("brand", {}) or {}
    items = cfg.get("items", []) or []

    logo_path = _safe_logo(brand.get("logo_path"))
    brand_icon = brand.get("icon", "🧭")
    brand_title = brand.get("title", "Teevra18")

    c1, c2, c3 = st.columns([2, 5, 2], vertical_alignment="center")

    with c1:
        cols = st.columns([1, 8], vertical_alignment="center")
        with cols[0]:
            if logo_path:
                st.image(logo_path, width=32)
            else:
                st.markdown(f"<div style='font-size:22px'>{brand_icon}</div>", unsafe_allow_html=True)
        with cols[1]:
            st.markdown(
                f"<div style='font-size:18px;font-weight:700;margin-top:2px'>{brand_title}</div>",
                unsafe_allow_html=True
            )

    with c2:
        pill_cols = st.columns(len(items) or 1)
        for i, it in enumerate(items):
            lbl = f"{it.get('icon','')} {it.get('label','').upper()}".strip()
            tgt = it.get("target")
            with pill_cols[i]:
                try:
                    st.page_link(tgt, label=lbl)
                except Exception:
                    st.button(lbl, disabled=True)

    with c3:
        st.markdown(
            f"<div style='text-align:right;font-size:14px'>👤 {user_name or 'Guest'}</div>",
            unsafe_allow_html=True
        )

    st.markdown("<hr style='margin:8px 0 2px 0;'/>", unsafe_allow_html=True)
