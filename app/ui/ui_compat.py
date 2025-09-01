# -*- coding: utf-8 -*-
import streamlit as st

def show_image_auto(container, img_path: str):
    try:
        container.image(img_path, use_container_width=True)
    except TypeError:
        container.image(img_path, use_column_width=True)
    except Exception:
        container.image(img_path, width=240)

def metric_row(items):
    cols = st.columns(len(items))
    for i, it in enumerate(items):
        with cols[i]:
            st.metric(it.get("label",""), it.get("value",""), it.get("delta", None))
