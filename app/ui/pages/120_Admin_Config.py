# pages/120_Admin_Config.py
import streamlit as st
import pandas as pd
from services.config_admin import list_configs, soft_delete_config, reset_config

st.set_page_config(page_title="Config Admin | Teevra18", layout="wide")

st.title("Config Admin — Delete / Reset Strategy")

colL, colR = st.columns([2, 1], gap="large")

with colL:
    st.subheader("Select Config")
    show_deleted = st.toggle("Show deleted", value=False, help="Include soft-deleted configs in the list")
    rows = list_configs(include_deleted=show_deleted)
    if not rows:
        st.info("No configs available.")
        st.stop()

    df = pd.DataFrame(rows, columns=["id", "name", "status"])
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Friendly label for selection
    options = [f"{r[1]}  |  {r[0]}  |  status:{r[2]}" for r in rows]
    choice = st.selectbox("Choose a config", options, index=0)
    chosen_id = choice.split("|")[1].strip()

with colR:
    st.subheader("Action")
    action = st.radio("What do you want to do?", ["Soft Delete", "Reset (Purge)"], index=0,
                      help="Soft Delete keeps the config row but marks it deleted. Reset removes params/policies/liquidity/notifications but keeps the same config id.")

    if action == "Soft Delete":
        reason = st.text_area("Reason (optional)")
        confirm_id = st.text_input(f"Type the Config ID to confirm:", value="", placeholder=chosen_id)
        disabled = (confirm_id.strip() != chosen_id)
        do_it = st.button("🗑️ Soft Delete", type="primary", disabled=disabled)

        if do_it:
            with st.spinner("Applying soft delete..."):
                soft_delete_config(chosen_id, reason=reason, actor="ui-admin")
            st.success(f"Config {chosen_id} soft-deleted.")
            st.rerun()

    else:
        st.caption("Reset purges children: params, policies, liquidity, notifications. It keeps the master id.")
        confirm_id = st.text_input(f"Type the Config ID to confirm:", value="", placeholder=chosen_id)
        st.warning("This cannot be undone from UI. Ensure you have a DB backup.")
        do_it = st.button("♻️ Reset (Purge)", type="primary", disabled=(confirm_id.strip() != chosen_id))
        if do_it:
            with st.spinner("Purging child records..."):
                reset_config(chosen_id, actor="ui-admin")
            st.success(f"Config {chosen_id} reset completed.")
            st.rerun()
