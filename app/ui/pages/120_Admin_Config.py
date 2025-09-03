import streamlit as st
import pandas as pd

from services.config_admin import (
    list_configs,
    soft_delete_config,
    reset_config,
    restore_config,
)

st.set_page_config(page_title="Config Admin | Teevra18", layout="wide")

st.title("Config Admin — Delete / Reset / Restore")

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

    options = [f"{r[1]}  |  {r[0]}  |  status:{r[2]}" for r in rows]
    choice = st.selectbox("Choose a config", options, index=0)
    chosen_id = choice.split("|")[1].strip()
    current_status = choice.split("status:")[-1].strip()

with colR:
    st.subheader("Action")
    # Action options adapt to current status
    actions = ["Soft Delete", "Reset (Purge)"]
    if current_status == "deleted":
        actions.append("Restore (Undelete)")
    action = st.radio("What do you want to do?", actions, index=0)

    if action == "Soft Delete":
        reason = st.text_area("Reason (optional)")
        confirm_id = st.text_input("Type the Config ID to confirm:", value="", placeholder=chosen_id)
        disabled = (confirm_id.strip() != chosen_id)
        do_it = st.button("🗑️ Soft Delete", type="primary", disabled=disabled)

        if do_it:
            with st.spinner("Applying soft delete..."):
                soft_delete_config(chosen_id, reason=reason, actor="ui-admin")
            st.success(f"Config {chosen_id} soft-deleted.")
            st.rerun()

    elif action == "Reset (Purge)":
        st.caption("Reset purges children: params, policies, liquidity, notifications. It keeps the master id.")
        confirm_id = st.text_input("Type the Config ID to confirm:", value="", placeholder=chosen_id)
        st.warning("This cannot be undone from UI. Ensure you have a DB backup.")
        do_it = st.button("♻️ Reset (Purge)", type="primary", disabled=(confirm_id.strip() != chosen_id))
        if do_it:
            with st.spinner("Purging child records..."):
                reset_config(chosen_id, actor="ui-admin")
            st.success(f"Config {chosen_id} reset completed.")
            st.rerun()

    elif action == "Restore (Undelete)":
        st.caption("Restore will make the config active again and clear the deleted timestamp.")
        confirm_id = st.text_input("Type the Config ID to confirm:", value="", placeholder=chosen_id)
        do_it = st.button("🧩 Restore (Undelete)", type="primary", disabled=(confirm_id.strip() != chosen_id))
        if do_it:
            with st.spinner("Restoring config..."):
                restore_config(chosen_id, actor="ui-admin")
            st.success(f"Config {chosen_id} restored to active.")
            st.rerun()

