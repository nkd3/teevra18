import streamlit as st

st.set_page_config(page_title="My Profile • TeeVra18", page_icon="??", layout="wide")

user = st.session_state.get("user")
if not user:
    st.error("Please sign in from the landing page.")
    st.stop()

st.title("My Profile")
st.write(f"Username: **{user['username']}**")
st.write(f"Role: **{user['role']}**")
