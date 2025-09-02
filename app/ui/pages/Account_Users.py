import streamlit as st
import sqlite3
from pathlib import Path
from auth import require_role

st.set_page_config(page_title="User Accounts â€¢ TeeVra18", page_icon="??", layout="wide")

if not require_role(st.session_state, "admin"):
    st.error("Access denied. Admins only.")
    st.stop()

DB_PATH = Path(r"C:\teevra18\data\teevra18.db")

st.title("User Accounts")

with sqlite3.connect(DB_PATH) as conn:
    cur = conn.cursor()
    users = cur.execute("SELECT id, username, role, is_active, created_at FROM users ORDER BY id").fetchall()

st.dataframe(users, use_container_width=True, column_config={
    0: st.column_config.NumberColumn("ID"),
    1: st.column_config.TextColumn("Username"),
    2: st.column_config.TextColumn("Role"),
    3: st.column_config.CheckboxColumn("Active"),
    4: st.column_config.TextColumn("Created At"),
})




