# -*- coding: utf-8 -*-
import streamlit as st
import sqlite3
import json, secrets, hashlib, datetime
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import pandas as pd
from auth import require_role

st.set_page_config(page_title="User Accounts • TeeVra18", page_icon="👥", layout="wide")

# --------------------------------------------------------------------
# Access control
# --------------------------------------------------------------------
if not require_role(st.session_state, "admin"):
    st.error("Access denied. Admins only.")
    st.stop()

# --------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------
DB_PATH = Path(r"C:\teevra18\data\teevra18.db")
DATA_DIR = Path(r"C:\teevra18\data")
USERS_JSON = DATA_DIR / "users.json"

# --------------------------------------------------------------------
# Role mapping (UI <-> DB)
# UI shows "user"/"admin"; DB enforces CHECK(role IN ('admin','trader'))
# --------------------------------------------------------------------
UI_TO_DB_ROLE = {"user": "trader", "admin": "admin"}
DB_TO_UI_ROLE = {"trader": "user", "admin": "admin"}

def role_ui_to_db(role_ui: str) -> str:
    return UI_TO_DB_ROLE.get((role_ui or "").strip().lower(), "trader")

def role_db_to_ui(role_db: str) -> str:
    return DB_TO_UI_ROLE.get((role_db or "").strip().lower(), "user")

# --------------------------------------------------------------------
# users.json helpers (auth store)
# --------------------------------------------------------------------
def _load_json() -> Dict[str, Any]:
    if USERS_JSON.exists():
        try:
            return json.loads(USERS_JSON.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"users": {}}

def _save_json(obj: Dict[str, Any]):
    USERS_JSON.parent.mkdir(parents=True, exist_ok=True)
    USERS_JSON.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def _hash_pw(pw: str, salt_hex: str) -> str:
    return hashlib.sha256(bytes.fromhex(salt_hex) + (pw or "").encode("utf-8")).hexdigest()

def upsert_user_json(user_id: str, display: Optional[str], active: bool, password_plain: Optional[str] = None):
    store = _load_json()
    users = store.setdefault("users", {})
    key = user_id.lower().strip()
    rec = users.get(key, {})
    rec["active"] = bool(active)
    rec["display"] = (display or user_id).strip() or user_id
    if password_plain is not None:
        salt = secrets.token_hex(16)
        rec["salt"] = salt
        rec["hash"] = _hash_pw(password_plain, salt)
    else:
        if "salt" not in rec or "hash" not in rec:
            salt = secrets.token_hex(16)
            rec["salt"] = salt
            rec["hash"] = _hash_pw("", salt)
    users[key] = rec
    _save_json(store)

def deactivate_user_json(user_id: str):
    store = _load_json()
    users = store.get("users", {})
    key = user_id.lower().strip()
    if key in users:
        users[key]["active"] = False
        _save_json(store)

# --------------------------------------------------------------------
# SQLite helpers
# --------------------------------------------------------------------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _col_info(conn: sqlite3.Connection, table: str) -> Dict[str, Dict[str, Any]]:
    info = conn.execute(f"PRAGMA table_info({table})").fetchall()
    # row: (cid, name, type, notnull, dflt_value, pk)
    return { row[1]: {"type": row[2], "notnull": int(row[3]) } for row in info }

def _col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    return col in _col_info(conn, table)

def _index_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (name,)).fetchone()
    return row is not None

def _has_pw_cols(conn: sqlite3.Connection) -> Tuple[bool, bool, bool]:
    """
    Returns:
      (has_pw_hash, pw_hash_notnull, has_pw_salt)
    """
    info = _col_info(conn, "users")
    has_hash = "password_hash" in info
    has_salt = "password_salt" in info
    notnull = info.get("password_hash", {}).get("notnull", 0) == 1 if has_hash else False
    return has_hash, notnull, has_salt

def migrate_users_table(conn: sqlite3.Connection):
    # Create base table if missing (with CHECK constraint to satisfy legacy schema)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        role TEXT NOT NULL DEFAULT 'trader' CHECK(role IN ('admin','trader')),
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""")

    # Soft delete columns
    if not _col_exists(conn, "users", "is_deleted"):
        conn.execute("ALTER TABLE users ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0")
    if not _col_exists(conn, "users", "deleted_at"):
        conn.execute("ALTER TABLE users ADD COLUMN deleted_at TEXT")

    # Normalized username
    if not _col_exists(conn, "users", "username_norm"):
        conn.execute("ALTER TABLE users ADD COLUMN username_norm TEXT")
        conn.execute("UPDATE users SET username_norm = lower(trim(username))")

    # Unique on normalized username
    if not _index_exists(conn, "users_unq_username_norm"):
        conn.execute("CREATE UNIQUE INDEX users_unq_username_norm ON users(username_norm)")

    # Backfill/repair null/stale norms
    conn.execute("UPDATE users SET username_norm = lower(trim(username)) WHERE username_norm IS NULL OR username_norm != lower(trim(username))")

    # If password_hash is NOT NULL, ensure existing rows have a value
    has_hash, is_notnull, _ = _has_pw_cols(conn)
    if has_hash and is_notnull:
        conn.execute("UPDATE users SET password_hash = COALESCE(password_hash, '') WHERE password_hash IS NULL")

    conn.commit()

def fetch_users(conn: sqlite3.Connection):
    sql = """
    SELECT id, username, role, is_active, is_deleted, created_at, deleted_at
    FROM users
    ORDER BY id
    """
    return conn.execute(sql).fetchall()

def get_by_norm(conn: sqlite3.Connection, uname: str) -> Optional[sqlite3.Row]:
    norm = uname.strip().lower()
    return conn.execute("SELECT * FROM users WHERE username_norm=? LIMIT 1", (norm,)).fetchone()

def upsert_user(conn: sqlite3.Connection, uname: str, role_ui: str, active: bool) -> str:
    """
    Idempotent create/update:
      1) UPDATE by username_norm (revive/modify if present—deleted or not)
      2) If no row updated, INSERT new (populate optional pw columns if present)
    Returns: 'revived_or_updated' or 'inserted'
    """
    norm = uname.strip().lower()
    role_db = role_ui_to_db(role_ui)

    # UPDATE first
    cur = conn.execute(
        """UPDATE users
           SET username=?, role=?, is_active=?, is_deleted=0, deleted_at=NULL
           WHERE username_norm=?""",
        (uname, role_db, 1 if active else 0, norm)
    )
    if cur.rowcount and cur.rowcount > 0:
        conn.commit()
        return "revived_or_updated"

    # INSERT new — include password columns if they exist
    has_hash, is_notnull, has_salt = _has_pw_cols(conn)
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    if has_hash or has_salt:
        cols = ["username", "username_norm", "role", "is_active", "is_deleted", "created_at", "deleted_at"]
        vals = [uname, norm, role_db, 1 if active else 0, 0, now, None]
        placeholders = ["?"] * len(cols)

        if has_hash:
            cols.append("password_hash")
            placeholders.append("?")
            vals.append("" if is_notnull else None)  # satisfy NOT NULL if required
        if has_salt:
            cols.append("password_salt")
            placeholders.append("?")
            vals.append(None)

        sql = f"INSERT INTO users ({', '.join(cols)}) VALUES ({', '.join(placeholders)})"
        conn.execute(sql, tuple(vals))
    else:
        conn.execute(
            "INSERT INTO users (username, username_norm, role, is_active, is_deleted, created_at, deleted_at) VALUES (?,?,?,?,?,?,?)",
            (uname, norm, role_db, 1 if active else 0, 0, now, None)
        )
    conn.commit()
    return "inserted"

def soft_delete(conn: sqlite3.Connection, uname: str):
    norm = uname.strip().lower()
    conn.execute("UPDATE users SET is_active=0, is_deleted=1, deleted_at=datetime('now') WHERE username_norm=?", (norm,))
    conn.commit()

def rebuild_users_table(conn: sqlite3.Connection):
    """
    Recreate 'users' WITHOUT UNIQUE(username); keep unique(username_norm).
    Preserve optional password columns, and KEEP the CHECK(role IN ('admin','trader')).
    """
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("BEGIN IMMEDIATE")

    info = _col_info(conn, "users")
    has_hash = "password_hash" in info
    has_salt = "password_salt" in info

    # Create clean table (NO unique on username; CHECK on role; default 'trader')
    conn.execute("""
    CREATE TABLE IF NOT EXISTS users_new (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'trader' CHECK(role IN ('admin','trader')),
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        is_deleted INTEGER NOT NULL DEFAULT 0,
        deleted_at TEXT,
        username_norm TEXT NOT NULL
    )""")
    if has_hash:
        conn.execute("ALTER TABLE users_new ADD COLUMN password_hash TEXT")
    if has_salt:
        conn.execute("ALTER TABLE users_new ADD COLUMN password_salt TEXT")

    # Insert deduped by username_norm, keep latest id
    base_cols = "id, username, role, is_active, created_at, is_deleted, deleted_at, username_norm"
    extra_src = ""
    if has_hash:
        base_cols += ", password_hash"
        extra_src += ", u.password_hash"
    if has_salt:
        base_cols += ", password_salt"
        extra_src += ", u.password_salt"

    conn.execute(f"""
    INSERT INTO users_new ({base_cols})
    SELECT u.id, u.username, 
           CASE lower(u.role) WHEN 'user' THEN 'trader' WHEN 'trader' THEN 'trader' WHEN 'admin' THEN 'admin' ELSE 'trader' END as role,
           u.is_active, u.created_at, u.is_deleted, u.deleted_at, lower(trim(u.username)){extra_src}
    FROM users u
    JOIN (
        SELECT MAX(id) AS id
        FROM users
        GROUP BY lower(trim(username))
    ) last USING(id)
    """)

    conn.execute("DROP TABLE users")
    conn.execute("ALTER TABLE users_new RENAME TO users")

    # Only the normalized unique index
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS users_unq_username_norm ON users(username_norm)")

    # Ensure norms are set
    conn.execute("UPDATE users SET username_norm = lower(trim(username)) WHERE username_norm IS NULL")

    conn.execute("COMMIT")
    conn.execute("PRAGMA foreign_keys=ON")

# --------------------------------------------------------------------
# UI — header + table (unchanged look)
# --------------------------------------------------------------------
st.title("User Accounts")

with get_conn() as conn:
    migrate_users_table(conn)
    rows = fetch_users(conn)

# Map DB roles to UI roles for display
df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame(
    columns=["id","username","role","is_active","is_deleted","created_at","deleted_at"]
)
if not df.empty and "role" in df.columns:
    df["role"] = df["role"].map(role_db_to_ui)

df = df[["id","username","role","is_active","is_deleted","created_at","deleted_at"]]

st.dataframe(df, use_container_width=True, height=320)

st.markdown("---")

# --------------------------------------------------------------------
# Add User
# --------------------------------------------------------------------
with st.expander("➕ Add User", expanded=False):
    c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1])
    with c1:
        new_username = st.text_input("Username (unique)", key="au_username")
    with c2:
        # UI options stay "user"/"admin"
        new_role_ui = st.selectbox("Role", options=["user", "admin"], index=0, key="au_role")
    with c3:
        new_active = st.checkbox("Active", value=True, key="au_active")
    with c4:
        new_password = st.text_input("Password", type="password", key="au_password")

    if st.button("Create User", type="primary", key="au_create_btn"):
        uname = (new_username or "").strip()
        if not uname:
            st.error("Username is required.")
            st.stop()
        if uname.lower() == "admin" and new_role_ui != "admin":
            st.error("The 'admin' account must keep role = admin.")
            st.stop()
        try:
            with get_conn() as conn:
                migrate_users_table(conn)
                result = upsert_user(conn, uname, new_role_ui, new_active)
            upsert_user_json(user_id=uname, display=uname, active=new_active, password_plain=(new_password or ""))
            st.success(f"User '{uname}' {result}.")
            st.rerun()
        except sqlite3.IntegrityError as e:
            st.error(f"Create failed due to legacy constraint: {e}. Use 'Diagnostics & Repair' → Rebuild Users Table, then retry.")
        except Exception as e:
            st.error(f"Create failed: {e}")

# --------------------------------------------------------------------
# Modify User
# --------------------------------------------------------------------
with st.expander("✏️ Modify User (Role / Active)", expanded=False):
    all_usernames = df["username"].tolist()
    if not all_usernames:
        st.info("No users to modify.")
    else:
        sel_user = st.selectbox("Select user", options=all_usernames, key="mu_select")
        cur_row = df[df["username"] == sel_user].iloc[0] if not df.empty else None
        cur_role_ui = (cur_row["role"] if cur_row is not None else "user")
        cur_active = bool(int(cur_row["is_active"])) if cur_row is not None else True
        is_deleted = bool(int(cur_row["is_deleted"])) if cur_row is not None else False

        c1, c2, c3 = st.columns([1, 1, 1])
        with c1:
            new_role_ui2 = st.selectbox("Role", options=["user", "admin"], index=0 if cur_role_ui!="admin" else 1, key="mu_role")
        with c2:
            new_active2 = st.checkbox("Active", value=cur_active, key="mu_active")
        with c3:
            st.write(f"Deleted: **{is_deleted}**")

        if st.button("Save Changes", type="primary", key="mu_save_btn"):
            if sel_user.lower() == "admin" and new_role_ui2 != "admin":
                st.error("Cannot change 'admin' role away from admin.")
                st.stop()
            try:
                with get_conn() as conn:
                    _ = upsert_user(conn, sel_user, new_role_ui2, new_active2)
                upsert_user_json(user_id=sel_user, display=sel_user, active=new_active2, password_plain=None)
                st.success(f"User '{sel_user}' updated.")
                st.rerun()
            except Exception as e:
                st.error(f"Update failed: {e}")

# --------------------------------------------------------------------
# Reset Password
# --------------------------------------------------------------------
with st.expander("🔑 Reset Password", expanded=False):
    all_usernames = df["username"].tolist()
    if not all_usernames:
        st.info("No users available.")
    else:
        sel_user_pw = st.selectbox("Select user", options=all_usernames, key="rp_select")
        new_pw = st.text_input("New Password", type="password", key="rp_pw")
        if st.button("Reset Password", type="primary", key="rp_btn"):
            if not new_pw:
                st.error("Please enter a new password.")
                st.stop()
            try:
                upsert_user_json(user_id=sel_user_pw, display=sel_user_pw, active=True, password_plain=new_pw)
                st.success(f"Password reset for '{sel_user_pw}'.")
            except Exception as e:
                st.error(f"Password reset failed: {e}")

# --------------------------------------------------------------------
# Delete User (Soft)
# --------------------------------------------------------------------
with st.expander("🗑️ Delete User (Soft)", expanded=False):
    all_usernames = df["username"].tolist()
    if not all_usernames:
        st.info("No users available.")
    else:
        del_user = st.selectbox("Select user to delete", options=all_usernames, key="du_select")
        confirm = st.text_input("Type the username to confirm soft deletion", key="du_confirm")

        if st.button("Soft Delete User", type="secondary", key="du_btn"):
            if del_user.lower() == "admin":
                st.error("Deletion of 'admin' is not allowed.")
                st.stop()
            if confirm.strip().lower() != del_user.strip().lower():
                st.error("Confirmation text does not match the selected username.")
                st.stop()
            try:
                with get_conn() as conn:
                    soft_delete(conn, del_user)
                deactivate_user_json(del_user)
                st.success(f"User '{del_user}' soft-deleted (kept in table with deleted flags).")
                st.rerun()
            except Exception as e:
                st.error(f"Delete failed: {e}")

# --------------------------------------------------------------------
# Diagnostics & Repair
# --------------------------------------------------------------------
with st.expander("🧪 Diagnostics & Repair"):
    q = st.text_input("Check username", value="neel", key="diag_q")
    if st.button("Run Diagnostics", key="diag_btn"):
        try:
            with get_conn() as conn:
                rows = conn.execute(
                    """SELECT id, username, username_norm, role, is_active, is_deleted, created_at, deleted_at
                       FROM users WHERE username_norm=lower(trim(?))""", (q,)
                ).fetchall()
                st.write("Rows with same username_norm:", [dict(r) for r in rows])
                all_rows = conn.execute(
                    "SELECT id, username, username_norm, role, is_active, is_deleted, created_at, deleted_at FROM users ORDER BY id"
                ).fetchall()
                st.write("All rows:", [dict(r) for r in all_rows])
                indices = conn.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='users'").fetchall()
                st.write("Indices:", [dict(r) for r in indices])
                integrity = conn.execute("PRAGMA integrity_check").fetchall()
                st.write("PRAGMA integrity_check:", integrity)
                st.write("Columns:", _col_info(conn, "users"))
        except Exception as e:
            st.error(f"Diagnostics failed: {e}")

    st.markdown("---")
    if st.button("🛠 Rebuild Users Table (normalize + keep CHECK/admin,trader)", type="secondary", key="rebuild_btn"):
        try:
            with get_conn() as conn:
                rebuild_users_table(conn)
            st.success("Users table rebuilt successfully. Try creating the user again.")
            st.rerun()
        except Exception as e:
            st.error(f"Rebuild failed: {e}")
