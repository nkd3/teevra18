import os
import sqlite3
import bcrypt
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(r"C:\teevra18\.env")

DB_PATH = Path(r"C:\teevra18\data\teevra18.db")
JWT_SECRET = os.getenv("JWT_SECRET","dev_secret_change_me")

def verify_credentials(username: str, password: str):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, username, role, password_hash, is_active FROM users WHERE username = ?", (username,))
        row = cur.fetchone()
    if not row:
        return None
    user_id, uname, role, pw_hash, is_active = row
    if not is_active:
        return None
    try:
        ok = bcrypt.checkpw(password.encode("utf-8"), pw_hash)
    except Exception:
        return None
    if not ok:
        return None
    return {"id": user_id, "username": uname, "role": role}

def require_role(session_state, role: str) -> bool:
    user = session_state.get("user")
    return bool(user and user.get("role") == role)
