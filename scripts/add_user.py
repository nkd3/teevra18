import sqlite3
import getpass
from pathlib import Path
import bcrypt

DB_PATH = Path(r"C:\teevra18\data\teevra18.db")

def add_user(username: str, role: str, password: str):
    if role not in ("admin","trader"):
        raise ValueError("role must be 'admin' or 'trader'")
    pw_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12))
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO users (username, role, password_hash, is_active) VALUES (?,?,?,1)",
                    (username, role, pw_hash))
        conn.commit()
    print(f"OK: created {role} user '{username}'")

if __name__ == "__main__":
    print("Create a new user")
    username = input("Username: ").strip()
    role = input("Role (admin/trader): ").strip().lower()
    password = getpass.getpass("Password: ")
    add_user(username, role, password)
