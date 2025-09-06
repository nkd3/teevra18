# -*- coding: utf-8 -*-
import sqlite3, hashlib
from pathlib import Path

DB_PATH = Path(r"C:\teevra18\data\teevra18.db")

def _ensure_schema():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS app_users(
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL
        )
        """)
        con.commit()

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def verify_user(username: str, password: str) -> bool:
    _ensure_schema()
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute("SELECT password_hash FROM app_users WHERE username = ?", (username.strip().lower(),)).fetchone()
        if not row: return False
        return row[0] == sha256(password)

def upsert_user(username: str, password: str):
    _ensure_schema()
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
        INSERT INTO app_users(username, password_hash)
        VALUES(?, ?)
        ON CONFLICT(username) DO UPDATE SET password_hash=excluded.password_hash
        """, (username.strip().lower(), sha256(password)))
        con.commit()
