# -*- coding: utf-8 -*-
from pathlib import Path
import sqlite3
import pandas as pd

DB_PATH = Path(r"C:\teevra18\data\teevra18.db")

def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def table_exists(conn, name: str) -> bool:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return cur.fetchone() is not None

def columns(conn, table: str):
    try:
        cur = conn.execute(f"PRAGMA table_info({table});")
        return [r["name"] for r in cur.fetchall()]
    except Exception:
        return []

def read_df(conn, sql: str, params: tuple = ()):
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except Exception:
        return pd.DataFrame()

def first_existing(colnames, candidates):
    for c in candidates:
        if c in colnames:
            return c
    return None
