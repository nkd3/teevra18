from pathlib import Path
import sqlite3
from typing import Optional, Iterable, Union
import pandas as pd

DB_PATH = r"C:\teevra18\data\teevra18.db"

def get_conn(db_path: str = DB_PATH, timeout: float = 30.0) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, timeout=timeout)
    con.row_factory = sqlite3.Row
    return con

def table_exists(con: sqlite3.Connection, table: str) -> bool:
    cur = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,))
    return cur.fetchone() is not None

def columns(con: sqlite3.Connection, table: str) -> list[str]:
    cur = con.execute(f"PRAGMA table_info({table})")
    rows = cur.fetchall()
    out = []
    for r in rows:
        out.append(r["name"] if isinstance(r, sqlite3.Row) else r[1])
    return out

def read_df(con: sqlite3.Connection, sql: str, params: Optional[Union[tuple, dict]] = None) -> pd.DataFrame:
    return pd.read_sql_query(sql, con, params=params)

def first_existing(con: sqlite3.Connection, *tables: Iterable[str]) -> Optional[str]:
    for t in tables:
        if table_exists(con, t):
            return t
    return None
