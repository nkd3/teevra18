# -*- coding: utf-8 -*-
import json
from .db import read_df

def get_active_policy_row(conn):
    df = read_df(conn, "SELECT id,name,active,policy_json,updated_at FROM policy_configs ORDER BY active DESC, updated_at DESC")
    if df.empty:
        return None, {}
    row = df.iloc[0]
    try:
        policy = json.loads(row["policy_json"]) if row.get("policy_json") else {}
    except Exception:
        policy = {}
    return row, policy
