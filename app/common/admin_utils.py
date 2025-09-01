# -*- coding: utf-8 -*-
import json
from pathlib import Path
from .db import get_conn, read_df

def upsert_alert_setting(conn, channel: str, key: str, value: str):
    conn.execute("""
      INSERT INTO alerts_config (channel, key, value, updated_at)
      VALUES (?, ?, ?, datetime('now'))
    """, (channel, key, value))
    conn.commit()

def get_alerts_map(conn, channel: str) -> dict:
    df = read_df(conn, "SELECT key, value FROM alerts_config WHERE channel=?", (channel,))
    return {r["key"]: r["value"] for _, r in df.iterrows()} if not df.empty else {}
