# C:\teevra18\services\ltp_feeder\db_writer.py
import sqlite3
from datetime import datetime

DB = r"C:\teevra18\data\teevra18.db"

def ensure_tables(conn):
    conn.execute("""
      CREATE TABLE IF NOT EXISTS ltp_cache(
        option_symbol TEXT,
        ts_utc TEXT,
        ltp REAL
      );
    """)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS ltp_subscriptions(
        option_symbol TEXT PRIMARY KEY,
        broker TEXT,
        token TEXT,
        exchange TEXT
      );
    """)

def get_subscriptions(conn, broker: str):
    return conn.execute("""
      SELECT option_symbol, token, exchange
      FROM ltp_subscriptions
      WHERE broker=?
    """, (broker,)).fetchall()

def insert_ltp(conn, option_symbol: str, ltp: float):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("INSERT INTO ltp_cache(option_symbol, ts_utc, ltp) VALUES(?,?,?)",
                 (option_symbol, ts, float(ltp)))
