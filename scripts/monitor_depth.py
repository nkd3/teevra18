# monitor_depth.py
import sqlite3, pandas as pd

con = sqlite3.connect(r"C:\teevra18\data\teevra18.db")
df = pd.read_sql_query("""
SELECT substr(ts_recv_utc,12,12) as t, security_id, side, level, price, qty, orders, latency_ms
FROM depth20_levels
ORDER BY ts_recv_utc DESC, security_id, side, level
LIMIT 40
""", con)
print(df)
con.close()
