import sqlite3, pandas as pd

DB = r"C:\teevra18\data\teevra18.db"
con = sqlite3.connect(DB)

summary = pd.read_sql_query("""
SELECT COUNT(*) AS rows_total,
       MIN(ts_recv_utc) AS first_ts,
       MAX(ts_recv_utc) AS last_ts
FROM depth20_levels
""", con)

latest = pd.read_sql_query("""
WITH latest AS (
  SELECT security_id, MAX(ts_recv_utc) AS ts
  FROM depth20_levels
  GROUP BY security_id
)
SELECT d.security_id, d.ts_recv_utc,
       MAX(d.top5_bid_qty)  AS bid5,
       MAX(d.top5_ask_qty)  AS ask5,
       ROUND(MAX(d.pressure_1_5),4) AS pressure_1_5,
       ROUND(AVG(d.latency_ms),1)   AS avg_latency_ms
FROM depth20_levels d
JOIN latest l ON l.security_id=d.security_id AND l.ts=d.ts_recv_utc
GROUP BY d.security_id, d.ts_recv_utc
ORDER BY d.ts_recv_utc DESC
LIMIT 10
""", con)

print("== Summary ==")
print(summary)
print("\n== Latest per security ==")
print(latest)

con.close()
