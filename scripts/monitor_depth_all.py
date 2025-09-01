import sqlite3, pandas as pd, os

DB = r"C:\teevra18\data\teevra18.db"

def sanity_check(con):
    print("== Sanity Check ==")
    print("DB exists:", os.path.exists(DB))
    cur = con.cursor()
    tables = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%depth%';"
    )]
    print("Tables:", tables)
    try:
        cur.execute("SELECT COUNT(*), MAX(ts_recv_utc) FROM depth20_levels")
        cnt, last_ts = cur.fetchone()
        print("depth20_levels rows:", cnt, "latest ts:", last_ts)
    except sqlite3.OperationalError as e:
        print("Table check error:", e)
    print()

def quick_view(con):
    print("== Quick View (last 40 rows) ==")
    sql = """
    SELECT substr(ts_recv_utc,12,12) as t, security_id, side, level, price, qty, orders, latency_ms
    FROM depth20_levels
    ORDER BY ts_recv_utc DESC, security_id, side, level
    LIMIT 40
    """
    df = pd.read_sql_query(sql, con)
    print(df)
    print()

def health_view(con):
    print("== Health View ==")
    summary = pd.read_sql_query("""
    SELECT COUNT(*) AS rows_total,
           MIN(ts_recv_utc) AS first_ts,
           MAX(ts_recv_utc) AS last_ts
    FROM depth20_levels
    """, con)
    print("Summary:")
    print(summary)

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

    print("\nLatest per security:")
    print(latest)
    print()

if __name__ == "__main__":
    con = sqlite3.connect(DB)
    sanity_check(con)
    quick_view(con)
    health_view(con)
    con.close()
