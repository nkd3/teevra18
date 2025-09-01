import sqlite3, os

DB = r"C:\teevra18\data\teevra18.db"
print("DB exists:", os.path.exists(DB))

con = sqlite3.connect(DB)
cur = con.cursor()

# List tables mentioning "depth"
tables = [r[0] for r in cur.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%depth%';"
)]
print("Tables:", tables)

# Ensure table exists and show counts
try:
    cur.execute("SELECT COUNT(*), MAX(ts_recv_utc) FROM depth20_levels")
    cnt, last_ts = cur.fetchone()
    print("depth20_levels rows:", cnt, "latest ts:", last_ts)
except sqlite3.OperationalError as e:
    print("Table check error:", e)

con.close()
