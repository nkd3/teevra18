import sqlite3, os

db = r"C:\teevra18\data\teevra18.db"
os.makedirs(os.path.dirname(db), exist_ok=True)
con = sqlite3.connect(db)
cur = con.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS universe_depth20 (
  security_id INTEGER PRIMARY KEY,
  exchange_seg TEXT NOT NULL
)
""")

# Replace with real SecurityIds from Dhan later
rows = [
    (1333, "NSE_EQ"),    # example only: RELIANCE EQ
    (532540, "NSE_FNO")  # example only: NIFTY FUT
]
cur.executemany("INSERT OR IGNORE INTO universe_depth20(security_id, exchange_seg) VALUES (?,?)", rows)

con.commit()
con.close()
print("Universe seeded OK.")
