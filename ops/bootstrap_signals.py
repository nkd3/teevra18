import sqlite3, os
DB = r"C:\teevra18\data\ops\teevra18.db"
os.makedirs(os.path.dirname(DB), exist_ok=True)
con=sqlite3.connect(DB);cur=con.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS signals(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  symbol TEXT NOT NULL,
  driver TEXT NOT NULL,
  action TEXT NOT NULL,
  rr REAL, sl REAL, tp REAL,
  state TEXT NOT NULL DEFAULT 'NEW'
)""")
# add state if missing
cur.execute("PRAGMA table_info(signals)")
cols={r[1] for r in cur.fetchall()}
if "state" not in cols:
    cur.execute("ALTER TABLE signals ADD COLUMN state TEXT NOT NULL DEFAULT 'NEW'")
# ensure one row exists
cur.execute("SELECT COUNT(*) FROM signals")
if cur.fetchone()[0]==0:
    cur.execute("""INSERT INTO signals(ts,symbol,driver,action,rr,sl,tp,state)
                   VALUES(datetime('now'),'NIFTY','smoke','BUY',1.5,99.0,103.0,'NEW')""")
con.commit(); con.close()
print("[OK] signals schema/data ensured at", DB)
