import sqlite3, time, os
db = os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db")
c = sqlite3.connect(db)
for i in range(12):
    rows = c.execute("SELECT COUNT(1) FROM ticks_raw").fetchone()[0]
    st   = c.execute("SELECT value FROM health WHERE key='m1_status'").fetchone()
    cpu  = c.execute("SELECT value FROM health WHERE key='m1_cpu'").fetchone()
    print(f"[{i*5:02d}s] rows={rows} status={st} cpu%={cpu}")
    time.sleep(5)
