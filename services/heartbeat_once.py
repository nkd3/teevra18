import sqlite3, os
DB_PATH = r"C:\teevra18\data\teevra18.db"
con = sqlite3.connect(DB_PATH)
cur = con.cursor()
cur.execute("INSERT INTO health(service, ok, detail) VALUES ('manual_heartbeat',1,'operator check');")
cur.execute("UPDATE breaker_state SET state='RUNNING', reason='operator check', updated_at=datetime('now') WHERE id=1;")
con.commit()
con.close()
print("[OK] Heartbeat recorded.")
