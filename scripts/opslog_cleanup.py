import os, sqlite3
db = os.getenv('DB_PATH', r'C:\teevra18\data\teevra18.db')
con = sqlite3.connect(db)
cur = con.cursor()
cur.execute("DELETE FROM ops_log WHERE component IS NULL AND status IS NULL")
con.commit()
print("Deleted", cur.rowcount, "blank ops_log rows")
con.close()
