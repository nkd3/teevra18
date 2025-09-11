import sqlite3
DB = r"C:\teevra18\data\ops\teevra18.db"
con = sqlite3.connect(DB)
cur = con.cursor()
cur.execute("SELECT COUNT(*) FROM signals")
print("signals count:", cur.fetchone()[0])
for row in cur.execute("SELECT id, ts, symbol, driver, action, rr, sl, tp FROM signals ORDER BY id DESC LIMIT 5"):
    print(row)
con.close()
