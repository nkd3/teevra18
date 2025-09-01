import sqlite3

con = sqlite3.connect(r"C:\teevra18\data\teevra18.db")
rows = list(con.execute("SELECT security_id,exchange_seg FROM universe_depth20"))
print("universe_depth20 entries:", rows)
con.close()