import sqlite3, json
DB = r"C:\teevra18\data\teevra18.db"
con = sqlite3.connect(DB)
cols = con.execute("PRAGMA table_info(ticks_raw)").fetchall()
print("ticks_raw columns:")
for c in cols:
    print(f"- {c[1]} (type={c[2]})")
# Peek first 3 rows to see example data
try:
    rows = con.execute("SELECT * FROM ticks_raw LIMIT 3").fetchall()
    colnames = [r[1] for r in cols]
    print("\nSample rows:")
    for r in rows:
        print(dict(zip(colnames, r)))
except Exception as e:
    print("Peek error:", e)
con.close()
