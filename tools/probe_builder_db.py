import sqlite3, pandas as pd, os, hashlib
DB = r"C:\teevra18\data\ops.db"  # <-- must match the path you intend
conn = sqlite3.connect(DB)
print("[PROBE] PRAGMA database_list from builder path:")
print(pd.read_sql("PRAGMA database_list;", conn))
conn.close()

# Hash + size so we can compare with the query_db.py hash
h = hashlib.new("sha256")
with open(DB, "rb") as f:
    h.update(f.read())
print("[PROBE] SHA256:", h.hexdigest())
print("[PROBE] size:", os.path.getsize(DB))
