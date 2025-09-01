import os, sqlite3
from dotenv import load_dotenv

load_dotenv(r"C:\teevra18\.env")
db = os.getenv("DB_PATH")
print("DB_PATH =", db)

con = sqlite3.connect(db)
cur = con.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
print("Tables:", [r[0] for r in cur.fetchall()])
con.close()
