import sqlite3, os

DB = r"C:\teevra18\data\ops\teevra18.db"
os.makedirs(os.path.dirname(DB), exist_ok=True)

REQUIRED_COLUMNS = [
    ("id",      "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("ts",      "TEXT NOT NULL"),
    ("symbol",  "TEXT NOT NULL"),
    ("driver",  "TEXT NOT NULL"),
    ("action",  "TEXT NOT NULL"),
    ("rr",      "REAL"),
    ("sl",      "REAL"),
    ("tp",      "REAL"),
    ("state",   "TEXT NOT NULL DEFAULT 'NEW'")  # <- missing in your logs
]

def table_exists(cur, name):
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def get_columns(cur, name):
    cur.execute(f"PRAGMA table_info({name})")
    return {row[1]: row[2] for row in cur.fetchall()}  # {col: type}

con = sqlite3.connect(DB)
cur = con.cursor()

# 1) Create table if missing
if not table_exists(cur, "signals"):
    cols = ", ".join([f"{c} {t}" for c,t in REQUIRED_COLUMNS])
    cur.execute(f"CREATE TABLE signals ({cols})")
    con.commit()
    print("[OK] Created table 'signals'")
else:
    print("[OK] Table 'signals' exists")

# 2) Add missing columns idempotently
existing = get_columns(cur, "signals")
added = []
for col, ctype in REQUIRED_COLUMNS:
    if col not in existing:
        cur.execute(f"ALTER TABLE signals ADD COLUMN {col} {ctype}")
        added.append(col)
if added:
    con.commit()
    print(f"[OK] Added columns: {', '.join(added)}")
else:
    print("[OK] No column adds required")

# 3) Ensure at least one row (for smoke test)
cur.execute("SELECT COUNT(*) FROM signals")
count = cur.fetchone()[0]
if count == 0:
    cur.execute("""INSERT INTO signals(ts, symbol, driver, action, rr, sl, tp, state)
                   VALUES (datetime('now'), 'NIFTY', 'smoke', 'BUY', 1.5, 99.0, 103.0, 'NEW')""")
    con.commit()
    print("[OK] Inserted 1 dummy row")
else:
    print(f"[OK] signals currently has {count} row(s)")

con.close()
print(f"[DONE] signals schema verified at {DB}")
