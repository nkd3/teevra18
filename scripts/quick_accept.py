import sqlite3

DB=r"C:\teevra18\data\teevra18.db"
with sqlite3.connect(DB) as conn:
    c=conn.cursor()
    c.execute("PRAGMA table_info(signals);")
    cols = [r[1] for r in c.fetchall()]
    print("[signals schema]", cols)

    # Pick columns if present
    sel_cols = [x for x in ["ts","symbol","driver","action","rr","sl","tp","reason","state"] if x in cols]
    if not sel_cols:
        print("[WARN] No expected columns found in signals table.")
    else:
        q = f"SELECT {','.join(sel_cols)} FROM signals ORDER BY ROWID DESC LIMIT 10;"
        c.execute(q)
        rows=c.fetchall()
        print("[signals latest 10]")
        for r in rows:
            print(r)

    if "ts" in cols:
        try:
            c.execute("SELECT COUNT(*) FROM signals WHERE date(ts)=date('now');")
            print("[today signals]", c.fetchone()[0])
        except Exception as e:
            print("[today signals] check skipped (ts not ISO).", e)
