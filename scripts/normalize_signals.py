import sqlite3

DB = r"C:\teevra18\data\teevra18.db"

CANONICAL_COLS = [
    "id INTEGER PRIMARY KEY AUTOINCREMENT",
    "ts TEXT",
    "symbol TEXT",
    "driver TEXT",
    "action TEXT",
    "rr REAL",
    "sl REAL",
    "tp REAL",
    "reason TEXT",
    "state TEXT DEFAULT 'NEW'"
]

def recreate_with_copy(conn, old_cols):
    cur = conn.cursor()
    cur.execute("ALTER TABLE signals RENAME TO signals_old;")
    conn.commit()

    # Create canonical signals
    ddl = "CREATE TABLE signals(" + ",".join(CANONICAL_COLS) + ");"
    cur.execute(ddl)

    # Build insert from what exists
    common = [c for c in ["symbol","driver","action","rr","sl","tp","reason","state","ts"] if c in old_cols]
    placeholders = ",".join(["?" for _ in common])
    select_expr = ",".join(common)

    if common:
        cur.execute(f"INSERT INTO signals({','.join(common)}) SELECT {select_expr} FROM signals_old;")
    conn.commit()

    cur.execute("DROP TABLE signals_old;")
    conn.commit()

    print(f"[OK] signals table recreated. Preserved columns: {common}")

with sqlite3.connect(DB) as conn:
    c = conn.cursor()
    c.execute("PRAGMA table_info(signals);")
    cols = [r[1] for r in c.fetchall()]

    if not cols:
        ddl = "CREATE TABLE signals(" + ",".join(CANONICAL_COLS) + ");"
        c.execute(ddl)
        conn.commit()
        print("[OK] signals table created fresh (no previous table).")
    else:
        missing = [col.split()[0] for col in CANONICAL_COLS if col.split()[0] not in cols]
        if missing:
            print(f"[INFO] Existing signals missing cols: {missing}. Rebuilding …")
            recreate_with_copy(conn, cols)
        else:
            print("[OK] signals already canonical, no action.")
