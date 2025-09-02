import sqlite3

DB = r"C:\teevra18\data\teevra18.db"

NEEDED = [
    ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("ts", "TEXT"),
    ("symbol", "TEXT"),
    ("side", "TEXT"),
    ("qty", "INTEGER"),
    ("entry", "REAL"),
    ("sl", "REAL"),
    ("tp", "REAL"),
    ("status", "TEXT"),
    ("ref_signal_id", "INTEGER")
]

def table_columns(conn, table):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    return [r[1] for r in cur.fetchall()]

with sqlite3.connect(DB) as conn:
    cur = conn.cursor()
    # Does table exist?
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='paper_orders';")
    exists = cur.fetchone() is not None

    if not exists:
        # Fresh create
        cols_sql = ", ".join([f"{n} {t}" for n,t in NEEDED])
        cur.execute(f"CREATE TABLE paper_orders ({cols_sql});")
        conn.commit()
        print("[OK] Created paper_orders fresh.")
    else:
        cols = table_columns(conn, "paper_orders")
        missing = [n for n,_ in NEEDED if n not in cols]
        if not missing:
            print("[OK] paper_orders already matches expected schema.")
        else:
            print("[INFO] Migrating paper_orders; missing:", missing)
            # 1) Rename old
            cur.execute("ALTER TABLE paper_orders RENAME TO paper_orders_old;")
            # 2) Create new
            cols_sql = ", ".join([f"{n} {t}" for n,t in NEEDED])
            cur.execute(f"CREATE TABLE paper_orders ({cols_sql});")
            # 3) Copy overlap
            new_cols = [n for n,_ in NEEDED]
            old_cols = table_columns(conn, "paper_orders_old")
            overlap = [c for c in new_cols if c in old_cols]
            if overlap:
                cur.execute(
                    f"INSERT INTO paper_orders ({', '.join(overlap)}) "
                    f"SELECT {', '.join(overlap)} FROM paper_orders_old;"
                )
            # 4) Drop old
            cur.execute("DROP TABLE paper_orders_old;")
            conn.commit()
            print("[OK] paper_orders migrated and data copied (overlap columns).")
