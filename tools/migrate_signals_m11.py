import sqlite3, os
DB = os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db")

def col_exists(conn, table, col):
    cur = conn.execute(f"PRAGMA table_info({table});")
    return any(r[1] == col for r in cur.fetchall())

with sqlite3.connect(DB) as conn:
    # Add rr_est if missing
    if not col_exists(conn, "signals_m11", "rr_est"):
        conn.execute("ALTER TABLE signals_m11 ADD COLUMN rr_est REAL;")
        print("[MIGRATE] Added column rr_est REAL")

    # Add sl_per_lot if missing
    if not col_exists(conn, "signals_m11", "sl_per_lot"):
        conn.execute("ALTER TABLE signals_m11 ADD COLUMN sl_per_lot REAL;")
        print("[MIGRATE] Added column sl_per_lot REAL")

    conn.commit()
    cur = conn.execute("PRAGMA table_info(signals_m11);")
    print("[INFO] signals_m11 columns now:")
    for r in cur.fetchall():
        print(" -", r[1], r[2])
