# C:\teevra18\scripts\migrate_m8_v2_idempotent.py
import sqlite3, sys

DB = r"C:\teevra18\data\teevra18.db"
TABLE = "rr_profiles"

NEEDED = [
    ("sl_cap_per_trade", "REAL DEFAULT 1500"),
    ("include_charges", "INTEGER DEFAULT 1"),
    ("charges_broker", "TEXT DEFAULT 'ZERODHA'"),
    ("charges_overrides_json", "TEXT DEFAULT NULL"),
]

def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # fetch existing columns
    cols = {row["name"] for row in cur.execute(f"PRAGMA table_info({TABLE});").fetchall()}

    # build ALTERs only for missing columns
    alters = []
    for col, decl in NEEDED:
        if col not in cols:
            alters.append(f"ALTER TABLE {TABLE} ADD COLUMN {col} {decl};")

    if not alters:
        print("[M8] Nothing to do. All columns already present.")
        return

    sql = "\n".join(alters)
    cur.executescript(sql)
    conn.commit()
    print("[M8] Migration applied:\n" + sql)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[M8] Migration failed:", e)
        sys.exit(1)
