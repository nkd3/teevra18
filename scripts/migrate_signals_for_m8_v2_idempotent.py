import sqlite3, sys

DB = r"C:\teevra18\data\teevra18.db"
TABLE = "signals"

# Columns M8 V2 expects/uses. We'll add any that are missing.
NEEDED = [
    ("signal_id", "TEXT"),
    ("option_symbol", "TEXT"),
    ("underlying_root", "TEXT"),
    ("side", "TEXT"),
    ("entry_price", "REAL"),
    ("sl_points", "REAL"),
    ("tp_points", "REAL"),
    ("lots", "INTEGER DEFAULT 1"),
    ("ts_utc", "TEXT"),
    ("state", "TEXT DEFAULT 'PENDING'"),
    ("rr_validated", "INTEGER"),           # 1/0
    ("rr_reject_reason", "TEXT"),
    ("rr_metrics_json", "TEXT")
]

def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cols = {row["name"] for row in cur.execute(f"PRAGMA table_info({TABLE});").fetchall()}

    alters = []
    for col, decl in NEEDED:
        if col not in cols:
            alters.append(f"ALTER TABLE {TABLE} ADD COLUMN {col} {decl};")

    if alters:
        cur.executescript("\n".join(alters))
        conn.commit()
        print("[M8] signals migration applied:\n" + "\n".join(alters))
    else:
        print("[M8] signals: Nothing to do. All columns present.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[M8] signals migration failed:", e)
        sys.exit(1)
