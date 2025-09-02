import sqlite3

DB = r"C:\teevra18\data\teevra18.db"

with sqlite3.connect(DB) as conn:
    c = conn.cursor()

    # Drop old index if it exists (to avoid duplicate constraint errors)
    c.execute("PRAGMA index_list(signals);")
    idx_list = [row[1] for row in c.fetchall()]
    if "ux_signal_per_day" in idx_list:
        print("[INFO] Dropping existing ux_signal_per_day index...")
        c.execute("DROP INDEX IF EXISTS ux_signal_per_day;")
        conn.commit()

    # Create a fresh UNIQUE index on (date(ts), driver, reason)
    c.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_signal_per_day
        ON signals(date(ts), driver, reason);
    """)
    conn.commit()

print("[OK] Unique index ux_signal_per_day ensured.")
