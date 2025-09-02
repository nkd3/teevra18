import sqlite3

DB = r"C:\teevra18\data\teevra18.db"

with sqlite3.connect(DB) as conn:
    c = conn.cursor()

    # 1) Find duplicates on (date(ts), driver, reason)
    c.execute("""
        WITH g AS (
          SELECT
            date(ts) AS d,
            driver,
            reason,
            COUNT(*) AS cnt
          FROM signals
          GROUP BY date(ts), driver, reason
          HAVING COUNT(*) > 1
        )
        SELECT s.id, s.ts, s.driver, s.reason
        FROM signals s
        JOIN g
          ON date(s.ts)=g.d
         AND s.driver=g.driver
         AND s.reason=g.reason
        ORDER BY date(s.ts), s.driver, s.reason, s.id ASC;
    """)
    dup_rows = c.fetchall()

    if not dup_rows:
        print("[OK] No duplicates found.")
    else:
        print(f"[INFO] Found {len(dup_rows)} duplicate rows (will keep the earliest per group).")

        # Build map to keep the earliest id per (date, driver, reason)
        c.execute("""
            SELECT
              MIN(id) AS keep_id,
              date(ts) AS d,
              driver,
              reason
            FROM signals
            GROUP BY date(ts), driver, reason
            HAVING COUNT(*) > 1
        """)
        keepers = {(d,driver,reason): keep_id for keep_id,d,driver,reason in c.fetchall()}

        # Gather ids to delete
        ids_to_delete = []
        for sid, ts, driver, reason in dup_rows:
            key = (ts.split(" ")[0], driver, reason)  # date(ts), driver, reason
            keep_id = keepers.get(key)
            if keep_id is not None and sid != keep_id:
                ids_to_delete.append(sid)

        if ids_to_delete:
            # Prefer marking as DUPED instead of hard delete; toggle as you like.
            # Hard delete:
            # c.executemany("DELETE FROM signals WHERE id=?", [(i,) for i in ids_to_delete])
            # Mark as DUPED (safer):
            c.executemany("UPDATE signals SET state='DUPED' WHERE id=?", [(i,) for i in ids_to_delete])
            conn.commit()
            print(f"[OK] Marked {len(ids_to_delete)} duplicates as DUPED.")

    # 2) Make sure no conflicting index remains
    c.execute("PRAGMA index_list(signals);")
    idx_list = [row[1] for row in c.fetchall()]
    if "ux_signal_per_day" in idx_list:
        print("[INFO] Dropping existing ux_signal_per_day index...")
        c.execute("DROP INDEX IF EXISTS ux_signal_per_day;")
        conn.commit()

    # 3) Create the unique index (one per day/driver/reason)
    try:
        c.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_signal_per_day
            ON signals(date(ts), driver, reason);
        """)
        conn.commit()
        print("[OK] Unique index ux_signal_per_day created.")
    except sqlite3.IntegrityError as e:
        # If something still violates, show a small diagnostic
        print("[ERR] Could not create unique index due to remaining duplicates.")
        c.execute("""
            SELECT date(ts), driver, reason, COUNT(*)
            FROM signals
            GROUP BY date(ts), driver, reason
            HAVING COUNT(*) > 1
        """)
        print(c.fetchall())
        raise
