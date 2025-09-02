import sqlite3

DB = r"C:\teevra18\data\teevra18.db"

with sqlite3.connect(DB) as conn:
    c = conn.cursor()

    # --- 1) Dedupe among NON-DUPED rows (keep earliest id per day/driver/reason) ---
    # Find groups with duplicates, ignoring already DUPED rows
    c.execute("""
        WITH g AS (
          SELECT date(ts) AS d, driver, reason, COUNT(*) AS cnt
          FROM signals
          WHERE COALESCE(state,'') <> 'DUPED'
          GROUP BY date(ts), driver, reason
          HAVING COUNT(*) > 1
        )
        SELECT s.id, s.ts, s.driver, s.reason
        FROM signals s
        JOIN g
          ON date(s.ts)=g.d
         AND s.driver=g.driver
         AND s.reason=g.reason
        WHERE COALESCE(s.state,'') <> 'DUPED'
        ORDER BY date(s.ts), s.driver, s.reason, s.id ASC;
    """)
    dup_rows = c.fetchall()

    if dup_rows:
        # Pick earliest id to keep per group
        c.execute("""
            SELECT MIN(id) AS keep_id, date(ts) AS d, driver, reason
            FROM signals
            WHERE COALESCE(state,'') <> 'DUPED'
            GROUP BY date(ts), driver, reason
            HAVING COUNT(*) > 1
        """)
        keep_map = {(d, driver, reason): keep_id
                    for keep_id, d, driver, reason in c.fetchall()}

        ids_to_dupe = []
        for sid, ts, driver, reason in dup_rows:
            key = (ts.split(" ")[0], driver, reason)
            keep_id = keep_map.get(key)
            if keep_id is not None and sid != keep_id:
                ids_to_dupe.append(sid)

        if ids_to_dupe:
            # Mark as DUPED instead of deleting (keeps audit trail)
            c.executemany("UPDATE signals SET state='DUPED' WHERE id=?", [(i,) for i in ids_to_dupe])
            conn.commit()
            print(f"[OK] Marked {len(ids_to_dupe)} duplicates as DUPED among non-DUPED rows.")
    else:
        print("[OK] No non-DUPED duplicates found.")

    # --- 2) Drop any existing index named ux_signal_per_day (partial or not) ---
    c.execute("PRAGMA index_list(signals);")
    idx_names = [row[1] for row in c.fetchall()]
    if "ux_signal_per_day" in idx_names:
        print("[INFO] Dropping existing ux_signal_per_day index...")
        c.execute("DROP INDEX IF EXISTS ux_signal_per_day;")
        conn.commit()

    # --- 3) Create a PARTIAL UNIQUE index only for rows where state<>'DUPED' ---
    # This ensures one (date, driver, reason) per day among active rows.
    try:
        c.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_signal_per_day
            ON signals(date(ts), driver, reason)
            WHERE COALESCE(state,'') <> 'DUPED';
        """)
        conn.commit()
        print("[OK] Partial UNIQUE index ux_signal_per_day created (excludes DUPED rows).")
    except sqlite3.IntegrityError:
        # If it still fails, show remaining offending groups
        print("[ERR] Still have duplicates among non-DUPED rows.")
        c.execute("""
            SELECT date(ts), driver, reason, COUNT(*)
            FROM signals
            WHERE COALESCE(state,'') <> 'DUPED'
            GROUP BY date(ts), driver, reason
            HAVING COUNT(*) > 1
        """)
        print(c.fetchall())
        raise

    # --- 4) Optional: show summary after fix ---
    c.execute("""
        SELECT date(ts), driver, reason, COUNT(*)
        FROM signals
        WHERE COALESCE(state,'') <> 'DUPED'
        GROUP BY date(ts), driver, reason
        ORDER BY date(ts) DESC, driver;
    """)
    summary = c.fetchall()
    print("[SUMMARY non-DUPED counts per (day, driver, reason)]:")
    for row in summary:
        print(row)
