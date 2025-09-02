import sqlite3, sys, datetime

DB = r"C:\teevra18\data\teevra18.db"

with sqlite3.connect(DB) as conn:
    c = conn.cursor()

    # Show a summary before we touch anything
    c.execute("""
        SELECT date(ts), driver, reason, COUNT(*)
        FROM signals
        WHERE date(ts)=date('now')
        GROUP BY 1,2,3
        ORDER BY 2,3
    """)
    rows = c.fetchall()
    print("[SUMMARY today BEFORE]:")
    for r in rows: print(r)

    # 1) Move today's non-DUPED to DUPED to free the UNIQUE slot
    c.execute("""
        UPDATE signals
        SET state='DUPED'
        WHERE date(ts)=date('now')
          AND COALESCE(state,'') <> 'DUPED'
    """)
    print(f"[OK] Marked {c.rowcount} rows as DUPED for today.")

    # 2) Optional: also clear paper_orders that reference today's signals (keeps things consistent)
    #    If your paper_orders.ref_signal_id exists, close today's OPEN orders.
    try:
        c.execute("""
            UPDATE paper_orders
            SET status='CLOSED'
            WHERE ref_signal_id IN (
              SELECT id FROM signals WHERE date(ts)=date('now')
            ) AND COALESCE(status,'')='OPEN'
        """)
        if c.rowcount is not None:
            print(f"[OK] Closed {c.rowcount} OPEN paper orders linked to today's signals.")
    except sqlite3.OperationalError:
        # paper_orders may not exist yet; ignore
        pass

    conn.commit()

    # Show an after snapshot
    c.execute("""
        SELECT date(ts), driver, reason, state, COUNT(*)
        FROM signals
        WHERE date(ts)=date('now')
        GROUP BY 1,2,3,4
        ORDER BY 2,3,4
    """)
    rows = c.fetchall()
    print("[SUMMARY today AFTER]:")
    for r in rows: print(r)

print("[DONE] Today reset complete. Run strategy-core again.")
