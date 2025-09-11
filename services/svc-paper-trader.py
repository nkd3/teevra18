# C:\teevra18\services\svc-paper-trader.py
import sqlite3, time, pathlib, argparse
from datetime import datetime, timezone

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def log_open(path):
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    def _log(msg):
        line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}\n"
        with open(p, "a", encoding="utf-8") as f:
            f.write(line)
        print(msg)
    return _log

def view_exists(cur, name):
    row = cur.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('view','table') AND name=?",
        (name,)
    ).fetchone()
    return row is not None

def cols_of(cur, table_or_view):
    return [r[1] for r in cur.execute(f"PRAGMA table_info({table_or_view})").fetchall()]

def main():
    pa = argparse.ArgumentParser()
    pa.add_argument("--db", required=True)
    pa.add_argument("--log", required=True)
    args = pa.parse_args()

    log = log_open(args.log)
    conn = sqlite3.connect(args.db)
    cur  = conn.cursor()

    # Ensure orders table exists (unchanged)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS paper_orders(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT, symbol TEXT, side TEXT, qty INTEGER,
          entry REAL, sl REAL, tp REAL, status TEXT, ref_signal_id INTEGER
        );
    """)
    conn.commit()

    log("[INIT] Paper trader running…")

    # ---- Selection strategy (robust & backward compatible) ----
    # 1) Prefer the compatibility view you created earlier.
    if view_exists(cur, "signals_legacy"):
        select_sql = """
            SELECT id, ts, symbol, driver, action, rr, sl, tp
            FROM signals_legacy
            ORDER BY id ASC
            LIMIT 20
        """
        post_action = "delete_from_view"   # DELETE FROM signals_legacy WHERE id=?
    else:
        # 2) Fall back to inspecting the 'signals' table and map columns.
        if not view_exists(cur, "signals"):
            log("[INFO] No signals source found."); conn.close(); return

        sig_cols = set(c.lower() for c in cols_of(cur, "signals"))

        if {"ts_utc","symbol","reason","side","rr","sl_per_lot","target","state"}.issubset(sig_cols):
            # New schema
            select_sql = """
                SELECT id, ts_utc AS ts, symbol, reason AS driver, side AS action,
                       rr, sl_per_lot AS sl, target AS tp
                FROM signals
                WHERE state='PENDING'
                ORDER BY ts_utc ASC
                LIMIT 20
            """
            post_action = "mark_sent_new"   # UPDATE signals SET state='SENT' WHERE id=?
        elif {"ts","symbol","driver","action","rr","sl","tp"}.issubset(sig_cols):
            # Legacy (no state column). We will delete processed rows.
            select_sql = """
                SELECT id, ts, symbol, driver, action, rr, sl, tp
                FROM signals
                ORDER BY ts ASC
                LIMIT 20
            """
            post_action = "delete_legacy"   # DELETE FROM signals WHERE id=?
        else:
            log("[INFO] signals table has an unexpected schema; nothing to do.")
            conn.close()
            return

    # Fetch candidate signals
    cur.execute(select_sql)
    rows = cur.fetchall()
    if not rows:
        log("[INFO] No NEW signals."); conn.close(); return

    # Process each signal into a paper order (keep existing logging style)
    for sid, ts, sym, drv, act, rr, sl, tp in rows:
        cur.execute("""
            INSERT INTO paper_orders(ts, symbol, side, qty, entry, sl, tp, status, ref_signal_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
        """, (now_iso(), sym, act, 1, None, sl, tp, sid))

        if post_action == "delete_from_view":
            # Your INSTEAD OF DELETE trigger on the view will delete from base table.
            cur.execute("DELETE FROM signals_legacy WHERE id=?", (sid,))
        elif post_action == "mark_sent_new":
            cur.execute("UPDATE signals SET state='SENT' WHERE id=?", (sid,))
        elif post_action == "delete_legacy":
            cur.execute("DELETE FROM signals WHERE id=?", (sid,))

        conn.commit()
        log(f"[ORDER] {sym} {act} x1 (signal {sid})")

    log("[EXIT] Paper trader done.")
    conn.close()

if __name__ == "__main__":
    main()
