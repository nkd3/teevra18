import sqlite3, time, pathlib, argparse
from datetime import datetime, timezone
def now_iso(): return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
def log_open(path):
    p = pathlib.Path(path); p.parent.mkdir(parents=True, exist_ok=True)
    def _log(msg):
        line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}\n"
        with open(p, "a", encoding="utf-8") as f: f.write(line)
        print(msg)
    return _log
def main():
    pa=argparse.ArgumentParser()
    pa.add_argument("--db", required=True)
    pa.add_argument("--log", required=True)
    args=pa.parse_args()
    log=log_open(args.log)
    conn=sqlite3.connect(args.db); cur=conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS paper_orders(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts TEXT, symbol TEXT, side TEXT, qty INTEGER,
      entry REAL, sl REAL, tp REAL, status TEXT, ref_signal_id INTEGER
    );""")
    conn.commit()
    log("[INIT] Paper trader running…")
    cur.execute("""SELECT id, ts, symbol, driver, action, rr, sl, tp
                   FROM signals WHERE state='NEW' ORDER BY ts ASC LIMIT 20""")
    rows=cur.fetchall()
    if not rows:
        log("[INFO] No NEW signals."); conn.close(); return
    for sid,ts,sym,drv,act,rr,sl,tp in rows:
        cur.execute("""INSERT INTO paper_orders(ts,symbol,side,qty,entry,sl,tp,status,ref_signal_id)
                       VALUES (?,?,?,?,?,?,?,'OPEN',?)""",
                    (now_iso(),sym,act,1,None,sl,tp,sid))
        cur.execute("UPDATE signals SET state='SENT' WHERE id=?", (sid,))
        conn.commit()
        log(f"[ORDER] {sym} {act} x1 (signal {sid})")
    log("[EXIT] Paper trader done."); conn.close()
if __name__=="__main__": main()
