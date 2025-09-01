import os, sqlite3, time, json, subprocess, sys

DB = os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db")

def val():
    con = sqlite3.connect(DB)
    h = dict(con.execute("SELECT key,value FROM health WHERE key IN ('m2_latency_ms','m2_status','m2_last_symbols')").fetchall())
    # Confirm NIFTY50 (IDX_I) row exists within last 5 minutes
    r = con.execute("""
      SELECT COUNT(1) FROM quote_snap
      WHERE exchange_segment='IDX_I'
        AND ts_utc >= datetime('now','-5 minutes')
    """).fetchone()[0]
    con.close()
    return h, r

if __name__ == "__main__":
    # Trigger a fresh snap
    out = subprocess.run([sys.executable, r"C:\teevra18\services\svc_quote_snap.py"], capture_output=True, text=True)
    print(out.stdout.strip())
    h, count_idx = val()
    try:
        ms = int(h.get("m2_latency_ms","9999"))
    except: ms = 9999
    ok_latency = (ms <= 1000)
    ok_nifty50 = (count_idx >= 1)
    print(f"Latency(ms): {ms}  (<=1000 OK? {ok_latency})")
    print(f"NIFTY50 present? {ok_nifty50}")
    print(f"Status: {h.get('m2_status')}, Symbols: {h.get('m2_last_symbols')}")
    print(f"PASS: {ok_latency and ok_nifty50 and h.get('m2_status')=='ok'}")
