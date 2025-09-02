import sqlite3
DB=r"C:\teevra18\data\teevra18.db"
with sqlite3.connect(DB) as conn:
    c=conn.cursor()
    print("[symbols + counts]")
    c.execute("SELECT symbol, COUNT(*) FROM candles_1m_std GROUP BY symbol ORDER BY symbol;")
    for r in c.fetchall(): print(r)
    sym="NIFTY"
    print(f"\n[last 10 closes for {sym}]")
    c.execute("""SELECT ts,c FROM candles_1m_std WHERE symbol=? ORDER BY ts DESC LIMIT 10;""",(sym,))
    rows=c.fetchall()
    for ts,cl in rows: print(ts, cl)
