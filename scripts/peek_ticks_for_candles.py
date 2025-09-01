import sqlite3
con = sqlite3.connect(r"C:\teevra18\data\teevra18.db")
rows = con.execute("SELECT instrument_id, ts_event_ms, price, qty FROM ticks_for_candles LIMIT 5").fetchall()
con.close()
for r in rows:
    print(r)
