import sqlite3
con = sqlite3.connect(r"C:\teevra18\data\teevra18.db")
for t in ("candles_1m","candles_5m","candles_15m","candles_60m"):
    c = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(t, c)
con.close()
