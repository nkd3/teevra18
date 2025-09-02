import sqlite3
DB=r"C:\teevra18\data\teevra18.db"
SYMS=("RELIANCE","TCS","INFY","HDFCBANK")
with sqlite3.connect(DB) as conn:
    c=conn.cursor()
    for s, n in c.execute(
        f"SELECT symbol, COUNT(*) FROM candles_1m_std WHERE symbol IN ({','.join(['?']*len(SYMS))}) GROUP BY symbol",
        SYMS
    ):
        print(s, n)
