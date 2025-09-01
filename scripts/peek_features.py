import os, sqlite3
db = os.getenv('DB_PATH', r'C:\teevra18\data\teevra18.db')
con = sqlite3.connect(db)
cur = con.cursor()
for row in cur.execute("""SELECT ts_fetch_utc, underlying, expiry, atm_strike,
                                 ROUND(pcr_oi,2), ROUND(iv_atm_ce,2), ROUND(iv_atm_pe,2)
                          FROM option_chain_features
                          ORDER BY ts_fetch_utc DESC, underlying
                          LIMIT 10"""):
    print(row)
con.close()
