import sqlite3
DB=r"C:\teevra18\data\teevra18.db"
with sqlite3.connect(DB) as conn:
    c=conn.cursor()
    print("[counts by instrument_type (UPPER)]")
    c.execute("SELECT UPPER(instrument_type), COUNT(*) FROM universe_derivatives GROUP BY UPPER(instrument_type) ORDER BY 2 DESC")
    for r in c.fetchall(): print(r)

    print("\n[NIFTY/BANKNIFTY FUTIDX sample rows]")
    for und in ("NIFTY","BANKNIFTY"):
        c.execute("""
          SELECT instrument_type, underlying_symbol, tradingsymbol, symbol, expiry, strike, option_type, enabled
          FROM universe_derivatives
          WHERE UPPER(underlying_symbol)=UPPER(?) AND UPPER(instrument_type)='FUTIDX'
          ORDER BY expiry ASC LIMIT 5
        """,(und,))
        print(und, "FUTIDX ->", c.fetchall())

    print("\n[NIFTY/BANKNIFTY OPTIDX near-ATM sample rows]")
    for und in ("NIFTY","BANKNIFTY"):
        # choose some plausible ATM area by picking smallest strike
        c.execute("""
          SELECT instrument_type, underlying_symbol, tradingsymbol, symbol, expiry, strike, option_type, enabled
          FROM universe_derivatives
          WHERE UPPER(underlying_symbol)=UPPER(?) AND UPPER(instrument_type)='OPTIDX'
          ORDER BY ABS(COALESCE(strike,0)) ASC, expiry ASC LIMIT 5
        """,(und,))
        print(und, "OPTIDX ->", c.fetchall())
