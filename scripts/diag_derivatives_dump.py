import sqlite3
DB=r"C:\teevra18\data\teevra18.db"
with sqlite3.connect(DB) as conn:
    c=conn.cursor()
    print("[distinct instrument_type]")
    c.execute("SELECT DISTINCT instrument_type FROM universe_derivatives")
    print(c.fetchall())

    print("\n[distinct underlying_symbol for FUT]")
    c.execute("""
      SELECT DISTINCT underlying_symbol
      FROM universe_derivatives
      WHERE UPPER(instrument_type)='FUT'
      ORDER BY 1
    """)
    unds = [r[0] for r in c.fetchall()]
    print(unds)

    print("\n[FUT sample rows]")
    c.execute("""
      SELECT instrument_type, underlying_symbol, tradingsymbol, symbol, expiry, strike, option_type, enabled
      FROM universe_derivatives
      WHERE UPPER(instrument_type)='FUT'
      ORDER BY expiry ASC LIMIT 10
    """)
    for r in c.fetchall():
        print(r)
