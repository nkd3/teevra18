import sqlite3
DB=r"C:\teevra18\data\teevra18.db"
with sqlite3.connect(DB) as conn:
    c=conn.cursor()
    c.execute("UPDATE universe_derivatives SET enabled=1 WHERE enabled IS NULL")
    # Also enable any FUT rows whose underlying looks like NIFTY/BANKNIFTY
    c.execute("""
      UPDATE universe_derivatives
      SET enabled=1
      WHERE UPPER(instrument_type)='FUT'
        AND (
          UPPER(underlying_symbol) LIKE '%NIFTY%'
          OR UPPER(underlying_symbol) LIKE '%BANK%'
        )
    """)
    conn.commit()
print("[OK] Enabled FUT rows for NIFTY/BANK.")
