import sqlite3, sys
DB = r"C:\teevra18\data\teevra18.db"
con = sqlite3.connect(DB)
cur = con.cursor()
try:
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_strategies_catalog_id ON strategies_catalog(strategy_id)")
    con.commit()
    print("[OK] Unique index ensured on strategies_catalog(strategy_id).")
except Exception as e:
    print("[ERR]", e); sys.exit(1)
finally:
    con.close()
