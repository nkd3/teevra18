import sqlite3, textwrap

DB = r"C:\teevra18\data\teevra18.db"
with sqlite3.connect(DB) as conn:
    c = conn.cursor()

    print("\n[all tables]")
    c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = [r[0] for r in c.fetchall()]
    for t in tables: print(" -", t)

    print("\n[candidate derivative-like tables]")
    c.execute("""
      SELECT name FROM sqlite_master
      WHERE type='table' AND (
          lower(name) LIKE '%deriv%' OR
          lower(name) LIKE '%option%' OR
          lower(name) LIKE '%fut%' OR
          lower(name) LIKE '%instrument%'
      )
      ORDER BY name;
    """)
    candidates = [r[0] for r in c.fetchall()]
    for t in candidates: print(" -", t)

    def cols(t):
        c.execute(f"PRAGMA table_info({t});")
        return [r[1] for r in c.fetchall()]

    print("\n[columns for candidates]")
    for t in candidates:
        print(f" {t}: {cols(t)}")

    # show a few rows for the best-looking table
    preferred = None
    for t in candidates:
        cs = [x.lower() for x in cols(t)]
        if any(x in cs for x in ['instrument','instrument_type','type']) and \
           any(x in cs for x in ['underlying_symbol','underlying','symbol','ticker','name']):
            preferred = t
            break
    if preferred:
        print(f"\n[sample rows from {preferred}]")
        c.execute(f"SELECT * FROM {preferred} LIMIT 3;")
        rows = c.fetchall()
        # get columns to print dict-like
        c.execute(f"PRAGMA table_info({preferred});")
        colnames = [r[1] for r in c.fetchall()]
        for r in rows:
            print({colnames[i]: r[i] for i in range(len(colnames))})
    else:
        print("\n[WARN] No obvious derivatives table found.")
