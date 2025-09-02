import sqlite3, sys, traceback

DB = r"C:\teevra18\data\teevra18.db"

def table_exists(cur, name):
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return cur.fetchone() is not None

def get_cols(cur, table):
    cur.execute(f"PRAGMA table_info({table});")
    return [r[1] for r in cur.fetchall()]

def pick(cols, candidates, required=False, label=""):
    for c in candidates:
        if c in cols:
            return c
    if required:
        raise KeyError(f"Missing required column for {label}. Tried {candidates} in {cols}")
    return None

def find_broken_views(cur):
    """Return list of view names that error when selected from."""
    cur.execute("SELECT name, sql FROM sqlite_master WHERE type='view';")
    broken = []
    for name, _sql in cur.fetchall():
        try:
            cur.execute(f"SELECT * FROM [{name}] LIMIT 1;")
            _ = cur.fetchall()
        except Exception:
            broken.append(name)
    return broken

def drop_views(cur, names):
    for v in names:
        try:
            cur.execute(f"DROP VIEW IF EXISTS [{v}];")
            print(f"[DROP] Dropped broken view: {v}")
        except Exception:
            print(f"[WARN] Failed to drop view {v}:\n{traceback.format_exc()}")

with sqlite3.connect(DB) as conn:
    conn.isolation_level = None  # manual transactions
    cur = conn.cursor()

    # 1) Drop broken views that block schema changes (e.g., option_chain_view)
    try:
        cur.execute("BEGIN IMMEDIATE;")
        broken = find_broken_views(cur)
        if broken:
            print(f"[INFO] Found broken views blocking DDL: {broken}")
            drop_views(cur, broken)
        cur.execute("COMMIT;")
    except Exception:
        cur.execute("ROLLBACK;")
        print("[WARN] Could not scan/drop broken views safely, proceeding anyway.")
        # We still proceed — if DDL fails again, you'll see the message.

    # 2) Ensure universe_watchlist has the canonical schema: symbol TEXT PRIMARY KEY
    try:
        cur.execute("BEGIN IMMEDIATE;")

        if not table_exists(cur, "universe_watchlist"):
            cur.execute("""
                CREATE TABLE universe_watchlist(
                  symbol TEXT PRIMARY KEY
                );
            """)
            print("[OK] Created table universe_watchlist(symbol TEXT PRIMARY KEY)")
        else:
            wcols = get_cols(cur, "universe_watchlist")
            if "symbol" not in wcols or len(wcols) != 1:
                # Prepare new table with canonical schema
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS universe_watchlist_new(
                      symbol TEXT PRIMARY KEY
                    );
                """)

                # Try to migrate any recognizable column from the old table
                src_col = None
                for cand in ["symbol","SYMBOL","ticker","code","underlying_symbol","tradingsymbol","scrip","name"]:
                    if cand in wcols:
                        src_col = cand
                        break
                if src_col:
                    cur.execute(f"""
                        INSERT OR IGNORE INTO universe_watchlist_new(symbol)
                        SELECT DISTINCT TRIM({src_col})
                        FROM universe_watchlist
                        WHERE {src_col} IS NOT NULL AND TRIM({src_col}) <> '';
                    """)
                    print(f"[OK] Migrated existing watchlist values from {src_col} -> symbol")
                else:
                    print("[INFO] No recognizable column in existing universe_watchlist; starting clean.")

                # Swap tables atomically
                cur.execute("DROP TABLE universe_watchlist;")
                cur.execute("ALTER TABLE universe_watchlist_new RENAME TO universe_watchlist;")
                print("[OK] Recreated universe_watchlist with symbol TEXT PRIMARY KEY")

        # 3) Seed NIFTY / BANKNIFTY
        cur.executemany(
            "INSERT OR IGNORE INTO universe_watchlist(symbol) VALUES (?);",
            [("NIFTY",), ("BANKNIFTY",)]
        )

        # 4) Add NIFTY50 equities
        equities = set()

        if table_exists(cur, "universe_underlyings"):
            ucols = get_cols(cur, "universe_underlyings")
            u_symbol = pick(ucols, ["underlying_symbol","symbol","SYMBOL","ticker","code","name"], required=True, label="underlying_symbol")
            u_category = pick(ucols, ["category","type","kind"], required=False)
            if u_category:
                cur.execute(f"""
                    SELECT DISTINCT {u_symbol}
                    FROM universe_underlyings
                    WHERE {u_symbol} NOT IN ('NIFTY','BANKNIFTY')
                      AND UPPER({u_category})='EQUITY';
                """)
                equities.update([r[0] for r in cur.fetchall()])

        # Fallback inference via derivatives (FUTSTK/OPTSTK)
        if not equities and table_exists(cur, "universe_derivatives"):
            dcols = get_cols(cur, "universe_derivatives")
            d_instr = pick(dcols, ["instrument","instrument_type","INSTRUMENT","type","inst"], required=True, label="instrument")
            d_und   = pick(dcols, ["underlying_symbol","underlying","symbol","SYMBOL","ticker","name"], required=True, label="underlying_symbol")

            cur.execute(f"""
                SELECT DISTINCT {d_und}
                FROM universe_derivatives
                WHERE LOWER({d_instr}) IN ('futstk','optstk')
                  AND {d_und} NOT IN ('NIFTY','BANKNIFTY');
            """)
            equities.update([r[0] for r in cur.fetchall()])

        # Insert inferred equities
        if equities:
            cur.executemany(
                "INSERT OR IGNORE INTO universe_watchlist(symbol) VALUES (?);",
                [(e,) for e in equities if e and str(e).strip()]
            )

        # 5) Summary
        cur.execute("SELECT COUNT(*) FROM universe_watchlist;")
        total = cur.fetchone()[0]
        cur.execute("SELECT symbol FROM universe_watchlist ORDER BY symbol LIMIT 25;")
        peek = [r[0] for r in cur.fetchall()]

        cur.execute("COMMIT;")
        print(f"[OK] universe_watchlist updated. Total symbols: {total}")
        print("[Peek] first 25:", peek)

    except Exception as e:
        cur.execute("ROLLBACK;")
        print("[ERR] Failed to update watchlist due to:", e)
        print("Traceback:")
        traceback.print_exc()
        sys.exit(1)
