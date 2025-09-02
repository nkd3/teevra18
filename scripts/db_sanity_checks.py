import sqlite3, sys

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
        raise KeyError(f"Missing required column for {label}. Tried: {candidates} in {cols}")
    return None

with sqlite3.connect(DB) as conn:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # ---------- underlyings ----------
    if not table_exists(cur, "universe_underlyings"):
        print("[WARN] Table 'universe_underlyings' not found.")
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        print("Existing tables:", [r[0] for r in cur.fetchall()])
        sys.exit(0)

    ucols = get_cols(cur, "universe_underlyings")
    u_symbol   = pick(ucols, ["underlying_symbol", "symbol", "SYMBOL", "ticker", "code"], required=True, label="underlying symbol")
    u_display  = pick(ucols, ["display_name", "name", "DISPLAY_NAME", "long_name"], required=False)
    u_category = pick(ucols, ["category", "type", "kind"], required=False)

    print("\n[underlyings → NIFTY/BANKNIFTY/RELIANCE/TCS]")
    sel_cols = [u_symbol + " AS underlying_symbol"]
    if u_display:  sel_cols.append(u_display + " AS display_name")
    if u_category: sel_cols.append(u_category + " AS category")
    sel_expr = ", ".join(sel_cols)

    q_underlyings = f"""
        SELECT {sel_expr}
        FROM universe_underlyings
        WHERE {u_symbol} IN ('NIFTY','BANKNIFTY','RELIANCE','TCS')
        ORDER BY {u_symbol};
    """
    for r in conn.execute(q_underlyings):
        print(tuple(r))

    # ---------- derivatives ----------
    if not table_exists(cur, "universe_derivatives"):
        print("\n[WARN] Table 'universe_derivatives' not found.")
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        print("Existing tables:", [r[0] for r in cur.fetchall()])
        sys.exit(0)

    dcols = get_cols(cur, "universe_derivatives")

    # include 'instrument_type' in candidates
    d_instr = pick(
        dcols,
        ["instrument", "instrument_type", "INSTRUMENT", "inst", "type"],
        required=True,
        label="instrument"
    )
    d_und   = pick(
        dcols,
        ["underlying_symbol", "underlying", "SYMBOL", "symbol", "ticker"],
        required=True,
        label="underlying_symbol"
    )

    print("\n[derivatives → counts by instrument & underlying (sample)]")
    q_deriv_counts = f"""
        SELECT {d_instr} AS instrument, {d_und} AS underlying_symbol, COUNT(*) AS cnt
        FROM universe_derivatives
        GROUP BY {d_instr}, {d_und}
        ORDER BY {d_instr}, {d_und}
        LIMIT 25;
    """
    for r in conn.execute(q_deriv_counts):
        print(tuple(r))

    print("\n[quick totals]")
    for label, sql in [
        ("underlyings total", "SELECT COUNT(*) FROM universe_underlyings;"),
        ("derivatives total", "SELECT COUNT(*) FROM universe_derivatives;"),
    ]:
        cur.execute(sql)
        print(label, cur.fetchone()[0])

    print("\n[Schema] universe_underlyings:", ucols)
    print("[Schema] universe_derivatives:", dcols)
