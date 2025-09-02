import sqlite3, sys

DB = r"C:\teevra18\data\teevra18.db"

def table_exists(cur, name):
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return cur.fetchone() is not None

def view_exists(cur, name):
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='view' AND name=?;", (name,))
    return cur.fetchone() is not None

def cols(cur, table):
    cur.execute(f"PRAGMA table_info({table});")
    return [r[1] for r in cur.fetchall()]

def pick(candidates, pool):
    for c in candidates:
        if c in pool:
            return c
    return None

with sqlite3.connect(DB) as conn:
    cur = conn.cursor()

    if not table_exists(cur, "candles_1m"):
        # If nothing exists, just create a canonical empty table we’ll use going forward.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS candles_1m_std(
          symbol TEXT,
          ts TEXT,
          o REAL, h REAL, l REAL, c REAL, v REAL,
          PRIMARY KEY(symbol, ts)
        );
        """)
        conn.commit()
        print("[OK] No candles_1m found. Created empty canonical table candles_1m_std.")
        sys.exit(0)

    # Detect source columns from existing candles_1m
    csrc = cols(cur, "candles_1m")

    sym = pick(["symbol","underlying_symbol","ticker","tradingsymbol","instrument","SYMBOL","name","scrip"], csrc)
    ts  = pick(["ts","ts_utc","timestamp","time","datetime","bar_time"], csrc)
    o   = pick(["o","open","OPEN"], csrc)
    h   = pick(["h","high","HIGH"], csrc)
    l   = pick(["l","low","LOW"], csrc)
    c   = pick(["c","close","CLOSE","last","ltp"], csrc)
    v   = pick(["v","vol","volume","VOLUME"], csrc)

    # If we can map all 7 columns, create/replace a VIEW
    if all([sym, ts, o, h, l, c, v]):
        # Drop existing std table/view first (be tolerant)
        cur.execute("DROP VIEW IF EXISTS candles_1m_std;")
        # If a table by same name exists, drop it to prefer a live VIEW mapping
        cur.execute("DROP TABLE IF EXISTS candles_1m_std;")
        cur.execute(f"""
        CREATE VIEW candles_1m_std AS
        SELECT
          {sym} AS symbol,
          {ts}  AS ts,
          {o}   AS o,
          {h}   AS h,
          {l}   AS l,
          {c}   AS c,
          {v}   AS v
        FROM candles_1m;
        """)
        conn.commit()
        print("[OK] Created VIEW candles_1m_std mapping your existing columns to canonical names.")
        print(f"[MAP] symbol={sym}, ts={ts}, o={o}, h={h}, l={l}, c={c}, v={v}")
    else:
        # Can’t map → create an empty canonical TABLE so future code can write to it
        cur.execute("DROP VIEW IF EXISTS candles_1m_std;")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS candles_1m_std(
          symbol TEXT,
          ts TEXT,
          o REAL, h REAL, l REAL, c REAL, v REAL,
          PRIMARY KEY(symbol, ts)
        );
        """)
        conn.commit()
        print("[INFO] Could not fully map your columns. Created TABLE candles_1m_std (empty).")
        print("[HINT] We’ll write demo candles there; your ingestion can switch to populate this table directly.")
