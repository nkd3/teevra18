# C:\teevra18\services\candles\svc_candles.py
from common.bootstrap import init_runtime
init_runtime()
import os, argparse, sqlite3
from pathlib import Path
import pandas as pd

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = lambda *a, **k: None

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", r"C:\teevra18"))
ENV_PATH     = PROJECT_ROOT / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH.as_posix())

DB_PATH   = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
LOCAL_TZ  = os.getenv("TZ", "Asia/Kolkata")
CHUNK_TICKS = int(os.getenv("CANDLE_CHUNK_TICKS", "500000"))

def get_con():
    con = sqlite3.connect(DB_PATH.as_posix(), timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con

def ensure_schema(con: sqlite3.Connection):
    # robust schema creation; auto-repair only if empty
    def table_exists(tbl):
        r = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,)).fetchone()
        return r is not None
    def has_col(tbl, col):
        try:
            rows = con.execute(f"PRAGMA table_info({tbl})").fetchall()
            return any(r[1] == col for r in rows)
        except Exception:
            return False
    def is_empty(tbl):
        try:
            c = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            return (c == 0)
        except Exception:
            return True

    for tf in ("1m","5m","15m","60m"):
        table = f"candles_{tf}"
        if table_exists(table) and not has_col(table, "t_start"):
            if is_empty(table):
                con.execute(f"DROP TABLE {table}")
            else:
                print(f"[WARN] {table} exists without t_start and has data. Skipping auto-repair.")
                continue

        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            instrument_id TEXT NOT NULL,
            t_start       INTEGER NOT NULL,  -- epoch seconds (IST bucket start mapped to UTC)
            open          REAL NOT NULL,
            high          REAL NOT NULL,
            low           REAL NOT NULL,
            close         REAL NOT NULL,
            volume        REAL NOT NULL,
            trades        INTEGER NOT NULL,
            vwap          REAL,
            PRIMARY KEY (instrument_id, t_start)
        );
        """)

        # create index if column available
        if has_col(table, "t_start"):
            con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_t ON {table}(t_start);")

    con.execute("""
    CREATE TABLE IF NOT EXISTS candles_checkpoint (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """)

def floor_to_bucket(ts_utc_ms: pd.Series, minutes: int) -> pd.Series:
    dt_utc = pd.to_datetime(ts_utc_ms, unit="ms", utc=True)
    dt_ist = dt_utc.dt.tz_convert(LOCAL_TZ)
    floored = dt_ist.dt.floor(f"{minutes}min")
    floored_utc = floored.dt.tz_convert("UTC")
    # Convert to epoch seconds (int64) without .view()
    return floored_utc.astype("int64") // 10**9


def aggregate_to_candles(df_ticks: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df_ticks.empty:
        return df_ticks.iloc[:0]

    df = df_ticks.copy()
    if "qty" not in df.columns:
        df["qty"] = 1

    df["bucket"] = floor_to_bucket(df["ts_event_ms"], minutes)
    df = df.sort_values(["instrument_id","ts_event_ms"])

    # group per instrument + bucket
    g = df.groupby(["instrument_id","bucket"], sort=False)

    o = g["price"].first().rename("open")
    h = g["price"].max().rename("high")
    l = g["price"].min().rename("low")
    c = g["price"].last().rename("close")
    v = g["qty"].sum().astype(float).rename("volume")
    n = g.size().astype(int).rename("trades")

    # Compute pv = sum(price*qty) without using groupby.apply()
    pv = (df.assign(pxq=df["price"] * df["qty"])
            .groupby(["instrument_id","bucket"], sort=False)["pxq"]
            .sum()
            .rename("pv"))
    vwap = (pv / v).rename("vwap")

    res = pd.concat([o,h,l,c,v,n,vwap], axis=1).reset_index().rename(columns={"bucket":"t_start"})
    return res[["instrument_id","t_start","open","high","low","close","volume","trades","vwap"]]


def upsert_candles(con: sqlite3.Connection, df: pd.DataFrame, tf_table: str):
    if df.empty:
        return 0
    rows = list(df.itertuples(index=False, name=None))
    q = f"""
    INSERT INTO {tf_table}
        (instrument_id, t_start, open, high, low, close, volume, trades, vwap)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(instrument_id, t_start) DO UPDATE SET
        open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close,
        volume=excluded.volume, trades=excluded.trades, vwap=excluded.vwap;
    """
    cur = con.cursor()
    cur.executemany(q, rows)
    return cur.rowcount

def rollup_minutes(con: sqlite3.Connection, df_ticks: pd.DataFrame):
    spec = {"1m":1, "5m":5, "15m":15, "60m":60}
    counts = {}
    for tf, mins in spec.items():
        dfc = aggregate_to_candles(df_ticks, mins)
        counts[tf] = upsert_candles(con, dfc, f"candles_{tf}")
    return counts

def get_ticks_iter(con, where_sql="", params=()):
    # IMPORTANT: read from the COMPATIBILITY VIEW, not ticks_raw
    base = "SELECT instrument_id, ts_event_ms, price, qty FROM ticks_for_candles"
    if where_sql:
        base += " WHERE " + where_sql
    base += " ORDER BY ts_event_ms ASC"
    cur = con.cursor()
    cur.execute(base, params)
    while True:
        rows = cur.fetchmany(CHUNK_TICKS)
        if not rows:
            break
        yield pd.DataFrame(rows, columns=["instrument_id","ts_event_ms","price","qty"])

def backfill(con: sqlite3.Connection, since_ms: int|None, until_ms: int|None, instrument_id: str|None):
    where, params = [], []
    if since_ms is not None:
        where.append("ts_event_ms >= ?"); params.append(int(since_ms))
    if until_ms is not None:
        where.append("ts_event_ms <= ?"); params.append(int(until_ms))
    if instrument_id:
        where.append("instrument_id = ?"); params.append(instrument_id)
    where_sql = " AND ".join(where) if where else ""
    total = {"1m":0,"5m":0,"15m":0,"60m":0}
    for df in get_ticks_iter(con, where_sql, tuple(params)):
        res = rollup_minutes(con, df)
        for k,v in res.items(): total[k]+=v
    return total

def get_ck(con, key):
    r = con.execute("SELECT value FROM candles_checkpoint WHERE key=?", (key,)).fetchone()
    return int(r[0]) if r else None

def set_ck(con, key, value):
    con.execute("""
    INSERT INTO candles_checkpoint(key, value, updated_at)
    VALUES(?, ?, datetime('now'))
    ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
    """, (key, str(int(value))))

def follow(con: sqlite3.Connection, poll_ms=1500, lookback_ms=600000):
    import time as _t
    key = "candles_last_ts"
    last = get_ck(con, key)
    while True:
        try:
            if last is None:
                m = con.execute("SELECT MAX(ts_event_ms) FROM ticks_for_candles").fetchone()[0]
                if m is None:
                    _t.sleep(poll_ms/1000); continue
                last = max(0, int(m) - lookback_ms)
            rows = con.execute("""
                SELECT instrument_id, ts_event_ms, price, qty
                FROM ticks_for_candles WHERE ts_event_ms >= ?
                ORDER BY ts_event_ms ASC
            """, (last,)).fetchall()
            if rows:
                df = pd.DataFrame(rows, columns=["instrument_id","ts_event_ms","price","qty"])
                rollup_minutes(con, df)
                last = int(df["ts_event_ms"].max())
                set_ck(con, key, last)
            _t.sleep(poll_ms/1000)
        except KeyboardInterrupt:
            print("Stopped."); break

def main():
    ap = argparse.ArgumentParser(description="Teevra18 Candles Service")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backfill")
    b.add_argument("--since-ms", type=int, default=None)
    b.add_argument("--until-ms", type=int, default=None)
    b.add_argument("--instrument", type=str, default=None)
    f = sub.add_parser("follow")
    f.add_argument("--poll-ms", type=int, default=1500)
    f.add_argument("--lookback-ms", type=int, default=600000)
    args = ap.parse_args()
    con = get_con()
    ensure_schema(con)
    if args.cmd == "backfill":
        print("UPSERT counts:", backfill(con, args.since_ms, args.until_ms, args.instrument))
    elif args.cmd == "follow":
        print("Following live… Ctrl+C to stop")
        follow(con, args.poll_ms, args.lookback_ms)

if __name__ == "__main__":
    main()
