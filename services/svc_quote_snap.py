# -*- coding: utf-8 -*-
"""
M2 — Quote Snapshot (Local-only, market-closed safe)
- Loads .env
- Calls DhanHQ /marketfeed/quote -> /ohlc -> /ltp (fallbacks)
- Writes to quote_snap
- Updates health regardless of schema variant (ts_utc / updated_utc / none)
- Verbose: prints HTTP status & body on failures
"""

from common.bootstrap import init_runtime
init_runtime()
import os, json, time, sqlite3, urllib.request, urllib.error, argparse
from pathlib import Path

# --------------------------- Load .env ----------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv(Path(r"C:\teevra18\.env"), override=True)
except Exception:
    pass  # ok if dotenv missing; but then env must already be set

# --------------------------- Config / Env -------------------------------------
DB = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
BASE = (os.getenv("DHAN_REST_BASE", "https://api.dhan.co") or "").rstrip("/")
TOKEN = os.getenv("DHAN_ACCESS_TOKEN") or ""
CLIENT_ID = os.getenv("DHAN_CLIENT_ID") or ""
TIMEOUT = int(os.getenv("HTTP_TIMEOUT_MS", "1200")) / 1000.0  # seconds
FORCE_LTP = os.getenv("M2_FORCE_LTP", "0").strip() in ("1","true","TRUE","yes","YES")

# Sanity
if not BASE:
    raise SystemExit("DHAN_REST_BASE missing. Set it in C:\\teevra18\\.env")
if not TOKEN or not CLIENT_ID:
    print("[WARN] DHAN credentials missing or empty. Check your .env (DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN).")

# --------------------------- HTTP helpers -------------------------------------
def _headers():
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": TOKEN,
        "client-id": CLIENT_ID,
    }

def _post(path, payload):
    url = f"{BASE}/v2{path}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=_headers(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            body = resp.read().decode("utf-8", "ignore")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "ignore")
        raise RuntimeError(f"HTTPError {e.code} {e.reason} for {path} :: {body}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"URLError for {path}: {e}")
    except Exception as e:
        raise RuntimeError(f"Request error for {path}: {e}")

# --------------------------- SQLite helpers -----------------------------------
def _ensure_health_ops():
    con = sqlite3.connect(DB)
    con.execute("""CREATE TABLE IF NOT EXISTS health(
        key TEXT PRIMARY KEY,
        value TEXT
    )""")
    con.execute("""CREATE TABLE IF NOT EXISTS ops_log(
        ts_utc TEXT DEFAULT (datetime('now')),
        level  TEXT,
        area   TEXT,
        msg    TEXT
    )""")
    con.commit()
    con.close()

def _health_cols(con): return {r[1] for r in con.execute("PRAGMA table_info(health)")}
def _ops_cols(con):    return {r[1] for r in con.execute("PRAGMA table_info(ops_log)")}

def _put_health(k, v):
    _ensure_health_ops()
    con = sqlite3.connect(DB)
    try:
        cols = _health_cols(con)
        fields, values, updates = ["key","value"], ["?","?"], ["value=excluded.value"]
        if "updated_utc" in cols:
            fields.append("updated_utc"); values.append("datetime('now')"); updates.append("updated_utc=datetime('now')")
        if "ts_utc" in cols:
            fields.append("ts_utc"); values.append("datetime('now')");     updates.append("ts_utc=datetime('now')")
        sql = f"INSERT INTO health({','.join(fields)}) VALUES({','.join(values)}) ON CONFLICT(key) DO UPDATE SET {', '.join(updates)}"
        con.execute(sql, (k, str(v))); con.commit()
    finally:
        con.close()

def _log(level, area, msg):
    _ensure_health_ops()
    con = sqlite3.connect(DB)
    try:
        cols = _ops_cols(con)
        if "ts_utc" in cols:
            con.execute("INSERT INTO ops_log(level,area,msg,ts_utc) VALUES(?,?,?,datetime('now'))", (level, area, msg))
        else:
            con.execute("INSERT INTO ops_log(level,area,msg) VALUES(?,?,?)", (level, area, msg))
        con.commit()
    finally:
        con.close()

def _get_ids():
    con = sqlite3.connect(DB); con.row_factory = sqlite3.Row
    try:
        r1 = con.execute("SELECT security_id FROM instrument_map WHERE key='IDX_I:NIFTY50'").fetchone()
        r2 = con.execute("SELECT security_id FROM instrument_map WHERE key='NSE_FNO:FUTIDX:NIFTY:NEAR'").fetchone()
        return (int(r1[0]) if r1 else None, int(r2[0]) if r2 else None)
    finally:
        con.close()

def _insert_rows(rows):
    con = sqlite3.connect(DB)
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS quote_snap(
          ts_utc TEXT NOT NULL,
          exchange_segment TEXT NOT NULL,
          security_id INTEGER NOT NULL,
          last_price REAL,
          average_price REAL,
          buy_quantity INTEGER,
          sell_quantity INTEGER,
          volume INTEGER,
          oi INTEGER,
          ohlc_open REAL,
          ohlc_high REAL,
          ohlc_low REAL,
          ohlc_close REAL,
          net_change REAL,
          upper_circuit_limit REAL,
          lower_circuit_limit REAL
        )""")
        con.executemany("""INSERT INTO quote_snap(
            ts_utc, exchange_segment, security_id, last_price, average_price, buy_quantity, sell_quantity,
            volume, oi, ohlc_open, ohlc_high, ohlc_low, ohlc_close, net_change, upper_circuit_limit, lower_circuit_limit
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
        con.commit()
    finally:
        con.close()

# --------------------------- Builders -----------------------------------------
def _row(now, seg, sid, src):
    get = lambda *keys: next((src[k] for k in keys if isinstance(src, dict) and k in src), None)
    ohlc = src.get("ohlc") if isinstance(src, dict) else None
    g = (lambda key: (ohlc.get(key) if isinstance(ohlc, dict) else None))
    return (
        now, seg, sid,
        get("last_price","lastPrice","ltp"),
        get("average_price","averagePrice","atp"),
        get("buy_quantity","buyQty","totalBuyQty"),
        get("sell_quantity","sellQty","totalSellQty"),
        get("volume","totalTradedVolume"),
        get("oi","openInterest"),
        g("open"), g("high"), g("low"), g("close"),
        get("net_change","netChange"),
        get("upper_circuit_limit","upperCircuitLimit"),
        get("lower_circuit_limit","lowerCircuitLimit"),
    )

# --------------------------- Core snapshot ------------------------------------
def snap_once(force_ltp=False, verbose=False):
    idx_sid, fut_sid = _get_ids()
    if not idx_sid or not fut_sid:
        raise RuntimeError("Missing instrument_map rows. Run resolve_instruments.py first.")

    t0 = time.perf_counter()
    rows, reasons = [], []
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    def try_call(tag, fn, *a, **k):
        try:
            return fn(*a, **k)
        except Exception as e:
            reasons.append(f"{tag}_err")
            _log("ERROR", "m2", f"{tag} failed: {e}")
            if verbose: print(f"[{tag}] {e}")
            return None

    if not force_ltp:
        # 1) /quote
        q = try_call("quote", _post, "/marketfeed/quote", {"IDX_I":[idx_sid], "NSE_FNO":[fut_sid]})
        if q and isinstance(q, dict):
            data = q.get("data", {})
            fs = ((data.get("NSE_FNO") or {}).get(str(fut_sid)) or {})
            if fs: rows.append(_row(now, "NSE_FNO", fut_sid, fs))
            else:  reasons.append("fut_empty_quote")
            idx_obj = ((data.get("IDX_I") or {}).get(str(idx_sid)) or {})
            if idx_obj and idx_obj.get("last_price") is not None:
                rows.append(_row(now, "IDX_I", idx_sid, idx_obj))
            else:
                reasons.append("idx_empty_quote")

        # 2) /ohlc fallback for any missing
        miss_idx = not any(r[1]=="IDX_I" for r in rows)
        miss_fut = not any(r[1]=="NSE_FNO" for r in rows)
        if miss_idx or miss_fut:
            payload = {}
            if miss_idx: payload["IDX_I"]=[idx_sid]
            if miss_fut: payload["NSE_FNO"]=[fut_sid]
            o = try_call("ohlc", _post, "/marketfeed/ohlc", payload)
            if o and isinstance(o, dict):
                dd = o.get("data", {})
                if miss_idx:
                    od = ((dd.get("IDX_I") or {}).get(str(idx_sid)) or {})
                    if od: rows.append(_row(now, "IDX_I", idx_sid, od))
                    else: reasons.append("idx_empty_ohlc")
                if miss_fut:
                    fd = ((dd.get("NSE_FNO") or {}).get(str(fut_sid)) or {})
                    if fd: rows.append(_row(now, "NSE_FNO", fut_sid, fd))
                    else: reasons.append("fut_empty_ohlc")

    # 3) /ltp last resort (works off-hours)
    miss_idx = not any(r[1]=="IDX_I" for r in rows)
    miss_fut = not any(r[1]=="NSE_FNO" for r in rows)
    if force_ltp or miss_idx or miss_fut:
        p = {}
        if force_ltp or miss_idx: p["IDX_I"]=[idx_sid]
        if force_ltp or miss_fut: p["NSE_FNO"]=[fut_sid]
        l = try_call("ltp", _post, "/marketfeed/ltp", p)
        if l and isinstance(l, dict):
            dd = l.get("data", {})
            if "IDX_I" in dd and (dd.get("IDX_I") or {}).get(str(idx_sid)):
                ld = (dd.get("IDX_I") or {}).get(str(idx_sid))
                rows = [r for r in rows if r[1]!="IDX_I"]
                rows.append(_row(now, "IDX_I", idx_sid, {"last_price": ld.get("last_price")}))
            if "NSE_FNO" in dd and (dd.get("NSE_FNO") or {}).get(str(fut_sid)):
                lf = (dd.get("NSE_FNO") or {}).get(str(fut_sid))
                rows = [r for r in rows if r[1]!="NSE_FNO"]
                rows.append(_row(now, "NSE_FNO", fut_sid, {"last_price": lf.get("last_price")}))

    # Persist + health
    if rows:
        _insert_rows(rows)
    elapsed_ms = int(1000*(time.perf_counter()-t0))
    _put_health("m2_latency_ms", elapsed_ms)
    _put_health("m2_status", "ok" if rows else "empty_no_data")
    _put_health("m2_last_symbols", f"IDX_I:{idx_sid},NSE_FNO:{fut_sid}")
    if verbose:
        _put_health("m2_debug_reason", ",".join(reasons) if reasons else "none")
    return elapsed_ms, len(rows), reasons

# --------------------------- Entrypoint ---------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--force-ltp", action="store_true", help="Only use /ltp (useful when market is closed)")
    ap.add_argument("--verbose", action="store_true", help="Log reasons to health and print HTTP errors")
    args = ap.parse_args()
    try:
        ms, n, reasons = snap_once(force_ltp=(args.force_ltp or FORCE_LTP), verbose=args.verbose)
        if args.verbose and reasons: print("reasons:", reasons)
        print(f"M2 snap OK: {n} rows in {ms} ms")
    except Exception as e:
        try:
            _put_health("m2_status", "error"); _log("ERROR","m2",str(e))
        finally:
            pass
        raise
