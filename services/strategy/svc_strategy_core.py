# C:\teevra18\services\strategy\svc_strategy_core.py
from common.bootstrap import init_runtime
init_runtime()
import os, sys, sqlite3, datetime, uuid, hashlib, argparse, json, warnings
from pathlib import Path
import pandas as pd
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

# --- Project root + lib path ---
PROJECT_ROOT = Path(r"C:\teevra18")
if str(PROJECT_ROOT / "lib") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "lib"))

from t18_db_helpers import t18_fetch_lot_size

# --- Paths & constants ---
DB  = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
CFG = Path(r"C:\teevra18\configs\m7_strategy.json")
MASTER_CSV = Path(r"C:\teevra18\data\api-scrip-master-detailed.csv")
VERSION = "m7-core-1.1"

RR_MIN = 2.0
SL_PER_LOT_CAP = 1000.0
GROUP_NAME_DEFAULT = "LIVE"
STRATEGY_ID = "core_v1"

# -------------------- Utility helpers --------------------
def now_utc():
    return datetime.datetime.utcnow().isoformat(timespec="seconds")

def deterministic_hash(s: str):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def map_side(raw_side: str) -> str:
    s = (raw_side or "").strip().upper()
    if s in ("BUY","B","LONG"): return "LONG"
    if s in ("SELL","S","SHORT"): return "SHORT"
    return "LONG"

def latest_ltp(conn, symbol: str):
    r = conn.execute(
        "SELECT ltp FROM quote_snap WHERE symbol=? ORDER BY ts_utc DESC LIMIT 1", (symbol,)
    ).fetchone()
    return float(r[0]) if r and r[0] is not None else None

def compute_bands(entry: float, lots: float, side: str,
                  rr_min=RR_MIN, sl_cap_per_lot=SL_PER_LOT_CAP):
    lots = max(1.0, float(lots))
    price_risk = sl_cap_per_lot / lots
    side_u = map_side(side)
    if side_u == "LONG":
        stop = entry - price_risk
        target = entry + price_risk * rr_min
    else:
        stop = entry + price_risk
        target = entry - price_risk * rr_min
    denom = (entry - stop)
    rr = abs((target - entry)/denom) if denom != 0 else 0.0
    sl_per_lot = abs(entry - stop) * lots
    return {"side": side_u,"entry":entry,"stop":stop,"target":target,"rr":rr,"sl_per_lot":sl_per_lot}

def make_ids(symbol, side, entry, stop, target, group_name):
    ts = now_utc()
    run_id = str(uuid.uuid4())
    hsrc = f"{symbol}|{side}|{entry}|{stop}|{target}|{STRATEGY_ID}|{group_name}|{ts}"
    deterministic = hashlib.sha1(hsrc.encode("utf-8")).hexdigest()
    return ts, run_id, deterministic

# -------------------- Health / limits --------------------
def get_breaker_state(conn):
    row = conn.execute(
        "SELECT value FROM health WHERE key IN ('m7_breaker','breaker_state') ORDER BY ts_utc DESC LIMIT 1"
    ).fetchone()
    return (row[0] if row else "RUNNING").upper()

def count_today(conn):
    return conn.execute(
        "SELECT COUNT(*) FROM signals WHERE substr(created_at_utc,1,10)=?",
        (datetime.datetime.utcnow().strftime("%Y-%m-%d"),)
    ).fetchone()[0]

# -------------------- Master CSV (optional) --------------------
import pandas as pd
from pandas.errors import EmptyDataError

def load_master():
    # Ensure MASTER_CSV points to a real file; fall back to empty frame if not
    try:
        if not MASTER_CSV.exists() or MASTER_CSV.stat().st_size == 0:
            print(f"[WARN] MASTER_CSV missing/empty at {MASTER_CSV}; continuing with empty master.")
            return pd.DataFrame()
        df = pd.read_csv(MASTER_CSV, low_memory=False)
        return df
    except EmptyDataError:
        print(f"[WARN] MASTER_CSV has no columns: {MASTER_CSV}; continuing with empty master.")
        return pd.DataFrame()
    except Exception as e:
        print(f"[WARN] MASTER_CSV load failed ({e}); continuing with empty master.")
        return pd.DataFrame()


# -------------------- Candle column detection (robust) --------------------
def _colmap(conn, table="candles_1m"):
    df = pd.read_sql_query(f"PRAGMA table_info({table});", conn)
    if df.empty:
        raise RuntimeError(f"{table}: table not found or empty schema.")
    return {str(r["name"]).lower(): str(r["name"]) for _, r in df.iterrows()}

def _pick_exact(colnames, options):
    for n in options:
        if n in colnames: return colnames[n]
    return None

def _pick_contains(colnames, patterns):
    for k_lower, orig in colnames.items():
        for p in patterns:
            if p in k_lower:
                return orig
    return None

def _guess_ts_by_sample(conn, table="candles_1m"):
    df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 200;", conn)
    if df.empty: return None
    for c in df.columns:
        s = df[c].dropna().head(20)
        if s.empty: continue
        if s.dtype == object:
            parsed = pd.to_datetime(s, errors="coerce", utc=True)
            if parsed.notna().mean() > 0.6:
                return c
        if pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(s):
            v = float(s.iloc[0])
            if v > 1e12: return c   # likely epoch ms
            if 1e9 < v < 2e10: return c  # likely epoch s
    return None

def detect_candle_columns(conn, table="candles_1m"):
    colnames = _colmap(conn, table)
    ts_col = _pick_exact(colnames, [
        "ts_utc","ts","bar_time_utc","timestamp","time_utc","dt_utc"
    ]) or _pick_contains(colnames, ["ts_utc","timestamp","datetime","time","bar"]) \
      or _guess_ts_by_sample(conn, table)
    if not ts_col:
        raise RuntimeError(f"{table}: could not find timestamp column.")

    sid_col = _pick_exact(colnames, [
        "security_id","securityid","instrument_id","instrumentid","token","id","symbol"
    ]) or _pick_contains(colnames, ["security","instrument","symbol","token","sid"])
    if not sid_col:
        raise RuntimeError(f"{table}: could not find security id column.")

    open_col  = _pick_exact(colnames, ["open","o","open_price"])  or _pick_contains(colnames, ["open"])
    high_col  = _pick_exact(colnames, ["high","h","high_price"])  or _pick_contains(colnames, ["high"])
    low_col   = _pick_exact(colnames, ["low","l","low_price"])    or _pick_contains(colnames, ["low"])
    close_col = _pick_exact(colnames, ["close","c","close_price","last_price","ltp"]) or _pick_contains(colnames, ["close","last","ltp"])
    missing = [n for n,v in {"open":open_col,"high":high_col,"low":low_col,"close":close_col}.items() if v is None]
    if missing:
        raise RuntimeError(f"{table}: missing OHLC columns -> {', '.join(missing)}")
    return {"ts": ts_col, "sid": sid_col, "o": open_col, "h": high_col, "l": low_col, "c": close_col}

def fetch_last2(conn, table="candles_1m"):
    cols = detect_candle_columns(conn, table)
    ts, sid, o, h, l, c = cols["ts"], cols["sid"], cols["o"], cols["h"], cols["l"], cols["c"]
    # Portable: fetch last N and pair in pandas (works even if window functions are limited)
    sql = f"SELECT {ts} AS ts_col, {sid} AS sid, {o} AS o, {h} AS h, {l} AS l, {c} AS c FROM {table};"
    df = pd.read_sql_query(sql, conn)
    if df.empty: return {}
    # sort per sid by ts; keep last 2
    pairs = {}
    for sid_val, g in df.groupby("sid"):
        g = g.sort_values("ts_col")
        if len(g) >= 2:
            prev = {"ts_utc": g.iloc[-2]["ts_col"], "security_id": sid_val,
                    "open": g.iloc[-2]["o"], "high": g.iloc[-2]["h"],
                    "low": g.iloc[-2]["l"], "close": g.iloc[-2]["c"]}
            curr = {"ts_utc": g.iloc[-1]["ts_col"], "security_id": sid_val,
                    "open": g.iloc[-1]["o"], "high": g.iloc[-1]["h"],
                    "low": g.iloc[-1]["l"], "close": g.iloc[-1]["c"]}
            pairs[str(sid_val)] = (prev, curr)
    return pairs

# -------------------- Strategies --------------------
def strat_bo2(prev,curr):
    if curr["close"]>prev["high"]: return {"side":"LONG","reason":"close>prev_high"}
    if curr["close"]<prev["low"] : return {"side":"SHORT","reason":"close<prev_low"}
    return None

def strat_rb1(prev,curr):
    rng=max(1e-9, prev["high"]-prev["low"])
    upper=prev["high"]-0.1*rng; lower=prev["low"]+0.1*rng
    if curr["close"]<=lower: return {"side":"LONG","reason":"close<=lower10%"}
    if curr["close"]>=upper: return {"side":"SHORT","reason":"close>=upper10%"}
    return None

def build_candidate(group,strat_id,prev,curr,min_rr):
    if strat_id=="BO2": ans=strat_bo2(prev,curr)
    elif strat_id=="RB1": ans=strat_rb1(prev,curr)
    else: ans=None
    if not ans: return None
    side=ans["side"]; entry=float(curr["close"])
    if side=="LONG":
        stop=float(prev["low"]); sl=max(1e-9, entry-stop); target=entry+min_rr*sl
    else:
        stop=float(prev["high"]); sl=max(1e-9, stop-entry); target=entry-min_rr*sl
    rr=abs((target-entry)/(entry-stop)) if entry!=stop else 0.0
    return {"group_name":group,"strategy_id":strat_id,"side":side,"entry":entry,"stop":stop,"target":target,"rr":rr,"reason":ans["reason"]}

# -------------------- Emitters --------------------
def emit_signal_base(conn, symbol: str, group_name: str, c: dict, lot: float):
    ts_utc, run_id, deterministic = make_ids(symbol, c["side"], c["entry"], c["stop"], c["target"], group_name)
    sl_per_lot = abs(c["entry"]-c["stop"])*lot
    payload = {
        "ts_utc": ts_utc, "created_at_utc": ts_utc,
        "security_id": symbol, "symbol": symbol,
        "group_name": group_name, "strategy_id": c["strategy_id"],
        "side": c["side"], "entry": c["entry"], "stop": c["stop"], "target": c["target"],
        "rr": c["rr"], "sl_per_lot": sl_per_lot, "reason": c["reason"],
        "version": VERSION, "state": "PENDING", "deterministic_hash": deterministic, "run_id": run_id,
        "direction": c["side"], "entry_price": c["entry"], "lot_size": lot
    }
    cols, vals = list(payload.keys()), list(payload.values())
    q = f"INSERT OR IGNORE INTO signals({','.join(cols)}) VALUES({','.join(['?']*len(cols))})"
    conn.execute(q, vals)
    print(f"[OK][BASE] {symbol} {c['side']} E:{c['entry']} S:{c['stop']} T:{c['target']} RR:{c['rr']:.2f} SL/lot:{sl_per_lot:.2f}")

def emit_signal_fallback(conn, symbol: str, group_name: str, raw_side: str, entry: float, lot: float):
    ts_utc = now_utc()
    run_id = str(uuid.uuid4())
    deterministic = hashlib.sha1(f"{symbol}|{raw_side}|{entry}|{STRATEGY_ID}|{group_name}|{ts_utc}".encode("utf-8")).hexdigest()
    payload = {
        "ts_utc": ts_utc, "created_at_utc": ts_utc,
        "security_id": symbol, "symbol": symbol,
        "group_name": group_name, "strategy_id": STRATEGY_ID,
        "side": raw_side, "entry": entry, "stop": entry, "target": entry,
        "rr": 0.0, "sl_per_lot": 0.0, "reason": "fallback_emit",
        "version": VERSION, "state": "PENDING", "deterministic_hash": deterministic, "run_id": run_id,
        "direction": raw_side, "entry_price": entry, "lot_size": lot
    }
    cols, vals = list(payload.keys()), list(payload.values())
    q = f"INSERT INTO signals({','.join(cols)}) VALUES({','.join(['?']*len(cols))})"
    conn.execute(q, vals)
    print(f"[OK][FALLBACK] {symbol} {raw_side} entry_price={entry} lot_size={lot}")

# -------------------- Main --------------------
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["generate"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--max-signals", type=int, default=None)
    args=ap.parse_args()

    cfg=json.loads(Path(CFG).read_text(encoding="utf-8-sig"))

    min_rr=float(cfg["risk"]["min_rr"])
    max_sl=float(cfg["risk"]["max_sl_per_lot"])
    day_cap=int(cfg["risk"]["max_trades_per_day"])
    groups_cfg = cfg.get("groups", [])

    with sqlite3.connect(DB) as conn:
        if get_breaker_state(conn) in ("PAUSED","HALT"):
            print("[BREAKER] Paused/Halt."); return
        today = count_today(conn)
        if today>=day_cap:
            print(f"[LIMIT] {today}/{day_cap} today."); return

        # fetch last 2 candles per instrument (robust column detection)
        try:
            pairs = fetch_last2(conn, table="candles_1m")
        except Exception as e:
            print(f"[ERR] candles_1m column detection: {e}")
            return

        # master csv (optional) – gives symbols and lots; else we’ll fall back to tolerant lookup
        master = load_master()

        emitted = 0
        for sid,(prev,curr) in pairs.items():
            # prefer master symbol; else fall back to tolerant lot lookup path
            md = master.get(str(sid), {"symbol": None, "lot_size": None})
            symbol = md["symbol"] or str(sid)
            # lot preference: master -> helper -> 1.0
            lot = md["lot_size"] if md["lot_size"] else t18_fetch_lot_size(conn, symbol, default_ls=1.0)

            for grp in groups_cfg:
                if not grp.get("enabled", True):
                    continue
                gname = grp.get("name", GROUP_NAME_DEFAULT)
                emit_mode = (grp.get("emit_mode") or "base").lower()  # "base" or "fallback"

                for st in grp.get("strategies", []):
                    if not st.get("enabled", True):
                        continue
                    strat_id = st.get("id", "NA")
                    cand = build_candidate(gname, strat_id, prev, curr, min_rr)
                    if not cand:
                        continue

                    # hard gates (M7-side)
                    sl_per_lot = abs(cand["entry"] - cand["stop"]) * float(lot or 1.0)
                    if sl_per_lot > max_sl:         # SL per lot cap
                        continue
                    if cand["rr"] + 1e-9 < min_rr:   # RR threshold, strict (no eps baked globally)
                        continue

                    if args.dry_run:
                        print(f"[DRY] {symbol} | {gname}/{strat_id} | {cand['side']} "
                              f"| E:{cand['entry']} S:{cand['stop']} T:{cand['target']} "
                              f"| RR:{cand['rr']:.2f} | SL/lot:{sl_per_lot:.2f} | mode={emit_mode}")
                        emitted += 1
                        continue

                    if emit_mode == "fallback":
                        # let M8 compute bands using direction, entry_price, lot_size
                        emit_signal_fallback(conn, symbol, gname, cand["side"], cand["entry"], float(lot or 1.0))
                    else:
                        # preferred: write base set now
                        emit_signal_base(conn, symbol, gname, cand, float(lot or 1.0))
                    emitted += 1

        if not args.dry_run:
            conn.commit()

        if args.max_signals is not None and emitted > args.max_signals:
            print(f"[NOTE] Emitted {emitted}; consider using --max-signals to limit output.")
        else:
            print(f"Emitted {emitted} signal(s).")

if __name__=="__main__":
    main()
