# C:\teevra18\services\historical\svc_historical_loader.py
# Teevra18 M6 — Historical Loader (NSE only) → Parquet partitions
# - Forces NSE_EQ / NSE_FNO mapping (won't mix BSE)
# - Trims uneven arrays safely (no pandas length errors)
# - Accepts manual (--security-id ...) or CSV+symbols (NSE filtered)

from __future__ import annotations
import os, sys, time, json, re
import argparse
import datetime as dt
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable=None, **kwargs):
        return iterable if iterable is not None else []

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", r"C:\teevra18"))
DATA_DIR     = Path(os.getenv("DATA_DIR", r"C:\teevra18\data"))
HIST_DIR     = DATA_DIR / "history"
ENV_PATH     = PROJECT_ROOT / ".env"
DEFAULT_BASE = os.getenv("DHAN_REST_BASE", "https://api.dhan.co").rstrip("/")

load_dotenv(ENV_PATH)
DHAN_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
DHAN_BASE  = os.getenv("DHAN_REST_BASE", DEFAULT_BASE).rstrip("/")
if not DHAN_TOKEN:
    print("[FATAL] DHAN_ACCESS_TOKEN missing in .env"); sys.exit(2)

HEADERS = {"Content-Type": "application/json", "Accept": "application/json", "access-token": DHAN_TOKEN}

def parse_date(d: str, with_time=False) -> str:
    d = d.strip()
    return (d + " 00:00:00") if (with_time and len(d) == 10) else (d[:10] if not with_time else d)

def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)
def N(x): return re.sub(r'[^A-Z0-9]', '', str(x).upper()) if x is not None else ""

def sanitize_symbol(s: str) -> str:
    s = s or ""
    s = re.sub(r'[\\/:*?"<>|]+', "_", s)
    s = re.sub(r'\s+', "_", s.strip())
    return s[:80] if s else "UNKNOWN"

def map_exchange_segment(seg:str, instrument:str) -> str:
    s, i = N(seg), N(instrument)
    if s in {"E","EQ","CM","EQUITY","CASH"}: return "NSE_EQ"
    if s in {"D","FO","FNO","F&O","DERIVATIVE","DERIVATIVES"}: return "NSE_FNO"
    if i.startswith("OPT") or i.startswith("FUT"): return "NSE_FNO"
    return "NSE_EQ"

def dh_post(endpoint: str, body: dict, retries=3, pause=0.5) -> dict:
    url = f"{DHAN_BASE}{endpoint}"
    last = None
    for attempt in range(1, retries + 1):
        r = requests.post(url, headers=HEADERS, data=json.dumps(body), timeout=30)
        last = r
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                pass
        # Try to parse error body
        try:
            err = r.json()
        except Exception:
            err = {"raw": r.text[:400] if r is not None else "<no response>"}
        # Soft-skip: DH-905 + "no data present"
        if isinstance(err, dict) and str(err.get("errorCode","")).upper() == "DH-905":
            msg = str(err.get("errorMessage","")).lower()
            if "no data present" in msg or "unable to fetch data" in msg:
                # Return empty payload structure the rest of the code understands
                return {"open": [], "high": [], "low": [], "close": [], "volume": [], "timestamp": [], "open_interest": []}
        if attempt < retries:
            time.sleep(pause * attempt)
    # Hard-fail otherwise
    try:
        err = last.json()
    except Exception:
        err = last.text[:400] if last is not None else "<no response>"
    raise RuntimeError(f"Dhan API failed ({endpoint}): {last.status_code if last else 'n/a'} | body={body} | resp={err}")


def to_frame(payload: dict, meta: dict) -> pd.DataFrame:
    o = payload.get("open", []) or []
    h = payload.get("high", []) or []
    l = payload.get("low", []) or []
    c = payload.get("close", []) or []
    v = payload.get("volume", []) or []
    t = payload.get("timestamp", []) or []
    oi= payload.get("open_interest", []) or []
    L = min(len(o), len(h), len(l), len(c), len(v), len(t), len(oi) if oi else 10**9)
    if L == 0:
        return pd.DataFrame(columns=[
            "ts_utc","ts_ist","open","high","low","close","volume","open_interest",
            "securityId","symbol","segment","instrument","expiryCode","timeframe","year","month"
        ])
    if len(oi) == 0: oi = [0]*L
    df = pd.DataFrame({
        "open":o[:L], "high":h[:L], "low":l[:L], "close":c[:L],
        "volume":v[:L], "epoch":t[:L], "open_interest":oi[:L]
    })
    ts_utc = pd.to_datetime(df["epoch"], unit="s", utc=True)
    try: ts_ist = ts_utc.dt.tz_convert("Asia/Kolkata")
    except Exception: ts_ist = ts_utc
    df["ts_utc"] = ts_utc; df["ts_ist"] = ts_ist; df.drop(columns=["epoch"], inplace=True)
    for k, v in meta.items(): df[k] = v
    df["year"]  = df["year"].astype("int16"); df["month"] = df["month"].astype("int8")
    return df

def write_partition(df: pd.DataFrame, root: Path, meta: dict, from_s: str, to_s: str):
    if df.empty:
        return []

    def _safe_time(s: str) -> str:
        # Make date/time safe for Windows filenames:
        # "YYYY-MM-DD HH:MM:SS" -> "YYYY-MM-DDT HH-MM-SS" then remove any stray chars
        s = (s or "").strip()
        s = s.replace(" ", "T").replace(":", "-").replace("/", "-")
        # keep only safe chars
        s = re.sub(r"[^A-Za-z0-9T_\-\.]", "", s)
        return s

    tf, seg, inst = meta["timeframe"], meta["segment"], meta["instrument"]
    sym = sanitize_symbol(meta.get("symbol") or str(meta["securityId"]))

    safe_from = _safe_time(from_s)
    safe_to   = _safe_time(to_s)

    written = []
    for (y, m), chunk in df.groupby(["year","month"]):
        out_dir = root / f"{tf}" / f"{seg}" / f"{inst}" / f"{sym}" / f"year={y}" / f"month={m:02d}"
        ensure_dir(out_dir)
        out_file = out_dir / f"part-{safe_from}_{safe_to}.parquet"
        chunk.sort_values("ts_utc").reset_index(drop=True).to_parquet(
            out_file, engine="pyarrow", compression="snappy", index=False
        )
        written.append(out_file)
    return written


def load_universe_nse(csv_path: str, wanted_symbols: list[str] | None = None) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    # NSE only
    df = df[df["EXCH_ID"].astype(str).str.upper() == "NSE"]
    if df.empty:
        print("[ERROR] Your CSV has 0 NSE rows. Provide a master with EXCH_ID=NSE.")
        return pd.DataFrame(columns=["securityId","exchangeSegment","instrument","symbol","expiryCode"])
    # Build display symbol
    name_cols = [c for c in ["TRADING_SYMBOL","SYMBOL_NAME","DISPLAY_NAME","UNDERLYING_SYMBOL","SYMBOL"] if c in df.columns]
    if name_cols:
        sym = df[name_cols[0]].fillna("").astype(str)
        for c in name_cols[1:]:
            sym = sym.where(sym != "", df[c].fillna("").astype(str))
    else:
        sym = df["SECURITY_ID"].astype(str)
    # Map segment
    df["exchangeSegment"] = df.apply(lambda r: map_exchange_segment(r["SEGMENT"], r.get("INSTRUMENT","")), axis=1)
    out = pd.DataFrame({
        "securityId": df["SECURITY_ID"].astype(str),
        "exchangeSegment": df["exchangeSegment"].astype(str),
        "instrument": df["INSTRUMENT"].astype(str) if "INSTRUMENT" in df.columns else df["INSTRUMENT_TYPE"].astype(str),
        "symbol": sym.astype(str),
        "expiryCode": "0",
    })
    if wanted_symbols:
        wanted = [N(x) for x in wanted_symbols if x]
        norm_sym = out["symbol"].map(N)
        mask = pd.Series(False, index=out.index)
        for w in wanted:
            mask = mask | (norm_sym == w) | norm_sym.str.contains(w, na=False)
        out = out[mask]
    return out.drop_duplicates(subset=["securityId"]).reset_index(drop=True)

def run_single(securityId: str, segment: str, instrument: str, mode: str,
               date_from: str, date_to: str, interval: int | None, oi: bool,
               symbol: str | None, expiryCode: str | int | None):
    tf = "1d" if mode == "daily" else f"{interval}m"
    # enforce NSE only
    if segment not in ("NSE_EQ","NSE_FNO","NSE_CURRENCY"):
        raise ValueError(f"[FATAL] Non-NSE segment '{segment}' not allowed in this loader.")
    meta = {
        "securityId": securityId, "symbol": symbol or str(securityId),
        "segment": segment, "instrument": instrument,
        "expiryCode": int(expiryCode) if (expiryCode not in (None,"","0","NaN")) else 0,
        "timeframe": tf,
    }
    def base_body():
        b = {"securityId": str(securityId), "exchangeSegment": segment, "instrument": instrument, "oi": bool(oi)}
        if meta["expiryCode"] > 0: b["expiryCode"] = meta["expiryCode"]
        return b

    if mode == "daily":
        body = base_body() | {"fromDate": parse_date(date_from, with_time=False), "toDate": parse_date(date_to, with_time=False)}
        payload = dh_post("/v2/charts/historical", body)
        df = to_frame(payload, meta)
        return df.shape[0], write_partition(df, HIST_DIR, meta, body["fromDate"], body["toDate"])
    else:
        max_days = 90
        f = dt.datetime.strptime(parse_date(date_from, True), "%Y-%m-%d %H:%M:%S")
        t = dt.datetime.strptime(parse_date(date_to,   True), "%Y-%m-%d %H:%M:%S")
        total_rows, all_files = 0, []
        for (a,b) in tqdm(list(_chunks(f,t,max_days)), desc=f"{meta['symbol']} {tf}"):
            body = base_body() | {
                "interval": str(interval),
                "fromDate": a.strftime("%Y-%m-%d %H:%M:%S"),
                "toDate":   b.strftime("%Y-%m-%d %H:%M:%S"),
            }
            payload = dh_post("/v2/charts/intraday", body)
            df = to_frame(payload, meta)
            written = write_partition(df, HIST_DIR, meta, body["fromDate"], body["toDate"])
            total_rows += df.shape[0]; all_files.extend(written); time.sleep(0.2)
        return total_rows, all_files

def _chunks(start: dt.datetime, end: dt.datetime, max_days: int):
    cur = start; step = dt.timedelta(days=max_days)
    while cur < end:
        nxt = min(cur+step, end); yield cur, nxt; cur = nxt

def main():
    ap = argparse.ArgumentParser(description="Teevra18 M6 Historical Loader — NSE only")
    ap.add_argument("--universe-csv", type=str, required=False)
    ap.add_argument("--symbols", type=str, required=False, help="Comma-separated names")
    ap.add_argument("--security-id", type=str, required=False)
    ap.add_argument("--segment", type=str, required=False, help="NSE_EQ or NSE_FNO (NSE only)")
    ap.add_argument("--instrument", type=str, required=False)
    ap.add_argument("--expiry-code", type=str, required=False)
    ap.add_argument("--mode", type=str, choices=["daily","intraday"], required=True)
    ap.add_argument("--interval", type=int, required=False)
    ap.add_argument("--from", dest="date_from", type=str, required=True)
    ap.add_argument("--to",   dest="date_to",   type=str, required=True)
    ap.add_argument("--oi", type=str, default="false")
    args = ap.parse_args()

    include_oi = str(args.oi).lower() in ("1","true","yes","y")
    tasks = []

    if args.security_id and args.segment and args.instrument:
        if not args.segment.startswith("NSE_"):
            print("[FATAL] This loader only accepts NSE segments (NSE_EQ / NSE_FNO)."); sys.exit(2)
        tasks.append({
            "securityId": args.security_id,
            "exchangeSegment": args.segment,
            "instrument": args.instrument,
            "expiryCode": args.expiry_code if args.expiry_code else "0",
            "symbol": None,
        })
    elif args.universe_csv:
        wanted = [s.strip() for s in args.symbols.split(",")] if args.symbols else None
        df = load_universe_nse(args.universe_csv, wanted_symbols=wanted)
        if df.empty:
            print("[ERROR] No NSE instruments matched your symbols or CSV lacks NSE."); sys.exit(3)
        for _, row in df.iterrows():
            tasks.append({
                "securityId": row["securityId"],
                "exchangeSegment": row["exchangeSegment"],
                "instrument": row["instrument"],
                "expiryCode": row.get("expiryCode","0"),
                "symbol": row.get("symbol", row["securityId"]),
            })
    else:
        print("[FATAL] Provide either (--security-id,--segment,--instrument) or --universe-csv"); sys.exit(2)

    ensure_dir(HIST_DIR)

    total_rows, total_files = 0, []
    for t in tasks:
        rows, files = run_single(
            securityId=t["securityId"], segment=t["exchangeSegment"], instrument=t["instrument"],
            mode=args.mode, date_from=args.date_from, date_to=args.date_to,
            interval=args.interval, oi=include_oi, symbol=t.get("symbol"), expiryCode=t.get("expiryCode"),
        )
        print(f"OK: {t.get('symbol', t['securityId'])} | {t['exchangeSegment']} {t['instrument']} → rows={rows} files={len(files)}")
        total_rows += rows; total_files.extend(files)

    print("\n=== SUMMARY ==="); print(f"Total rows: {total_rows}"); print(f"Files made: {len(total_files)}")
    for p in total_files[:10]: print(f"  {p}")
    if len(total_files) > 10: print(f"  ... (+{len(total_files)-10} more)")
    print("Done.")

if __name__ == "__main__":
    main()
