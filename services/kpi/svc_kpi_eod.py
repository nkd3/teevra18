# C:\teevra18\services\kpi\svc_kpi_eod.py
# M10 — KPI + EOD: compute KPIs, send Telegram summary, archive day’s data.

from common.bootstrap import init_runtime
init_runtime()

import os, json, sqlite3, zipfile, io, argparse, datetime as dt
from dataclasses import dataclass
from zoneinfo import ZoneInfo
import pandas as pd
import requests
from dotenv import load_dotenv

# Keep logs clean + safe on Windows consoles
import warnings, sys
warnings.filterwarnings("ignore", category=FutureWarning)
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

@dataclass
class Env:
    DB_PATH: str
    DATA_DIR: str
    TZ: str
    TELEGRAM_BOT_TOKEN: str | None
    TELEGRAM_CHAT_ID: str | None

def list_columns(conn, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return {row[1] for row in cur.fetchall()}

def pick_time_col(conn, table: str, candidates: list[str], fallback: str | None = None) -> str:
    cols = list_columns(conn, table)
    for c in candidates:
        if c in cols:
            return c
    if fallback and fallback in cols:
        return fallback
    return ""  # no match found

def load_env() -> Env:
    load_dotenv(os.path.join(os.getcwd(), ".env"))
    return Env(
        DB_PATH=os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"),
        DATA_DIR=os.getenv("DATA_DIR", r"C:\teevra18\data"),
        TZ=os.getenv("TZ", "Asia/Kolkata"),
        TELEGRAM_BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN"),
        TELEGRAM_CHAT_ID=os.getenv("TELEGRAM_CHAT_ID"),
    )

def day_bounds_utc(trade_date_str: str, tz_name: str):
    tz = ZoneInfo(tz_name)
    d = dt.date.fromisoformat(trade_date_str)
    start_local = dt.datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=tz)
    end_local = start_local + dt.timedelta(days=1)
    return start_local.astimezone(ZoneInfo("UTC")), end_local.astimezone(ZoneInfo("UTC"))

def read_df(conn, sql: str, params: tuple = ()):
    return pd.read_sql_query(sql, conn, params=params)

def try_coalesce_columns(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def compute_pnl_if_needed(po: pd.DataFrame) -> pd.Series:
    if "pnl" in po.columns:
        return po["pnl"].astype(float)
    entry = try_coalesce_columns(po, ["entry_price", "entry", "price_entry"])
    exitp = try_coalesce_columns(po, ["exit_price", "exit", "price_exit"])
    qty = try_coalesce_columns(po, ["qty", "quantity", "lots"])
    side = try_coalesce_columns(po, ["side", "direction"])
    if not all([entry, exitp, qty, side]):
        return pd.Series([0.0]*len(po), index=po.index, dtype=float)
    e, x, q = po[entry].astype(float), po[exitp].astype(float), po[qty].astype(float)
    s = po[side].astype(str).str.upper()
    pnl = (x - e) * q
    return pnl.where(s == "BUY", (e - x) * q)

def compute_rr_if_available(po: pd.DataFrame) -> pd.Series | None:
    rr_col = try_coalesce_columns(po, ["rr_actual", "rr", "risk_reward"])
    if rr_col:
        return pd.to_numeric(po[rr_col], errors="coerce")
    risk_col = try_coalesce_columns(po, ["risk_amt", "risk", "sl_amount"])
    if risk_col and "pnl" in po.columns:
        risk = pd.to_numeric(po[risk_col], errors="coerce").replace(0, pd.NA)
        return pd.to_numeric(po["pnl"], errors="coerce") / risk
    return None

def max_drawdown(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    roll_max = series.cummax()
    return float((series - roll_max).min())

def avg_trade_duration_sec(po: pd.DataFrame) -> float:
    start_col = try_coalesce_columns(po, ["entry_ts_utc", "ts_entry_utc", "entry_ts"])
    end_col = try_coalesce_columns(po, ["exit_ts_utc", "ts_exit_utc", "exit_ts"])
    if not start_col or not end_col:
        return 0.0
    try:
        s, e = pd.to_datetime(po[start_col], utc=True), pd.to_datetime(po[end_col], utc=True)
        return float((e - s).dt.total_seconds().dropna().mean()) if len(e) else 0.0
    except Exception:
        return 0.0

def ensure_kpi_table(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS kpi_daily (
      trade_date TEXT NOT NULL,
      group_name TEXT NOT NULL,
      strategy_id TEXT NOT NULL,
      trades_total INTEGER,
      wins INTEGER,
      losses INTEGER,
      win_rate REAL,
      avg_rr REAL,
      gross_pnl REAL,
      fees REAL,
      net_pnl REAL,
      max_drawdown REAL,
      avg_trade_duration_sec REAL,
      kpi_json TEXT,
      created_at_utc TEXT NOT NULL,
      PRIMARY KEY (trade_date, group_name, strategy_id)
    );""")

def upsert_kpi(conn, row: dict):
    cols = list(row.keys())
    placeholders = ",".join("?" for _ in cols)
    update_clause = ",".join([f"{c}=excluded.{c}" for c in cols if c not in ("trade_date","group_name","strategy_id")])
    sql = f"""
    INSERT INTO kpi_daily ({",".join(cols)})
    VALUES ({placeholders})
    ON CONFLICT(trade_date, group_name, strategy_id) DO UPDATE SET
      {update_clause};
    """
    conn.execute(sql, tuple(row[c] for c in cols))

def make_telegram_text(trade_date: str, overall: dict, per_group: list[dict], top_strats: list[dict]) -> str:
    fmt_pct = lambda x: f"{x*100:.1f}%"
    fmt_money = lambda x: f"₹{x:,.0f}"
    lines = [f"📊 *Teevra18 — EOD* ({trade_date})"]
    lines.append(f"• Trades: {overall.get('trades_total',0)} | WinRate: {fmt_pct(overall.get('win_rate',0.0))} | Net: {fmt_money(overall.get('net_pnl',0.0))}")
    if per_group:
        lines.append("• By Group:")
        for g in per_group:
            lines.append(f"    - {g['group_name']}: {g['trades_total']} trades, {fmt_pct(g['win_rate'])}, Net {fmt_money(g['net_pnl'])}")
    if top_strats:
        lines.append("• Top Strategies:")
        for s in top_strats[:3]:
            lines.append(f"    - {s['strategy_id']}: {s['trades_total']} trades, Net {fmt_money(s['net_pnl'])}")
    lines.append("— End of day. Archive saved.")
    return "\n".join(lines)

def send_telegram(env: Env, text: str) -> bool:
    if not env.TELEGRAM_BOT_TOKEN or not env.TELEGRAM_CHAT_ID:
        print("[WARN] Telegram not configured; skipping send."); return False
    url = f"https://api.telegram.org/bot{env.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": env.TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        ok = (r.status_code == 200) and r.json().get("ok") is True
        print(f"[INFO] Telegram send status: {ok}, http={r.status_code}")
        if not ok: print(f"[DEBUG] Telegram response: {r.text}")
        return ok
    except Exception as e:
        print(f"[ERROR] Telegram send failed: {e}"); return False

def archive_day(env: Env, trade_date: str, dfs: dict):
    arch_dir = os.path.join(env.DATA_DIR, "archive")
    os.makedirs(arch_dir, exist_ok=True)
    zip_path = os.path.join(arch_dir, f"{trade_date}_eod.zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, df in dfs.items():
            buf = io.StringIO(); df.to_csv(buf, index=False)
            zf.writestr(f"{trade_date}/{name}.csv", buf.getvalue())
    print(f"[INFO] Archive written: {zip_path}")
    return zip_path

def main():
    parser = argparse.ArgumentParser(description="M10 — KPI + EOD")
    parser.add_argument("--date", help="Trade date (YYYY-MM-DD)", default=None)
    parser.add_argument("--tz", help="Trading timezone", default=None)
    parser.add_argument("--send", action="store_true", help="Send Telegram EOD")
    parser.add_argument("--archive", action="store_true", help="Zip CSVs into data/archive")
    parser.add_argument("--dry-run", action="store_true", help="Compute but do not write KPIs")
    args = parser.parse_args()

    env = load_env()
    tz_name = args.tz or env.TZ
    trade_date = args.date or dt.datetime.now(ZoneInfo(tz_name)).date().isoformat()
    start_utc, end_utc = day_bounds_utc(trade_date, tz_name)
    print(f"[INFO] Trade date: {trade_date} | Window UTC: {start_utc} -> {end_utc}")

    conn = sqlite3.connect(env.DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    ensure_kpi_table(conn)

    # Auto-detect time cols
    sig_time = pick_time_col(conn, "signals", ["ts_utc","created_at_utc","entry_ts_utc","exit_ts_utc"])
    po_time  = pick_time_col(conn, "paper_orders", ["ts_utc","exit_ts_utc","created_at_utc","entry_ts_utc"])

    sig = read_df(conn, f"SELECT * FROM signals{' WHERE '+sig_time+' >= ? AND '+sig_time+' < ?' if sig_time else ''}",
                  (start_utc.isoformat(" "), end_utc.isoformat(" ")) if sig_time else ())
    po = read_df(conn, f"SELECT * FROM paper_orders{' WHERE '+po_time+' >= ? AND '+po_time+' < ?' if po_time else ''}",
                 (start_utc.isoformat(" "), end_utc.isoformat(" ")) if po_time else ())

    if po.empty and sig.empty:
        print("[WARN] No signals or paper orders for this day; nothing to report.")
        return

    if "group_name" not in po.columns:
        po["group_name"] = "DEFAULT"
    if "strategy_id" not in po.columns:
        po["strategy_id"] = "UNKNOWN"
    po["pnl"] = compute_pnl_if_needed(po)
    po["rr_use"] = compute_rr_if_available(po)

    po["is_win"], po["is_loss"] = po["pnl"] > 0, po["pnl"] <= 0
    avg_dur = avg_trade_duration_sec(po)
    sort_key = try_coalesce_columns(po, ["exit_ts_utc","ts_utc"]) or po.columns[0]
    cum = po.sort_values(by=sort_key)["pnl"].cumsum()
    mdd = max_drawdown(cum)

    def agg(df):
        return pd.Series({
            "trades_total": int(len(df)),
            "wins": int(df["is_win"].sum()),
            "losses": int(df["is_loss"].sum()),
            "win_rate": float(df["is_win"].mean()) if len(df) else 0.0,
            "avg_rr": float(pd.to_numeric(df["rr_use"], errors="coerce").mean(skipna=True)) if "rr_use" in df else None,
            "gross_pnl": float(df["pnl"].sum()),
            "fees": float(pd.to_numeric(df[try_coalesce_columns(df, ['fees','brokerage'])], errors='coerce').sum()) if try_coalesce_columns(df, ['fees','brokerage']) else 0.0,
        })

    by_group = po.groupby("group_name", group_keys=False).apply(agg).reset_index()
    by_strat = po.groupby("strategy_id", group_keys=False).apply(agg).reset_index()

    overall = agg(po).to_dict() | {
        "max_drawdown": mdd,
        "avg_trade_duration_sec": avg_dur,
        "group_name": "ALL",
        "strategy_id": "ALL"
    }

    now_utc = dt.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC")).isoformat()
    rows = []
    for _, r in by_group.iterrows():
        rows.append({
            **r.to_dict(),
            "trade_date": trade_date,
            "strategy_id": "ALL",
            "net_pnl": r["gross_pnl"] - r["fees"],
            "max_drawdown": None,
            "avg_trade_duration_sec": None,
            "kpi_json": None,
            "created_at_utc": now_utc
        })
    for _, r in by_strat.iterrows():
        rows.append({
            **r.to_dict(),
            "trade_date": trade_date,
            "group_name": "ALL",
            "net_pnl": r["gross_pnl"] - r["fees"],
            "max_drawdown": None,
            "avg_trade_duration_sec": None,
            "kpi_json": None,
            "created_at_utc": now_utc
        })
    rows.append({
        **overall,
        "trade_date": trade_date,
        "net_pnl": overall["gross_pnl"] - overall["fees"],
        "kpi_json": json.dumps({"from_utc": start_utc.isoformat(), "to_utc": end_utc.isoformat()}),
        "created_at_utc": now_utc
    })

    if not args.dry_run:
        for row in rows:
            upsert_kpi(conn, row)
        conn.commit()
        print(f"[INFO] Upserted KPI rows: {len(rows)}")
    else:
        print(f"[DRY] Would upsert KPI rows: {len(rows)}")

    # Build Telegram message (full Unicode) and print preview
    tg_text = make_telegram_text(
        trade_date=trade_date,
        overall=rows[-1],
        per_group=by_group.assign(net_pnl=by_group["gross_pnl"] - by_group["fees"]).to_dict("records"),
        top_strats=by_strat.assign(net_pnl=by_strat["gross_pnl"] - by_strat["fees"]).sort_values("net_pnl", ascending=False).to_dict("records"),
    )
    print("------ Telegram Preview ------")
    print(tg_text)  # wrapper sets UTF-8, so this is safe
    print("------------------------------")
    if args.send:
        send_telegram(env, tg_text)

    if args.archive:
        archive_day(env, trade_date, {
            "signals": sig,
            "paper_orders": po,
            "kpi_daily": pd.DataFrame(rows)
        })

    print("[DONE] M10 KPI+EOD complete.")

if __name__ == "__main__":
    main()
