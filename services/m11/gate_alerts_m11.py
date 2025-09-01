# C:\teevra18\services\m11\gate_alerts_m11.py
import os, sqlite3, yaml, pandas as pd, argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone

CFG_PATH = Path(r"C:\teevra18\config\m11.yaml")
DB_PATH  = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

def now_utc():
    return datetime.now(timezone.utc)

def load_cfg():
    try:
        with open(CFG_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--min-prob", type=float, default=None, help="Override prob threshold (e.g., 0.50 for testing)")
    p.add_argument("--max-trades", type=int, default=None, help="Override max trades per run (e.g., 5)")
    p.add_argument("--pre-minutes", type=int, default=None, help="Override pre-alert minutes (e.g., 3)")
    return p.parse_args()

def table_has_cols(conn: sqlite3.Connection, table: str, cols: list[str]) -> dict:
    info = pd.read_sql(f"PRAGMA table_info({table});", conn)
    have = set(info["name"].tolist())
    return {c: (c in have) for c in cols}

def main():
    cfg  = load_cfg()
    args = parse_args()

    thr = float(args.min_prob if args.min_prob is not None
                else cfg.get("prediction", {}).get("prob_min",
                     cfg.get("prediction", {}).get("min_probability", 0.85)))
    cap = int(args.max_trades if args.max_trades is not None
              else cfg.get("prediction", {}).get("max_trades_per_day", 5))
    pre = int(args.pre_minutes if args.pre_minutes is not None
              else cfg.get("prediction", {}).get("pre_alert_minutes",
                   cfg.get("alerts", {}).get("early_seconds", 180)//60 or 3))

    rr_min = float(cfg.get("prediction", {}).get("rr_min", 2.0))
    sl_max = float(cfg.get("prediction", {}).get("sl_per_lot_max", 1000))

    conn = sqlite3.connect(DB_PATH)

    # Optional columns in predictions table
    exists = table_has_cols(conn, "predictions_m11", ["rr_est", "sl_per_lot"])
    rr_exists = bool(exists.get("rr_est"))
    sl_exists = bool(exists.get("sl_per_lot"))

    select_cols = [
        "ts_utc",
        "instrument",
        "prob_up",
        "exp_move_abs",
        "created_at",
        ("rr_est" if rr_exists else "NULL AS rr_est"),
        ("sl_per_lot" if sl_exists else "NULL AS sl_per_lot"),
    ]
    sql = f"""
        SELECT {", ".join(select_cols)}
        FROM predictions_m11
        ORDER BY created_at DESC, ts_utc DESC
    """
    preds = pd.read_sql(sql, conn)

    if preds.empty:
        print("[WARN] No predictions to gate.")
        conn.close()
        return

    preds["ts_utc"] = pd.to_datetime(preds["ts_utc"], utc=True, errors="coerce")
    preds = preds.sort_values(["instrument", "ts_utc"]).groupby("instrument", as_index=False).tail(1)

    winners = preds[preds["prob_up"] >= thr].copy()
    if winners.empty:
        print(f"[INFO] No predictions meet threshold {thr}.")
        conn.close()
        return

    # Hard constraints only if columns truly exist
    if rr_exists:
        winners = winners[winners["rr_est"] >= rr_min]
    if sl_exists:
        winners = winners[winners["sl_per_lot"] <= sl_max]

    if winners.empty:
        msg = "[INFO] No predictions passed hard constraints"
        details = []
        if rr_exists: details.append(f"rr_min={rr_min}")
        if sl_exists: details.append(f"sl_max={sl_max}")
        if details: msg += f" ({', '.join(details)})."
        print(msg)
        conn.close()
        return

    # Respect daily cap across the entire day (UTC)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM signals_m11 WHERE date(created_at)=date('now');")
    already_today = int(cur.fetchone()[0] or 0)
    remaining = max(0, cap - already_today)
    if remaining <= 0:
        print(f"[INFO] Daily cap reached ({cap}); no new signals today.")
        conn.close()
        return

    winners = winners.sort_values("prob_up", ascending=False).head(remaining).copy()

    # Prepare output
    now = now_utc()
    winners["pre_alert_at"] = (now + timedelta(minutes=pre)).strftime("%Y-%m-%d %H:%M:%S")
    winners["created_at"]   = now.strftime("%Y-%m-%d %H:%M:%S")
    winners["decision"]     = (winners["prob_up"] >= thr).astype(int)

    # Ensure signals table (migration-safe)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS signals_m11 (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at   TEXT NOT NULL,
      instrument   TEXT NOT NULL,
      ts_utc       TEXT NOT NULL,
      prob_up      REAL NOT NULL,
      exp_move_abs REAL,
      rr_est       REAL,
      sl_per_lot   REAL,
      pre_alert_at TEXT NOT NULL,
      decision     INTEGER NOT NULL,
      status       TEXT NOT NULL
    );
    """)
    conn.commit()

    # Manual UPSERT to honor unique index (instrument, ts_utc)
    rows = winners[[
        "created_at","instrument","ts_utc","prob_up","exp_move_abs",
        "rr_est","sl_per_lot","pre_alert_at","decision"
    ]].copy()
    rows["status"] = "PENDING"

    payload = [
        (
            str(r.created_at),
            str(r.instrument),
            str(r.ts_utc),
            float(r.prob_up),
            None if pd.isna(r.exp_move_abs) else float(r.exp_move_abs),
            None if ("rr_est" not in rows.columns or pd.isna(r.rr_est)) else float(r.rr_est),
            None if ("sl_per_lot" not in rows.columns or pd.isna(r.sl_per_lot)) else float(r.sl_per_lot),
            str(r.pre_alert_at),
            int(r.decision),
            "PENDING",
        )
        for r in rows.itertuples(index=False)
    ]

    sql_upsert = """
    INSERT INTO signals_m11
    (created_at,instrument,ts_utc,prob_up,exp_move_abs,rr_est,sl_per_lot,pre_alert_at,decision,status)
    VALUES (?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(instrument, ts_utc) DO NOTHING;
    """
    cur.executemany(sql_upsert, payload)
    conn.commit()

    inserted = cur.rowcount if hasattr(cur, "rowcount") and cur.rowcount is not None else len(payload)
    cnote = []
    if rr_exists: cnote.append(f"rr_min={rr_min}")
    if sl_exists: cnote.append(f"sl_max={sl_max}")
    ctext = ", ".join(cnote) if cnote else "rr/sl not applied (columns absent)"

    print(f"[OK] Signals upserted: {inserted} (min_prob={thr}, daily_cap={cap}, remaining={remaining}), pre_alert=+{pre}m, {ctext}")

    conn.close()

if __name__ == "__main__":
    main()
