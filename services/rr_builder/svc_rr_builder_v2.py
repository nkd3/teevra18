from common.bootstrap import init_runtime
init_runtime()
import argparse, sqlite3, json
from rr_rules_v2 import validate_signal_row

def fetch_candidate_signals(conn, limit):
    sql = """
      SELECT rowid, signal_id, option_symbol, underlying_root, side, entry_price, sl_points, tp_points, lots
      FROM signals
      WHERE state='PENDING' AND rr_validated IS NULL
      ORDER BY ts_utc ASC
      LIMIT ?
    """
    rows = conn.execute(sql, (limit,)).fetchall()
    out = []
    for r in rows:
        out.append({
            "rowid": r[0], "signal_id": r[1], "option_symbol": r[2],
            "underlying_root": r[3], "side": r[4], "entry_price": r[5],
            "sl_points": r[6], "tp_points": r[7], "lots": r[8]
        })
    return out

def mark_result(conn, rowid, ok, reason, metrics):
    if ok:
        conn.execute("""
          UPDATE signals
          SET rr_validated=1, rr_reject_reason=NULL, rr_metrics_json=?
          WHERE rowid=?
        """, (json.dumps(metrics), rowid))
    else:
        conn.execute("""
          UPDATE signals
          SET rr_validated=0, rr_reject_reason=?, state='ARCHIVED', rr_metrics_json=?
          WHERE rowid=?
        """, (reason, json.dumps(metrics), rowid))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=r"C:\teevra18\data\teevra18.db")
    ap.add_argument("--profile", default="BASELINE_V2")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    cands = fetch_candidate_signals(conn, args.limit)
    if not cands:
        print("[M8] No pending signals.")
        return

    accepted = 0
    for s in cands:
        ok, reason, metrics = validate_signal_row(conn, s, profile_name=args.profile)
        tag = "ACCEPT" if ok else f"REJECT({reason})"
        print(f"[{tag}] {s['signal_id']} {s['option_symbol']} {s['side']} lots={s['lots']} "
              f"entry={s['entry_price']} sl={s['sl_points']} tp={s['tp_points']} "
              f"rr_eff={metrics['rr_eff']:.2f} risk₹={metrics['effective_risk']:.0f}")
        if not args.dry_run:
            mark_result(conn, s["rowid"], ok, reason, metrics)
            conn.commit()
        if ok: accepted += 1

    print(f"[M8] Done. Accepted={accepted}, Reviewed={len(cands)}.")

if __name__ == "__main__":
    main()
