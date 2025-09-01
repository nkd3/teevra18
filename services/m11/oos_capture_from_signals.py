# C:\teevra18\services\m11\oos_capture_from_signals.py
import os, sqlite3
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

DB = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def predictions_has_id(conn):
    return any(r[1] == "id" for r in conn.execute("PRAGMA table_info(predictions_m11);"))

with sqlite3.connect(DB) as conn:
    # prefer real primary key if present; otherwise rowid
    use_id = predictions_has_id(conn)
    pred_id_expr = "p.id" if use_id else "p.rowid"

    # Only take ALERTED signals that:
    #  - are NOT yet in pred_oos_log, and
    #  - HAVE a matching prediction row (INNER JOIN) => pred_id guaranteed
    sql = f"""
      SELECT
        s.id AS signal_id,
        s.created_at,
        s.instrument,
        s.ts_utc,
        s.prob_up,
        s.exp_move_abs,
        s.rr_est,
        s.sl_per_lot,
        {pred_id_expr} AS pred_id
      FROM signals_m11 s
      LEFT JOIN pred_oos_log o
        ON o.signal_id = s.id
      INNER JOIN predictions_m11 p
        ON p.instrument = s.instrument
       AND p.ts_utc     = s.ts_utc
      WHERE s.status = 'ALERTED'
        AND o.signal_id IS NULL
      ORDER BY s.id DESC
      LIMIT 1000;
    """

    df = pd.read_sql(sql, conn)
    if df.empty:
        print("[INFO] No new ALERTED signals with matching predictions to capture.")
        raise SystemExit(0)

    # Build rows (pred_id is guaranteed by INNER JOIN)
    rows = []
    for r in df.itertuples(index=False):
        rows.append((
            NOW,                        # created_at
            str(r.instrument),          # instrument
            str(r.ts_utc),              # ts_utc
            float(r.prob_up),           # prob_up
            None if pd.isna(r.exp_move_abs) else float(r.exp_move_abs),
            None if pd.isna(r.rr_est) else float(r.rr_est),
            None if pd.isna(r.sl_per_lot) else float(r.sl_per_lot),
            int(r.signal_id),           # signal_id
            None,                       # label (to be filled later)
            None,                       # realized_at
            "auto-captured from signals_m11",  # notes
            int(r.pred_id)              # pred_id (NOT NULL)
        ))

    conn.executemany("""
      INSERT INTO pred_oos_log
      (created_at,instrument,ts_utc,prob_up,exp_move_abs,rr_est,sl_per_lot,signal_id,label,realized_at,notes,pred_id)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    print(f"[OK] Captured {len(rows)} rows to pred_oos_log. (pred_id from {'id' if use_id else 'rowid'})")
