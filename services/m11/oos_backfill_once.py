import os, sqlite3
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

DB = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

with sqlite3.connect(DB) as conn:
    # ensure pred_id exists
    cols = {r[1] for r in conn.execute("PRAGMA table_info(pred_oos_log);")}
    if "pred_id" not in cols:
        conn.execute("ALTER TABLE pred_oos_log ADD COLUMN pred_id INTEGER;")
        conn.commit()

    # prefer real primary key if present, else rowid
    has_id = any(r[1] == "id" for r in conn.execute("PRAGMA table_info(predictions_m11);"))
    pred_id_expr = "p.id" if has_id else "p.rowid"

    sql = f"""
      SELECT
        s.id AS signal_id,
        s.instrument,
        s.ts_utc,
        s.prob_up,
        s.exp_move_abs,
        s.rr_est,
        s.sl_per_lot,
        {pred_id_expr} AS pred_id
      FROM signals_m11 s
      LEFT JOIN pred_oos_log o ON o.signal_id = s.id
      INNER JOIN predictions_m11 p
        ON p.instrument = s.instrument
       AND p.ts_utc     = s.ts_utc
      WHERE s.status='ALERTED'
        AND o.signal_id IS NULL
      ORDER BY s.id DESC
      LIMIT 2000;
    """
    df = pd.read_sql(sql, conn)
    if df.empty:
        print("[INFO] Nothing to backfill.")
    else:
        rows = []
        for r in df.itertuples(index=False):
            rows.append((
                NOW, str(r.instrument), str(r.ts_utc),
                float(r.prob_up),
                None if pd.isna(r.exp_move_abs) else float(r.exp_move_abs),
                None if pd.isna(r.rr_est) else float(r.rr_est),
                None if pd.isna(r.sl_per_lot) else float(r.sl_per_lot),
                int(r.signal_id), None, None, "backfill", int(r.pred_id)
            ))
        conn.executemany("""
          INSERT INTO pred_oos_log
          (created_at,instrument,ts_utc,prob_up,exp_move_abs,rr_est,sl_per_lot,signal_id,label,realized_at,notes,pred_id)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        conn.commit()
        print(f"[OK] Backfilled {len(rows)} rows into pred_oos_log.")
