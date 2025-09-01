import os, sqlite3
from pathlib import Path
import pandas as pd

DB = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

with sqlite3.connect(DB) as conn:
    # Get unlabeled OOS rows
    oos = pd.read_sql("""
      SELECT id, instrument, ts_utc
      FROM pred_oos_log
      WHERE label IS NULL
      ORDER BY id DESC
      LIMIT 5000
    """, conn)

    if oos.empty:
        print("[INFO] No unlabeled rows.")
        raise SystemExit(0)

    # Bring in base close at t0
    base = pd.read_sql("""
      SELECT instrument_id AS instrument,
             datetime(t_start,'unixepoch') AS ts_utc,
             close
      FROM candles_1m
    """, conn)
    base["ts_utc"] = pd.to_datetime(base["ts_utc"], utc=True)

    # Prepare t+3 joins
    oos["ts_utc"] = pd.to_datetime(oos["ts_utc"], utc=True)
    t0 = oos.merge(base.rename(columns={"close":"close_t0"}),
                   on=["instrument","ts_utc"], how="left")

    # Compute t+3 timestamp per row and join close_t3
    t0["ts_utc_t3"] = t0["ts_utc"] + pd.Timedelta(minutes=3)
    t3 = base.rename(columns={"close":"close_t3"})[["instrument","ts_utc","close_t3"]]
    t0 = t0.merge(t3, left_on=["instrument","ts_utc_t3"], right_on=["instrument","ts_utc"], how="left", suffixes=("","_drop"))
    if "ts_utc_drop" in t0.columns: t0 = t0.drop(columns=["ts_utc_drop"])

    # Label: up if close_t3 > close_t0
    t0["label"] = (t0["close_t3"] > t0["close_t0"]).astype("Int64")

    # Only update rows we could label
    upd = t0.dropna(subset=["label"])[["id","label"]]
    if upd.empty:
        print("[INFO] Could not label any rows (missing candles alignments).")
        raise SystemExit(0)

    conn.executemany("UPDATE pred_oos_log SET label=? WHERE id=?", [(int(r.label), int(r.id)) for r in upd.itertuples(index=False)])
    conn.commit()
    print(f"[OK] Labeled {len(upd)} rows with T+3 close rule.")
