import os, json, sqlite3, pandas as pd, numpy as np
from pathlib import Path
from datetime import datetime, timezone

DB_PATH   = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
MODELS    = Path(r"C:\teevra18\models\m11")
FEATURES  = MODELS / "latest_features.parquet"
MODEL     = MODELS / "model_m11.json"
PREDS_PAR = MODELS / "predictions_latest.parquet"

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def sigmoid(z): return 1.0 / (1.0 + np.exp(-z))

def main():
    if not FEATURES.exists():
        print(f"[FATAL] Features not found: {FEATURES}")
        return
    if not MODEL.exists():
        print(f"[FATAL] Model not found: {MODEL} (run train_m11.py)")
        return

    feats = pd.read_parquet(FEATURES)
    if feats.empty:
        print("[FATAL] Features are empty; cannot infer.")
        return

    with open(MODEL, "r", encoding="utf-8") as f:
        model = json.load(f)

    Xcols = model["x_cols"]
    mu = model["scaler"]["mean"]
    sd = model["scaler"]["std"]
    w  = model["weights"]

    for c in Xcols:
        if c not in feats.columns:
            feats[c] = 0.0

    # Standardize
    X = feats[Xcols].copy()
    for c in Xcols:
        X[c] = (X[c] - mu.get(c, 0.0)) / max(sd.get(c, 1.0), 1e-9)

    z = w["intercept"]
    for c in Xcols:
        z += w.get(c, 0.0) * X[c]
    prob_up = sigmoid(z.astype(float))

    out = pd.DataFrame({
        "ts_utc": feats["ts_utc"],
        "instrument": feats["instrument"],
        "prob_up": prob_up.clip(0.0, 1.0),
        "prob_down": (1.0 - prob_up).clip(0.0, 1.0),
        "exp_move_abs": feats.get("exp_move_abs", pd.Series(0.0)),
        "features_hash": feats.get("feat_hash", pd.Series("")),
    })

    # Save parquet
    MODELS.mkdir(parents=True, exist_ok=True)
    out.to_parquet(PREDS_PAR, index=False)
    print(f"[OK] predictions parquet: {PREDS_PAR}, rows={len(out)}")

    # Save to SQLite (new, safe table)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS predictions_m11 (
      ts_utc TEXT NOT NULL,
      instrument TEXT NOT NULL,
      prob_up REAL NOT NULL,
      prob_down REAL NOT NULL,
      exp_move_abs REAL,
      features_hash TEXT,
      created_at TEXT NOT NULL
    );
    """)
    conn.commit()

    out_sql = out.copy()
    out_sql["created_at"] = now_utc()
    out_sql.to_sql("predictions_m11", conn, if_exists="append", index=False)
    conn.close()
    print(f"[OK] predictions saved to SQLite: predictions_m11")

if __name__ == "__main__":
    main()
