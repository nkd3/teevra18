import os, sqlite3, json, pandas as pd, numpy as np
from pathlib import Path
from datetime import datetime, timezone

DB_PATH  = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
MODELS   = Path(r"C:\teevra18\models\m11")
FEATURES = MODELS / "latest_features.parquet"
MODEL    = MODELS / "model_m11.json"

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def main():
    MODELS.mkdir(parents=True, exist_ok=True)
    if not FEATURES.exists():
        print(f"[FATAL] Features parquet not found: {FEATURES}. Run build_features_m11.py first.")
        return

    feats = pd.read_parquet(FEATURES)
    if feats.empty:
        print("[FATAL] Features are empty. Adjust config (instrument_universe) and rebuild features.")
        return

    # Fill NaNs, compute scaling statistics (z-score standardization)
    for col in ["vwap_gap","ema_vs_price","l20_imbalance","exp_move_abs","vol_norm"]:
        if col not in feats.columns:
            feats[col] = 0.0
    Xcols = ["vwap_gap","ema_vs_price","l20_imbalance","exp_move_abs","vol_norm"]
    mu = feats[Xcols].mean().to_dict()
    sd = (feats[Xcols].std(ddof=0).replace(0, 1.0)).to_dict()

    # Simple weights (hand-tuned priors; adjust later after OOS tracking)
    weights = {
        "intercept": 0.0,
        "vwap_gap":  1.2,
        "ema_vs_price": 0.6,
        "l20_imbalance": 0.9,
        "exp_move_abs": 0.4,
        "vol_norm": 0.2
    }

    model = {
        "created_at": now_utc(),
        "db_path": str(DB_PATH),
        "x_cols": Xcols,
        "scaler": {"mean": mu, "std": sd},
        "weights": weights,
        "link": "logistic"  # sigmoid
    }
    with open(MODEL, "w", encoding="utf-8") as f:
        json.dump(model, f, indent=2)
    print(f"[OK] Saved model to {MODEL}")

if __name__ == "__main__":
    main()
	