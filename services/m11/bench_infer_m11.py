import time, json
from pathlib import Path
import pandas as pd
import numpy as np

MODELS = Path(r"C:\teevra18\models\m11")
MODEL  = MODELS / "model_m11.json"
FEATS  = MODELS / "latest_features.parquet"

def sigmoid(x): return 1.0/(1.0+np.exp(-x))
def logit(p):
    p = np.clip(p, 1e-6, 1-1e-6)
    return np.log(p/(1-p))

def main():
    m = json.loads(MODEL.read_text(encoding="utf-8"))
    w = m.get("weights", {})
    cal = m.get("calibration", {})

    df = pd.read_parquet(FEATS)
    if df.empty:
        print("[FATAL] No features.")
        return

    # assume features=*all* numeric cols except ['ts_utc','instrument']
    numeric = df.select_dtypes(include="number").columns.tolist()
    X = df[numeric].fillna(0.0).to_numpy()

    # weights vector aligned by column name
    beta = np.array([float(w.get(col, 0.0)) for col in numeric], dtype=float)
    intercept = float(w.get("intercept", 0.0))

    t0 = time.perf_counter()
    z = intercept + X.dot(beta)
    p_raw = sigmoid(z)

    if cal.get("type") == "platt":
        a = float(cal.get("a", 0.0)); b = float(cal.get("b", 1.0))
        zc = a + b * logit(p_raw)
        p = sigmoid(zc)
    else:
        p = p_raw
    t1 = time.perf_counter()

    dt_ms = (t1 - t0)*1000
    print(f"[OK] Inference time for {len(p)} rows: {dt_ms:.2f} ms")
    print(f"[OK] Mean prob: {p.mean():.3f}, Max prob: {p.max():.3f}")

if __name__ == "__main__":
    main()
