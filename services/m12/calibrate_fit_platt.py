# C:\teevra18\services\m12\calibrate_fit_platt.py
import os, sqlite3, json
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timezone

DB = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
MODELS = Path(r"C:\teevra18\models\m11")
MODELS.mkdir(parents=True, exist_ok=True)

with sqlite3.connect(DB) as conn:
    df = pd.read_sql("""
      SELECT prob_up, label
      FROM pred_oos_log
      WHERE label IS NOT NULL
      ORDER BY id DESC
      LIMIT 5000
    """, conn)

if df.empty or df["label"].nunique() < 2:
    print("[WARN] Not enough labeled OOS to calibrate.")
    raise SystemExit(0)

# Simple Platt scaling (logistic fit)
p = np.clip(df["prob_up"].values, 1e-6, 1-1e-6)
y = df["label"].values.astype(float)

# Fit via Newton steps
A, B = 0.0, 0.0
for _ in range(25):
    f = 1.0 / (1.0 + np.exp(A * p + B))
    W = f * (1 - f)
    z = (y - f) / (W + 1e-9)
    X1 = np.sum(W * p * p)
    X2 = np.sum(W * p)
    X3 = np.sum(W)
    Y1 = np.sum(W * p * (z + (A*p + B)))
    Y2 = np.sum(W * (z + (A*p + B)))
    det = X1*X3 - X2*X2 + 1e-9
    dA = (Y1*X3 - Y2*X2) / det
    dB = (X1*Y2 - X2*Y1) / det
    A -= dA
    B -= dB
    if abs(dA)+abs(dB) < 1e-6:
        break

cal = {
  "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
  "type": "platt",
  "A": float(A),
  "B": float(B)
}
(Path(MODELS) / "calibration_m11.json").write_text(json.dumps(cal, indent=2))
print("[OK] Wrote calibration:", cal)

# Optionally bake into model_m11.json if present
mj = Path(MODELS) / "model_m11.json"
if mj.exists():
    m = json.loads(mj.read_text())
    m["calibration"] = cal
    mj.write_text(json.dumps(m, indent=2))
    print("[OK] Injected calibration into model_m11.json")
