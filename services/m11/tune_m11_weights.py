import json
from pathlib import Path

MODEL = Path(r"C:\teevra18\models\m11\model_m11.json")
FACTOR = 1.75  # increase to push probabilities more extreme

def main():
    with open(MODEL, "r", encoding="utf-8") as f:
        m = json.load(f)
    w = m.get("weights", {})
    for k in list(w.keys()):
        if k != "intercept":
            w[k] = float(w[k]) * FACTOR
    # small intercept lift as well
    w["intercept"] = float(w.get("intercept", 0.0)) + 0.25
    m["weights"] = w
    with open(MODEL, "w", encoding="utf-8") as f:
        json.dump(m, f, indent=2)
    print("[OK] Updated weights with factor", FACTOR, "and intercept +0.25")

if __name__ == "__main__":
    main()
