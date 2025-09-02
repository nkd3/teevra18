import json, sys, pathlib
M = r"C:\teevra18\models\m7\strategy_matrix.json"
L = r"C:\teevra18\lists\nifty50_demo.txt"

data = json.load(open(M, "r", encoding="utf-8"))
eqs  = [ln.strip() for ln in open(L, "r", encoding="utf-8") if ln.strip() and not ln.startswith("#")]
data["equities"] = sorted(set(eqs))
pathlib.Path(M).write_text(json.dumps(data, indent=2), encoding="utf-8")
print(f"[OK] equities updated ({len(eqs)}) in {M}")
