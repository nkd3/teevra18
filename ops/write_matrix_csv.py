import os, csv, datetime
PATH = r"C:\teevra18\data\matrix\latest.csv"
os.makedirs(os.path.dirname(PATH), exist_ok=True)

now = datetime.datetime.utcnow().replace(second=0, microsecond=0)
rows = []
for i in range(12):  # 60 minutes of 5-min bars
    ts = now - datetime.timedelta(minutes=5*(11-i))
    base = 100 + i*0.25
    rows.append({
        "ts": ts.isoformat(),
        "symbol": "NIFTY",
        "open": round(base,4),
        "high": round(base+0.7,4),
        "low":  round(base-0.7,4),
        "close":round(base+0.2,4),
        "volume": 1000 + 5*i
    })

with open(PATH, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["ts","symbol","open","high","low","close","volume"])
    w.writeheader()
    w.writerows(rows)

print("[DONE] wrote CSV:", PATH)
