import os, json, datetime

PATH = r"C:\teevra18\data\matrix\latest.jsonl"
os.makedirs(os.path.dirname(PATH), exist_ok=True)

now = datetime.datetime.utcnow().replace(second=0, microsecond=0)
rows = []
for i in range(12):  # 60 minutes @ 5 min bars
    ts = now - datetime.timedelta(minutes=5*(11-i))
    base = 100 + i*0.25
    rows.append({
        "ts": ts.isoformat(),          # <-- timestamp
        "symbol": "NIFTY",             # <-- instrument
        "open": round(base,6),
        "high": round(base+0.7,6),
        "low":  round(base-0.7,6),
        "close":round(base+0.2,6),
        "volume": 1000 + 5*i
    })

with open(PATH, "w", encoding="utf-8") as f:
    for r in rows:
        f.write(json.dumps(r, separators=(",",":"))+"\n")

# quick sanity
assert os.path.getsize(PATH) > 0, "jsonl file is empty"
with open(PATH, "r", encoding="utf-8") as f:
    _ = json.loads(f.readline())

print("[DONE] wrote JSONL:", PATH, "rows:", len(rows))
