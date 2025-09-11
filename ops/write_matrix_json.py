import os, json, datetime

OUT = r"C:\teevra18\data\matrix\latest.json"
os.makedirs(os.path.dirname(OUT), exist_ok=True)

now = datetime.datetime.utcnow().replace(second=0, microsecond=0)
rows = []
for i in range(12):  # 60 minutes @ 5-min bars
    ts = now - datetime.timedelta(minutes=5*(11-i))
    base = 100 + i*0.25
    rows.append({
        "ts": ts.isoformat(),
        "symbol": "NIFTY",
        "open": round(base,6),
        "high": round(base+0.7,6),
        "low":  round(base-0.7,6),
        "close":round(base+0.2,6),
        "volume": 1000 + 5*i
    })

# Write as a SINGLE JSON document (array)
with open(OUT, "w", encoding="utf-8") as f:
    json.dump(rows, f, ensure_ascii=False, separators=(",",":"))

# Sanity: must parse cleanly with json.load()
with open(OUT, "r", encoding="utf-8") as f:
    _ = json.load(f)

print("[DONE] wrote JSON array:", OUT, "rows:", len(rows))
