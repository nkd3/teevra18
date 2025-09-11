import os, datetime
import pandas as pd

PATH = r"C:\teevra18\data\matrix\latest.parquet"
os.makedirs(os.path.dirname(PATH), exist_ok=True)

now = datetime.datetime.utcnow().replace(second=0, microsecond=0)
rows = []
for i in range(12):  # last 60 minutes, 5-min bars
    ts = now - datetime.timedelta(minutes=5*(11-i))
    base = 100 + i
    rows.append({
        "ts": ts.isoformat(),
        "symbol": "NIFTY",
        "open": base,
        "high": base + 0.8,
        "low":  base - 0.8,
        "close": base + 0.3,
        "volume": 1000 + 10*i
    })

df = pd.DataFrame(rows, columns=["ts","symbol","open","high","low","close","volume"])
df.to_parquet(PATH, index=False)
print("[DONE] wrote", PATH, "rows", len(df), "columns", list(df.columns))
