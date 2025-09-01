import os, glob, re, sys
import pyarrow.dataset as ds
import pandas as pd

root = sys.argv[1] if len(sys.argv) > 1 else r"C:\teevra18\data\history"
rows = []
for f in glob.glob(os.path.join(root, "**", "*.parquet"), recursive=True):
    try:
        d = ds.dataset(f, format="parquet")
        t = d.to_table(columns=["ts_utc","securityId","segment","instrument","timeframe"])
        if t.num_rows == 0: 
            continue
        df = t.to_pandas()
        rows.append({
            "path": f,
            "segment": df["segment"].iloc[0],
            "instrument": df["instrument"].iloc[0],
            "securityId": df["securityId"].iloc[0],
            "timeframe": df["timeframe"].iloc[0],
            "ts_min": df["ts_utc"].min(),
            "ts_max": df["ts_utc"].max(),
            "rows": len(df)
        })
    except Exception as e:
        print(f"[SKIP] {f}: {e}")

out = pd.DataFrame(rows).sort_values(["segment","instrument","securityId","timeframe","ts_min"])
out_path = os.path.join(root, "_manifest.csv")
out.to_csv(out_path, index=False)
print(f"Wrote {out_path} with {len(out)} rows")
