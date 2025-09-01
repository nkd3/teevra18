# C:\teevra18\tools\verify_history_partitions.py
import sys
from pathlib import Path
import pandas as pd

root = Path(r"C:\teevra18\data\history")
if len(sys.argv) > 1:
    root = Path(sys.argv[1])

matches = list(root.rglob("*.parquet"))
print(f"Found {len(matches)} parquet files under: {root}")

for fp in matches[:5]:
    df = pd.read_parquet(fp)
    print("\n---", fp)
    print(df.head(3))
    print("rows:", len(df), "cols:", list(df.columns))
    # Simple checks
    if not df["ts_utc"].is_monotonic_increasing:
        print("WARN: ts_utc not sorted (file-level).")
