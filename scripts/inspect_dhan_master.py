# C:\teevra18\scripts\inspect_dhan_master.py
import csv
from collections import Counter
from pathlib import Path

CSV_PATH = Path(r"C:\teevra18\data\api-scrip-master-detailed.csv")

def safe_open(path):
    return path.open("r", encoding="utf-8-sig", newline="")

with safe_open(CSV_PATH) as f:
    r = csv.DictReader(f)
    cols = r.fieldnames or []
    print("Columns:", cols)

    seg_counter = Counter()
    itype_counter = Counter()
    rows = list(r)

    for row in rows[:50000]:  # cap scan to 50k for speed
        seg_counter.update([row.get("SEGMENT","").strip()])
        itype_counter.update([row.get("INSTRUMENT_TYPE","").strip()])

    print("\nTop SEGMENT values:")
    for seg, c in seg_counter.most_common(15):
        print(f"  {seg!r}: {c}")

    print("\nTop INSTRUMENT_TYPE values:")
    for t, c in itype_counter.most_common(15):
        print(f"  {t!r}: {c}")

    # Show some NIFTY/BANKNIFTY samples
    def hit(s): return s and ("NIFTY" in s.upper())

    print("\nSample rows mentioning NIFTY (showing a few):")
    shown = 0
    for row in rows:
        if hit(row.get("UNDERLYING_SYMBOL")) or hit(row.get("SYMBOL_NAME")) or hit(row.get("DISPLAY_NAME")):
            print({
                "SEGMENT": row.get("SEGMENT"),
                "UNDERLYING_SYMBOL": row.get("UNDERLYING_SYMBOL"),
                "UNDERLYING_SECURITY_ID": row.get("UNDERLYING_SECURITY_ID"),
                "SYMBOL_NAME": row.get("SYMBOL_NAME"),
                "DISPLAY_NAME": row.get("DISPLAY_NAME"),
                "INSTRUMENT_TYPE": row.get("INSTRUMENT_TYPE"),
                "SECURITY_ID": row.get("SECURITY_ID"),
            })
            shown += 1
            if shown >= 10:
                break
