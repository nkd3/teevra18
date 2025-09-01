# C:\teevra18\services\ltp_feeder\validate_instruments_csv.py
import csv, sys, os
CSV = r"C:\teevra18\data\dhan_instruments.csv"
req = ["security_id","tradingsymbol","underlying","expiry","strike","option_type","exchange_segment","lot_size"]

if not os.path.exists(CSV):
    print(f"Missing: {CSV}"); sys.exit(1)

ok = bad = 0
with open(CSV, newline="", encoding="utf-8") as f:
    rdr = csv.DictReader((line for line in f if not line.strip().startswith("#")))
    if rdr.fieldnames is None:
        print("No header found"); sys.exit(2)
    missing_in_header = [c for c in req if c not in rdr.fieldnames]
    if missing_in_header:
        print(f"Header missing columns: {missing_in_header}")
        print(f"Header was: {rdr.fieldnames}")
        sys.exit(2)

    for i, r in enumerate(rdr, start=2):
        miss = [k for k in req if not r.get(k)]
        if miss: print(f"Line {i}: missing {miss}"); bad += 1; continue
        if r["option_type"] not in ("CE","PE"): print(f"Line {i}: bad option_type {r['option_type']}"); bad += 1; continue
        if r["exchange_segment"] != "NSE_FNO": print(f"Line {i}: bad exchange_segment {r['exchange_segment']}"); bad += 1; continue
        try: float(r["strike"])
        except: print(f"Line {i}: strike not numeric {r['strike']}"); bad += 1; continue
        ok += 1

print(f"OK rows: {ok}, Bad rows: {bad}")
sys.exit(0 if bad==0 else 2)
