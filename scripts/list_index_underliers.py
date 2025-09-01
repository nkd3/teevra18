import csv
from collections import defaultdict, Counter
from pathlib import Path

CSV_PATH = Path(r'C:\teevra18\data\api-scrip-master-detailed.csv')
IDX_TYPES = {'OPTIDX','FUTIDX'}
EXCLUDE = {'FINNIFTY'}

def U(s): return (s or '').strip().upper()

with CSV_PATH.open('r', encoding='utf-8-sig', newline='') as f:
    r = csv.DictReader(f)
    seen = defaultdict(Counter)  # key -> UNDERLYING_SECURITY_ID -> count
    examples = {}               # key -> a sample name

    for row in r:
        itype = U(row.get('INSTRUMENT_TYPE'))
        if itype not in IDX_TYPES: 
            continue
        usym = U(row.get('UNDERLYING_SYMBOL')) or U(row.get('DISPLAY_NAME')) or U(row.get('SYMBOL_NAME'))
        uid  = (row.get('UNDERLYING_SECURITY_ID') or '').strip()
        if not usym or not uid.isdigit():
            continue
        if any(x in usym for x in EXCLUDE):
            continue
        key = usym
        seen[key][int(uid)] += 1
        examples.setdefault(key, usym)

    # Show top 40 underlier keys by total count
    totals = [(sum(cnt.values()), key) for key, cnt in seen.items()]
    for total, key in sorted(totals, reverse=True)[:40]:
        best_uid, best_count = seen[key].most_common(1)[0]
        print(f'{key:30s}  -> UnderlyingSecurityId={best_uid}  (hits={best_count}, total_rows={total})')
