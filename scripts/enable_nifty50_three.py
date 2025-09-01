import csv, json, sys
from pathlib import Path
from collections import Counter

CSV_PATH  = Path(r'C:\teevra18\data\api-scrip-master-detailed.csv')
JSON_PATH = Path(r'C:\teevra18\config\underlyings_chain.json')

SYMS = ['RELIANCE','HDFCBANK','INFY']   # add more later if you like

def U(s): return (s or '').strip().upper()

def infer_ids():
    out = {}
    with CSV_PATH.open('r', encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        need = {'INSTRUMENT_TYPE','UNDERLYING_SYMBOL','UNDERLYING_SECURITY_ID'}
        if not need.issubset(set(r.fieldnames or [])):
            print('CSV missing columns:', r.fieldnames); sys.exit(2)
        bins = {s: Counter() for s in SYMS}
        for row in r:
            if U(row.get('INSTRUMENT_TYPE')) != 'OPTSTK':
                continue
            sym = U(row.get('UNDERLYING_SYMBOL'))
            uid = (row.get('UNDERLYING_SECURITY_ID') or '').strip()
            if not sym or not uid.isdigit():
                continue
            if sym in bins:
                bins[sym][int(uid)] += 1
        for s in SYMS:
            out[s] = bins[s].most_common(1)[0][0] if bins[s] else None
    return out

def update_json(ids):
    data = json.loads(JSON_PATH.read_text(encoding='utf-8-sig'))
    groups = data.setdefault('groups', {})
    lst = groups.setdefault('nifty50', [])
    # map by underlying for update/insert
    idx = {U(it.get('underlying')): it for it in lst}
    changed = []
    for sym, uid in ids.items():
        if not uid:
            print(f'WARN: No ID found for {sym}'); 
            continue
        row = idx.get(sym)
        if not row:
            row = {'underlying': sym}
            lst.append(row)
        row['underlying_scrip'] = int(uid)
        row['underlying_seg'] = 'EQ_I'
        row['enabled'] = True
        changed.append((sym, uid))
    JSON_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    return changed

if __name__ == '__main__':
    ids = infer_ids()
    print('Detected IDs:', ids)
    changed = update_json(ids)
    print('Enabled/updated:', changed)
