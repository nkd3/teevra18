import csv, json, sys
from collections import Counter, defaultdict
from pathlib import Path

CSV_PATH  = Path(r'C:\teevra18\data\api-scrip-master-detailed.csv')
JSON_PATH = Path(r'C:\teevra18\config\underlyings_chain.json')

IDX_TYPES = {'OPTIDX','FUTIDX'}  # search both
EXCLUDE_NIFTY_FAM = {'FINNIFTY','FIN','MID','SMALL','IT','AUTO','FMCG','MEDIA','PHAR','PHARMA','METAL','REALTY','CPSE','ENERGY'}

def U(x): return (x or '').strip().upper()
def is_int(s): 
    s = (s or '').strip()
    return s.isdigit()

def label_for(textU):
    # BANKNIFTY exact or clear variants
    if ('BANK' in textU and 'NIFTY' in textU): return 'BANKNIFTY'
    # NIFTY core (exclude family variants)
    if 'NIFTY' in textU and not any(bad in textU for bad in EXCLUDE_NIFTY_FAM):
        return 'NIFTY'
    return None

def find_ids():
    with CSV_PATH.open('r', encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        need = {'INSTRUMENT_TYPE','UNDERLYING_SYMBOL','UNDERLYING_SECURITY_ID','DISPLAY_NAME','SYMBOL_NAME'}
        if not need.issubset(set(r.fieldnames or [])):
            print('Missing required columns. Found:', r.fieldnames); sys.exit(1)

        bins = {'NIFTY': Counter(), 'BANKNIFTY': Counter()}
        for row in r:
            itype = U(row.get('INSTRUMENT_TYPE'))
            if itype not in IDX_TYPES:
                continue

            usym = U(row.get('UNDERLYING_SYMBOL')) or U(row.get('DISPLAY_NAME')) or U(row.get('SYMBOL_NAME'))
            uid  = (row.get('UNDERLYING_SECURITY_ID') or '').strip()
            if not usym or not is_int(uid):
                continue

            lbl = label_for(usym)
            if not lbl:
                continue
            bins[lbl][int(uid)] += 1

        pick = {}
        for lbl in ('NIFTY','BANKNIFTY'):
            if bins[lbl]:
                uid, _ = bins[lbl].most_common(1)[0]
                pick[lbl] = uid
            else:
                pick[lbl] = None
        return pick

def update_json(ids):
    data = json.loads(JSON_PATH.read_text(encoding='utf-8-sig'))
    for it in data.get('groups',{}).get('indices',[]):
        u = it.get('underlying')
        if u in ids and ids[u]:
            it['underlying_scrip'] = ids[u]
            it['underlying_seg']   = 'IDX_I'
    JSON_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    print('Updated JSON:', ids)

if __name__ == '__main__':
    ids = find_ids()
    print('Detected IDs:', ids)
    if not ids.get('NIFTY') or not ids.get('BANKNIFTY'):
        print('Could not confidently determine both IDs from OPTIDX/FUTIDX.')
        sys.exit(1)
    update_json(ids)
