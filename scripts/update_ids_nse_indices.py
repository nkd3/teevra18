import csv, json, sys
from pathlib import Path

CSV_PATH  = Path(r'C:\teevra18\data\api-scrip-master-detailed.csv')
JSON_PATH = Path(r'C:\teevra18\config\underlyings_chain.json')

def U(x): return (x or '').strip().upper()

def pick_ids_from_csv():
    # We prefer INSTRUMENT_TYPE == 'OP' with exact UNDERLYING_SYMBOL match.
    nifty_id = None
    bank_id  = None
    with CSV_PATH.open('r', encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        need = {'INSTRUMENT_TYPE','UNDERLYING_SYMBOL','UNDERLYING_SECURITY_ID'}
        if not need.issubset(set(r.fieldnames or [])):
            print('Missing required columns. Found:', r.fieldnames); sys.exit(1)
        for row in r:
            itype = U(row.get('INSTRUMENT_TYPE'))
            usym  = U(row.get('UNDERLYING_SYMBOL'))
            uid   = (row.get('UNDERLYING_SECURITY_ID') or '').strip()
            if itype == 'OP' and uid.isdigit():
                if usym == 'NIFTY' and nifty_id is None:
                    nifty_id = int(uid)
                if usym == 'BANKNIFTY' and bank_id is None:
                    bank_id = int(uid)
            if nifty_id and bank_id:
                break
    return nifty_id, bank_id

def update_json(nifty_id, bank_id):
    data = json.loads(JSON_PATH.read_text(encoding='utf-8-sig'))
    for it in data.get('groups',{}).get('indices',[]):
        u = it.get('underlying')
        if u == 'NIFTY' and nifty_id:
            it['underlying_scrip'] = nifty_id
            it['underlying_seg']   = 'IDX_I'
        if u == 'BANKNIFTY' and bank_id:
            it['underlying_scrip'] = bank_id
            it['underlying_seg']   = 'IDX_I'
    JSON_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'Updated JSON with IDs: NIFTY={nifty_id} BANKNIFTY={bank_id}')

if __name__ == '__main__':
    n, b = pick_ids_from_csv()
    print('Detected from CSV -> NIFTY:', n, ' BANKNIFTY:', b)
    if not n or not b:
        print('Could not find both IDs via INSTRUMENT_TYPE=OP + exact UNDERLYING_SYMBOL.'); sys.exit(1)
    update_json(n, b)
