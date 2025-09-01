import csv, json, sys
from pathlib import Path

CSV_PATH  = Path(r'C:\teevra18\data\api-scrip-master-detailed.csv')
JSON_PATH = Path(r'C:\teevra18\config\underlyings_chain.json')

def U(x): return (x or '').strip().upper()

def find_index_security_ids():
    nifty = None
    bank  = None
    with CSV_PATH.open('r', encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        need = {'INSTRUMENT_TYPE','SYMBOL_NAME','DISPLAY_NAME','SECURITY_ID'}
        if not need.issubset(set(r.fieldnames or [])):
            print('Missing required columns. Found:', r.fieldnames); sys.exit(1)
        for row in r:
            itype = U(row.get('INSTRUMENT_TYPE'))
            sym   = U(row.get('SYMBOL_NAME'))
            disp  = U(row.get('DISPLAY_NAME'))
            secid = (row.get('SECURITY_ID') or '').strip()
            if itype == 'INDEX' and secid.isdigit():
                if sym == 'NIFTY' or disp == 'NIFTY 50' or 'NIFTY 50' in disp:
                    nifty = int(secid)
                if sym == 'BANKNIFTY' or disp == 'NIFTY BANK' or 'NIFTY BANK' in disp:
                    bank = int(secid)
            if nifty and bank:
                break
    return nifty, bank

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
    print(f'Updated JSON with INDEX SecurityIds: NIFTY={nifty_id} BANKNIFTY={bank_id}')

if __name__ == '__main__':
    n, b = find_index_security_ids()
    print('Detected from INDEX rows -> NIFTY:', n, ' BANKNIFTY:', b)
    if not n or not b:
        print('Error: Could not find both INDEX SecurityIds.'); sys.exit(1)
    update_json(n, b)
