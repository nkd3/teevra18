import csv, json, sys
from pathlib import Path

CSV_PATH  = Path(r'C:\teevra18\data\api-scrip-master-detailed.csv')
JSON_PATH = Path(r'C:\teevra18\config\underlyings_chain.json')

def U(x): return (x or '').strip().upper()

def is_nse(exch_id):
    v = U(exch_id)
    if not v: return False
    if v in {'NSE','NS','N'}: return True
    if 'NSE' in v: return True
    # Numeric convention: 2 = NSE (common), 1 = BSE
    try:
        n = int(v)
        if n == 2:
            return True
    except:
        pass
    return False

def score_nifty(sym, disp):
    s = 0
    if sym == 'NIFTY': s += 70
    if disp == 'NIFTY 50': s += 120
    if 'NIFTY 50' in disp: s += 100
    if 'NIFTY' in sym or 'NIFTY' in disp: s += 30
    # Exclude other NIFTY families
    for bad in ['FINNIFTY','FIN','MID','SMALL','IT','AUTO','FMCG','MEDIA','PHAR','PHARMA','METAL','REALTY','CPSE','ENERGY']:
        if bad in sym or bad in disp:
            s -= 200
    return s

def score_bank(sym, disp):
    s = 0
    if sym == 'BANKNIFTY': s += 120
    if 'NIFTY BANK' in disp or 'BANK NIFTY' in disp: s += 100
    if ('BANK' in sym or 'BANK' in disp) and ('NIFTY' in sym or 'NIFTY' in disp): s += 40
    return s

def detect():
    nifty_cands = []
    bank_cands  = []
    with CSV_PATH.open('r', encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        need = {'INSTRUMENT_TYPE','SYMBOL_NAME','DISPLAY_NAME','SECURITY_ID','EXCH_ID'}
        if not need.issubset(set(r.fieldnames or [])):
            print('Missing required columns. Found:', r.fieldnames); sys.exit(1)
        for row in r:
            if U(row.get('INSTRUMENT_TYPE')) != 'INDEX':
                continue
            if not is_nse(row.get('EXCH_ID')):
                continue
            secid = (row.get('SECURITY_ID') or '').strip()
            if not secid.isdigit():
                continue
            sym  = U(row.get('SYMBOL_NAME'))
            disp = U(row.get('DISPLAY_NAME'))
            sn = score_nifty(sym, disp)
            sb = score_bank(sym, disp)
            if sn > 0:
                nifty_cands.append((sn, int(secid), sym, disp))
            if sb > 0:
                bank_cands.append((sb, int(secid), sym, disp))

    pick_nifty = max(nifty_cands, default=(None, None, None, None))
    pick_bank  = max(bank_cands,  default=(None, None, None, None))
    return pick_nifty, pick_bank, sorted(nifty_cands, reverse=True)[:5], sorted(bank_cands, reverse=True)[:5]

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
    print(f'Updated JSON: NIFTY={nifty_id} BANKNIFTY={bank_id}')

if __name__ == '__main__':
    (sn, nid, nsym, ndisp), (sb, bid, bsym, bdisp), topN, topB = detect()
    print('Top NIFTY candidates:', topN)
    print('Top BANKNIFTY candidates:', topB)
    nifty_id = nid
    bank_id  = bid
    if not nifty_id or not bank_id:
        print('Could not detect both NSE INDEX SecurityIds.'); sys.exit(1)
    update_json(nifty_id, bank_id)
