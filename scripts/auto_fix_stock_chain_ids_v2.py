import os, sys, csv, json, time, datetime as dt
from pathlib import Path
import requests
from collections import defaultdict, Counter

CSV_PATH  = Path(r'C:\teevra18\data\api-scrip-master-detailed.csv')
JSON_PATH = Path(r'C:\teevra18\config\underlyings_chain.json')

BASE = os.getenv('DHAN_REST_BASE','https://api.dhan.co')
CID  = os.getenv('DHAN_CLIENT_ID')
TOK  = os.getenv('DHAN_ACCESS_TOKEN')
HDR  = {'Content-Type':'application/json','client-id':CID or '', 'access-token':TOK or ''}

RATE_GAP = 3.2
# Try segments in this order (broad to specific). Keep 'EQ_I' early, as many brokers use that for stock underlyings.
SEG_CANDIDATES = ['EQ_I','NSE_FNO','NSE_EQ','EQ','STK_I']

SYMS = ['RELIANCE','HDFCBANK','INFY']  # extend if you like

def U(s): return (s or '').strip().upper()

def csv_fieldnames():
    with CSV_PATH.open('r', encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        return r.fieldnames or []

def gather_candidates(symbol):
    """
    Build a candidate ID list for the underlying:
      1) Cash equity rows (INSTRUMENT_TYPE in ES/EQ) -> SECURITY_ID  (prefer EXCH_ID with 'NSE')
      2) FUTSTK rows -> UNDERLYING_SECURITY_ID (prefer NSE)
      3) OPTSTK rows -> UNDERLYING_SECURITY_ID (prefer NSE)
    Deduplicate, keep priority order, and cap to top ~8-10.
    """
    fns = set(csv_fieldnames())
    need = {'INSTRUMENT_TYPE','UNDERLYING_SYMBOL','UNDERLYING_SECURITY_ID','EXCH_ID','SECURITY_ID'}
    if not need.issubset(fns):
        print('CSV missing required columns. Found:', sorted(fns)); sys.exit(2)

    buckets = []  # list of (priority, ex_pref, id_int)
    with CSV_PATH.open('r', encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            itype = U(row.get('INSTRUMENT_TYPE'))
            usym  = U(row.get('UNDERLYING_SYMBOL'))
            exid  = U(row.get('EXCH_ID'))
            if usym != symbol:
                # For cash rows the symbol may be in SYMBOL_NAME instead; we still require it to match the symbol
                if itype in ('ES','EQ'):
                    # Some CSVs put symbol in SYMBOL_NAME; try loose match
                    sn = U(row.get('SYMBOL_NAME'))
                    if sn != symbol:
                        continue
                else:
                    continue

            pref = 0 if ('NSE' in exid) else (1 if exid else 2)

            # 1) Cash equity -> SECURITY_ID
            if itype in ('ES','EQ'):
                secid = (row.get('SECURITY_ID') or '').strip()
                if secid.isdigit():
                    buckets.append((1, pref, int(secid)))

            # 2) FUTSTK -> UNDERLYING_SECURITY_ID
            if itype == 'FUTSTK':
                uid = (row.get('UNDERLYING_SECURITY_ID') or '').strip()
                if uid.isdigit():
                    buckets.append((2, pref, int(uid)))

            # 3) OPTSTK -> UNDERLYING_SECURITY_ID
            if itype == 'OPTSTK':
                uid = (row.get('UNDERLYING_SECURITY_ID') or '').strip()
                if uid.isdigit():
                    buckets.append((3, pref, int(uid)))

    # Sort by (priority, NSE-first), then de-dupe keeping first occurrence
    buckets.sort(key=lambda t: (t[0], t[1]))
    out, seen = [], set()
    for _, _, val in buckets:
        if val not in seen:
            out.append(val); seen.add(val)
        if len(out) >= 10:
            break
    return out

def try_expirylist(underlying_scrip, seg, last_call_time):
    wait = RATE_GAP - (time.time() - last_call_time)
    if wait > 0: time.sleep(wait)
    url = f'{BASE}/v2/optionchain/expirylist'
    body = {'UnderlyingScrip': underlying_scrip, 'UnderlyingSeg': seg}
    r = requests.post(url, headers=HDR, json=body, timeout=25)
    last_call_time = time.time()
    ok = False
    data = []
    try:
        r.raise_for_status()
        j = r.json()
        data = j.get('data',[]) or []
        today = dt.date.today().isoformat()
        fut = [x for x in data if x and x >= today]
        ok = len(fut) > 0
    except Exception:
        ok = False
    return ok, data, last_call_time, r.status_code, (r.text[:200] if r is not None else '')

def update_json(sym2pick):
    data = json.loads(JSON_PATH.read_text(encoding='utf-8-sig'))
    lst = data.setdefault('groups',{}).setdefault('nifty50',[])
    idx = { (it.get('underlying') or '').upper(): it for it in lst }
    changed = []
    for sym, (uid, seg) in sym2pick.items():
        row = idx.get(sym)
        if not row:
            row = {'underlying': sym}
            lst.append(row)
        row['underlying_scrip'] = int(uid)
        row['underlying_seg']   = seg
        row['enabled']          = True
        changed.append((sym, uid, seg))
    JSON_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    return changed

def main():
    if not CID or not TOK:
        print('Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN in environment.'); sys.exit(2)

    sym2pick = {}
    last_net = 0.0
    for sym in SYMS:
        cands = gather_candidates(sym)
        if not cands:
            print(f'{sym}: no ID candidates from CSV.'); continue
        picked = None
        for uid in cands:
            for seg in SEG_CANDIDATES:
                ok, data, last_net, code, body = try_expirylist(uid, seg, last_net)
                print(f'TRY {sym}: uid={uid} seg={seg} -> {code} ok={ok} (expiries~{len(data)})')
                if ok:
                    picked = (uid, seg)
                    break
            if picked:
                break
        if not picked:
            print(f'WARN {sym}: no working (uid, seg) found among {len(cands)} candidates.')
        else:
            sym2pick[sym] = picked
            print(f'PICK {sym}: uid={picked[0]} seg={picked[1]}')

    if not sym2pick:
        print('No working picks; nothing to update.'); sys.exit(1)

    changed = update_json(sym2pick)
    print('Updated JSON for:', changed)

if __name__ == '__main__':
    main()
