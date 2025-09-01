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
SEG_CANDIDATES = ['EQ_I','NSE_FNO','NSE_EQ','EQ']  # try in this order

SYMS = ['RELIANCE','HDFCBANK','INFY']  # extend if you like

def U(s): return (s or '').strip().upper()

def candidates_from_csv(symbol):
    """
    Find candidate UNDERLYING_SECURITY_IDs for a stock symbol using OPTSTK rows.
    Group by EXCH_ID to prefer NSE-looking rows.
    """
    groups = defaultdict(Counter)  # EXCH_ID -> Counter(UnderlyingSecurityId)
    with CSV_PATH.open('r', encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        need = {'INSTRUMENT_TYPE','UNDERLYING_SYMBOL','UNDERLYING_SECURITY_ID','EXCH_ID'}
        if not need.issubset(set(r.fieldnames or [])):
            print('CSV missing columns:', r.fieldnames); sys.exit(2)
        for row in r:
            if U(row.get('INSTRUMENT_TYPE')) != 'OPTSTK':
                continue
            if U(row.get('UNDERLYING_SYMBOL')) != symbol:
                continue
            uid = (row.get('UNDERLYING_SECURITY_ID') or '').strip()
            if not uid.isdigit():
                continue
            ex = U(row.get('EXCH_ID'))
            groups[ex][int(uid)] += 1
    # Build an ordered list: prefer EXCH_ID containing 'NSE' first, then others
    ordered = []
    ex_keys = sorted(groups.keys(), key=lambda k: (0 if 'NSE' in k else 1, k))
    for ex in ex_keys:
        for uid, _ in groups[ex].most_common():
            ordered.append((ex, uid))
    # Deduplicate by uid, keep first occurrence
    out, seen = [], set()
    for ex, uid in ordered:
        if uid not in seen:
            out.append(uid); seen.add(uid)
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
        # accept only if we actually got a list of expiries in the future
        today = dt.date.today()
        fut = [x for x in data if x and x >= today.isoformat()]
        ok = len(fut) > 0
    except Exception:
        ok = False
    return ok, data, last_call_time, r.status_code, (r.text[:200] if r is not None else '')

def update_json(sym2pick):
    data = json.loads(JSON_PATH.read_text(encoding='utf-8-sig'))
    lst = data.setdefault('groups',{}).setdefault('nifty50',[])
    idx = { U(it.get('underlying')): it for it in lst }
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
        uids = candidates_from_csv(sym)
        if not uids:
            print(f'{sym}: no candidates found in CSV (OPTSTK).')
            continue
        picked = None
        for uid in uids:
            for seg in SEG_CANDIDATES:
                ok, data, last_net, code, body = try_expirylist(uid, seg, last_net)
                print(f'TRY {sym}: uid={uid} seg={seg} -> {code} ok={ok}  (expiries~{len(data)})')
                if ok:
                    picked = (uid, seg)
                    break
            if picked:
                break
        if not picked:
            print(f'WARN {sym}: could not find a working (uid, seg).')
        else:
            sym2pick[sym] = picked
            print(f'PICK {sym}: uid={picked[0]} seg={picked[1]}')

    if not sym2pick:
        print('No working picks; nothing to update.'); sys.exit(1)

    changed = update_json(sym2pick)
    print('Updated JSON for:', changed)

if __name__ == '__main__':
    main()
