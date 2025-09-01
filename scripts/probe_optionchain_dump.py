import os, sys, json, datetime as dt
from pathlib import Path
import requests

BASE = os.getenv('DHAN_REST_BASE','https://api.dhan.co')
CID  = os.getenv('DHAN_CLIENT_ID')
TOK  = os.getenv('DHAN_ACCESS_TOKEN')

if not CID or not TOK:
    print('Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN'); sys.exit(2)

HDR = {'Content-Type':'application/json','client-id':CID,'access-token':TOK}
IDX = {'NIFTY':13, 'BANKNIFTY':25}

def nearest_future_expiry(secid):
    url = f'{BASE}/v2/optionchain/expirylist'
    body = {'UnderlyingScrip': secid, 'UnderlyingSeg': 'IDX_I'}
    r = requests.post(url, headers=HDR, json=body, timeout=15)
    r.raise_for_status()
    data = r.json().get('data',[])
    today = dt.date.today()
    fut = [dt.date.fromisoformat(x) for x in data if dt.date.fromisoformat(x) >= today]
    return fut[0].isoformat() if fut else None

def dump_chain(label, secid, expiry):
    url = f'{BASE}/v2/optionchain'
    body = {'UnderlyingScrip': secid, 'UnderlyingSeg': 'IDX_I', 'Expiry': expiry}
    r = requests.post(url, headers=HDR, json=body, timeout=20)
    print(f'{label} optionchain HTTP {r.status_code}')
    if r.status_code != 200:
        print(r.text[:200])
        return
    j = r.json()
    data = j.get('data',{})
    ltp = data.get('last_price')
    oc  = data.get('oc',{}) or {}
    strikes = list(oc.keys())
    print(f'  expiry={expiry} ltp={ltp} strikes={len(strikes)}')
    # Count ce/pe availability
    ce = sum(1 for k in strikes if isinstance(oc.get(k),dict) and oc[k].get('ce'))
    pe = sum(1 for k in strikes if isinstance(oc.get(k),dict) and oc[k].get('pe'))
    print(f'  ce_count≈{ce}  pe_count≈{pe}')
    # Show a few sample strikes near median
    if strikes:
        try:
            vals = sorted([float(s) for s in strikes])
        except:
            vals = strikes
        mids = vals[len(vals)//2-2:len(vals)//2+3] if len(vals)>=5 else vals[:5]
        print('  sample_strikes:', mids)
        for s in [str(m) for m in mids]:
            rec = oc.get(s) or oc.get(f'{float(s):.6f}')
            if rec:
                ce_iv = rec.get('ce',{}).get('implied_volatility')
                pe_iv = rec.get('pe',{}).get('implied_volatility')
                print(f'    {s}: ce_iv={ce_iv} pe_iv={pe_iv}')
    else:
        print('  NOTE: empty oc')

def main():
    for label, secid in IDX.items():
        exp = nearest_future_expiry(secid)
        print(f'{label}: nearest_future_expiry={exp}')
        if not exp:
            print(f'  No expiry found for {label}')
            continue
        dump_chain(label, secid, exp)

if __name__ == '__main__':
    main()
