import os, json, sys
from pathlib import Path
import requests

JSON_PATH = Path(r'C:\teevra18\config\underlyings_chain.json')
BASE = os.getenv('DHAN_REST_BASE', 'https://api.dhan.co')
CID  = os.getenv('DHAN_CLIENT_ID')
TOK  = os.getenv('DHAN_ACCESS_TOKEN')

if not CID or not TOK:
    print('Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN in environment.'); sys.exit(2)

data = json.loads(JSON_PATH.read_text(encoding='utf-8-sig'))
ids = {}
for it in data.get('groups',{}).get('indices',[]):
    ids[it['underlying']] = it['underlying_scrip']

def probe(label, secid):
    url = f'{BASE}/v2/optionchain/expirylist'
    hdr = {'Content-Type':'application/json','client-id':CID,'access-token':TOK}
    body = {'UnderlyingScrip': int(secid), 'UnderlyingSeg': 'IDX_I'}
    try:
        resp = requests.post(url, headers=hdr, json=body, timeout=15)
        print(f'{label} ({secid}) -> {resp.status_code}: {resp.text[:180]}')
    except Exception as e:
        print(f'{label} ({secid}) -> EXC: {e}')

for k,v in ids.items():
    probe(k, v)
