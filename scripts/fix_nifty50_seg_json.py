import json
from pathlib import Path

JSON_PATH = Path(r'C:\teevra18\config\underlyings_chain.json')
data = json.loads(JSON_PATH.read_text(encoding='utf-8-sig'))
lst = data.setdefault('groups',{}).setdefault('nifty50',[])
idx = { (it.get('underlying') or '').upper(): it for it in lst }
for sym in ('RELIANCE','HDFCBANK','INFY'):
    row = idx.get(sym)
    if not row:
        row = {'underlying': sym}
        lst.append(row)
    row['underlying_seg'] = 'NSE_FNO'
    row['enabled'] = True
JSON_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
print('OK: JSON updated for RELIANCE/HDFCBANK/INFY -> NSE_FNO & enabled.')
