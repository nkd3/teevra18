from common.bootstrap import init_runtime
init_runtime()
import os, sys, json, time, sqlite3, argparse, datetime as dt
from pathlib import Path
import requests

DB_PATH = Path(os.getenv('DB_PATH', r'C:\teevra18\data\teevra18.db'))
CONF_PATH = Path(r'C:\teevra18\config\underlyings_chain.json')

BASE = os.getenv('DHAN_REST_BASE', 'https://api.dhan.co')
CID  = os.getenv('DHAN_CLIENT_ID')
TOK  = os.getenv('DHAN_ACCESS_TOKEN')

HDR = {'Content-Type':'application/json'}
if CID and TOK:
    HDR.update({'client-id':CID, 'access-token':TOK})

RATE_GAP = 3.2  # >= 3 seconds per API

def u(s): return (s or '').strip().upper()

def load_config(p=CONF_PATH):
    return json.loads(p.read_text(encoding='utf-8-sig'))

def get_underlyings(group, cfg):
    out = []
    if group == 'indices':
        defaults = {'NIFTY':13, 'BANKNIFTY':25}
        for it in cfg.get('groups',{}).get('indices',[]):
            if not it.get('enabled'): continue
            und = u(it.get('underlying')); seg = it.get('underlying_seg') or 'IDX_I'
            sid = it.get('underlying_scrip') or defaults.get(und)
            if und and sid: out.append((und, int(sid), seg))
        have = {a for a,_,_ in out}
        for und, sid in defaults.items():
            if und not in have: out.append((und, sid, 'IDX_I'))
    elif group == 'nifty50':
        for it in cfg.get('groups',{}).get('nifty50',[]):
            if not it.get('enabled'): continue
            und = u(it.get('underlying')); seg = it.get('underlying_seg') or 'NSE_FNO'
            sid = it.get('underlying_scrip')
            if und and sid: out.append((und, int(sid), seg))
    return out

def expirylist(secid, seg):
    url = f'{BASE}/v2/optionchain/expirylist'
    r = requests.post(url, headers=HDR, json={'UnderlyingScrip': secid, 'UnderlyingSeg': seg}, timeout=25)
    r.raise_for_status()
    return r.json().get('data',[]) or []

def classify_expiry(date_str):
    d = dt.date.fromisoformat(date_str)
    return 'monthly' if d.day >= 25 else 'weekly'

def pick_expiries(exp_list, mode):
    if not exp_list: return []
    weekly = [e for e in exp_list if classify_expiry(e)=='weekly']
    monthly= [e for e in exp_list if classify_expiry(e)=='monthly']
    if mode=='nearest-weekly': return weekly[:1] or exp_list[:1]
    if mode=='next-monthly':  return monthly[:1] or (exp_list[:1] if exp_list else [])
    if mode=='both':
        picks=[]; 
        if weekly: picks.append(weekly[0])
        if monthly: picks.append(monthly[0])
        seen=set(); res=[]
        for e in exp_list:
            if e in picks and e not in seen:
                res.append(e); seen.add(e)
        return res
    return exp_list[:1]

def fetch_chain(secid, seg, expiry):
    url = f'{BASE}/v2/optionchain'
    body = {'UnderlyingScrip': secid, 'UnderlyingSeg': seg, 'Expiry': expiry}
    r = requests.post(url, headers=HDR, json=body, timeout=30)
    if r.status_code == 429:
        time.sleep(3.6)
        r = requests.post(url, headers=HDR, json=body, timeout=30)
    r.raise_for_status()
    return r.json().get('data',{}) or {}

# ---------- DB helpers ----------
def table_info(conn, table):
    cur = conn.execute(f'PRAGMA table_info({table})')
    return [{'name':r[1], 'type':(r[2] or '').upper(), 'notnull':int(r[3] or 0), 'pk':int(r[5] or 0)} for r in cur.fetchall()]

def default_for(column, colmeta, ctx):
    t = (colmeta.get('type') or 'TEXT').upper()
    name = column.lower()
    if name == 'ts': return ctx['ts']
    if 'symbol' in name: return ctx['und']
    if (name.endswith('_utc') or name.endswith('_at') or name.startswith('ts_')) and t.startswith('TEXT'): return ctx['ts']
    if 'expiry' in name: return ctx['expiry']
    if 'side' in name: return ctx['side']
    if 'strike' in name: return ctx['strike']
    if 'underlying_scrip' in name: return ctx['uscrip']
    if 'underlying' in name and name != 'underlying_scrip': return ctx['und']
    if 'seg' in name or 'segment' in name: return ctx['seg']
    if 'exchange' in name: return 'NSE'
    if 'provider' in name or 'source' in name or name == 'src': return 'dhan'
    if t.startswith('INT'): return 0
    if t.startswith('REAL') or t.startswith('NUM'): return 0.0
    return ''

def build_insert_plan(conn):
    tinfo = table_info(conn, 'option_chain_snap')
    if not tinfo: raise SystemExit('option_chain_snap is missing.')
    required = [c['name'] for c in tinfo if c['notnull']==1 and c['pk']==0]
    base_cols = [
        'ts_fetch_utc','underlying','underlying_scrip','underlying_seg','expiry',
        'last_price','strike','side','implied_volatility','ltp','oi','previous_oi',
        'volume','previous_volume','previous_close_price','top_ask_price','top_ask_quantity',
        'top_bid_price','top_bid_quantity','delta','theta','gamma','vega','src_status','chain_json'
    ]
    col_list=[]
    for c in required:
        if c.lower()!='id' and c not in col_list: col_list.append(c)
    for c in base_cols:
        if c not in col_list: col_list.append(c)
    placeholders = ','.join(['?']*len(col_list))
    sql = f'INSERT OR IGNORE INTO option_chain_snap ({",".join(col_list)}) VALUES ({placeholders})'
    tmeta = {c['name']:c for c in tinfo}
    return sql, col_list, tmeta

def row_values(col_list, tmeta, ts, und, uscrip, seg, expiry, last_price, strike, side, leg):
    g = (leg.get('greeks') or {}) if isinstance(leg, dict) else {}
    import json as _json
    base = {
        'ts_fetch_utc': ts,'underlying': und,'underlying_scrip': uscrip,'underlying_seg': seg,
        'expiry': expiry,'last_price': last_price,'strike': float(strike),'side': side,
        'implied_volatility': leg.get('implied_volatility') if isinstance(leg, dict) else None,
        'ltp': leg.get('last_price') if isinstance(leg, dict) else None,
        'oi': leg.get('oi') if isinstance(leg, dict) else None,'previous_oi': leg.get('previous_oi') if isinstance(leg, dict) else None,
        'volume': leg.get('volume') if isinstance(leg, dict) else None,'previous_volume': leg.get('previous_volume') if isinstance(leg, dict) else None,
        'previous_close_price': leg.get('previous_close_price') if isinstance(leg, dict) else None,
        'top_ask_price': leg.get('top_ask_price') if isinstance(leg, dict) else None,'top_ask_quantity': leg.get('top_ask_quantity') if isinstance(leg, dict) else None,
        'top_bid_price': leg.get('top_bid_price') if isinstance(leg, dict) else None,'top_bid_quantity': leg.get('top_bid_quantity') if isinstance(leg, dict) else None,
        'delta': g.get('delta'),'theta': g.get('theta'),'gamma': g.get('gamma'),'vega': g.get('vega'),
        'src_status': 'success','chain_json': _json.dumps(leg, separators=(',',':')) if isinstance(leg, dict) else None
    }
    ctx = {'ts': ts, 'und': und, 'uscrip': uscrip, 'seg': seg, 'expiry': expiry, 'strike': float(strike), 'side': side}
    return [ base.get(name, default_for(name, tmeta.get(name,{}), ctx)) for name in col_list ]

# ---------- ops_log adaptive insert ----------
def opslog_insert_adaptive(ts, component, status, rows, warns, extra):
    import json as _json
    with sqlite3.connect(DB_PATH) as c:
        cols = [(r[1], int(r[3] or 0), (r[2] or '').upper()) for r in c.execute('PRAGMA table_info(ops_log)')]
        col_names = [n for (n,_,_) in cols]
        values_map = {
            'ts_utc': ts,
            'component': component,
            'status': status,
            'rows': rows,
            'warns': _json.dumps(warns),
            'extra': _json.dumps(extra),
            # common legacy fields:
            'level': 'INFO' if status=='ok' else ('WARN' if status=='warn' else 'ERROR'),
            'area': component,
            'msg': f'{status}; rows={rows}; warns={len(warns)}'
        }
        insert_cols=[]; insert_vals=[]
        for (name, notnull, typ) in cols:
            if name=='id': continue
            if name in values_map:
                insert_cols.append(name); insert_vals.append(values_map[name])
            else:
                # ensure NOT NULL satisfied with type-based default
                if notnull==1:
                    if typ.startswith('INT'): insert_cols.append(name); insert_vals.append(0)
                    elif typ.startswith('REAL') or typ.startswith('NUM'): insert_cols.append(name); insert_vals.append(0.0)
                    else: insert_cols.append(name); insert_vals.append('')
                else:
                    # nullable and we don't care -> skip
                    pass
        if not insert_cols:
            return
        qmarks = ','.join(['?']*len(insert_cols))
        sql = f'INSERT INTO ops_log({",".join(insert_cols)}) VALUES ({qmarks})'
        c.execute(sql, insert_vals)
        c.commit()

# ---------- core run ----------
def run_once(group, expiries_mode):
    cfg = load_config()
    ulys = get_underlyings(group, cfg)
    if not ulys:
        print(f'[ERR] group={group} has no enabled underlyings'); return 1

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    warns = []
    now_ts = dt.datetime.utcnow().isoformat(timespec='seconds') + 'Z'

    with sqlite3.connect(DB_PATH) as conn:
        sql, col_list, tmeta = build_insert_plan(conn)
        cur = conn.cursor()
        last_call = 0.0

        for (und, uscrip, seg) in ulys:
            wait = RATE_GAP - (time.time() - last_call)
            if wait > 0: time.sleep(wait)
            try:
                exps = expirylist(uscrip, seg)
            except requests.exceptions.HTTPError as e:
                print(f'[ERR] {und}: expirylist {e}')
                last_call = time.time(); continue
            last_call = time.time()
            picks = pick_expiries(exps, expiries_mode)
            if not picks:
                print(f'[ERR] {und}: no expiries (mode={expiries_mode})'); continue

            for exp in picks:
                wait = RATE_GAP - (time.time() - last_call)
                if wait > 0: time.sleep(wait)
                try:
                    data = fetch_chain(uscrip, seg, exp)
                except requests.exceptions.HTTPError as e:
                    print(f'[ERR] {und} {exp}: optionchain {e}')
                    last_call = time.time(); continue
                last_call = time.time()

                oc = data.get('oc') or {}
                last_price = data.get('last_price')
                n = 0; zero_g = 0; zero_iv = 0; tot = 0
                for k, rec in oc.items():
                    try:
                        strike = float(k)
                    except:
                        try: strike = float(f'{float(k):.6f}')
                        except: continue
                    for side, leg in (('CE', rec.get('ce')), ('PE', rec.get('pe'))):
                        if not leg: continue
                        vals = row_values(col_list, tmeta, now_ts, und, uscrip, seg, exp, last_price, strike, side, leg)
                        cur.execute(sql, vals)
                        n += 1; tot += 1
                        g = (leg.get('greeks') or {}) if isinstance(leg, dict) else {}
                        if (g.get('delta') in (None,0)) and (g.get('gamma') in (None,0)) and (g.get('vega') in (None,0)): zero_g += 1
                        iv = leg.get('implied_volatility')
                        if iv in (None,0,0.0): zero_iv += 1
                conn.commit()
                total_rows += n
                zg_pct = (zero_g*100/tot) if tot else 0.0
                zi_pct = (zero_iv*100/tot) if tot else 0.0
                if zg_pct > 30: warns.append(f'{und}@{exp}: zero_greeks {zg_pct:.1f}%')
                if zi_pct > 40: warns.append(f'{und}@{exp}: zero_iv {zi_pct:.1f}%')
                print(f'[OK] {und} {exp}: stored {n} rows  (zero_g={zero_g}/{tot} {zg_pct:.1f}%  zero_iv={zi_pct:.1f}%)')

    try:
        opslog_insert_adaptive(now_ts, 'chain', 'ok' if total_rows>0 else 'warn', total_rows, warns, {'group':group,'expiries':expiries_mode})
    except Exception as e:
        print('[WARN] ops_log insert failed:', e)

    return 0 if total_rows>0 else 2

def follow_loop(group, expiries_mode, poll_s):
    print('Follow loop started. Ctrl+C to stop.')
    while True:
        try:
            run_once(group, expiries_mode)
        except KeyboardInterrupt:
            print('Stopped by user.'); return
        except Exception as e:
            print('[ERR] run_once exception:', e)
        time.sleep(max(1, poll_s))

def main():
    ap = argparse.ArgumentParser(description='Option Chain Snapshot Service (adaptive)')
    sub = ap.add_subparsers(dest='cmd', required=True)

    ap_once = sub.add_parser('once')
    ap_once.add_argument('--group', choices=['indices','nifty50'], default='indices')
    ap_once.add_argument('--expiries', choices=['nearest-weekly','next-monthly','both'], default='nearest-weekly')

    ap_follow = sub.add_parser('follow')
    ap_follow.add_argument('--group', choices=['indices','nifty50'], default='indices')
    ap_follow.add_argument('--expiries', choices=['nearest-weekly','next-monthly','both'], default='nearest-weekly')
    ap_follow.add_argument('--poll-seconds', type=int, default=9)

    args = ap.parse_args()
    if not CID or not TOK:
        print('Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN'); sys.exit(2)

    if args.cmd == 'once':
        sys.exit(run_once(args.group, args.expiries))
    else:
        follow_loop(args.group, args.expiries, args.poll_seconds)

if __name__ == '__main__':
    main()

