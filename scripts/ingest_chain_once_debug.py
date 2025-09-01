import os, sys, json, sqlite3, datetime as dt, time
from pathlib import Path
import requests

DB_PATH = Path(os.getenv('DB_PATH', r'C:\teevra18\data\teevra18.db'))
BASE = os.getenv('DHAN_REST_BASE','https://api.dhan.co')
CID  = os.getenv('DHAN_CLIENT_ID')
TOK  = os.getenv('DHAN_ACCESS_TOKEN')

if not CID or not TOK:
    print('Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN'); sys.exit(2)

HDR = {'Content-Type':'application/json','client-id':CID,'access-token':TOK}
# NSE index SecurityIds confirmed by your probe
IDX = [('NIFTY',13), ('BANKNIFTY',25)]

# ------------------- Network helpers -------------------
def nearest_future_expiry(secid):
    url = f"{BASE}/v2/optionchain/expirylist"
    body = {'UnderlyingScrip': secid, 'UnderlyingSeg': 'IDX_I'}
    r = requests.post(url, headers=HDR, json=body, timeout=20)
    r.raise_for_status()
    data = r.json().get('data',[])
    today = dt.date.today().isoformat()
    fut = [x for x in data if x >= today]
    return fut[0] if fut else None

def fetch_chain(secid, expiry):
    url = f"{BASE}/v2/optionchain"
    body = {'UnderlyingScrip': secid, 'UnderlyingSeg': 'IDX_I', 'Expiry': expiry}
    r = requests.post(url, headers=HDR, json=body, timeout=25)
    if r.status_code == 429:
        time.sleep(3.6)  # backoff once
        r = requests.post(url, headers=HDR, json=body, timeout=25)
    r.raise_for_status()
    return r.json().get('data',{})

# ------------------- DB helpers -------------------
def table_info(conn, table):
    cur = conn.execute(f"PRAGMA table_info({table})")
    # returns: (cid, name, type, notnull, dflt_value, pk)
    rows = cur.fetchall()
    return [{'name':r[1], 'type':(r[2] or '').upper(), 'notnull':int(r[3] or 0), 'pk':int(r[5] or 0)} for r in rows]

def build_column_plan(tinfo):
    # Base payload cols we know how to populate
    base_cols = [
        'ts_fetch_utc','underlying','underlying_scrip','underlying_seg','expiry',
        'last_price','strike','side','implied_volatility','ltp','oi','previous_oi',
        'volume','previous_volume','previous_close_price','top_ask_price','top_ask_quantity',
        'top_bid_price','top_bid_quantity','delta','theta','gamma','vega','src_status'
    ]
    base_set = set(base_cols)

    # Required NOT NULL columns (excluding PK/id)
    required = [c['name'] for c in tinfo if c['notnull']==1 and c['pk']==0]
    # Ensure required columns are first (stable order), then add the rest of base set not already included
    col_list = []
    for c in required:
        if c not in col_list and c.lower() != 'id':
            col_list.append(c)
    for c in base_cols:
        if c not in col_list:
            col_list.append(c)

    # We will supply values for every name in col_list.
    # For anything not in base_set, we compute a safe default at runtime.
    return col_list, {c['name']:c for c in tinfo}

def default_for(column, colmeta, ctx):
    # ctx: dict(ts, und, uscrip, seg, expiry, strike, side, last_price, leg)
    t = (colmeta.get('type') or 'TEXT').upper()
    name = column.lower()

    # Smart mappings
    if name == 'ts':
        return ctx['ts']
    if 'symbol' in name:
        # underlying_symbol, symbol, option_symbol, etc.
        return ctx['und']
    if (name.endswith('_utc') or name.endswith('_at') or name.startswith('ts_')) and t.startswith('TEXT'):
        return ctx['ts']
    if 'expiry' in name:
        return ctx['expiry']
    if 'side' in name:
        return ctx['side']
    if 'strike' in name:
        return ctx['strike']
    if 'underlying_scrip' in name:
        return ctx['uscrip']
    if 'underlying' in name and name != 'underlying_scrip':
        return ctx['und']
    if 'seg' in name or 'segment' in name:
        return ctx['seg']
    if 'exchange' in name:
        return 'NSE'
    if 'provider' in name or 'source' in name or name == 'src':
        return 'dhan'

    # Last-ditch defaults based on type
    if t.startswith('INT'):
        return 0
    if t.startswith('REAL') or t.startswith('NUM'):
        return 0.0
    # TEXT or anything else
    return ''

def row_values(col_list, tmeta, ts, und, uscrip, seg, expiry, last_price, strike, side, leg):
    g = (leg.get('greeks') or {}) if isinstance(leg, dict) else {}
    base = {
        'ts': ts,
        'ts_fetch_utc': ts,
        'underlying': und,
        'underlying_scrip': uscrip,
        'underlying_seg': seg,
        'expiry': expiry,
        'last_price': last_price,
        'strike': float(strike),
        'side': side,
        'implied_volatility': leg.get('implied_volatility') if isinstance(leg, dict) else None,
        'ltp': leg.get('last_price') if isinstance(leg, dict) else None,
        'oi': leg.get('oi') if isinstance(leg, dict) else None,
        'previous_oi': leg.get('previous_oi') if isinstance(leg, dict) else None,
        'volume': leg.get('volume') if isinstance(leg, dict) else None,
        'previous_volume': leg.get('previous_volume') if isinstance(leg, dict) else None,
        'previous_close_price': leg.get('previous_close_price') if isinstance(leg, dict) else None,
        'top_ask_price': leg.get('top_ask_price') if isinstance(leg, dict) else None,
        'top_ask_quantity': leg.get('top_ask_quantity') if isinstance(leg, dict) else None,
        'top_bid_price': leg.get('top_bid_price') if isinstance(leg, dict) else None,
        'top_bid_quantity': leg.get('top_bid_quantity') if isinstance(leg, dict) else None,
        'delta': g.get('delta'),
        'theta': g.get('theta'),
        'gamma': g.get('gamma'),
        'vega': g.get('vega'),
        'src_status': 'success',
    }
    ctx = {
        'ts': ts, 'und': und, 'uscrip': uscrip, 'seg': seg, 'expiry': expiry,
        'strike': float(strike), 'side': side, 'last_price': last_price, 'leg': leg
    }
    out = []
    for name in col_list:
        if name in base:
            out.append(base[name])
        else:
            out.append(default_for(name, tmeta.get(name, {}), ctx))
    return out

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        # Discover table and build column plan
        tinfo = table_info(conn, 'option_chain_snap')
        if not tinfo:
            print("Error: table option_chain_snap not found."); sys.exit(2)
        col_list, tmeta = build_column_plan(tinfo)
        placeholders = ','.join(['?']*len(col_list))
        sql = f"INSERT OR IGNORE INTO option_chain_snap ({','.join(col_list)}) VALUES ({placeholders})"
        print("[plan] insert columns:", ', '.join(col_list))

        cur = conn.cursor()
        total = 0
        last_net = 0.0

        for (und, uscrip) in IDX:
            # rate-limit spacing (>=3.2s between calls)
            wait = 3.2 - (time.time() - last_net)
            if wait > 0: time.sleep(wait)

            expiry = nearest_future_expiry(uscrip)
            last_net = time.time()
            if not expiry:
                print(f"[SKIP] {und}: no future expiry")
                continue

            wait = 3.2 - (time.time() - last_net)
            if wait > 0: time.sleep(wait)

            data = fetch_chain(uscrip, expiry)
            last_net = time.time()

            oc = data.get('oc') or {}
            last_price = data.get('last_price')
            ts = dt.datetime.utcnow().isoformat(timespec='seconds') + 'Z'

            n_rows = 0
            for k, rec in oc.items():
                try:
                    strike = float(k)
                except:
                    try:
                        strike = float(f"{float(k):.6f}")
                    except:
                        continue
                for side, leg in (('CE', rec.get('ce')), ('PE', rec.get('pe'))):
                    if leg is None:
                        continue
                    vals = row_values(col_list, tmeta, ts, und, uscrip, 'IDX_I', expiry, last_price, strike, side, leg)
                    cur.execute(sql, vals)
                    n_rows += 1

            conn.commit()
            print(f"[OK] {und} {expiry}: inserted {n_rows} rows")
            total += n_rows

        print(f"Total inserted: {total}")

if __name__ == '__main__':
    try:
        main()
    except requests.exceptions.HTTPError as e:
        print('HTTPError:', e, file=sys.stderr)
        if hasattr(e, 'response') and e.response is not None:
            print(e.response.text[:300], file=sys.stderr)
        sys.exit(1)

