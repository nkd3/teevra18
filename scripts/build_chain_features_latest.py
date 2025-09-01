import os, sqlite3, argparse
from pathlib import Path

DB = Path(os.getenv('DB_PATH', r'C:\teevra18\data\teevra18.db'))

def get_latest_ts_by_underlying(cur, und):
    row = cur.execute('SELECT MAX(ts_fetch_utc) FROM option_chain_snap WHERE underlying=?', (und,)).fetchone()
    return row[0] if row else None

def fetch_rows(cur, ts, und):
    # grab all rows for that underlying & timestamp (both CE/PE, all strikes/expiries)
    sql = '''SELECT expiry, last_price, strike, side,
                    implied_volatility, delta, volume, oi
             FROM option_chain_snap
             WHERE ts_fetch_utc=? AND underlying=?'''
    rows = cur.execute(sql, (ts, und)).fetchall()
    out = {}
    last_price = None
    for expiry, ltp, strike, side, iv, delta, vol, oi in rows:
        last_price = ltp if last_price is None else last_price
        d = out.setdefault(expiry, {})
        s = d.setdefault(strike, {'CE':None,'PE':None})
        leg = {'iv': iv or 0.0, 'delta': delta or 0.0, 'vol': int(vol or 0), 'oi': int(oi or 0)}
        s[side] = leg
    return out, last_price

def nearest_strikes(sorted_strikes, ltp, k):
    # return k closest strikes overall (CE/PE sit on same strike price)
    return sorted(sorted_strikes, key=lambda x: abs(x - (ltp or x)))[:k]

def compute_features_for_exp(expiry_map, ltp, k):
    if not expiry_map:
        return None
    strikes = sorted(expiry_map.keys())
    if not strikes:
        return None
    # ATM = strike with min distance to LTP
    atm = min(strikes, key=lambda s: abs(s - (ltp or s)))
    # window
    wins = nearest_strikes(strikes, ltp, k)

    def safe(o): return int(o or 0)
    def safe_f(x): return float(x or 0.0)

    ce_oi = sum(safe(expiry_map[s]['CE']['oi']) for s in wins if expiry_map[s]['CE'])
    pe_oi = sum(safe(expiry_map[s]['PE']['oi']) for s in wins if expiry_map[s]['PE'])
    ce_vol= sum(safe(expiry_map[s]['CE']['vol']) for s in wins if expiry_map[s]['CE'])
    pe_vol= sum(safe(expiry_map[s]['PE']['vol']) for s in wins if expiry_map[s]['PE'])

    atm_ce = expiry_map.get(atm,{}).get('CE')
    atm_pe = expiry_map.get(atm,{}).get('PE')
    iv_atm_ce = safe_f(atm_ce and atm_ce.get('iv'))
    iv_atm_pe = safe_f(atm_pe and atm_pe.get('iv'))
    atm_delta_ce = safe_f(atm_ce and atm_ce.get('delta'))
    atm_delta_pe = safe_f(atm_pe and atm_pe.get('delta'))

    pcr = (pe_oi / (ce_oi if ce_oi>0 else 1))
    iv_skew = iv_atm_pe - iv_atm_ce

    return {
        'atm': float(atm), 'window_n': int(k),
        'ce_oi': ce_oi, 'pe_oi': pe_oi, 'pcr': float(pcr),
        'ce_vol': ce_vol, 'pe_vol': pe_vol,
        'iv_atm_ce': iv_atm_ce, 'iv_atm_pe': iv_atm_pe,
        'iv_skew': iv_skew, 'atm_delta_ce': atm_delta_ce, 'atm_delta_pe': atm_delta_pe
    }

def upsert_feature(conn, ts, und, expiry, ltp, feat):
    sql = '''INSERT INTO option_chain_features
             (ts_fetch_utc, underlying, expiry, last_price, atm_strike, window_n,
              ce_oi_sum, pe_oi_sum, pcr_oi, ce_vol_sum, pe_vol_sum,
              iv_atm_ce, iv_atm_pe, iv_skew, atm_delta_ce, atm_delta_pe)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
             ON CONFLICT(ts_fetch_utc, underlying, expiry) DO UPDATE SET
               last_price=excluded.last_price,
               atm_strike=excluded.atm_strike,
               window_n=excluded.window_n,
               ce_oi_sum=excluded.ce_oi_sum,
               pe_oi_sum=excluded.pe_oi_sum,
               pcr_oi=excluded.pcr_oi,
               ce_vol_sum=excluded.ce_vol_sum,
               pe_vol_sum=excluded.pe_vol_sum,
               iv_atm_ce=excluded.iv_atm_ce,
               iv_atm_pe=excluded.iv_atm_pe,
               iv_skew=excluded.iv_skew,
               atm_delta_ce=excluded.atm_delta_ce,
               atm_delta_pe=excluded.atm_delta_pe
          '''
    conn.execute(sql, (ts, und, expiry, float(ltp or 0.0),
                       feat['atm'], feat['window_n'],
                       feat['ce_oi'], feat['pe_oi'], feat['pcr'],
                       feat['ce_vol'], feat['pe_vol'],
                       feat['iv_atm_ce'], feat['iv_atm_pe'], feat['iv_skew'],
                       feat['atm_delta_ce'], feat['atm_delta_pe']))

def main(window_k):
    DB.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB) as conn:
        cur = conn.cursor()
        for und in ('NIFTY','BANKNIFTY'):
            ts = get_latest_ts_by_underlying(cur, und)
            if not ts: 
                print(f'{und}: no data yet, skipping'); 
                continue
            # map: expiry -> { strike -> {'CE':{..},'PE':{..}} }
            all_map, ltp_any = fetch_rows(cur, ts, und)
            for expiry, strikes_map in all_map.items():
                feat = compute_features_for_exp(strikes_map, ltp_any, window_k)
                if not feat: 
                    print(f'{und} {expiry}: no strikes, skip'); 
                    continue
                upsert_feature(conn, ts, und, expiry, ltp_any, feat)
                print(f'[feat] {und} {expiry} ts={ts} | ATM={feat["atm"]} PCR={feat["pcr"]:.2f} ivCE={feat["iv_atm_ce"]:.2f} ivPE={feat["iv_atm_pe"]:.2f}')
        conn.commit()

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--window', type=int, default=10, help='number of nearest strikes (overall) used for sums')
    args = ap.parse_args()
    main(args.window)
