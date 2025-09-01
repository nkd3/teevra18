import sqlite3, os
from pathlib import Path

DB_PATH = Path(os.getenv('DB_PATH', r'C:\teevra18\data\teevra18.db'))

def pct(n, d):
    if not d:
        return 0.0
    return (n * 100.0) / d

with sqlite3.connect(DB_PATH) as c:
    cur = c.cursor()

    # Overall latest fetch (for info only)
    ts_overall = cur.execute('SELECT MAX(ts_fetch_utc) FROM option_chain_snap').fetchone()[0]
    print('Overall latest ts_fetch_utc:', ts_overall)

    for und in ('NIFTY','BANKNIFTY'):
        # Latest per underlying
        ts_u = cur.execute('SELECT MAX(ts_fetch_utc) FROM option_chain_snap WHERE underlying=?', (und,)).fetchone()[0]
        if not ts_u:
            print(f'{und}: no rows yet.')
            continue

        total = cur.execute('SELECT COUNT(*) FROM option_chain_snap WHERE ts_fetch_utc=? AND underlying=?',
                            (ts_u,und)).fetchone()[0]
        zero_g = cur.execute('''SELECT COUNT(*) FROM option_chain_snap
                                 WHERE ts_fetch_utc=? AND underlying=?
                                   AND IFNULL(delta,0)=0 AND IFNULL(gamma,0)=0 AND IFNULL(vega,0)=0''',
                             (ts_u,und)).fetchone()[0]
        zero_iv = cur.execute('''SELECT COUNT(*) FROM option_chain_snap
                                 WHERE ts_fetch_utc=? AND underlying=?
                                   AND (implied_volatility IS NULL OR implied_volatility=0)''',
                              (ts_u,und)).fetchone()[0]

        print(f'{und} | ts={ts_u} | total={total}  zero_greeks={zero_g} ({pct(zero_g,total):.1f}%)  zero_IV={zero_iv} ({pct(zero_iv,total):.1f}%)')

        if total>0 and pct(zero_g,total)>30:
            print(f'WARN: {und} has >30% zero greeks in latest fetch. Check provider or filters.')
