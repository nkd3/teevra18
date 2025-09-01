# C:\teevra18\tools\validate_m7_acceptance.py
import sqlite3, sys, datetime
from pathlib import Path

DB = Path(r"C:\teevra18\data\teevra18.db")
MIN_RR = 2.0
MAX_SL = 1000.0
MAX_TRADES = 5

def today():
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")

def main():
    with sqlite3.connect(DB) as conn:
        rows = conn.execute("""
            SELECT id, ts_utc, security_id, symbol, group_name, strategy_id, side,
                   entry, stop, target, rr, sl_per_lot, deterministic_hash, created_at_utc
            FROM signals
            WHERE substr(created_at_utc,1,10) = ?
            ORDER BY created_at_utc
        """, (today(),)).fetchall()

        ok_rr = all((r[10] or 0) >= MIN_RR for r in rows)
        ok_sl = all((r[11] or 0) <= MAX_SL for r in rows)
        ok_cnt = len(rows) <= MAX_TRADES
        uniq = len({(r[1], r[2], r[5], r[6], r[12]) for r in rows}) == len(rows)

        print(f"Signals today: {len(rows)}")
        print(f"R:R >= {MIN_RR}: {'PASS' if ok_rr else 'FAIL'}")
        print(f"SL/lot <= {MAX_SL}: {'PASS' if ok_sl else 'FAIL'}")
        print(f"<= {MAX_TRADES} trades/day: {'PASS' if ok_cnt else 'FAIL'}")
        print(f"Determinism (unique key sets): {'PASS' if uniq else 'FAIL'}")
        all_ok = ok_rr and ok_sl and ok_cnt and uniq
        print('--- OVERALL:', 'M7 PASS ✅' if all_ok else 'M7 FAIL ❌', '---')
        if not all_ok:
            # Show failing rows for quick diagnostics
            for r in rows:
                if (r[10] or 0) < MIN_RR or (r[11] or 0) > MAX_SL:
                    print('  RowFail:', dict(
                        ts_utc=r[1], sid=r[2], sym=r[3], grp=r[4], strat=r[5], side=r[6],
                        entry=r[7], stop=r[8], target=r[9], rr=r[10], sl_per_lot=r[11], hash=r[12]
                    ))
            sys.exit(1)

if __name__ == "__main__":
    main()
