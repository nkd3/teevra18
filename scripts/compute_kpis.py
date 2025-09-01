# C:\teevra18\scripts\compute_kpis.py
import json, sqlite3, sys, math
from pathlib import Path
import statistics as stats

CFG = json.loads(Path(r"C:\teevra18\teevra18.config.json").read_text(encoding="utf-8"))
DB = CFG["db_path"]

USAGE = """
Usage:
  python C:\\teevra18\\scripts\\compute_kpis.py <stage> <config_id> "<label>"
Example:
  python C:\\teevra18\\scripts\\compute_kpis.py Backtest 1 "BT_2025-08-30"
"""

def kpis_from_pnls(pnls):
    trades = len(pnls)
    if trades == 0:
        return dict(trades=0, win_rate=0, profit_factor=0, avg_trade=0, expectancy=0, max_dd_pct=0,
                    gp=0, gl=0, net=0)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gp = sum(wins) if wins else 0.0
    gl = sum(losses) if losses else 0.0
    net = sum(pnls)
    wr = (len(wins) / trades) * 100.0
    pf = (gp / abs(gl)) if gl < 0 else (float('inf') if gp > 0 else 0.0)
    avg = net / trades
    exp = ((wr/100.0) * (gp/len(wins) if wins else 0.0)) + (((1-wr/100.0)) * (abs(gl)/len(losses) if losses else 0.0) * -1)
    # Max drawdown on equity curve
    curve = []
    cum = 0.0
    for p in pnls:
        cum += p
        curve.append(cum)
    peak = -math.inf
    max_dd = 0.0
    for v in curve:
        if v > peak: peak = v
        dd = peak - v
        if dd > max_dd: max_dd = dd
    # pct drawdown: we need capital; use fixed capital if available
    # We'll convert to % later in UI by dividing with policies.fixed_capital
    return dict(trades=trades, win_rate=wr, profit_factor=pf, avg_trade=avg, expectancy=exp,
                max_dd_abs=max_dd, gp=gp, gl=gl, net=net)

def main():
    if len(sys.argv) < 4:
        print(USAGE); sys.exit(1)
    stage = sys.argv[1]; cid = int(sys.argv[2]); label = sys.argv[3]
    con = sqlite3.connect(DB); cur = con.cursor()
    cur.execute("SELECT pnl FROM exec_trades WHERE stage=? AND config_id=? ORDER BY id", (stage, cid))
    pnls = [r[0] for r in cur.fetchall()]
    if not pnls:
        print(f"No trades found for stage={stage}, config_id={cid}")
        sys.exit(2)
    k = kpis_from_pnls(pnls)
    # store summary (max_drawdown_pct conversion deferred; store 0 for now, compute in UI with policy cap)
    cur.execute("""INSERT INTO kpi_summary(stage, config_id, label, trades_count, win_rate, profit_factor, avg_trade, expectancy, max_drawdown_pct, gross_profit, gross_loss, net_pnl)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (stage, cid, label, k["trades"], k["win_rate"], k["profit_factor"], k["avg_trade"], k["expectancy"], 0.0, k["gp"], k["gl"], k["net"]))
    con.commit(); con.close()
    print(f"KPIs computed for {stage} config {cid} label {label}")
    print(f"Trades={k['trades']}  WR={k['win_rate']:.2f}%  PF={k['profit_factor']:.2f}  Net={k['net']:.2f}  MDD(abs)={k['max_dd_abs']:.2f}")

if __name__ == "__main__":
    main()
