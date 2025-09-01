# C:\teevra18\scripts\load_trades_csv.py
import csv, json, sqlite3, sys
from pathlib import Path

# Bootstrap path for core if needed
PROJECT_ROOT = Path(r"C:\teevra18")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

CFG = json.loads(Path(r"C:\teevra18\teevra18.config.json").read_text(encoding="utf-8"))
DB = CFG["db_path"]

USAGE = """
Usage:
  python C:\\teevra18\\scripts\\load_trades_csv.py <csv_path>
Fields required:
  stage,config_id,trade_id,side,qty_lots,entry_time,exit_time,entry_price,exit_price,fees,pnl
"""

def main():
    if len(sys.argv) < 2:
        print(USAGE); sys.exit(1)
    csv_path = Path(sys.argv[1])
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}"); sys.exit(2)
    con = sqlite3.connect(DB); cur = con.cursor()
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        rows = [(r["stage"], int(r["config_id"]), r["trade_id"], r["side"], int(r["qty_lots"]),
                 r["entry_time"], r["exit_time"], float(r["entry_price"]), float(r["exit_price"]),
                 float(r.get("fees", 0.0)), float(r["pnl"]))
                for r in rdr]
    cur.executemany("""INSERT INTO exec_trades(stage,config_id,trade_id,side,qty_lots,entry_time,exit_time,entry_price,exit_price,fees,pnl)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""", rows)
    con.commit(); con.close()
    print(f"Loaded {len(rows)} trades from {csv_path}")

if __name__ == "__main__":
    main()
