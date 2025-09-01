# C:\teevra18\scripts\list_configs.py
import json, sqlite3
from pathlib import Path

CFG = json.loads(Path(r"C:\teevra18\teevra18.config.json").read_text(encoding="utf-8"))
DB = CFG["db_path"]
con = sqlite3.connect(DB); cur = con.cursor()
for stage in ("Backtest","Paper","Live-Ready"):
    print(f"\n== {stage} ==")
    for row in cur.execute("SELECT id,name,stage,version,is_active,notes FROM strategy_configs WHERE stage=? ORDER BY id", (stage,)):
        print(f"ID={row[0]} | {row[1]} v{row[3]} | active={bool(row[4])} | notes={row[5]}")
con.close()
