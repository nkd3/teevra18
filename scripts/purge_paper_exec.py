# -*- coding: utf-8 -*-
import json, sqlite3
from pathlib import Path
CFG = json.loads(Path(r"C:\teevra18\teevra18.config.json").read_text(encoding="utf-8"))
con = sqlite3.connect(CFG["db_path"]); cur = con.cursor()
cur.execute("DELETE FROM exec_trades WHERE stage='Paper' AND config_id=?", (2,))
con.commit(); print("Purged exec_trades rows for Paper config_id=2.")
con.close()
