import json, os, sqlite3, sys
from pathlib import Path
cfgp = Path(r"C:\teevra18\config\orchestrator.config.json")
ok = True; msgs=[]
if not cfgp.exists():
    ok=False; msgs.append("config missing")
else:
    cfg=json.loads(cfgp.read_text())
    db = cfg["core"]["db"]
    mx = cfg["core"]["matrix"]
    if not os.path.exists(db): ok=False; msgs.append(f"db missing: {db}")
    if not os.path.exists(mx): ok=False; msgs.append(f"matrix missing: {mx}")
    # minimal DB sanity
    try:
        con=sqlite3.connect(db); cur=con.cursor()
        cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='signals'")
        if cur.fetchone() is None: ok=False; msgs.append("table 'signals' missing")
        else:
            cur.execute("PRAGMA table_info(signals)")
            cols={r[1] for r in cur.fetchall()}
            need={'id','ts','symbol','driver','action','rr','sl','tp','state'}
            missing=need-cols
            if missing: ok=False; msgs.append("signals missing cols: "+",".join(sorted(missing)))
        con.close()
    except Exception as e:
        ok=False; msgs.append("db error: "+str(e))
print(json.dumps({"ready": ok, "messages": msgs}))
sys.exit(0 if ok else 2)
