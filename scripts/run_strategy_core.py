import os, sys, subprocess, pathlib

ROOT = pathlib.Path(r"C:\teevra18")
DB   = ROOT / r"data\teevra18.db"
MATR = ROOT / r"models\m7\strategy_matrix.json"
LOG  = ROOT / r"logs\strategy-core.log"
CORE = ROOT / r"services\svc-strategy-core.py"

args = [
    sys.executable, str(CORE),
    "--db", str(DB),
    "--universe", "indices+equities",
    "--rr_profile", "BASELINE",
    "--matrix", str(MATR),
    "--log", str(LOG)
]
print("[RUN]", " ".join(map(str, args)))
sys.exit(subprocess.call(args))
