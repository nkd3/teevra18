# C:\teevra18\services\svc-strategy-core.py
import sys, subprocess

py     = r"C:\teevra18\.venv\Scripts\python.exe"
target = r"C:\teevra18\services\strategy\svc_strategy_core.py"

# Only forward flags the real core supports
allowed = {"--dry-run","--max-signals"}
fwd = []
i = 1
while i < len(sys.argv):
    a = sys.argv[i]
    k = a.split("=")[0]
    if k in allowed:
        fwd.append(a)
        # pass the value if it's of the form "--max-signals 5"
        if k == "--max-signals" and "=" not in a and i+1 < len(sys.argv):
            fwd.append(sys.argv[i+1]); i += 1
    i += 1

sys.exit(subprocess.call([py, target, "generate"] + fwd))
