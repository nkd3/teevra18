# C:\teevra18\scripts\promote_config.py
import sys, json
from pathlib import Path

# --- Bootstrap so Python can find C:\teevra18\core ---
PROJECT_ROOT = Path(r"C:\teevra18")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# ----------------------------------------------------

from core.config_store import ConfigStore

CFG = json.loads(Path(r"C:\teevra18\teevra18.config.json").read_text(encoding="utf-8"))
STORE = ConfigStore(CFG["db_path"])

USAGE = """
Usage:
  python C:\\teevra18\\scripts\\promote_config.py <config_id> <target_stage> "<label>"
Example:
  python C:\\teevra18\\scripts\\promote_config.py 3 "Paper" "Passed BT KPI 2025-08-30"
"""

def main():
    if len(sys.argv) < 4:
        print(USAGE); sys.exit(1)
    cid = int(sys.argv[1]); target = sys.argv[2]; label = sys.argv[3]
    bundle = STORE.get_config_bundle(cid)
    if target not in ("Backtest","Paper","Live-Ready"):
        print("Invalid target stage."); sys.exit(2)
    new_name = f"{bundle['meta']['name']}@{target}"
    new_id = STORE.import_snapshot(stage=target, name=new_name, snapshot=bundle, notes=f"Promotion: {label}")
    print(f"Promoted {cid} -> {new_id} ({target})")
    # snapshot on source as well
    STORE.snapshot(cid, f"promoted-to:{target}", bundle)
    print("Snapshot saved.")

if __name__ == "__main__":
    main()
