# -*- coding: utf-8 -*-
import sys
from pathlib import Path

def init_runtime():
    ROOT = Path(r"C:\teevra18")
    APP  = ROOT / "app"
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    if str(APP) not in sys.path:
        sys.path.insert(0, str(APP))
    # Load merged env (root .env + config\.env)
    try:
        from common.env import load_environment
        load_environment()
    except Exception as e:
        print(f"[WARN] env bootstrap failed: {e}")
