# -*- coding: utf-8 -*-
import os, sys
from pathlib import Path

ROOT = Path(r"C:\teevra18")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.env import load_environment

def main():
    load_environment()
    print("DHAN_CLIENT_ID =", os.environ.get("DHAN_CLIENT_ID"))
    print("DHAN_API_KEY   =", os.environ.get("DHAN_API_KEY"))
    print("DHAN_ACCESS_TOKEN set? ", bool(os.environ.get("DHAN_ACCESS_TOKEN")))
    print("DATA_DIR       =", os.environ.get("DATA_DIR"))
    print("DB_PATH        =", os.environ.get("DB_PATH"))
    print("LOG_DIR        =", os.environ.get("LOG_DIR"))

if __name__ == "__main__":
    main()
