# C:\teevra18\services\svc_ingest_dhan.py

# --- make sure Python can see our project root ---
from common.bootstrap import init_runtime
init_runtime()
import sys, os
PROJECT_ROOT = r"C:\teevra18"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# --- env loading ---
from dotenv import load_dotenv

# Load .env from project root explicitly
load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, ".env"))

# --- now local imports work ---
from teevra.dhan_ws import Ingestor
from teevra.db import log, put_health

def main():
    log("INFO", "ingest", "M1 starting (router-aware)")
    put_health("m1_status", "boot")
    ing = Ingestor()
    try:
        ing.run()
    except KeyboardInterrupt:
        pass
    finally:
        ing.stop()
        log("INFO", "ingest", "M1 stopped")

if __name__ == "__main__":
    main()
