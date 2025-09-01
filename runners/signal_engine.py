import time
from core.ops import should_continue, heartbeat
from core.cfg import get_db_path

DB = get_db_path()
RUNNER = "signal-engine"  # must match name in ops.runners to light up correctly

def compute_signals_once() -> int:
    # TODO: implement your actual signal logic; return number of signals computed
    time.sleep(0.3)
    return 3

if __name__ == "__main__":
    try:
        while True:
            if not should_continue(DB, RUNNER, idle_sleep=1.0):
                break  # PANIC -> exit
            c = compute_signals_once()
            heartbeat(DB, RUNNER, "RUNNING", f"signals={c}")
    except Exception as e:
        heartbeat(DB, RUNNER, "ERROR", f"{type(e).__name__}: {e}")
        raise