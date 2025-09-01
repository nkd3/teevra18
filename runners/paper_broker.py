import time
from core.ops import should_continue, heartbeat
from core.cfg import get_db_path

DB = get_db_path()
RUNNER = "paper-broker"  # must match name in ops.runners

def place_paper_orders_once() -> int:
    # TODO: implement your paper order placement; return number of orders placed
    time.sleep(0.25)
    return 1

if __name__ == "__main__":
    try:
        while True:
            if not should_continue(DB, RUNNER, idle_sleep=1.0):
                break  # PANIC -> exit
            k = place_paper_orders_once()
            heartbeat(DB, RUNNER, "RUNNING", f"paper_orders={k}")
    except Exception as e:
        heartbeat(DB, RUNNER, "ERROR", f"{type(e).__name__}: {e}")
        raise