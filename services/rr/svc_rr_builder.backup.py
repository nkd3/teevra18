# C:\teevra18\services\rr\svc_rr_builder.py
import os
import sqlite3
import logging
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

import sys
from pathlib import Path
PROJECT_ROOT = Path(r"C:\teevra18")
if str(PROJECT_ROOT / "lib") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "lib"))

from t18_db_helpers import t18_fetch_lot_size

# --- Setup ---
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", r"C:\teevra18"))
DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
LOG_PATH = Path(os.getenv("LOG_DIR", r"C:\teevra18\logs")) / "rr_builder.log"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

MAX_SL_PER_LOT = float(os.getenv("MAX_SL_PER_LOT", 1000))
RR_MIN = float(os.getenv("RR_MIN", 2.0))

logging.basicConfig(filename=LOG_PATH, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

REQUIRED_COLS = {
    "sl_price": "REAL",
    "tp_price": "REAL",
    "rr_ratio": "REAL",
    "rr_validated": "INTEGER",
    "rr_reject_reason": "TEXT"
}

OPTIONAL_COLS = {
    "direction": "TEXT",
    "entry_price": "REAL",
    "lot_size": "REAL"
}

CANDIDATE_KEYS = ["signal_id", "id", "sig_id", "uuid", "guid", "nonce"]

def ensure_columns(conn, table, required, optional):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,))
    if not cur.fetchone():
        raise RuntimeError(f"Table '{table}' not found.")

    cur.execute(f"PRAGMA table_info({table});")
    existing = {row[1] for row in cur.fetchall()}

    for c, t in required.items():
        if c not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {c} {t};")
            logging.info(f"Added missing column {c} {t}")

    for c, t in optional.items():
        if c not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {c} {t};")
            logging.info(f"Added optional column {c} {t}")

    conn.commit()

def detect_key_column(conn, table="signals"):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    cols = [row[1] for row in cur.fetchall()]
    # try common names first
    for name in CANDIDATE_KEYS:
        if name in cols:
            return name, False  # not rowid
    # fall back to rowid
    return "rowid", True

def validate_signal(sig, rr_profile):
    """
    Validates one signal and returns (bands_dict, reject_reason_or_None).

    Prefers schema: side, entry, stop, target, rr, sl_per_lot.
    Falls back to: direction, entry_price, lot_size (price_risk computed).
    """
    EPS = 1e-9  # float tolerance
    rr_min = rr_profile.get("rr_min", RR_MIN)
    sl_cap = rr_profile.get("sl_cap_per_lot", MAX_SL_PER_LOT)

    # ---------- PATH A: use base columns if present ----------
    if all(k in sig and pd.notna(sig[k]) for k in ("side", "entry", "stop", "target")):
        side = str(sig["side"]).upper()
        entry = float(sig["entry"])
        stop = float(sig["stop"])
        target = float(sig["target"])

        # compute rr if column missing
        if "rr" in sig and pd.notna(sig["rr"]):
            rr_ratio = float(sig["rr"])
        else:
            denom = (entry - stop)
            rr_ratio = abs((target - entry) / denom) if denom != 0 else 0.0

        # sl per lot: either provided or compute from distance * lot_size if available
        if "sl_per_lot" in sig and pd.notna(sig["sl_per_lot"]):
            sl_per_lot = float(sig["sl_per_lot"])
        else:
            # compute if we can
            lot = float(sig["lot_size"]) if "lot_size" in sig and pd.notna(sig["lot_size"]) else 1.0
            sl_per_lot = lot * abs(entry - stop)

        # sanity checks
        if sl_per_lot > sl_cap + EPS:
            return None, f"SL exceeds {sl_cap}/lot"

        if rr_ratio + EPS < rr_min:
            return None, f"RR {rr_ratio:.2f} < {rr_min}"

        # accept and map to M8 output columns
        return {
            "sl_price": stop,          # map stop -> sl_price
            "tp_price": target,        # map target -> tp_price
            "rr_ratio": rr_ratio       # store rr
        }, None

    # ---------- PATH B: fallback to direction/entry_price/lot_size ----------
    for f in ("direction", "entry_price", "lot_size"):
        if f not in sig or pd.isna(sig.get(f)):
            return None, f"missing_field:{f}"

    entry = float(sig["entry_price"])
    lots = max(1.0, float(sig["lot_size"]))
    direction = str(sig["direction"]).upper()

    # risk per lot in price terms (₹ per lot cap converted to price distance)
    price_risk = min(sl_cap, MAX_SL_PER_LOT) / lots

    if direction == "LONG":
        sl_price = entry - price_risk
        tp_price = entry + (price_risk * rr_min)
    elif direction == "SHORT":
        sl_price = entry + price_risk
        tp_price = entry - (price_risk * rr_min)
    else:
        return None, f"bad_direction:{direction}"

    # cap check
    if lots * abs(entry - sl_price) > sl_cap + EPS:
        return None, f"SL exceeds {sl_cap}/lot"

    denom = (entry - sl_price)
    rr_ratio = abs((tp_price - entry) / denom) if denom != 0 else 0.0

    if rr_ratio + EPS < rr_min:
        return None, f"RR {rr_ratio:.2f} < {rr_min}"

    return {"sl_price": sl_price, "tp_price": tp_price, "rr_ratio": rr_ratio}, None


def run_once():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Auto-migrate required/optional columns if missing
    ensure_columns(conn, "signals", REQUIRED_COLS, OPTIONAL_COLS)

    pk_col, is_rowid = detect_key_column(conn, "signals")
    logging.info(f"signals key column -> {pk_col} (rowid_fallback={is_rowid})")

    # Select pending signals, include rowid if needed
    select_pk = f"{pk_col} AS pk" if not is_rowid else "rowid AS pk"

    q = f"""
    SELECT {select_pk}, *
    FROM signals
    WHERE rr_validated IS NULL
    ORDER BY COALESCE(ts_utc, CURRENT_TIMESTAMP) DESC
    LIMIT 500
    """
    sigs = pd.read_sql(q, conn)

    if sigs.empty:
        logging.info("No pending signals for RR validation.")
        conn.close()
        return

    for _, sig in sigs.iterrows():
        rr_profile = {"sl_cap_per_lot": MAX_SL_PER_LOT, "rr_min": RR_MIN}

        bands, reason = validate_signal(sig, rr_profile)
        if bands:
            conn.execute(
                "UPDATE signals SET sl_price=?, tp_price=?, rr_ratio=?, rr_validated=1, rr_reject_reason=NULL WHERE rowid=?" if is_rowid
                else f"UPDATE signals SET sl_price=?, tp_price=?, rr_ratio=?, rr_validated=1, rr_reject_reason=NULL WHERE {pk_col}=?",
                (bands["sl_price"], bands["tp_price"], bands["rr_ratio"], sig["pk"])
            )
            logging.info(f"Signal {sig['pk']} VALID: RR={bands['rr_ratio']:.2f}")
        else:
            conn.execute(
                "UPDATE signals SET rr_validated=0, rr_reject_reason=? WHERE rowid=?" if is_rowid
                else f"UPDATE signals SET rr_validated=0, rr_reject_reason=? WHERE {pk_col}=?",
                (reason, sig["pk"])
            )
            logging.warning(f"Signal {sig['pk']} REJECT: {reason}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    run_once()
