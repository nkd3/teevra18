# C:\teevra18\services\paper_trader\m9_worker.py
import sqlite3, argparse, time
from datetime import datetime, timedelta

# ----------------- Charges model & helper -----------------
class ChargesModel:
    def __init__(self, brokerage_per_order=20.0, gst_rate=0.18, stt_sell_rate=0.001,
                 exch_txn_rate=0.0003503, sebi_rate=0.000001, stamp_buy_rate=0.00003):
        self.brokerage = brokerage_per_order
        self.gst = gst_rate
        self.stt = stt_sell_rate
        self.exch = exch_txn_rate
        self.sebi = sebi_rate
        self.stamp = stamp_buy_rate

def estimate_roundtrip_charges(entry, exit_, qty, cm: ChargesModel):
    buy_turnover  = float(entry) * int(qty)
    sell_turnover = max(float(exit_), 0.0) * int(qty)
    brokerage = cm.brokerage * 2.0
    exch = cm.exch * (buy_turnover + sell_turnover)
    sebi = cm.sebi * (buy_turnover + sell_turnover)
    stt  = cm.stt * sell_turnover
    stamp= cm.stamp * buy_turnover
    gst  = cm.gst * (brokerage + exch + sebi)
    return brokerage + exch + sebi + stt + stamp + gst

CHARGES = ChargesModel()
DB = r"C:\teevra18\data\teevra18.db"

# ----------------- Utilities -----------------
def view_exists(conn, name: str) -> bool:
    r = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('view','table') AND name=?", (name,)).fetchone()
    return bool(r)

def get_ltp(conn: sqlite3.Connection, option_symbol: str, ts_after_iso: str):
    # Optional LTP source; if absent, we fallback to entry
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ltp_cache';").fetchone()
    if not row:
        return None
    row = conn.execute("""
        SELECT ltp FROM ltp_cache
        WHERE option_symbol=? AND ts_utc >= ?
        ORDER BY ts_utc ASC
        LIMIT 1
    """, (option_symbol, ts_after_iso)).fetchone()
    return float(row[0]) if row else None

def _opslog_columns(conn):
    return {r[1] for r in conn.execute("PRAGMA table_info(ops_log);").fetchall()}

def log(conn, level: str, event: str, ref_table: str, ref_id: int, message: str):
    """
    Compatible with legacy ops_log (ts_utc, level, area, msg are NOT NULL)
    and modern columns (source, event, ref_table, ref_id, message).
    """
    cols = _opslog_columns(conn)
    col_list, placeholders, values = [], [], []

    # Many legacy schemas had ts_utc NOT NULL without default -> set explicitly
    col_list.append("ts_utc"); placeholders.append("datetime('now')")

    if "level" in cols: col_list.append("level"); placeholders.append("?"); values.append(level)
    if "area"  in cols: col_list.append("area");  placeholders.append("?"); values.append("M9")
    if "msg"   in cols: col_list.append("msg");   placeholders.append("?"); values.append(f"{event}: {message}")

    if "source" in cols:    col_list.append("source");    placeholders.append("?"); values.append("M9")
    if "event" in cols:     col_list.append("event");     placeholders.append("?"); values.append(event)
    if "ref_table" in cols: col_list.append("ref_table"); placeholders.append("?"); values.append(ref_table)
    if "ref_id" in cols:    col_list.append("ref_id");    placeholders.append("?"); values.append(ref_id)
    if "message" in cols:   col_list.append("message");   placeholders.append("?"); values.append(message)

    sql = f"INSERT INTO ops_log ({', '.join(col_list)}) VALUES ({', '.join(placeholders)})"
    conn.execute(sql, values)

# ----------------- Data access -----------------
def fetch_ready_signals(conn, limit: int):
    if not view_exists(conn, "v_signals_ready_for_m9"):
        raise RuntimeError("View v_signals_ready_for_m9 not found. Create it before running M9.")
    rows = conn.execute("""
        SELECT id, signal_id, option_symbol, underlying_root, side,
               entry_price, sl_points, tp_points, lot_size, lots, ts_utc
        FROM v_signals_ready_for_m9
        ORDER BY ts_utc ASC
        LIMIT ?
    """, (limit,)).fetchall()
    return rows

def _map_side_for_paper_orders(signals_side: str) -> str:
    """
    signals.side is LONG/SHORT (from M8). paper_orders has CHECK side IN ('BUY','SELL').
    Map: LONG->BUY, SHORT->SELL. If already BUY/SELL, keep it; default BUY.
    """
    s = (signals_side or "").upper()
    if s == "LONG": return "BUY"
    if s == "SHORT": return "SELL"
    if s in ("BUY", "SELL"): return s
    return "BUY"

# ----------------- Order lifecycle -----------------
def create_paper_order(conn, sig):
    """
    Insert a new paper order from a validated signal.
    Supports BUY (LONG) and SELL (SHORT) by setting correct SL/TP for option premium.
    """
    po_side = _map_side_for_paper_orders(sig["side"])

    entry_price = float(sig["entry_price"])
    sl_pts = float(sig["sl_points"])
    tp_pts = float(sig["tp_points"])

    # For OPTIONS premium:
    # BUY:  SL = entry - sl_pts;  TP = entry + tp_pts
    # SELL: SL = entry + sl_pts;  TP = entry - tp_pts
    if po_side == "BUY":
        sl_price = max(entry_price - sl_pts, 0.0)
        tp_price = max(entry_price + tp_pts, 0.0)
    else:  # SELL (SHORT)
        sl_price = max(entry_price + sl_pts, 0.0)
        tp_price = max(entry_price - tp_pts, 0.0)

    # Normalize ts, store with space (not 'T') so SQLite datetime() can parse
    base_ts = (sig["ts_utc"] or "").replace("T", " ")
    delayed_fill_at = (datetime.fromisoformat(base_ts) + timedelta(seconds=7)).strftime("%Y-%m-%d %H:%M:%S")

    rrj_row = conn.execute("SELECT rr_metrics_json FROM signals WHERE id=?", (sig["id"],)).fetchone()
    rrj = rrj_row[0] if rrj_row else None

    qty = int(sig["lot_size"]) * int(sig["lots"])

    conn.execute("""
        INSERT INTO paper_orders (
          -- legacy columns (compat)
          signal_id, ts_signal, symbol, side, qty, entry, sl, tp, status,
          -- new M9 columns
          signal_row_id, option_symbol, underlying_root,
          lots, lot_size, state,
          entry_price, sl_price, tp_price, delayed_fill_at, rr_metrics_json
        )
        VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN',
          ?, ?, ?,
          ?, ?, 'PENDING_DELAY',
          ?, ?, ?, ?, ?
        )
    """, (
        sig["signal_id"], sig["ts_utc"], sig["option_symbol"], po_side, qty, entry_price, sl_price, tp_price,
        sig["id"], sig["option_symbol"], sig["underlying_root"],
        int(sig["lots"]), int(sig["lot_size"]),
        entry_price, sl_price, tp_price, delayed_fill_at, rrj
    ))
    oid = conn.execute("SELECT last_insert_rowid();").fetchone()[0]
    log(conn, "INFO", "CREATE", "paper_orders", oid,
        f"Created order from signal {sig['signal_id']} (+7s at {delayed_fill_at})")
    return oid

def due_fill_ids(conn):
    # Use datetime() to parse stored string and be robust to format
    return conn.execute("""
        SELECT id FROM paper_orders
        WHERE state='PENDING_DELAY' AND datetime('now') >= datetime(delayed_fill_at)
        ORDER BY id ASC
    """).fetchall()

def filled_ids(conn):
    return conn.execute("SELECT id FROM paper_orders WHERE state='FILLED'").fetchall()

def try_fill_order(conn, order_id: int):
    po = conn.execute("SELECT * FROM paper_orders WHERE id=?", (order_id,)).fetchone()
    if not po or po["state"] != "PENDING_DELAY":
        return
    fill_after = po["delayed_fill_at"] or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    ltp = get_ltp(conn, po["option_symbol"], fill_after)
    fill_price = float(ltp) if ltp is not None else float(po["entry_price"])

    # ----------------- Slippage Guard (NEW) -----------------
    # Abort fills that are unrealistically far from planned entry.
    # Default: 30% threshold; tune as needed or make profile-based later.
    entry = float(po["entry_price"])
    po_side = (po["side"] or "").upper()  # BUY/SELL
    max_slip_pct = 0.30  # 30% allowed move vs entry

    # BUY should not fill too far ABOVE entry; SELL should not fill too far BELOW entry.
    if (po_side == "BUY"  and fill_price > entry * (1 + max_slip_pct)) or \
       (po_side == "SELL" and fill_price < entry * (1 - max_slip_pct)):
        conn.execute("UPDATE paper_orders SET state='ARCHIVED', notes='fill_slippage_exceeded' WHERE id=?", (order_id,))
        log(conn, "WARN", "ARCHIVE", "paper_orders", order_id,
            f"Aborted fill due to slippage [{po_side}] fill={fill_price:.2f} vs entry={entry:.2f} (>{max_slip_pct*100:.0f}% threshold)")
        return
    # --------------------------------------------------------

    conn.execute("""
        UPDATE paper_orders
        SET state='FILLED', fill_price=?, filled_ts_utc=datetime('now')
        WHERE id=?
    """, (fill_price, order_id))
    origin = "ltp_cache" if ltp is not None else "entry_fallback"
    log(conn, "INFO", "FILL", "paper_orders", order_id, f"Filled at {fill_price} ({origin})")

def check_and_close(conn, order_id: int):
    po = conn.execute("SELECT * FROM paper_orders WHERE id=?", (order_id,)).fetchone()
    if not po or po["state"] != "FILLED":
        return

    # read a "current" price; without LTP feed we use fill_price so it won't move
    now_iso = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    ltp = get_ltp(conn, po["option_symbol"], now_iso)
    last = float(ltp) if ltp is not None else float(po["fill_price"])

    po_side = (po["side"] or "").upper()  # BUY or SELL
    hit_event, exit_price = None, None

    if po_side == "BUY":
        if last >= float(po["tp_price"]):   # TP for BUY
            hit_event, exit_price = "TP_HIT", float(po["tp_price"])
        elif last <= float(po["sl_price"]): # SL for BUY
            hit_event, exit_price = "SL_HIT", float(po["sl_price"])
    else:  # SELL (SHORT)
        if last <= float(po["tp_price"]):   # TP for SELL is lower price
            hit_event, exit_price = "TP_HIT", float(po["tp_price"])
        elif last >= float(po["sl_price"]): # SL for SELL is higher price
            hit_event, exit_price = "SL_HIT", float(po["sl_price"])

    if not hit_event:
        return

    qty = int(po["qty"])
    fill_price = float(po["fill_price"])

    # P&L: BUY gains when price rises; SELL gains when price falls
    if po_side == "BUY":
        pnl_gross = (exit_price - fill_price) * qty
    else:  # SELL
        pnl_gross = (fill_price - exit_price) * qty

    # Exit charges & net
    charges_exit = estimate_roundtrip_charges(fill_price, exit_price, qty, CHARGES)
    pnl_net = pnl_gross - charges_exit

    conn.execute("""
        UPDATE paper_orders
        SET state=?, exit_price=?, closed_ts_utc=datetime('now'),
            pnl_gross=?, pnl_net=?, charges_at_exit=?
        WHERE id=?
    """, (hit_event, exit_price, pnl_gross, pnl_net, charges_exit, order_id))
    log(conn, "INFO", hit_event, "paper_orders", order_id,
        f"Closed at {exit_price} [{po_side}], pnl_gross={pnl_gross:.2f}, pnl_net={pnl_net:.2f}, charges={charges_exit:.2f}")

    # Transition to final CLOSED
    conn.execute("UPDATE paper_orders SET state='CLOSED' WHERE id=?", (order_id,))
    log(conn, "INFO", "CLOSE", "paper_orders", order_id, "Order CLOSED")

# ----------------- Main loop -----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB)
    ap.add_argument("--batch", type=int, default=5, help="how many signals to convert this pass")
    ap.add_argument("--tick", type=float, default=2.0, help="seconds between loop iterations")
    ap.add_argument("--once", action="store_true", help="run a single pass only")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    # 1) Create orders from ready signals (avoid duplicates)
    sigs = fetch_ready_signals(conn, args.batch)
    for s in sigs:
        exists = conn.execute("SELECT 1 FROM paper_orders WHERE signal_row_id=?", (s["id"],)).fetchone()
        if exists:
            continue
        _ = create_paper_order(conn, s)
        conn.commit()

    # 2) Fill due orders and check closes
    if args.once:
        for r in due_fill_ids(conn):
            try_fill_order(conn, r["id"]); conn.commit()
        for r in filled_ids(conn):
            check_and_close(conn, r["id"]); conn.commit()
    else:
        while True:
            for r in due_fill_ids(conn):
                try_fill_order(conn, r["id"]); conn.commit()
            for r in filled_ids(conn):
                check_and_close(conn, r["id"]); conn.commit()
            time.sleep(args.tick)

if __name__ == "__main__":
    main()
