# C:\teevra18\services\svc-depth20.py
import os, json, time, struct, sqlite3, threading
from datetime import datetime, timezone
from urllib.parse import urlencode
from dotenv import load_dotenv
import pandas as pd
from websocket import WebSocketApp  # websocket-client

# ---------------- Env & Config ----------------
ROOT = r"C:\teevra18"
DB_PATH = os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db")

load_dotenv(os.path.join(ROOT, ".env"))
DHAN_WS_BASE = os.getenv("DHAN_DEPTH20_WS", "wss://depth-api-feed.dhan.co/twentydepth")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

assert DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN, "Set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in .env"

# Latency budget (processing)
LATENCY_WARN_MS = 200.0

# Endianness: Dhan v2 depth generally uses LITTLE endian for numeric fields.
# If numbers look absurd, flip to big-endian as per Troubleshooting.
ENDIAN = "<"  # little-endian

# Header: length(int16), feed_code(uint8), exch(uint8), security_id(int32), seq(uint32)  = 12 bytes
HDR_FMT = ENDIAN + "HBBiI"
HDR_SIZE = struct.calcsize(HDR_FMT)

# Each level = price(float64) + qty(uint32) + orders(uint32) = 16 bytes
LVL_FMT = ENDIAN + "dII"
LVL_SIZE = struct.calcsize(LVL_FMT)
LEVELS = 20

# Feed codes (per docs)
FEED_CODE_BID = 41
FEED_CODE_ASK = 51

EXCH_CODE_MAP = {
    1: "NSE_EQ",   # If Dhan changes codes, adjust here
    2: "NSE_FNO",
}

# ---------------- DB Helpers ----------------
def get_watchlist(db_path=DB_PATH):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    try:
        cur.execute("SELECT security_id, exchange_seg FROM universe_depth20")
        rows = cur.fetchall()
    finally:
        con.close()
    if not rows:
        raise RuntimeError("universe_depth20 is empty. Seed it in Step 3.")
    return [{"SecurityId": int(sid), "ExchangeSegment": seg} for sid, seg in rows]

def insert_levels(rows):
    if not rows:
        return
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executemany("""
      INSERT OR REPLACE INTO depth20_levels(
        ts_recv_utc, security_id, exchange_seg, side, level,
        price, qty, orders, top5_bid_qty, top5_ask_qty, top10_bid_qty, top10_ask_qty,
        pressure_1_5, pressure_1_10, latency_ms
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    con.commit()
    con.close()

# ---------------- Parsing ----------------
def compute_pressures(bid_levels, ask_levels):
    # bid_levels/ask_levels are lists of (price, qty, orders) ordered best->20
    b5 = sum(q for _, q, _ in bid_levels[:5])
    a5 = sum(q for _, q, _ in ask_levels[:5])
    b10 = sum(q for _, q, _ in bid_levels[:10])
    a10 = sum(q for _, q, _ in ask_levels[:10])
    def safe_ratio(x, y):
        s = x + y
        return (x - y) / s if s else 0.0
    return b5, a5, b10, a10, safe_ratio(b5, a5), safe_ratio(b10, a10)

def parse_packet(buf, offset, exch_from_hdr=True):
    # returns (next_offset, snapshot_dict) or (next_offset, None) if unknown code
    hdr = struct.unpack_from(HDR_FMT, buf, offset)
    msg_len, feed_code, exch_code, security_id, _seq = hdr
    start = offset + HDR_SIZE
    end = offset + msg_len  # msg_len includes header+payload per docs
    # Sanity: ensure we have full message
    if end > len(buf):
        # incomplete frame; drop
        return len(buf), None

    side = None
    if feed_code == FEED_CODE_BID:
        side = "BID"
    elif feed_code == FEED_CODE_ASK:
        side = "ASK"

    exchange_seg = EXCH_CODE_MAP.get(exch_code, "NSE_FNO" if exch_code==2 else "NSE_EQ")
    levels = []
    pos = start
    for _ in range(LEVELS):
        price, qty, orders = struct.unpack_from(LVL_FMT, buf, pos)
        levels.append((price, int(qty), int(orders)))
        pos += LVL_SIZE

    return end, {
        "security_id": int(security_id),
        "exchange_seg": exchange_seg,
        "side": side,
        "levels": levels
    }

# ---------------- WebSocket Client ----------------
def build_subscribe_message(instruments):
    # RequestCode 23 for 20-Level Market Depth
    # Accepts up to 50 instruments per connection
    msg = {
        "RequestCode": 23,
        "InstrumentCount": len(instruments),
        "InstrumentList": [
            {
                "ExchangeSegment": it["ExchangeSegment"],
                "SecurityId": str(it["SecurityId"])
            } for it in instruments
        ]
    }
    return json.dumps(msg)

def on_open(ws):
    print("WS opened; subscribing...")
    instruments = get_watchlist()
    sub = build_subscribe_message(instruments)
    ws.send(sub)
    print("Subscribed to", len(instruments), "instruments.")

def on_message(ws, message):
    t_recv = time.perf_counter()
    ts_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    if isinstance(message, bytes):
        buf = message
        offset = 0
        # Dhan may stack multiple [BID+ASK] packets for multiple instruments in one message.
        # Use header length to walk the buffer.
        snapshots = []
        while offset + HDR_SIZE <= len(buf):
            try:
                next_off, snap = parse_packet(buf, offset)
            except struct.error:
                break
            if not snap or snap["side"] is None:
                offset = next_off
                continue
            snapshots.append(snap)
            offset = next_off

        # Group by security_id to compute pressures using both sides if available
        by_sid = {}
        for s in snapshots:
            by_sid.setdefault(s["security_id"], {}).update({s["side"]: s})

        rows = []
        t_parsed = time.perf_counter()
        latency_ms = (t_parsed - t_recv) * 1000.0

        for sid, sides in by_sid.items():
            bid_levels = sides.get("BID", {}).get("levels", [])
            ask_levels = sides.get("ASK", {}).get("levels", [])
            if not bid_levels and not ask_levels:
                continue

            # If one side missing, compute with zeros for that side
            b5,a5,b10,a10,p15,p110 = compute_pressures(bid_levels, ask_levels)

            # Insert 20 rows for each available side
            for side_name, levels in [("BID", bid_levels), ("ASK", ask_levels)]:
                if not levels: 
                    continue
                exchange_seg = sides[side_name]["exchange_seg"]
                for idx, (price, qty, orders) in enumerate(levels, start=1):
                    rows.append((
                        ts_iso, sid, exchange_seg, side_name, idx,
                        float(price), int(qty), int(orders),
                        int(b5), int(a5), int(b10), int(a10),
                        float(p15), float(p110), float(latency_ms)
                    ))
        if rows:
            insert_levels(rows)
            if latency_ms > LATENCY_WARN_MS:
                print(f"[WARN] Latency {latency_ms:.1f}ms > {LATENCY_WARN_MS}ms (rows={len(rows)})")
    else:
        # Some servers send text keepalives or errors
        print("TEXT:", message)

def on_error(ws, err):
    print("WS error:", err)

def on_close(ws, code, msg):
    print(f"WS closed: code={code} msg={msg}")

def run_ws():
    params = {
        "token": DHAN_ACCESS_TOKEN,
        "clientId": DHAN_CLIENT_ID,
        "authType": 2
    }
    url = DHAN_WS_BASE + "?" + urlencode(params)
    ws = WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    # Auto-reconnect loop
    while True:
        try:
            ws.run_forever(ping_interval=10, ping_timeout=8)  # library handles pong
        except Exception as e:
            print("run_forever exception:", e)
        print("Reconnecting in 3s...")
        time.sleep(3)

if __name__ == "__main__":
    print("Starting svc-depth20 ...")
    run_ws()
