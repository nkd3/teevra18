# C:\teevra18\services\ltp_feeder\feeder_dhan.py
import os, json, sqlite3, time, math, threading, struct
from datetime import datetime, timezone
from websocket import WebSocketApp

DB = r"C:\teevra18\data\teevra18.db"
WS_BASE = "wss://api-feed.dhan.co"  # v2 WebSocket root (no path)

# --- DB helpers ----------------------------------------------------------------
def db_conn():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def load_subscriptions(conn):
    rows = conn.execute("""
        SELECT option_symbol, token, exchange
        FROM ltp_subscriptions
        WHERE broker='DHAN'
        ORDER BY option_symbol
    """).fetchall()
    # Map option_symbol -> (securityId, exchangeSegment)
    # Dhan wants ExchangeSegment text like "NSE_FNO" in subscribe message.
    syms = []
    for r in rows:
        sec = (r["token"] or "").strip()
        if not sec or sec.startswith("<SECID"):
            # skip placeholders
            continue
        exch = r["exchange"] or "NFO"
        # normalize to Dhan Annexure value
        exch_seg = "NSE_FNO" if exch.upper() in ("NFO","NSE_FNO","NSE-FO") else exch
        syms.append((r["option_symbol"], sec, exch_seg))
    return syms

def symbol_by_security_id(conn):
    # For reverse lookup while decoding (securityId -> option_symbol)
    rows = conn.execute("""
        SELECT option_symbol, token FROM ltp_subscriptions WHERE broker='DHAN'
    """).fetchall()
    return { (r["token"] or "").strip(): r["option_symbol"] for r in rows if r["token"] }

def upsert_ltp(conn, option_symbol, ltp):
    conn.execute(
        "INSERT INTO ltp_cache(option_symbol, ts_utc, ltp) VALUES(?,?,?)",
        (option_symbol, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), float(ltp))
    )
    conn.commit()

# --- Dhan v2 details -----------------------------------------------------------
# Docs:
# Connection URL must be: wss://api-feed.dhan.co?version=2&token=...&clientId=...&authType=2
# Subscribe (JSON text message):
#   {
#     "RequestCode": 15,               # Ticker (LTP)
#     "InstrumentCount": N,
#     "InstrumentList": [{"ExchangeSegment":"NSE_FNO","SecurityId":"..."} ...]
#   }
# Response is BINARY. Ticker packet layout (Annexure / Live Market Feed):
#   Header (8 bytes): [resp_code:1][msg_len:int16][exch_seg:1][secid:int32]
#   Payload (8 bytes): [ltp:float32][ltt:int32]
#   resp_code for Ticker = 2

RESP_CODE_TICKER = 2

def make_ws_url():
    client_id = (os.environ.get("DHAN_CLIENT_ID") or "").strip()
    token     = (os.environ.get("DHAN_ACCESS_TOKEN") or "").strip()
    if not client_id or not token:
        raise RuntimeError("DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN not set in environment.")
    return f"{WS_BASE}?version=2&token={token}&clientId={client_id}&authType=2"

def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# --- Decoder for ticker binary -------------------------------------------------
def parse_ticker_packet(b):
    # Expect at least 16 bytes
    if len(b) < 16:
        return None
    # Header
    resp_code = b[0]
    if resp_code != RESP_CODE_TICKER:
        return None
    # int16 message length at [1:3] (big-endian per docs examples appear BE; Dhan uses network order)
    msg_len = struct.unpack(">H", b[1:3])[0]
    exch_seg = b[3]  # not used here
    sec_id   = struct.unpack(">i", b[4:8])[0]  # 32-bit int
    # Payload
    ltp      = struct.unpack(">f", b[8:12])[0]
    # ltt   = struct.unpack(">i", b[12:16])[0]  # last trade time epoch (ignored)
    return (str(sec_id), float(ltp))

# --- WebSocket callbacks -------------------------------------------------------
class DhanFeeder:
    def __init__(self):
        self.ws = None
        self.url = make_ws_url()
        self.conn = db_conn()
        self.sym_map = symbol_by_security_id(self.conn)   # secId -> symbol
        self.to_subscribe = load_subscriptions(self.conn) # (symbol, secId, exch)
        self.active = True

    def on_open(self, ws):
        print("[DHAN] opened; subscribing...")
        # Subscribe in chunks of 100 instruments
        count_total = 0
        for chunk in chunked(self.to_subscribe, 100):
            instr_list = [{"ExchangeSegment": exch, "SecurityId": sec} for (_, sec, exch) in chunk]
            msg = {
                "RequestCode": 15,  # Ticker/LTP
                "InstrumentCount": len(instr_list),
                "InstrumentList": instr_list
            }
            ws.send(json.dumps(msg))
            count_total += len(instr_list)
            time.sleep(0.05)
        print(f"[DHAN] subscribed {count_total} instruments")

    def on_message(self, ws, message):
        # message is bytes (binary)
        if isinstance(message, (bytes, bytearray)):
            parsed = parse_ticker_packet(message)
            if not parsed:
                return
            sec_id, ltp = parsed
            # Map sec_id back to symbol; if not found, try to refresh once
            sym = self.sym_map.get(sec_id)
            if sym is None:
                self.sym_map = symbol_by_security_id(self.conn)
                sym = self.sym_map.get(sec_id)
            if sym:
                try:
                    upsert_ltp(self.conn, sym, ltp)
                except Exception as e:
                    print("[DHAN] db write error:", repr(e))
        else:
            # Some servers may push text admin messages; print for visibility
            print("[DHAN] text:", message)

    def on_error(self, ws, error):
        print("[DHAN] error:", repr(error))

    def on_close(self, ws, code, reason):
        print("[DHAN] socket closed", code, reason)
        self.active = False

    def run_forever(self):
        self.ws = WebSocketApp(
            self.url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        # Dhan pings every ~10s; client replies automatically via WebSocketApp
        self.ws.run_forever()

def main():
    print(f"[DHAN] using DB: {DB}")
    feeder = DhanFeeder()
    feeder.run_forever()

if __name__ == "__main__":
    main()
