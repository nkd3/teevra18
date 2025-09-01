# C:\teevra18\teevra\dhan_ws.py
import os
import time
import threading
import asyncio
import inspect
from pathlib import Path
from collections import deque
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psutil

from teevra.db import ensure_schema, connect, put_health, log

# ---- Settings / Paths --------------------------------------------------------
ENV = os.getenv("ENV", "local")
DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\teevra18\data"))
PARQUET_DIR = DATA_DIR / "parquet" / "ticks"
LOG_DIR = Path(os.getenv("LOG_DIR", r"C:\teevra18\logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

DHAN_CLIENT_ID = os.environ["DHAN_CLIENT_ID"]
DHAN_ACCESS_TOKEN = os.environ["DHAN_ACCESS_TOKEN"]
MODE = os.getenv("DHAN_FEED_MODE", "QUOTE").upper().strip()  # QUOTE | FULL | TICKER

BATCH_FLUSH_MS = int(os.getenv("BATCH_FLUSH_MS", "750"))
SQLITE_BATCH_SIZE = int(os.getenv("SQLITE_BATCH_SIZE", "500"))
PARQUET_FLUSH_SECS = float(os.getenv("PARQUET_FLUSH_SECS", "5"))
PING_WARN_MS = int(os.getenv("PING_WARN_MS", "2000"))
GAP_MAX_SECS = float(os.getenv("GAP_MAX_SECS", "3"))
WATCHLIST_REFRESH_SECS = float(os.getenv("WATCHLIST_REFRESH_SECS", "15"))

# Tuning knobs (CPU smoothing / status / sleeps)
CPU_WARN_THRESHOLD      = float(os.getenv("CPU_WARN_THRESHOLD", "70"))
CPU_SAMPLE_SECS         = float(os.getenv("CPU_SAMPLE_SECS", "0.15"))
FLUSH_LOOP_SLEEP_SECS   = float(os.getenv("FLUSH_LOOP_SLEEP_SECS", "0.25"))
IDLE_POLL_SLEEP_SECS    = float(os.getenv("IDLE_POLL_SLEEP_SECS", "0.01"))
RECEIVING_FRESH_SECS    = float(os.getenv("RECEIVING_FRESH_SECS", "1.5"))
STATUS_MIN_UPDATE_SECS  = float(os.getenv("STATUS_MIN_UPDATE_SECS", "1.0"))
CPU_WARN_COOLDOWN_SECS  = float(os.getenv("CPU_WARN_COOLDOWN_SECS", "30"))

# ---- Legacy SDK (v2.0.2) imports --------------------------------------------
from dhanhq import marketfeed as mf  # legacy async API: DhanFeed, Quote/Full/Ticker
MODE_CONST = {"QUOTE": mf.Quote, "FULL": mf.Full, "TICKER": mf.Ticker}[MODE]

# ---- Data model --------------------------------------------------------------
@dataclass
class TickRow:
    ts_utc: str
    exchange_segment: int
    security_id: int
    mode: str
    ltt_epoch: Optional[int]
    ltp: Optional[float]
    atp: Optional[float]
    last_qty: Optional[int]
    volume: Optional[int]
    buy_qty_total: Optional[int]
    sell_qty_total: Optional[int]
    oi: Optional[int]
    day_open: Optional[float]
    day_high: Optional[float]
    day_low: Optional[float]
    day_close: Optional[float]
    prev_close: Optional[float]
    recv_ts_utc: str

def _now_utc_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

# ---- Ingestor ----------------------------------------------------------------
class Ingestor:
    def __init__(self):
        ensure_schema()
        self.stop_evt = threading.Event()
        self.buffer = deque()        # SQLite
        self.parquet_buffer = []     # Parquet
        self.lock = threading.Lock()
        self.last_recv_ts = 0.0

        # Subscriptions
        self._wanted = set()       # set[(seg:int, sid:str, MODE_CONST)]
        self._subscribed = set()

        # Async loop thread
        self._loop = None
        self._async_thread = None

        # Live connection/status helpers
        self.connected = False
        self._status_cache = None
        self._last_status_push = 0.0
        self._last_cpu_warn = 0.0

    # --- DB flushers ----------------------------------------------------------
    def _flush_sqlite(self):
        if not self.buffer:
            return
        rows = []
        with self.lock:
            while self.buffer and len(rows) < SQLITE_BATCH_SIZE:
                tr: TickRow = self.buffer.popleft()
                rows.append(
                    (
                        tr.ts_utc, tr.exchange_segment, tr.security_id, tr.mode, tr.ltt_epoch,
                        tr.ltp, tr.atp, tr.last_qty, tr.volume, tr.buy_qty_total, tr.sell_qty_total,
                        tr.oi, tr.day_open, tr.day_high, tr.day_low, tr.day_close, tr.prev_close, tr.recv_ts_utc
                    )
                )
        if rows:
            with connect() as c:
                c.executemany(
                    """
                    INSERT INTO ticks_raw(
                        ts_utc,exchange_segment,security_id,mode,ltt_epoch,ltp,atp,last_qty,volume,
                        buy_qty_total,sell_qty_total,oi,day_open,day_high,day_low,day_close,prev_close,recv_ts_utc
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    rows,
                )

    def _flush_parquet(self):
        if not self.parquet_buffer:
            return
        df = pd.DataFrame(self.parquet_buffer)
        self.parquet_buffer.clear()
        if df.empty:
            return
        df["ds"] = pd.to_datetime(df["ts_utc"]).dt.date.astype(str)
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(
            table,
            root_path=str(PARQUET_DIR),
            partition_cols=["ds", "exchange_segment", "security_id"]
        )

    # --- Watchlist ------------------------------------------------------------
    def _load_watchlist(self):
        with connect() as c:
            cur = c.execute(
                "SELECT DISTINCT exchange_segment, security_id FROM universe_watchlist WHERE is_active=1"
            )
            # legacy API wants (seg:int, sid:str, modeConst)
            rows = [(int(r[0]), str(int(r[1])), MODE_CONST) for r in cur.fetchall()]
        return rows

    def _refresh_wanted(self):
        try:
            self._wanted = set(self._load_watchlist())
            put_health("m1_subscribed_count", str(len(self._subscribed)))
        except Exception as e:
            log("ERROR", "watchlist", str(e))

    # --- Helpers ---------------------------------------------------------------
    @staticmethod
    async def _maybe_await(callable_obj, *args, **kwargs):
        """Call a function that might be sync or async. Await only if needed."""
        if callable_obj is None:
            return None
        try:
            result = callable_obj(*args, **kwargs)
        except TypeError as e:
            raise e
        if inspect.isawaitable(result):
            return await result
        return result

    async def _subscribe_batch(self, feed, instruments):
        """
        Try known subscribe methods in order:
        1) subscribe_symbols(instruments)
        2) subscribe(instruments)
        3) (fallback) set feed.instruments then subscribe_instruments()
        Handles both sync and async methods.
        """
        for name in ("subscribe_symbols", "subscribe"):
            fn = getattr(feed, name, None)
            if fn:
                try:
                    return await self._maybe_await(fn, instruments)
                except TypeError as e:
                    log("WARN", "subscribe", f"{name} signature mismatch: {e}")
                except Exception as e:
                    log("WARN", "subscribe", f"{name} error: {e}")

        # Fallback: feed.subscribe_instruments() uses feed.instruments internally
        try:
            setattr(feed, "instruments", instruments)
        except Exception:
            pass
        fn = getattr(feed, "subscribe_instruments", None)
        if fn:
            try:
                return await self._maybe_await(fn)
            except Exception as e:
                log("WARN", "subscribe", f"subscribe_instruments error: {e}")
        else:
            log("WARN", "subscribe", "No subscribe* method found on feed")
        return None

    async def _unsubscribe_batch(self, feed, instruments):
        for name in ("unsubscribe_symbols", "unsubscribe"):
            fn = getattr(feed, name, None)
            if fn:
                try:
                    return await self._maybe_await(fn, instruments)
                except TypeError as e:
                    log("WARN", "unsubscribe", f"{name} signature mismatch: {e}")
                except Exception as e:
                    log("WARN", "unsubscribe", f"{name} error: {e}")
        return None

    def _set_status(self, new_status: str):
        """Push m1_status only on change or occasionally to avoid DB spam."""
        now = time.time()
        if new_status != self._status_cache or (now - self._last_status_push) >= STATUS_MIN_UPDATE_SECS:
            put_health("m1_status", new_status)
            self._status_cache = new_status
            self._last_status_push = now

    # --- Tick parsing (hardened) ----------------------------------------------
    def _parse_and_buffer(self, packet: dict):
        """
        Safely parse a marketfeed packet.
        - Tolerates missing/None fields.
        - Skips packets without a valid SecurityId.
        """

        def first_key(d, *names):
            for k in names:
                if k in d and d[k] is not None:
                    return d[k]
            return None

        def as_int(x):
            try:
                if x is None or (isinstance(x, str) and x.strip() == ""):
                    return None
                if isinstance(x, float):
                    return int(x)
                return int(str(x).strip())
            except (ValueError, TypeError):
                return None

        def as_float(x):
            try:
                if x is None or (isinstance(x, str) and x.strip() == ""):
                    return None
                return float(str(x).strip())
            except (ValueError, TypeError):
                return None

        # Some SDK builds send {"Data": {...}}; others send dict directly
        D = packet.get("Data") if isinstance(packet, dict) else None
        if not isinstance(D, dict):
            D = packet if isinstance(packet, dict) else {}

        seg_raw = first_key(D, "ExchangeSegment", "exchangeSegment", "exchange_segment", "Segment", "segment")
        sid_raw = first_key(D, "SecurityId", "securityId", "security_id", "InstrumentIdentifier", "instrumentIdentifier")
        ltt_raw = first_key(D, "LTT", "LastTradeTime", "lastTradeTime", "last_trade_time")

        seg = as_int(seg_raw) or 2  # default to NSE F&O
        sid = as_int(sid_raw)
        if sid is None:
            return  # Cannot map to instrument; skip

        row = TickRow(
            ts_utc=_now_utc_iso(),
            exchange_segment=seg,
            security_id=sid,
            mode=("F" if MODE_CONST == mf.Full else ("Q" if MODE_CONST == mf.Quote else "T")),
            ltt_epoch=as_int(ltt_raw),
            ltp=as_float(first_key(D, "LTP", "ltp")),
            atp=as_float(first_key(D, "ATP", "atp")),
            last_qty=as_int(first_key(D, "LastQty", "lastQty", "LastTradedQty")),
            volume=as_int(first_key(D, "Volume", "volume", "TotalTradedVolume")),
            buy_qty_total=as_int(first_key(D, "TotalBuyQty", "totalBuyQty")),
            sell_qty_total=as_int(first_key(D, "TotalSellQty", "totalSellQty")),
            oi=as_int(first_key(D, "OI", "oi", "OpenInterest")),
            day_open=as_float(first_key(D, "Open", "open")),
            day_high=as_float(first_key(D, "High", "high")),
            day_low=as_float(first_key(D, "Low", "low")),
            day_close=as_float(first_key(D, "Close", "close")),
            prev_close=as_float(first_key(D, "PrevClose", "prevClose", "PreviousClose")),
            recv_ts_utc=_now_utc_iso(),
        )

        with self.lock:
            self.buffer.append(row)
            self.parquet_buffer.append(row.__dict__)
        self.last_recv_ts = time.time()

    def _parse_any(self, pkt):
        if isinstance(pkt, list):
            for p in pkt:
                if isinstance(p, dict):
                    self._parse_and_buffer(p)
        elif isinstance(pkt, dict):
            self._parse_and_buffer(pkt)

    # --- Flusher & health thread ---------------------------------------------
    def _t_flushers(self):
        last_sql = 0.0
        last_par = 0.0
        while not self.stop_evt.is_set():
            now = time.perf_counter()
            if (now - last_sql) * 1000 >= BATCH_FLUSH_MS:
                self._flush_sqlite()
                last_sql = now
            if (now - last_par) >= PARQUET_FLUSH_SECS:
                self._flush_parquet()
                last_par = now

            # CPU with smoothing + cooldowned warnings
            cpu = psutil.cpu_percent(interval=CPU_SAMPLE_SECS)
            put_health("m1_cpu", f"{cpu:.1f}")
            if cpu > CPU_WARN_THRESHOLD:
                wall = time.time()
                if (wall - self._last_cpu_warn) >= CPU_WARN_COOLDOWN_SECS:
                    log("WARN", "ingest", f"CPU high {cpu:.1f}%")
                    self._last_cpu_warn = wall

            # Gap check
            if time.time() - self.last_recv_ts > GAP_MAX_SECS:
                put_health("m1_gap_warning", "1")
            else:
                put_health("m1_gap_warning", "0")

            # Live status: receiving vs idle (only when connected)
            if self.connected:
                age = time.time() - self.last_recv_ts
                if age < RECEIVING_FRESH_SECS:
                    self._set_status("receiving")
                else:
                    self._set_status("connected_idle")

            time.sleep(FLUSH_LOOP_SLEEP_SECS)

    # --- Async driver ---------------------------------------------------------
    async def _async_main(self):
        """Connect, (re)subscribe, and consume packets."""
        self._set_status("starting")
        while not self.stop_evt.is_set():
            try:
                feed = mf.DhanFeed(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, [], "v2")
                res = feed.connect()
                if inspect.isawaitable(res):
                    await res
                self.connected = True
                self._set_status("connected_waiting")

                self._refresh_wanted()
                desired = list(self._wanted)

                # Subscribe in chunks; handle sync/async subscribe methods
                CHUNK = 100
                for i in range(0, len(desired), CHUNK):
                    batch = desired[i: i + CHUNK]
                    try:
                        await self._subscribe_batch(feed, batch)
                    except Exception as e:
                        log("WARN", "subscribe", f"{e}")

                self._subscribed = set(desired)
                put_health("m1_subscribed_count", str(len(self._subscribed)))

                last_refresh = 0.0
                while not self.stop_evt.is_set():
                    try:
                        pkt = feed.get_instrument_data()
                        if inspect.isawaitable(pkt):
                            pkt = await pkt
                        if pkt:
                            self._parse_any(pkt)
                        else:
                            # avoid busy-spin when no packet is available
                            await asyncio.sleep(IDLE_POLL_SLEEP_SECS)
                    except Exception as e:
                        log("ERROR", "consume", str(e))
                        await asyncio.sleep(0.05)

                    # periodic reconcile with DB watchlist
                    now = time.time()
                    if now - last_refresh >= WATCHLIST_REFRESH_SECS:
                        last_refresh = now
                        prev = set(self._subscribed)
                        self._refresh_wanted()
                        current = set(self._wanted)

                        # Unsubscribe removed
                        to_del = list(prev - current)
                        for j in range(0, len(to_del), CHUNK):
                            try:
                                await self._unsubscribe_batch(feed, to_del[j: j + CHUNK])
                            except Exception as e:
                                log("WARN", "unsubscribe", f"{e}")

                        # Subscribe new
                        to_add = list(current - prev)
                        for j in range(0, len(to_add), CHUNK):
                            try:
                                await self._subscribe_batch(feed, to_add[j: j + CHUNK])
                            except Exception as e:
                                log("WARN", "subscribe", f"{e}")

                        self._subscribed = current
                        put_health("m1_subscribed_count", str(len(self._subscribed)))

                # Graceful disconnect
                self.connected = False
                disc = getattr(feed, "disconnect", None)
                try:
                    if disc:
                        res = disc()
                        if inspect.isawaitable(res):
                            await res
                except Exception:
                    pass

            except Exception as e:
                log("ERROR", "driver_async", f"{e}; reconnecting soon")
                self.connected = False
                self._set_status("reconnecting")
                await asyncio.sleep(1.0)

        self._set_status("stopped")

    # --- Public lifecycle ------------------------------------------------------
    def run(self):
        ensure_schema()
        PARQUET_DIR.mkdir(parents=True, exist_ok=True)

        t_flush = threading.Thread(target=self._t_flushers, daemon=True)
        t_flush.start()

        def _runner():
            try:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
                self._loop.run_until_complete(self._async_main())
            finally:
                try:
                    self._loop.close()
                except Exception:
                    pass

        self._async_thread = threading.Thread(target=_runner, daemon=True)
        self._async_thread.start()

        while not self.stop_evt.is_set():
            time.sleep(0.5)

    def stop(self):
        self.stop_evt.set()
