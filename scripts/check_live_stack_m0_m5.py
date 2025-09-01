# -*- coding: utf-8 -*-
"""
check_live_stack_m0_m5.py (v1.5)
- FIX: Remove ROWID usage; sort by ts/ts_close instead.
- Market status (NSE, IST): LIVE / CLOSED with exact countdown.
- Works with SMART views produced by create_compat_views_m0_m5.py.
"""

import os, sys, sqlite3, datetime
from pathlib import Path

try:
    from colorama import init as colorama_init, Fore, Style
    colorama_init(autoreset=True)
except Exception:
    class _D: 
        def __getattr__(self, k): return ''
    Fore = Style = _D()

# ---------------- Config ----------------
ROOT = Path(os.getenv("PROJECT_ROOT", r"C:\teevra18"))
DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

CPU_LIMIT = 40
M1_MAX_GAP_SECS = 3
M2_FRESH_SECS = 60
M3_FRESH_SECS = 2
M4_BAR_STALE_SECS = 120
M5_FRESH_SECS = 60

HOL_FILE = Path(r"C:\teevra18\config\holidays_nse_2025.txt")
IST_OFFSET = datetime.timedelta(hours=5, minutes=30)

# --------------- Time helpers ---------------
def utc_now(): return datetime.datetime.utcnow()
def now_ist(): return utc_now() + IST_OFFSET

def fmt_td(td):
    if td is None: return "N/A"
    secs = int(round(td.total_seconds()))
    sign = ""
    if secs < 0: secs = -secs; sign = "-"
    if secs < 60: return f"{sign}{secs}s"
    mins, s = divmod(secs, 60)
    if mins < 60: return f"{sign}{mins}m {s}s"
    hrs, mins = divmod(mins, 60)
    return f"{sign}{hrs}h {mins}m {s}s"

def ts_to_dt(ts):
    """Accept ISO string or epoch seconds; return UTC datetime or None."""
    if ts is None: return None
    if isinstance(ts, (int, float)):
        try: return datetime.datetime.utcfromtimestamp(ts)
        except Exception: return None
    if isinstance(ts, str):
        s = ts.strip()
        if not s: return None
        fmts = ("%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%SZ")
        for fmt in fmts:
            try: return datetime.datetime.strptime(s.replace("Z",""), fmt)
            except Exception: pass
        # numeric in string
        try: return datetime.datetime.utcfromtimestamp(float(s))
        except Exception: return None
    return None

# --------------- Market status ---------------
def load_holidays():
    d = set()
    if HOL_FILE.exists():
        for line in HOL_FILE.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#"): continue
            y,m,dd = map(int, s.split("-"))
            d.add(datetime.date(y,m,dd))
    return d
HOLIDAYS = load_holidays()

def is_biz_day(d: datetime.date):
    return d.weekday() < 5 and d not in HOLIDAYS

def next_biz_day(d: datetime.date):
    x = d
    while True:
        x += datetime.timedelta(days=1)
        if is_biz_day(x): return x

def session_bounds_ist(dt_ist):
    d = dt_ist.date()
    pre_open = datetime.datetime(d.year, d.month, d.day, 9, 0)
    openI    = datetime.datetime(d.year, d.month, d.day, 9, 15)
    closeI   = datetime.datetime(d.year, d.month, d.day, 15, 30)
    return pre_open, openI, closeI

def market_status():
    nowI = now_ist()
    today = nowI.date()
    if not is_biz_day(today):
        nb = next_biz_day(today)
        nxt_open = datetime.datetime(nb.year, nb.month, nb.day, 9, 15)
        return ("CLOSED", None, nxt_open, nxt_open - nowI)
    pre_open, openI, closeI = session_bounds_ist(nowI)
    if nowI < pre_open: return ("CLOSED", None, openI, openI - nowI)
    if pre_open <= nowI < openI: return ("CLOSED", None, openI, openI - nowI)
    if openI <= nowI <= closeI: return ("LIVE", closeI - nowI, None, None)
    nb = next_biz_day(today)
    nxt_open = datetime.datetime(nb.year, nb.month, nb.day, 9, 15)
    return ("CLOSED", None, nxt_open, nxt_open - nowI)

# --------------- SQLite helpers ---------------
def q(conn, sql, args=()):
    try:
        cur = conn.execute(sql, args)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []
        return rows, cols, None
    except Exception as e:
        return None, None, str(e)

def obj_exists(conn, name):
    rows, _, _ = q(conn, "SELECT name FROM sqlite_master WHERE name=?", (name,))
    return bool(rows)

# --------------- Print helpers ---------------
def hdr(t): print(f"\n{Style.BRIGHT}{t}{Style.RESET_ALL}")
def ok(m): print(f"{Fore.GREEN}PASS{Fore.RESET}  - {m}")
def warn(m): print(f"{Fore.YELLOW}SOFT-FAIL{Fore.RESET} - {m}")
def bad(m): print(f"{Fore.RED}FAIL{Fore.RESET}  - {m}")

# ================= Main =================
hdr("Teevra18 Live Health Checker — Stages M0 → M5 (v1.5)")

status, ttc, nxt, eta = market_status()
if status == "LIVE":
    ok(f"Market: LIVE (time to close: {fmt_td(ttc)})")
else:
    warn(f"Market: CLOSED (next open in {fmt_td(eta)})")

try:
    conn = sqlite3.connect(DB_PATH.as_posix())
    ok(f"Connected to DB: {DB_PATH}")
except Exception as e:
    bad(f"Cannot open DB: {e}")
    sys.exit(2)

# ---------- M0 ----------
hdr("M0 — Core / DB / Health")
m0_ok = True
if not obj_exists(conn, "health_view"):
    m0_ok = False
    bad("health_view not found. Run create_compat_views_m0_m5.py first.")
else:
    rows, _, err = q(conn, "SELECT updated_at, breaker_state FROM health_view LIMIT 1;")
    if err or not rows:
        m0_ok = False
        bad("health_view has no data.")
    else:
        updated_at, breaker_state = rows[0]
        dt = ts_to_dt(updated_at)
        age = (utc_now() - dt) if dt else None
        if breaker_state != "RUNNING":
            m0_ok = False
            warn(f"breaker_state={breaker_state} (expected RUNNING).")
        else:
            ok("breaker_state=RUNNING.")
        if age is not None and age.total_seconds() <= 600:
            ok(f"Heartbeat fresh (last update {fmt_td(age)} ago).")
        else:
            # When market is CLOSED, stale heartbeat could be acceptable
            m0_ok = False
            msg = "Heartbeat stale (>10m) or timestamp unreadable."
            if status != "LIVE": msg += " Market is CLOSED (may be acceptable)."
            warn(msg)

# ---------- M1 ----------
hdr("M1 — Ingest (ticks_raw)")
m1_ok = True
if not obj_exists(conn, "ticks_raw_view"):
    m1_ok = False
    bad("ticks_raw_view not found.")
else:
    # Pull latest 1000 by ts (DESC). No ROWID usage.
    rows, _, err = q(conn, "SELECT ts FROM ticks_raw_view ORDER BY ts DESC LIMIT 1000;")
    if err:
        m1_ok = False
        bad(f"Cannot read ticks: {err}")
    elif not rows:
        m1_ok = False
        msg = "No ticks found."
        if status != "LIVE": msg += " Market is CLOSED (expected)."
        warn(msg)
    else:
        now = utc_now()
        dts = [ts_to_dt(r[0]) for r in rows]
        dts = [d for d in dts if d is not None and (now - d).total_seconds() <= 120]
        if not dts:
            m1_ok = False
            msg = "No ticks in last 120s."
            if status != "LIVE": msg += " Market is CLOSED (expected)."
            warn(msg)
        else:
            dts.sort()
            gaps = [(b - a).total_seconds() for a, b in zip(dts, dts[1:])]
            max_gap = max(gaps) if gaps else 0.0
            if max_gap <= M1_MAX_GAP_SECS:
                ok(f"No gap > {M1_MAX_GAP_SECS}s in last 120s (max_gap={max_gap:.2f}s).")
            else:
                m1_ok = False
                warn(f"Ingest gap detected: {max_gap:.2f}s (> {M1_MAX_GAP_SECS}s).")

    # CPU (soft)
    try:
        import psutil
        cpu = psutil.cpu_percent(interval=0.5)
        if cpu <= CPU_LIMIT:
            ok(f"CPU within limit: {cpu:.1f}% ≤ {CPU_LIMIT}%")
        else:
            warn(f"High CPU: {cpu:.1f}% > {CPU_LIMIT}% (reduce universe/batching).")
    except Exception:
        warn("psutil not installed → CPU check skipped.")

# ---------- M2 ----------
hdr("M2 — Quote Snap")
m2_ok = True
if not obj_exists(conn, "quote_snap_view"):
    m2_ok = False
    warn("quote_snap_view not found.")
else:
    rows, _, err = q(conn, "SELECT MAX(ts) FROM quote_snap_view;")
    if err:
        m2_ok = False
        bad(f"Cannot read quote_snap_view: {err}")
    else:
        dt = ts_to_dt(rows[0][0]) if rows and rows[0] else None
        if not dt:
            m2_ok = False
            warn("quote_snap_view ts unreadable.")
        else:
            age = utc_now() - dt
            if age.total_seconds() <= M2_FRESH_SECS:
                ok(f"Quote snap fresh (age={fmt_td(age)} ≤ {M2_FRESH_SECS}s).")
            else:
                m2_ok = False
                m = f"Quote snap stale (age={fmt_td(age)} > {M2_FRESH_SECS}s)."
                if status != "LIVE": m += " Market is CLOSED (may be expected)."
                warn(m)

# ---------- M3 ----------
hdr("M3 — Depth20")
m3_ok = True
if not obj_exists(conn, "depth20_snap_view"):
    m3_ok = False
    warn("depth20_snap_view not found.")
else:
    rows, _, err = q(conn, "SELECT MAX(ts) FROM depth20_snap_view;")
    if err:
        m3_ok = False
        bad(f"Cannot read depth20_snap_view: {err}")
    else:
        last_ts = rows[0][0] if rows else None
        if last_ts is None:
            m3_ok = False
            msg = "No depth20 rows."
            if status != "LIVE": msg += " Market is CLOSED (expected)."
            warn(msg)
        else:
            dt = ts_to_dt(last_ts)
            if not dt:
                m3_ok = False
                warn("depth20 last ts unreadable.")
            else:
                age = utc_now() - dt
                if age.total_seconds() <= M3_FRESH_SECS:
                    ok(f"Depth feed fresh (age={fmt_td(age)} ≤ {M3_FRESH_SECS}s).")
                else:
                    m3_ok = False
                    m = f"Depth feed stale (age={fmt_td(age)} > {M3_FRESH_SECS}s)."
                    if status != "LIVE": m += " Market is CLOSED (may be expected)."
                    warn(m)
    # soft sanity: ORDER BY ts (not ROWID)
    for cname in ("bid_qty_total","ask_qty_total","imbalance"):
        rows, _, err = q(conn, f"SELECT {cname} FROM depth20_snap_view WHERE {cname} IS NOT NULL ORDER BY ts DESC LIMIT 10;")
        if err or not rows: 
            continue
        ok(f"Depth column {cname} populated (last {len(rows)} rows ok).")

# ---------- M4 ----------
hdr("M4 — Candles (1m)")
m4_ok = True
if not obj_exists(conn, "candles_1m_view"):
    m4_ok = False
    warn("candles_1m_view not found.")
else:
    rows, _, err = q(conn, "SELECT ts_close, open, high, low, close FROM candles_1m_view ORDER BY ts_close DESC LIMIT 1;")
    if err or not rows:
        m4_ok = False
        warn("No recent candle found.")
    else:
        t, o, h, l, c = rows[0]
        dt = ts_to_dt(t)
        if dt:
            age = utc_now() - dt
            if age.total_seconds() <= M4_BAR_STALE_SECS:
                ok(f"Latest 1m candle closed {fmt_td(age)} ago (≤ {M4_BAR_STALE_SECS}s).")
            else:
                m4_ok = False
                m = f"Latest 1m candle seems stale ({fmt_td(age)})."
                if status != "LIVE": m += " Market is CLOSED (may be expected)."
                warn(m)
        else:
            m4_ok = False
            warn("Latest candle timestamp unreadable.")
        if None in (o,h,l,c):
            m4_ok = False
            warn("OHLC has NULLs on latest row.")
        elif not (l <= o <= h and l <= c <= h and l <= h):
            m4_ok = False
            warn("OHLC range sanity failed (low ≤ open/close ≤ high).")
        else:
            ok("OHLC sanity OK on latest candle.")

# ---------- M5 ----------
hdr("M5 — Option Chain")
m5_ok = True
if not obj_exists(conn, "option_chain_snap_view"):
    m5_ok = False
    warn("option_chain_snap_view not found.")
else:
    rows, _, err = q(conn, "SELECT MAX(ts) FROM option_chain_snap_view;")
    if err:
        m5_ok = False
        bad(f"Cannot read option_chain_snap_view: {err}")
    else:
        dt = ts_to_dt(rows[0][0]) if rows and rows[0] else None
        if dt:
            age = utc_now() - dt
            if age.total_seconds() <= M5_FRESH_SECS:
                ok(f"Option chain fresh (age={fmt_td(age)} ≤ {M5_FRESH_SECS}s).")
            else:
                m5_ok = False
                m = f"Option chain stale (age={fmt_td(age)} > {M5_FRESH_SECS}s)."
                if status != "LIVE": m += " Market is CLOSED (may be expected)."
                warn(m)
        else:
            m5_ok = False
            warn("Option chain timestamp unreadable.")

    # sanity (ORDER BY ts, not ROWID)
    for cname, rng in [("iv", (0.0, 200.0)), ("oi", (0, None)), ("delta", (-5, 5))]:
        rows, _, err = q(conn, f"SELECT {cname} FROM option_chain_snap_view WHERE {cname} IS NOT NULL ORDER BY ts DESC LIMIT 20;")
        if err or not rows: 
            continue
        vals = [r[0] for r in rows]
        lo, hi = rng
        def okv(v):
            if lo is not None and v < lo: return False
            if hi is not None and v > hi: return False
            return True
        if all(isinstance(v,(int,float)) and okv(v) for v in vals):
            ok(f"{cname} sane in last {len(vals)} rows.")
        else:
            m5_ok = False
            warn(f"{cname} out-of-range values in last {len(vals)} rows.")

# ---------- Summary ----------
hdr("Summary")
results = [("M0", m0_ok), ("M1", m1_ok), ("M2", m2_ok), ("M3", m3_ok), ("M4", m4_ok), ("M5", m5_ok)]
ok_count = sum(1 for _, v in results if v)
for name, val in results:
    mark = f"{Fore.GREEN}OK{Fore.RESET}" if val else f"{Fore.RED}ISSUE{Fore.RESET}"
    print(f"{name}: {mark}")

if ok_count == len(results):
    ok("All stages M0 → M5 meet acceptance.")
    sys.exit(0)
else:
    warn(f"{len(results)-ok_count} stage(s) need attention. See details above.")
    sys.exit(1)
