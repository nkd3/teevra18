import argparse, json, sqlite3, time, pathlib, sys
from datetime import datetime, timezone

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def log_open(path):
    p = pathlib.Path(path); p.parent.mkdir(parents=True, exist_ok=True)
    def _log(msg):
        line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}\n"
        with open(p, "a", encoding="utf-8") as f:
            f.write(line)
        print(msg)
    return _log

# ----------------- Core TA -----------------

def ema(series, n):
    if not series or len(series) < n:
        return []
    k = 2/(n+1)
    out = []
    ema_val = sum(series[:n])/n
    out.extend([None]*(n-1))
    out.append(ema_val)
    for px in series[n:]:
        ema_val = px*k + ema_val*(1-k)
        out.append(ema_val)
    return out

def pick_entry_from_cross(c_prices, eps=1e-6, recent_bars=0):
    # Strict last-bar cross; recent_bars==0 means only last bar
    e9  = ema(c_prices, 9)
    e21 = ema(c_prices, 21)
    if not e9 or not e21 or len(c_prices) < 22:
        return None
    a9_prev, a21_prev = e9[-2], e21[-2]
    a9_now,  a21_now  = e9[-1], e21[-1]
    if a9_prev is not None and a21_prev is not None:
        if (a9_prev - a21_prev) <= eps and (a9_now - a21_now) > eps:
            return "BUY"
        if (a9_prev - a21_prev) >= -eps and (a9_now - a21_now) < -eps:
            return "SELL"
    # Optional: allow a cross within N previous bars (keep 0 for production)
    for k in range(2, 2 + max(0, recent_bars)):
        if len(e9) < k+1 or len(e21) < k+1: break
        a9_prev_k, a21_prev_k = e9[-(k+1)], e21[-(k+1)]
        if a9_prev_k is None or a21_prev_k is None: continue
        if (a9_prev_k - a21_prev_k) <= eps and (a9_now - a21_now) > eps:
            return "BUY"
        if (a9_prev_k - a21_prev_k) >= -eps and (a9_now - a21_now) < -eps:
            return "SELL"
    return None

def compute_rr(close_price, sl_abs_cap=1000, rr_min=2.0):
    # 0.2% floor SL, capped at ₹1000 per lot; TP = rr_min * SL
    sl = min(1000.0, max(100.0, close_price*0.002))
    tp = sl * rr_min
    rr = tp / sl if sl > 0 else 0
    return rr, sl, tp

# ------------- DB helpers / pickers -------------

def get_last_close(cur, symbol):
    cur.execute("SELECT c FROM candles_1m_std WHERE symbol=? ORDER BY ts DESC LIMIT 1;", (symbol,))
    row = cur.fetchone()
    return row[0] if row else None

def pick_targets_for_index(cur, driver, last_px, want=("FUTIDX","OPTIDX")):
    """
    Index path: match instrument_type variants (FUTIDX/FUT/...) and
    fuzzy-match underlying_symbol using LIKE and normalized compare.
    """
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='universe_derivatives';")
    if not cur.fetchone():
        return []

    # Accept lots of variants for instrument_type
    fut_types = ("FUTIDX","FUT","FUTURE","FUTURES","IDXFUT","INDEX FUTURE","INDEXFUT")
    opt_types = ("OPTIDX","OPT","OPTION","OPTIONS","IDXOPT","INDEX OPTION","INDEXOPT")

    # Underlying synonyms and LIKE patterns
    synonyms = {
        "NIFTY":      ["NIFTY", "NIFTY 50", "NIFTY50", "NSEI"],
        "BANKNIFTY":  ["BANKNIFTY", "BANK NIFTY", "NIFTY BANK", "BANK-NIFTY", "BANKNIFTY 50"]
    }
    und_list = synonyms.get(driver, [driver])

    # Build WHERE: LIKE patterns; also compare normalized (remove spaces/hyphens)
    like_params = [f"%{u}%" for u in und_list]
    like_clause = " OR ".join(["UPPER(underlying_symbol) LIKE UPPER(?)" for _ in und_list])
    norm_list = [u.replace(" ","").replace("-","").upper() for u in und_list]
    norm_clause = " OR ".join(["REPLACE(REPLACE(UPPER(underlying_symbol),' ',''),'-','')=?" for _ in norm_list])

    picks = []

    # FUT nearest expiry
    if "FUTIDX" in want or "FUT" in want:
        cur.execute(f"""
            SELECT tradingsymbol, symbol, expiry, exchange, lot_size, underlying_symbol, instrument_type
            FROM universe_derivatives
            WHERE UPPER(instrument_type) IN ({",".join(["?"]*len(fut_types))})
              AND ( {like_clause} OR {norm_clause} )
              AND (enabled IS NULL OR enabled=1)
            ORDER BY expiry ASC
            LIMIT 1
        """, tuple(t.upper() for t in fut_types) + tuple(like_params) + tuple(norm_list))
        r = cur.fetchone()
        if r:
            tsym, sym, exp, exch, lot, und, itype = r
            disp = tsym or sym or (f"{driver} FUT {exp}" if exp else f"{driver} FUT")
            if (disp.upper() == driver.upper()) and exp:
                disp = f"{driver} FUT {exp}"
            picks.append({"type":"FUTIDX","symbol":disp,"expiry":exp,"exchange":exch,"lot_size":lot})

    # OPTIDX ATM (may be empty until options are loaded)
    if ("OPTIDX" in want) and (last_px is not None):
        cur.execute(f"""
            SELECT tradingsymbol, symbol, expiry, strike, option_type, exchange, lot_size
            FROM universe_derivatives
            WHERE UPPER(instrument_type) IN ({",".join(["?"]*len(opt_types))})
              AND ( {like_clause} OR {norm_clause} )
              AND (enabled IS NULL OR enabled=1)
            ORDER BY ABS(COALESCE(strike, 0) - ?), expiry ASC
            LIMIT 30
        """, tuple(t.upper() for t in opt_types) + tuple(like_params) + tuple(norm_list) + (last_px,))
        rows = cur.fetchall() or []
        ce = next((r for r in rows if str(r[4]).upper() in ("CE","C","CALL")), None)
        pe = next((r for r in rows if str(r[4]).upper() in ("PE","P","PUT")), None)
        if ce:
            tsym, sym, exp, strike, opt, exch, lot = ce
            disp = tsym or sym or (f"{driver} {int(strike) if strike else ''} CE {exp}" if exp else f"{driver} CE")
            picks.append({"type":"OPTIDX","symbol":disp,"expiry":exp,"strike":strike,"opt_type":"CE","exchange":exch,"lot_size":lot})
        if pe:
            tsym, sym, exp, strike, opt, exch, lot = pe
            disp = tsym or sym or (f"{driver} {int(strike) if strike else ''} PE {exp}" if exp else f"{driver} PE")
            picks.append({"type":"OPTIDX","symbol":disp,"expiry":exp,"strike":strike,"opt_type":"PE","exchange":exch,"lot_size":lot})

    return picks

def pick_targets_for_equity(cur, stock, last_px, want=("CASH","FUTSTK","OPTSTK")):
    """
    Equity path: CASH via universe_equities (if present) else <stock>-EQ; FUTSTK/OPTSTK via universe_derivatives.
    """
    picks = []

    # CASH
    if "CASH" in want:
        cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='universe_equities';")
        if cur.fetchone():
            cur.execute("PRAGMA table_info(universe_equities);")
            eq_cols = [r[1] for r in cur.fetchall()]
            c_sym  = "tradingsymbol" if "tradingsymbol" in eq_cols else ("symbol" if "symbol" in eq_cols else None)
            c_exch = "exchange" if "exchange" in eq_cols else None
            if c_sym:
                cur.execute(f"""
                    SELECT {c_sym}{','+c_exch if c_exch else ''}
                    FROM universe_equities
                    WHERE UPPER(symbol)=UPPER(?) OR UPPER(tradingsymbol)=UPPER(?) OR UPPER(name)=UPPER(?)
                    LIMIT 1
                """, (stock, stock, stock))
                row = cur.fetchone()
                if row:
                    disp = row[0]
                    exch = row[1] if c_exch and len(row)>1 else None
                    picks.append({"type":"CASH","symbol":disp,"exchange":exch})
        if not any(p['type']=="CASH" for p in picks):
            picks.append({"type":"CASH","symbol":f"{stock}-EQ","exchange":"NSE"})

    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='universe_derivatives';")
    has_der = bool(cur.fetchone())

    # FUTSTK
    if has_der and "FUTSTK" in want:
        cur.execute("""
            SELECT tradingsymbol, symbol, expiry, exchange, lot_size
            FROM universe_derivatives
            WHERE UPPER(instrument_type) IN ('FUTSTK','FUTSTOCK','STKFUT','STOCK FUTURE')
              AND UPPER(underlying_symbol)=UPPER(?)
              AND (enabled IS NULL OR enabled=1)
            ORDER BY expiry ASC
            LIMIT 1
        """, (stock,))
        r = cur.fetchone()
        if r:
            tsym, sym, exp, exch, lot = r
            disp = tsym or sym or f"{stock} FUTSTK"
            if (disp.upper() == stock.upper()) and exp:
                disp = f"{stock} FUTSTK {exp}"
            picks.append({"type":"FUTSTK","symbol":disp,"expiry":exp,"exchange":exch,"lot_size":lot})

    # OPTSTK
    if has_der and "OPTSTK" in want and last_px is not None:
        cur.execute("""
            SELECT tradingsymbol, symbol, expiry, strike, option_type, exchange, lot_size
            FROM universe_derivatives
            WHERE UPPER(instrument_type) IN ('OPTSTK','OPTSTOCK','STKOPT','STOCK OPTION')
              AND UPPER(underlying_symbol)=UPPER(?)
              AND (enabled IS NULL OR enabled=1)
            ORDER BY ABS(COALESCE(strike,0) - ?), expiry ASC
            LIMIT 30
        """, (stock, last_px))
        rows = cur.fetchall() or []
        ce = next((r for r in rows if str(r[4]).upper() in ("CE","C","CALL")), None)
        pe = next((r for r in rows if str(r[4]).upper() in ("PE","P","PUT")), None)
        if ce:
            tsym, sym, exp, strike, opt, exch, lot = ce
            disp = tsym or sym or f"{stock} CE"
            picks.append({"type":"OPTSTK","symbol":disp,"expiry":exp,"strike":strike,"opt_type":"CE","exchange":exch,"lot_size":lot})
        if pe:
            tsym, sym, exp, strike, opt, exch, lot = pe
            disp = tsym or sym or f"{stock} PE"
            picks.append({"type":"OPTSTK","symbol":disp,"expiry":exp,"strike":strike,"opt_type":"PE","exchange":exch,"lot_size":lot})
    return picks

# ------------- Duplicate guard -------------

def already_signalled_today(cur, driver_or_stock, reason="ema9/21_cross_v1"):
    cur.execute("""
        SELECT 1 FROM signals
        WHERE date(ts)=date('now')
          AND driver=?
          AND reason=?
          AND COALESCE(state,'') <> 'DUPED'
        LIMIT 1;
    """, (driver_or_stock, reason))
    return cur.fetchone() is not None

def safe_insert_signal(conn, cur, payload, log):
    """
    payload: (ts, symbol, driver, action, rr, sl, tp, reason)
    Tries normal INSERT, falls back to INSERT OR IGNORE so the core never crashes on the UNIQUE index.
    """
    try:
        cur.execute("""
            INSERT INTO signals(ts, symbol, driver, action, rr, sl, tp, reason, state)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'NEW');
        """, payload)
        conn.commit()
        return True, "inserted"
    except sqlite3.IntegrityError:
        cur.execute("""
            INSERT OR IGNORE INTO signals(ts, symbol, driver, action, rr, sl, tp, reason, state)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'NEW');
        """, payload)
        conn.commit()
        if conn.total_changes == 0:
            return False, "ignored_by_index"
        return True, "inserted_via_ignore"

# ----------------- Main -----------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--matrix", required=True)
    p.add_argument("--rr_profile", required=True)
    p.add_argument("--universe", required=True)
    p.add_argument("--log", required=True)
    args = p.parse_args()

    log = log_open(args.log)
    log("[INIT] Strategy Core starting...")
    log(f"[ARGS] db={args.db}, matrix={args.matrix}, rr_profile={args.rr_profile}, universe={args.universe}")

    # Load matrix / risk
    try:
        with open(args.matrix, "r", encoding="utf-8") as f:
            matrix = json.load(f)
        drivers = matrix.get("drivers", [])
        equities = matrix.get("equities", [])
        risk = matrix.get("risk", {"rr_min":2.0, "sl_cap_per_lot":1000, "max_trades_per_day":5})
        targets_cfg = matrix.get("targets", {})
        log(f"[LOAD] Strategy matrix drivers={drivers} risk={risk}")
        if equities:
            log(f"[LOAD] equities={len(equities)}")
    except Exception as e:
        log(f"[ERR] Failed to load matrix: {e}")
        sys.exit(1)

    # Connect DB
    try:
        conn = sqlite3.connect(args.db)
        cur = conn.cursor()
        log("[DB] Connected")
    except Exception as e:
        log(f"[ERR] DB connect failed: {e}")
        sys.exit(1)

    # Today's breaker count (checked again in-loop)
    cur.execute("SELECT COUNT(*) FROM signals WHERE date(ts)=date('now') AND COALESCE(state,'') <> 'DUPED';")
    made_today = cur.fetchone()[0]
    made_this_run = 0

    # -------- Indices loop --------
    for driver in drivers:
        cur.execute("""
            SELECT c FROM candles_1m_std
            WHERE symbol=?
            ORDER BY ts DESC
            LIMIT 40;
        """, (driver,))
        rows = cur.fetchall()
        closes = [r[0] for r in rows][::-1]
        if len(closes) < 22:
            log(f"[SKIP] {driver}: Need ≥22 candles."); continue

        e9_dbg  = ema(closes, 9)
        e21_dbg = ema(closes, 21)
        log(f"[DBG] {driver} closes[-3:]={closes[-3:]}")
        log(f"[DBG] {driver} ema9/ema21 prev={e9_dbg[-2] if e9_dbg else None} / {e21_dbg[-2] if e21_dbg else None}")
        log(f"[DBG] {driver} ema9/ema21 last={e9_dbg[-1] if e9_dbg else None} / {e21_dbg[-1] if e21_dbg else None}")

        side = pick_entry_from_cross(closes, eps=1e-6, recent_bars=0)
        if not side:
            log(f"[INFO] {driver}: no cross"); continue

        last_close = closes[-1]
        rr, sl, tp = compute_rr(last_close, sl_abs_cap=risk.get("sl_cap_per_lot",1000), rr_min=risk.get("rr_min",2.0))
        if rr < risk.get("rr_min",2.0) or sl > risk.get("sl_cap_per_lot",1000):
            log(f"[REJECT] {driver}: rr/sl gate rr={rr:.2f} sl={sl:.0f}"); continue

        if made_today >= risk.get("max_trades_per_day",5):
            log(f"[PAUSE] breaker hit ({made_today})"); break

        want_types = tuple(t.upper() for t in targets_cfg.get(driver, ["FUTIDX","OPTIDX"]))
        last_px = get_last_close(cur, driver)
        picks = pick_targets_for_index(cur, driver, last_px, want=want_types)
        target_symbol = (picks[0]["symbol"] if picks else f"{driver}_FUT_DEMO")

        ts = now_iso()
        reason = "ema9/21_cross_v1"

        if already_signalled_today(cur, driver, reason):
            log(f"[SKIP] Duplicate for today: {driver} / {reason}")
            continue

        ok, how = safe_insert_signal(conn, cur, (ts, target_symbol, driver,
                                                 "BUY" if side=="BUY" else "SELL",
                                                 rr, sl, tp, reason), log)
        if ok:
            made_today += 1
            made_this_run += 1
            log(f"[SIGNAL] {driver} -> {target_symbol} {side} rr={rr:.2f} sl={sl:.0f} tp={tp:.0f}")
        else:
            log(f"[SKIP] Duplicate (ignored by index): {driver} / {reason}")

    # -------- Equities loop --------
    for stock in equities:
        if made_today >= risk.get("max_trades_per_day",5):
            log(f"[PAUSE] breaker hit ({made_today})"); break

        cur.execute("""
            SELECT c FROM candles_1m_std
            WHERE symbol=?
            ORDER BY ts DESC
            LIMIT 40;
        """, (stock,))
        rows = cur.fetchall()
        closes = [r[0] for r in rows][::-1]
        if len(closes) < 22:
            log(f"[SKIP] {stock}: Need ≥22 candles."); continue

        e9_dbg  = ema(closes, 9)
        e21_dbg = ema(closes, 21)
        log(f"[DBG] {stock} closes[-3:]={closes[-3:]}")
        log(f"[DBG] {stock} ema9/ema21 prev={e9_dbg[-2] if e9_dbg else None} / {e21_dbg[-2] if e21_dbg else None}")
        log(f"[DBG] {stock} ema9/ema21 last={e9_dbg[-1] if e9_dbg else None} / {e21_dbg[-1] if e21_dbg else None}")

        side = pick_entry_from_cross(closes, eps=1e-6, recent_bars=0)
        if not side:
            log(f"[INFO] {stock}: no cross"); continue

        last_close = closes[-1]
        rr, sl, tp = compute_rr(last_close, sl_abs_cap=risk.get("sl_cap_per_lot",1000), rr_min=risk.get("rr_min",2.0))
        if rr < risk.get("rr_min",2.0) or sl > risk.get("sl_cap_per_lot",1000):
            log(f"[REJECT] {stock}: rr/sl gate rr={rr:.2f} sl={sl:.0f}"); continue

        want_types = tuple(t.upper() for t in targets_cfg.get(stock, targets_cfg.get("EQUITY_DEFAULT", ["CASH","FUTSTK","OPTSTK"])))
        last_px = get_last_close(cur, stock)
        picks = pick_targets_for_equity(cur, stock, last_px, want=want_types)
        target_symbol = (picks[0]["symbol"] if picks else f"{stock}-EQ")

        ts = now_iso()
        reason = "ema9/21_cross_v1"

        if already_signalled_today(cur, stock, reason):
            log(f"[SKIP] Duplicate for today: {stock} / {reason}")
            continue

        ok, how = safe_insert_signal(conn, cur, (ts, target_symbol, stock,
                                                 "BUY" if side=="BUY" else "SELL",
                                                 rr, sl, tp, reason), log)
        if ok:
            made_today += 1
            made_this_run += 1
            log(f"[SIGNAL] {stock} -> {target_symbol} {side} rr={rr:.2f} sl={sl:.0f} tp={tp:.0f}")
        else:
            log(f"[SKIP] Duplicate (ignored by index): {stock} / {reason}")

    conn.close()
    if made_this_run == 0:
        log("[INFO] Completed scan: no eligible signals.")
    log("[EXIT] Strategy Core stopped.")

if __name__ == "__main__":
    main()
