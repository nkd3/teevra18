# -*- coding: utf-8 -*-
"""
Fail-safe resolver for:
  1) IDX_I : NIFTY 50 (spot index)
  2) NSE_FNO: FUTIDX NIFTY (near = nearest non-expired; if not parseable, best-match fallback)

CSV (locked): C:\teevra18\data\api-scrip-master-detailed.csv
"""

import os, sys, csv, sqlite3, re
import datetime as dt
from pathlib import Path

DB_PATH  = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
CSV_PATH = Path(r"C:\teevra18\data\api-scrip-master-detailed.csv")

# ---------- date parsing ----------
MON = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
TODAY = dt.date.today()

def parse_date_any(s):
    if s is None: return None
    s = str(s).strip()
    if not s or s.upper() in {"NA","N/A","NONE"}: return None
    # 1) numeric YYYYMMDD / DDMMYYYY
    if s.isdigit() and len(s) == 8:
        for fmt in ("%Y%m%d","%d%m%Y"):
            try: return dt.datetime.strptime(s, fmt).date()
            except: pass
    # 2) common formats
    for fmt in ("%Y-%m-%d","%d-%b-%Y","%d-%b-%y","%d/%m/%Y","%d/%m/%y","%d-%m-%Y"):
        try: return dt.datetime.strptime(s, fmt).date()
        except: pass
    return None

def parse_expiry_from_symbol(sym):
    s = "" if sym is None else str(sym).upper()
    # DD-MMM-YYYY or DD-MMM-YY
    m = re.search(r'(\d{1,2})[-\s]?([A-Z]{3})[-\s]?(\d{2,4})', s)
    if m:
        dd, mon, yy = m.groups()
        if mon in MON:
            year = int(yy) if len(yy)==4 else 2000+int(yy)
            try: return dt.date(year, MON[mon], int(dd))
            except: pass
    # YYYY[-/]MM[-/]DD
    m = re.search(r'(20\d{2})[-/]?(\d{2})[-/]?(\d{2})', s)
    if m:
        y, mo, d = m.groups()
        try: return dt.date(int(y), int(mo), int(d))
        except: pass
    # DDMMYYYY contiguous
    m = re.search(r'(\d{2})(\d{2})(\d{4})', s)
    if m:
        d, mo, y = m.groups()
        try: return dt.date(int(y), int(mo), int(d))
        except: pass
    return None

# ---------- DB helpers ----------
def ensure_table():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
    CREATE TABLE IF NOT EXISTS instrument_map(
      key TEXT PRIMARY KEY,
      exchange_segment TEXT NOT NULL,
      instrument TEXT,
      symbol TEXT,
      underlying TEXT,
      expiry_code INTEGER,
      security_id INTEGER NOT NULL,
      updated_utc TEXT DEFAULT (datetime('now'))
    );""")
    con.commit(); con.close()

def upsert(key, eseg, instr, sym, und, exp, sid):
    con = sqlite3.connect(DB_PATH)
    con.execute("""
      INSERT INTO instrument_map(key,exchange_segment,instrument,symbol,underlying,expiry_code,security_id)
      VALUES(?,?,?,?,?,?,?)
      ON CONFLICT(key) DO UPDATE SET
        exchange_segment=excluded.exchange_segment,
        instrument=excluded.instrument,
        symbol=excluded.symbol,
        underlying=excluded.underlying,
        expiry_code=excluded.expiry_code,
        security_id=excluded.security_id,
        updated_utc=datetime('now');""",
        (key, eseg, instr, sym, und, (None if exp is None else int(exp)), int(sid))
    )
    con.commit(); con.close()

# ---------- utility ----------
def U(x): return ("" if x is None else str(x)).strip().upper()

def find_security_id(row):
    """Try many header heuristics to get a numeric Security ID."""
    for k,v in row.items():
        ku = (k or "").upper()
        if any(t in ku for t in ("SECURITY","SECID","INSTRUMENTIDENTIFIER","INSTRUMENT_ID","SM_SECURITY","SEM_SM_SECURITY")):
            val = str(v).strip()
            # keep integers or integer-like
            if val and re.fullmatch(r"\d{1,10}", val):
                return val
    # fallback: look for any purely numeric-ish column
    for k,v in row.items():
        val = str(v).strip()
        if re.fullmatch(r"\d{3,10}", val):
            return val
    return None

def row_text(row):
    """Concatenate all values for fuzzy search."""
    return " ".join([U(v) for v in row.values()])

def main():
    if not CSV_PATH.exists():
        print("CSV not found:", CSV_PATH); sys.exit(1)

    ensure_table()

    # open with tolerant encodings
    reader = None
    for enc in ("utf-8","utf-8-sig","latin-1"):
        try:
            f = open(CSV_PATH, newline="", encoding=enc)
            reader = csv.DictReader(f)
            break
        except UnicodeDecodeError:
            try: f.close()
            except: pass
            continue
    if reader is None:
        print("Unable to read CSV in utf-8/latin-1"); sys.exit(1)

    idx_sid = None
    fut_candidates = []  # list[(expiry_date or None, sid, preview_dict)]
    fallback_candidates = []  # where expiry couldn't be parsed

    # Pass 1: try to find a clean NIFTY50 index row
    for row in reader:
        up = { (k or "").upper(): ("" if v is None else str(v)) for k,v in row.items() }
        sym = U(up.get("SYMBOL_NAME") or up.get("SYMBOL") or up.get("DISPLAY_NAME") or up.get("TRADING_SYMBOL"))
        inst= U(up.get("INSTRUMENT") or up.get("SEM_INSTRUMENT_NAME") or up.get("PRODUCT"))
        # simple index detection
        if ("NIFTY 50" in sym) and ("INDEX" in inst or inst == "" or "SPOT" in inst or "IDX" in inst):
            sid = find_security_id(up)
            if sid:
                idx_sid = sid
                upsert("IDX_I:NIFTY50", "IDX_I", "INDEX", "NIFTY 50", "", None, sid)
        # build future candidates — we’ll also do a 2nd pass with full-text scan
        txt = row_text(up)
        if ("NIFTY" in txt) and ("FUT" in txt):
            sid = find_security_id(up)
            if sid:
                # try expiry from any plausible field
                expiry = None
                # try common expiry columns first
                for key in ("EXPIRY","EXPIRY_DATE","EXP_DATE","EXPIRY_CODE","EXPDATE","EXPIRYDT"):
                    if key in up and up[key]:
                        expiry = parse_date_any(up[key]) or expiry
                # if still none, derive from symbol text
                expiry = expiry or parse_expiry_from_symbol(sym) or parse_expiry_from_symbol(txt)
                if expiry and expiry >= TODAY:
                    fut_candidates.append((expiry, sid, {"SYM": sym[:24], "EXP": str(expiry)}))
                else:
                    fallback_candidates.append((None, sid, {"SYM": sym[:24], "EXP": up.get("EXPIRY_DATE","")[:16]}))

    # If reader was consumed, reopen for future robustness (some Python versions keep it ok).
    try: f.close()
    except: pass

    # If nothing yet, do a 2nd pass purely text-based (some dumps use very alien headers)
    if not fut_candidates and not fallback_candidates:
        for enc in ("utf-8","utf-8-sig","latin-1"):
            try:
                f2 = open(CSV_PATH, newline="", encoding=enc)
                r2 = csv.DictReader(f2)
                for row in r2:
                    up = { (k or "").upper(): ("" if v is None else str(v)) for k,v in row.items() }
                    txt = row_text(up)
                    if ("NIFTY" in txt) and ("FUT" in txt):
                        sid = find_security_id(up)
                        if not sid: continue
                        sym = U(up.get("SYMBOL_NAME") or up.get("SYMBOL") or up.get("DISPLAY_NAME") or up.get("TRADING_SYMBOL"))
                        expiry = None
                        # scan *all* values for any date
                        for v in up.values():
                            expiry = parse_date_any(v) or expiry
                        expiry = expiry or parse_expiry_from_symbol(sym) or parse_expiry_from_symbol(txt)
                        if expiry and expiry >= TODAY:
                            fut_candidates.append((expiry, sid, {"SYM": sym[:24], "EXP": str(expiry)}))
                        else:
                            fallback_candidates.append((None, sid, {"SYM": sym[:24], "EXP": ""}))
                try: f2.close()
                except: pass
                break
            except UnicodeDecodeError:
                try: f2.close()
                except: pass
                continue

    # Choose near-month by actual expiry if available; else pick first fallback
    fut_sid = None
    if fut_candidates:
        fut_candidates.sort(key=lambda t: t[0])
        best = fut_candidates[0]
        fut_sid = best[1]
        expcode = int(best[0].strftime("%Y%m%d"))
        upsert("NSE_FNO:FUTIDX:NIFTY:NEAR", "NSE_FNO", "FUTIDX", "NIFTY", "NIFTY", expcode, fut_sid)
    elif fallback_candidates:
        # No parseable expiry — still map best candidate (expiry_code=NULL)
        best = fallback_candidates[0]
        fut_sid = best[1]
        upsert("NSE_FNO:FUTIDX:NIFTY:NEAR", "NSE_FNO", "FUTIDX", "NIFTY", "NIFTY", None, fut_sid)

    print("Resolved:", {
        "IDX_I:NIFTY50": idx_sid,
        "NSE_FNO:FUTIDX:NIFTY:NEAR": fut_sid
    })

    # Helpful debug if still None
    if fut_sid is None:
        print("\n[DEBUG] Could not find any NIFTY futures row. Please run this probe and share the output:")
        print(r"""
$py = @"
import csv
p = r'C:\teevra18\data\api-scrip-master-detailed.csv'
with open(p, encoding='utf-8', newline='') as f:
    rd = csv.DictReader(f)
    print('HEADERS:', [h for h in rd.fieldnames])
    c=0
    for r in rd:
        up = { (k or '').upper(): ('' if v is None else str(v)) for k,v in r.items() }
        line = " ".join(up.values()).upper()
        if 'NIFTY' in line:
            if 'FUT' in line or 'FUTIDX' in line:
                print({k: up.get(k,'') for k in list(up)[:20]})
                c+=1
                if c>=8: break
"@
$py | python -
        """)
if __name__ == "__main__":
    main()
