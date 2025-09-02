import csv, sqlite3, sys, os
from pathlib import Path

DB_PATH = r"C:\teevra18\data\teevra18.db"
CSV_PATH = r"C:\teevra18\data\api-scrip-master-detailed.csv"
OUT_SQL = r"C:\teevra18\data\universe_seed.sql"

# Config — adjust if your CSV uses different column names
COL = {
    "exch_id": "EXCH_ID",
    "segment": "SEGMENT",
    "instrument": "INSTRUMENT",
    "security_id": "SECURITY_ID",
    "underlying_symbol": "UNDERLYING_SYMBOL",
    "symbol_name": "SYMBOL_NAME",
    "display_name": "DISPLAY_NAME",
    # Optional/derived
    "expiry": None,        # try EXPIRY / EXPIRY_DATE / expiryCode
    "expiry_code": "expiryCode",
    "strike": None,        # try STRIKE / STRIKE_PRICE
    "opt_type": None,      # try OPTION_TYPE / OPT_TYPE / CALL_PUT
    "lot_size": None,      # try LOT_SIZE / LOT
}

# Derive optional column mappings
def detect_optional(headers, candidates):
    for c in candidates:
        if c in headers:
            return c
    return None

# Ensure output dir
Path(os.path.dirname(OUT_SQL)).mkdir(parents=True, exist_ok=True)

with open(CSV_PATH, "r", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    headers = reader.fieldnames or []

    # best-effort map optional columns
    if COL["expiry"] is None:
        COL["expiry"] = detect_optional(headers, ["EXPIRY", "EXPIRY_DATE", "expiry", "expiry_date"])
    if COL["strike"] is None:
        COL["strike"] = detect_optional(headers, ["STRIKE", "STRIKE_PRICE", "strike", "strike_price"])
    if COL["opt_type"] is None:
        COL["opt_type"] = detect_optional(headers, ["OPTION_TYPE", "OPT_TYPE", "CALL_PUT", "OPTIONTYPE", "opt_type"])
    if COL["lot_size"] is None:
        COL["lot_size"] = detect_optional(headers, ["LOT_SIZE", "LOT", "lot_size", "marketlot"])

    rows = list(reader)

# Collect sets
nifty50_equities = set()
underlyings = set()         # (UNDERLYING_SYMBOL)
index_underlyings = {"NIFTY", "BANKNIFTY"}

# Identify NIFTY50 equities from SEGMENT=E & INSTRUMENT=EQUITY (common in master files)
for r in rows:
    seg = r.get(COL["segment"], "")
    inst = r.get(COL["instrument"], "")
    sym = r.get(COL["symbol_name"], "")
    und = r.get(COL["underlying_symbol"], "")
    if seg == "E" and inst == "EQUITY" and sym:
        nifty50_equities.add(sym)

# Build underlyings from:
# - indices (NIFTY, BANKNIFTY)
# - equities (NIFTY50 set above)
# - any derivative row's UNDERLYING_SYMBOL as fallback
for r in rows:
    und = r.get(COL["underlying_symbol"], "")
    if und:
        underlyings.add(und)

# Normalize underlyings to include indices + NIFTY50 equities
all_underlyings = set(nifty50_equities) | index_underlyings

# Prepare SQL buffers
sql = []
sql.append("-- Create tables if not exists")
sql.append("""
CREATE TABLE IF NOT EXISTS universe_underlyings(
  underlying_symbol TEXT PRIMARY KEY,
  display_name TEXT,
  category TEXT,             -- 'INDEX' or 'EQUITY'
  exch_id TEXT,
  segment TEXT
);
""".strip())

sql.append("""
CREATE TABLE IF NOT EXISTS universe_derivatives(
  der_key TEXT PRIMARY KEY,  -- exch_id|instrument|security_id
  exch_id TEXT,
  segment TEXT,
  instrument TEXT,
  security_id TEXT,
  underlying_symbol TEXT,
  symbol_name TEXT,
  display_name TEXT,
  expiry TEXT,
  expiry_code TEXT,
  strike REAL,
  opt_type TEXT,
  lot_size INTEGER
);
""".strip())

# Upsert helpers
sql.append("""
-- UPSERT helpers (SQLite 3.24+)
""")

# Insert/Upsert underlyings (indices + NIFTY50 equities)
for r in rows:
    sym = r.get(COL["symbol_name"], "")
    disp = r.get(COL["display_name"], "")
    und = r.get(COL["underlying_symbol"], "")
    seg = r.get(COL["segment"], "")
    exch = r.get(COL["exch_id"], "")

    # Index underlyings: ensure rows present
    for idx in index_underlyings:
        if idx == r.get(COL["underlying_symbol"], "") or idx == sym:
            sql.append(f"""
INSERT INTO universe_underlyings(underlying_symbol, display_name, category, exch_id, segment)
VALUES ('{idx}','{idx}','INDEX','{exch}','{seg}')
ON CONFLICT(underlying_symbol) DO NOTHING;
""".strip())

    # Equity underlyings: only if part of NIFTY50 set
    if sym in all_underlyings and sym not in index_underlyings:
        # 'display_name' fallback to sym if missing
        dn = disp if disp else sym
        sql.append(f"""
INSERT INTO universe_underlyings(underlying_symbol, display_name, category, exch_id, segment)
VALUES ('{sym}','{dn}','EQUITY','{exch}','{seg}')
ON CONFLICT(underlying_symbol) DO NOTHING;
""".strip())

# Insert/Upsert derivatives for:
# - FUTIDX/OPTIDX of NIFTY/BANKNIFTY
# - FUTSTK/OPTSTK for NIFTY50
def safe(v):
    if v is None:
        return ""
    return str(v).replace("'", "''")

for r in rows:
    exch = safe(r.get(COL["exch_id"], ""))
    seg  = safe(r.get(COL["segment"], ""))
    inst = safe(r.get(COL["instrument"], ""))
    sec  = safe(r.get(COL["security_id"], ""))
    und  = safe(r.get(COL["underlying_symbol"], ""))
    sym  = safe(r.get(COL["symbol_name"], ""))
    dnm  = safe(r.get(COL["display_name"], ""))

    exp  = safe(r.get(COL["expiry"], "")) if COL["expiry"] else ""
    expc = safe(r.get(COL["expiry_code"], "")) if COL["expiry_code"] else ""
    strike = r.get(COL["strike"], "")
    strike = safe(strike) if strike != "" else "NULL"
    optt   = safe(r.get(COL["opt_type"], "")) if COL["opt_type"] else ""
    lot    = r.get(COL["lot_size"], "")
    lot    = safe(lot) if lot != "" else "NULL"

    # filter only relevant instruments
    allowed = {"FUTIDX","OPTIDX","FUTSTK","OPTSTK"}
    if inst not in allowed:
        continue

    # keep only index derivatives for NIFTY/BANKNIFTY, and stock derivatives for NIFTY50 equities
    if inst in {"FUTIDX","OPTIDX"} and und not in index_underlyings:
        continue
    if inst in {"FUTSTK","OPTSTK"} and und not in (all_underlyings - index_underlyings):
        continue

    der_key = f"{exch}|{inst}|{sec}"
    sql.append(f"""
INSERT INTO universe_derivatives(der_key, exch_id, segment, instrument, security_id, underlying_symbol, symbol_name, display_name, expiry, expiry_code, strike, opt_type, lot_size)
VALUES ('{der_key}','{exch}','{seg}','{inst}','{sec}','{und}','{sym}','{dnm}','{exp}','{expc}',{strike},'{optt}',{lot})
ON CONFLICT(der_key) DO NOTHING;
""".strip())

# Write file
with open(OUT_SQL, "w", encoding="utf-8") as out:
    out.write("-- AUTO-GENERATED FROM api-scrip-master-detailed.csv\n\n")
    out.write("\n\n".join(sql))

print(f"[OK] Wrote SQL to {OUT_SQL}")
print("[Hint] Apply with:")
print(f'sqlite3 "{DB_PATH}" ".read {OUT_SQL}"')
