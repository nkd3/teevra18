# C:\teevra18\tools\find_security_id_nse.py
# Find Dhan SECURITY_ID strictly for NSE (NSE_EQ / NSE_FNO).
# Searches DISPLAY_NAME / SYMBOL_NAME / UNDERLYING_SYMBOL, prints API-ready segment.

import argparse, re, sys
import pandas as pd

def N(x): return re.sub(r'[^A-Z0-9]', '', str(x).upper()) if x is not None else ""

def map_exchange_segment(seg:str, instrument:str) -> str:
    s, i = N(seg), N(instrument)
    # NSE cash vs derivatives
    if s in {"E","EQ","CM","EQUITY","CASH"}: return "NSE_EQ"
    if s in {"D","FO","FNO","F&O","DERIVATIVE","DERIVATIVES"}: return "NSE_FNO"
    # Fallback: derive from instrument
    if i.startswith("OPT") or i.startswith("FUT"): return "NSE_FNO"
    return "NSE_EQ"

ap = argparse.ArgumentParser()
ap.add_argument("--csv", required=True)
ap.add_argument("--symbol", required=True, help="Name to search (RELIANCE, INFY, etc.)")
ap.add_argument("--segment", choices=["E","D","ANY"], default="ANY", help="E=Equity, D=Derivatives, ANY=both")
ap.add_argument("--show-top", type=int, default=15)
args = ap.parse_args()

df = pd.read_csv(args.csv, dtype=str, low_memory=False)

need_cols = ["EXCH_ID","SEGMENT","SECURITY_ID"]
for c in need_cols:
    if c not in df.columns:
        print(f"[ERROR] CSV missing required column: {c}. Found: {list(df.columns)}")
        sys.exit(2)

# Filter to NSE only
df_nse = df[df["EXCH_ID"].astype(str).str.upper() == "NSE"].copy()
if df_nse.empty:
    print("[ERROR] Your CSV has 0 rows for EXCH_ID = NSE. Please load a master that includes NSE instruments.")
    # Show sample exchanges present
    print("\nExchanges found in your CSV (counts):")
    print(df["EXCH_ID"].value_counts(dropna=False).head(10).to_string())
    sys.exit(3)

# Optional segment filter
seg_req = args.segment.upper()
if seg_req in ("E","D"):
    df_nse = df_nse[df_nse["SEGMENT"].astype(str).str.upper().str.startswith(seg_req)]
    if df_nse.empty:
        print(f"[ERROR] No NSE rows for SEGMENT={seg_req}.")
        print("\nAvailable NSE segments+counts:")
        print(df[df["EXCH_ID"].astype(str).str.upper()=="NSE"]["SEGMENT"].value_counts().to_string())
        sys.exit(4)

# Name columns to search
name_cols = [c for c in ["DISPLAY_NAME","SYMBOL_NAME","UNDERLYING_SYMBOL","TRADING_SYMBOL","SYMBOL"] if c in df_nse.columns]
if not name_cols:
    print("[ERROR] CSV lacks any name columns (DISPLAY_NAME/SYMBOL_NAME/UNDERLYING_SYMBOL/TRADING_SYMBOL/SYMBOL).")
    sys.exit(5)

target = N(args.symbol)
mask = False
for c in name_cols:
    m = df_nse[c].fillna("").map(N)
    m2 = (m == target) | m.str.contains(target, na=False)
    mask = (mask | m2) if isinstance(mask, pd.Series) else m2

sel = df_nse[mask] if isinstance(mask, pd.Series) else df_nse.head(0)
if sel.empty:
    print(f"[ERROR] NOT FOUND in NSE for symbol like '{args.symbol}'. Try broader text or another symbol.")
    print("\nExample NSE equities in your CSV:")
    try:
        sample = df_nse[df_nse["SEGMENT"].astype(str).str.upper().str.startswith("E")].head(10)
        cols = [x for x in ["SECURITY_ID","DISPLAY_NAME","SYMBOL_NAME"] if x in df_nse.columns]
        print(sample[cols].to_string(index=False))
    except Exception:
        pass
    sys.exit(6)

# Deduplicate by SECURITY_ID
sel = sel.drop_duplicates(subset=["SECURITY_ID"])

# Prepare display
show_cols = [x for x in ["EXCH_ID","SEGMENT","INSTRUMENT","SECURITY_ID","DISPLAY_NAME","SYMBOL_NAME","UNDERLYING_SYMBOL"] if x in sel.columns]
view = sel[show_cols].head(args.show_top)

if len(view) > 1:
    print("Multiple NSE matches. Top candidates:")
    print(view.to_string(index=False))
    print("\nPicking the first candidate for convenience...")

row = view.iloc[0]
segment_api = map_exchange_segment(row["SEGMENT"], row.get("INSTRUMENT",""))

# Prefer DISPLAY_NAME > SYMBOL_NAME > UNDERLYING_SYMBOL > TRADING_SYMBOL > SYMBOL
prefer = None
for nm in ["DISPLAY_NAME","SYMBOL_NAME","UNDERLYING_SYMBOL","TRADING_SYMBOL","SYMBOL"]:
    if nm in row.index:
        prefer = nm; break
name_val = row[prefer] if prefer else "<unknown>"

print(f"securityId={row['SECURITY_ID']} | exch=NSE | seg={row['SEGMENT']} | segment_api={segment_api} | instrument={row.get('INSTRUMENT','')} | name={name_val}")
