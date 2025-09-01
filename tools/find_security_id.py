# C:\teevra18\tools\find_security_id.py
# Shows SECURITY_ID + EXCH_ID + SEGMENT + instrument + API-ready segment.
import argparse, re, sys
import pandas as pd

def N(x): return re.sub(r'[^A-Z0-9]', '', str(x).upper()) if x is not None else ""

def map_exchange_segment(exch_id:str, seg:str, instrument:str) -> str:
    e, s, i = N(exch_id), N(seg), N(instrument)
    # NSE
    if e == "NSE":
        if s in {"E","EQ","CM","EQUITY","CASH"}: return "NSE_EQ"
        if s in {"D","FO","FNO","F&O","DERIVATIVE","DERIVATIVES"}: return "NSE_FNO"
        if s in {"CD","CURRENCY","CURR","FX"}: return "NSE_CURRENCY"
        if i.startswith("OPT") or i.startswith("FUT"): return "NSE_FNO"
        return "NSE_EQ"
    # BSE
    if e == "BSE":
        if s in {"E","EQ","CM","EQUITY","CASH"}: return "BSE_EQ"
        if s in {"D","FO","FNO","F&O","DERIVATIVE","DERIVATIVES"}: return "BSE_FNO"
        return "BSE_EQ"
    # MCX
    if e == "MCX": return "MCX_COMM"
    # Fallbacks
    if i.startswith("OPT") or i.startswith("FUT"): return "NSE_FNO"
    return "NSE_EQ"

ap = argparse.ArgumentParser()
ap.add_argument("--csv", required=True)
ap.add_argument("--symbol", required=True, help="Name to search (uses DISPLAY_NAME / SYMBOL_NAME / UNDERLYING_SYMBOL)")
ap.add_argument("--show-top", type=int, default=10)
args = ap.parse_args()

df = pd.read_csv(args.csv, dtype=str, low_memory=False)
need = ["EXCH_ID","SEGMENT","SECURITY_ID"]
for c in need:
    if c not in df.columns:
        print(f"CSV missing required column: {c}. Found: {list(df.columns)}"); sys.exit(2)

name_cols = [c for c in ["DISPLAY_NAME","SYMBOL_NAME","UNDERLYING_SYMBOL","TRADING_SYMBOL","SYMBOL"] if c in df.columns]
if not name_cols:
    print("CSV lacks name columns (DISPLAY_NAME/SYMBOL_NAME/UNDERLYING_SYMBOL/TRADING_SYMBOL/SYMBOL)."); sys.exit(3)

target = N(args.symbol)
mask = False
for c in name_cols:
    m = df[c].fillna("").map(N)
    mask = (mask | (m == target) | m.str.contains(target, na=False)) if isinstance(mask, pd.Series) else ((m == target) | m.str.contains(target, na=False))

sel = df[mask] if isinstance(mask, pd.Series) else df.head(0)
if sel.empty:
    print("NOT FOUND with given filters."); sys.exit(4)

# Dedup by SECURITY_ID
sel = sel.drop_duplicates(subset=["SECURITY_ID"])
cols_to_show = ["EXCH_ID","SEGMENT","INSTRUMENT","SECURITY_ID"] + [c for c in name_cols if c in sel.columns]
view = sel[cols_to_show].head(args.show_top)

if len(view) > 1:
    print("Multiple matches. Top candidates:")
    print(view.to_string(index=False))
    print("\nPicking the first candidate for convenience...")

row = view.iloc[0]
api_seg = map_exchange_segment(row["EXCH_ID"], row["SEGMENT"], row.get("INSTRUMENT", ""))

# prefer DISPLAY_NAME > SYMBOL_NAME > UNDERLYING_SYMBOL > TRADING_SYMBOL > SYMBOL
pref = next((c for c in ["DISPLAY_NAME","SYMBOL_NAME","UNDERLYING_SYMBOL","TRADING_SYMBOL","SYMBOL"] if c in row.index), None)
name_val = row[pref] if pref else "<unknown>"

print(f"securityId={row['SECURITY_ID']} | exch={row['EXCH_ID']} | seg={row['SEGMENT']} | segment_api={api_seg} | instrument={row.get('INSTRUMENT','')} | name={name_val}")
