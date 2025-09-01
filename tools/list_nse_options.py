# Lists NSE_FNO Options for an underlying, filtered by expiry window and strike band.
import argparse, pandas as pd
from datetime import datetime, timedelta

ap = argparse.ArgumentParser()
ap.add_argument("--csv", required=True)
ap.add_argument("--underlying", required=True, help="e.g., RELIANCE")
ap.add_argument("--days", type=int, default=45, help="Expiry ± days window around today")
ap.add_argument("--strike-min", type=float, default=1000)
ap.add_argument("--strike-max", type=float, default=6000)
args = ap.parse_args()

df = pd.read_csv(args.csv, dtype=str, low_memory=False)
df = df[(df["EXCH_ID"].str.upper()=="NSE") & (df["SEGMENT"].str.upper().str.startswith("D"))]
df = df[df["INSTRUMENT"].str.upper().str.startswith("OPT")]  # OPTSTK/OPTIDX

# Parse numeric strike
if "STRIKE_PRICE" in df.columns:
    try:
        df["STRIKE_PRICE_NUM"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce")
    except Exception:
        df["STRIKE_PRICE_NUM"] = None
else:
    df["STRIKE_PRICE_NUM"] = None

# Parse expiry
if "SM_EXPIRY_DATE" in df.columns:
    def parse_exp(x):
        if pd.isna(x): return None
        x = str(x).strip()
        for fmt in ("%d-%b-%Y","%d-%m-%Y","%Y-%m-%d","%d/%m/%Y","%d-%b-%y"):
            try: return datetime.strptime(x, fmt)
            except: pass
        return None
    df["EXPDT"] = df["SM_EXPIRY_DATE"].apply(parse_exp)
else:
    df["EXPDT"] = None

U = args.underlying.upper()
mask_u = (df["UNDERLYING_SYMBOL"].astype(str).str.upper()==U) | (df.get("SYMBOL_NAME","").astype(str).str.upper().str.contains(U))
df = df[mask_u]

today = datetime.utcnow().date()
if df["EXPDT"].notna().any():
    lo = today - timedelta(days=args.days)
    hi = today + timedelta(days=args.days)
    df = df[(df["EXPDT"].isna()) | ((df["EXPDT"].dt.date>=lo) & (df["EXPDT"].dt.date<=hi))]

if df["STRIKE_PRICE_NUM"].notna().any():
    df = df[(df["STRIKE_PRICE_NUM"]>=args.strike_min) & (df["STRIKE_PRICE_NUM"]<=args.strike_max)]

cols = [c for c in ["SECURITY_ID","DISPLAY_NAME","SYMBOL_NAME","UNDERLYING_SYMBOL","SM_EXPIRY_DATE","STRIKE_PRICE","INSTRUMENT"] if c in df.columns]
out = df[cols].drop_duplicates().sort_values(by=["SM_EXPIRY_DATE","STRIKE_PRICE"], na_position="last").head(40)
print(out.to_string(index=False))
