# Lists NSE_FNO Futures for an underlying, optionally near expiry window (±N days)
import argparse, pandas as pd
from datetime import datetime, timedelta

ap = argparse.ArgumentParser()
ap.add_argument("--csv", required=True)
ap.add_argument("--underlying", required=True, help="e.g., RELIANCE")
ap.add_argument("--days", type=int, default=45, help="Show expiries within +/- days from today")
args = ap.parse_args()

df = pd.read_csv(args.csv, dtype=str, low_memory=False)
df = df[(df["EXCH_ID"].str.upper()=="NSE") & (df["SEGMENT"].str.upper().str.startswith("D"))]
df = df[df["INSTRUMENT"].str.upper().str.startswith("FUT")]  # FUTSTK/FUTIDX

# Parse expiry if present
if "SM_EXPIRY_DATE" in df.columns:
    def parse_exp(x):
        if pd.isna(x): return None
        x = str(x).strip()
        # try common formats
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

cols = [c for c in ["SECURITY_ID","DISPLAY_NAME","SYMBOL_NAME","UNDERLYING_SYMBOL","SM_EXPIRY_DATE","INSTRUMENT"] if c in df.columns]
print(df[cols].drop_duplicates().head(20).to_string(index=False))
