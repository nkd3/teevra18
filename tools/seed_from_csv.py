# C:\teevra18\tools\seed_from_csv.py
import os, sqlite3, re
import pandas as pd
from datetime import datetime

CSV = r"C:\teevra18\data\api-scrip-master-detailed.csv"
DB  = r"C:\teevra18\data\teevra18.db"

def die(msg):
    print("ERROR:", msg)
    raise SystemExit(1)

if not os.path.exists(CSV):
    die(f"CSV not found at {CSV}")

# Read CSV (keep types flexible, avoid dtype issues)
df = pd.read_csv(CSV, low_memory=False)

# Required Dhan columns (as commonly present)
need = ["EXCH_ID","SEGMENT","SECURITY_ID","UNDERLYING_SYMBOL",
        "SYMBOL_NAME","DISPLAY_NAME","INSTRUMENT","INSTRUMENT_TYPE",
        "SM_EXPIRY_DATE","STRIKE_PRICE","OPTION_TYPE"]

missing = [c for c in need if c not in df.columns]
if missing:
    print("Available columns:", list(df.columns))
    die(f"CSV missing columns: {missing}")

# Focus NSE Derivatives (Segment 'D') & NIFTY underlying
dd = df[(df["EXCH_ID"]=="NSE") & (df["SEGMENT"]=="D") & (df["UNDERLYING_SYMBOL"].astype(str)=="NIFTY")].copy()
if dd.empty:
    die("No rows for NSE / Derivatives / UNDERLYING_SYMBOL=NIFTY")

# Parse expiries as dates
dd["SM_EXPIRY_DATE"] = pd.to_datetime(dd["SM_EXPIRY_DATE"], errors="coerce")
today = pd.Timestamp.today().normalize()

# --- Pick NIFTY FUT (FUTIDX) with upcoming/nearest expiry ---
futs = dd[dd["INSTRUMENT"]=="FUTIDX"].dropna(subset=["SM_EXPIRY_DATE"]).copy()
if futs.empty:
    die("No FUTIDX rows for NIFTY in CSV")

futs_upcoming = futs[futs["SM_EXPIRY_DATE"] >= today].sort_values(["SM_EXPIRY_DATE","DISPLAY_NAME"])
if futs_upcoming.empty:
    # fallback to the latest FUTIDX if all are past
    fut_row = futs.sort_values(["SM_EXPIRY_DATE","DISPLAY_NAME"]).iloc[-1]
else:
    fut_row = futs_upcoming.iloc[0]

fut_sid  = int(fut_row["SECURITY_ID"])
fut_name = str(fut_row["DISPLAY_NAME"])
fut_exp  = pd.to_datetime(fut_row["SM_EXPIRY_DATE"]).date()

# --- Pick two near-ATM options (OPTIDX) around the same (or nearest) expiry ---
opts = dd[dd["INSTRUMENT"]=="OPTIDX"].dropna(subset=["SM_EXPIRY_DATE","STRIKE_PRICE"]).copy()
if opts.empty:
    die("No OPTIDX rows for NIFTY in CSV")

# Prefer same expiry as FUT, else nearest future expiry, else most common expiry
same_exp = opts[opts["SM_EXPIRY_DATE"]==fut_row["SM_EXPIRY_DATE"]].copy()
if same_exp.empty:
    future_exp = opts[opts["SM_EXPIRY_DATE"]>=today]["SM_EXPIRY_DATE"]
    if not future_exp.empty:
        nearest = future_exp.min()
        same_exp = opts[opts["SM_EXPIRY_DATE"]==nearest].copy()
if same_exp.empty:
    # choose modal expiry
    exp_counts = opts["SM_EXPIRY_DATE"].value_counts()
    if exp_counts.empty:
        die("Could not determine any option expiry to use")
    same_exp = opts[opts["SM_EXPIRY_DATE"]==exp_counts.idxmax()].copy()

same_exp = same_exp.sort_values("STRIKE_PRICE")
if same_exp.empty:
    die("Filtered option set is empty")

# Median strike as ATM proxy (no live LTP available off-market)
mid_idx = len(same_exp)//2
mid_strike = float(same_exp.iloc[mid_idx]["STRIKE_PRICE"])

# Helper: pick CE/PE at or nearest to the mid_strike
def pick_side(side: str):
    at = same_exp[(same_exp["OPTION_TYPE"]==side) & (same_exp["STRIKE_PRICE"]==mid_strike)]
    if not at.empty:
        r = at.iloc[0]
        return int(r["SECURITY_ID"]), str(r["DISPLAY_NAME"])
    # nearest by absolute difference
    side_df = same_exp[same_exp["OPTION_TYPE"]==side].copy()
    if side_df.empty:
        return None
    side_df["diff"] = (side_df["STRIKE_PRICE"] - mid_strike).abs()
    r = side_df.sort_values("diff").iloc[0]
    return int(r["SECURITY_ID"]), str(r["DISPLAY_NAME"])

pick_ce = pick_side("CE")
pick_pe = pick_side("PE")
if pick_ce is None or pick_pe is None:
    die("Could not find both CE and PE near ATM")

# --- Write to DB: universe_watchlist (used by M1) ---
con = sqlite3.connect(DB)
con.executescript("""
CREATE TABLE IF NOT EXISTS universe_watchlist (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  exchange_segment INTEGER NOT NULL,
  security_id INTEGER NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  is_hot_option INTEGER NOT NULL DEFAULT 0
);
""")

con.execute("DELETE FROM universe_watchlist;")
con.execute("INSERT INTO universe_watchlist(exchange_segment,security_id,is_hot_option) VALUES(2,?,0)", (fut_sid,))
con.execute("INSERT INTO universe_watchlist(exchange_segment,security_id,is_hot_option) VALUES(2,?,1)", (pick_pe[0],))
con.execute("INSERT INTO universe_watchlist(exchange_segment,security_id,is_hot_option) VALUES(2,?,1)", (pick_ce[0],))
con.commit()

print("Seeded watchlist:")
print("  FUT:", fut_sid, "-", fut_name, "(expiry:", fut_exp, ")")
print("  PE :", pick_pe[0], "-", pick_pe[1])
print("  CE :", pick_ce[0], "-", pick_ce[1])
